import csv
import io
import json
import os
import sqlite3
import zipfile
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

from flask import Flask, Response, jsonify, request

APP_NAME = "FX Session Structure Logger v1.1"
DB_PATH = os.environ.get("DB_PATH", "fx_session_logger.sqlite")
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "").strip()

app = Flask(__name__)


# ----------------------------- Utilities ------------------------------------

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def ms_to_iso(ms: Any) -> Optional[str]:
    try:
        if ms is None:
            return None
        return datetime.fromtimestamp(float(ms) / 1000.0, tz=timezone.utc).isoformat(timespec="seconds")
    except Exception:
        return None


def parse_dt(value: Any) -> Optional[datetime]:
    s = str(value or "").strip()
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def display_uk(value: Any) -> str:
    dt = parse_dt(value)
    if not dt:
        return str(value or "")
    if ZoneInfo is None:
        return dt.strftime("%Y-%m-%d %H:%M UTC")
    return dt.astimezone(ZoneInfo("Europe/London")).strftime("%Y-%m-%d %H:%M UK")


def as_float(v: Any) -> Optional[float]:
    if v in (None, "", "null"):
        return None
    try:
        return float(v)
    except Exception:
        return None


def as_int(v: Any) -> Optional[int]:
    if v in (None, "", "null"):
        return None
    try:
        return int(float(v))
    except Exception:
        return None


def as_bool(v: Any) -> Optional[int]:
    if v is None:
        return None
    if isinstance(v, bool):
        return 1 if v else 0
    if isinstance(v, (int, float)):
        return 1 if v else 0
    s = str(v).strip().lower()
    if s in ("true", "1", "yes", "y"):
        return 1
    if s in ("false", "0", "no", "n"):
        return 0
    return None


def boolish(v: Any) -> bool:
    return bool(as_bool(v))


def esc(value: Any) -> str:
    return str(value if value is not None else "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def fmt(value: Any, dp: int = 3) -> str:
    v = as_float(value)
    if v is None:
        return ""
    return f"{v:.{dp}f}"


def fmt_int(value: Any) -> str:
    try:
        return str(int(value or 0))
    except Exception:
        return "0"


def pnl_class(value: Any) -> str:
    v = as_float(value)
    if v is None:
        return "flat"
    if v > 0:
        return "pos"
    if v < 0:
        return "neg"
    return "flat"


def connect() -> sqlite3.Connection:
    parent = os.path.dirname(DB_PATH)
    if parent:
        os.makedirs(parent, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        conn.execute("PRAGMA busy_timeout = 30000")
    except Exception:
        pass
    return conn


def init_db() -> None:
    with connect() as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            signal_id TEXT PRIMARY KEY,
            received_at TEXT,
            pair TEXT,
            tickerid TEXT,
            timeframe TEXT,
            signal_time_utc TEXT,
            bar_time_ms INTEGER,
            model_version TEXT,
            model_family TEXT,
            setup_name TEXT,
            direction TEXT,
            session_timezone TEXT,
            entry REAL,
            stop REAL,
            risk_pips REAL,
            sl_pct REAL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            asia_high REAL,
            asia_low REAL,
            london_high REAL,
            london_low REAL,
            london_mid REAL,
            london_range_pips REAL,
            prev_day_high REAL,
            prev_day_low REAL,
            entry_vs_london_mid_pips REAL,
            swept_london_high_today INTEGER,
            swept_london_low_today INTEGER,
            in_asia_session INTEGER,
            in_london_session INTEGER,
            in_ny_session INTEGER,
            raw_json TEXT
        )
        """)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS outcomes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_id TEXT,
            received_at TEXT,
            pair TEXT,
            timeframe TEXT,
            outcome_time_utc TEXT,
            bar_time_ms INTEGER,
            model_version TEXT,
            model_family TEXT,
            direction TEXT,
            horizon_min INTEGER,
            entry REAL,
            stop REAL,
            close REAL,
            raw_return_pct REAL,
            raw_return_r REAL,
            mfe_pct REAL,
            mae_pct REAL,
            sl_hit INTEGER,
            outcome_with_sl_pct REAL,
            outcome_with_sl_r REAL,
            raw_json TEXT,
            UNIQUE(signal_id, horizon_min)
        )
        """)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS candles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            received_at TEXT,
            pair TEXT,
            tickerid TEXT,
            timeframe TEXT,
            bar_time_utc TEXT,
            bar_time_ms INTEGER,
            model_version TEXT,
            session_timezone TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            asia_high REAL,
            asia_low REAL,
            london_high REAL,
            london_low REAL,
            london_mid REAL,
            london_range_pips REAL,
            prev_day_high REAL,
            prev_day_low REAL,
            swept_london_high_today INTEGER,
            swept_london_low_today INTEGER,
            in_asia_session INTEGER,
            in_london_session INTEGER,
            in_ny_session INTEGER,
            raw_json TEXT,
            UNIQUE(pair, timeframe, bar_time_ms)
        )
        """)
        conn.commit()


init_db()


# ----------------------------- Inserts --------------------------------------

def insert_signal(data: Dict[str, Any]) -> None:
    with connect() as conn:
        conn.execute("""
        INSERT OR REPLACE INTO signals (
            signal_id, received_at, pair, tickerid, timeframe, signal_time_utc, bar_time_ms,
            model_version, model_family, setup_name, direction, session_timezone,
            entry, stop, risk_pips, sl_pct, open, high, low, close,
            asia_high, asia_low, london_high, london_low, london_mid, london_range_pips,
            prev_day_high, prev_day_low, entry_vs_london_mid_pips,
            swept_london_high_today, swept_london_low_today,
            in_asia_session, in_london_session, in_ny_session, raw_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            data.get("signal_id"), now_iso(), data.get("pair"), data.get("tickerid"), data.get("timeframe"),
            ms_to_iso(data.get("bar_time_ms")), as_int(data.get("bar_time_ms")),
            data.get("model_version"), data.get("model_family"), data.get("setup_name"), data.get("direction"),
            data.get("session_timezone"), as_float(data.get("entry")), as_float(data.get("stop")),
            as_float(data.get("risk_pips")), as_float(data.get("sl_pct")),
            as_float(data.get("open")), as_float(data.get("high")), as_float(data.get("low")), as_float(data.get("close")),
            as_float(data.get("asia_high")), as_float(data.get("asia_low")),
            as_float(data.get("london_high")), as_float(data.get("london_low")), as_float(data.get("london_mid")),
            as_float(data.get("london_range_pips")), as_float(data.get("prev_day_high")), as_float(data.get("prev_day_low")),
            as_float(data.get("entry_vs_london_mid_pips")),
            as_bool(data.get("swept_london_high_today")), as_bool(data.get("swept_london_low_today")),
            as_bool(data.get("in_asia_session")), as_bool(data.get("in_london_session")), as_bool(data.get("in_ny_session")),
            json.dumps(data, separators=(",", ":")),
        ))
        conn.commit()


def insert_outcome(data: Dict[str, Any]) -> None:
    with connect() as conn:
        conn.execute("""
        INSERT OR REPLACE INTO outcomes (
            signal_id, received_at, pair, timeframe, outcome_time_utc, bar_time_ms,
            model_version, model_family, direction, horizon_min,
            entry, stop, close, raw_return_pct, raw_return_r,
            mfe_pct, mae_pct, sl_hit, outcome_with_sl_pct, outcome_with_sl_r, raw_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            data.get("signal_id"), now_iso(), data.get("pair"), data.get("timeframe"),
            ms_to_iso(data.get("bar_time_ms")), as_int(data.get("bar_time_ms")),
            data.get("model_version"), data.get("model_family"), data.get("direction"), as_int(data.get("horizon_min")),
            as_float(data.get("entry")), as_float(data.get("stop")), as_float(data.get("close")),
            as_float(data.get("raw_return_pct")), as_float(data.get("raw_return_r")),
            as_float(data.get("mfe_pct")), as_float(data.get("mae_pct")), as_bool(data.get("sl_hit")),
            as_float(data.get("outcome_with_sl_pct")), as_float(data.get("outcome_with_sl_r")),
            json.dumps(data, separators=(",", ":")),
        ))
        conn.commit()


def insert_candle(data: Dict[str, Any]) -> None:
    with connect() as conn:
        conn.execute("""
        INSERT OR REPLACE INTO candles (
            received_at, pair, tickerid, timeframe, bar_time_utc, bar_time_ms,
            model_version, session_timezone, open, high, low, close,
            asia_high, asia_low, london_high, london_low, london_mid, london_range_pips,
            prev_day_high, prev_day_low, swept_london_high_today, swept_london_low_today,
            in_asia_session, in_london_session, in_ny_session, raw_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            now_iso(), data.get("pair"), data.get("tickerid"), data.get("timeframe"),
            ms_to_iso(data.get("bar_time_ms")), as_int(data.get("bar_time_ms")),
            data.get("model_version"), data.get("session_timezone"),
            as_float(data.get("open")), as_float(data.get("high")), as_float(data.get("low")), as_float(data.get("close")),
            as_float(data.get("asia_high")), as_float(data.get("asia_low")),
            as_float(data.get("london_high")), as_float(data.get("london_low")), as_float(data.get("london_mid")),
            as_float(data.get("london_range_pips")), as_float(data.get("prev_day_high")), as_float(data.get("prev_day_low")),
            as_bool(data.get("swept_london_high_today")), as_bool(data.get("swept_london_low_today")),
            as_bool(data.get("in_asia_session")), as_bool(data.get("in_london_session")), as_bool(data.get("in_ny_session")),
            json.dumps(data, separators=(",", ":")),
        ))
        conn.commit()


# ----------------------------- Summary logic --------------------------------

def merged_sql() -> str:
    return """
    SELECT
        s.*,
        o60.raw_return_pct AS raw_60m_pct,
        o60.raw_return_r AS raw_60m_r,
        o60.outcome_with_sl_pct AS fixed_60m_pct,
        o60.outcome_with_sl_r AS fixed_60m_r,
        o60.mfe_pct AS mfe_60m_pct,
        o60.mae_pct AS mae_60m_pct,
        o60.sl_hit AS sl_hit_60m,

        o120.raw_return_pct AS raw_120m_pct,
        o120.raw_return_r AS raw_120m_r,
        o120.outcome_with_sl_pct AS fixed_120m_pct,
        o120.outcome_with_sl_r AS fixed_120m_r,
        o120.mfe_pct AS mfe_120m_pct,
        o120.mae_pct AS mae_120m_pct,
        o120.sl_hit AS sl_hit_120m,

        o240.raw_return_pct AS raw_240m_pct,
        o240.raw_return_r AS raw_240m_r,
        o240.outcome_with_sl_pct AS fixed_240m_pct,
        o240.outcome_with_sl_r AS fixed_240m_r,
        o240.mfe_pct AS mfe_240m_pct,
        o240.mae_pct AS mae_240m_pct,
        o240.sl_hit AS sl_hit_240m
    FROM signals s
    LEFT JOIN outcomes o60 ON s.signal_id = o60.signal_id AND o60.horizon_min = 60
    LEFT JOIN outcomes o120 ON s.signal_id = o120.signal_id AND o120.horizon_min = 120
    LEFT JOIN outcomes o240 ON s.signal_id = o240.signal_id AND o240.horizon_min = 240
    ORDER BY s.bar_time_ms DESC
    """


def load_merged_rows() -> List[Dict[str, Any]]:
    with connect() as conn:
        rows = [dict(r) for r in conn.execute(merged_sql()).fetchall()]
    for r in rows:
        enrich_row(r)
    return rows


def range_bucket(pips: Any) -> str:
    v = as_float(pips)
    if v is None:
        return "unknown"
    if v < 25:
        return "tight_lt_25p"
    if v <= 60:
        return "normal_25_60p"
    return "wide_gt_60p"


def entry_mid_bucket(v: Any) -> str:
    x = as_float(v)
    if x is None:
        return "unknown"
    if x > 5:
        return "above_mid_gt_5p"
    if x < -5:
        return "below_mid_gt_5p"
    return "near_mid_±5p"


def sweep_bucket(high_swept: Any, low_swept: Any) -> str:
    h = boolish(high_swept)
    l = boolish(low_swept)
    if h and l:
        return "both_high_and_low_swept"
    if h:
        return "high_swept"
    if l:
        return "low_swept"
    return "no_london_sweep_yet"


def ny_time_bucket(signal_time_utc: Any) -> str:
    dt = parse_dt(signal_time_utc)
    if not dt:
        return "unknown"
    if ZoneInfo is not None:
        dt = dt.astimezone(ZoneInfo("Europe/London"))
    hour = dt.hour
    minute = dt.minute
    minutes = hour * 60 + minute
    if minutes < 13 * 60 + 30:
        return "early_NY_1230_1329"
    if minutes < 15 * 60:
        return "mid_NY_1330_1459"
    return "late_NY_1500_plus"


def enrich_row(r: Dict[str, Any]) -> None:
    r["range_bucket"] = range_bucket(r.get("london_range_pips"))
    r["entry_mid_bucket"] = entry_mid_bucket(r.get("entry_vs_london_mid_pips"))
    r["sweep_bucket"] = sweep_bucket(r.get("swept_london_high_today"), r.get("swept_london_low_today"))
    r["ny_time_bucket"] = ny_time_bucket(r.get("signal_time_utc"))
    r["pair_family_direction"] = f"{r.get('pair') or ''} | {r.get('model_family') or ''} | {r.get('direction') or ''}"
    r["signal_time_uk"] = display_uk(r.get("signal_time_utc"))
    sl_pct = as_float(r.get("sl_pct"))
    for h in [60, 120, 240]:
        mfe = as_float(r.get(f"mfe_{h}m_pct"))
        mae = as_float(r.get(f"mae_{h}m_pct"))
        r[f"mfe_{h}m_r"] = (mfe / sl_pct) if (mfe is not None and sl_pct and sl_pct > 0) else None
        r[f"mae_{h}m_r"] = (mae / sl_pct) if (mae is not None and sl_pct and sl_pct > 0) else None


def avg(vals: List[Optional[float]]) -> Optional[float]:
    clean = [v for v in vals if v is not None]
    if not clean:
        return None
    return sum(clean) / len(clean)


def summarise(rows: List[Dict[str, Any]], group_key: str) -> List[Dict[str, Any]]:
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        key = str(r.get(group_key) if r.get(group_key) not in (None, "") else "unknown")
        groups.setdefault(key, []).append(r)

    out = []
    for key, rs in groups.items():
        completed_60 = [r for r in rs if as_float(r.get("raw_60m_r")) is not None]
        completed_120 = [r for r in rs if as_float(r.get("raw_120m_r")) is not None]
        completed_240 = [r for r in rs if as_float(r.get("raw_240m_r")) is not None]
        row = {
            "group": key,
            "rows": len(rs),
            "done_60": len(completed_60),
            "done_120": len(completed_120),
            "done_240": len(completed_240),
            "avg_fixed_60m_R": avg([as_float(r.get("fixed_60m_r")) for r in rs]),
            "avg_fixed_120m_R": avg([as_float(r.get("fixed_120m_r")) for r in rs]),
            "avg_raw_60m_R": avg([as_float(r.get("raw_60m_r")) for r in rs]),
            "avg_raw_120m_R": avg([as_float(r.get("raw_120m_r")) for r in rs]),
            "avg_raw_240m_R": avg([as_float(r.get("raw_240m_r")) for r in rs]),
            "avg_MFE_120m_R": avg([as_float(r.get("mfe_120m_r")) for r in rs]),
            "avg_MAE_120m_R": avg([as_float(r.get("mae_120m_r")) for r in rs]),
            "fixed_120m_SL_hits": sum(1 for r in rs if boolish(r.get("sl_hit_120m"))),
            "fixed_120m_win_rate_pct": None,
        }
        done_fixed = [as_float(r.get("fixed_120m_r")) for r in rs if as_float(r.get("fixed_120m_r")) is not None]
        if done_fixed:
            row["fixed_120m_win_rate_pct"] = sum(1 for v in done_fixed if v > 0) / len(done_fixed) * 100.0
        out.append(row)

    out.sort(key=lambda x: (-(as_float(x.get("avg_fixed_120m_R")) if as_float(x.get("avg_fixed_120m_R")) is not None else -999), str(x["group"])))
    return out


def table_html(rows: List[Dict[str, Any]], max_rows: int = 200) -> str:
    if not rows:
        return "<p class='muted'>No rows yet.</p>"
    headers = list(rows[0].keys())
    head = "".join(f"<th>{esc(h)}</th>" for h in headers)
    body = ""
    for r in rows[:max_rows]:
        cells = ""
        for h in headers:
            v = r.get(h)
            cls = pnl_class(v) if any(x in h.lower() for x in ["avg_", "return", "_r", "win_rate"]) else ""
            if isinstance(v, float):
                text = fmt(v, 3 if "pct" not in h.lower() else 1)
            else:
                text = esc(v)
            cells += f"<td class='{cls}'>{text}</td>"
        body += f"<tr>{cells}</tr>"
    return f"<div class='table-scroll'><table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table></div>"


def latest_signals_table(rows: List[Dict[str, Any]], limit: int = 30) -> List[Dict[str, Any]]:
    out = []
    for r in rows[:limit]:
        out.append({
            "time_uk": r.get("signal_time_uk"),
            "pair": r.get("pair"),
            "family": r.get("model_family"),
            "direction": r.get("direction"),
            "entry": r.get("entry"),
            "london_range_pips": r.get("london_range_pips"),
            "range_bucket": r.get("range_bucket"),
            "entry_mid_bucket": r.get("entry_mid_bucket"),
            "sweep_bucket": r.get("sweep_bucket"),
            "fixed_120m_R": r.get("fixed_120m_r"),
            "raw_240m_R": r.get("raw_240m_r"),
        })
    return out


# ----------------------------- Routes ---------------------------------------

@app.get("/health")
def health():
    with connect() as conn:
        signals = conn.execute("SELECT COUNT(*) AS n FROM signals").fetchone()["n"]
        outcomes = conn.execute("SELECT COUNT(*) AS n FROM outcomes").fetchone()["n"]
        candles = conn.execute("SELECT COUNT(*) AS n FROM candles").fetchone()["n"]
        pairs = conn.execute("SELECT COUNT(DISTINCT pair) AS n FROM signals").fetchone()["n"]
    return jsonify({
        "ok": True,
        "app": "fx_session_logger_v1_1",
        "time_utc": now_iso(),
        "db_path": DB_PATH,
        "signals": signals,
        "outcomes": outcomes,
        "candles": candles,
        "pairs_seen": pairs,
    })


@app.post("/webhook")
def webhook():
    try:
        data = request.get_json(force=True, silent=False)
    except Exception as e:
        return jsonify({"ok": False, "error": f"Invalid JSON: {e}"}), 400

    if not isinstance(data, dict):
        return jsonify({"ok": False, "error": "JSON body must be an object"}), 400

    if WEBHOOK_SECRET:
        # Accept either JSON-body secret or URL secret:
        # /webhook?secret=YOUR_SECRET
        supplied = str(data.get("secret") or request.args.get("secret", "")).strip()
        if supplied != WEBHOOK_SECRET:
            return jsonify({"ok": False, "error": "Bad webhook secret"}), 403

    event_type = str(data.get("event_type", "")).strip().lower()

    try:
        if event_type == "signal":
            if not data.get("signal_id"):
                return jsonify({"ok": False, "error": "Missing signal_id"}), 400
            insert_signal(data)
        elif event_type == "outcome":
            if not data.get("signal_id"):
                return jsonify({"ok": False, "error": "Missing signal_id"}), 400
            insert_outcome(data)
        elif event_type == "bar":
            insert_candle(data)
        else:
            return jsonify({"ok": False, "error": f"Unsupported event_type: {event_type}"}), 400
    except Exception as e:
        return jsonify({"ok": False, "error": f"DB insert failed: {e}"}), 500

    return jsonify({"ok": True, "event_type": event_type, "time_utc": now_iso()})


def rows_to_csv(rows, headers) -> str:
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(headers)
    for row in rows:
        writer.writerow([row.get(h) if isinstance(row, dict) else row[h] for h in headers])
    return output.getvalue()


def csv_response(filename: str, rows: List[Dict[str, Any]]) -> Response:
    headers = list(rows[0].keys()) if rows else []
    csv_text = rows_to_csv(rows, headers)
    return Response(csv_text, mimetype="text/csv", headers={"Content-Disposition": f"attachment; filename={filename}"})


@app.get("/download/signals.csv")
def download_signals():
    with connect() as conn:
        rows = [dict(r) for r in conn.execute("SELECT * FROM signals ORDER BY bar_time_ms DESC").fetchall()]
    return csv_response("fx_signals.csv", rows)


@app.get("/download/outcomes.csv")
def download_outcomes():
    with connect() as conn:
        rows = [dict(r) for r in conn.execute("SELECT * FROM outcomes ORDER BY bar_time_ms DESC, horizon_min").fetchall()]
    return csv_response("fx_outcomes.csv", rows)


@app.get("/download/candles.csv")
def download_candles():
    with connect() as conn:
        rows = [dict(r) for r in conn.execute("SELECT * FROM candles ORDER BY bar_time_ms DESC").fetchall()]
    return csv_response("fx_candles.csv", rows)


@app.get("/download/merged.csv")
def download_merged():
    return csv_response("fx_merged_signals_outcomes.csv", load_merged_rows())


@app.get("/download/summary-by-pair.csv")
def download_summary_by_pair():
    return csv_response("fx_summary_by_pair.csv", summarise(load_merged_rows(), "pair"))


@app.get("/download/summary-all.csv")
def download_summary_all():
    rows = load_merged_rows()
    sections = []
    for group_key in ["pair", "model_family", "direction", "pair_family_direction", "range_bucket", "entry_mid_bucket", "sweep_bucket", "ny_time_bucket"]:
        for r in summarise(rows, group_key):
            d = {"section": group_key}
            d.update(r)
            sections.append(d)
    return csv_response("fx_summary_all.csv", sections)


@app.get("/download/all.zip")
def download_all_zip():
    merged = load_merged_rows()
    files = {
        "fx_merged_signals_outcomes.csv": merged,
        "fx_summary_by_pair.csv": summarise(merged, "pair"),
        "fx_summary_by_family.csv": summarise(merged, "model_family"),
        "fx_summary_by_direction.csv": summarise(merged, "direction"),
        "fx_summary_by_pair_family_direction.csv": summarise(merged, "pair_family_direction"),
        "fx_summary_by_london_range_bucket.csv": summarise(merged, "range_bucket"),
        "fx_summary_by_entry_mid_bucket.csv": summarise(merged, "entry_mid_bucket"),
        "fx_summary_by_sweep_bucket.csv": summarise(merged, "sweep_bucket"),
        "fx_summary_by_ny_time_bucket.csv": summarise(merged, "ny_time_bucket"),
    }
    with connect() as conn:
        files["fx_signals.csv"] = [dict(r) for r in conn.execute("SELECT * FROM signals ORDER BY bar_time_ms DESC").fetchall()]
        files["fx_outcomes.csv"] = [dict(r) for r in conn.execute("SELECT * FROM outcomes ORDER BY bar_time_ms DESC, horizon_min").fetchall()]
        files["fx_candles.csv"] = [dict(r) for r in conn.execute("SELECT * FROM candles ORDER BY bar_time_ms DESC").fetchall()]

    mem = io.BytesIO()
    with zipfile.ZipFile(mem, "w", zipfile.ZIP_DEFLATED) as z:
        for filename, rows in files.items():
            headers = list(rows[0].keys()) if rows else []
            z.writestr(filename, rows_to_csv(rows, headers))
    mem.seek(0)
    return Response(mem.getvalue(), mimetype="application/zip", headers={"Content-Disposition": "attachment; filename=fx_session_logger_export.zip"})


@app.get("/")
@app.get("/summary")
def dashboard():
    with connect() as conn:
        counts = conn.execute("""
            SELECT
                (SELECT COUNT(*) FROM signals) AS signals,
                (SELECT COUNT(*) FROM outcomes) AS outcomes,
                (SELECT COUNT(*) FROM candles) AS candles,
                (SELECT COUNT(DISTINCT pair) FROM signals) AS pairs
        """).fetchone()

    rows = load_merged_rows()

    html = f"""
    <!doctype html>
    <html>
    <head>
        <title>FX Session Logger v1.1</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 24px; background: #101217; color: #e8e8e8; }}
            a {{ color: #8ab4ff; }}
            .cards {{ display: flex; gap: 12px; flex-wrap: wrap; margin: 12px 0 24px; }}
            .card {{ background: #1b1f2a; border: 1px solid #33394a; border-radius: 10px; padding: 14px; min-width: 155px; }}
            .big {{ font-size: 30px; font-weight: 700; }}
            .muted {{ color: #a9b0c3; }}
            .note {{ background: #151923; border: 1px solid #33394a; padding: 12px; border-radius: 10px; color: #c9d1e6; }}
            table {{ border-collapse: collapse; width: 100%; margin: 12px 0 30px; background: #151923; }}
            th, td {{ border: 1px solid #303647; padding: 7px; font-size: 13px; }}
            th {{ background: #202638; text-align: left; position: sticky; top: 0; }}
            h1, h2 {{ margin-top: 26px; }}
            .downloads a {{ margin-right: 18px; }}
            .table-scroll {{ overflow-x: auto; }}
            .pos {{ color: #72e28a; }}
            .neg {{ color: #ff8c8c; }}
            .flat {{ color: #e8e8e8; }}
            details {{ margin: 18px 0; border: 1px solid #303647; border-radius: 10px; padding: 10px 14px; background: #121722; }}
            summary {{ cursor: pointer; font-size: 19px; font-weight: 700; }}
            code {{ color: #b8c7ff; }}
        </style>
    </head>
    <body>
        <h1>FX Session Structure Logger v1.1</h1>
        <p class="muted">Shadow-only logger for London range continuation and London sweep-and-reclaim FX research.</p>

        <div class="cards">
            <div class="card"><div class="muted">Signals</div><div class="big">{counts['signals']}</div></div>
            <div class="card"><div class="muted">Outcomes</div><div class="big">{counts['outcomes']}</div></div>
            <div class="card"><div class="muted">Candles</div><div class="big">{counts['candles']}</div></div>
            <div class="card"><div class="muted">Pairs seen</div><div class="big">{counts['pairs']}</div></div>
        </div>

        <div class="note">
            <strong>Research rule:</strong> do not trust early rows. Use this dashboard to check plumbing and patterns only. Real judgement needs 8–12 weeks of data.
            Main decision metric for now is <code>avg_fixed_120m_R</code>, supported by raw 240m follow-through, MFE/MAE, and SL-hit behaviour.
        </div>

        <h2>Downloads</h2>
        <p class="downloads">
            <a href="/download/merged.csv">Merged CSV</a>
            <a href="/download/summary-all.csv">Summary All CSV</a>
            <a href="/download/summary-by-pair.csv">Summary by Pair CSV</a>
            <a href="/download/signals.csv">Signals CSV</a>
            <a href="/download/outcomes.csv">Outcomes CSV</a>
            <a href="/download/candles.csv">Candles CSV</a>
            <a href="/download/all.zip">All ZIP</a>
        </p>

        <details open>
            <summary>By Pair</summary>
            {table_html(summarise(rows, "pair"))}
        </details>

        <details open>
            <summary>By Signal Family</summary>
            {table_html(summarise(rows, "model_family"))}
        </details>

        <details open>
            <summary>By Direction</summary>
            {table_html(summarise(rows, "direction"))}
        </details>

        <details open>
            <summary>By Pair + Family + Direction</summary>
            {table_html(summarise(rows, "pair_family_direction"))}
        </details>

        <details>
            <summary>By London Range Size</summary>
            {table_html(summarise(rows, "range_bucket"))}
        </details>

        <details>
            <summary>By Entry vs London Midpoint</summary>
            {table_html(summarise(rows, "entry_mid_bucket"))}
        </details>

        <details>
            <summary>By Sweep Context</summary>
            {table_html(summarise(rows, "sweep_bucket"))}
        </details>

        <details>
            <summary>By NY Time Bucket</summary>
            {table_html(summarise(rows, "ny_time_bucket"))}
        </details>

        <h2>Latest Signals</h2>
        {table_html(latest_signals_table(rows))}
    </body>
    </html>
    """
    return Response(html, mimetype="text/html")


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
