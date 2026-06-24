import csv
import io
import json
import os
import sqlite3
import zipfile
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from flask import Flask, Response, jsonify, request

app = Flask(__name__)

DB_PATH = os.environ.get("DB_PATH", "fx_session_logger.sqlite")
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "").strip()


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


def connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
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
            data.get("signal_id"),
            now_iso(),
            data.get("pair"),
            data.get("tickerid"),
            data.get("timeframe"),
            ms_to_iso(data.get("bar_time_ms")),
            as_int(data.get("bar_time_ms")),
            data.get("model_version"),
            data.get("model_family"),
            data.get("setup_name"),
            data.get("direction"),
            data.get("session_timezone"),
            as_float(data.get("entry")),
            as_float(data.get("stop")),
            as_float(data.get("risk_pips")),
            as_float(data.get("sl_pct")),
            as_float(data.get("open")),
            as_float(data.get("high")),
            as_float(data.get("low")),
            as_float(data.get("close")),
            as_float(data.get("asia_high")),
            as_float(data.get("asia_low")),
            as_float(data.get("london_high")),
            as_float(data.get("london_low")),
            as_float(data.get("london_mid")),
            as_float(data.get("london_range_pips")),
            as_float(data.get("prev_day_high")),
            as_float(data.get("prev_day_low")),
            as_float(data.get("entry_vs_london_mid_pips")),
            as_bool(data.get("swept_london_high_today")),
            as_bool(data.get("swept_london_low_today")),
            as_bool(data.get("in_asia_session")),
            as_bool(data.get("in_london_session")),
            as_bool(data.get("in_ny_session")),
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
            data.get("signal_id"),
            now_iso(),
            data.get("pair"),
            data.get("timeframe"),
            ms_to_iso(data.get("bar_time_ms")),
            as_int(data.get("bar_time_ms")),
            data.get("model_version"),
            data.get("model_family"),
            data.get("direction"),
            as_int(data.get("horizon_min")),
            as_float(data.get("entry")),
            as_float(data.get("stop")),
            as_float(data.get("close")),
            as_float(data.get("raw_return_pct")),
            as_float(data.get("raw_return_r")),
            as_float(data.get("mfe_pct")),
            as_float(data.get("mae_pct")),
            as_bool(data.get("sl_hit")),
            as_float(data.get("outcome_with_sl_pct")),
            as_float(data.get("outcome_with_sl_r")),
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
            now_iso(),
            data.get("pair"),
            data.get("tickerid"),
            data.get("timeframe"),
            ms_to_iso(data.get("bar_time_ms")),
            as_int(data.get("bar_time_ms")),
            data.get("model_version"),
            data.get("session_timezone"),
            as_float(data.get("open")),
            as_float(data.get("high")),
            as_float(data.get("low")),
            as_float(data.get("close")),
            as_float(data.get("asia_high")),
            as_float(data.get("asia_low")),
            as_float(data.get("london_high")),
            as_float(data.get("london_low")),
            as_float(data.get("london_mid")),
            as_float(data.get("london_range_pips")),
            as_float(data.get("prev_day_high")),
            as_float(data.get("prev_day_low")),
            as_bool(data.get("swept_london_high_today")),
            as_bool(data.get("swept_london_low_today")),
            as_bool(data.get("in_asia_session")),
            as_bool(data.get("in_london_session")),
            as_bool(data.get("in_ny_session")),
            json.dumps(data, separators=(",", ":")),
        ))
        conn.commit()


# ----------------------------- Routes ---------------------------------------

@app.get("/health")
def health():
    with connect() as conn:
        signals = conn.execute("SELECT COUNT(*) AS n FROM signals").fetchone()["n"]
        outcomes = conn.execute("SELECT COUNT(*) AS n FROM outcomes").fetchone()["n"]
        candles = conn.execute("SELECT COUNT(*) AS n FROM candles").fetchone()["n"]
    return jsonify({
        "ok": True,
        "app": "fx_session_logger_v1",
        "time_utc": now_iso(),
        "signals": signals,
        "outcomes": outcomes,
        "candles": candles,
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
        # Accept the secret either inside the TradingView JSON body or in the webhook URL:
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
        writer.writerow([row[h] if h in row.keys() else None for h in headers])
    return output.getvalue()


def csv_response(filename: str, sql: str, headers=None) -> Response:
    with connect() as conn:
        rows = conn.execute(sql).fetchall()
        if headers is None:
            headers = rows[0].keys() if rows else []
        csv_text = rows_to_csv(rows, list(headers))
    return Response(
        csv_text,
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )


@app.get("/download/signals.csv")
def download_signals():
    return csv_response("fx_signals.csv", "SELECT * FROM signals ORDER BY bar_time_ms DESC")


@app.get("/download/outcomes.csv")
def download_outcomes():
    return csv_response("fx_outcomes.csv", "SELECT * FROM outcomes ORDER BY bar_time_ms DESC, horizon_min")


@app.get("/download/candles.csv")
def download_candles():
    return csv_response("fx_candles.csv", "SELECT * FROM candles ORDER BY bar_time_ms DESC")


@app.get("/download/merged.csv")
def download_merged():
    sql = """
    SELECT
        s.*,
        o60.raw_return_pct AS raw_60m_pct,
        o60.raw_return_r AS raw_60m_r,
        o60.outcome_with_sl_pct AS sl_60m_pct,
        o60.outcome_with_sl_r AS sl_60m_r,
        o60.mfe_pct AS mfe_60m_pct,
        o60.mae_pct AS mae_60m_pct,
        o60.sl_hit AS sl_hit_60m,

        o120.raw_return_pct AS raw_120m_pct,
        o120.raw_return_r AS raw_120m_r,
        o120.outcome_with_sl_pct AS sl_120m_pct,
        o120.outcome_with_sl_r AS sl_120m_r,
        o120.mfe_pct AS mfe_120m_pct,
        o120.mae_pct AS mae_120m_pct,
        o120.sl_hit AS sl_hit_120m,

        o240.raw_return_pct AS raw_240m_pct,
        o240.raw_return_r AS raw_240m_r,
        o240.mfe_pct AS mfe_240m_pct,
        o240.mae_pct AS mae_240m_pct,
        o240.sl_hit AS sl_seen_by_240m
    FROM signals s
    LEFT JOIN outcomes o60 ON s.signal_id = o60.signal_id AND o60.horizon_min = 60
    LEFT JOIN outcomes o120 ON s.signal_id = o120.signal_id AND o120.horizon_min = 120
    LEFT JOIN outcomes o240 ON s.signal_id = o240.signal_id AND o240.horizon_min = 240
    ORDER BY s.bar_time_ms DESC
    """
    return csv_response("fx_merged_signals_outcomes.csv", sql)


@app.get("/download/all.zip")
def download_all_zip():
    files = {
        "fx_signals.csv": "SELECT * FROM signals ORDER BY bar_time_ms DESC",
        "fx_outcomes.csv": "SELECT * FROM outcomes ORDER BY bar_time_ms DESC, horizon_min",
        "fx_candles.csv": "SELECT * FROM candles ORDER BY bar_time_ms DESC",
        "fx_merged_signals_outcomes.csv": """
            SELECT
                s.*,
                o60.raw_return_pct AS raw_60m_pct,
                o60.raw_return_r AS raw_60m_r,
                o60.outcome_with_sl_pct AS sl_60m_pct,
                o60.outcome_with_sl_r AS sl_60m_r,
                o60.mfe_pct AS mfe_60m_pct,
                o60.mae_pct AS mae_60m_pct,
                o60.sl_hit AS sl_hit_60m,
                o120.raw_return_pct AS raw_120m_pct,
                o120.raw_return_r AS raw_120m_r,
                o120.outcome_with_sl_pct AS sl_120m_pct,
                o120.outcome_with_sl_r AS sl_120m_r,
                o120.mfe_pct AS mfe_120m_pct,
                o120.mae_pct AS mae_120m_pct,
                o120.sl_hit AS sl_hit_120m,
                o240.raw_return_pct AS raw_240m_pct,
                o240.raw_return_r AS raw_240m_r,
                o240.mfe_pct AS mfe_240m_pct,
                o240.mae_pct AS mae_240m_pct,
                o240.sl_hit AS sl_seen_by_240m
            FROM signals s
            LEFT JOIN outcomes o60 ON s.signal_id = o60.signal_id AND o60.horizon_min = 60
            LEFT JOIN outcomes o120 ON s.signal_id = o120.signal_id AND o120.horizon_min = 120
            LEFT JOIN outcomes o240 ON s.signal_id = o240.signal_id AND o240.horizon_min = 240
            ORDER BY s.bar_time_ms DESC
        """,
    }

    mem = io.BytesIO()
    with zipfile.ZipFile(mem, "w", zipfile.ZIP_DEFLATED) as z:
        with connect() as conn:
            for filename, sql in files.items():
                rows = conn.execute(sql).fetchall()
                headers = list(rows[0].keys()) if rows else []
                z.writestr(filename, rows_to_csv(rows, headers))
    mem.seek(0)
    return Response(
        mem.getvalue(),
        mimetype="application/zip",
        headers={"Content-Disposition": "attachment; filename=fx_session_logger_export.zip"}
    )


@app.get("/")
def dashboard():
    with connect() as conn:
        counts = conn.execute("""
            SELECT
                (SELECT COUNT(*) FROM signals) AS signals,
                (SELECT COUNT(*) FROM outcomes) AS outcomes,
                (SELECT COUNT(*) FROM candles) AS candles,
                (SELECT COUNT(DISTINCT pair) FROM signals) AS pairs
        """).fetchone()

        by_family = conn.execute("""
            SELECT model_family, direction, COUNT(*) AS n
            FROM signals
            GROUP BY model_family, direction
            ORDER BY model_family, direction
        """).fetchall()

        latest = conn.execute("""
            SELECT signal_time_utc, pair, model_family, direction, entry, london_high, london_low, london_range_pips
            FROM signals
            ORDER BY bar_time_ms DESC
            LIMIT 20
        """).fetchall()

        perf = conn.execute("""
            SELECT
                s.pair,
                s.model_family,
                s.direction,
                COUNT(*) AS completed_120m,
                ROUND(AVG(o.raw_return_r), 3) AS avg_raw_120m_r,
                ROUND(AVG(o.outcome_with_sl_r), 3) AS avg_sl_120m_r,
                ROUND(AVG(CASE WHEN o.outcome_with_sl_r > 0 THEN 1.0 ELSE 0.0 END) * 100.0, 1) AS win_rate_120m_pct,
                ROUND(AVG(o.mfe_pct), 3) AS avg_mfe_pct,
                ROUND(AVG(o.mae_pct), 3) AS avg_mae_pct
            FROM signals s
            JOIN outcomes o ON s.signal_id = o.signal_id AND o.horizon_min = 120
            GROUP BY s.pair, s.model_family, s.direction
            ORDER BY s.pair, s.model_family, s.direction
        """).fetchall()

    def table(rows):
        if not rows:
            return "<p>No rows yet.</p>"
        headers = rows[0].keys()
        head = "".join(f"<th>{h}</th>" for h in headers)
        body = ""
        for r in rows:
            body += "<tr>" + "".join(f"<td>{r[h] if r[h] is not None else ''}</td>" for h in headers) + "</tr>"
        return f"<table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>"

    html = f"""
    <!doctype html>
    <html>
    <head>
        <title>FX Session Logger v1</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 24px; background: #101217; color: #e8e8e8; }}
            a {{ color: #8ab4ff; }}
            .cards {{ display: flex; gap: 12px; flex-wrap: wrap; }}
            .card {{ background: #1b1f2a; border: 1px solid #33394a; border-radius: 10px; padding: 14px; min-width: 150px; }}
            .big {{ font-size: 28px; font-weight: 700; }}
            .muted {{ color: #a9b0c3; }}
            table {{ border-collapse: collapse; width: 100%; margin: 12px 0 28px; background: #151923; }}
            th, td {{ border: 1px solid #303647; padding: 8px; font-size: 13px; }}
            th {{ background: #202638; text-align: left; }}
            h1, h2 {{ margin-top: 26px; }}
            .downloads a {{ margin-right: 18px; }}
        </style>
    </head>
    <body>
        <h1>FX Session Structure Logger v1</h1>
        <p class="muted">Shadow-only logger for London range continuation and London sweep-and-reclaim FX research.</p>

        <div class="cards">
            <div class="card"><div class="muted">Signals</div><div class="big">{counts['signals']}</div></div>
            <div class="card"><div class="muted">Outcomes</div><div class="big">{counts['outcomes']}</div></div>
            <div class="card"><div class="muted">Candles</div><div class="big">{counts['candles']}</div></div>
            <div class="card"><div class="muted">Pairs seen</div><div class="big">{counts['pairs']}</div></div>
        </div>

        <h2>Downloads</h2>
        <p class="downloads">
            <a href="/download/merged.csv">Merged CSV</a>
            <a href="/download/signals.csv">Signals CSV</a>
            <a href="/download/outcomes.csv">Outcomes CSV</a>
            <a href="/download/candles.csv">Candles CSV</a>
            <a href="/download/all.zip">All ZIP</a>
        </p>

        <h2>Signals by family/direction</h2>
        {table(by_family)}

        <h2>Early 120m performance snapshot</h2>
        <p class="muted">Do not trust this until you have a proper 8–12 week sample.</p>
        {table(perf)}

        <h2>Latest signals</h2>
        {table(latest)}
    </body>
    </html>
    """
    return Response(html, mimetype="text/html")


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
