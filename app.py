import csv
import io
import json
import os
import sqlite3
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from flask import Flask, Response, jsonify, request

APP_NAME = "FX Asia Continuation Logger v2"
MODEL_VERSION = "fx_asia_continuation_v2"

DB_PATH = os.getenv("DB_PATH", "/app/data/fx_session_logger.sqlite")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip()

app = Flask(__name__)


# -------------------------
# DB helpers
# -------------------------

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def db_dir_exists() -> None:
    db_file = Path(DB_PATH)
    db_file.parent.mkdir(parents=True, exist_ok=True)


def connect():
    db_dir_exists()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def table_exists(conn, table_name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    return row is not None


def init_db():
    with connect() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS raw_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                received_at_utc TEXT NOT NULL,
                event_type TEXT,
                model_version TEXT,
                signal_id TEXT,
                pair TEXT,
                payload_json TEXT NOT NULL
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS v2_signals (
                signal_id TEXT PRIMARY KEY,
                received_at_utc TEXT NOT NULL,
                pair TEXT,
                tickerid TEXT,
                timeframe TEXT,
                session_timezone TEXT,
                bar_time_ms INTEGER,
                signal_time_utc TEXT,
                signal_time_uk TEXT,

                model_version TEXT,
                model_family TEXT,
                setup_name TEXT,
                direction TEXT,

                entry REAL,
                open REAL,
                high REAL,
                low REAL,
                close REAL,

                pip REAL,
                atr REAL,
                atr_pips REAL,

                asia_high REAL,
                asia_low REAL,
                asia_mid REAL,
                asia_range_pips REAL,

                london_high REAL,
                london_low REAL,
                london_mid REAL,
                london_range_pips REAL,

                prev_day_high REAL,
                prev_day_low REAL,

                minute_uk INTEGER,
                ny_time_bucket TEXT,
                asia_range_bucket TEXT,
                entry_vs_asia_mid_pips REAL,
                entry_mid_bucket TEXT,

                raw_json TEXT NOT NULL
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS v2_outcomes (
                signal_id TEXT NOT NULL,
                horizon_min INTEGER NOT NULL,
                stop_model TEXT NOT NULL,
                received_at_utc TEXT NOT NULL,

                pair TEXT,
                direction TEXT,
                entry REAL,
                stop REAL,
                risk_pips REAL,
                sl_pct REAL,

                raw_pct REAL,
                raw_R REAL,
                fixed_pct REAL,
                fixed_R REAL,
                mfe_pct REAL,
                mae_pct REAL,
                mfe_R REAL,
                mae_R REAL,
                sl_hit INTEGER,

                raw_json TEXT NOT NULL,

                PRIMARY KEY (signal_id, horizon_min, stop_model)
            )
        """)

        conn.commit()


init_db()


# -------------------------
# parsing helpers
# -------------------------

def fval(data, key, default=None):
    try:
        value = data.get(key, default)
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def ival(data, key, default=None):
    try:
        value = data.get(key, default)
        if value is None or value == "":
            return default
        return int(float(value))
    except Exception:
        return default


def sval(data, key, default=""):
    value = data.get(key, default)
    if value is None:
        return default
    return str(value)


def split_pipe(value):
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v) for v in value]
    return str(value).split("|")


def get_by_index(items, i, default=None, cast=None):
    try:
        if i >= len(items):
            return default
        v = items[i]
        if v is None or v == "":
            return default
        if cast is None:
            return v
        return cast(v)
    except Exception:
        return default


def parse_ms_time_to_utc(ms):
    try:
        if ms is None:
            return ""
        return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc).isoformat()
    except Exception:
        return ""


# -------------------------
# inserts
# -------------------------

def insert_raw_event(conn, data, received_at):
    conn.execute(
        """
        INSERT INTO raw_events (
            received_at_utc, event_type, model_version, signal_id, pair, payload_json
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            received_at,
            sval(data, "event_type", ""),
            sval(data, "model_version", ""),
            sval(data, "signal_id", ""),
            sval(data, "pair", ""),
            json.dumps(data, sort_keys=True),
        ),
    )


def insert_v2_signal(conn, data, received_at):
    bar_time_ms = ival(data, "bar_time_ms")
    signal_time_utc = sval(data, "signal_time_utc", "") or parse_ms_time_to_utc(bar_time_ms)

    conn.execute(
        """
        INSERT OR REPLACE INTO v2_signals (
            signal_id, received_at_utc, pair, tickerid, timeframe, session_timezone,
            bar_time_ms, signal_time_utc, signal_time_uk,
            model_version, model_family, setup_name, direction,
            entry, open, high, low, close,
            pip, atr, atr_pips,
            asia_high, asia_low, asia_mid, asia_range_pips,
            london_high, london_low, london_mid, london_range_pips,
            prev_day_high, prev_day_low,
            minute_uk, ny_time_bucket, asia_range_bucket,
            entry_vs_asia_mid_pips, entry_mid_bucket,
            raw_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            sval(data, "signal_id"),
            received_at,
            sval(data, "pair"),
            sval(data, "tickerid"),
            sval(data, "timeframe"),
            sval(data, "session_timezone"),
            bar_time_ms,
            signal_time_utc,
            sval(data, "signal_time_uk"),
            sval(data, "model_version", MODEL_VERSION),
            sval(data, "model_family"),
            sval(data, "setup_name"),
            sval(data, "direction"),
            fval(data, "entry"),
            fval(data, "open"),
            fval(data, "high"),
            fval(data, "low"),
            fval(data, "close"),
            fval(data, "pip"),
            fval(data, "atr"),
            fval(data, "atr_pips"),
            fval(data, "asia_high"),
            fval(data, "asia_low"),
            fval(data, "asia_mid"),
            fval(data, "asia_range_pips"),
            fval(data, "london_high"),
            fval(data, "london_low"),
            fval(data, "london_mid"),
            fval(data, "london_range_pips"),
            fval(data, "prev_day_high"),
            fval(data, "prev_day_low"),
            ival(data, "minute_uk"),
            sval(data, "ny_time_bucket"),
            sval(data, "asia_range_bucket"),
            fval(data, "entry_vs_asia_mid_pips"),
            sval(data, "entry_mid_bucket"),
            json.dumps(data, sort_keys=True),
        ),
    )


def insert_v2_outcome(conn, data, received_at):
    signal_id = sval(data, "signal_id")
    horizon_min = ival(data, "horizon_min")
    pair = sval(data, "pair")
    direction = sval(data, "direction")
    entry = fval(data, "entry")

    # v2 Pine sends pipe-delimited stop model result lists in one alert per horizon.
    names = split_pipe(data.get("stop_model_names") or data.get("stop_models"))

    if not names:
        # Fallback: allow one-row outcome format.
        names = [sval(data, "stop_model", "unknown")]

    stop_values = split_pipe(data.get("stop_values"))
    risk_pips = split_pipe(data.get("risk_pips_list"))
    sl_pct = split_pipe(data.get("sl_pct_list"))
    raw_R = split_pipe(data.get("raw_R_list"))
    fixed_R = split_pipe(data.get("fixed_R_list"))
    fixed_pct = split_pipe(data.get("fixed_pct_list"))
    mfe_R = split_pipe(data.get("mfe_R_list"))
    mae_R = split_pipe(data.get("mae_R_list"))
    sl_hit = split_pipe(data.get("sl_hit_list"))

    raw_pct_common = fval(data, "raw_pct")
    mfe_pct_common = fval(data, "mfe_pct")
    mae_pct_common = fval(data, "mae_pct")

    inserted = 0

    for i, name in enumerate(names):
        name = str(name).strip()
        if not name:
            continue

        row_payload = dict(data)
        row_payload["expanded_stop_model"] = name
        row_payload["expanded_index"] = i

        conn.execute(
            """
            INSERT OR REPLACE INTO v2_outcomes (
                signal_id, horizon_min, stop_model, received_at_utc,
                pair, direction, entry, stop, risk_pips, sl_pct,
                raw_pct, raw_R, fixed_pct, fixed_R,
                mfe_pct, mae_pct, mfe_R, mae_R, sl_hit,
                raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                signal_id,
                horizon_min,
                name,
                received_at,
                pair,
                direction,
                entry,
                get_by_index(stop_values, i, fval(data, "stop"), float),
                get_by_index(risk_pips, i, fval(data, "risk_pips"), float),
                get_by_index(sl_pct, i, fval(data, "sl_pct"), float),
                raw_pct_common if raw_pct_common is not None else fval(data, "raw_pct"),
                get_by_index(raw_R, i, fval(data, "raw_R"), float),
                get_by_index(fixed_pct, i, fval(data, "fixed_pct"), float),
                get_by_index(fixed_R, i, fval(data, "fixed_R"), float),
                mfe_pct_common if mfe_pct_common is not None else fval(data, "mfe_pct"),
                mae_pct_common if mae_pct_common is not None else fval(data, "mae_pct"),
                get_by_index(mfe_R, i, fval(data, "mfe_R"), float),
                get_by_index(mae_R, i, fval(data, "mae_R"), float),
                get_by_index(sl_hit, i, ival(data, "sl_hit"), int),
                json.dumps(row_payload, sort_keys=True),
            ),
        )
        inserted += 1

    return inserted


# -------------------------
# dataframes
# -------------------------

def read_sql_df(query, params=()):
    with connect() as conn:
        return pd.read_sql_query(query, conn, params=params)


def get_v2_signals_df():
    return read_sql_df("SELECT * FROM v2_signals ORDER BY signal_time_utc")


def get_v2_outcomes_df():
    return read_sql_df("SELECT * FROM v2_outcomes ORDER BY signal_id, horizon_min, stop_model")


def get_v2_merged_df():
    return read_sql_df(
        """
        SELECT
            s.signal_id,
            s.received_at_utc AS signal_received_at_utc,
            s.pair,
            s.tickerid,
            s.timeframe,
            s.session_timezone,
            s.signal_time_utc,
            s.signal_time_uk,
            s.model_version,
            s.model_family,
            s.setup_name,
            s.direction,
            s.entry,
            s.open,
            s.high,
            s.low,
            s.close,
            s.pip,
            s.atr,
            s.atr_pips,
            s.asia_high,
            s.asia_low,
            s.asia_mid,
            s.asia_range_pips,
            s.london_high,
            s.london_low,
            s.london_mid,
            s.london_range_pips,
            s.prev_day_high,
            s.prev_day_low,
            s.minute_uk,
            s.ny_time_bucket,
            s.asia_range_bucket,
            s.entry_vs_asia_mid_pips,
            s.entry_mid_bucket,
            o.horizon_min,
            o.stop_model,
            o.received_at_utc AS outcome_received_at_utc,
            o.stop,
            o.risk_pips,
            o.sl_pct,
            o.raw_pct,
            o.raw_R,
            o.fixed_pct,
            o.fixed_R,
            o.mfe_pct,
            o.mae_pct,
            o.mfe_R,
            o.mae_R,
            o.sl_hit
        FROM v2_signals s
        LEFT JOIN v2_outcomes o ON s.signal_id = o.signal_id
        ORDER BY s.signal_time_utc, o.horizon_min, o.stop_model
        """
    )


def win_rate(series):
    s = pd.to_numeric(series, errors="coerce").dropna()
    if len(s) == 0:
        return None
    return float((s > 0).mean() * 100.0)


def summary_table(df, group_cols):
    if isinstance(group_cols, str):
        group_cols = [group_cols]

    if df.empty:
        return pd.DataFrame()

    rows = []

    # Drop rows without outcomes for summary calculations, but keep base signal counts from the whole group.
    for keys, g in df.groupby(group_cols, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)

        row = {col: val for col, val in zip(group_cols, keys)}
        row["base_signals"] = int(g["signal_id"].nunique())

        for horizon in [60, 120, 240]:
            gh = g[g["horizon_min"] == horizon].copy()
            row[f"done_{horizon}m"] = int(gh["fixed_R"].notna().sum())
            row[f"avg_fixed_{horizon}m_R"] = float(gh["fixed_R"].mean()) if len(gh) else None
            row[f"avg_raw_{horizon}m_R"] = float(gh["raw_R"].mean()) if len(gh) else None
            row[f"win_fixed_{horizon}m_pct"] = win_rate(gh["fixed_R"])
            row[f"sl_hits_{horizon}m"] = int(pd.to_numeric(gh["sl_hit"], errors="coerce").fillna(0).sum()) if len(gh) else 0
            row[f"sl_hit_rate_{horizon}m_pct"] = (
                row[f"sl_hits_{horizon}m"] / row[f"done_{horizon}m"] * 100.0
                if row[f"done_{horizon}m"] else None
            )
            row[f"avg_MFE_{horizon}m_R"] = float(gh["mfe_R"].mean()) if len(gh) else None
            row[f"avg_MAE_{horizon}m_R"] = float(gh["mae_R"].mean()) if len(gh) else None

        if "risk_pips" in g.columns:
            row["avg_risk_pips"] = float(pd.to_numeric(g["risk_pips"], errors="coerce").mean())

        rows.append(row)

    out = pd.DataFrame(rows)

    sort_cols = []
    ascending = []
    if "avg_fixed_120m_R" in out.columns:
        sort_cols.append("avg_fixed_120m_R")
        ascending.append(False)
    if "base_signals" in out.columns:
        sort_cols.append("base_signals")
        ascending.append(False)
    if sort_cols:
        out = out.sort_values(sort_cols, ascending=ascending)

    return out


def get_summaries():
    df = get_v2_merged_df()
    if df.empty:
        return {}

    summaries = {
        "by_pair": summary_table(df, "pair"),
        "by_direction": summary_table(df, "direction"),
        "by_stop_model": summary_table(df, "stop_model"),
        "by_pair_direction_stop": summary_table(df, ["pair", "direction", "stop_model"]),
        "by_pair_stop": summary_table(df, ["pair", "stop_model"]),
        "by_ny_time_bucket_stop": summary_table(df, ["ny_time_bucket", "stop_model"]),
        "by_asia_range_bucket_stop": summary_table(df, ["asia_range_bucket", "stop_model"]),
        "by_entry_mid_bucket_stop": summary_table(df, ["entry_mid_bucket", "stop_model"]),
    }

    # Survivor-style table, deliberately stricter than the normal summaries.
    pds = summaries["by_pair_direction_stop"].copy()
    if not pds.empty:
        survivors = pds[
            (pds["base_signals"] >= 10)
            & (pds["avg_fixed_120m_R"] > 0)
            & (pds["avg_fixed_240m_R"] > 0)
            & (pds["win_fixed_120m_pct"] >= 45)
        ].copy()
        survivors = survivors.sort_values(
            ["avg_fixed_120m_R", "avg_fixed_240m_R", "base_signals"],
            ascending=[False, False, False],
        )
        summaries["survivor_watchlist"] = survivors

    return summaries


# -------------------------
# HTML helpers
# -------------------------

def fmt_num(x, digits=3):
    if x is None:
        return ""
    try:
        if pd.isna(x):
            return ""
        return f"{float(x):.{digits}f}"
    except Exception:
        return str(x)


def df_to_html(df, max_rows=30):
    if df is None or df.empty:
        return "<p class='muted'>No rows yet.</p>"

    show = df.head(max_rows).copy()

    for col in show.columns:
        if pd.api.types.is_float_dtype(show[col]):
            if col.endswith("_pct"):
                show[col] = show[col].map(lambda x: fmt_num(x, 1))
            elif col.endswith("_R"):
                show[col] = show[col].map(lambda x: fmt_num(x, 3))
            elif col.endswith("pips"):
                show[col] = show[col].map(lambda x: fmt_num(x, 2))
            else:
                show[col] = show[col].map(lambda x: fmt_num(x, 4))

    return show.to_html(index=False, classes="data", border=0)


def legacy_counts():
    with connect() as conn:
        counts = {}
        for table in ["signals", "outcomes", "candles"]:
            if table_exists(conn, table):
                try:
                    counts[table] = conn.execute(f"SELECT COUNT(*) AS n FROM {table}").fetchone()["n"]
                except Exception:
                    counts[table] = None
        return counts


# -------------------------
# routes
# -------------------------

@app.route("/health")
def health():
    with connect() as conn:
        v2_signals = conn.execute("SELECT COUNT(*) AS n FROM v2_signals").fetchone()["n"]
        v2_outcomes = conn.execute("SELECT COUNT(*) AS n FROM v2_outcomes").fetchone()["n"]
        raw_events = conn.execute("SELECT COUNT(*) AS n FROM raw_events").fetchone()["n"]

    return jsonify({
        "ok": True,
        "app": APP_NAME,
        "model_version": MODEL_VERSION,
        "db_path": DB_PATH,
        "v2_signals": v2_signals,
        "v2_outcomes": v2_outcomes,
        "raw_events": raw_events,
        "legacy_counts": legacy_counts(),
        "utc": utc_now_iso(),
    })


@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        data = request.get_json(force=True, silent=False)
        if not isinstance(data, dict):
            return jsonify({"ok": False, "error": "JSON body must be an object"}), 400

        if WEBHOOK_SECRET:
            supplied = str(data.get("secret") or request.args.get("secret", "")).strip()
            if supplied != WEBHOOK_SECRET:
                return jsonify({"ok": False, "error": "Bad webhook secret"}), 403

        received_at = utc_now_iso()
        event_type = sval(data, "event_type")
        model_version = sval(data, "model_version")

        with connect() as conn:
            insert_raw_event(conn, data, received_at)

            inserted = 0
            if event_type == "signal" and model_version == MODEL_VERSION:
                insert_v2_signal(conn, data, received_at)
                inserted = 1

            elif event_type == "outcome" and model_version == MODEL_VERSION:
                inserted = insert_v2_outcome(conn, data, received_at)

            conn.commit()

        return jsonify({
            "ok": True,
            "event_type": event_type,
            "model_version": model_version,
            "signal_id": sval(data, "signal_id"),
            "inserted_rows": inserted,
        })

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/")
def dashboard():
    signals = get_v2_signals_df()
    outcomes = get_v2_outcomes_df()
    merged = get_v2_merged_df()
    summaries = get_summaries()

    signal_count = int(len(signals))
    outcome_count = int(len(outcomes))
    base_with_any_outcome = int(outcomes["signal_id"].nunique()) if not outcomes.empty else 0
    pairs_seen = int(signals["pair"].nunique()) if not signals.empty else 0

    done_60 = int(outcomes[outcomes["horizon_min"] == 60]["signal_id"].nunique()) if not outcomes.empty else 0
    done_120 = int(outcomes[outcomes["horizon_min"] == 120]["signal_id"].nunique()) if not outcomes.empty else 0
    done_240 = int(outcomes[outcomes["horizon_min"] == 240]["signal_id"].nunique()) if not outcomes.empty else 0

    legacy = legacy_counts()

    latest = signals.sort_values("signal_time_utc", ascending=False).head(20) if not signals.empty else pd.DataFrame()

    html = f"""
    <!doctype html>
    <html>
    <head>
      <title>{APP_NAME}</title>
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <style>
        body {{
          margin: 0;
          padding: 24px;
          background: #0f172a;
          color: #e5e7eb;
          font-family: Arial, sans-serif;
        }}
        h1 {{ margin-top: 0; }}
        h2 {{ margin-top: 34px; border-bottom: 1px solid #334155; padding-bottom: 8px; }}
        a {{ color: #93c5fd; }}
        .muted {{ color: #94a3b8; }}
        .grid {{
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
          gap: 12px;
          margin: 18px 0;
        }}
        .tile {{
          background: #111827;
          border: 1px solid #334155;
          border-radius: 10px;
          padding: 14px;
        }}
        .tile .label {{ color: #94a3b8; font-size: 13px; }}
        .tile .value {{ font-size: 26px; font-weight: bold; margin-top: 6px; }}
        table.data {{
          border-collapse: collapse;
          width: 100%;
          font-size: 13px;
          margin-top: 10px;
        }}
        table.data th, table.data td {{
          border: 1px solid #334155;
          padding: 6px 8px;
          text-align: right;
          white-space: nowrap;
        }}
        table.data th {{
          background: #1e293b;
          color: #bfdbfe;
        }}
        table.data td:first-child, table.data th:first-child {{
          text-align: left;
        }}
        .section {{
          background: #111827;
          border: 1px solid #334155;
          border-radius: 10px;
          padding: 16px;
          margin-top: 14px;
          overflow-x: auto;
        }}
        code {{
          background: #020617;
          padding: 2px 5px;
          border-radius: 4px;
        }}
      </style>
    </head>
    <body>
      <h1>{APP_NAME}</h1>
      <p class="muted">Focus: Asia range continuation during New York. Pairs intended: AUDUSD, EURUSD, GBPUSD, USDCAD.</p>

      <div class="grid">
        <div class="tile"><div class="label">V2 Signals</div><div class="value">{signal_count}</div></div>
        <div class="tile"><div class="label">V2 Outcome Rows</div><div class="value">{outcome_count}</div></div>
        <div class="tile"><div class="label">Signals With Outcomes</div><div class="value">{base_with_any_outcome}</div></div>
        <div class="tile"><div class="label">Pairs Seen</div><div class="value">{pairs_seen}</div></div>
        <div class="tile"><div class="label">Signals Done 60m</div><div class="value">{done_60}</div></div>
        <div class="tile"><div class="label">Signals Done 120m</div><div class="value">{done_120}</div></div>
        <div class="tile"><div class="label">Signals Done 240m</div><div class="value">{done_240}</div></div>
      </div>

      <p>
        Downloads:
        <a href="/download/v2-signals.csv">v2 signals</a> |
        <a href="/download/v2-outcomes.csv">v2 outcomes</a> |
        <a href="/download/v2-merged.csv">v2 merged</a> |
        <a href="/download/v2-summary-all.csv">v2 summary all</a> |
        <a href="/download/all.zip">all.zip</a>
      </p>

      <p class="muted">Legacy v1 counts if old tables exist: {legacy}</p>

      <h2>Survivor Watchlist</h2>
      <div class="section">{df_to_html(summaries.get("survivor_watchlist", pd.DataFrame()), 50)}</div>

      <h2>By Pair + Direction + Stop</h2>
      <div class="section">{df_to_html(summaries.get("by_pair_direction_stop", pd.DataFrame()), 60)}</div>

      <h2>By Stop Model</h2>
      <div class="section">{df_to_html(summaries.get("by_stop_model", pd.DataFrame()), 30)}</div>

      <h2>By Pair</h2>
      <div class="section">{df_to_html(summaries.get("by_pair", pd.DataFrame()), 30)}</div>

      <h2>By Direction</h2>
      <div class="section">{df_to_html(summaries.get("by_direction", pd.DataFrame()), 30)}</div>

      <h2>By NY Time Bucket + Stop</h2>
      <div class="section">{df_to_html(summaries.get("by_ny_time_bucket_stop", pd.DataFrame()), 40)}</div>

      <h2>By Asia Range Bucket + Stop</h2>
      <div class="section">{df_to_html(summaries.get("by_asia_range_bucket_stop", pd.DataFrame()), 40)}</div>

      <h2>By Entry vs Asia Midpoint + Stop</h2>
      <div class="section">{df_to_html(summaries.get("by_entry_mid_bucket_stop", pd.DataFrame()), 40)}</div>

      <h2>Latest V2 Signals</h2>
      <div class="section">{df_to_html(latest, 20)}</div>
    </body>
    </html>
    """
    return Response(html, mimetype="text/html")


@app.route("/summary")
def summary_json():
    summaries = get_summaries()
    return jsonify({name: df.to_dict(orient="records") for name, df in summaries.items()})


def csv_response(df, filename):
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    return Response(
        buf.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@app.route("/download/v2-signals.csv")
def download_v2_signals():
    return csv_response(get_v2_signals_df(), "fx_v2_signals.csv")


@app.route("/download/v2-outcomes.csv")
def download_v2_outcomes():
    return csv_response(get_v2_outcomes_df(), "fx_v2_outcomes.csv")


@app.route("/download/v2-merged.csv")
def download_v2_merged():
    return csv_response(get_v2_merged_df(), "fx_v2_merged.csv")


@app.route("/download/v2-summary-all.csv")
def download_v2_summary_all():
    summaries = get_summaries()
    rows = []
    for name, df in summaries.items():
        if df is None or df.empty:
            continue
        t = df.copy()
        t.insert(0, "section", name)
        rows.append(t)
    out = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()
    return csv_response(out, "fx_v2_summary_all.csv")


@app.route("/download/all.zip")
def download_all_zip():
    mem = io.BytesIO()
    with zipfile.ZipFile(mem, "w", zipfile.ZIP_DEFLATED) as z:
        files = {
            "fx_v2_signals.csv": get_v2_signals_df(),
            "fx_v2_outcomes.csv": get_v2_outcomes_df(),
            "fx_v2_merged.csv": get_v2_merged_df(),
        }

        summaries = get_summaries()
        rows = []
        for name, df in summaries.items():
            if df is not None and not df.empty:
                t = df.copy()
                t.insert(0, "section", name)
                rows.append(t)
                s_buf = io.StringIO()
                df.to_csv(s_buf, index=False)
                z.writestr(f"fx_v2_summary_{name}.csv", s_buf.getvalue())

        files["fx_v2_summary_all.csv"] = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()

        for filename, df in files.items():
            buf = io.StringIO()
            df.to_csv(buf, index=False)
            z.writestr(filename, buf.getvalue())

        # Include raw events for debugging.
        raw = read_sql_df("SELECT * FROM raw_events ORDER BY id")
        buf = io.StringIO()
        raw.to_csv(buf, index=False)
        z.writestr("raw_events.csv", buf.getvalue())

        meta = {
            "app": APP_NAME,
            "model_version": MODEL_VERSION,
            "created_at_utc": utc_now_iso(),
            "db_path": DB_PATH,
            "legacy_counts": legacy_counts(),
        }
        z.writestr("metadata.json", json.dumps(meta, indent=2))

    mem.seek(0)
    return Response(
        mem.getvalue(),
        mimetype="application/zip",
        headers={"Content-Disposition": "attachment; filename=fx_asia_continuation_v2_export.zip"},
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
