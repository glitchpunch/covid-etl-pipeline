"""Stage 4: idempotent load into PostgreSQL (staging table + upsert)."""
import io
import json
import logging

import pandas as pd
import psycopg2

from .config import DAILY_DIR, DB, MONTHLY_DIR, SCHEMA_SQL

log = logging.getLogger(__name__)

DAILY_COLS = ["date", "country", "confirmed", "recovered", "deaths", "new_confirmed",
              "new_deaths", "new_confirmed_7d_avg", "case_fatality_pct", "is_correction"]
MONTHLY_COLS = ["country", "year", "month", "new_confirmed", "new_deaths", "confirmed_eom"]


def _upsert(cur, table: str, df: pd.DataFrame, cols: list[str], keys: list[str]) -> None:
    staging = f"_stg_{table}"
    cur.execute(f"CREATE TEMP TABLE {staging} (LIKE {table} INCLUDING DEFAULTS) ON COMMIT DROP")
    buf = io.StringIO()
    df[cols].to_csv(buf, index=False, header=False)
    buf.seek(0)
    cur.copy_expert(f"COPY {staging} ({', '.join(cols)}) FROM STDIN WITH CSV", buf)
    updates = ", ".join(f"{c}=EXCLUDED.{c}" for c in cols if c not in keys)
    cur.execute(
        f"INSERT INTO {table} ({', '.join(cols)}) SELECT {', '.join(cols)} FROM {staging} "
        f"ON CONFLICT ({', '.join(keys)}) DO UPDATE SET {updates}"
    )


def load_all() -> dict:
    daily = pd.read_parquet(DAILY_DIR)
    monthly = pd.read_parquet(MONTHLY_DIR)
    for c in ("new_confirmed_7d_avg", "case_fatality_pct"):
        daily[c] = daily[c].astype("float64")

    conn = psycopg2.connect(**DB)
    try:
        with conn, conn.cursor() as cur:   # one transaction: all-or-nothing
            cur.execute(SCHEMA_SQL.read_text())
            _upsert(cur, "daily_stats", daily, DAILY_COLS, ["date", "country"])
            _upsert(cur, "monthly_summary", monthly, MONTHLY_COLS, ["country", "year", "month"])
            cur.execute("SELECT COUNT(*) FROM daily_stats")
            n_daily = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM monthly_summary")
            n_monthly = cur.fetchone()[0]
    finally:
        conn.close()
    log.info("Loaded daily_stats=%s monthly_summary=%s", n_daily, n_monthly)
    return {"daily_stats_rows": n_daily, "monthly_summary_rows": n_monthly}


def record_run(started_at, metrics: dict) -> None:
    conn = psycopg2.connect(**DB)
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO pipeline_runs (started_at, rows_in, rows_out, quality_warnings,"
                " seconds_total, metrics_json) VALUES (%s,%s,%s,%s,%s,%s)",
                (started_at, metrics["rows_in"], metrics["rows_out"],
                 sum(metrics["quality"]["warnings"].values()),
                 metrics["seconds_total"], json.dumps(metrics, default=str)),
            )
    finally:
        conn.close()
