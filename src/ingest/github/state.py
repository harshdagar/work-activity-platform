# src/ingest/github/state.py
from datetime import datetime, timezone
from src.common.snowflake_conn import exec_sql

STATE_TABLE = "RAW.INGEST_STATE"

def ensure_state_table(conn):
    exec_sql(conn, f"""
    CREATE TABLE IF NOT EXISTS {STATE_TABLE} (
      pipeline STRING,
      last_success_ts TIMESTAMP_NTZ,
      updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
    """)

def get_last_success(conn, pipeline: str):
    ensure_state_table(conn)
    rows = exec_sql(conn, f"""
      SELECT last_success_ts
      FROM {STATE_TABLE}
      WHERE pipeline = %s
      ORDER BY updated_at DESC
      LIMIT 1
    """, (pipeline,))
    return rows[0][0] if rows else None

def set_last_success(conn, pipeline: str, ts: datetime):
    ensure_state_table(conn)
    exec_sql(conn, f"""
      INSERT INTO {STATE_TABLE} (pipeline, last_success_ts)
      VALUES (%s, %s)
    """, (pipeline, ts.replace(tzinfo=None)))