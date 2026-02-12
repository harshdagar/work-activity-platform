# src/ingest/github/raw_tables.py
# src/ingest/github/raw_tables.py
from src.common.snowflake_conn import exec_sql

def ensure_raw_tables(conn):
    # Store the full API payload as VARIANT (semi-structured JSON)
    # plus a few columns for easy filtering / debugging.
    exec_sql(conn, """
    CREATE TABLE IF NOT EXISTS RAW.GITHUB_PULL_REQUESTS (
      repo STRING,
      pr_number INTEGER,
      pr_id NUMBER,
      updated_at TIMESTAMP_NTZ,
      ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
      payload VARIANT
    )
    """)

    exec_sql(conn, """
    CREATE TABLE IF NOT EXISTS RAW.GITHUB_PULL_REQUEST_REVIEWS (
      repo STRING,
      pr_number INTEGER,
      review_id NUMBER,
      submitted_at TIMESTAMP_NTZ,
      ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
      payload VARIANT
    )
    """)