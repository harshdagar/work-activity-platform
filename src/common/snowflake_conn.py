# src/common/snowflake_conn.py
import os
from contextlib import contextmanager
from dotenv import load_dotenv
import snowflake.connector

load_dotenv()

@contextmanager
def snowflake_connection():
    """
    Context manager that opens a Snowflake connection using .env variables,
    then closes it safely.
    """
    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )
    try:
        yield conn
    finally:
        conn.close()

def exec_sql(conn, sql: str, params=None):
    """
    Runs a SQL statement (optionally parameterized).
    """
    cur = conn.cursor()
    try:
        cur.execute(sql, params) if params else cur.execute(sql)
        return cur.fetchall() if cur.description else None
    finally:
        cur.close()