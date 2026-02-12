# src/ingest/github/load_reviews.py
from datetime import datetime, timezone
import json
from src.ingest.github.client import GitHubClient
from src.ingest.github.raw_tables import ensure_raw_tables
from src.common.snowflake_conn import snowflake_connection, exec_sql

OWNER = "dagster-io"
REPO = "dagster"
REPO_FULL = f"{OWNER}/{REPO}"

def get_recent_pr_numbers(conn, repo_full: str, limit: int = 200):
    rows = exec_sql(conn, """
      SELECT DISTINCT pr_number
      FROM RAW.GITHUB_PULL_REQUESTS
      WHERE repo = %s
      ORDER BY pr_number DESC
      LIMIT %s
    """, (repo_full, limit))
    return [r[0] for r in rows] if rows else []

def upsert_review_rows(conn, repo_full: str, pr_number: int, reviews: list[dict]) -> int:
    """
    Upserts reviews using a temp table + MERGE so reruns don't duplicate.
    Uses per-row inserts into temp table to avoid Snowflake connector rewrite issues.
    """
    if not reviews:
        return 0

    # Temp table for this run
    exec_sql(conn, """
    CREATE TEMP TABLE IF NOT EXISTS TMP_GITHUB_REVIEWS (
      repo STRING,
      pr_number INTEGER,
      review_id NUMBER,
      submitted_at TIMESTAMP_NTZ,
      payload VARIANT
    )
    """)

    # Insert each review into temp table (stable, avoids executemany rewrite issues)
    sql_insert = """
    INSERT INTO TMP_GITHUB_REVIEWS (repo, pr_number, review_id, submitted_at, payload)
    SELECT %s, %s, %s, %s, PARSE_JSON(%s)
    """

    cur = conn.cursor()
    try:
        for rv in reviews:
            submitted = rv.get("submitted_at")
            submitted_ntz = submitted.replace("Z", "") if submitted else None

            cur.execute(sql_insert, (
                repo_full,
                pr_number,
                rv["id"],
                submitted_ntz,
                json.dumps(rv),
            ))
    finally:
        cur.close()

    # Merge into RAW table (idempotent)
    exec_sql(conn, """
    MERGE INTO RAW.GITHUB_PULL_REQUEST_REVIEWS t
    USING TMP_GITHUB_REVIEWS s
      ON t.repo = s.repo AND t.review_id = s.review_id
    WHEN MATCHED THEN UPDATE SET
      t.pr_number = s.pr_number,
      t.submitted_at = s.submitted_at,
      t.payload = s.payload,
      t.ingested_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (repo, pr_number, review_id, submitted_at, payload)
      VALUES (s.repo, s.pr_number, s.review_id, s.submitted_at, s.payload)
    """)

    return len(reviews)

def run() -> int:
    gh = GitHubClient()

    with snowflake_connection() as conn:
        ensure_raw_tables(conn)

        pr_numbers = get_recent_pr_numbers(conn, REPO_FULL, limit=200)
        if not pr_numbers:
            raise RuntimeError("No PRs found in RAW.GITHUB_PULL_REQUESTS. Run load_pull_requests first.")

        inserted_total = 0

        for pr_number in pr_numbers:
            reviews = list(
                gh.get_all_pages(
                    f"/repos/{OWNER}/{REPO}/pulls/{pr_number}/reviews",
                    params={"per_page": 100},
                )
            )
            inserted = upsert_review_rows(conn, REPO_FULL, pr_number, reviews)
            inserted_total += inserted

            if inserted > 0:
                print(f"[insert] PR #{pr_number}: +{inserted} reviews")

        print(f"Done. Inserted {inserted_total} review rows.")
        return inserted_total


def main():
    run()


if __name__ == "__main__":
    main()