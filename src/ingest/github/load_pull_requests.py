# src/ingest/github/load_pull_requests.py
from datetime import datetime, timezone
from src.ingest.github.client import GitHubClient
from src.ingest.github.raw_tables import ensure_raw_tables
from src.ingest.github.state import get_last_success, set_last_success
from src.common.snowflake_conn import snowflake_connection, exec_sql
from datetime import datetime, timezone, timedelta

OWNER = "dagster-io"
REPO = "dagster"
REPO_FULL = f"{OWNER}/{REPO}"
PIPELINE_NAME = f"github_prs::{REPO_FULL}"

import json

import json

def insert_pr_rows(conn, repo_full: str, prs: list[dict]) -> int:
    """
    Stable insert method: 1 row per execute.
    This avoids Snowflake connector 'multi-row insert rewrite' failures with VARIANT JSON.
    """
    if not prs:
        return 0

    sql = """
    INSERT INTO RAW.GITHUB_PULL_REQUESTS (repo, pr_number, pr_id, updated_at, payload)
    SELECT %s, %s, %s, %s, PARSE_JSON(%s)
    """

    cur = conn.cursor()
    try:
        count = 0
        for pr in prs:
            cur.execute(
                sql,
                (
                    repo_full,
                    pr["number"],
                    pr["id"],
                    pr["updated_at"].replace("Z", ""),
                    json.dumps(pr),
                ),
            )
            count += 1
        return count
    finally:
        cur.close()

def run() -> int:
    """
    Entry point used by Dagster. Returns number of inserted PR rows.
    """
    gh = GitHubClient()

    with snowflake_connection() as conn:
        ensure_raw_tables(conn)

        last_ts = get_last_success(conn, PIPELINE_NAME)
        print(f"[state] last success ts: {last_ts}")

        params = {
            "state": "all",
            "sort": "updated",
            "direction": "desc",
            "per_page": 100,
        }

        batch = []
        inserted_total = 0
        now = datetime.now(timezone.utc)

        # (keep your FIRST_RUN_LOOKBACK_DAYS cutoff logic here as you already added)

        for pr in gh.get_all_pages(f"/repos/{OWNER}/{REPO}/pulls", params=params):
            pr_updated = datetime.fromisoformat(pr["updated_at"].replace("Z", "+00:00"))

            if last_ts and pr_updated <= last_ts.replace(tzinfo=timezone.utc):
                break

            # first-run cutoff (you already added)
            if not last_ts and pr_updated < first_run_cutoff:
                break

            batch.append(pr)

            if len(batch) >= 100:
                inserted = insert_pr_rows(conn, REPO_FULL, batch)
                inserted_total += inserted
                print(f"[insert] +{inserted} PRs (total={inserted_total})")
                batch = []

        if batch:
            inserted = insert_pr_rows(conn, REPO_FULL, batch)
            inserted_total += inserted
            print(f"[insert] +{inserted} PRs (total={inserted_total})")

        set_last_success(conn, PIPELINE_NAME, now)
        print(f"✅ Done. Inserted {inserted_total} PR rows.")
        return inserted_total


def main():
    run()


if __name__ == "__main__":
    main()