from dagster import asset, AssetExecutionContext

# We reuse your existing scripts by importing and calling their main() logic.
# To make this clean, we'll import the modules and call a function we add next.
from src.ingest.github import load_pull_requests, load_reviews


@asset(
    description="Loads GitHub pull requests (raw JSON) into Snowflake RAW.GITHUB_PULL_REQUESTS",
)
def github_pull_requests_raw(context: AssetExecutionContext) -> None:
    """
    Runs your PR ingestion code. The script already handles:
    - pagination
    - rate limits
    - incremental watermark
    - first-run lookback cutoff
    """
    inserted = load_pull_requests.run()
    context.log.info(f"Inserted {inserted} PR rows into RAW.GITHUB_PULL_REQUESTS")


@asset(
    description="Loads GitHub pull request reviews (raw JSON) into Snowflake RAW.GITHUB_PULL_REQUEST_REVIEWS",
    deps=[github_pull_requests_raw],  # ensures PRs load before reviews
)
def github_pull_request_reviews_raw(context: AssetExecutionContext) -> None:
    """
    Runs your review ingestion code after PRs exist.
    """
    inserted = load_reviews.run()
    context.log.info(f"Inserted {inserted} review rows into RAW.GITHUB_PULL_REQUEST_REVIEWS")