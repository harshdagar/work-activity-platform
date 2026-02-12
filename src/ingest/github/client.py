# src/ingest/github/client.py
import os
import time
import requests
from urllib.parse import urlparse, parse_qs
from dotenv import load_dotenv

load_dotenv()

GITHUB_API = "https://api.github.com"

class GitHubClient:
    def __init__(self, token: str | None = None):
        self.token = token or os.getenv("GITHUB_TOKEN")
        if not self.token:
            raise ValueError("Missing GITHUB_TOKEN in environment")

        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        })

    def _handle_rate_limit(self, resp: requests.Response):
        """
        If we hit a rate limit, GitHub returns headers like:
        X-RateLimit-Remaining: 0
        X-RateLimit-Reset: <unix timestamp>
        We'll sleep until reset + a small buffer.
        """
        if resp.status_code in (403, 429):
            remaining = resp.headers.get("X-RateLimit-Remaining")
            reset = resp.headers.get("X-RateLimit-Reset")
            if remaining == "0" and reset:
                sleep_for = max(0, int(reset) - int(time.time()) + 2)
                print(f"[rate-limit] sleeping {sleep_for}s until reset...")
                time.sleep(sleep_for)

    def get(self, path: str, params: dict | None = None):
        """
        Basic GET wrapper.
        """
        url = path if path.startswith("http") else f"{GITHUB_API}{path}"
        while True:
            resp = self.session.get(url, params=params, timeout=60)
            if resp.status_code in (403, 429):
                self._handle_rate_limit(resp)
                # after sleeping, retry
                continue
            resp.raise_for_status()
            return resp

    def get_all_pages(self, path: str, params: dict | None = None):
        """
        GitHub paginates results. This function follows the 'Link' header
        and yields items across all pages.
        """
        resp = self.get(path, params=params)
        data = resp.json()
        if isinstance(data, dict) and "items" in data:
            data = data["items"]

        for item in data:
            yield item

        # Follow "next" links if present
        while resp.links and "next" in resp.links:
            next_url = resp.links["next"]["url"]
            resp = self.get(next_url)
            data = resp.json()
            for item in data:
                yield item