"""
SCOS Compatibility RAG interface using a remote search endpoint.

Endpoint contract:
    POST  <base_url>/api/v1/scos/compatibility/search
    Body: {"query": "<code snippet>", "limit": <int>}
    Response: {"results": [{code, score, root_cause, additional_notes, test_name}, ...]}
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field

import requests

from .base import BaseRAG, SCOSSearchResult

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://api-sit-assessment.azurewebsites.net"
DEFAULT_SEARCH_PATH = "/api/v1/scos/compatibility/search"


@dataclass
class SCOSRemoteRAGConfig:
    """Configuration for the remote SCOS compatibility search endpoint."""

    base_url: str = DEFAULT_BASE_URL
    search_path: str = DEFAULT_SEARCH_PATH
    timeout_seconds: int = 30
    max_retries: int = 3
    backoff_base_seconds: float = 1.0
    headers: dict[str, str] = field(default_factory=lambda: {"Content-Type": "application/json"})

    @property
    def search_url(self) -> str:
        return f"{self.base_url.rstrip('/')}{self.search_path}"


class SCOSRemoteRAG(BaseRAG):
    """
    SCOS Compatibility RAG backed by a remote HTTP search API.

    Production backend — no Snowflake dependency required.
    """

    def __init__(self, config: SCOSRemoteRAGConfig | None = None) -> None:
        self.config = config or SCOSRemoteRAGConfig()

    def search(self, query: str, limit: int = 5) -> list[SCOSSearchResult]:
        """
        Semantic search for similar failure patterns via the remote endpoint.

        Args:
            query: PySpark code or SQL to search for similar patterns.
            limit: Maximum number of results to return.

        Returns:
            List of SCOSSearchResult with matching patterns.
        """
        last_exc: Exception | None = None
        for attempt in range(self.config.max_retries):
            try:
                resp = requests.post(
                    self.config.search_url,
                    json={"query": query, "limit": limit},
                    headers=self.config.headers,
                    timeout=self.config.timeout_seconds,
                )
                resp.raise_for_status()
                body = resp.json()
                results_raw = body if isinstance(body, list) else body.get("results", [])
                return [SCOSSearchResult.from_response(r) for r in results_raw]
            except requests.RequestException as exc:
                last_exc = exc
                if attempt < self.config.max_retries - 1:
                    delay = self.config.backoff_base_seconds * (2 ** attempt)
                    logger.warning(
                        "SCOS remote search attempt %d/%d failed: %s — retrying in %.1fs",
                        attempt + 1,
                        self.config.max_retries,
                        exc,
                        delay,
                    )
                    time.sleep(delay)

        raise last_exc  # type: ignore[misc]


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(
        description="Test the remote SCOS compatibility search endpoint"
    )
    parser.add_argument(
        "--base-url",
        type=str,
        default=DEFAULT_BASE_URL,
        help=f"Base URL for the search API (default: {DEFAULT_BASE_URL})",
    )
    parser.add_argument(
        "--query",
        type=str,
        default='df.select(col("date"), expr("add_months(to_date(date), 1)"))',
        help="Code snippet to search for",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=3,
        help="Maximum number of results (default: 3)",
    )
    args = parser.parse_args()

    rag = SCOSRemoteRAG(config=SCOSRemoteRAGConfig(base_url=args.base_url))
    prediction = rag.predict_failure(args.query, limit=args.limit)

    print("\n" + "=" * 60)
    print("QUERY:", args.query)
    print("=" * 60)

    print(f"\nFailure Likelihood: {prediction['failure_likelihood']:.1f}%")

    if prediction["matching_code"]:
        print(f"\nMatching Code: {prediction['matching_code'][:100]}...")
        print(f"Root Cause: {prediction['root_cause']}")
        print(f"Additional Notes: {prediction['additional_notes']}")
        print(f"Test Name: {prediction['test_name']}")

    print("\n--- Similar Patterns ---")
    for idx, result in enumerate(prediction["similar_patterns"]):
        print(f"\n[{idx + 1}] Similarity: {result.score:.1%}")
        code_preview = (
            result.code[:80] + "..." if len(result.code) > 80 else result.code
        )
        print(f"    Code: {code_preview}")
        print(f"    Root Cause: {result.root_cause}")
