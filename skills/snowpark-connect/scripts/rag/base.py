# flake8: noqa: T201

"""
Base interface and shared types for SCOS compatibility RAG services.

Defines the ``BaseRAG`` ABC that both the Cortex Search (dev) and
Remote API (production) implementations conform to.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Self


@dataclass
class SCOSSearchResult:
    """A search result from the SCOS RAG service."""

    code: str
    score: float
    root_cause: str | None = None
    additional_notes: str | None = None
    test_name: str | None = None

    @property
    def will_likely_fail(self) -> bool:
        """Returns True if this pattern indicates a failure."""
        return self.root_cause is not None

    @classmethod
    def from_response(cls, data: dict) -> Self:
        cosine_similarity = data.get("@scores", {}).get("cosine_similarity", 0.0)

        return cls(
            code=data.get("code", ""),
            score=cosine_similarity,
            root_cause=data.get("root_cause") or None,
            additional_notes=data.get("additional_notes") or None,
            test_name=data.get("test_name") or None,
        )


class BaseRAG(ABC):
    """
    Abstract base for SCOS compatibility RAG backends.

    Subclasses only need to implement ``search``; prediction logic is shared.
    """

    @abstractmethod
    def search(self, query: str, limit: int = 5) -> list[SCOSSearchResult]:
        """Search for similar failing patterns. Implemented by subclasses."""

    def predict_failure(self, query: str, limit: int = 3) -> dict[str, Any]:
        """
        Predict if a given code/SQL snippet will fail based on similar patterns.

        Args:
            query: The code or SQL to analyze.
            limit: Maximum number of similar patterns to return.

        Returns:
            Dict with prediction results including failure_likelihood
            and similar_patterns.
        """
        results = self.search(query, limit=limit)
        if not results:
            return self._get_empty_prediction()
        return self._build_prediction(results[0], results)

    @staticmethod
    def _get_empty_prediction() -> dict[str, Any]:
        return {
            "failure_likelihood": 0.0,
            "matching_code": None,
            "root_cause": None,
            "additional_notes": None,
            "test_name": None,
            "similar_patterns": [],
        }

    @staticmethod
    def _build_prediction(
        top_result: SCOSSearchResult,
        results: list[SCOSSearchResult],
    ) -> dict[str, Any]:
        failure_likelihood = (
            top_result.score * 100 if top_result.will_likely_fail else 0.0
        )
        return {
            "failure_likelihood": failure_likelihood,
            "matching_code": top_result.code,
            "root_cause": top_result.root_cause,
            "additional_notes": top_result.additional_notes,
            "test_name": top_result.test_name,
            "similar_patterns": results,
        }
