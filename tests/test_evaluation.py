"""Tests for evaluation module (ranking_validation, ranking_spearman)."""
from dataclasses import dataclass

from omx_brainstorm.evaluation import ranking_spearman


@dataclass
class FakeRankedItem:
    ticker: str
    company_name: str
    aggregate_score: float
    first_signal_at: str


def test_ranking_spearman_perfect_correlation():
    ranking = [
        FakeRankedItem("A", "A Co", 90.0, "2026-01-01"),
        FakeRankedItem("B", "B Co", 80.0, "2026-01-01"),
        FakeRankedItem("C", "C Co", 70.0, "2026-01-01"),
    ]
    # Returns match the ranking order exactly
    validation = {
        f"top_{len(ranking)}": {
            "positions": [
                {"ticker": "A", "return_pct": 30.0},
                {"ticker": "B", "return_pct": 20.0},
                {"ticker": "C", "return_pct": 10.0},
            ]
        }
    }
    result = ranking_spearman(ranking, validation)
    assert result is not None
    assert result == 1.0


def test_ranking_spearman_inverse_correlation():
    ranking = [
        FakeRankedItem("A", "A Co", 90.0, "2026-01-01"),
        FakeRankedItem("B", "B Co", 80.0, "2026-01-01"),
        FakeRankedItem("C", "C Co", 70.0, "2026-01-01"),
    ]
    # Returns are the exact opposite of ranking
    validation = {
        f"top_{len(ranking)}": {
            "positions": [
                {"ticker": "A", "return_pct": 10.0},
                {"ticker": "B", "return_pct": 20.0},
                {"ticker": "C", "return_pct": 30.0},
            ]
        }
    }
    result = ranking_spearman(ranking, validation)
    assert result is not None
    assert result < 0  # Negative correlation for inverse ranking


def test_ranking_spearman_returns_none_for_single_position():
    ranking = [FakeRankedItem("A", "A Co", 90.0, "2026-01-01")]
    validation = {
        "top_1": {"positions": [{"ticker": "A", "return_pct": 10.0}]}
    }
    result = ranking_spearman(ranking, validation)
    assert result is None


def test_ranking_spearman_returns_none_for_empty_validation():
    ranking = [
        FakeRankedItem("A", "A Co", 90.0, "2026-01-01"),
        FakeRankedItem("B", "B Co", 80.0, "2026-01-01"),
    ]
    result = ranking_spearman(ranking, {})
    assert result is None


def test_ranking_spearman_handles_missing_tickers():
    ranking = [
        FakeRankedItem("A", "A Co", 90.0, "2026-01-01"),
        FakeRankedItem("B", "B Co", 80.0, "2026-01-01"),
        FakeRankedItem("C", "C Co", 70.0, "2026-01-01"),
    ]
    # Only 2 tickers present in validation
    validation = {
        f"top_{len(ranking)}": {
            "positions": [
                {"ticker": "A", "return_pct": 30.0},
                {"ticker": "B", "return_pct": 10.0},
            ]
        }
    }
    result = ranking_spearman(ranking, validation)
    assert result is not None
    assert isinstance(result, float)
