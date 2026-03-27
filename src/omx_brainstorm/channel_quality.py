from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass(slots=True)
class ChannelQualityReport:
    """Combined quality assessment for one channel."""
    slug: str
    display_name: str
    actionable_ratio: float
    avg_signal_score: float  # average quality_scorecard.overall
    hit_rate_5d: float | None  # from signal tracker
    hit_rate_10d: float | None
    avg_return_5d: float | None
    avg_return_10d: float | None
    spearman_correlation: float | None
    ranking_predictive_power: float
    overall_quality_score: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def compute_channel_quality(
    channel_comparison: dict[str, Any],
    accuracy_by_channel: dict[str, Any] | None = None,
) -> list[ChannelQualityReport]:
    """Combine comparison scorecard data with signal tracker accuracy.

    Args:
        channel_comparison: The 'channels' dict from a comparison JSON output.
        accuracy_by_channel: Dict mapping channel_slug -> AccuracyStats.to_dict().
            Can be None if signal tracker has no data yet.

    Returns:
        List of ChannelQualityReport for each channel.
    """
    accuracy_by_channel = accuracy_by_channel or {}
    reports = []

    for slug, info in channel_comparison.items():
        scorecard = info.get("quality_scorecard", {})
        accuracy = accuracy_by_channel.get(slug, {})

        actionable_ratio = float(info.get("actionable_ratio", 0))
        scorecard_overall = float(scorecard.get("overall", 0))
        ranking_pp = float(scorecard.get("ranking_predictive_power", 0))
        spearman = info.get("ranking_spearman")

        hit_rate_5d = accuracy.get("hit_rate_5d")
        hit_rate_10d = accuracy.get("hit_rate_10d")
        avg_return_5d = accuracy.get("avg_return_5d")
        avg_return_10d = accuracy.get("avg_return_10d")

        # Overall quality = weighted combination of scorecard + accuracy
        # Base: scorecard overall (0-100 scale)
        quality = scorecard_overall * 0.4

        # Actionable density bonus (channels that produce more signals are more useful)
        quality += min(actionable_ratio * 100, 100) * 0.15

        # Ranking predictive power from backtest
        quality += ranking_pp * 0.2

        # Signal tracker accuracy bonus (if data exists)
        if hit_rate_5d is not None:
            # hit_rate_5d is 0-100%, normalize
            quality += min(hit_rate_5d, 100) * 0.15
        else:
            # No accuracy data yet, give neutral score
            quality += 50 * 0.15

        # Return quality bonus
        if avg_return_5d is not None:
            # Positive returns boost, cap at +-10%
            return_factor = max(-10, min(10, avg_return_5d)) / 10 * 100
            quality += max(0, return_factor) * 0.10
        else:
            quality += 50 * 0.10

        reports.append(ChannelQualityReport(
            slug=slug,
            display_name=info.get("display_name", slug),
            actionable_ratio=actionable_ratio,
            avg_signal_score=scorecard_overall,
            hit_rate_5d=hit_rate_5d,
            hit_rate_10d=hit_rate_10d,
            avg_return_5d=avg_return_5d,
            avg_return_10d=avg_return_10d,
            spearman_correlation=spearman,
            ranking_predictive_power=ranking_pp,
            overall_quality_score=round(quality, 1),
        ))

    return reports


def rank_channels(reports: list[ChannelQualityReport]) -> list[ChannelQualityReport]:
    """Return channels sorted by overall_quality_score descending."""
    return sorted(reports, key=lambda r: (-r.overall_quality_score, r.slug))
