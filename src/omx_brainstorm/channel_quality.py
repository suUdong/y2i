from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def _mean(values: list[float]) -> float | None:
    if not values:
        return None
    return sum(values) / len(values)


@dataclass(slots=True)
class ChannelQualityReport:
    """Combined quality assessment for one channel."""
    slug: str
    display_name: str
    actionable_ratio: float
    avg_signal_score: float  # average quality_scorecard.overall
    hit_rate_1d: float | None
    hit_rate_3d: float | None
    hit_rate_5d: float | None  # from signal tracker
    hit_rate_10d: float | None
    avg_return_1d: float | None
    avg_return_3d: float | None
    avg_return_5d: float | None
    avg_return_10d: float | None
    spearman_correlation: float | None
    ranking_predictive_power: float
    overall_quality_score: float
    weight_multiplier: float | None = None
    target_count: int = 0
    target_hit_rate: float | None = None
    avg_target_progress_pct: float | None = None
    pending_targets: int = 0

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

        hit_rate_1d = accuracy.get("hit_rate_1d")
        hit_rate_3d = accuracy.get("hit_rate_3d")
        hit_rate_5d = accuracy.get("hit_rate_5d")
        hit_rate_10d = accuracy.get("hit_rate_10d")
        avg_return_1d = accuracy.get("avg_return_1d")
        avg_return_3d = accuracy.get("avg_return_3d")
        avg_return_5d = accuracy.get("avg_return_5d")
        avg_return_10d = accuracy.get("avg_return_10d")
        target_count = int(accuracy.get("target_count", 0) or 0)
        target_hit_rate = accuracy.get("target_hit_rate")
        avg_target_progress_pct = accuracy.get("avg_target_progress_pct")
        pending_targets = int(accuracy.get("pending_targets", 0) or 0)
        short_hit_rates = [float(value) for value in (hit_rate_1d, hit_rate_3d, hit_rate_5d) if value is not None]
        short_returns = [float(value) for value in (avg_return_1d, avg_return_3d, avg_return_5d) if value is not None]
        short_coverages = [
            float(((accuracy.get("window_stats", {}) or {}).get(window, {}) or {}).get("coverage_pct", 0) or 0)
            for window in ("1d", "3d", "5d")
            if ((accuracy.get("window_stats", {}) or {}).get(window, {}) or {}).get("tracked", 0)
        ]
        avg_short_hit = _mean(short_hit_rates)
        avg_short_return = _mean(short_returns)
        avg_short_coverage = _mean(short_coverages)

        # Overall quality blends structural scorecard and measured short-horizon accuracy.
        quality = scorecard_overall * 0.35
        quality += min(actionable_ratio * 100, 100) * 0.10
        quality += ranking_pp * 0.20
        quality += (avg_short_hit if avg_short_hit is not None else 50.0) * 0.20
        return_score = 50.0
        if avg_short_return is not None:
            return_score = _clamp(50.0 + avg_short_return * 5.0, 0.0, 100.0)
        quality += return_score * 0.10
        quality += (avg_short_coverage if avg_short_coverage is not None else 50.0) * 0.05

        reports.append(ChannelQualityReport(
            slug=slug,
            display_name=info.get("display_name", slug),
            actionable_ratio=actionable_ratio,
            avg_signal_score=scorecard_overall,
            hit_rate_1d=hit_rate_1d,
            hit_rate_3d=hit_rate_3d,
            hit_rate_5d=hit_rate_5d,
            hit_rate_10d=hit_rate_10d,
            avg_return_1d=avg_return_1d,
            avg_return_3d=avg_return_3d,
            avg_return_5d=avg_return_5d,
            avg_return_10d=avg_return_10d,
            target_count=target_count,
            target_hit_rate=target_hit_rate,
            avg_target_progress_pct=avg_target_progress_pct,
            pending_targets=pending_targets,
            spearman_correlation=spearman,
            ranking_predictive_power=ranking_pp,
            overall_quality_score=round(quality, 1),
        ))

    return reports


def rank_channels(reports: list[ChannelQualityReport]) -> list[ChannelQualityReport]:
    """Return channels sorted by overall_quality_score descending."""
    return sorted(reports, key=lambda r: (-r.overall_quality_score, r.slug))


def compute_dynamic_weights(
    ranked_reports: list[ChannelQualityReport],
) -> dict[str, float]:
    """Derive per-channel weight multipliers from measured quality and accuracy.

    The multiplier remains centered near 1.0 for channels with sparse evidence,
    boosts channels with strong short-horizon accuracy, and automatically
    downweights channels with persistently weak hit rates or negative returns.

    Returns:
        Dict mapping channel slug -> weight multiplier (0.4 – 1.5).
    """
    if not ranked_reports:
        return {}
    weights: dict[str, float] = {}
    for report in ranked_reports:
        multiplier = 0.55 + _clamp(report.overall_quality_score, 0.0, 100.0) / 100.0
        short_hit_rates = [float(value) for value in (report.hit_rate_1d, report.hit_rate_3d, report.hit_rate_5d) if value is not None]
        short_returns = [float(value) for value in (report.avg_return_1d, report.avg_return_3d, report.avg_return_5d) if value is not None]

        if short_hit_rates:
            avg_hit = sum(short_hit_rates) / len(short_hit_rates)
            multiplier += (avg_hit - 50.0) / 200.0
            if avg_hit < 45.0:
                multiplier -= 0.15
            elif avg_hit >= 60.0:
                multiplier += 0.05
        else:
            multiplier = _clamp(multiplier, 0.9, 1.1)

        if short_returns:
            avg_return = sum(short_returns) / len(short_returns)
            multiplier += _clamp(avg_return / 25.0, -0.2, 0.15)
            if avg_return < 0:
                multiplier -= min(0.15, abs(avg_return) / 20.0)

        weights[report.slug] = round(_clamp(multiplier, 0.4, 1.5), 3)
    return weights
