from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any

from .master_engine import master_variance_score
from .price_targets import aggregate_price_targets
from .reporting import format_number

RANKING_FORMULA = (
    "aggregate_score = avg_signal_strength*0.85 + min(total_mentions*0.35, 8) "
    "+ min(appearances*1.0, 2) + avg_master_variance*0.2"
)
CONSENSUS_FORMULA = (
    "aggregate_score = weighted_base_score + quality_weight_adjustment "
    "+ consensus_weight_bonus + consensus_density_bonus - consensus_disagreement_penalty"
)
OPTIMIZED_CONSENSUS_MIN_SCORE = 80.0
OPTIMIZED_CONSENSUS_MIN_CROSS_VALIDATION = 72.0
OPTIMIZED_CONSENSUS_MIN_CHANNEL_WEIGHT_SUM = 2.15
OPTIMIZED_CONSENSUS_MIN_MAJORITY_RATIO = 0.6
OPTIMIZED_CONSENSUS_MIN_VERDICT_ALIGNMENT_RATIO = 0.5
OPTIMIZED_CONSENSUS_MAX_SCORE_SPREAD = 18.0
OPTIMIZED_CONSENSUS_ALLOWED_STRENGTHS = frozenset({"MODERATE", "STRONG"})
OPTIMIZED_CONSENSUS_ALLOWED_STATUSES = frozenset({"CONFIRMED"})

BULLISH_VERDICTS = {"STRONG_BUY", "BUY"}
CAUTIOUS_VERDICTS = {"WATCH", "HOLD"}
BEARISH_VERDICTS = {"REJECT", "SELL", "AVOID"}


def qualifies_weighted_consensus(
    stock: dict[str, Any],
    *,
    min_score: float = OPTIMIZED_CONSENSUS_MIN_SCORE,
    min_cross_validation_score: float = OPTIMIZED_CONSENSUS_MIN_CROSS_VALIDATION,
    min_channel_count: int = 2,
    min_channel_weight_sum: float = OPTIMIZED_CONSENSUS_MIN_CHANNEL_WEIGHT_SUM,
    min_majority_ratio: float = OPTIMIZED_CONSENSUS_MIN_MAJORITY_RATIO,
    min_verdict_alignment_ratio: float = OPTIMIZED_CONSENSUS_MIN_VERDICT_ALIGNMENT_RATIO,
    max_score_spread: float | None = OPTIMIZED_CONSENSUS_MAX_SCORE_SPREAD,
    allowed_strengths: frozenset[str] | set[str] | None = OPTIMIZED_CONSENSUS_ALLOWED_STRENGTHS,
    allowed_statuses: frozenset[str] | set[str] | None = OPTIMIZED_CONSENSUS_ALLOWED_STATUSES,
) -> bool:
    """Return True when a multi-channel signal clears the ROI-tuned consensus bar."""
    channel_count = int(stock.get("channel_count", 0) or 0)
    if channel_count < min_channel_count:
        return False
    if float(stock.get("aggregate_score", 0) or 0) < min_score:
        return False
    if float(stock.get("cross_validation_score", 0) or 0) < min_cross_validation_score:
        return False

    weight_sum = stock.get("channel_weight_sum")
    effective_weight_sum = float(weight_sum if weight_sum is not None else channel_count)
    if effective_weight_sum < min_channel_weight_sum:
        return False

    if allowed_statuses:
        status = str(stock.get("cross_validation_status", "")).upper()
        if status and status not in allowed_statuses:
            return False

    majority_ratio = stock.get("cross_validation_majority_ratio")
    if majority_ratio is not None and float(majority_ratio) < min_majority_ratio:
        return False

    verdict_alignment_ratio = stock.get("verdict_alignment_ratio")
    if verdict_alignment_ratio is not None and float(verdict_alignment_ratio) < min_verdict_alignment_ratio:
        return False

    score_spread = stock.get("score_spread")
    if max_score_spread is not None and score_spread is not None and float(score_spread) > float(max_score_spread):
        return False

    if allowed_strengths:
        strength = str(stock.get("consensus_strength", "")).upper()
        if strength and strength not in allowed_strengths:
            return False
    return True


@dataclass(slots=True)
class RankedStock:
    ticker: str
    company_name: str | None
    aggregate_score: float
    aggregate_verdict: str
    appearances: int
    total_mentions: int
    average_signal_strength: float
    differentiation_score: float
    average_final_score: float
    best_final_score: float
    first_signal_at: str | None
    last_signal_at: str | None
    latest_checked_at: str | None
    latest_price: float | None
    currency: str | None
    price_target: dict[str, Any] | None = None
    source_video_ids: list[str] = field(default_factory=list)
    source_video_titles: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def build_cross_video_ranking(videos: list[dict[str, Any]]) -> list[RankedStock]:
    buckets: dict[str, dict[str, Any]] = {}
    for video in videos:
        if not video.get("should_analyze_stocks"):
            continue
        for stock in video.get("stocks", []):
            ticker = stock["ticker"]
            fundamentals = dict(stock.get("fundamentals", {}) or {})
            bucket = buckets.setdefault(
                ticker,
                {
                    "ticker": ticker,
                    "company_name": stock.get("company_name"),
                    "appearances": 0,
                    "total_mentions": 0,
                    "signal_strength_sum": 0.0,
                    "master_variance_sum": 0.0,
                    "score_sum": 0.0,
                    "best_final_score": 0.0,
                    "first_signal_at": None,
                    "last_signal_at": None,
                    "latest_checked_at": None,
                    "latest_price": None,
                    "currency": fundamentals.get("currency"),
                    "price_targets": [],
                    "source_video_ids": [],
                    "source_video_titles": [],
                },
            )
            bucket["appearances"] += 1
            bucket["total_mentions"] += int(stock.get("mention_count", 0) or 0)
            bucket["signal_strength_sum"] += _signal_strength(video, stock)
            bucket["master_variance_sum"] += master_variance_score(stock.get("master_opinions", []))
            final_score = float(stock.get("final_score", 0.0) or 0.0)
            bucket["score_sum"] += final_score
            bucket["best_final_score"] = max(bucket["best_final_score"], final_score)
            published_at = _normalize_signal_date(video.get("published_at"))
            if _is_earlier_timestamp(published_at, bucket["first_signal_at"]):
                bucket["first_signal_at"] = published_at
            if _is_newer_timestamp(published_at, bucket["last_signal_at"]):
                bucket["last_signal_at"] = published_at
            checked_at = fundamentals.get("checked_at")
            if _is_newer_timestamp(checked_at, bucket["latest_checked_at"]):
                bucket["latest_checked_at"] = checked_at
                bucket["latest_price"] = fundamentals.get("current_price")
                bucket["currency"] = fundamentals.get("currency")
                bucket["company_name"] = stock.get("company_name") or bucket["company_name"]
            if stock.get("price_target"):
                bucket["price_targets"].append(dict(stock["price_target"]))
            bucket["source_video_ids"].append(video.get("video_id", ""))
            bucket["source_video_titles"].append(video.get("title", ""))

    ranking = []
    for bucket in buckets.values():
        average_signal_strength = bucket["signal_strength_sum"] / bucket["appearances"]
        average_master_variance = bucket["master_variance_sum"] / bucket["appearances"]
        average_final_score = bucket["score_sum"] / bucket["appearances"]
        price_target = aggregate_price_targets(
            bucket["price_targets"],
            latest_price=bucket["latest_price"],
            currency=bucket["currency"],
        )
        aggregate_score = min(
            100.0,
            average_signal_strength * 0.85
            + min(bucket["total_mentions"] * 0.35, 8.0)
            + min(bucket["appearances"] * 1.0, 2.0)
            + average_master_variance * 0.2,
        )
        ranking.append(
            RankedStock(
                ticker=bucket["ticker"],
                company_name=bucket["company_name"],
                aggregate_score=round(aggregate_score, 1),
                aggregate_verdict=aggregate_verdict(aggregate_score),
                appearances=bucket["appearances"],
                total_mentions=bucket["total_mentions"],
                average_signal_strength=round(average_signal_strength, 1),
                differentiation_score=round(average_master_variance, 2),
                average_final_score=round(average_final_score, 1),
                best_final_score=round(bucket["best_final_score"], 1),
                first_signal_at=bucket["first_signal_at"],
                last_signal_at=bucket["last_signal_at"],
                latest_checked_at=bucket["latest_checked_at"],
                latest_price=bucket["latest_price"],
                currency=bucket["currency"],
                price_target=price_target,
                source_video_ids=bucket["source_video_ids"],
                source_video_titles=bucket["source_video_titles"],
            )
        )
    ranking.sort(key=lambda item: (-item.aggregate_score, -item.appearances, -item.total_mentions, item.ticker))
    return ranking


def build_consensus_ranking(
    channel_rankings: dict[str, list[dict[str, Any]]],
    *,
    channel_weights: dict[str, float] | None = None,
    channel_names: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    """Aggregate per-channel rankings into one weighted consensus ranking."""
    channel_weights = channel_weights or {}
    channel_names = channel_names or {}
    buckets: dict[str, dict[str, Any]] = {}

    for slug, ranking in channel_rankings.items():
        channel_weight = max(0.1, float(channel_weights.get(slug, 1.0) or 1.0))
        for item in ranking:
            ticker = str(item.get("ticker", "")).strip()
            if not ticker:
                continue
            score = float(item.get("aggregate_score", 0) or 0)
            bucket = buckets.setdefault(
                ticker,
                {
                    "ticker": ticker,
                    "company_name": item.get("company_name"),
                    "appearances": 0,
                    "total_mentions": 0,
                    "weighted_score_sum": 0.0,
                    "channel_weight_sum": 0.0,
                    "first_signal_at": None,
                    "last_signal_at": None,
                    "latest_checked_at": None,
                    "latest_price": None,
                    "currency": item.get("currency"),
                    "price_targets": [],
                    "source_video_titles": [],
                    "_source_channels": [],
                    "_channel_scores": {},
                    "_channel_weights": {},
                    "_channel_verdicts": {},
                    "_channel_directions": {},
                },
            )
            bucket["appearances"] += int(item.get("appearances", 0) or 0)
            bucket["total_mentions"] += int(item.get("total_mentions", 0) or 0)
            bucket["weighted_score_sum"] += score * channel_weight
            bucket["channel_weight_sum"] += channel_weight
            bucket["_source_channels"].append(slug)
            bucket["_channel_scores"][slug] = score
            bucket["_channel_weights"][slug] = channel_weight
            verdict = _normalize_verdict(item.get("aggregate_verdict"))
            bucket["_channel_verdicts"][slug] = verdict
            bucket["_channel_directions"][slug] = _verdict_direction(verdict)
            bucket["source_video_titles"].extend(item.get("source_video_titles", []))
            if item.get("price_target"):
                bucket["price_targets"].append(dict(item["price_target"]))

            if score >= max((value for key, value in bucket["_channel_scores"].items() if key != slug), default=-1.0):
                bucket["company_name"] = item.get("company_name") or bucket["company_name"]
                bucket["latest_price"] = item.get("latest_price") or bucket["latest_price"]
                bucket["currency"] = item.get("currency", bucket["currency"])

            if _is_newer_timestamp(item.get("latest_checked_at"), bucket["latest_checked_at"]):
                bucket["latest_checked_at"] = item.get("latest_checked_at")
                bucket["latest_price"] = item.get("latest_price") or bucket["latest_price"]
                bucket["currency"] = item.get("currency", bucket["currency"])
            if _is_newer_timestamp(item.get("last_signal_at"), bucket["last_signal_at"]):
                bucket["last_signal_at"] = item.get("last_signal_at")
            if _is_earlier_timestamp(item.get("first_signal_at"), bucket["first_signal_at"]):
                bucket["first_signal_at"] = item.get("first_signal_at")

    consensus_ranking: list[dict[str, Any]] = []
    for bucket in buckets.values():
        channel_count = len(bucket["_source_channels"])
        weight_sum = float(bucket["channel_weight_sum"] or 0.0)
        weighted_base_score = bucket["weighted_score_sum"] / weight_sum if weight_sum > 0 else 0.0
        quality_adjustment = max(-10.0, min(10.0, (weight_sum - channel_count) * 6.0))
        price_target = aggregate_price_targets(
            bucket["price_targets"],
            latest_price=bucket["latest_price"],
            currency=bucket["currency"],
        )
        cross_validation = _cross_validate_channel_signals(
            bucket["_channel_scores"],
            bucket["_channel_weights"],
            bucket["_channel_verdicts"],
            bucket["_channel_directions"],
        )
        consensus_bonus = _consensus_weight_bonus(
            channel_count,
            cross_validation["status"],
            cross_validation["score"],
        )
        density_bonus = min(4.0, max(0, bucket["appearances"] - channel_count) * 0.75)
        disagreement_penalty = _consensus_disagreement_penalty(cross_validation)
        aggregate_score = min(
            100.0,
            max(0.0, weighted_base_score + consensus_bonus + quality_adjustment + density_bonus - disagreement_penalty),
        )

        item = {
            "ticker": bucket["ticker"],
            "company_name": bucket["company_name"],
            "aggregate_score": round(aggregate_score, 1),
            "aggregate_verdict": aggregate_verdict(aggregate_score),
            "appearances": bucket["appearances"],
            "total_mentions": bucket["total_mentions"],
            "latest_price": bucket["latest_price"],
            "currency": bucket["currency"],
            "price_target": price_target,
            "last_signal_at": bucket["last_signal_at"],
            "first_signal_at": bucket["first_signal_at"],
            "latest_checked_at": bucket["latest_checked_at"],
            "source_video_titles": bucket["source_video_titles"],
            "_source_channel": bucket["_source_channels"][0] if bucket["_source_channels"] else "",
            "_source_channels": bucket["_source_channels"],
            "_source_channels_display": [channel_names.get(slug, slug) for slug in bucket["_source_channels"]],
            "channel_count": channel_count,
            "consensus_candidate": channel_count > 1,
            "consensus_strength": cross_validation["consensus_strength"],
            "cross_validation_status": cross_validation["status"],
            "cross_validation_score": cross_validation["score"],
            "cross_validation_majority_ratio": cross_validation["majority_ratio"],
            "verdict_alignment_ratio": cross_validation["verdict_alignment_ratio"],
            "score_spread": cross_validation["score_spread"],
            "majority_direction": cross_validation["majority_direction"],
            "majority_verdict": cross_validation["majority_verdict"],
            "channel_weight_sum": round(weight_sum, 3),
            "channel_weight_avg": round(weight_sum / channel_count, 3) if channel_count else 0.0,
            "weighted_base_score": round(weighted_base_score, 1),
            "consensus_bonus": round(consensus_bonus, 1),
            "quality_weight_adjustment": round(quality_adjustment, 1),
            "consensus_density_bonus": round(density_bonus, 1),
            "consensus_disagreement_penalty": round(disagreement_penalty, 1),
            "channel_scores": bucket["_channel_scores"],
            "channel_weights": bucket["_channel_weights"],
            "channel_verdicts": bucket["_channel_verdicts"],
        }
        item["consensus_signal"] = qualifies_weighted_consensus(item)
        item["signal_kind"] = "CONSENSUS" if channel_count > 1 else "SINGLE_SOURCE"
        consensus_ranking.append(item)

    return sorted(
        consensus_ranking,
        key=lambda item: (
            item.get("aggregate_score", 0),
            item.get("channel_count", 0),
            item.get("total_mentions", 0),
            item.get("ticker", ""),
        ),
        reverse=True,
    )


def render_cross_video_ranking_text(ranking: list[RankedStock]) -> str:
    lines = [
        "[통합 종목 랭킹]",
        f"산식: {RANKING_FORMULA}",
    ]
    if not ranking:
        lines.append("- 집계 대상 종목 없음")
        return "\n".join(lines)

    for idx, item in enumerate(ranking, start=1):
        price_target = getattr(item, "price_target", None)
        lines.append(
            "  ".join(
                [
                    f"{idx}. {item.ticker}",
                    f"{item.company_name or 'unknown'}",
                    f"aggregate={item.aggregate_score:.1f} ({item.aggregate_verdict})",
                    f"avg_signal={item.average_signal_strength:.1f}",
                    f"avg_final={item.average_final_score:.1f}",
                    f"appearances={item.appearances}",
                    f"mentions={item.total_mentions}",
                    f"master_var={item.differentiation_score:.2f}",
                    f"first_signal_at={item.first_signal_at or '-'}",
                    f"latest_checked_at={item.latest_checked_at or '-'}",
                    f"latest_price={format_number(item.latest_price, item.currency)}",
                    f"target={(format_number(price_target.get('target_price'), price_target.get('currency')) if price_target else '-')}",
                ]
            )
        )
    return "\n".join(lines)


def _normalize_verdict(value: object) -> str:
    verdict = str(value or "").strip().upper()
    return verdict or "WATCH"


def _verdict_direction(verdict: str) -> str:
    normalized = _normalize_verdict(verdict)
    if normalized in BULLISH_VERDICTS:
        return "BULLISH"
    if normalized in BEARISH_VERDICTS:
        return "BEARISH"
    return "CAUTIOUS"


def _cross_validate_channel_signals(
    channel_scores: dict[str, float],
    channel_weights: dict[str, float],
    channel_verdicts: dict[str, str],
    channel_directions: dict[str, str],
) -> dict[str, Any]:
    channel_count = len(channel_scores)
    if channel_count < 2:
        verdict = next(iter(channel_verdicts.values()), "WATCH")
        return {
            "status": "SINGLE_SOURCE",
            "score": 0.0,
            "majority_ratio": 1.0 if channel_count else 0.0,
            "verdict_alignment_ratio": 1.0 if channel_count else 0.0,
            "score_spread": 0.0,
            "majority_direction": next(iter(channel_directions.values()), "CAUTIOUS"),
            "majority_verdict": verdict,
            "consensus_strength": "SINGLE_SOURCE",
        }

    weight_sum = max(0.001, sum(float(value or 0.0) for value in channel_weights.values()))
    direction_totals: dict[str, float] = defaultdict(float)
    verdict_totals: dict[str, float] = defaultdict(float)
    for slug, weight in channel_weights.items():
        direction_totals[channel_directions.get(slug, "CAUTIOUS")] += float(weight or 0.0)
        verdict_totals[channel_verdicts.get(slug, "WATCH")] += float(weight or 0.0)

    majority_direction, majority_weight = sorted(
        direction_totals.items(),
        key=lambda item: (-item[1], item[0]),
    )[0]
    majority_verdict, majority_verdict_weight = sorted(
        verdict_totals.items(),
        key=lambda item: (-item[1], item[0]),
    )[0]
    majority_ratio = majority_weight / weight_sum
    verdict_alignment_ratio = majority_verdict_weight / weight_sum

    scores = [float(score or 0.0) for score in channel_scores.values()]
    score_spread = max(scores) - min(scores) if scores else 0.0
    score_alignment = max(0.0, 1.0 - min(score_spread, 40.0) / 40.0)

    avg_weight = weight_sum / channel_count if channel_count else 0.0
    quality_support = _clamp((avg_weight - 0.4) / 1.1, 0.0, 1.0)
    cross_validation_score = 100.0 * (
        0.45 * majority_ratio
        + 0.25 * verdict_alignment_ratio
        + 0.20 * score_alignment
        + 0.10 * quality_support
    )
    cross_validation_score = round(_clamp(cross_validation_score, 0.0, 100.0), 1)

    if majority_ratio >= 0.74 and verdict_alignment_ratio >= 0.5 and score_spread <= 18.0:
        status = "CONFIRMED"
    elif majority_ratio >= 0.55 and score_spread <= 28.0:
        status = "MIXED"
    else:
        status = "DIVERGENT"

    if status == "CONFIRMED" and cross_validation_score >= 82.0:
        consensus_strength = "STRONG"
    elif cross_validation_score >= 66.0:
        consensus_strength = "MODERATE"
    else:
        consensus_strength = "WEAK"

    return {
        "status": status,
        "score": cross_validation_score,
        "majority_ratio": round(majority_ratio, 3),
        "verdict_alignment_ratio": round(verdict_alignment_ratio, 3),
        "score_spread": round(score_spread, 1),
        "majority_direction": majority_direction,
        "majority_verdict": majority_verdict,
        "consensus_strength": consensus_strength,
    }


def _consensus_weight_bonus(
    channel_count: int,
    cross_validation_status: str,
    cross_validation_score: float,
) -> float:
    if channel_count < 2:
        return 0.0

    status = str(cross_validation_status or "").upper()
    if status == "CONFIRMED":
        base_bonus = min(18.0, max(0, channel_count - 1) * 6.5)
        verification_bonus = min(8.0, max(0.0, cross_validation_score - 60.0) * 0.18)
    elif status == "MIXED":
        base_bonus = min(8.0, max(0, channel_count - 1) * 2.5)
        verification_bonus = min(4.0, max(0.0, cross_validation_score - 55.0) * 0.08)
    else:
        base_bonus = 0.0
        verification_bonus = 0.0
    return round(base_bonus + verification_bonus, 1)


def _consensus_disagreement_penalty(cross_validation: dict[str, Any]) -> float:
    status = str(cross_validation.get("status", "")).upper()
    majority_ratio = float(cross_validation.get("majority_ratio", 0.0) or 0.0)
    verdict_alignment_ratio = float(cross_validation.get("verdict_alignment_ratio", 0.0) or 0.0)
    score_spread = float(cross_validation.get("score_spread", 0.0) or 0.0)

    penalty = max(0.0, 0.55 - verdict_alignment_ratio) * 8.0
    penalty += max(0.0, 0.7 - majority_ratio) * 4.0
    penalty += max(0.0, score_spread - 14.0) * 0.12
    if status == "DIVERGENT":
        penalty += 2.5
    elif status == "MIXED":
        penalty += 0.5
    return round(min(8.0, penalty), 2)


def aggregate_verdict(score: float) -> str:
    """Map aggregate ranking scores into human-readable verdict buckets."""
    if score >= 80:
        return "STRONG_BUY"
    if score >= 68:
        return "BUY"
    if score >= 55:
        return "WATCH"
    return "REJECT"


def _is_newer_timestamp(candidate: str | None, current: str | None) -> bool:
    if candidate is None:
        return False
    if current is None:
        return True
    return _parse_ts(candidate) > _parse_ts(current)


def _is_earlier_timestamp(candidate: str | None, current: str | None) -> bool:
    if candidate is None:
        return False
    if current is None:
        return True
    return _parse_ts(candidate) < _parse_ts(current)


def _signal_strength(video: dict[str, Any], stock: dict[str, Any]) -> float:
    if stock.get("signal_strength_score") is not None:
        return float(stock["signal_strength_score"])
    video_signal = float(video.get("signal_score", 0.0) or 0.0)
    mention_count = int(stock.get("mention_count", 0) or 0)
    master_var = master_variance_score(stock.get("master_opinions", []))
    return min(100.0, video_signal * 0.7 + min(mention_count * 3.0, 18.0) + master_var * 0.8)


def _normalize_signal_date(value: str | None) -> str | None:
    if not value:
        return None
    if len(value) == 8 and value.isdigit():
        return f"{value[:4]}-{value[4:6]}-{value[6:8]}"
    return value[:10]


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def _parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))
