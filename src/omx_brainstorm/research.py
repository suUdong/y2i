from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any

from .master_engine import master_variance_score
from .reporting import format_number

RANKING_FORMULA = (
    "aggregate_score = avg_signal_strength*0.85 + min(total_mentions*0.35, 8) "
    "+ min(appearances*1.0, 2) + avg_master_variance*0.2"
)


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
            bucket["source_video_ids"].append(video.get("video_id", ""))
            bucket["source_video_titles"].append(video.get("title", ""))

    ranking = []
    for bucket in buckets.values():
        average_signal_strength = bucket["signal_strength_sum"] / bucket["appearances"]
        average_master_variance = bucket["master_variance_sum"] / bucket["appearances"]
        average_final_score = bucket["score_sum"] / bucket["appearances"]
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
                    "source_video_titles": [],
                    "_source_channels": [],
                    "_channel_scores": {},
                    "_channel_weights": {},
                },
            )
            bucket["appearances"] += int(item.get("appearances", 0) or 0)
            bucket["total_mentions"] += int(item.get("total_mentions", 0) or 0)
            bucket["weighted_score_sum"] += score * channel_weight
            bucket["channel_weight_sum"] += channel_weight
            bucket["_source_channels"].append(slug)
            bucket["_channel_scores"][slug] = score
            bucket["_channel_weights"][slug] = channel_weight
            bucket["source_video_titles"].extend(item.get("source_video_titles", []))

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
        consensus_bonus = min(15.0, max(0, channel_count - 1) * 5.0)
        quality_adjustment = max(-10.0, min(10.0, (weight_sum - channel_count) * 6.0))
        density_bonus = min(4.0, max(0, bucket["appearances"] - channel_count) * 0.75)
        aggregate_score = min(100.0, max(0.0, weighted_base_score + consensus_bonus + quality_adjustment + density_bonus))

        consensus_ranking.append(
            {
                "ticker": bucket["ticker"],
                "company_name": bucket["company_name"],
                "aggregate_score": round(aggregate_score, 1),
                "aggregate_verdict": aggregate_verdict(aggregate_score),
                "appearances": bucket["appearances"],
                "total_mentions": bucket["total_mentions"],
                "latest_price": bucket["latest_price"],
                "currency": bucket["currency"],
                "last_signal_at": bucket["last_signal_at"],
                "first_signal_at": bucket["first_signal_at"],
                "latest_checked_at": bucket["latest_checked_at"],
                "source_video_titles": bucket["source_video_titles"],
                "_source_channel": bucket["_source_channels"][0] if bucket["_source_channels"] else "",
                "_source_channels": bucket["_source_channels"],
                "_source_channels_display": [channel_names.get(slug, slug) for slug in bucket["_source_channels"]],
                "channel_count": channel_count,
                "channel_weight_sum": round(weight_sum, 3),
                "channel_weight_avg": round(weight_sum / channel_count, 3) if channel_count else 0.0,
                "weighted_base_score": round(weighted_base_score, 1),
                "consensus_bonus": round(consensus_bonus, 1),
                "quality_weight_adjustment": round(quality_adjustment, 1),
                "consensus_density_bonus": round(density_bonus, 1),
                "channel_scores": bucket["_channel_scores"],
                "channel_weights": bucket["_channel_weights"],
            }
        )

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
                ]
            )
        )
    return "\n".join(lines)


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


def _parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))
