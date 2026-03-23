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
