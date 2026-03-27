from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import Any


class VideoType(Enum):
    STOCK_PICK = "STOCK_PICK"
    MACRO = "MACRO"
    MARKET_REVIEW = "MARKET_REVIEW"
    EXPERT_INTERVIEW = "EXPERT_INTERVIEW"
    SECTOR = "SECTOR"
    NEWS_EVENT = "NEWS_EVENT"
    OTHER = "OTHER"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


@dataclass(slots=True)
class TranscriptSegment:
    start: float
    duration: float
    text: str


@dataclass(slots=True)
class VideoInput:
    video_id: str
    title: str
    url: str
    channel_id: str | None = None
    channel_title: str | None = None
    published_at: str | None = None
    description: str | None = None
    tags: list[str] = field(default_factory=list)


@dataclass(slots=True)
class TickerMention:
    ticker: str
    company_name: str | None = None
    confidence: float = 0.0
    reason: str = ""
    evidence: list[str] = field(default_factory=list)


@dataclass(slots=True)
class PriceTarget:
    target_price: float
    currency: str | None = None
    confidence: float = 0.0
    direction: str = "UP"
    time_horizon: str | None = None
    reasoning: str = ""
    evidence: list[str] = field(default_factory=list)


@dataclass(slots=True)
class VideoSignalAssessment:
    signal_score: float
    video_signal_class: str
    should_analyze_stocks: bool
    reason: str
    skip_reason: str = ""
    video_type: str = "OTHER"
    metrics: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class MacroInsight:
    indicator: str          # e.g. "interest_rate", "fx", "oil", "fomc", "cpi", "employment"
    direction: str          # "UP", "DOWN", "NEUTRAL"
    label: str              # human-readable Korean label
    confidence: float       # 0.0 ~ 1.0
    matched_keywords: list[str] = field(default_factory=list)
    sentiment: str = "NEUTRAL"  # "BULLISH", "BEARISH", "NEUTRAL"
    beneficiary_sectors: list[str] = field(default_factory=list)


@dataclass(slots=True)
class MarketReviewSummary:
    indices: list[dict[str, str]] = field(default_factory=list)       # [{"name": "코스피", "direction": "UP", "detail": "2600 돌파"}]
    direction: str = "NEUTRAL"                                         # overall market direction
    risk_events: list[str] = field(default_factory=list)
    sector_focus: list[str] = field(default_factory=list)
    key_points: list[str] = field(default_factory=list)
    macro_insights: list[MacroInsight] = field(default_factory=list)


@dataclass(slots=True)
class AnalysisScore:
    framework: str
    score: float
    max_score: float
    verdict: str
    summary: str
    risks: list[str] = field(default_factory=list)
    citations: list[str] = field(default_factory=list)
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class FundamentalSnapshot:
    ticker: str
    company_name: str | None = None
    checked_at: str | None = None
    currency: str | None = None
    current_price: float | None = None
    market_cap: float | None = None
    trailing_pe: float | None = None
    forward_pe: float | None = None
    price_to_book: float | None = None
    revenue_growth: float | None = None
    earnings_growth: float | None = None
    operating_margin: float | None = None
    return_on_equity: float | None = None
    debt_to_equity: float | None = None
    fifty_two_week_change: float | None = None
    data_source: str = ""
    notes: list[str] = field(default_factory=list)


@dataclass(slots=True)
class MasterOpinion:
    master: str
    verdict: str
    score: float
    max_score: float
    one_liner: str
    rationale: list[str] = field(default_factory=list)
    risks: list[str] = field(default_factory=list)
    citations: list[str] = field(default_factory=list)


@dataclass(slots=True)
class StockAnalysis:
    ticker: str
    company_name: str | None
    extracted_from_video: str
    fundamentals: FundamentalSnapshot
    basic_state: str
    basic_signal_summary: str
    basic_signal_verdict: str
    master_opinions: list[MasterOpinion]
    thesis_summary: str
    framework_scores: list[AnalysisScore]
    total_score: float
    max_score: float
    final_verdict: str
    invalidation_triggers: list[str] = field(default_factory=list)
    citations: list[str] = field(default_factory=list)
    raw_llm_payload: dict[str, Any] = field(default_factory=dict)
    price_targets: list[PriceTarget] = field(default_factory=list)


@dataclass(slots=True)
class StructuredClaim:
    claim: str
    reasoning: str = ""
    confidence: float = 0.5
    direction: str = "NEUTRAL"  # "BULLISH", "BEARISH", "NEUTRAL"


@dataclass(slots=True)
class ExpertInsight:
    expert_name: str
    affiliation: str
    key_claims: list[str]
    topic: str = ""
    sentiment: str = "NEUTRAL"  # "BULLISH", "BEARISH", "NEUTRAL"
    mentioned_tickers: list[str] = field(default_factory=list)
    structured_claims: list[StructuredClaim] = field(default_factory=list)


@dataclass(slots=True)
class VideoAnalysisReport:
    run_id: str
    created_at: str
    provider: str
    mode: str
    video: VideoInput
    signal_assessment: VideoSignalAssessment
    transcript_text: str
    transcript_language: str | None
    ticker_mentions: list[TickerMention]
    stock_analyses: list[StockAnalysis]
    macro_insights: list[MacroInsight] = field(default_factory=list)
    market_review: MarketReviewSummary | None = None
    expert_insights: list[ExpertInsight] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
