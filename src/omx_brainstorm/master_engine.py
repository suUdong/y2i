from __future__ import annotations

from statistics import pstdev
from typing import Callable, Protocol

from .models import FundamentalSnapshot, MasterOpinion


class MasterStrategy(Protocol):
    """Protocol for master-specific scoring and opinion generation."""

    def __call__(
        self,
        ticker: str,
        primary_evidence: str,
        mention_count: int,
        video_signal_score: float,
        revenue: float,
        margin: float,
        roe: float,
        momentum: float,
        forward_pe: float,
        leverage: float,
        theme_score: int,
        snapshot: FundamentalSnapshot,
    ) -> MasterOpinion: ...


def build_master_opinions(
    ticker: str,
    company_name: str | None,
    snapshot: FundamentalSnapshot,
    mention_count: int,
    video_title: str,
    video_signal_score: float,
    evidence_snippets: list[str],
) -> list[MasterOpinion]:
    """Build stock-specific opinions for the supported master set."""
    evidence_snippets = [item.strip() for item in evidence_snippets if item and item.strip()]
    primary_evidence = evidence_snippets[0] if evidence_snippets else video_title
    revenue = _pct(snapshot.revenue_growth)
    margin = _pct(snapshot.operating_margin)
    roe = _pct(snapshot.return_on_equity)
    momentum = _pct(snapshot.fifty_two_week_change)
    forward_pe = snapshot.forward_pe or 0.0
    leverage = snapshot.debt_to_equity or 0.0
    theme_score = 1 if any(word in video_title.lower() for word in ["로드맵", "roadmap", "foundry", "gpu", "전력", "memory"]) else 0

    opinions = [
        strategy(
            ticker=ticker,
            primary_evidence=primary_evidence,
            mention_count=mention_count,
            video_signal_score=video_signal_score,
            revenue=revenue,
            margin=margin,
            roe=roe,
            momentum=momentum,
            forward_pe=forward_pe,
            leverage=leverage,
            theme_score=theme_score,
            snapshot=snapshot,
        )
        for strategy in MASTER_STRATEGIES
    ]
    return validate_master_opinions(opinions)


def master_variance_score(master_opinions: list[dict] | list[MasterOpinion]) -> float:
    """Measure disagreement across master scores."""
    scores = []
    for item in master_opinions:
        if isinstance(item, MasterOpinion):
            scores.append(float(item.score))
        else:
            scores.append(float(item.get("score", 0.0)))
    if len(scores) < 2:
        return 0.0
    return round(pstdev(scores), 2)


def validate_master_opinions(master_opinions: list[MasterOpinion]) -> list[MasterOpinion]:
    """Validate that each master opinion contains required non-boilerplate content."""
    one_liners = [item.one_liner.strip() for item in master_opinions if item.one_liner.strip()]
    if len(one_liners) != len(master_opinions):
        raise ValueError("master one-liner missing")
    if len(set(one_liners)) != len(one_liners):
        raise ValueError("master one-liners must be unique per stock")
    for item in master_opinions:
        if not item.rationale:
            raise ValueError(f"{item.master} rationale missing")
        if not any(citation.startswith("fundamentals:") for citation in item.citations):
            raise ValueError(f"{item.master} fundamentals citation missing")
        if not any(citation.startswith("evidence:") for citation in item.citations):
            raise ValueError(f"{item.master} evidence citation missing")
    return master_opinions


def validate_cross_stock_master_quality(stocks: list[dict]) -> None:
    """Reject repeated identical master sentences across different stocks."""
    seen: dict[tuple[str, str], str] = {}
    for stock in stocks:
        ticker = stock.get("ticker", "unknown")
        for item in stock.get("master_opinions", []):
            master = item["master"] if isinstance(item, dict) else item.master
            sentence = (item["one_liner"] if isinstance(item, dict) else item.one_liner).strip()
            if not sentence:
                raise ValueError(f"{ticker} missing master one-liner")
            key = (master, sentence)
            if key in seen and seen[key] != ticker:
                raise ValueError(f"Repeated master sentence across stocks: {master} -> {sentence}")
            seen[key] = ticker


def _pct(value: float | None) -> float:
    return 0.0 if value is None else value * 100.0


def master_verdict(score: float) -> str:
    if score >= 80:
        return "BUY"
    if score >= 62:
        return "WATCH"
    return "REJECT"


def _clamp(score: float) -> float:
    return max(0.0, min(100.0, score))


def _build_druckenmiller(**kwargs) -> MasterOpinion:
    ticker = kwargs["ticker"]
    primary_evidence = kwargs["primary_evidence"]
    mention_count = kwargs["mention_count"]
    video_signal_score = kwargs["video_signal_score"]
    revenue = kwargs["revenue"]
    margin = kwargs["margin"]
    forward_pe = kwargs["forward_pe"]
    theme_score = kwargs["theme_score"]

    mention_component = min(mention_count * 2.2, 12)
    revenue_component = min(revenue * 0.12, 10)
    signal_component = min(video_signal_score * 0.08, 8)
    theme_component = theme_score * 6
    valuation_penalty = max(forward_pe - 35, 0) * 0.25
    score = _clamp(56 + mention_component + revenue_component + signal_component + theme_component - valuation_penalty)
    return MasterOpinion(
        master="druckenmiller",
        verdict=master_verdict(score),
        score=round(score, 1),
        max_score=100.0,
        one_liner=(
            f"{ticker}는 매출성장률 {revenue:.1f}%와 영상 내 촉매 '{primary_evidence}' 덕분에 "
            f"18~24개월 선행 드라이버는 강하지만, forward PER {forward_pe:.1f}배 구간이라 추격 강매수보다는 유동성 점검이 먼저다."
        ),
        rationale=[
            f"영상 신호 점수 {video_signal_score:.1f}와 mention_count {mention_count}가 수급·드라이버 지속성을 지지한다.",
            f"매출성장률 {revenue:.1f}%와 영업이익률 {margin:.1f}%는 이익 레버리지가 아직 꺾이지 않았음을 보여준다.",
        ],
        risks=[
            "수요 드라이버 약화 또는 capex 피크아웃",
            "정책/유동성 변화로 멀티플이 압축될 위험",
        ],
        citations=[
            f"fundamentals:revenue_growth={revenue:.1f}%",
            f"fundamentals:forward_pe={forward_pe:.1f}",
            f"evidence:{primary_evidence}",
        ],
    )


def _build_buffett(**kwargs) -> MasterOpinion:
    ticker = kwargs["ticker"]
    primary_evidence = kwargs["primary_evidence"]
    margin = kwargs["margin"]
    roe = kwargs["roe"]
    forward_pe = kwargs["forward_pe"]
    leverage = kwargs["leverage"]

    margin_component = min(margin * 0.18, 12)
    roe_component = min(roe * 0.10, 10)
    leverage_component = 6 if leverage < 60 else 2 if leverage < 120 else -5
    valuation_component = 6 if 0 < forward_pe < 22 else 1 if forward_pe < 30 else -6
    score = _clamp(48 + margin_component + roe_component + leverage_component + valuation_component)
    return MasterOpinion(
        master="buffett",
        verdict=master_verdict(score),
        score=round(score, 1),
        max_score=100.0,
        one_liner=(
            f"{ticker}는 영업이익률 {margin:.1f}%와 ROE {roe:.1f}%로 사업 질은 좋지만, "
            f"부채비율 {leverage:.1f}와 밸류에이션을 감안하면 '{primary_evidence}'만으로 안전마진이 충분하다고 보긴 어렵다."
        ),
        rationale=[
            f"ROE {roe:.1f}%와 영업이익률 {margin:.1f}%는 자본효율과 가격결정력을 뒷받침한다.",
            f"forward PER {forward_pe:.1f}배와 부채비율 {leverage:.1f}는 버핏식 보수적 진입 여부를 가른다.",
        ],
        risks=[
            "밸류에이션이 장기 복리 수익률을 잠식할 위험",
            "산업 사이클과 고객 집중이 예측 가능성을 낮출 위험",
        ],
        citations=[
            f"fundamentals:operating_margin={margin:.1f}%",
            f"fundamentals:return_on_equity={roe:.1f}%",
            f"evidence:{primary_evidence}",
        ],
    )


def _build_soros(**kwargs) -> MasterOpinion:
    ticker = kwargs["ticker"]
    primary_evidence = kwargs["primary_evidence"]
    mention_count = kwargs["mention_count"]
    video_signal_score = kwargs["video_signal_score"]
    momentum = kwargs["momentum"]
    forward_pe = kwargs["forward_pe"]
    theme_score = kwargs["theme_score"]
    snapshot = kwargs["snapshot"]

    momentum_component = min(momentum * 0.10, 10)
    signal_component = min(video_signal_score * 0.10, 9)
    mention_component = min(mention_count * 1.4, 8)
    theme_component = theme_score * 4
    valuation_penalty = max(forward_pe - 40, 0) * 0.15
    score = _clamp(50 + momentum_component + signal_component + mention_component + theme_component - valuation_penalty)
    return MasterOpinion(
        master="soros",
        verdict=master_verdict(score),
        score=round(score, 1),
        max_score=100.0,
        one_liner=(
            f"{ticker}는 52주 수익률 {momentum:.1f}%와 '{primary_evidence}'가 맞물려 서사가 아직 자기강화 구간에 있지만, "
            f"신호 점수 {video_signal_score:.1f}가 높을수록 반사성 꺾임도 빨라질 수 있다."
        ),
        rationale=[
            "영상 제목과 근거 문구는 현재 테마가 네트워크/메모리/전력 병목 해소 서사 위에 있다는 점을 보여준다.",
            f"mention_count {mention_count}와 52주 변화율 {momentum:.1f}%는 추세 추종 자금이 붙기 쉬운 조건이다.",
        ],
        risks=[
            "서사 약화 시 추세 반전이 급격할 수 있음",
            "포지셔닝 과열로 작은 악재에도 변동성이 커질 수 있음",
        ],
        citations=[
            f"fundamentals:fifty_two_week_change={momentum:.1f}%",
            f"fundamentals:current_price={snapshot.current_price or 0.0}",
            f"evidence:{primary_evidence}",
        ],
    )


MASTER_STRATEGIES: list[MasterStrategy] = [
    _build_druckenmiller,
    _build_buffett,
    _build_soros,
]
