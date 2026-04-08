from __future__ import annotations

from .models import FundamentalSnapshot, MasterOpinion, TickerMention
from .stock_registry import COMPANY_MAP
from .utils import normalize_ws, split_sentences, unique_preserve

INSUFFICIENT_TRANSCRIPT_REASON = "실자막이 없어 전문가 발언 근거를 검증할 수 없으므로 종목 분석을 건너뜁니다."


def is_transcript_backed(transcript_language: str | None, transcript_source: str | None = None) -> bool:
    language = (transcript_language or "").strip().lower()
    source = (transcript_source or "").strip().lower()
    if source == "metadata_fallback":
        return False
    return not language.endswith("metadata_fallback")


def build_video_summary(title: str, transcript_text: str, *, max_sentences: int = 2) -> str:
    sentences = split_sentences(transcript_text)
    selected: list[str] = []
    for sentence in sentences:
        if len(sentence) < 20:
            continue
        selected.append(sentence)
        if len(selected) >= max_sentences:
            break
    if not selected:
        return normalize_ws(title)
    return " ".join(selected)


def build_stock_context_summary(evidence_bullets: list[str], thesis_summary: str) -> str:
    if evidence_bullets:
        return " / ".join(evidence_bullets[:2])
    return normalize_ws(thesis_summary)


def build_stock_evidence(transcript_text: str, mention: TickerMention, max_items: int = 4) -> list[str]:
    evidence = [normalize_ws(item) for item in mention.evidence if normalize_ws(item)]
    aliases = _ticker_aliases(mention.ticker, mention.company_name)
    for sentence in split_sentences(transcript_text):
        lowered = sentence.lower()
        if any(alias in lowered for alias in aliases):
            short = sentence if len(sentence) <= 220 else sentence[:217].rstrip() + "..."
            evidence.append(short)
        if len(evidence) >= max_items:
            break
    return unique_preserve(evidence)[:max_items]


def assess_reasoning_strength(
    *,
    evidence_bullets: list[str],
    fundamentals: FundamentalSnapshot,
    master_opinions: list[MasterOpinion],
    transcript_backed: bool,
) -> tuple[str, str]:
    score = 0
    if transcript_backed:
        score += 2
    evidence_count = len([item for item in evidence_bullets if item.strip()])
    if evidence_count >= 3:
        score += 2
    elif evidence_count >= 1:
        score += 1

    metric_count = sum(
        value is not None
        for value in (
            fundamentals.current_price,
            fundamentals.forward_pe,
            fundamentals.revenue_growth,
            fundamentals.operating_margin,
            fundamentals.return_on_equity,
            fundamentals.debt_to_equity,
        )
    )
    if metric_count >= 5:
        score += 2
    elif metric_count >= 3:
        score += 1

    if len(master_opinions) >= 3:
        score += 1

    if score >= 6:
        label = "STRONG"
    elif score >= 4:
        label = "MODERATE"
    elif score >= 2:
        label = "LIMITED"
    else:
        label = "WEAK"

    summary = (
        f"실자막 {'확보' if transcript_backed else '미확보'}, "
        f"직접 근거 {evidence_count}건, "
        f"재무지표 {metric_count}개 확인, "
        f"구루 관점 {len(master_opinions)}개 반영"
    )
    return label, summary


def _ticker_aliases(ticker: str, company_name: str | None = None) -> list[str]:
    aliases = [ticker, ticker.split(".", 1)[0]]
    if company_name:
        aliases.extend([company_name, *company_name.split()])
    for alias, (mapped_ticker, english_name) in COMPANY_MAP.items():
        if mapped_ticker.upper() == ticker.upper():
            aliases.extend([alias, english_name])
    return unique_preserve(
        alias.lower()
        for alias in aliases
        if normalize_ws(alias) and len(normalize_ws(alias)) >= 2
    )
