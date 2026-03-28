from __future__ import annotations

from collections import Counter
from dataclasses import asdict
import logging
import re
from typing import Any

from .expert_interview import extract_expert_insights
from .fundamentals import FundamentalsFetcher
from .macro_signals import extract_macro_insights, indirect_macro_mentions
from .market_review import extract_market_review
from .master_engine import build_master_opinions, master_variance_score
from .models import FundamentalSnapshot, TickerMention, VideoInput, VideoType
from .price_targets import aggregate_price_targets, extract_price_targets
from .signal_features import stock_signal_strength
from .signal_gate import assess_video_signal, downgrade_signal_without_tickers
from .stock_registry import COMPANY_MAP, resolve_kr_ticker
from .transcript_cache import TranscriptCache
from .transcript_runtime import resolve_transcript_text
from .utils import normalize_ws, unique_preserve
from .youtube import TranscriptFetcher

logger = logging.getLogger(__name__)
_TOKEN_RE = re.compile(r"[0-9A-Za-z가-힣&]+")
_AMBIGUOUS_ALIAS_KEYS = {"kt", "lg", "sk", "삼성", "한화"}
_EMAIL_OR_URL_RE = re.compile(r"(?:https?://\S+|www\.\S+|[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,})", re.I)


def _count_alias_hits(text: str, alias: str) -> int:
    escaped = re.escape(alias)
    if alias in _AMBIGUOUS_ALIAS_KEYS:
        return len(re.findall(rf"(?<![0-9a-z가-힣&]){escaped}(?![0-9a-z가-힣&])", text))
    if alias.isascii():
        return len(re.findall(rf"(?<![0-9a-z]){escaped}(?![0-9a-z])", text))
    return text.count(alias)


def _fallback_kr_company_hits(text: str) -> list[tuple[str, tuple[str, str]]]:
    hits: list[tuple[str, tuple[str, str]]] = []
    tokens = [match.group(0).lower() for match in _TOKEN_RE.finditer(text)]
    idx = 0
    while idx < len(tokens):
        matched = False
        for window in range(min(3, len(tokens) - idx), 1, -1):
            candidate = "".join(tokens[idx : idx + window])
            if text.count(candidate) > 0:
                continue
            resolved = resolve_kr_ticker(candidate)
            if resolved is None:
                continue
            hits.append((candidate, resolved))
            idx += window
            matched = True
            break
        if not matched:
            idx += 1
    return hits


def extract_mentions(title: str, text: str, metadata_text: str = "") -> list[tuple[TickerMention, int]]:
    """Extract direct company mentions and merge higher-confidence indirect macro mentions."""
    lower = _EMAIL_OR_URL_RE.sub(" ", f"{title} {text} {metadata_text}".lower())
    counts: Counter[tuple[str, str]] = Counter()
    scores: dict[tuple[str, str], float] = {}
    reasons: dict[tuple[str, str], list[str]] = {}
    for key, (ticker, company) in COMPANY_MAP.items():
        count = _count_alias_hits(lower, key)
        if count > 0:
            counts[(ticker, company)] += count
            scores[(ticker, company)] = max(scores.get((ticker, company), 0.0), float(count) + 0.5)
            reasons.setdefault((ticker, company), []).append(key)
    for candidate, (ticker, company) in _fallback_kr_company_hits(lower):
        counts[(ticker, company)] += 1
        scores[(ticker, company)] = max(scores.get((ticker, company), 0.0), 1.35)
        reasons.setdefault((ticker, company), []).append(candidate)
    suppressed_keys: set[tuple[str, str]] = set()
    for ambiguous in _AMBIGUOUS_ALIAS_KEYS:
        ambiguous_entry = COMPANY_MAP.get(ambiguous)
        if not ambiguous_entry:
            continue
        ambiguous_key = (ambiguous_entry[0], ambiguous_entry[1])
        if ambiguous_key not in reasons:
            continue
        has_longer_prefixed_reason = any(
            reason.startswith(ambiguous) and reason != ambiguous
            for key, key_reasons in reasons.items()
            if key != ambiguous_key
            for reason in key_reasons
        )
        if has_longer_prefixed_reason:
            suppressed_keys.add(ambiguous_key)
            continue
        ambiguous_reasons = sorted(set(reasons.get(ambiguous_key, [])))
        ambiguous_count = counts.get(ambiguous_key, 0)
        if ambiguous_reasons and ambiguous_count < 2 and set(ambiguous_reasons) == {ambiguous}:
            suppressed_keys.add(ambiguous_key)
    macro_context = " ".join(part for part in [text, metadata_text] if part)
    for mention in indirect_macro_mentions(title, macro_context):
        key = (mention.ticker, mention.company_name or "")
        if mention.confidence < 0.55:
            continue
        counts.setdefault(key, 1)
        scores[key] = max(scores.get(key, 0.0), mention.confidence)
        reasons.setdefault(key, list(mention.evidence or [mention.reason]))
    mentions = []
    ranked = sorted(
        (
            (key, score)
            for key, score in scores.items()
            if key not in suppressed_keys
        ),
        key=lambda item: (-item[1], item[0][0]),
    )[:6]
    for (ticker, company), _score in ranked:
        count = counts.get((ticker, company), 1)
        mentions.append(
            (
                TickerMention(
                    ticker=ticker,
                    company_name=company,
                    confidence=min(0.99, 0.45 + count * 0.08),
                    reason=f"제목/자막/메타데이터에서 {', '.join(sorted(set(reasons[(ticker, company)])))} 반복 언급",
                    evidence=list(sorted(set(reasons[(ticker, company)])))[:3],
                ),
                count,
            )
        )
    return mentions


def basic_assessment(snapshot: FundamentalSnapshot) -> tuple[float, str, str, str]:
    """Score a fundamentals snapshot into a simple base verdict and summary."""
    score = 50.0
    notes = []
    if snapshot.revenue_growth is not None:
        rg = _safe_pct(snapshot.revenue_growth)
        score += 8 if rg > 20 else 4 if rg > 5 else -4
        notes.append(f"매출성장률 {rg:.1f}%")
    if snapshot.operating_margin is not None:
        opm = _safe_pct(snapshot.operating_margin)
        score += 8 if opm > 20 else 4 if opm > 10 else -4
        notes.append(f"영업이익률 {opm:.1f}%")
    if snapshot.return_on_equity is not None:
        roe = _safe_pct(snapshot.return_on_equity)
        score += 6 if roe > 15 else 2 if roe > 8 else -3
        notes.append(f"ROE {roe:.1f}%")
    if snapshot.debt_to_equity is not None:
        de = snapshot.debt_to_equity
        score += 4 if de < 80 else 0 if de < 150 else -5
        notes.append(f"D/E {de:.1f}")
    if snapshot.forward_pe is not None:
        fpe = snapshot.forward_pe
        score += 2 if fpe < 25 else -2 if fpe > 40 else 0
        notes.append(f"Forward PE {fpe:.1f}")
    score = max(0.0, min(100.0, score))
    if score >= 72:
        verdict = "BUY"
        state = "기본 재무/수익성 지표가 전반적으로 양호한 상태"
    elif score >= 58:
        verdict = "WATCH"
        state = "기본 지표는 준수하지만 가격 또는 성장 지속성 확인이 필요한 상태"
    else:
        verdict = "REJECT"
        state = "기본 지표만으로는 적극적 진입 근거가 약한 상태"
    return score, verdict, state, " / ".join(notes[:5])


def final_verdict(scores: list[float]) -> tuple[float, str]:
    """Collapse individual framework scores into a single aggregate verdict."""
    total = sum(scores) / len(scores)
    if total >= 72:
        return total, "STRONG_BUY"
    if total >= 60:
        return total, "BUY"
    if total >= 48:
        return total, "WATCH"
    return total, "REJECT"


def analyze_video_heuristic(
    video: VideoInput,
    cache: TranscriptCache,
    fetcher: TranscriptFetcher,
    fundamentals: FundamentalsFetcher,
    *,
    max_fundamental_workers: int | None = None,
    description_max_chars: int = 280,
    max_tags: int = 12,
) -> dict[str, Any]:
    """Run the fast heuristic analysis lane for one resolved video."""
    analysis_text, transcript_language, evidence_source, cached_entry = resolve_transcript_text(video, cache, fetcher, logger)
    metadata_text = " ".join(part for part in [video.description or "", " ".join(video.tags)] if part).strip()
    signal = assess_video_signal(
        video.title,
        analysis_text,
        description=video.description or "",
        tags=video.tags,
        transcript_source=evidence_source,
    )
    video_type = VideoType(signal.video_type)
    cached_video = (cached_entry or {}).get("video", {}) if isinstance(cached_entry, dict) else {}
    published_at = video.published_at or cached_video.get("published_at")

    # VideoType-based enrichment
    macro_insights_data = []
    market_review_data = None
    expert_insights_data = []

    if video_type == VideoType.MARKET_REVIEW:
        try:
            mr = extract_market_review(video.title, analysis_text)
        except Exception as exc:
            logger.warning("Market review extraction failed for %s: %s", video.video_id, exc)
        else:
            market_review_data = asdict(mr)
            macro_insights_data = [asdict(i) for i in mr.macro_insights]
    elif video_type == VideoType.EXPERT_INTERVIEW:
        try:
            expert_insights_data = [asdict(i) for i in extract_expert_insights(video.title, analysis_text, video.description or "")]
        except Exception as exc:
            logger.warning("Expert insight extraction failed for %s: %s", video.video_id, exc)
        try:
            macro_insights_data = [asdict(i) for i in extract_macro_insights(video.title, analysis_text)]
        except Exception as exc:
            logger.warning("Macro insight extraction failed for %s: %s", video.video_id, exc)
    elif video_type not in (VideoType.STOCK_PICK, VideoType.SECTOR):
        try:
            macro_insights_data = [asdict(i) for i in extract_macro_insights(video.title, analysis_text)]
        except Exception as exc:
            logger.warning("Macro insight extraction failed for %s: %s", video.video_id, exc)

    row = {
        "video_id": video.video_id,
        "title": video.title,
        "url": video.url,
        "published_at": published_at,
        "description": _compact_text(video.description or "", max_chars=description_max_chars),
        "tags": _compact_tags(video.tags, max_items=max_tags),
        "video_type": video_type.value,
        "signal_score": signal.signal_score,
        "video_signal_class": signal.video_signal_class,
        "should_analyze_stocks": signal.should_analyze_stocks,
        "reason": signal.reason,
        "skip_reason": signal.skip_reason or (signal.reason if not signal.should_analyze_stocks else ""),
        "signal_metrics": dict(signal.metrics),
        "transcript_language": transcript_language,
        "macro_insights": macro_insights_data,
        "market_review": market_review_data,
        "expert_insights": expert_insights_data,
        "stocks": [],
    }
    if not signal.should_analyze_stocks:
        return row

    mentions = extract_mentions(video.title, analysis_text, metadata_text=metadata_text)
    if not mentions:
        signal = downgrade_signal_without_tickers(signal)
        row["signal_score"] = signal.signal_score
        row["video_signal_class"] = signal.video_signal_class
        row["should_analyze_stocks"] = signal.should_analyze_stocks
        row["reason"] = signal.reason
        row["skip_reason"] = signal.skip_reason
        row["signal_metrics"] = dict(signal.metrics)
        return row
    if hasattr(fundamentals, "fetch_many"):
        try:
            snapshots = fundamentals.fetch_many(
                [mention for mention, _mention_count in mentions],
                max_workers=max_fundamental_workers,
            )
        except Exception as exc:
            logger.warning("Batch fundamentals fetch failed for %s: %s", video.video_id, exc)
            snapshots = {}
    else:
        snapshots = {}
    cached_evidence = {item["ticker"]: list(item.get("evidence", [])) for item in (cached_entry or {}).get("ticker_mentions", [])}
    for mention, mention_count in mentions:
        try:
            snapshot = snapshots.get(mention.ticker)
            if snapshot is None:
                snapshot = fundamentals.fetch(mention)
        except Exception as exc:
            logger.warning("Fundamentals fetch failed for %s in %s: %s", mention.ticker, video.video_id, exc)
            snapshot = FundamentalSnapshot(
                ticker=mention.ticker,
                company_name=mention.company_name,
                data_source="fundamentals_error",
                notes=[f"fetch_error:{type(exc).__name__}"],
            )

        try:
            basic_score, basic_verdict, basic_state, basic_summary = basic_assessment(snapshot)
            evidence_snippets = _compact_evidence(cached_evidence.get(mention.ticker) or list(mention.evidence or [mention.reason]))
            price_targets = extract_price_targets(
                analysis_text,
                ticker=mention.ticker,
                company_name=snapshot.company_name or mention.company_name,
                current_price=snapshot.current_price,
                currency=snapshot.currency,
            )
            price_target_payloads = [asdict(item) for item in price_targets]
            mops = build_master_opinions(
                ticker=mention.ticker,
                company_name=snapshot.company_name or mention.company_name,
                snapshot=snapshot,
                mention_count=mention_count,
                video_title=video.title,
                video_signal_score=signal.signal_score,
                evidence_snippets=evidence_snippets,
            )
            variance = master_variance_score(mops)
            total_score, verdict = final_verdict([basic_score] + [item.score for item in mops])
            row["stocks"].append(
                {
                    "ticker": mention.ticker,
                    "company_name": snapshot.company_name or mention.company_name,
                    "mention_count": mention_count,
                    "signal_timestamp": video.published_at,
                    "signal_strength_score": stock_signal_strength(
                        ticker=mention.ticker,
                        company_name=snapshot.company_name or mention.company_name,
                        video_signal_score=signal.signal_score,
                        mention_count=mention_count,
                        master_variance=variance,
                        evidence_snippets=evidence_snippets,
                        evidence_source=evidence_source,
                    ),
                    "evidence_source": evidence_source,
                    "evidence_snippets": evidence_snippets,
                    "basic_state": basic_state,
                    "basic_signal_summary": basic_summary,
                    "basic_signal_verdict": basic_verdict,
                    "fundamentals": asdict(snapshot),
                    "master_opinions": [asdict(item) for item in mops],
                    "final_score": round(total_score, 1),
                    "final_verdict": verdict,
                    "invalidation_triggers": ["실적/가이던스 둔화", "섹터 CAPEX 둔화", "멀티플 재조정"],
                    "price_targets": price_target_payloads,
                    "price_target": aggregate_price_targets(
                        price_target_payloads,
                        latest_price=snapshot.current_price,
                        currency=snapshot.currency,
                    ),
                }
            )
        except Exception as exc:
            logger.warning("Stock analysis failed for %s in %s: %s", mention.ticker, video.video_id, exc)
    return row


def heuristic_rows_to_reports(rows: list[dict]) -> list[VideoAnalysisReport]:
    """Convert heuristic pipeline dict rows into VideoAnalysisReport objects for dashboard rendering."""
    from .models import (
        AnalysisScore, ExpertInsight, MacroInsight, MarketReviewSummary,
        MasterOpinion, PriceTarget, StockAnalysis, TickerMention, VideoAnalysisReport,
        VideoInput, VideoSignalAssessment,
    )

    reports: list[VideoAnalysisReport] = []
    for row in rows:
        video = VideoInput(
            video_id=row["video_id"],
            title=row["title"],
            url=row["url"],
            published_at=row.get("published_at"),
            description=row.get("description"),
            tags=row.get("tags", []),
        )
        signal = VideoSignalAssessment(
            signal_score=row["signal_score"],
            video_signal_class=row["video_signal_class"],
            should_analyze_stocks=row["should_analyze_stocks"],
            reason=row["reason"],
            skip_reason=row.get("skip_reason", ""),
            video_type=row.get("video_type", "OTHER"),
        )
        macro_insights = [MacroInsight(**m) for m in row.get("macro_insights", [])]
        expert_insights = [ExpertInsight(**e) for e in row.get("expert_insights", [])]
        mr_data = row.get("market_review")
        market_review = None
        if mr_data:
            mr_macros = [MacroInsight(**m) for m in mr_data.pop("macro_insights", [])]
            market_review = MarketReviewSummary(**mr_data, macro_insights=mr_macros)

        stock_analyses = []
        for s in row.get("stocks", []):
            mops = [MasterOpinion(**m) for m in s.get("master_opinions", [])]
            snap = FundamentalSnapshot(**s["fundamentals"])
            price_targets = [PriceTarget(**item) for item in s.get("price_targets", [])]
            stock_analyses.append(StockAnalysis(
                ticker=s["ticker"],
                company_name=s.get("company_name"),
                extracted_from_video=row["video_id"],
                fundamentals=snap,
                basic_state=s.get("basic_state", ""),
                basic_signal_summary=s.get("basic_signal_summary", ""),
                basic_signal_verdict=s.get("basic_signal_verdict", ""),
                master_opinions=mops,
                thesis_summary="",
                framework_scores=[],
                total_score=s.get("final_score", 0.0),
                max_score=100.0,
                final_verdict=s.get("final_verdict", ""),
                invalidation_triggers=s.get("invalidation_triggers", []),
                price_targets=price_targets,
            ))

        mentions = [
            TickerMention(ticker=s["ticker"], company_name=s.get("company_name"))
            for s in row.get("stocks", [])
        ]
        reports.append(VideoAnalysisReport(
            run_id=row["video_id"][:10],
            created_at=row.get("published_at") or "",
            provider="heuristic",
            mode="heuristic",
            video=video,
            signal_assessment=signal,
            transcript_text="",
            transcript_language=row.get("transcript_language"),
            ticker_mentions=mentions,
            stock_analyses=stock_analyses,
            macro_insights=macro_insights,
            market_review=market_review,
            expert_insights=expert_insights,
        ))
    return reports


def render_heuristic_dashboard(rows: list[dict], output_dir, label: str = "heuristic_dashboard"):
    """Generate a combined dashboard from heuristic pipeline results."""
    from .reporting import save_combined_dashboard
    from pathlib import Path
    reports = heuristic_rows_to_reports(rows)
    if not reports:
        return None
    return save_combined_dashboard(reports, Path(output_dir), label=label)


def _safe_pct(value: float | None) -> float:
    return 0.0 if value is None else value * 100


def _compact_text(text: str, max_chars: int) -> str:
    normalized = normalize_ws(text)
    if len(normalized) <= max_chars:
        return normalized
    return normalized[: max_chars - 3].rstrip() + "..."


def _compact_tags(tags: list[str], max_items: int) -> list[str]:
    compacted = [normalize_ws(tag)[:40] for tag in tags if normalize_ws(tag)]
    return unique_preserve(compacted)[:max_items]


def _compact_evidence(items: list[str], max_items: int = 3, max_chars: int = 160) -> list[str]:
    compacted = []
    for item in items:
        normalized = normalize_ws(item)
        if not normalized:
            continue
        compacted.append(normalized[:max_chars])
    return unique_preserve(compacted)[:max_items]
