from __future__ import annotations

from dataclasses import asdict
from typing import Any

from .models import StockAnalysis, TickerMention, VideoAnalysisReport
from .price_targets import aggregate_price_targets
from .signal_features import stock_signal_strength


def report_to_comparison_row(report: VideoAnalysisReport) -> dict[str, Any]:
    evidence_source = _evidence_source(report)
    row = {
        "video_id": report.video.video_id,
        "title": report.video.title,
        "url": report.video.url,
        "published_at": report.video.published_at,
        "description": report.video.description or "",
        "tags": list(report.video.tags),
        "video_type": report.signal_assessment.video_type,
        "signal_score": report.signal_assessment.signal_score,
        "video_signal_class": report.signal_assessment.video_signal_class,
        "should_analyze_stocks": report.signal_assessment.should_analyze_stocks,
        "reason": report.signal_assessment.reason,
        "skip_reason": report.signal_assessment.skip_reason or (report.signal_assessment.reason if not report.signal_assessment.should_analyze_stocks else ""),
        "signal_metrics": dict(report.signal_assessment.metrics),
        "transcript_language": report.transcript_language,
        "transcript_backed": report.transcript_backed,
        "source_quality_note": report.source_quality_note,
        "video_summary": report.video_summary,
        "macro_insights": [asdict(item) for item in report.macro_insights],
        "market_review": asdict(report.market_review) if report.market_review is not None else None,
        "expert_insights": [asdict(item) for item in report.expert_insights],
        "stocks": [],
    }

    mentions_by_ticker = {item.ticker: item for item in report.ticker_mentions}
    for stock in report.stock_analyses:
        mention = mentions_by_ticker.get(stock.ticker, TickerMention(ticker=stock.ticker, company_name=stock.company_name))
        if stock.evidence_bullets:
            evidence_snippets = list(stock.evidence_bullets)
        elif mention.evidence:
            evidence_snippets = list(mention.evidence)
        elif mention.reason:
            evidence_snippets = [mention.reason]
        else:
            evidence_snippets = []
        mention_count = _mention_count(mention, stock)
        variance = _master_variance(stock)
        price_target_payloads = [asdict(item) for item in stock.price_targets]
        row["stocks"].append(
            {
                "ticker": stock.ticker,
                "company_name": stock.company_name,
                "mention_count": mention_count,
                "signal_timestamp": report.video.published_at,
                "signal_strength_score": stock_signal_strength(
                    ticker=stock.ticker,
                    company_name=stock.company_name,
                    video_signal_score=report.signal_assessment.signal_score,
                    mention_count=mention_count,
                    master_variance=variance,
                    evidence_snippets=evidence_snippets,
                    evidence_source=evidence_source,
                ),
                "evidence_source": evidence_source,
                "evidence_snippets": evidence_snippets,
                "basic_state": stock.basic_state,
                "basic_signal_summary": stock.basic_signal_summary,
                "basic_signal_verdict": stock.basic_signal_verdict,
                "fundamentals": asdict(stock.fundamentals),
                "master_opinions": [asdict(item) for item in stock.master_opinions],
                "final_score": round(stock.total_score, 1),
                "final_verdict": stock.final_verdict,
                "invalidation_triggers": list(stock.invalidation_triggers),
                "plain_summary": stock.plain_summary,
                "video_context_summary": stock.video_context_summary,
                "reasoning_strength": stock.reasoning_strength,
                "reasoning_strength_summary": stock.reasoning_strength_summary,
                "price_targets": price_target_payloads,
                "price_target": aggregate_price_targets(
                    price_target_payloads,
                    latest_price=stock.fundamentals.current_price,
                    currency=stock.fundamentals.currency,
                ),
            }
        )
    return row


def reports_to_comparison_rows(reports: list[VideoAnalysisReport]) -> list[dict[str, Any]]:
    return [report_to_comparison_row(report) for report in reports]


def _evidence_source(report: VideoAnalysisReport) -> str:
    language = (report.transcript_language or "").lower()
    if language.startswith("cache:"):
        return "transcript_cache"
    if report.transcript_backed:
        return "transcript_api"
    return "metadata_fallback"


def _mention_count(mention: TickerMention, stock: StockAnalysis) -> int:
    return max(1, len(stock.evidence_bullets) or len(mention.evidence))


def _master_variance(stock: StockAnalysis) -> float:
    scores = [float(item.score) for item in stock.master_opinions]
    if not scores:
        return 0.0
    high = max(scores)
    low = min(scores)
    return round((high - low) / 2.0, 2)
