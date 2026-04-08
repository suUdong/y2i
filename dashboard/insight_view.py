"""Insight-first view models for the simplified dashboard."""
from __future__ import annotations

from typing import Any

from dashboard.data_loader import (
    first_non_empty,
    format_price,
    format_ticker_display,
    translate_signal_class,
    translate_video_type,
)

MASTER_ORDER = ("druckenmiller", "buffett", "soros")


def build_header_metrics(
    *,
    overview: dict[str, Any],
    comparison: dict[str, Any],
    live_feed: dict[str, Any],
) -> list[dict[str, str]]:
    pipeline_summary = comparison.get("pipeline_summary", {}) if isinstance(comparison, dict) else {}
    signal_accuracy = comparison.get("signal_accuracy", {}) if isinstance(comparison, dict) else {}
    overall_accuracy = signal_accuracy.get("overall", {}) if isinstance(signal_accuracy, dict) else {}
    consensus_signals = comparison.get("consensus_signals", []) if isinstance(comparison, dict) else []
    return [
        {"label": "Last Update", "value": str(live_feed.get("last_update") or "-")},
        {"label": "Channels", "value": str(int(pipeline_summary.get("total_channels", overview.get("channel_count", 0)) or 0))},
        {"label": "Actionable Videos", "value": str(int(pipeline_summary.get("actionable_videos", overview.get("analyzable_count", 0)) or 0))},
        {"label": "Tracked Signals", "value": str(int(overall_accuracy.get("total_signals", 0) or 0))},
        {"label": "Consensus Signals", "value": str(len(consensus_signals) if isinstance(consensus_signals, list) else 0)},
    ]


def build_priority_signal_rows(rankings: list[dict[str, Any]], *, limit: int = 8) -> list[dict[str, Any]]:
    if not rankings:
        return []
    sorted_rankings = sorted(
        rankings,
        key=lambda item: (
            int(bool(item.get("consensus_signal"))),
            float(item.get("aggregate_score", 0) or 0),
            int(item.get("channel_count", 0) or 0),
        ),
        reverse=True,
    )
    rows: list[dict[str, Any]] = []
    for item in sorted_rankings[:limit]:
        rows.append(
            {
                "ticker": format_ticker_display(str(item.get("ticker", "")), str(item.get("company_name") or "")),
                "score": round(float(item.get("aggregate_score", 0) or 0), 1),
                "verdict": str(item.get("aggregate_verdict") or "-"),
                "channels": ", ".join(item.get("_source_channels_display", [])[:3]) or str(item.get("channel_count", 0) or 0),
                "signal_type": "CONSENSUS" if item.get("consensus_signal") else str(item.get("signal_kind") or "SINGLE_SOURCE"),
                "why_now": _ranking_why_now(item),
            }
        )
    return rows


def build_recent_video_rows(
    recent_videos: list[dict[str, Any]],
    channel_names: dict[str, str],
    *,
    limit: int = 12,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for video in recent_videos[:limit]:
        rows.append(
            {
                "channel": channel_names.get(str(video.get("_channel", "")), str(video.get("_channel", ""))),
                "title": str(video.get("title", "")),
                "signal": translate_signal_class(str(video.get("video_signal_class", "UNKNOWN"))),
                "score": round(float(video.get("signal_score", 0) or 0), 1),
                "video_type": translate_video_type(str(video.get("video_type", "OTHER"))),
                "why": first_non_empty(
                    video.get("video_summary"),
                    video.get("skip_reason"),
                    video.get("reason"),
                ),
            }
        )
    return rows


def build_channel_focus_payload(slug: str, data_30d: dict[str, Any]) -> dict[str, Any]:
    videos = list(data_30d.get("videos", []) or [])
    ranking = list(data_30d.get("cross_video_ranking", []) or [])
    stock_cards = build_stock_insight_cards(ranking, videos=videos, limit=6)
    top_stocks = [
        {
            "ticker": format_ticker_display(str(item.get("ticker", "")), str(item.get("company_name") or "")),
            "score": round(float(item.get("aggregate_score", 0) or 0), 1),
            "verdict": str(item.get("aggregate_verdict") or "-"),
            "why": _ranking_why_now(item),
        }
        for item in ranking[:8]
    ]

    macro_points: list[dict[str, Any]] = []
    seen_macro: set[tuple[str, str]] = set()
    for video in videos:
        for insight in video.get("macro_insights", []) or []:
            key = (str(insight.get("indicator", "")), str(insight.get("direction", "")))
            if key in seen_macro:
                continue
            seen_macro.add(key)
            macro_points.append(
                {
                    "label": str(insight.get("label") or insight.get("indicator") or "-"),
                    "direction": str(insight.get("direction") or "-"),
                    "confidence": float(insight.get("confidence", 0) or 0),
                    "source_video": str(video.get("title") or ""),
                }
            )
            if len(macro_points) >= 8:
                break
        if len(macro_points) >= 8:
            break

    expert_points: list[dict[str, Any]] = []
    for video in videos:
        for expert in video.get("expert_insights", []) or []:
            summary = first_non_empty(
                expert.get("summary"),
                *(claim.get("claim") for claim in expert.get("structured_claims", [])[:1] if isinstance(claim, dict)),
                *(expert.get("key_claims", [])[:1] if isinstance(expert.get("key_claims", []), list) else []),
            )
            expert_points.append(
                {
                    "expert": str(expert.get("expert_name") or "-"),
                    "topic": str(expert.get("topic") or "-"),
                    "summary": summary,
                    "source_video": str(video.get("title") or ""),
                }
            )
            if len(expert_points) >= 8:
                break
        if len(expert_points) >= 8:
            break

    recent_videos = [
        {
            "title": str(video.get("title") or ""),
            "signal": translate_signal_class(str(video.get("video_signal_class", "UNKNOWN"))),
            "score": round(float(video.get("signal_score", 0) or 0), 1),
            "why": first_non_empty(video.get("video_summary"), video.get("skip_reason"), video.get("reason")),
        }
        for video in videos[:10]
    ]

    return {
        "slug": slug,
        "channel_name": str(data_30d.get("channel_name") or slug),
        "video_count": len(videos),
        "top_stocks": top_stocks,
        "stock_cards": stock_cards,
        "macro_points": macro_points,
        "expert_points": expert_points,
        "recent_videos": recent_videos,
    }


def build_stock_insight_cards(
    rankings: list[dict[str, Any]],
    *,
    videos: list[dict[str, Any]] | None = None,
    limit: int = 6,
) -> list[dict[str, Any]]:
    cards: list[dict[str, Any]] = []
    for item in rankings[:limit]:
        summaries = _sorted_source_summaries(item)
        lead_summary = summaries[0] if summaries else {}
        fundamentals = _best_fundamentals(summaries)
        ticker = str(item.get("ticker", ""))
        company_name = str(item.get("company_name") or fundamentals.get("company_name") or "")
        price_target = item.get("price_target") if isinstance(item.get("price_target"), dict) else {}
        cards.append(
            {
                "ticker": ticker,
                "ticker_display": format_ticker_display(ticker, company_name),
                "company_name": company_name,
                "score": round(float(item.get("aggregate_score", 0) or 0), 1),
                "verdict": str(item.get("aggregate_verdict") or "-"),
                "conviction": _conviction_label(item),
                "signal_kind": str(item.get("signal_kind") or ("CONSENSUS" if item.get("consensus_signal") else "SINGLE_SOURCE")),
                "signal_note": _signal_note(item),
                "channel_count": int(item.get("channel_count", 1) or 1),
                "source_channels": list(item.get("_source_channels_display", []) or []),
                "why_now": _ranking_why_now(item),
                "plain_summary": first_non_empty(
                    lead_summary.get("plain_summary"),
                    lead_summary.get("signal_summary"),
                ),
                "video_context_summary": first_non_empty(
                    lead_summary.get("video_context_summary"),
                    lead_summary.get("thesis_summary"),
                ),
                "supporting_videos": _supporting_videos(summaries),
                "expert_views": _collect_expert_views(
                    ticker,
                    summaries=summaries,
                    videos=list(videos or []),
                ),
                "master_views": _collect_master_views(summaries),
                "analyst_summary": _analyst_summary(fundamentals),
                "price_target_summary": _price_target_summary(price_target, fundamentals),
                "fundamental_rows": _fundamental_rows(fundamentals),
            }
        )
    return cards


def _ranking_why_now(item: dict[str, Any]) -> str:
    for summary in item.get("source_video_summaries", []) or []:
        if not isinstance(summary, dict):
            continue
        value = first_non_empty(
            summary.get("video_context_summary"),
            summary.get("plain_summary"),
            summary.get("thesis_summary"),
            summary.get("signal_summary"),
        )
        if value:
            return str(value)
    return first_non_empty(
        str(item.get("consensus_strength") or ""),
        str(item.get("cross_validation_status") or ""),
        str(item.get("aggregate_verdict") or ""),
    )


def _sorted_source_summaries(item: dict[str, Any]) -> list[dict[str, Any]]:
    summaries = [summary for summary in item.get("source_video_summaries", []) or [] if isinstance(summary, dict)]
    return sorted(
        summaries,
        key=lambda summary: float(summary.get("final_score", 0) or 0),
        reverse=True,
    )


def _best_fundamentals(summaries: list[dict[str, Any]]) -> dict[str, Any]:
    for summary in summaries:
        fundamentals = summary.get("fundamentals", {})
        if isinstance(fundamentals, dict) and fundamentals:
            return fundamentals
    return {}


def _supporting_videos(summaries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for summary in summaries[:3]:
        rows.append(
            {
                "title": str(summary.get("video_title") or "-"),
                "verdict": str(summary.get("verdict") or "-"),
                "score": round(float(summary.get("final_score", 0) or 0), 1),
                "summary": first_non_empty(
                    summary.get("video_context_summary"),
                    summary.get("plain_summary"),
                    summary.get("thesis_summary"),
                    summary.get("signal_summary"),
                ),
            }
        )
    return rows


def _collect_master_views(summaries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    best_by_master: dict[str, dict[str, Any]] = {}
    for summary in summaries:
        for opinion in summary.get("master_opinions", []) or []:
            if not isinstance(opinion, dict):
                continue
            master = str(opinion.get("master") or "")
            if not master:
                continue
            score = float(opinion.get("score", 0) or 0)
            current = best_by_master.get(master)
            if current is None or score > float(current.get("score", 0) or 0):
                best_by_master[master] = {
                    "master": master,
                    "verdict": str(opinion.get("verdict") or "-"),
                    "score": round(score, 1),
                    "one_liner": str(opinion.get("one_liner") or ""),
                }
    return sorted(
        best_by_master.values(),
        key=lambda item: (
            MASTER_ORDER.index(item["master"]) if item["master"] in MASTER_ORDER else len(MASTER_ORDER),
            item["master"],
        ),
    )


def _collect_expert_views(
    ticker: str,
    *,
    summaries: list[dict[str, Any]],
    videos: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not videos:
        return []
    source_titles = {str(summary.get("video_title") or "") for summary in summaries if summary.get("video_title")}
    views: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    normalized_ticker = ticker.upper()
    for video in videos:
        video_title = str(video.get("title") or "")
        related_video = video_title in source_titles or any(
            str(stock.get("ticker") or "").upper() == normalized_ticker
            for stock in video.get("stocks", []) or []
            if isinstance(stock, dict)
        )
        for expert in video.get("expert_insights", []) or []:
            if not isinstance(expert, dict):
                continue
            mentioned = {
                str(value or "").upper()
                for value in expert.get("mentioned_tickers", []) or []
            }
            if normalized_ticker not in mentioned and not related_video:
                continue
            key = (str(expert.get("expert_name") or ""), video_title)
            if key in seen:
                continue
            seen.add(key)
            structured_claims = [
                {
                    "claim": str(claim.get("claim") or ""),
                    "direction": str(claim.get("direction") or "NEUTRAL"),
                    "confidence": float(claim.get("confidence", 0) or 0),
                    "reasoning": str(claim.get("reasoning") or ""),
                }
                for claim in expert.get("structured_claims", [])[:2]
                if isinstance(claim, dict)
            ]
            views.append(
                {
                    "expert": str(expert.get("expert_name") or "-"),
                    "topic": str(expert.get("topic") or "-"),
                    "sentiment": str(expert.get("sentiment") or "NEUTRAL"),
                    "summary": first_non_empty(
                        expert.get("summary"),
                        *(claim["claim"] for claim in structured_claims if claim.get("claim")),
                        *(expert.get("key_claims", [])[:1] if isinstance(expert.get("key_claims", []), list) else []),
                    ),
                    "claims": structured_claims,
                    "source_video": video_title,
                }
            )
    return views[:3]


def _fundamental_rows(fundamentals: dict[str, Any]) -> list[dict[str, str]]:
    if not fundamentals:
        return []
    return [
        {"label": "현재가", "value": format_price(_as_float(fundamentals.get("current_price")), str(fundamentals.get("currency") or "USD"))},
        {"label": "Forward PE", "value": _format_ratio(fundamentals.get("forward_pe"))},
        {"label": "매출성장", "value": _format_pct(fundamentals.get("revenue_growth"))},
        {"label": "영업마진", "value": _format_pct(fundamentals.get("operating_margin"))},
        {"label": "ROE", "value": _format_pct(fundamentals.get("return_on_equity"))},
        {"label": "부채비율", "value": _format_ratio(fundamentals.get("debt_to_equity"))},
    ]


def _analyst_summary(fundamentals: dict[str, Any]) -> str:
    if not fundamentals:
        return ""
    recommendation = str(fundamentals.get("recommendation_key") or "").strip()
    analyst_count = fundamentals.get("analyst_count")
    parts: list[str] = []
    if recommendation:
        parts.append(_translate_recommendation(recommendation))
    if analyst_count:
        parts.append(f"애널리스트 {int(analyst_count)}명")
    distribution = _analyst_distribution_summary(fundamentals)
    if distribution:
        parts.append(distribution)
    return " · ".join(parts)


def _price_target_summary(price_target: dict[str, Any], fundamentals: dict[str, Any]) -> str:
    target_price = price_target.get("target_price")
    currency = str(price_target.get("currency") or fundamentals.get("currency") or "USD")
    if target_price is None:
        target_price = fundamentals.get("target_median_price") or fundamentals.get("target_mean_price")
    if target_price is None:
        return ""
    current_price = _as_float(fundamentals.get("current_price"))
    delta_pct = None
    if current_price not in {None, 0}:
        delta_pct = ((float(target_price) - current_price) / current_price) * 100.0
    delta_label = f" ({delta_pct:+.1f}%)" if delta_pct is not None else ""
    return f"{format_price(float(target_price), currency)}{delta_label}"


def _conviction_label(item: dict[str, Any]) -> str:
    score = float(item.get("aggregate_score", 0) or 0)
    if item.get("consensus_signal") or score >= 82:
        return "HIGH"
    if score >= 68:
        return "MEDIUM"
    return "WATCH"


def _signal_note(item: dict[str, Any]) -> str:
    channel_count = int(item.get("channel_count", 1) or 1)
    if item.get("consensus_signal"):
        return f"{channel_count}채널 {str(item.get('consensus_strength') or 'CONSENSUS')}"
    if channel_count > 1:
        return f"{channel_count}채널 MIXED"
    return "SINGLE SOURCE"


def _translate_recommendation(value: str) -> str:
    key = value.strip().lower()
    return {
        "strong_buy": "강한 매수",
        "buy": "매수 우위",
        "hold": "중립",
        "underperform": "비중축소",
        "sell": "매도",
    }.get(key, value.upper())


def _analyst_distribution_summary(fundamentals: dict[str, Any]) -> str:
    counts = [
        ("SB", int(fundamentals.get("analyst_strong_buy", 0) or 0)),
        ("B", int(fundamentals.get("analyst_buy", 0) or 0)),
        ("H", int(fundamentals.get("analyst_hold", 0) or 0)),
        ("S", int(fundamentals.get("analyst_sell", 0) or 0)),
        ("SS", int(fundamentals.get("analyst_strong_sell", 0) or 0)),
    ]
    counts = [(label, count) for label, count in counts if count]
    return " / ".join(f"{label} {count}" for label, count in counts)


def _format_pct(value: Any) -> str:
    numeric = _as_float(value)
    return "-" if numeric is None else f"{numeric * 100:.1f}%"


def _format_ratio(value: Any) -> str:
    numeric = _as_float(value)
    return "-" if numeric is None else f"{numeric:.1f}"


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
