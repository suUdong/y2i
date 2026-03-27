from __future__ import annotations

import html
import logging
from typing import Any, Sequence

from .app_config import NotificationConfig
from .notifications import send_telegram_message
from .reporting import format_number

logger = logging.getLogger(__name__)

DEFAULT_MIN_SCORE = 68.0
DEFAULT_HIGH_CONFIDENCE_MIN_SCORE = 82.0
DEFAULT_CONSENSUS_MIN_SCORE = 78.0
DEFAULT_CONSENSUS_MIN_CROSS_VALIDATION = 65.0
DEFAULT_MIN_CHANNEL_QUALITY = 50.0
MAX_SIGNALS_PER_MESSAGE = 10
MAX_CHANNEL_SUMMARY_SIGNALS = 3
MAX_LEADERBOARD_ROWS = 5
HIGH_CONFIDENCE_VERDICTS = {"BUY", "STRONG_BUY"}
CONSENSUS_STATUS_LABELS = {
    "SINGLE_SOURCE": "단일 채널",
    "CONFIRMED": "교차검증 완료",
    "MIXED": "부분 일치",
    "DIVERGENT": "의견 분산",
}
CONSENSUS_STRENGTH_LABELS = {
    "SINGLE_SOURCE": "단일 출처",
    "WEAK": "약한 합의",
    "MODERATE": "중간 합의",
    "STRONG": "강한 합의",
}


def filter_high_quality_signals(
    ranked_stocks: list[dict[str, Any]],
    channel_quality_scores: dict[str, float] | None = None,
    channel_slug: str = "",
    min_score: float = DEFAULT_MIN_SCORE,
    min_channel_quality: float = DEFAULT_MIN_CHANNEL_QUALITY,
) -> list[dict[str, Any]]:
    """Filter ranked stocks to only high-quality actionable signals.

    Args:
        ranked_stocks: List of RankedStock dicts from cross_video_ranking.
        channel_quality_scores: Dict mapping channel_slug -> overall_quality_score.
        channel_slug: The source channel for these stocks.
        min_score: Minimum aggregate_score threshold.
        min_channel_quality: Minimum channel quality threshold.

    Returns:
        Filtered list of stock dicts meeting quality thresholds.
    """
    channel_quality_scores = channel_quality_scores or {}

    # Check channel quality gate
    if channel_slug and channel_quality_scores:
        channel_quality = channel_quality_scores.get(channel_slug, 0)
        if channel_quality < min_channel_quality:
            logger.info(
                "Channel %s quality %.1f below threshold %.1f, skipping alerts",
                channel_slug, channel_quality, min_channel_quality,
            )
            return []

    return [
        stock for stock in ranked_stocks
        if float(stock.get("aggregate_score", 0)) >= min_score
    ]


def format_telegram_alert(
    signals: list[dict[str, Any]],
    channel_name: str = "",
    channel_slug: str = "",
    title: str = "🔔 Y2I 고품질 시그널 알림",
    footer_threshold: float = DEFAULT_MIN_SCORE,
) -> str:
    """Format high-quality signals into a Korean-language Telegram alert with HTML.

    Returns HTML-formatted message string.
    """
    if not signals:
        return ""

    lines = [
        f"<b>{html.escape(title)}</b>",
        "",
    ]
    if channel_name:
        lines.append(f"📺 채널: <b>{html.escape(channel_name)}</b>")
        lines.append("")

    for idx, stock in enumerate(signals[:MAX_SIGNALS_PER_MESSAGE], start=1):
        ticker = html.escape(stock.get("ticker", ""))
        name = html.escape(stock.get("company_name") or "unknown")
        score = float(stock.get("aggregate_score", 0))
        verdict = stock.get("aggregate_verdict", "")
        price = stock.get("latest_price")
        currency = stock.get("currency")
        appearances = int(stock.get("appearances", 0))
        mentions = int(stock.get("total_mentions", 0))

        # Verdict emoji
        verdict_emoji = {"STRONG_BUY": "🟢", "BUY": "🔵", "WATCH": "🟡"}.get(verdict, "⚪")

        lines.append(f"<b>{idx}. {name}</b> ({ticker})")
        lines.append(f"   {verdict_emoji} {verdict} | 점수: <b>{score:.1f}</b>")
        if price is not None:
            lines.append(f"   💰 현재가: {format_number(price, currency)}")
        lines.append(f"   📊 등장: {appearances}회 | 언급: {mentions}회")

        # Master opinion summary from source videos if available
        master_opinions = stock.get("master_opinions", [])
        if master_opinions:
            for op in master_opinions[:2]:
                master = op.get("master", "")
                one_liner = op.get("one_liner", "")
                if master and one_liner:
                    lines.append(f"   🎯 {html.escape(master)}: <i>{html.escape(one_liner)}</i>")

        lines.append("")

    lines.append(f"<i>총 {len(signals)}개 시그널 | aggregate_score ≥ {footer_threshold:g}</i>")
    return "\n".join(lines)


def summarize_signal(stock: dict[str, Any]) -> str:
    """Build a compact plain-text summary for one ranked stock signal."""
    verdict = str(stock.get("aggregate_verdict", "")).strip() or "N/A"
    score = float(stock.get("aggregate_score", 0))
    mentions = int(stock.get("total_mentions", 0))
    appearances = int(stock.get("appearances", 0))
    summary_parts = [verdict, f"점수 {score:.1f}"]
    if mentions:
        summary_parts.append(f"언급 {mentions}회")
    elif appearances:
        summary_parts.append(f"등장 {appearances}회")

    master_opinions = stock.get("master_opinions", [])
    if master_opinions:
        lead = master_opinions[0]
        master = str(lead.get("master", "")).strip()
        one_liner = str(lead.get("one_liner", "")).strip()
        if master and one_liner:
            summary_parts.append(f"{master}: {one_liner}")
        elif one_liner:
            summary_parts.append(one_liner)

    return " | ".join(summary_parts)


def build_channel_signal_summary(
    ranked_stocks: Sequence[dict[str, Any]],
    *,
    channel_slug: str = "",
    channel_name: str = "",
    limit: int = MAX_CHANNEL_SUMMARY_SIGNALS,
) -> dict[str, Any]:
    """Build a compact per-channel signal summary for downstream notifications."""
    signals: list[dict[str, Any]] = []
    for stock in ranked_stocks[:limit]:
        ticker = str(stock.get("ticker", "")).strip()
        if not ticker:
            continue
        signals.append(
            {
                "ticker": ticker,
                "company_name": stock.get("company_name"),
                "aggregate_score": float(stock.get("aggregate_score", 0)),
                "aggregate_verdict": stock.get("aggregate_verdict", ""),
                "signal_summary": summarize_signal(stock),
            }
        )
    return {
        "channel_slug": channel_slug,
        "channel_name": channel_name or channel_slug,
        "signals": signals,
    }


def filter_high_confidence_signals(
    ranked_stocks: list[dict[str, Any]],
    channel_quality_scores: dict[str, float] | None = None,
    channel_slug: str = "",
    min_score: float = DEFAULT_HIGH_CONFIDENCE_MIN_SCORE,
    min_channel_quality: float = DEFAULT_MIN_CHANNEL_QUALITY,
    allowed_verdicts: set[str] | None = None,
) -> list[dict[str, Any]]:
    """Return only the strongest actionable signals for a separate alert lane."""
    allowed_verdicts = allowed_verdicts or HIGH_CONFIDENCE_VERDICTS
    filtered = filter_high_quality_signals(
        ranked_stocks,
        channel_quality_scores=channel_quality_scores,
        channel_slug=channel_slug,
        min_score=min_score,
        min_channel_quality=min_channel_quality,
    )
    return [
        stock for stock in filtered
        if str(stock.get("aggregate_verdict", "")).upper() in allowed_verdicts
    ]


def filter_consensus_signals(
    ranked_stocks: Sequence[dict[str, Any]],
    *,
    min_score: float = DEFAULT_CONSENSUS_MIN_SCORE,
    min_cross_validation_score: float = DEFAULT_CONSENSUS_MIN_CROSS_VALIDATION,
    min_channel_count: int = 2,
) -> list[dict[str, Any]]:
    """Return only explicit multi-channel consensus signals."""
    signals: list[dict[str, Any]] = []
    for stock in ranked_stocks:
        if int(stock.get("channel_count", 0) or 0) < min_channel_count:
            continue
        if float(stock.get("aggregate_score", 0) or 0) < min_score:
            continue
        if float(stock.get("cross_validation_score", 0) or 0) < min_cross_validation_score:
            continue
        signals.append(stock)
    return signals


def send_signal_alerts(
    config: NotificationConfig,
    ranked_stocks: list[dict[str, Any]],
    channel_name: str = "",
    channel_slug: str = "",
    channel_quality_scores: dict[str, float] | None = None,
    min_score: float = DEFAULT_MIN_SCORE,
    min_channel_quality: float = DEFAULT_MIN_CHANNEL_QUALITY,
    weight_multipliers: dict[str, float] | None = None,
) -> bool:
    """Filter, format, and send high-quality signal alerts via Telegram.

    Args:
        weight_multipliers: Per-channel multiplier from leaderboard ranking.
            Effective threshold = min_channel_quality / multiplier, so
            high-quality channels (multiplier > 1) pass more easily.

    Returns True if message was sent successfully.
    """
    effective_min_quality = min_channel_quality
    if weight_multipliers and channel_slug:
        multiplier = weight_multipliers.get(channel_slug, 1.0)
        if multiplier > 0:
            effective_min_quality = min_channel_quality / multiplier

    signals = filter_high_quality_signals(
        ranked_stocks,
        channel_quality_scores=channel_quality_scores,
        channel_slug=channel_slug,
        min_score=min_score,
        min_channel_quality=effective_min_quality,
    )
    if not signals:
        logger.info("No high-quality signals to alert for %s", channel_slug or "all channels")
        return False

    message = format_telegram_alert(signals, channel_name=channel_name, channel_slug=channel_slug)
    if not message:
        return False

    return _send_telegram_html(config, message)


def send_high_confidence_signal_alerts(
    config: NotificationConfig,
    ranked_stocks: list[dict[str, Any]],
    channel_name: str = "",
    channel_slug: str = "",
    channel_quality_scores: dict[str, float] | None = None,
    min_score: float = DEFAULT_HIGH_CONFIDENCE_MIN_SCORE,
    min_channel_quality: float = DEFAULT_MIN_CHANNEL_QUALITY,
    weight_multipliers: dict[str, float] | None = None,
) -> bool:
    """Send a stricter high-confidence-only signal alert via Telegram."""
    effective_min_quality = min_channel_quality
    if weight_multipliers and channel_slug:
        multiplier = weight_multipliers.get(channel_slug, 1.0)
        if multiplier > 0:
            effective_min_quality = min_channel_quality / multiplier

    signals = filter_high_confidence_signals(
        ranked_stocks,
        channel_quality_scores=channel_quality_scores,
        channel_slug=channel_slug,
        min_score=min_score,
        min_channel_quality=effective_min_quality,
    )
    if not signals:
        logger.info("No high-confidence signals to alert for %s", channel_slug or "all channels")
        return False

    message = format_telegram_alert(
        signals,
        channel_name=channel_name,
        channel_slug=channel_slug,
        title="🚨 Y2I 고신뢰 시그널 알림",
        footer_threshold=min_score,
    )
    if not message:
        return False
    return _send_telegram_html(config, message)


def _send_telegram_html(config: NotificationConfig, text: str) -> bool:
    """Send HTML-formatted Telegram message."""
    if not config.telegram_bot_token or not config.telegram_chat_id:
        logger.info("Telegram alert skipped: credentials missing")
        return False
    import requests
    url = f"https://api.telegram.org/bot{config.telegram_bot_token}/sendMessage"
    try:
        response = requests.post(
            url,
            data={
                "chat_id": config.telegram_chat_id,
                "text": text,
                "parse_mode": "HTML",
            },
            timeout=10,
        )
        body = response.json()
        return bool(body.get("ok"))
    except Exception as exc:
        logger.warning("Telegram HTML alert failed: %s", exc)
        return False


def format_consensus_telegram_alert(
    signals: Sequence[dict[str, Any]],
    *,
    title: str = "🤝 Y2I 합의 시그널 알림",
    footer_threshold: float = DEFAULT_CONSENSUS_MIN_SCORE,
) -> str:
    """Format a dedicated consensus-signal Telegram alert."""
    if not signals:
        return ""

    lines = [
        f"<b>{html.escape(title)}</b>",
        "",
    ]
    for idx, stock in enumerate(signals[:MAX_SIGNALS_PER_MESSAGE], start=1):
        label = stock.get("company_name") or stock.get("ticker", "")
        ticker = stock.get("ticker", "")
        score = float(stock.get("aggregate_score", 0) or 0)
        channel_count = int(stock.get("channel_count", 0) or 0)
        xval_score = float(stock.get("cross_validation_score", 0) or 0)
        strength = CONSENSUS_STRENGTH_LABELS.get(str(stock.get("consensus_strength", "")), str(stock.get("consensus_strength", "")))
        status = CONSENSUS_STATUS_LABELS.get(str(stock.get("cross_validation_status", "")), str(stock.get("cross_validation_status", "")))
        channels = ", ".join(str(item) for item in stock.get("_source_channels_display", [])[:3] if item)
        if ticker and label and ticker != label:
            heading = f"{label} ({ticker})"
        else:
            heading = label or ticker

        lines.append(f"<b>{idx}. {html.escape(str(heading))}</b>")
        lines.append(
            f"   🤝 {channel_count}개 채널 | 점수 <b>{score:.1f}</b> | "
            f"{html.escape(str(strength))} | {html.escape(str(status))} | XVAL <b>{xval_score:.1f}</b>"
        )
        if channels:
            lines.append(f"   📺 {html.escape(channels)}")
        lines.append("")

    lines.append(f"<i>총 {len(signals)}개 합의 시그널 | aggregate_score ≥ {footer_threshold:g}</i>")
    return "\n".join(lines)


def format_analysis_summary(
    new_videos: dict[str, list[str]],
    trigger: str = "",
    top_signals: Sequence[dict[str, Any]] | None = None,
    channel_names: dict[str, str] | None = None,
    channel_signal_summaries: Sequence[dict[str, Any]] | None = None,
) -> str:
    """Format an analysis-completion summary as an HTML Telegram message."""
    channel_names = channel_names or {}
    total_videos = sum(len(ids) for ids in new_videos.values())
    lines = [
        "<b>📡 Y2I 분석 완료</b>",
        "",
        f"트리거: <b>{html.escape(trigger or 'manual')}</b>",
        f"새 영상: <b>{total_videos}</b>개",
        "",
    ]
    for slug, video_ids in new_videos.items():
        display = html.escape(channel_names.get(slug, slug))
        lines.append(f"  📺 {display}: {len(video_ids)}개")

    if channel_signal_summaries:
        lines.append("")
        lines.append("<b>📺 채널별 핵심 시그널</b>")
        for item in channel_signal_summaries[:MAX_LEADERBOARD_ROWS]:
            slug = str(item.get("channel_slug", ""))
            display_name = str(item.get("channel_name") or channel_names.get(slug, slug)).strip() or slug
            lines.append(f"  📺 <b>{html.escape(display_name)}</b>")
            signals = item.get("signals", [])
            if not signals:
                lines.append("    • 상위 시그널 없음")
                continue
            for sig in signals[:MAX_CHANNEL_SUMMARY_SIGNALS]:
                label = sig.get("company_name") or sig.get("ticker", "")
                ticker = sig.get("ticker", "")
                summary = sig.get("signal_summary") or summarize_signal(sig)
                if ticker and label and ticker != label:
                    name = f"{label} ({ticker})"
                else:
                    name = label or ticker
                lines.append(f"    • {html.escape(str(name))} — {html.escape(str(summary))}")

    if top_signals:
        lines.append("")
        header = "<b>🤝 합의 시그널</b>" if any(bool(sig.get("consensus_signal")) for sig in top_signals) else "<b>🏆 주요 시그널</b>"
        lines.append(header)
        for sig in top_signals[:5]:
            name = html.escape(sig.get("company_name") or sig.get("ticker", ""))
            score = float(sig.get("aggregate_score", 0))
            verdict = sig.get("aggregate_verdict", "")
            channel_count = int(sig.get("channel_count", 0) or 0)
            source_channels = sig.get("_source_channels_display", [])
            support_label = ""
            if channel_count > 1:
                support_label = f" | {channel_count}개 채널 합의"
            elif source_channels:
                support_label = f" | {len(source_channels)}개 채널"
            support_channels = ""
            if source_channels:
                joined = ", ".join(str(item) for item in source_channels[:3] if item)
                if joined:
                    suffix = f" 외 {channel_count - 3}" if channel_count > 3 else ""
                    support_channels = f" [{html.escape(joined + suffix)}]"
            consensus_meta = ""
            if sig.get("consensus_signal"):
                status = CONSENSUS_STATUS_LABELS.get(str(sig.get("cross_validation_status", "")), str(sig.get("cross_validation_status", "")))
                strength = CONSENSUS_STRENGTH_LABELS.get(str(sig.get("consensus_strength", "")), str(sig.get("consensus_strength", "")))
                xval = float(sig.get("cross_validation_score", 0) or 0)
                consensus_meta = f" | {html.escape(str(strength))} | {html.escape(str(status))} | XVAL {xval:.1f}"
            lines.append(f"  • {name}{support_channels} — {verdict} ({score:.1f}{support_label}{consensus_meta})")

    return "\n".join(lines)


def format_daily_leaderboard_summary(
    leaderboard: Sequence[dict[str, Any]],
    *,
    generated_at: str = "",
) -> str:
    """Format the daily channel leaderboard summary as Telegram HTML."""
    if not leaderboard:
        return ""

    lines = [
        "<b>🏅 Y2I 일일 채널 리더보드</b>",
        "",
    ]
    if generated_at:
        lines.append(f"기준 run: <b>{html.escape(generated_at)}</b>")
        lines.append("")

    for idx, item in enumerate(leaderboard[:MAX_LEADERBOARD_ROWS], start=1):
        display = html.escape(str(item.get("display_name") or item.get("slug", "-")))
        quality = item.get("overall_quality_score")
        weight_multiplier = item.get("weight_multiplier")
        hit_rate_3d = item.get("hit_rate_3d")
        hit_rate = item.get("hit_rate_5d")
        avg_return = item.get("avg_return_5d")
        actionable_ratio = item.get("actionable_ratio")
        parts = []
        if quality is not None:
            parts.append(f"품질 {float(quality):.1f}")
        if weight_multiplier is not None:
            parts.append(f"가중치 {float(weight_multiplier):.2f}x")
        if hit_rate_3d is not None:
            parts.append(f"3d 적중률 {float(hit_rate_3d):.1f}%")
        if hit_rate is not None:
            parts.append(f"5d 적중률 {float(hit_rate):.1f}%")
        if avg_return is not None:
            parts.append(f"5d 평균 {float(avg_return):.2f}%")
        if actionable_ratio is not None:
            parts.append(f"액션비율 {float(actionable_ratio):.0%}")
        lines.append(f"<b>{idx}. {display}</b>")
        lines.append(f"   {' | '.join(parts) if parts else '상세 지표 없음'}")

    return "\n".join(lines)


def send_analysis_summary_alert(
    config: NotificationConfig,
    new_videos: dict[str, list[str]],
    trigger: str = "",
    top_signals: Sequence[dict[str, Any]] | None = None,
    channel_names: dict[str, str] | None = None,
    channel_signal_summaries: Sequence[dict[str, Any]] | None = None,
) -> bool:
    """Send an analysis-completion summary via Telegram.

    Returns True if the message was sent.
    """
    if not new_videos and not top_signals and not channel_signal_summaries:
        return False
    message = format_analysis_summary(
        new_videos,
        trigger=trigger,
        top_signals=top_signals,
        channel_names=channel_names,
        channel_signal_summaries=channel_signal_summaries,
    )
    return _send_telegram_html(config, message)


def send_consensus_signal_alerts(
    config: NotificationConfig,
    ranked_stocks: Sequence[dict[str, Any]],
    *,
    min_score: float = DEFAULT_CONSENSUS_MIN_SCORE,
    min_cross_validation_score: float = DEFAULT_CONSENSUS_MIN_CROSS_VALIDATION,
) -> bool:
    """Send a dedicated consensus-signal Telegram alert when strong agreement exists."""
    signals = filter_consensus_signals(
        ranked_stocks,
        min_score=min_score,
        min_cross_validation_score=min_cross_validation_score,
    )
    if not signals:
        logger.info("No consensus signals to alert")
        return False
    message = format_consensus_telegram_alert(signals, footer_threshold=min_score)
    if not message:
        return False
    return _send_telegram_html(config, message)


def send_daily_leaderboard_alert(
    config: NotificationConfig,
    leaderboard: Sequence[dict[str, Any]],
    *,
    generated_at: str = "",
) -> bool:
    """Send the daily channel leaderboard summary via Telegram."""
    message = format_daily_leaderboard_summary(leaderboard, generated_at=generated_at)
    if not message:
        return False
    return _send_telegram_html(config, message)
