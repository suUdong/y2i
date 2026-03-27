from __future__ import annotations

import html
import logging
from typing import Any, Sequence

from .app_config import NotificationConfig
from .notifications import send_telegram_message
from .reporting import format_number

logger = logging.getLogger(__name__)

DEFAULT_MIN_SCORE = 68.0
DEFAULT_MIN_CHANNEL_QUALITY = 50.0
MAX_SIGNALS_PER_MESSAGE = 10


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
) -> str:
    """Format high-quality signals into a Korean-language Telegram alert with HTML.

    Returns HTML-formatted message string.
    """
    if not signals:
        return ""

    lines = [
        "<b>🔔 Y2I 고품질 시그널 알림</b>",
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

    lines.append(f"<i>총 {len(signals)}개 시그널 | aggregate_score ≥ {DEFAULT_MIN_SCORE}</i>")
    return "\n".join(lines)


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


def format_analysis_summary(
    new_videos: dict[str, list[str]],
    trigger: str = "",
    top_signals: Sequence[dict[str, Any]] | None = None,
    channel_names: dict[str, str] | None = None,
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

    if top_signals:
        lines.append("")
        lines.append("<b>🏆 주요 시그널</b>")
        for sig in top_signals[:5]:
            name = html.escape(sig.get("company_name") or sig.get("ticker", ""))
            score = float(sig.get("aggregate_score", 0))
            verdict = sig.get("aggregate_verdict", "")
            lines.append(f"  • {name} — {verdict} ({score:.1f})")

    return "\n".join(lines)


def send_analysis_summary_alert(
    config: NotificationConfig,
    new_videos: dict[str, list[str]],
    trigger: str = "",
    top_signals: Sequence[dict[str, Any]] | None = None,
    channel_names: dict[str, str] | None = None,
) -> bool:
    """Send an analysis-completion summary via Telegram.

    Returns True if the message was sent.
    """
    if not new_videos:
        return False
    message = format_analysis_summary(
        new_videos,
        trigger=trigger,
        top_signals=top_signals,
        channel_names=channel_names,
    )
    return _send_telegram_html(config, message)
