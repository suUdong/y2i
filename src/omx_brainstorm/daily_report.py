from __future__ import annotations

import html
from pathlib import Path
from typing import Any

from .comparison import RunContext
from .utils import ensure_dir


def build_daily_report_payload(
    channel_payloads: dict[str, dict[str, Any]],
    comparison: dict[str, Any],
    leaderboard: list[dict[str, Any]],
    context: RunContext,
) -> dict[str, Any]:
    """Build a compact daily summary payload from one comparison run."""
    pipeline_summary = comparison.get("pipeline_summary", {}) if isinstance(comparison, dict) else {}
    signal_accuracy = comparison.get("signal_accuracy", {}) if isinstance(comparison, dict) else {}
    overall_accuracy = signal_accuracy.get("overall", {}) if isinstance(signal_accuracy, dict) else {}
    consensus_signals = list(comparison.get("consensus_signals", []) or [])

    total_videos = 0
    total_signals = 0
    unique_tickers: set[str] = set()
    channel_rows: list[dict[str, Any]] = []

    for slug, item in channel_payloads.items():
        rows = list(item.get("rows", []) or [])
        analyzed_videos = len(rows)
        signal_count = 0
        for row in rows:
            stocks = row.get("stocks", []) or []
            signal_count += len(stocks)
            unique_tickers.update(
                str(stock.get("ticker", "")).strip()
                for stock in stocks
                if str(stock.get("ticker", "")).strip()
            )
        info = comparison.get("channels", {}).get(slug, {}) if isinstance(comparison.get("channels", {}), dict) else {}
        channel_rows.append(
            {
                "slug": slug,
                "display_name": item.get("display_name", slug),
                "analyzed_videos": analyzed_videos,
                "signal_count": signal_count,
                "actionable_videos": int(info.get("actionable_videos", 0) or 0),
                "strict_actionable_videos": int(info.get("strict_actionable_videos", 0) or 0),
                "hit_rate_3d": info.get("signal_accuracy", {}).get("hit_rate_3d") if isinstance(info.get("signal_accuracy"), dict) else None,
                "hit_rate_5d": info.get("hit_rate_5d"),
                "target_hit_rate": info.get("target_hit_rate"),
                "tracked_signals": int(info.get("tracked_signals", 0) or 0),
                "overall_quality_score": info.get("overall_quality_score"),
            }
        )
        total_videos += analyzed_videos
        total_signals += signal_count

    channel_rows.sort(key=lambda item: (-item["signal_count"], item["display_name"]))
    channel_hit_rates = [
        {
            "slug": str(item.get("slug", "")),
            "display_name": str(item.get("display_name") or item.get("slug", "")),
            "overall_quality_score": item.get("overall_quality_score"),
            "weight_multiplier": item.get("weight_multiplier"),
            "hit_rate_3d": item.get("hit_rate_3d"),
            "hit_rate_5d": item.get("hit_rate_5d"),
            "avg_return_5d": item.get("avg_return_5d"),
            "target_hit_rate": item.get("target_hit_rate"),
            "tracked_signals": item.get("total_signals"),
            "actionable_ratio": item.get("actionable_ratio"),
        }
        for item in leaderboard
    ]

    return {
        "generated_at": context.run_id,
        "report_date": context.today,
        "window_days": context.window_days,
        "totals": {
            "videos_analyzed": total_videos,
            "signal_count": total_signals,
            "unique_ticker_count": len(unique_tickers),
            "actionable_videos": int(pipeline_summary.get("actionable_videos", 0) or 0),
            "strict_actionable_videos": int(pipeline_summary.get("strict_actionable_videos", 0) or 0),
            "consensus_signal_count": len(consensus_signals),
            "tracked_signal_count": int(overall_accuracy.get("total_signals", 0) or 0),
        },
        "consensus_signals": consensus_signals[:10],
        "channel_hit_rates": channel_hit_rates[:10],
        "channels": channel_rows,
    }


def render_daily_report_markdown(report: dict[str, Any]) -> str:
    """Render the daily summary into Markdown."""
    totals = report.get("totals", {}) if isinstance(report, dict) else {}
    channel_hit_rates = list(report.get("channel_hit_rates", []) or [])
    channels = list(report.get("channels", []) or [])
    consensus_signals = list(report.get("consensus_signals", []) or [])

    lines = [
        f"# Y2I 일일 요약 리포트 - {report.get('report_date', '-')}",
        "",
        f"- Run ID: `{report.get('generated_at', '-')}`",
        f"- Window Days: `{report.get('window_days', '-')}`",
        f"- 오늘 분석된 영상 수: **{int(totals.get('videos_analyzed', 0) or 0)}**",
        f"- 오늘 추출 시그널 수: **{int(totals.get('signal_count', 0) or 0)}**",
        f"- 오늘 유니크 티커 수: **{int(totals.get('unique_ticker_count', 0) or 0)}**",
        f"- 분석 가능 영상 수: **{int(totals.get('actionable_videos', 0) or 0)}**",
        f"- 엄격 ACTIONABLE 수: **{int(totals.get('strict_actionable_videos', 0) or 0)}**",
        f"- 합의 시그널 수: **{int(totals.get('consensus_signal_count', 0) or 0)}**",
        f"- 누적 트래킹 시그널 수: **{int(totals.get('tracked_signal_count', 0) or 0)}**",
        "",
        "## 합의 시그널",
        "",
    ]

    if consensus_signals:
        lines.extend([
            "| 종목 | 점수 | 판정 | 합의강도 | 교차검증 | 채널수 |",
            "|---|---:|---|---|---|---:|",
        ])
        for item in consensus_signals:
            label = item.get("company_name") or item.get("ticker", "-")
            ticker = item.get("ticker", "")
            display = f"{label} ({ticker})" if ticker and ticker != label else str(label)
            lines.append(
                f"| {display} | {float(item.get('aggregate_score', 0) or 0):.1f} | "
                f"{item.get('aggregate_verdict', '-')} | {item.get('consensus_strength', '-')} | "
                f"{item.get('cross_validation_status', '-')} | {int(item.get('channel_count', 0) or 0)} |"
            )
    else:
        lines.append("_오늘 합의 시그널 없음_")

    lines.extend(["", "## 채널별 적중률", ""])
    if channel_hit_rates:
        lines.extend([
            "| 채널 | 품질점수 | 3d 적중률 | 5d 적중률 | 타겟 적중률 | 5d 평균수익률 | 액션비율 |",
            "|---|---:|---:|---:|---:|---:|---:|",
        ])
        for item in channel_hit_rates:
            lines.append(
                f"| {item.get('display_name', item.get('slug', '-'))} | "
                f"{_fmt_float(item.get('overall_quality_score'))} | {_fmt_pct(item.get('hit_rate_3d'))} | "
                f"{_fmt_pct(item.get('hit_rate_5d'))} | {_fmt_pct(item.get('target_hit_rate'))} | "
                f"{_fmt_pct(item.get('avg_return_5d'))} | {_fmt_ratio(item.get('actionable_ratio'))} |"
            )
    else:
        lines.append("_채널 적중률 데이터 없음_")

    lines.extend(["", "## 채널별 활동량", ""])
    if channels:
        lines.extend([
            "| 채널 | 영상 수 | 시그널 수 | 분석 가능 | 엄격 ACTIONABLE | 누적 트래킹 | 5d 적중률 |",
            "|---|---:|---:|---:|---:|---:|---:|",
        ])
        for item in channels:
            lines.append(
                f"| {item.get('display_name', item.get('slug', '-'))} | {int(item.get('analyzed_videos', 0) or 0)} | "
                f"{int(item.get('signal_count', 0) or 0)} | {int(item.get('actionable_videos', 0) or 0)} | "
                f"{int(item.get('strict_actionable_videos', 0) or 0)} | {int(item.get('tracked_signals', 0) or 0)} | "
                f"{_fmt_pct(item.get('hit_rate_5d'))} |"
            )
    else:
        lines.append("_채널 활동 데이터 없음_")

    return "\n".join(lines).strip() + "\n"


def save_daily_report(
    report: dict[str, Any],
    report_dir: Path,
) -> Path:
    """Save the rendered Markdown report to disk."""
    ensure_dir(report_dir)
    report_date = str(report.get("report_date", "")).replace("-", "") or "unknown"
    run_id = str(report.get("generated_at", "") or "manual")
    path = report_dir / f"daily_summary_{report_date}_{run_id}.md"
    path.write_text(render_daily_report_markdown(report), encoding="utf-8")
    return path


def format_daily_report_telegram_caption(report: dict[str, Any]) -> str:
    """Build a compact HTML caption for Telegram document delivery."""
    totals = report.get("totals", {}) if isinstance(report, dict) else {}
    consensus = list(report.get("consensus_signals", []) or [])
    top_channel = next(iter(report.get("channel_hit_rates", []) or []), None)

    lines = [
        "<b>🧾 Y2I 일일 마감 리포트</b>",
        f"기준일: <b>{html.escape(str(report.get('report_date', '-')))}</b>",
        (
            f"분석 영상 {int(totals.get('videos_analyzed', 0) or 0)}개 | "
            f"시그널 {int(totals.get('signal_count', 0) or 0)}개 | "
            f"합의 {int(totals.get('consensus_signal_count', 0) or 0)}개"
        ),
    ]
    if top_channel:
        lines.append(
            f"상위 채널: <b>{html.escape(str(top_channel.get('display_name', top_channel.get('slug', '-'))))}</b>"
            f" | 5d 적중률 {_fmt_pct(top_channel.get('hit_rate_5d'))}"
        )
    if consensus:
        lead = consensus[0]
        label = lead.get("company_name") or lead.get("ticker", "-")
        ticker = lead.get("ticker", "")
        display = f"{label} ({ticker})" if ticker and ticker != label else str(label)
        lines.append(
            f"대표 합의: <b>{html.escape(display)}</b> | {html.escape(str(lead.get('aggregate_verdict', '-')))} "
            f"| 점수 {float(lead.get('aggregate_score', 0) or 0):.1f}"
        )
    return "\n".join(lines)[:1024]


def _fmt_float(value: object) -> str:
    if value is None or value == "":
        return "-"
    return f"{float(value):.1f}"


def _fmt_pct(value: object) -> str:
    if value is None or value == "":
        return "-"
    return f"{float(value):.1f}%"


def _fmt_ratio(value: object) -> str:
    if value is None or value == "":
        return "-"
    return f"{float(value) * 100:.0f}%"
