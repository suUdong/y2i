#!/usr/bin/env python3
"""Generate a GitHub-viewable markdown dashboard from OMX pipeline output."""

from __future__ import annotations

import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

OUTPUT_DIR = Path(__file__).resolve().parent.parent / "output"
DASHBOARD_PATH = Path(__file__).resolve().parent.parent / "DASHBOARD.md"

# Default display names used as a fallback when channel metadata is missing.
DEFAULT_CHANNELS = {
    "sampro": "삼프로TV",
    "syuka": "슈카월드",
    "hsacademy": "이효석아카데미",
    "sosumonkey": "소수몽키",
    "itgod": "IT의 신 이형수",
    "kimjakgatv": "김작가TV",
}

REFERENCE_KIND_LABELS = {
    "published_at": "게시",
    "generated_at": "스냅샷",
    "unknown": "미분류",
}


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def _latest_file(pattern: str, directory: Path | None = None) -> Path | None:
    d = directory or OUTPUT_DIR
    matches = sorted(d.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    return matches[0] if matches else None


def _file_for_run(pattern_template: str, run_id: str | None, directory: Path | None = None) -> Path | None:
    d = directory or OUTPUT_DIR
    if run_id:
        exact = d / pattern_template.format(run_id=run_id)
        if exact.exists():
            return exact
    return _latest_file(pattern_template.format(run_id="*"), d)


def load_json(path: Path | None) -> dict | list | None:
    if path is None or not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def load_integration_report(directory: Path | None = None) -> dict | None:
    d = directory or OUTPUT_DIR
    p = d / "sampro_integration_report.json"
    return load_json(p)


def load_latest_30d(channel: str = "sampro", directory: Path | None = None, preferred_run_id: str | None = None) -> dict | None:
    d = directory or OUTPUT_DIR
    return load_json(_file_for_run(f"{channel}_30d_{{run_id}}.json", preferred_run_id, d))


def load_latest_comparison(directory: Path | None = None) -> dict | None:
    d = directory or OUTPUT_DIR
    return load_json(_latest_file("channel_comparison_30d_*.json", d))


def get_available_channels(directory: Path | None = None) -> list[str]:
    d = directory or OUTPUT_DIR
    slugs = {
        p.stem.split("_30d_")[0]
        for p in d.glob("*_30d_*.json")
        if not p.stem.startswith("channel_comparison")
    }
    return sorted(slugs)


def channel_label(slug: str, data: dict | None = None) -> str:
    if data and data.get("channel_name"):
        return data["channel_name"]
    return DEFAULT_CHANNELS.get(slug, slug)


def load_all_channels(directory: Path | None = None, preferred_run_id: str | None = None) -> dict[str, dict | None]:
    """Load aligned 30d data for all detected channels, falling back to latest."""
    d = directory or OUTPUT_DIR
    return {slug: load_latest_30d(slug, d, preferred_run_id=preferred_run_id) for slug in get_available_channels(d)}


def build_summary_report(channel_data: dict[str, dict | None]) -> dict:
    type_distribution: dict[str, int] = {}
    signal_distribution: dict[str, int] = {}
    per_video: list[dict] = []
    total_videos = 0
    analyzable_count = 0

    for slug, data in channel_data.items():
        if not data:
            continue
        for video in data.get("videos", []):
            total_videos += 1
            video_type = video.get("video_type", "OTHER")
            signal_class = video.get("video_signal_class", "UNKNOWN")
            type_distribution[video_type] = type_distribution.get(video_type, 0) + 1
            signal_distribution[signal_class] = signal_distribution.get(signal_class, 0) + 1
            if video.get("should_analyze_stocks") or signal_class == "ACTIONABLE":
                analyzable_count += 1
            per_video.append({
                "video_id": video.get("video_id"),
                "title": video.get("title", ""),
                "video_type": video_type,
                "signal_class": signal_class,
                "signal_score": video.get("signal_score", 0),
                "should_analyze": video.get("should_analyze_stocks", False),
                "macro_count": len(video.get("macro_insights", [])),
                "expert_count": len(video.get("expert_insights", [])),
                "expert_names": [item.get("expert_name", "") for item in video.get("expert_insights", []) if item.get("expert_name")],
                "transcript_len": 0,
                "channel": channel_label(slug, data),
            })

    return {
        "total_videos": total_videos,
        "analyzable_count": analyzable_count,
        "type_distribution": type_distribution,
        "signal_distribution": signal_distribution,
        "per_video": per_video,
    }


def build_pipeline_summary_from_channels(channel_data: dict[str, dict | None]) -> dict:
    summary = {
        "total_channels": 0,
        "total_videos": 0,
        "actionable_videos": 0,
        "analyzable_videos": 0,
        "strict_actionable_videos": 0,
        "skipped_videos": 0,
        "transcript_backed_videos": 0,
        "metadata_fallback_videos": 0,
        "latest_published_at": "",
        "latest_reference_at": "",
        "latest_reference_kind": "unknown",
        "signal_breakdown": {},
        "top_skip_reasons": [],
    }
    signal_breakdown: Counter[str] = Counter()
    skip_reasons: Counter[str] = Counter()
    latest_published = ""
    latest_reference = ""
    latest_published_ts: datetime | None = None
    latest_reference_ts: datetime | None = None
    latest_reference_kind = "unknown"

    for data in channel_data.values():
        if not data:
            continue
        summary["total_channels"] += 1
        generated_at = data.get("generated_at", "")
        generated_ts = _parse_time_like(generated_at)
        if generated_ts is not None and (latest_reference_ts is None or generated_ts > latest_reference_ts):
            latest_reference_ts = generated_ts
            latest_reference = generated_at
            latest_reference_kind = "generated_at"
        for video in data.get("videos", []):
            summary["total_videos"] += 1
            signal_class = video.get("video_signal_class", "UNKNOWN")
            signal_breakdown[signal_class] += 1
            if video.get("should_analyze_stocks"):
                summary["actionable_videos"] += 1
            else:
                summary["skipped_videos"] += 1
                reason = (video.get("skip_reason") or video.get("reason") or "").strip()
                if reason:
                    skip_reasons[reason] += 1
            if signal_class == "ACTIONABLE":
                summary["strict_actionable_videos"] += 1

            transcript_language = video.get("transcript_language")
            if transcript_language == "metadata_fallback":
                summary["metadata_fallback_videos"] += 1
            elif transcript_language:
                summary["transcript_backed_videos"] += 1

            published_at = video.get("published_at")
            published_ts = _parse_time_like(published_at)
            if published_ts is not None and (latest_published_ts is None or published_ts > latest_published_ts):
                latest_published_ts = published_ts
                latest_published = published_at
            if published_ts is not None and (latest_reference_ts is None or published_ts > latest_reference_ts):
                latest_reference_ts = published_ts
                latest_reference = published_at
                latest_reference_kind = "published_at"

    summary["latest_published_at"] = latest_published
    summary["latest_reference_at"] = latest_reference
    summary["latest_reference_kind"] = latest_reference_kind if latest_reference else "unknown"
    summary["analyzable_videos"] = summary["actionable_videos"]
    summary["signal_breakdown"] = dict(signal_breakdown)
    summary["top_skip_reasons"] = [
        {"reason": reason, "count": count}
        for reason, count in skip_reasons.most_common(5)
    ]
    return summary


def build_channel_gate_health(channel_data: dict[str, dict | None]) -> dict[str, dict]:
    channel_health: dict[str, dict] = {}
    for slug, data in channel_data.items():
        if data is None:
            continue
        videos = data.get("videos", [])
        latest_published = ""
        latest_reference = data.get("generated_at", "") or ""
        latest_published_ts: datetime | None = None
        latest_reference_ts: datetime | None = _parse_time_like(latest_reference)
        skip_counts: Counter[str] = Counter(
            (video.get("skip_reason") or video.get("reason") or "").strip()
            for video in videos
            if not video.get("should_analyze_stocks") and (video.get("skip_reason") or video.get("reason"))
        )
        for video in videos:
            published_at = video.get("published_at")
            if not published_at:
                continue
            published_ts = _parse_time_like(published_at)
            if published_ts is not None and (latest_published_ts is None or published_ts > latest_published_ts):
                latest_published_ts = published_ts
                latest_published = published_at
            if published_ts is not None and (latest_reference_ts is None or published_ts > latest_reference_ts):
                latest_reference_ts = published_ts
                latest_reference = published_at

        channel_health[slug] = {
            "display_name": channel_label(slug, data),
            "skipped_videos": sum(1 for video in videos if not video.get("should_analyze_stocks")),
            "metadata_fallback_videos": sum(1 for video in videos if video.get("transcript_language") == "metadata_fallback"),
            "latest_published_at": latest_published,
            "latest_reference_at": latest_reference,
            "latest_reference_kind": "published_at" if latest_published else ("generated_at" if latest_reference else "unknown"),
            "top_skip_reasons": [
                {"reason": reason, "count": count}
                for reason, count in skip_counts.most_common(1)
            ],
        }
    return channel_health


# ---------------------------------------------------------------------------
# Render helpers
# ---------------------------------------------------------------------------

def _bar(value: int, max_value: int, width: int = 20) -> str:
    if max_value == 0:
        return ""
    filled = round(value / max_value * width)
    return "\u2588" * filled + "\u2591" * (width - filled)


def _pct(value: int, total: int) -> str:
    if total == 0:
        return "0.0%"
    return f"{value / total * 100:.1f}%"


def _format_date(value: str | None) -> str:
    if not value:
        return "N/A"
    normalized = value.strip()
    if not normalized:
        return "N/A"
    for fmt in ("%Y%m%dT%H%M%SZ", "%Y%m%d", "%Y-%m-%d"):
        try:
            parsed = datetime.strptime(normalized, fmt).replace(tzinfo=timezone.utc)
            return parsed.strftime("%Y-%m-%d")
        except ValueError:
            continue
    try:
        parsed = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except ValueError:
        return normalized
    return parsed.astimezone(timezone.utc).strftime("%Y-%m-%d")


def _format_reference_kind(kind: str | None) -> str:
    return REFERENCE_KIND_LABELS.get(kind or "unknown", kind or "unknown")


def _format_run_id(value: str | None) -> str:
    parsed = _parse_time_like(value)
    if parsed is None:
        return value or "N/A"
    return parsed.strftime("%Y-%m-%d %H:%M UTC")


def _parse_time_like(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.strip()
    if not normalized:
        return None
    for fmt in ("%Y%m%dT%H%M%SZ", "%Y%m%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(normalized, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    try:
        parsed = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


# ---------------------------------------------------------------------------
# Section renderers
# ---------------------------------------------------------------------------

def render_header(channel_data: dict[str, dict | None]) -> str:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    active = sum(1 for v in channel_data.values() if v is not None)
    lines = [
        "# OMX Pipeline Dashboard (6-Month / 180-Day Analysis)",
        "",
        f"> Auto-generated: {now}",
        f"> Channels analyzed: {active}/{len(channel_data)}",
        "> Data source: `output/` directory pipeline results",
        "",
        "---",
        "",
    ]
    return "\n".join(lines)


def render_channel_overview(channel_data: dict[str, dict | None]) -> str:
    lines = ["## Channel Overview", ""]
    lines.append("| Channel | Videos | Analyzable | Strict ACTIONABLE | Ratio | Stocks Found | Quality Score |")
    lines.append("|---------|------:|-----------:|------------------:|------:|------------:|--------------:|")

    for slug, data in channel_data.items():
        name = channel_label(slug, data)
        if data is None:
            lines.append(f"| {name} | - | - | - | - | - | - |")
            continue
        videos = data.get("videos", [])
        total = len(videos)
        analyzable = sum(1 for v in videos if v.get("should_analyze_stocks"))
        strict_actionable = sum(1 for v in videos if v.get("video_signal_class") == "ACTIONABLE")
        ratio = analyzable / total if total else 0
        stocks = len(data.get("cross_video_ranking", []))
        scorecard = data.get("quality_scorecard", {})
        quality = scorecard.get("overall", 0.0)
        lines.append(f"| {name} | {total} | {analyzable} | {strict_actionable} | {ratio:.1%} | {stocks} | {quality:.1f} |")

    lines.append("")
    return "\n".join(lines)


def render_channel_stock_ranking(slug: str, data: dict | None) -> str:
    name = channel_label(slug, data)
    lines = [f"### {name} - Stock Ranking", ""]
    if not data:
        lines.append(f"_No data available for {name}._")
        return "\n".join(lines)

    ranking = data.get("cross_video_ranking", [])
    if not ranking:
        lines.append("_No stock ranking data._")
        return "\n".join(lines)

    sorted_ranking = sorted(ranking, key=lambda x: x.get("aggregate_score", 0), reverse=True)[:15]

    lines.append("| Rank | Ticker | Company | Score | Verdict | Mentions | Price |")
    lines.append("|-----:|--------|---------|------:|---------|--------:|------:|")
    for i, stock in enumerate(sorted_ranking):
        rank = f"{i+1}."
        ticker = stock.get("ticker", "?")
        company = stock.get("company_name", "?")
        if len(company) > 30:
            company = company[:28] + ".."
        score = stock.get("aggregate_score", 0)
        verdict = stock.get("aggregate_verdict", "?")
        mentions = stock.get("total_mentions", 0)
        price = stock.get("latest_price", 0)
        currency = stock.get("currency", "")
        price_str = f"{price:,.0f} {currency}" if price else "N/A"
        lines.append(f"| {rank} | `{ticker}` | {company} | {score:.1f} | **{verdict}** | {mentions} | {price_str} |")

    lines.append("")
    return "\n".join(lines)


def render_all_stock_rankings(channel_data: dict[str, dict | None]) -> str:
    lines = ["## Per-Channel Stock Rankings (180 Days)", ""]
    for slug, data in channel_data.items():
        lines.append(render_channel_stock_ranking(slug, data))
    return "\n".join(lines)


def render_cross_channel_top_stocks(channel_data: dict[str, dict | None]) -> str:
    """Aggregate top stocks across all channels."""
    lines = ["## Cross-Channel Top Stocks", ""]

    # Merge rankings from all channels
    ticker_agg: dict[str, dict] = {}
    for slug, data in channel_data.items():
        if not data:
            continue
        name = channel_label(slug, data)
        for stock in data.get("cross_video_ranking", []):
            ticker = stock.get("ticker", "?")
            if ticker not in ticker_agg:
                ticker_agg[ticker] = {
                    "ticker": ticker,
                    "company": stock.get("company_name", "?"),
                    "total_score": 0.0,
                    "total_mentions": 0,
                    "channels": [],
                    "verdicts": [],
                }
            ticker_agg[ticker]["total_score"] += stock.get("aggregate_score", 0)
            ticker_agg[ticker]["total_mentions"] += stock.get("total_mentions", 0)
            ticker_agg[ticker]["channels"].append(name)
            verdict = stock.get("aggregate_verdict", "?")
            if verdict != "?":
                ticker_agg[ticker]["verdicts"].append(verdict)

    if not ticker_agg:
        lines.append("_No cross-channel stock data._")
        return "\n".join(lines)

    # Sort by number of channels mentioning, then by total score
    sorted_stocks = sorted(
        ticker_agg.values(),
        key=lambda x: (len(x["channels"]), x["total_score"]),
        reverse=True,
    )[:20]

    lines.append("| Ticker | Company | Channels | Total Score | Mentions | Consensus |")
    lines.append("|--------|---------|---------|------------:|---------:|-----------|")
    for s in sorted_stocks:
        company = s["company"]
        if len(company) > 25:
            company = company[:23] + ".."
        ch_count = len(set(s["channels"]))
        ch_names = ", ".join(sorted(set(s["channels"])))
        # Determine consensus verdict
        if s["verdicts"]:
            from collections import Counter
            verdict_counts = Counter(s["verdicts"])
            consensus = verdict_counts.most_common(1)[0][0]
        else:
            consensus = "N/A"
        lines.append(
            f"| `{s['ticker']}` | {company} | {ch_names} ({ch_count}) | "
            f"{s['total_score']:.1f} | {s['total_mentions']} | **{consensus}** |"
        )

    lines.append("")
    return "\n".join(lines)


def render_macro_signals(channel_data: dict[str, dict | None]) -> str:
    lines = ["## Macro Signals (All Channels)", ""]

    macro_agg: dict[str, dict] = {}
    for slug, data in channel_data.items():
        if not data:
            continue
        for video in data.get("videos", []):
            title = video.get("title", "")
            signal_class = video.get("video_signal_class", "NOISE")
            _aggregate_macro_from_title(title, signal_class, macro_agg)

    if not macro_agg:
        lines.append("_No macro signals detected._")
        return "\n".join(lines)

    sorted_macros = sorted(macro_agg.values(), key=lambda x: x["count"], reverse=True)

    lines.append("| Indicator | Direction | Sentiment | Frequency |")
    lines.append("|-----------|-----------|-----------|----------:|")
    for m in sorted_macros:
        lines.append(f"| {m['indicator']} | {m['direction']} | {m['sentiment']} | {m['count']} |")
    lines.append("")
    return "\n".join(lines)


# Macro keyword mapping for title-based extraction
_MACRO_KEYWORDS = {
    "금리": ("interest_rate", "NEUTRAL", "NEUTRAL"),
    "환율": ("fx", "NEUTRAL", "NEUTRAL"),
    "유가": ("oil", "NEUTRAL", "NEUTRAL"),
    "인플레": ("inflation", "UP", "BEARISH"),
    "CPI": ("cpi", "NEUTRAL", "NEUTRAL"),
    "고용": ("employment", "NEUTRAL", "NEUTRAL"),
    "전쟁": ("geopolitics", "UP", "BEARISH"),
    "중동": ("geopolitics", "UP", "BEARISH"),
    "이란": ("geopolitics", "UP", "BEARISH"),
    "트럼프": ("us_policy", "NEUTRAL", "NEUTRAL"),
    "반도체": ("semiconductor", "NEUTRAL", "BULLISH"),
    "에너지": ("energy", "UP", "NEUTRAL"),
    "방산": ("defense", "UP", "BULLISH"),
    "밸류에이션": ("valuation", "NEUTRAL", "NEUTRAL"),
    "펀더멘털": ("fundamentals", "NEUTRAL", "BULLISH"),
    "AI": ("ai_tech", "NEUTRAL", "BULLISH"),
    "관세": ("tariff", "UP", "BEARISH"),
    "무역": ("trade", "NEUTRAL", "NEUTRAL"),
    "ETF": ("etf", "NEUTRAL", "NEUTRAL"),
    "배당": ("dividend", "NEUTRAL", "BULLISH"),
    "테슬라": ("tesla", "NEUTRAL", "NEUTRAL"),
    "엔비디아": ("nvidia", "NEUTRAL", "BULLISH"),
    "비트코인": ("bitcoin", "NEUTRAL", "NEUTRAL"),
    "코인": ("crypto", "NEUTRAL", "NEUTRAL"),
}


def _aggregate_macro_from_title(title: str, signal_class: str, agg: dict[str, dict]) -> None:
    for keyword, (indicator, direction, sentiment) in _MACRO_KEYWORDS.items():
        if keyword in title:
            if indicator not in agg:
                agg[indicator] = {
                    "indicator": indicator,
                    "direction": direction,
                    "sentiment": sentiment,
                    "count": 0,
                }
            agg[indicator]["count"] += 1


def render_quality_comparison(channel_data: dict[str, dict | None]) -> str:
    lines = ["## Quality Scorecard Comparison", ""]
    scorecard_keys = ["transcript_coverage", "actionable_density", "ranking_predictive_power", "horizon_adequacy", "overall"]

    active_channels = {slug: data for slug, data in channel_data.items() if data is not None}
    if not active_channels:
        lines.append("_No channel data available._")
        return "\n".join(lines)

    header = "| Metric | " + " | ".join(channel_label(slug, data) for slug, data in active_channels.items()) + " |"
    sep = "|--------|" + "|".join("------:" for _ in active_channels) + "|"
    lines.append(header)
    lines.append(sep)

    for key in scorecard_keys:
        label = key.replace("_", " ").title()
        vals = " | ".join(
            f"{data.get('quality_scorecard', {}).get(key, 0.0):.1f}"
            for data in active_channels.values()
        )
        lines.append(f"| {label} | {vals} |")

    lines.append("")
    return "\n".join(lines)


def render_pipeline_health(comparison: dict | None, channel_data: dict[str, dict | None]) -> str:
    lines = ["## Pipeline Health", ""]
    summary = {}
    if comparison:
        summary = comparison.get("pipeline_summary", {})
    if not summary:
        summary = build_pipeline_summary_from_channels(channel_data)
    if not summary:
        lines.append("_No pipeline summary available._")
        return "\n".join(lines)

    lines.append("| Metric | Value |")
    lines.append("|--------|------:|")
    lines.append(f"| Snapshot run | {_format_run_id(comparison.get('generated_at') if comparison else None)} |")
    lines.append(f"| Channels | {summary.get('total_channels', 0)} |")
    lines.append(f"| Videos | {summary.get('total_videos', 0)} |")
    lines.append(f"| Analyzable | {summary.get('analyzable_videos', summary.get('actionable_videos', 0))} |")
    lines.append(f"| Strict ACTIONABLE | {summary.get('strict_actionable_videos', 0)} |")
    lines.append(f"| Skipped | {summary.get('skipped_videos', 0)} |")
    lines.append(f"| Transcript-backed | {summary.get('transcript_backed_videos', 0)} |")
    lines.append(f"| Metadata fallback | {summary.get('metadata_fallback_videos', 0)} |")
    reference_kind = summary.get("latest_reference_kind", "unknown")
    lines.append(f"| Latest reference | {_format_date(summary.get('latest_reference_at'))} ({_format_reference_kind(reference_kind)}) |")
    lines.append("")

    top_skip_reasons = summary.get("top_skip_reasons", [])
    if top_skip_reasons:
        lines.append("### Top Skip Reasons")
        lines.append("")
        lines.append("| Reason | Count |")
        lines.append("|--------|------:|")
        for item in top_skip_reasons:
            lines.append(f"| {item.get('reason', 'N/A')} | {item.get('count', 0)} |")
        lines.append("")

    fallback_channels = build_channel_gate_health(channel_data)
    channels = comparison.get("channels", {}) if comparison else {}
    merged_channels: dict[str, dict] = {}
    for slug in sorted(set(fallback_channels) | set(channels)):
        info = dict(fallback_channels.get(slug, {}))
        info.update(channels.get(slug, {}))
        if not info.get("display_name"):
            info["display_name"] = slug
        if "skipped_videos" not in info:
            info["skipped_videos"] = fallback_channels.get(slug, {}).get("skipped_videos", 0)
        if "metadata_fallback_videos" not in info:
            info["metadata_fallback_videos"] = fallback_channels.get(slug, {}).get("metadata_fallback_videos", 0)
        if not info.get("latest_published_at"):
            info["latest_published_at"] = fallback_channels.get(slug, {}).get("latest_published_at", "")
        if not info.get("latest_reference_at"):
            info["latest_reference_at"] = fallback_channels.get(slug, {}).get("latest_reference_at", "")
        if not info.get("latest_reference_kind"):
            info["latest_reference_kind"] = fallback_channels.get(slug, {}).get("latest_reference_kind", "unknown")
        if not info.get("top_skip_reasons"):
            info["top_skip_reasons"] = fallback_channels.get(slug, {}).get("top_skip_reasons", [])
        merged_channels[slug] = info
    channels = merged_channels
    if channels:
        lines.append("### Channel Gate Health")
        lines.append("")
        lines.append("| Channel | Skipped | Metadata Fallback | Latest Reference | Top Skip Reason |")
        lines.append("|---------|--------:|------------------:|------------------|-----------------|")
        for slug, info in channels.items():
            label = info.get("display_name", slug)
            top_reason = ""
            if info.get("top_skip_reasons"):
                top_reason = info["top_skip_reasons"][0].get("reason", "")
            lines.append(
                f"| {label} | {info.get('skipped_videos', 0)} | {info.get('metadata_fallback_videos', 0)} | "
                f"{_format_date(info.get('latest_reference_at'))} ({_format_reference_kind(info.get('latest_reference_kind', 'unknown'))}) | {top_reason or 'N/A'} |"
            )
        lines.append("")

    return "\n".join(lines)


def render_content_type_distribution(report: dict | None) -> str:
    lines = ["## Content Type Distribution (삼프로TV)", ""]
    if not report:
        lines.append("_No integration report data available._")
        return "\n".join(lines)

    dist = report.get("type_distribution", {})
    if not dist:
        lines.append("_No type distribution data._")
        return "\n".join(lines)

    total = sum(dist.values())
    sorted_types = sorted(dist.items(), key=lambda x: x[1], reverse=True)
    max_count = sorted_types[0][1] if sorted_types else 1

    lines.append("```")
    for vtype, count in sorted_types:
        bar = _bar(count, max_count, 25)
        lines.append(f"{vtype:<20s} {bar} {count:>3d} ({_pct(count, total):>5s})")
    lines.append("```")
    lines.append("")

    lines.append("| Type | Count | % |")
    lines.append("|------|------:|---:|")
    for vtype, count in sorted_types:
        lines.append(f"| {vtype} | {count} | {_pct(count, total)} |")
    lines.append("")
    return "\n".join(lines)


def render_expert_insights(report: dict | None) -> str:
    lines = ["## Expert Insights (삼프로TV)", ""]
    if not report:
        lines.append("_No integration report data available._")
        return "\n".join(lines)

    per_video = report.get("per_video", [])
    if not per_video:
        lines.append("_No per-video data._")
        return "\n".join(lines)

    experts: dict[str, dict] = {}
    for v in per_video:
        for name in v.get("expert_names", []):
            if name not in experts:
                experts[name] = {"name": name, "count": 0, "video_types": set(), "signal_classes": set()}
            experts[name]["count"] += 1
            experts[name]["video_types"].add(v.get("video_type", "OTHER"))
            experts[name]["signal_classes"].add(v.get("signal_class", "NOISE"))

    if not experts:
        lines.append("_No expert data extracted._")
        return "\n".join(lines)

    sorted_experts = sorted(experts.values(), key=lambda x: x["count"], reverse=True)

    lines.append("| Expert | Appearances | Video Types | Signal Classes |")
    lines.append("|--------|----------:|-------------|----------------|")
    for e in sorted_experts:
        vtypes = ", ".join(sorted(e["video_types"]))
        sclasses = ", ".join(sorted(e["signal_classes"]))
        lines.append(f"| **{e['name']}** | {e['count']} | {vtypes} | {sclasses} |")
    lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main generator
# ---------------------------------------------------------------------------

def generate_dashboard(output_dir: Path | None = None, dest: Path | None = None) -> str:
    d = output_dir or OUTPUT_DIR
    comparison = load_latest_comparison(d)
    preferred_run_id = comparison.get("generated_at") if comparison else None
    channel_data = load_all_channels(d, preferred_run_id=preferred_run_id)
    report = build_summary_report(channel_data)

    sections = [
        render_header(channel_data),
        render_channel_overview(channel_data),
        render_pipeline_health(comparison, channel_data),
        render_quality_comparison(channel_data),
        render_cross_channel_top_stocks(channel_data),
        render_all_stock_rankings(channel_data),
        render_macro_signals(channel_data),
        render_content_type_distribution(report),
        render_expert_insights(report),
    ]

    md = "\n".join(sections)

    target = dest or DASHBOARD_PATH
    target.write_text(md, encoding="utf-8")
    return str(target)


def main() -> None:
    path = generate_dashboard()
    print(f"Dashboard generated: {path}")


if __name__ == "__main__":
    main()
