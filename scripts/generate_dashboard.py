#!/usr/bin/env python3
"""Generate a GitHub-viewable markdown dashboard from OMX pipeline output."""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

OUTPUT_DIR = Path(__file__).resolve().parent.parent / "output"
DASHBOARD_PATH = Path(__file__).resolve().parent.parent / "DASHBOARD.md"


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def _latest_file(pattern: str, directory: Path | None = None) -> Path | None:
    d = directory or OUTPUT_DIR
    matches = sorted(d.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    return matches[0] if matches else None


def load_json(path: Path | None) -> dict | list | None:
    if path is None or not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def load_integration_report(directory: Path | None = None) -> dict | None:
    d = directory or OUTPUT_DIR
    p = d / "sampro_integration_report.json"
    return load_json(p)


def load_latest_30d(channel: str = "sampro", directory: Path | None = None) -> dict | None:
    d = directory or OUTPUT_DIR
    return load_json(_latest_file(f"{channel}_30d_*.json", d))


def load_latest_comparison(directory: Path | None = None) -> dict | None:
    d = directory or OUTPUT_DIR
    return load_json(_latest_file("channel_comparison_30d_*.json", d))


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


# ---------------------------------------------------------------------------
# Section renderers
# ---------------------------------------------------------------------------

def render_header(report: dict | None, comparison: dict | None) -> str:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [
        "# OMX Pipeline Dashboard",
        "",
        f"> Auto-generated: {now}",
        "> Data source: `output/` directory pipeline results",
        "",
        "---",
        "",
    ]
    return "\n".join(lines)


def render_pipeline_summary(report: dict | None) -> str:
    lines = ["## Pipeline Summary (30 Days)", ""]
    if not report:
        lines.append("_No integration report data available._")
        return "\n".join(lines)

    total = report.get("total_videos", 0)
    sig = report.get("signal_distribution", {})
    actionable = sig.get("ACTIONABLE", 0)
    noise = sig.get("NOISE", 0)

    lines.append(f"| Metric | Value |")
    lines.append(f"|--------|-------|")
    lines.append(f"| Total Videos | **{total}** |")
    lines.append(f"| ACTIONABLE | **{actionable}** ({_pct(actionable, total)}) |")
    lines.append(f"| NOISE | **{noise}** ({_pct(noise, total)}) |")
    lines.append(f"| Analyzable | **{report.get('analyzable_count', 0)}** |")
    lines.append(f"| Expert Extraction | {report.get('expert_extraction_rate', 'N/A')} |")
    lines.append(f"| Macro Coverage | {report.get('macro_coverage', 'N/A')} |")
    lines.append("")

    # Visual bar
    lines.append("**Signal Distribution:**")
    lines.append(f"```")
    lines.append(f"ACTIONABLE {_bar(actionable, total, 30)} {actionable}/{total}")
    lines.append(f"NOISE      {_bar(noise, total, 30)} {noise}/{total}")
    lines.append(f"```")
    lines.append("")
    return "\n".join(lines)


def render_content_type_distribution(report: dict | None) -> str:
    lines = ["## Content Type Distribution", ""]
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

    # Text bar chart
    lines.append("```")
    for vtype, count in sorted_types:
        bar = _bar(count, max_count, 25)
        lines.append(f"{vtype:<20s} {bar} {count:>3d} ({_pct(count, total):>5s})")
    lines.append("```")
    lines.append("")

    # Table
    lines.append("| Type | Count | % |")
    lines.append("|------|------:|---:|")
    for vtype, count in sorted_types:
        lines.append(f"| {vtype} | {count} | {_pct(count, total)} |")
    lines.append("")
    return "\n".join(lines)


def render_stock_ranking(data_30d: dict | None) -> str:
    lines = ["## Stock Ranking", ""]
    if not data_30d:
        lines.append("_No 30-day analysis data available._")
        return "\n".join(lines)

    ranking = data_30d.get("cross_video_ranking", [])
    if not ranking:
        lines.append("_No stock ranking data._")
        return "\n".join(lines)

    sorted_ranking = sorted(ranking, key=lambda x: x.get("aggregate_score", 0), reverse=True)
    medals = {0: "1.", 1: "2.", 2: "3."}

    lines.append("| Rank | Ticker | Company | Score | Verdict | Mentions | Price |")
    lines.append("|-----:|--------|---------|------:|---------|--------:|------:|")
    for i, stock in enumerate(sorted_ranking):
        rank = medals.get(i, f"{i+1}.")
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


def render_macro_signals(data_30d: dict | None) -> str:
    lines = ["## Macro Signals", ""]
    if not data_30d:
        lines.append("_No 30-day analysis data available._")
        return "\n".join(lines)

    # Aggregate macro signals from all videos
    macro_agg: dict[str, dict] = {}
    for video in data_30d.get("videos", []):
        for stock in video.get("stocks", []):
            # Macro signals come from signal_metrics
            pass
        # Check video-level macro data if present
        metrics = video.get("signal_metrics", {})
        macro_count = metrics.get("macro_signal_count", 0)
        if macro_count > 0:
            vid_class = video.get("video_signal_class", "NOISE")
            sectors = metrics.get("macro_sector_count", 0)
            actionable_macro = metrics.get("actionable_macro_count", 0)
            # Use video title keywords to infer macro themes
            title = video.get("title", "")
            _aggregate_macro_from_title(title, vid_class, macro_agg)

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


def render_expert_insights(report: dict | None) -> str:
    lines = ["## Expert Insights", ""]
    if not report:
        lines.append("_No integration report data available._")
        return "\n".join(lines)

    per_video = report.get("per_video", [])
    if not per_video:
        lines.append("_No per-video data._")
        return "\n".join(lines)

    # Aggregate expert appearances
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


def render_channel_comparison(comparison: dict | None) -> str:
    lines = ["## Channel Comparison", ""]
    if not comparison:
        lines.append("_No channel comparison data available._")
        return "\n".join(lines)

    channels = comparison.get("channels", {})
    if not channels:
        lines.append("_No channel data._")
        return "\n".join(lines)

    lines.append("| Channel | Videos | Actionable | Ratio | Quality Score |")
    lines.append("|---------|------:|----------:|------:|--------------:|")
    for slug, ch in channels.items():
        name = ch.get("display_name", slug)
        total = ch.get("total_videos", 0)
        actionable = ch.get("actionable_videos", 0)
        ratio = ch.get("actionable_ratio", 0.0)
        quality = ch.get("quality_scorecard", {}).get("overall", 0.0)
        lines.append(f"| {name} | {total} | {actionable} | {ratio:.1%} | {quality:.1f} |")
    lines.append("")

    # Quality scorecard details
    lines.append("### Quality Scorecard Details")
    lines.append("")
    scorecard_keys = ["transcript_coverage", "actionable_density", "ranking_predictive_power", "horizon_adequacy"]
    header = "| Metric | " + " | ".join(ch.get("display_name", slug) for slug, ch in channels.items()) + " |"
    sep = "|--------|" + "|".join("------:" for _ in channels) + "|"
    lines.append(header)
    lines.append(sep)
    for key in scorecard_keys:
        label = key.replace("_", " ").title()
        vals = " | ".join(
            f"{ch.get('quality_scorecard', {}).get(key, 0.0):.1f}" for ch in channels.values()
        )
        lines.append(f"| {label} | {vals} |")
    lines.append("")

    better_ranking = comparison.get("better_ranking_channel", "N/A")
    more_actionable = comparison.get("more_actionable_channel", "N/A")
    lines.append(f"- **More Actionable Channel:** {more_actionable}")
    lines.append(f"- **Better Ranking Channel:** {better_ranking}")
    lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main generator
# ---------------------------------------------------------------------------

def generate_dashboard(output_dir: Path | None = None, dest: Path | None = None) -> str:
    d = output_dir or OUTPUT_DIR
    report = load_integration_report(d)
    data_30d = load_latest_30d("sampro", d)
    comparison = load_latest_comparison(d)

    sections = [
        render_header(report, comparison),
        render_pipeline_summary(report),
        render_content_type_distribution(report),
        render_stock_ranking(data_30d),
        render_macro_signals(data_30d),
        render_expert_insights(report),
        render_channel_comparison(comparison),
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
