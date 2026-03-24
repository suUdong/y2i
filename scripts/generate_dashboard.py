#!/usr/bin/env python3
"""Generate a GitHub-viewable markdown dashboard from OMX pipeline output."""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

OUTPUT_DIR = Path(__file__).resolve().parent.parent / "output"
DASHBOARD_PATH = Path(__file__).resolve().parent.parent / "DASHBOARD.md"

# Channels to include in dashboard (slug -> display_name)
CHANNELS = {
    "sampro": "삼프로TV",
    "syuka": "슈카월드",
    "hsacademy": "이효석아카데미",
    "sosumonkey": "소수몽키",
    "itgod": "IT의 신 이형수",
    "kimjakgatv": "김작가TV",
}


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


def load_all_channels(directory: Path | None = None) -> dict[str, dict | None]:
    """Load latest 30d data for all configured channels."""
    d = directory or OUTPUT_DIR
    return {slug: load_latest_30d(slug, d) for slug in CHANNELS}


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

def render_header(channel_data: dict[str, dict | None]) -> str:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    active = sum(1 for v in channel_data.values() if v is not None)
    lines = [
        "# OMX Pipeline Dashboard (6-Month / 180-Day Analysis)",
        "",
        f"> Auto-generated: {now}",
        f"> Channels analyzed: {active}/{len(CHANNELS)}",
        "> Data source: `output/` directory pipeline results",
        "",
        "---",
        "",
    ]
    return "\n".join(lines)


def render_channel_overview(channel_data: dict[str, dict | None]) -> str:
    lines = ["## Channel Overview", ""]
    lines.append("| Channel | Videos | Actionable | Ratio | Stocks Found | Quality Score |")
    lines.append("|---------|------:|----------:|------:|------------:|--------------:|")

    for slug, data in channel_data.items():
        name = CHANNELS.get(slug, slug)
        if data is None:
            lines.append(f"| {name} | - | - | - | - | - |")
            continue
        videos = data.get("videos", [])
        total = len(videos)
        actionable = sum(1 for v in videos if v.get("should_analyze_stocks"))
        ratio = actionable / total if total else 0
        stocks = len(data.get("cross_video_ranking", []))
        scorecard = data.get("quality_scorecard", {})
        quality = scorecard.get("overall", 0.0)
        lines.append(f"| {name} | {total} | {actionable} | {ratio:.1%} | {stocks} | {quality:.1f} |")

    lines.append("")
    return "\n".join(lines)


def render_channel_stock_ranking(slug: str, data: dict | None) -> str:
    name = CHANNELS.get(slug, slug)
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
        name = CHANNELS.get(slug, slug)
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

    header = "| Metric | " + " | ".join(CHANNELS.get(slug, slug) for slug in active_channels) + " |"
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
    report = load_integration_report(d)
    channel_data = {slug: load_latest_30d(slug, d) for slug in CHANNELS}
    comparison = load_latest_comparison(d)

    sections = [
        render_header(channel_data),
        render_channel_overview(channel_data),
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
