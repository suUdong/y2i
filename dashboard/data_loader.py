"""Data loading utilities for the OMX Streamlit dashboard."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parent.parent / "output"


def _latest_file(output_dir: Path, pattern: str) -> Path | None:
    """Return the most recently modified file matching *pattern*, or None."""
    matches = sorted(output_dir.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    return matches[0] if matches else None


def _load_json(path: Path | None) -> dict[str, Any] | list[Any]:
    if path is None or not path.exists():
        return {}
    with open(path, encoding="utf-8") as f:
        return json.load(f)


# ── Integration report (sampro_integration_report.json) ──────────────────────

def load_integration_report(output_dir: Path = DEFAULT_OUTPUT_DIR) -> dict[str, Any]:
    path = output_dir / "sampro_integration_report.json"
    return _load_json(path)


# ── 30-day channel results ───────────────────────────────────────────────────

def load_30d_results(channel_slug: str, output_dir: Path = DEFAULT_OUTPUT_DIR) -> dict[str, Any]:
    path = _latest_file(output_dir, f"{channel_slug}_30d_*.json")
    return _load_json(path)


# ── Channel comparison ───────────────────────────────────────────────────────

def load_channel_comparison(output_dir: Path = DEFAULT_OUTPUT_DIR) -> dict[str, Any]:
    path = _latest_file(output_dir, "channel_comparison_30d_*.json")
    return _load_json(path)


# ── Video titles with labels ─────────────────────────────────────────────────

def load_video_titles(output_dir: Path = DEFAULT_OUTPUT_DIR) -> dict[str, Any]:
    path = output_dir / "sampro_video_titles.json"
    return _load_json(path)


# ── Individual video report JSONs ────────────────────────────────────────────

def load_video_reports(output_dir: Path = DEFAULT_OUTPUT_DIR) -> list[dict[str, Any]]:
    """Load all individual video analysis report JSONs (hash-based filenames)."""
    reports = []
    for p in sorted(output_dir.glob("*_*.json"), key=lambda p: p.stat().st_mtime, reverse=True):
        name = p.stem
        # Skip channel-level / comparison / title files
        if any(tag in name for tag in ("30d_", "comparison", "titles", "integration", "results_", "PIPELINE")):
            continue
        data = _load_json(p)
        if isinstance(data, dict) and "video" in data:
            reports.append(data)
    return reports


# ── Helpers for extracting dashboard-ready data ──────────────────────────────

def extract_type_distribution(report: dict[str, Any]) -> dict[str, int]:
    return report.get("type_distribution", {})


def extract_signal_distribution(report: dict[str, Any]) -> dict[str, int]:
    return report.get("signal_distribution", {})


def extract_per_video(report: dict[str, Any]) -> list[dict[str, Any]]:
    return report.get("per_video", [])


def extract_cross_video_ranking(data_30d: dict[str, Any]) -> list[dict[str, Any]]:
    return data_30d.get("cross_video_ranking", [])


def extract_videos(data_30d: dict[str, Any]) -> list[dict[str, Any]]:
    return data_30d.get("videos", [])


def extract_expert_insights(videos: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Collect expert insights from per-video data in a 30d result."""
    insights = []
    for v in videos:
        for expert in v.get("expert_insights", []):
            expert["source_video"] = v.get("title", v.get("video_id", ""))
            insights.append(expert)
    return insights


def extract_macro_signals(videos: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Collect macro insights from per-video data in a 30d result."""
    signals = []
    for v in videos:
        for macro in v.get("macro_insights", []):
            macro["source_video"] = v.get("title", v.get("video_id", ""))
            signals.append(macro)
    return signals


def get_available_channels(output_dir: Path = DEFAULT_OUTPUT_DIR) -> list[str]:
    """Detect channel slugs from *_30d_*.json filenames."""
    slugs = set()
    for p in output_dir.glob("*_30d_*.json"):
        parts = p.stem.split("_30d_")
        if parts[0] not in ("channel_comparison",):
            slugs.add(parts[0])
    return sorted(slugs)


# ── Last-update timestamp (US-002) ──────────────────────────────────────────

def get_last_update_time(output_dir: Path = DEFAULT_OUTPUT_DIR) -> datetime | None:
    """Return the mtime of the most recently modified JSON in output_dir."""
    jsons = list(output_dir.glob("*.json"))
    if not jsons:
        return None
    latest = max(jsons, key=lambda p: p.stat().st_mtime)
    return datetime.fromtimestamp(latest.stat().st_mtime, tz=timezone.utc)


# ── Recent videos across all channels (US-003) ─────────────────────────────

def get_recent_videos(
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    hours: int = 24,
) -> list[dict[str, Any]]:
    """Collect videos from all channels whose 30d file was updated within *hours*."""
    cutoff = datetime.now(tz=timezone.utc).timestamp() - hours * 3600
    recent: list[dict[str, Any]] = []
    for p in output_dir.glob("*_30d_*.json"):
        if p.stem.startswith("channel_comparison"):
            continue
        if p.stat().st_mtime < cutoff:
            continue
        data = _load_json(p)
        slug = data.get("channel_slug", p.stem.split("_30d_")[0])
        for v in data.get("videos", []):
            v["_channel"] = slug
            recent.append(v)
    return recent


# ── Actionable signal extraction (US-006) ───────────────────────────────────

def extract_actionable_signals(
    output_dir: Path = DEFAULT_OUTPUT_DIR,
) -> list[dict[str, Any]]:
    """Return ACTIONABLE videos with their ticker mentions across all channels."""
    signals: list[dict[str, Any]] = []
    for slug in get_available_channels(output_dir):
        data = load_30d_results(slug, output_dir)
        for v in data.get("videos", []):
            if v.get("video_signal_class") != "ACTIONABLE":
                continue
            tickers: list[str] = []
            for s in v.get("stocks", []):
                t = s.get("ticker", "")
                if t:
                    tickers.append(t)
            signals.append({
                "channel": slug,
                "title": v.get("title", ""),
                "signal_score": v.get("signal_score", 0),
                "tickers": tickers,
                "published_at": v.get("published_at", ""),
                "reason": v.get("reason", ""),
                "video_type": v.get("video_type", ""),
                "stocks": v.get("stocks", []),
            })
    signals.sort(key=lambda s: s.get("signal_score", 0), reverse=True)
    return signals


# ── Pipeline activity log (US-004) ──────────────────────────────────────────

def get_pipeline_activity(
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    limit: int = 15,
) -> list[dict[str, Any]]:
    """Return recent output file activity as a log of pipeline runs."""
    entries: list[dict[str, Any]] = []
    for p in sorted(output_dir.glob("*_30d_*.json"), key=lambda x: x.stat().st_mtime, reverse=True):
        if p.stem.startswith("channel_comparison"):
            continue
        mtime = datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc)
        slug = p.stem.split("_30d_")[0]
        entries.append({"channel": slug, "file": p.name, "timestamp": mtime})
        if len(entries) >= limit:
            break
    return entries


# ── Korean stock name mapping ─────────────────────────────────────────────

KOREAN_STOCK_NAMES: dict[str, str] = {
    "005930.KS": "삼성전자",
    "000660.KS": "SK하이닉스",
    "005380.KS": "현대차",
    "000270.KS": "기아",
    "035420.KS": "NAVER",
    "035720.KS": "카카오",
    "006400.KS": "삼성SDI",
    "051910.KS": "LG화학",
    "373220.KS": "LG에너지솔루션",
    "207940.KS": "삼성바이오로직스",
    "068270.KS": "셀트리온",
    "005490.KS": "POSCO홀딩스",
    "055550.KS": "신한지주",
    "105560.KS": "KB금융",
    "034730.KS": "SK",
    "028260.KS": "삼성물산",
    "012330.KS": "현대모비스",
    "066570.KS": "LG전자",
    "003550.KS": "LG",
    "017670.KS": "SK텔레콤",
    "030200.KS": "KT",
    "086790.KS": "하나금융지주",
    "316140.KS": "우리금융지주",
    "009150.KS": "삼성전기",
    "018260.KS": "삼성에스디에스",
    "036570.KS": "엔씨소프트",
    "259960.KS": "크래프톤",
    "352820.KS": "하이브",
    "247540.KS": "에코프로비엠",
    "086520.KS": "에코프로",
    "042700.KS": "한미반도체",
    "402340.KS": "SK스퀘어",
    "003670.KS": "포스코퓨처엠",
    "096770.KS": "SK이노베이션",
    "010130.KS": "고려아연",
    "032830.KS": "삼성생명",
    "251270.KS": "넷마블",
    "011200.KS": "HMM",
    "329180.KS": "HD현대중공업",
    "042660.KS": "한화오션",
    "267260.KS": "HD현대일렉트릭",
    "003490.KS": "대한항공",
    "015760.KS": "한국전력",
    "090430.KS": "아모레퍼시픽",
    "326030.KS": "SK바이오팜",
    "323410.KS": "카카오뱅크",
    "377300.KS": "카카오페이",
    "009540.KS": "한국조선해양",
    "004020.KS": "현대제철",
    "010950.KS": "S-Oil",
    "011170.KS": "롯데케미칼",
    "004990.KS": "롯데지주",
    # KOSDAQ
    "196170.KQ": "알테오젠",
    "403870.KQ": "HPSP",
    "058470.KQ": "리노공업",
    "039030.KQ": "이오테크닉스",
    "095340.KQ": "ISC",
    "241560.KQ": "두산퓨얼셀",
}


def format_ticker_display(ticker: str, company_name: str = "") -> str:
    """Format ticker for Korean display: code + Korean/English name."""
    kr_name = KOREAN_STOCK_NAMES.get(ticker)
    if kr_name:
        code = ticker.replace(".KS", "").replace(".KQ", "")
        return f"{code} {kr_name}"
    if company_name:
        return f"{ticker} {company_name}"
    return ticker


def format_price(price: float | None, currency: str = "KRW") -> str:
    """Format price with currency symbol."""
    if price is None:
        return "N/A"
    if currency == "KRW":
        return f"₩{price:,.0f}"
    return f"${price:,.2f}"


def get_channel_display_names(
    output_dir: Path = DEFAULT_OUTPUT_DIR,
) -> dict[str, str]:
    """Return {slug: display_name} mapping from comparison or 30d data."""
    comp = load_channel_comparison(output_dir)
    names: dict[str, str] = {}
    if comp and "channels" in comp:
        for slug, info in comp["channels"].items():
            names[slug] = info.get("display_name", slug)
    for slug in get_available_channels(output_dir):
        if slug not in names:
            data = load_30d_results(slug, output_dir)
            names[slug] = data.get("channel_name", slug)
    return names


def get_all_rankings(
    output_dir: Path = DEFAULT_OUTPUT_DIR,
) -> list[dict[str, Any]]:
    """Aggregate cross_video_ranking across all channels, best entry per ticker."""
    best: dict[str, dict[str, Any]] = {}
    for slug in get_available_channels(output_dir):
        data = load_30d_results(slug, output_dir)
        for item in data.get("cross_video_ranking", []):
            ticker = item.get("ticker", "")
            if not ticker:
                continue
            entry = dict(item)
            entry["_source_channel"] = slug
            existing = best.get(ticker)
            if existing is None or entry.get("aggregate_score", 0) > existing.get("aggregate_score", 0):
                best[ticker] = entry
    return sorted(best.values(), key=lambda x: x.get("aggregate_score", 0), reverse=True)
