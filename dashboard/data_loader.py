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
