"""Data loading utilities for the OMX Streamlit dashboard."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parent.parent / "output"


def _parse_timestamp(value: str) -> datetime | None:
    """Parse compact or ISO-like timestamps to aware UTC datetimes."""
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


def _latest_file(output_dir: Path, pattern: str) -> Path | None:
    """Return the most recently modified file matching *pattern*, or None."""
    matches = sorted(output_dir.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    return matches[0] if matches else None


def _file_for_run(output_dir: Path, pattern: str, run_id: str | None) -> Path | None:
    if run_id:
        exact = output_dir / pattern.format(run_id=run_id)
        if exact.exists():
            return exact
    return _latest_file(output_dir, pattern.format(run_id="*"))


def _load_json(path: Path | None) -> dict[str, Any] | list[Any]:
    if path is None or not path.exists():
        return {}
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _is_newer_timestamp(candidate: str, current: str) -> bool:
    candidate_ts = _parse_timestamp(candidate)
    current_ts = _parse_timestamp(current)
    if candidate_ts is None:
        return False
    if current_ts is None:
        return True
    return candidate_ts > current_ts


def _is_older_timestamp(candidate: str, current: str) -> bool:
    candidate_ts = _parse_timestamp(candidate)
    current_ts = _parse_timestamp(current)
    if candidate_ts is None:
        return False
    if current_ts is None:
        return True
    return candidate_ts < current_ts


def _is_transcript_backed(language: str | None) -> bool:
    return (language or "").startswith("cache") or language not in {None, "", "metadata_fallback"}


def _derive_channel_health(data_30d: dict[str, Any], slug: str) -> dict[str, Any]:
    videos = data_30d.get("videos", [])
    signal_distribution: dict[str, int] = {}
    skip_reason_counts: dict[str, int] = {}
    actionable_videos = 0
    strict_actionable_videos = 0
    transcript_backed_videos = 0
    metadata_fallback_videos = 0
    latest_published_at = data_30d.get("generated_at", "") or ""

    for video in videos:
        signal_class = video.get("video_signal_class", "UNKNOWN")
        signal_distribution[signal_class] = signal_distribution.get(signal_class, 0) + 1
        if video.get("should_analyze_stocks"):
            actionable_videos += 1
        if signal_class == "ACTIONABLE":
            strict_actionable_videos += 1
        else:
            skip_reason = (video.get("skip_reason") or video.get("reason") or "").strip()
            if skip_reason:
                skip_reason_counts[skip_reason] = skip_reason_counts.get(skip_reason, 0) + 1

        transcript_language = video.get("transcript_language")
        if transcript_language == "metadata_fallback":
            metadata_fallback_videos += 1
        elif _is_transcript_backed(transcript_language):
            transcript_backed_videos += 1

        published_at = video.get("published_at", "")
        if _is_newer_timestamp(published_at, latest_published_at):
            latest_published_at = published_at

    top_skip_reasons = [
        {"reason": reason, "count": count}
        for reason, count in sorted(skip_reason_counts.items(), key=lambda item: (-item[1], item[0]))[:5]
    ]
    return {
        "display_name": data_30d.get("channel_name", slug),
        "total_videos": len(videos),
        "actionable_videos": actionable_videos,
        "analyzable_videos": actionable_videos,
        "strict_actionable_videos": strict_actionable_videos,
        "actionable_ratio": round(actionable_videos / len(videos), 3) if videos else 0.0,
        "skipped_videos": len(videos) - actionable_videos,
        "transcript_backed_videos": transcript_backed_videos,
        "metadata_fallback_videos": metadata_fallback_videos,
        "latest_published_at": latest_published_at,
        "signal_breakdown": signal_distribution,
        "top_skip_reasons": top_skip_reasons,
        "quality_scorecard": data_30d.get("quality_scorecard", {}),
    }


def _build_pipeline_summary_from_channels(channels: dict[str, dict[str, Any]]) -> dict[str, Any]:
    signal_distribution: dict[str, int] = {}
    skip_reason_counts: dict[str, int] = {}
    latest_published_at = ""
    total_videos = 0
    actionable_videos = 0
    strict_actionable_videos = 0
    skipped_videos = 0
    transcript_backed_videos = 0
    metadata_fallback_videos = 0

    for info in channels.values():
        total_videos += info.get("total_videos", 0)
        actionable_videos += info.get("actionable_videos", 0)
        strict_actionable_videos += info.get("strict_actionable_videos", 0)
        skipped_videos += info.get("skipped_videos", 0)
        transcript_backed_videos += info.get("transcript_backed_videos", 0)
        metadata_fallback_videos += info.get("metadata_fallback_videos", 0)
        latest_value = info.get("latest_published_at", "")
        if _is_newer_timestamp(latest_value, latest_published_at):
            latest_published_at = latest_value

        for klass, count in info.get("signal_breakdown", {}).items():
            signal_distribution[klass] = signal_distribution.get(klass, 0) + count
        for item in info.get("top_skip_reasons", []):
            reason = item.get("reason", "")
            if reason:
                skip_reason_counts[reason] = skip_reason_counts.get(reason, 0) + int(item.get("count", 0))

    top_skip_reasons = [
        {"reason": reason, "count": count}
        for reason, count in sorted(skip_reason_counts.items(), key=lambda item: (-item[1], item[0]))[:5]
    ]
    return {
        "total_channels": len(channels),
        "total_videos": total_videos,
        "actionable_videos": actionable_videos,
        "analyzable_videos": actionable_videos,
        "strict_actionable_videos": strict_actionable_videos,
        "skipped_videos": skipped_videos,
        "transcript_backed_videos": transcript_backed_videos,
        "metadata_fallback_videos": metadata_fallback_videos,
        "latest_published_at": latest_published_at,
        "signal_breakdown": signal_distribution,
        "top_skip_reasons": top_skip_reasons,
    }


# ── Integration report (sampro_integration_report.json) ──────────────────────

def load_integration_report(output_dir: Path = DEFAULT_OUTPUT_DIR) -> dict[str, Any]:
    path = output_dir / "sampro_integration_report.json"
    return _load_json(path)


# ── 30-day channel results ───────────────────────────────────────────────────

def load_30d_results(
    channel_slug: str,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    preferred_run_id: str | None = None,
) -> dict[str, Any]:
    path = _file_for_run(output_dir, f"{channel_slug}_30d_{{run_id}}.json", preferred_run_id)
    return _load_json(path)


# ── Channel comparison ───────────────────────────────────────────────────────

def load_channel_comparison(output_dir: Path = DEFAULT_OUTPUT_DIR) -> dict[str, Any]:
    path = _latest_file(output_dir, "channel_comparison_30d_*.json")
    comparison = _load_json(path)
    if not isinstance(comparison, dict):
        return {}

    comparison_run_id = comparison.get("generated_at") if isinstance(comparison, dict) else None
    enriched_channels: dict[str, dict[str, Any]] = {}
    for slug in get_available_channels(output_dir):
        data_30d = load_30d_results(slug, output_dir, preferred_run_id=comparison_run_id)
        if isinstance(data_30d, dict) and data_30d:
            enriched_channels[slug] = _derive_channel_health(data_30d, slug)

    existing_channels = comparison.get("channels", {})
    merged_channels: dict[str, dict[str, Any]] = {}
    for slug in sorted(set(enriched_channels) | set(existing_channels)):
        merged = dict(existing_channels.get(slug, {}))
        if slug in enriched_channels:
            derived = enriched_channels[slug]
            for key in (
                "display_name",
                "total_videos",
                "actionable_videos",
                "analyzable_videos",
                "strict_actionable_videos",
                "actionable_ratio",
                "skipped_videos",
                "transcript_backed_videos",
                "metadata_fallback_videos",
                "latest_published_at",
                "signal_breakdown",
                "top_skip_reasons",
                "quality_scorecard",
            ):
                merged[key] = derived.get(key)
        merged_channels[slug] = merged

    if merged_channels:
        comparison["channels"] = merged_channels
        comparison["pipeline_summary"] = _build_pipeline_summary_from_channels(merged_channels)

    return comparison


# ── Video titles with labels ─────────────────────────────────────────────────

def load_video_titles(output_dir: Path = DEFAULT_OUTPUT_DIR) -> dict[str, Any]:
    path = output_dir / "sampro_video_titles.json"
    return _load_json(path)


def load_all_video_titles(output_dir: Path = DEFAULT_OUTPUT_DIR) -> dict[str, Any]:
    """Load and merge title-label data across all available channels."""
    merged_titles: list[dict[str, Any]] = []
    channels: list[dict[str, str]] = []

    for path in sorted(output_dir.glob("*_video_titles.json")):
        data = _load_json(path)
        if not isinstance(data, dict):
            continue

        slug = path.stem.removesuffix("_video_titles")
        channel_name = (
            data.get("channel_name")
            or data.get("channel")
            or slug
        )
        channels.append({"slug": slug, "channel_name": channel_name})

        for item in data.get("titles", []):
            row = dict(item)
            row.setdefault("_channel", slug)
            row.setdefault("_channel_name", channel_name)
            merged_titles.append(row)

    if not merged_titles and not channels:
        return {}

    return {"channels": channels, "titles": merged_titles}


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
            row = dict(expert)
            row["source_video"] = v.get("title", v.get("video_id", ""))
            insights.append(row)
    return insights


def extract_macro_signals(videos: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Collect macro insights from per-video data in a 30d result."""
    signals = []
    for v in videos:
        for macro in v.get("macro_insights", []):
            row = dict(macro)
            row["source_video"] = v.get("title", v.get("video_id", ""))
            signals.append(row)
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
        file_mtime = p.stat().st_mtime
        if file_mtime < cutoff:
            continue
        data = _load_json(p)
        slug = data.get("channel_slug", p.stem.split("_30d_")[0])
        updated_at = datetime.fromtimestamp(file_mtime, tz=timezone.utc)
        for v in data.get("videos", []):
            row = dict(v)
            row["_channel"] = slug
            row["_updated_at"] = updated_at
            row["skip_reason"] = row.get("skip_reason", row.get("reason", ""))
            recent.append(row)
    recent.sort(
        key=lambda item: (
            _parse_timestamp(item.get("published_at", "") or "") or datetime.min.replace(tzinfo=timezone.utc),
            item.get("_updated_at") or datetime.min.replace(tzinfo=timezone.utc),
            item.get("signal_score", 0),
            item.get("title", ""),
        ),
        reverse=True,
    )
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


def build_overview_report(
    output_dir: Path = DEFAULT_OUTPUT_DIR,
) -> dict[str, Any]:
    """Build an aggregate overview report from all latest 30d channel outputs."""
    type_distribution: dict[str, int] = {}
    signal_distribution: dict[str, int] = {}
    per_video: list[dict[str, Any]] = []
    total_videos = 0
    analyzable_count = 0
    macro_video_count = 0
    expert_video_count = 0
    ranked_stock_count = 0

    channel_names = get_channel_display_names(output_dir)
    channels = get_available_channels(output_dir)

    for slug in channels:
        data = load_30d_results(slug, output_dir)
        videos = data.get("videos", [])
        ranked_stock_count += len(data.get("cross_video_ranking", []))

        for video in videos:
            total_videos += 1

            video_type = video.get("video_type", "OTHER")
            type_distribution[video_type] = type_distribution.get(video_type, 0) + 1

            signal_class = video.get("video_signal_class", "UNKNOWN")
            signal_distribution[signal_class] = signal_distribution.get(signal_class, 0) + 1

            if video.get("should_analyze_stocks") or signal_class == "ACTIONABLE":
                analyzable_count += 1
            if video.get("macro_insights") or video_type in {"MACRO", "MARKET_REVIEW"}:
                macro_video_count += 1
            if video.get("expert_insights") or video_type == "EXPERT_INTERVIEW":
                expert_video_count += 1

            per_video.append({
                "channel": channel_names.get(slug, slug),
                "title": video.get("title", ""),
                "video_type": video_type,
                "signal_class": signal_class,
                "signal_score": video.get("signal_score", 0),
                "published_at": video.get("published_at", ""),
                "skip_reason": video.get("skip_reason", video.get("reason", "")),
            })

    per_video.sort(
        key=lambda item: (
            _parse_timestamp(item.get("published_at", "") or "") or datetime.min.replace(tzinfo=timezone.utc),
            item.get("signal_score", 0),
        ),
        reverse=True,
    )

    return {
        "channel_count": len(channels),
        "total_videos": total_videos,
        "analyzable_count": analyzable_count,
        "macro_video_count": macro_video_count,
        "expert_video_count": expert_video_count,
        "ranked_stock_count": ranked_stock_count,
        "type_distribution": type_distribution,
        "signal_distribution": signal_distribution,
        "per_video": per_video,
    }


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
    # Pipeline output tickers
    "240810.KQ": "원익IPS",
    "012450.KS": "한화에어로스페이스",
    "047810.KS": "한국항공우주",
    "007660.KS": "이수페타시스",
    "131970.KQ": "두산테스나",
    "222800.KQ": "심텍",
    "399720.KQ": "가온칩스",
    "253590.KQ": "네오셈",
    "036010.KQ": "에이비코전자",
    "005830.KS": "DB손해보험",
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
        return "미제공"
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
    """Aggregate cross_video_ranking across all channels with proper multi-channel aggregation."""
    agg: dict[str, dict[str, Any]] = {}
    channel_names = get_channel_display_names(output_dir)

    for slug in get_available_channels(output_dir):
        data = load_30d_results(slug, output_dir)
        for item in data.get("cross_video_ranking", []):
            ticker = item.get("ticker", "")
            if not ticker:
                continue
            score = item.get("aggregate_score", 0)
            appearances = item.get("appearances", 0)

            if ticker not in agg:
                agg[ticker] = {
                    "ticker": ticker,
                    "company_name": item.get("company_name", ""),
                    "aggregate_score": score,
                    "aggregate_verdict": item.get("aggregate_verdict", "WATCH"),
                    "appearances": appearances,
                    "total_mentions": item.get("total_mentions", 0),
                    "latest_price": item.get("latest_price"),
                    "currency": item.get("currency", "KRW"),
                    "last_signal_at": item.get("last_signal_at", ""),
                    "first_signal_at": item.get("first_signal_at", ""),
                    "latest_checked_at": item.get("latest_checked_at", ""),
                    "source_video_titles": list(item.get("source_video_titles", [])),
                    "_source_channels": [slug],
                    "_channel_scores": [score],
                    "_source_channel": slug,
                }
            else:
                existing = agg[ticker]
                existing["_source_channels"].append(slug)
                existing["_channel_scores"].append(score)
                existing["appearances"] += appearances
                existing["total_mentions"] = existing.get("total_mentions", 0) + item.get("total_mentions", 0)
                existing["source_video_titles"].extend(item.get("source_video_titles", []))
                # Keep best price/company info from the strongest scoring channel.
                if score > max(existing["_channel_scores"][:-1]):
                    existing["company_name"] = item.get("company_name") or existing["company_name"]
                    existing["latest_price"] = item.get("latest_price") or existing["latest_price"]
                    existing["currency"] = item.get("currency", existing["currency"])
                if _is_newer_timestamp(item.get("latest_checked_at", ""), existing.get("latest_checked_at", "")):
                    existing["latest_checked_at"] = item.get("latest_checked_at") or existing["latest_checked_at"]
                    existing["latest_price"] = item.get("latest_price") or existing["latest_price"]
                    existing["currency"] = item.get("currency", existing["currency"])
                if _is_newer_timestamp(item.get("last_signal_at", ""), existing.get("last_signal_at", "")):
                    existing["last_signal_at"] = item.get("last_signal_at") or existing["last_signal_at"]
                if _is_older_timestamp(item.get("first_signal_at", ""), existing.get("first_signal_at", "")):
                    existing["first_signal_at"] = item.get("first_signal_at") or existing["first_signal_at"]

    # Compute weighted average score and best verdict
    for entry in agg.values():
        scores = entry.pop("_channel_scores")
        entry["aggregate_score"] = sum(scores) / len(scores) if scores else 0
        entry["channel_count"] = len(entry["_source_channels"])
        # Boost score by channel breadth: +5 per additional channel
        entry["aggregate_score"] += (entry["channel_count"] - 1) * 5
        # Pick best verdict from score
        s = entry["aggregate_score"]
        if s >= 65:
            entry["aggregate_verdict"] = "BUY"
        elif s >= 50:
            entry["aggregate_verdict"] = "WATCH"
        else:
            entry["aggregate_verdict"] = "REJECT"
        # Display name for source channel (use first one)
        entry["_source_channel"] = entry["_source_channels"][0]
        entry["_source_channels_display"] = [channel_names.get(ch, ch) for ch in entry["_source_channels"]]

    return sorted(
        agg.values(),
        key=lambda x: (
            x.get("aggregate_score", 0),
            x.get("channel_count", 0),
            x.get("appearances", 0),
            _parse_timestamp(x.get("latest_checked_at", "") or "") or datetime.min.replace(tzinfo=timezone.utc),
            x.get("ticker", ""),
        ),
        reverse=True,
    )
