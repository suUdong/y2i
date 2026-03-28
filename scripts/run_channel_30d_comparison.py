from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import logging
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.request import urlopen
from xml.etree import ElementTree as ET

from omx_brainstorm.app_config import AppConfig, load_app_config
from omx_brainstorm.backtest import YFinanceHistoryProvider
from omx_brainstorm.comparison import RunContext, compare_channels, quality_scorecard, save_channel_artifacts
from omx_brainstorm.daily_report import (
    build_daily_report_payload,
    format_daily_report_telegram_caption,
    save_daily_report,
)
from omx_brainstorm.evaluation import ranking_validation
from omx_brainstorm.fundamentals import FundamentalsFetcher
from omx_brainstorm.heuristic_pipeline import analyze_video_heuristic
from omx_brainstorm.kindshot_feed import export_signals_for_kindshot
from omx_brainstorm.logging_utils import configure_logging
from omx_brainstorm.master_engine import validate_cross_stock_master_quality
from omx_brainstorm.research import build_consensus_ranking, build_cross_video_ranking
from omx_brainstorm.signal_alerts import (
    build_channel_signal_summary,
    send_consensus_signal_alerts,
    send_high_accuracy_target_alerts,
    send_high_confidence_signal_alerts,
)
from omx_brainstorm.signal_tracker import (
    SignalTrackerDB,
    build_signal_accuracy_summary,
    record_signals_from_output,
    save_signal_accuracy_report,
    update_price_snapshots,
)
from omx_brainstorm.transcript_cache import TranscriptCache
from omx_brainstorm.youtube import ChannelRegistry, TranscriptFetcher, YoutubeResolver, describe_youtube_error

logger = logging.getLogger(__name__)

DEFAULT_CHANNELS = {
    "itgod": {
        "display_name": "IT의 신 이형수",
        "url": "https://www.youtube.com/channel/UCQW05vzztAlwV54WL3pjGBQ/videos",
    },
    "kimjakgatv": {
        "display_name": "김작가TV",
        "url": "https://www.youtube.com/@lucky_tv/videos",
    },
}

REFERENCE_KIND_LABELS = {
    "published_at": "게시",
    "generated_at": "스냅샷",
    "unknown": "미분류",
}

SUMMARY_LABELS = {
    "total_channels": "채널 수",
    "total_videos": "분석 영상 수",
    "actionable_videos": "분석 가능 영상",
    "strict_actionable_videos": "엄격 액션 영상",
    "skipped_videos": "스킵 영상",
    "transcript_backed_videos": "실자막 기반 영상",
    "metadata_fallback_videos": "메타 fallback 영상",
    "latest_published_at": "최신 게시일",
    "latest_reference_at": "최신 기준 시각",
    "latest_reference_kind": "최신 기준 출처",
    "ranking_top_1_return_pct": "상위 1개 수익률",
    "ranking_top_3_return_pct": "상위 3개 수익률",
    "ranking_spearman": "순위 상관",
    "ranking_eval_positions": "평가 표본",
    "actionable_ratio": "분석 가능 비율",
}

QUALITY_SCORECARD_LABELS = {
    "overall": "종합",
    "transcript_coverage": "트랜스크립트",
    "actionable_density": "액션 밀도",
    "ranking_predictive_power": "랭킹 예측력",
    "horizon_adequacy": "기간 적합성",
}

SIGNAL_CLASS_LABELS = {
    "ACTIONABLE": "엄격 액션",
    "SECTOR_ONLY": "섹터 참고",
    "LOW_SIGNAL": "저신호",
    "NON_EQUITY": "비주식",
    "NOISE": "노이즈",
    "UNKNOWN": "미분류",
}

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


def _fmt_scalar(value: object) -> str:
    if value is None or value == "":
        return "미제공"
    if isinstance(value, float):
        return f"{value:.3f}".rstrip("0").rstrip(".")
    return str(value)


def _fmt_dateish(value: object) -> str:
    if value is None or value == "":
        return "미제공"
    text = str(value)
    for fmt, out_fmt in (
        ("%Y%m%dT%H%M%SZ", "%Y-%m-%d %H:%M UTC"),
        ("%Y%m%d", "%Y-%m-%d"),
        ("%Y-%m-%d", "%Y-%m-%d"),
    ):
        try:
            return datetime.strptime(text, fmt).strftime(out_fmt)
        except ValueError:
            continue
    return text


def _fmt_run_id(value: object) -> str:
    if value is None or value == "":
        return "미제공"
    text = str(value)
    try:
        return datetime.strptime(text, "%Y%m%dT%H%M%SZ").strftime("%Y-%m-%d %H:%M UTC")
    except ValueError:
        return text


def _fmt_ratio(value: object) -> str:
    if isinstance(value, (int, float)):
        return f"{value:.1%}"
    return _fmt_scalar(value)


def _fmt_reference_kind(value: object) -> str:
    return REFERENCE_KIND_LABELS.get(str(value or "unknown"), str(value or "unknown"))


def _fmt_percentage(value: object) -> str:
    if value is None or value == "":
        return "미제공"
    if isinstance(value, (int, float)):
        return f"{value:.2f}%"
    return str(value)


def _fmt_window_stats(window_stats: dict[str, object], key: str) -> str:
    stats = window_stats.get(key, {}) if isinstance(window_stats, dict) else {}
    tracked = stats.get("tracked", 0)
    hit_rate = stats.get("hit_rate")
    avg_return = stats.get("avg_return")
    if tracked == 0 or hit_rate is None or avg_return is None:
        return f"{key} 미성숙"
    return f"{key} 적중률 {hit_rate:.1f}% | 평균수익률 {avg_return:.2f}% | 표본 {int(tracked)}"


def enrich_comparison_with_signal_accuracy(
    comparison: dict[str, object],
    tracker_db: SignalTrackerDB,
) -> tuple[dict[str, dict[str, object]], list[dict[str, object]], dict[str, object]]:
    channel_comparison_data = comparison.get("channels", {})
    if not isinstance(channel_comparison_data, dict):
        comparison["signal_accuracy"] = {}
        return {}, [], {}

    summary = build_signal_accuracy_summary(tracker_db, channel_metadata=channel_comparison_data)
    comparison["signal_accuracy"] = summary
    accuracy_by_channel = summary.get("by_channel", {})
    leaderboard = summary.get("channel_leaderboard", [])
    quality_by_slug = {item["slug"]: item for item in leaderboard if item.get("slug")}

    for slug, info in channel_comparison_data.items():
        accuracy = accuracy_by_channel.get(slug, {})
        quality = quality_by_slug.get(slug, {})
        info["signal_accuracy"] = accuracy
        info["overall_quality_score"] = quality.get("overall_quality_score")
        info["hit_rate_5d"] = accuracy.get("hit_rate_5d")
        info["hit_rate_10d"] = accuracy.get("hit_rate_10d")
        info["avg_return_5d"] = accuracy.get("avg_return_5d")
        info["avg_return_10d"] = accuracy.get("avg_return_10d")
        info["tracked_signals"] = accuracy.get("total_signals", 0)
        info["tracked_signals_5d"] = accuracy.get("signals_with_price", 0)
        info["target_count"] = accuracy.get("target_count", 0)
        info["target_hit_rate"] = accuracy.get("target_hit_rate")
        info["avg_target_progress_pct"] = accuracy.get("avg_target_progress_pct")
        info["pending_targets"] = accuracy.get("pending_targets", 0)
        info["weight_multiplier"] = quality.get("weight_multiplier")
    return accuracy_by_channel, leaderboard, summary


def _fmt_scorecard(scorecard: dict[str, object]) -> str:
    parts: list[str] = []
    for key in ("overall", "transcript_coverage", "actionable_density", "ranking_predictive_power", "horizon_adequacy"):
        if key in scorecard:
            parts.append(f"{QUALITY_SCORECARD_LABELS[key]} {_fmt_scalar(scorecard[key])}")
    return " | ".join(parts) if parts else "미제공"


def _fmt_signal_breakdown(signal_breakdown: dict[str, object]) -> str:
    if not signal_breakdown:
        return "미제공"
    parts = [
        f"{SIGNAL_CLASS_LABELS.get(key, key)} {value}"
        for key, value in signal_breakdown.items()
    ]
    return " | ".join(parts)


def _fmt_skip_reasons(top_skip_reasons: list[dict[str, object]]) -> str:
    if not top_skip_reasons:
        return "미제공"
    return " | ".join(f"{item.get('reason', '미제공')} ({item.get('count', 0)})" for item in top_skip_reasons)


def _fmt_summary_value(key: str, value: object) -> str:
    if key == "actionable_ratio":
        return _fmt_ratio(value)
    if key in {"ranking_top_1_return_pct", "ranking_top_3_return_pct"}:
        return _fmt_percentage(value)
    if key in {"latest_published_at", "latest_reference_at"}:
        return _fmt_dateish(value)
    if key == "latest_reference_kind":
        return _fmt_reference_kind(value)
    return _fmt_scalar(value)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the 30-day paper-trading style channel comparison job.")
    parser.add_argument("--config", default="config.toml")
    parser.add_argument("--verbose", action="store_true")
    return parser


def register_channels(channels: dict[str, dict], registry_path: Path) -> dict[str, dict]:
    """Register configured channels and resolve channel IDs when possible."""
    resolver = YoutubeResolver()
    registry = ChannelRegistry(registry_path)
    rows = {}
    for slug, config in channels.items():
        try:
            discovery = resolver.discover_channel(config["url"])
            channel_title = discovery.get("channel_title") or config["display_name"]
            channel_id = discovery.get("channel_id")
            channel_url = discovery.get("url") or config["url"]
        except Exception as exc:
            logger.warning("Channel registration metadata lookup failed for %s: %s", slug, exc)
            channel_title = config["display_name"]
            channel_id = None
            channel_url = config["url"]
        rows[slug] = registry.register(
            channel_url,
            {"channel_id": channel_id, "channel_title": channel_title, "source_url": config["url"]},
        )
    return rows


def discover_recent_video_ids(
    channel_url: str,
    channel_id: str | None,
    *,
    days: int = 30,
    today: str | None = None,
    resolver: YoutubeResolver | None = None,
) -> list[str]:
    resolver = resolver or YoutubeResolver()
    rss_ids = recent_feed_video_ids(channel_id or "", days=days, today=today)
    if rss_ids:
        return list(dict.fromkeys(rss_ids))
    try:
        reference_date = date.fromisoformat(today or date.today().isoformat())
        fallback_videos = resolver.resolve_channel_videos_since(channel_url, days=days, reference_date=reference_date)
    except Exception as exc:
        logger.warning("Fallback channel discovery failed for %s: %s", channel_url, exc)
        return []
    return list(dict.fromkeys(video.video_id for video in fallback_videos if video.video_id))


def recent_feed_video_ids(channel_id: str, days: int = 30, *, today: str | None = None) -> list[str]:
    """Return video ids from a channel RSS feed within the desired window."""
    if not channel_id:
        logger.warning("Missing channel_id; returning no videos")
        return []
    cutoff = date.fromisoformat(today or date.today().isoformat()) - timedelta(days=days)
    feed_url = f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
    namespaces = {
        "atom": "http://www.w3.org/2005/Atom",
        "yt": "http://www.youtube.com/xml/schemas/2015",
    }
    try:
        with urlopen(feed_url, timeout=10) as response:
            root = ET.fromstring(response.read())
    except Exception as exc:
        logger.warning("Feed fetch failed for %s: %s", channel_id, exc)
        return []
    entries: list[str] = []
    for entry in root.findall("atom:entry", namespaces):
        published_text = entry.findtext("atom:published", default="", namespaces=namespaces)
        try:
            published_date = date.fromisoformat(published_text[:10])
        except ValueError:
            logger.warning("Skipping malformed RSS entry for %s with published=%r", channel_id, published_text)
            continue
        if published_date < cutoff:
            continue
        video_id = entry.findtext("yt:videoId", default="", namespaces=namespaces)
        if video_id:
            entries.append(video_id)
    return entries


def run_comparison_job(config: AppConfig) -> dict:
    """Run the multi-channel paper-trading comparison job from configuration."""
    output_dir = Path(config.output_dir)
    registry_path = Path(config.registry_path)
    window_days = config.strategy.window_days
    context = RunContext(
        run_id=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"),
        today=date.today().isoformat(),
        output_dir=output_dir,
        window_days=window_days,
    )
    configured_channels = {
        channel.slug: {"display_name": channel.display_name, "url": channel.url}
        for channel in config.channels
        if channel.enabled
    }

    output_dir.mkdir(parents=True, exist_ok=True)
    registry_rows = register_channels(configured_channels, registry_path)
    cache = TranscriptCache()
    cache.warm_from_output_dir(output_dir)
    channel_payloads: dict[str, dict] = {}
    for slug, channel in configured_channels.items():
        logger.info("Starting channel run: %s", slug)
        registry_row = registry_rows[slug]
        channel_id = registry_row.get("channel_id")
        channel_url = registry_row.get("url") or channel["url"]
        video_ids = discover_recent_video_ids(
            str(channel_url),
            str(channel_id or ""),
            days=window_days,
            today=context.today,
        )
        logger.info("Collected %s videos for %s", len(video_ids), slug)
        rows = _analyze_channel_rows(video_ids, cache, config)
        validate_cross_stock_master_quality([stock for row in rows for stock in row["stocks"]])
        ranking = build_cross_video_ranking(rows)
        validation = ranking_validation(ranking, context.today)
        scorecard = quality_scorecard(rows, validation, ranking)
        json_path, txt_path = save_channel_artifacts(
            slug,
            channel["display_name"],
            str(channel_url),
            rows,
            ranking,
            validation,
            scorecard,
            context,
        )
        channel_payloads[slug] = {
            "display_name": channel["display_name"],
            "rows": rows,
            "ranking": ranking,
            "validation": validation,
            "scorecard": scorecard,
            "json_path": str(json_path),
            "txt_path": str(txt_path),
        }

    comparison = compare_channels(channel_payloads, context)

    tracker_db = SignalTrackerDB()
    history_provider = YFinanceHistoryProvider()
    accuracy_by_channel: dict[str, dict[str, object]] = {}
    leaderboard: list[dict[str, object]] = []
    signal_accuracy_summary: dict[str, object] = {}
    signal_accuracy_report: dict[str, str] | None = None
    kindshot_feed: dict[str, object] | None = None
    try:
        total_tracked = 0
        for item in channel_payloads.values():
            json_path_obj = Path(item["json_path"])
            if json_path_obj.exists():
                total_tracked += record_signals_from_output(tracker_db, json_path_obj, history_provider=history_provider)
        if total_tracked:
            logger.info("Tracked %d new signals", total_tracked)
        updated = update_price_snapshots(tracker_db, history_provider=history_provider)
        if updated:
            logger.info("Updated price snapshots for %d signals", updated)
        accuracy_by_channel, leaderboard, signal_accuracy_summary = enrich_comparison_with_signal_accuracy(comparison, tracker_db)
        accuracy_json, accuracy_txt = save_signal_accuracy_report(signal_accuracy_summary, output_dir, context.run_id)
        signal_accuracy_report = {"json_path": str(accuracy_json), "txt_path": str(accuracy_txt)}
        kindshot_feed = export_signals_for_kindshot(
            tracker_db,
            output_dir.parent / ".omx" / "state" / "kindshot_feed.json",
            channel_weights={
                str(item.get("slug", "")): float(item.get("weight_multiplier", 1.0) or 1.0)
                for item in leaderboard
                if item.get("slug")
            },
        )
        comparison["signal_accuracy"] = signal_accuracy_summary
    except Exception as exc:
        logger.warning("Signal tracking failed (non-fatal): %s", exc)

    telegram_payload = build_telegram_payload(channel_payloads, leaderboard, context)
    if not accuracy_by_channel:
        try:
            accuracy_by_channel, leaderboard, signal_accuracy_summary = enrich_comparison_with_signal_accuracy(comparison, tracker_db)
            accuracy_json, accuracy_txt = save_signal_accuracy_report(signal_accuracy_summary, output_dir, context.run_id)
            signal_accuracy_report = {"json_path": str(accuracy_json), "txt_path": str(accuracy_txt)}
            kindshot_feed = export_signals_for_kindshot(
                tracker_db,
                output_dir.parent / ".omx" / "state" / "kindshot_feed.json",
                channel_weights={
                    str(item.get("slug", "")): float(item.get("weight_multiplier", 1.0) or 1.0)
                    for item in leaderboard
                    if item.get("slug")
                },
            )
            comparison["signal_accuracy"] = signal_accuracy_summary
            telegram_payload = build_telegram_payload(channel_payloads, leaderboard, context)
        except Exception as exc:
            logger.warning("Signal accuracy enrichment retry failed (non-fatal): %s", exc)
    comparison["consensus_signals"] = telegram_payload.get("analysis_summary", {}).get("consensus_signals", [])

    daily_report_payload = build_daily_report_payload(channel_payloads, comparison, leaderboard, context)
    daily_report_path = save_daily_report(daily_report_payload, output_dir.parent / "reports")
    daily_report_payload = {
        **daily_report_payload,
        "markdown_path": str(daily_report_path),
        "telegram_caption": format_daily_report_telegram_caption(daily_report_payload),
    }

    compare_json, compare_txt = save_comparison_artifacts(comparison, context)
    dashboard_markdown = None
    try:
        try:
            from scripts.generate_dashboard import generate_dashboard
        except ModuleNotFoundError:
            from generate_dashboard import generate_dashboard

        dashboard_markdown = str(generate_dashboard(output_dir, output_dir.parent / "DASHBOARD.md"))
    except Exception as exc:
        logger.warning("Dashboard markdown generation failed: %s", exc)

    try:
        quality_scores = {item["slug"]: item["overall_quality_score"] for item in leaderboard}
        dynamic_weights = {item["slug"]: float(item.get("weight_multiplier", 1.0) or 1.0) for item in leaderboard}
        for slug, item in channel_payloads.items():
            ranking_dicts = [s.to_dict() for s in item["ranking"]]
            send_high_confidence_signal_alerts(
                config.notifications,
                ranking_dicts,
                channel_name=item["display_name"],
                channel_slug=slug,
                channel_quality_scores=quality_scores,
                min_score=config.strategy.high_confidence_min_score,
                min_channel_quality=config.strategy.signal_alert_min_channel_quality,
                weight_multipliers=dynamic_weights,
            )
        send_consensus_signal_alerts(
            config.notifications,
            telegram_payload.get("analysis_summary", {}).get("consensus_signals", []),
            min_score=config.strategy.high_confidence_min_score,
        )
        send_high_accuracy_target_alerts(
            config.notifications,
            tracker_db.recent_records(limit=30, target_only=True),
            accuracy_by_channel=accuracy_by_channel,
            channel_names={slug: item["display_name"] for slug, item in channel_payloads.items()},
        )
    except Exception as exc:
        logger.warning("High-confidence signal alerts failed (non-fatal): %s", exc)

    payload = {
        "channels": {slug: {"json_path": item["json_path"], "txt_path": item["txt_path"]} for slug, item in channel_payloads.items()},
        "comparison_json": str(compare_json),
        "comparison_txt": str(compare_txt),
        "dashboard_markdown": dashboard_markdown,
        "telegram": telegram_payload,
        "daily_report": daily_report_payload,
        "signal_accuracy_report": signal_accuracy_report,
        "kindshot_feed": kindshot_feed,
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return payload


def _analyze_channel_rows(
    video_ids: list[str],
    cache: TranscriptCache,
    config: AppConfig,
) -> list[dict]:
    resolver = YoutubeResolver()
    fetcher = TranscriptFetcher()
    fundamentals = FundamentalsFetcher(max_workers=config.strategy.fundamentals_workers)

    if not video_ids:
        return []

    workers = min(max(1, config.strategy.video_workers), len(video_ids))
    if workers == 1:
        rows = []
        for video_id in video_ids:
            row = _analyze_single_video(video_id, resolver, fetcher, fundamentals, cache, config)
            if row is not None:
                rows.append(row)
        return rows

    results: list[dict | None] = [None] * len(video_ids)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        future_to_index = {
            pool.submit(_analyze_single_video, video_id, resolver, fetcher, fundamentals, cache, config): idx
            for idx, video_id in enumerate(video_ids)
        }
        for future in as_completed(future_to_index):
            idx = future_to_index[future]
            try:
                results[idx] = future.result()
            except Exception as exc:
                failed_video_id = video_ids[idx] if idx < len(video_ids) else "unknown"
                logger.warning("Parallel video analysis failed for %s: %s", failed_video_id, describe_youtube_error(exc))
    return [row for row in results if row is not None]


def _analyze_single_video(
    video_id: str,
    resolver: YoutubeResolver,
    fetcher: TranscriptFetcher,
    fundamentals: FundamentalsFetcher,
    cache: TranscriptCache,
    config: AppConfig,
) -> dict | None:
    try:
        video = resolver.resolve_video(video_id)
        return analyze_video_heuristic(
            video,
            cache,
            fetcher,
            fundamentals,
            max_fundamental_workers=config.strategy.fundamentals_workers,
        )
    except Exception as exc:
        logger.warning("Skipping video %s due to resolve/analyze failure: %s", video_id, describe_youtube_error(exc))
        return None


def build_telegram_payload(
    channel_payloads: dict[str, dict],
    leaderboard: list[dict[str, object]],
    context: RunContext,
) -> dict[str, object]:
    """Build a compact payload for scheduler-side Telegram notifications."""
    channel_signal_summaries: list[dict[str, object]] = []
    channel_weights = {
        str(item.get("slug", "")): float(item.get("weight_multiplier", 1.0) or 1.0)
        for item in leaderboard
        if item.get("slug")
    }

    for slug, item in channel_payloads.items():
        ranking_dicts = [stock.to_dict() for stock in item.get("ranking", [])]
        summary = build_channel_signal_summary(
            ranking_dicts,
            channel_slug=slug,
            channel_name=item.get("display_name", slug),
        )
        channel_signal_summaries.append(summary)

    top_signals = build_consensus_ranking(
        {
            slug: [stock.to_dict() for stock in item.get("ranking", [])]
            for slug, item in channel_payloads.items()
        },
        channel_weights=channel_weights,
        channel_names={slug: item.get("display_name", slug) for slug, item in channel_payloads.items()},
    )
    consensus_signals = [item for item in top_signals if item.get("consensus_signal")]
    channel_signal_summaries.sort(key=lambda item: item.get("channel_name", ""))
    return {
        "generated_at": context.run_id,
        "analysis_summary": {
            "channel_signal_summaries": channel_signal_summaries,
            "top_signals": top_signals[:5],
            "consensus_signals": consensus_signals[:5],
        },
        "daily_leaderboard": leaderboard[:5],
    }


def save_comparison_artifacts(comparison: dict, context: RunContext) -> tuple[Path, Path]:
    """Persist the multi-channel comparison summary as JSON and text."""
    json_path = context.output_dir / f"channel_comparison_30d_{context.run_id}.json"
    txt_path = context.output_dir / f"channel_comparison_30d_{context.run_id}.txt"
    json_path.write_text(json.dumps(comparison, ensure_ascii=False, indent=2), encoding="utf-8")
    channel_display_names = {
        slug: info.get("display_name", slug)
        for slug, info in comparison.get("channels", {}).items()
    }
    best_analyzable = channel_display_names.get(comparison["more_actionable_channel"], comparison["more_actionable_channel"])
    best_ranking = channel_display_names.get(comparison["better_ranking_channel"], comparison["better_ranking_channel"])
    lines = [
        f"30일 채널 비교 ({context.run_id})",
        f"분석 가능 비율 최고: {best_analyzable}",
        f"랭킹 예측력 최고: {best_ranking}",
        "",
    ]
    pipeline_summary = comparison.get("pipeline_summary", {})
    if pipeline_summary:
        lines.append("[파이프라인 요약]")
        lines.append(f"- 스냅샷 run: {_fmt_run_id(comparison.get('generated_at', context.run_id))}")
        for key in (
            "total_channels",
            "total_videos",
            "actionable_videos",
            "strict_actionable_videos",
            "skipped_videos",
            "transcript_backed_videos",
            "metadata_fallback_videos",
            "latest_published_at",
            "latest_reference_at",
            "latest_reference_kind",
        ):
            value = pipeline_summary.get(key, "")
            lines.append(f"- {SUMMARY_LABELS[key]}: {_fmt_summary_value(key, value)}")
        top_skip_reasons = pipeline_summary.get("top_skip_reasons", [])
        lines.append(f"- 상위 스킵 사유: {_fmt_skip_reasons(top_skip_reasons)}")
        lines.append("")
    signal_accuracy = comparison.get("signal_accuracy", {})
    overall_accuracy = signal_accuracy.get("overall", {}) if isinstance(signal_accuracy, dict) else {}
    if overall_accuracy:
        lines.append("[시그널 정확도]")
        lines.append(f"- 트래킹 신호 수: {overall_accuracy.get('total_signals', 0)}")
        lines.append(
            f"- 1일/3일/5일 표본: "
            f"{overall_accuracy.get('signals_with_price_1d', 0)} / "
            f"{overall_accuracy.get('signals_with_price_3d', 0)} / "
            f"{overall_accuracy.get('signals_with_price_5d', overall_accuracy.get('signals_with_price', 0))}"
        )
        lines.append(f"- 평균 시그널 점수: {_fmt_scalar(overall_accuracy.get('avg_signal_score'))}")
        lines.append(f"- 1일 적중률: {_fmt_percentage(overall_accuracy.get('hit_rate_1d'))}")
        lines.append(f"- 3일 적중률: {_fmt_percentage(overall_accuracy.get('hit_rate_3d'))}")
        lines.append(f"- 5일 적중률: {_fmt_percentage(overall_accuracy.get('hit_rate_5d'))}")
        lines.append(f"- 10일 적중률: {_fmt_percentage(overall_accuracy.get('hit_rate_10d'))}")
        lines.append(f"- 1일 평균수익률: {_fmt_percentage(overall_accuracy.get('avg_return_1d'))}")
        lines.append(f"- 3일 평균수익률: {_fmt_percentage(overall_accuracy.get('avg_return_3d'))}")
        lines.append(f"- 5일 평균수익률: {_fmt_percentage(overall_accuracy.get('avg_return_5d'))}")
        lines.append(f"- 10일 평균수익률: {_fmt_percentage(overall_accuracy.get('avg_return_10d'))}")
        lines.append(f"- 가격타겟 수: {_fmt_scalar(overall_accuracy.get('target_count', 0))}")
        lines.append(f"- 타겟 달성 수: {_fmt_scalar(overall_accuracy.get('target_hits', 0))}")
        lines.append(f"- 타겟 적중률: {_fmt_percentage(overall_accuracy.get('target_hit_rate'))}")
        lines.append(f"- 평균 타겟 진행률: {_fmt_percentage(overall_accuracy.get('avg_target_progress_pct'))}")
        lines.append(f"- 미달성 타겟: {_fmt_scalar(overall_accuracy.get('pending_targets', 0))}")
        lines.append(
            f"- 윈도우 요약: "
            f"{_fmt_window_stats(overall_accuracy.get('window_stats', {}), '1d')} | "
            f"{_fmt_window_stats(overall_accuracy.get('window_stats', {}), '3d')} | "
            f"{_fmt_window_stats(overall_accuracy.get('window_stats', {}), '5d')}"
        )
        lines.append("")
    consensus_accuracy = signal_accuracy.get("consensus_accuracy", {}) if isinstance(signal_accuracy, dict) else {}
    consensus_overall = consensus_accuracy.get("overall", {}) if isinstance(consensus_accuracy, dict) else {}
    if consensus_accuracy:
        lines.append("[합의 시그널 정확도]")
        lines.append(f"- 합의 후보 코호트: {int(consensus_accuracy.get('candidate_cohorts', 0) or 0)}")
        lines.append(f"- 통과 합의 시그널: {int(consensus_accuracy.get('qualified_signals', 0) or 0)}")
        lines.append(f"- 5일 표본: {_fmt_scalar(consensus_overall.get('signals_with_price_5d', 0))}")
        lines.append(f"- 5일 적중률: {_fmt_percentage(consensus_overall.get('hit_rate_5d'))}")
        lines.append(f"- 5일 방향수익률: {_fmt_percentage(consensus_overall.get('avg_directional_return_5d'))}")
        lines.append(f"- 5일 복리 ROI: {_fmt_percentage(consensus_overall.get('compounded_directional_roi_5d'))}")
        lines.append("")
    channel_leaderboard = signal_accuracy.get("channel_leaderboard", []) if isinstance(signal_accuracy, dict) else []
    if channel_leaderboard:
        lines.append("[채널 적중률 리더보드]")
        for idx, item in enumerate(channel_leaderboard[:10], start=1):
            lines.append(
                f"- {idx}. {item.get('display_name', item.get('slug', '-'))}"
                f" | quality={_fmt_scalar(item.get('overall_quality_score'))}"
                f" | weight={_fmt_scalar(item.get('weight_multiplier'))}"
                f" | 5d hit={_fmt_percentage(item.get('hit_rate_5d'))}"
                f" | 5d avg={_fmt_percentage(item.get('avg_return_5d'))}"
                f" | actionable={_fmt_ratio(item.get('actionable_ratio'))}"
            )
        lines.append("")
    ticker_leaderboard = signal_accuracy.get("ticker_leaderboard", []) if isinstance(signal_accuracy, dict) else []
    if ticker_leaderboard:
        lines.append("[종목 적중률 리더보드]")
        for idx, item in enumerate(ticker_leaderboard[:15], start=1):
            display_name = item.get("company_name") or item.get("ticker", "-")
            lines.append(
                f"- {idx}. {display_name} ({item.get('ticker', '-')})"
                f" | 5d hit={_fmt_percentage(item.get('hit_rate_5d'))}"
                f" | 5d dir={_fmt_percentage(item.get('avg_directional_return_5d'))}"
                f" | channels={_fmt_scalar(item.get('channel_count'))}"
                f" | sample={_fmt_scalar(item.get('signals_with_price_5d'))}"
            )
        lines.append("")
    consensus_signals = comparison.get("consensus_signals", [])
    if consensus_signals:
        lines.append("[합의 시그널]")
        for idx, item in enumerate(consensus_signals[:10], start=1):
            display_name = item.get("company_name") or item.get("ticker", "-")
            lines.append(
                f"- {idx}. {display_name} ({item.get('ticker', '-')})"
                f" | score={_fmt_scalar(item.get('aggregate_score'))}"
                f" | strength={CONSENSUS_STRENGTH_LABELS.get(str(item.get('consensus_strength')), item.get('consensus_strength'))}"
                f" | cross_validation={CONSENSUS_STATUS_LABELS.get(str(item.get('cross_validation_status')), item.get('cross_validation_status'))}"
                f" | xval={_fmt_scalar(item.get('cross_validation_score'))}"
                f" | channels={_fmt_scalar(item.get('channel_count'))}"
            )
        lines.append("")
    for slug, info in comparison["channels"].items():
        lines.append(f"[{info['display_name']}]")
        lines.append(f"- 채널 slug: {slug}")
        for key in (
            "total_videos",
            "actionable_videos",
            "strict_actionable_videos",
            "skipped_videos",
            "actionable_ratio",
            "transcript_backed_videos",
            "metadata_fallback_videos",
            "latest_published_at",
            "latest_reference_at",
            "latest_reference_kind",
            "ranking_top_1_return_pct",
            "ranking_top_3_return_pct",
            "ranking_spearman",
            "ranking_eval_positions",
        ):
            value = info.get(key, "")
            lines.append(f"- {SUMMARY_LABELS[key]}: {_fmt_summary_value(key, value)}")
        lines.append(f"- 추적 신호 수: {_fmt_scalar(info.get('tracked_signals', 0))}")
        lines.append(f"- 5일 표본: {_fmt_scalar(info.get('tracked_signals_5d', 0))}")
        lines.append(f"- 채널 가중치: {_fmt_scalar(info.get('weight_multiplier'))}")
        lines.append(f"- 1일 적중률: {_fmt_percentage(info.get('signal_accuracy', {}).get('hit_rate_1d'))}")
        lines.append(f"- 3일 적중률: {_fmt_percentage(info.get('signal_accuracy', {}).get('hit_rate_3d'))}")
        lines.append(f"- 5일 적중률: {_fmt_percentage(info.get('hit_rate_5d'))}")
        lines.append(f"- 10일 적중률: {_fmt_percentage(info.get('hit_rate_10d'))}")
        lines.append(f"- 1일 평균수익률: {_fmt_percentage(info.get('signal_accuracy', {}).get('avg_return_1d'))}")
        lines.append(f"- 3일 평균수익률: {_fmt_percentage(info.get('signal_accuracy', {}).get('avg_return_3d'))}")
        lines.append(f"- 5일 평균수익률: {_fmt_percentage(info.get('avg_return_5d'))}")
        lines.append(f"- 10일 평균수익률: {_fmt_percentage(info.get('avg_return_10d'))}")
        lines.append(f"- 가격타겟 수: {_fmt_scalar(info.get('target_count', 0))}")
        lines.append(f"- 타겟 적중률: {_fmt_percentage(info.get('target_hit_rate'))}")
        lines.append(f"- 평균 타겟 진행률: {_fmt_percentage(info.get('avg_target_progress_pct'))}")
        lines.append(f"- 미달성 타겟: {_fmt_scalar(info.get('pending_targets', 0))}")
        lines.append(f"- 종합 품질 점수: {_fmt_scalar(info.get('overall_quality_score'))}")
        lines.append(f"- 품질 점수표: {_fmt_scorecard(info['quality_scorecard'])}")
        lines.append(f"- 상위 스킵 사유: {_fmt_skip_reasons(info.get('top_skip_reasons', []))}")
        lines.append(f"- 시그널 분포: {_fmt_signal_breakdown(info.get('signal_breakdown', {}))}")
        lines.append("")
    txt_path.write_text("\n".join(lines), encoding="utf-8")
    return json_path, txt_path


def main() -> None:
    args = build_parser().parse_args()
    config = load_app_config(args.config)
    configure_logging(
        verbose=args.verbose,
        json_logs=config.logging.json,
        log_dir=config.logging.log_dir,
        retention_days=config.logging.retention_days,
    )
    run_comparison_job(config)


if __name__ == "__main__":
    main()
