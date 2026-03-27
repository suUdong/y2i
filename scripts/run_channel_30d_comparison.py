from __future__ import annotations

import argparse
import json
import logging
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.request import urlopen
from xml.etree import ElementTree as ET

from omx_brainstorm.app_config import AppConfig, load_app_config
from omx_brainstorm.backtest import YFinanceHistoryProvider
from omx_brainstorm.channel_quality import compute_channel_quality, rank_channels
from omx_brainstorm.comparison import RunContext, compare_channels, quality_scorecard, save_channel_artifacts
from omx_brainstorm.evaluation import ranking_validation
from omx_brainstorm.fundamentals import FundamentalsFetcher
from omx_brainstorm.heuristic_pipeline import analyze_video_heuristic
from omx_brainstorm.logging_utils import configure_logging
from omx_brainstorm.master_engine import validate_cross_stock_master_quality
from omx_brainstorm.research import build_cross_video_ranking
from omx_brainstorm.signal_alerts import send_signal_alerts
from omx_brainstorm.signal_tracker import SignalTrackerDB, record_signals_from_output, update_price_snapshots
from omx_brainstorm.transcript_cache import TranscriptCache
from omx_brainstorm.youtube import ChannelRegistry, TranscriptFetcher, YoutubeResolver

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
) -> tuple[dict[str, dict[str, object]], list[dict[str, object]]]:
    channel_comparison_data = comparison.get("channels", {})
    if not isinstance(channel_comparison_data, dict):
        comparison["signal_accuracy"] = {}
        return {}, []

    accuracy_by_channel: dict[str, dict[str, object]] = {}
    for slug in channel_comparison_data:
        accuracy_by_channel[slug] = tracker_db.accuracy_report(slug).to_dict()

    overall_accuracy = tracker_db.accuracy_report().to_dict()
    ranked_reports = rank_channels(compute_channel_quality(channel_comparison_data, accuracy_by_channel))
    leaderboard = [report.to_dict() for report in ranked_reports]
    quality_by_slug = {item["slug"]: item for item in leaderboard}

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

    updated_at = max(
        (record.last_updated for record in tracker_db.records if record.last_updated),
        default="",
    )
    comparison["signal_accuracy"] = {
        "updated_at": updated_at,
        "overall": overall_accuracy,
        "by_channel": accuracy_by_channel,
        "recent_signals": tracker_db.recent_records(limit=12),
        "channel_leaderboard": leaderboard,
    }
    return accuracy_by_channel, leaderboard


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
            videos = resolver.resolve_channel_videos(config["url"], limit=1)
            channel_title = videos[0].channel_title if videos else config["display_name"]
            channel_id = videos[0].channel_id if videos else None
        except Exception as exc:
            logger.warning("Channel registration metadata lookup failed for %s: %s", slug, exc)
            channel_title = config["display_name"]
            channel_id = None
        rows[slug] = registry.register(config["url"], {"channel_id": channel_id, "channel_title": channel_title})
    return rows


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
        with urlopen(feed_url) as response:
            root = ET.fromstring(response.read())
    except Exception as exc:
        logger.warning("Feed fetch failed for %s: %s", channel_id, exc)
        return []
    entries: list[str] = []
    for entry in root.findall("atom:entry", namespaces):
        published_text = entry.findtext("atom:published", default="", namespaces=namespaces)
        published_date = date.fromisoformat(published_text[:10])
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
    resolver = YoutubeResolver()
    fetcher = TranscriptFetcher()
    fundamentals = FundamentalsFetcher()

    channel_payloads: dict[str, dict] = {}
    for slug, channel in configured_channels.items():
        logger.info("Starting channel run: %s", slug)
        channel_id = registry_rows[slug].get("channel_id")
        video_ids = recent_feed_video_ids(channel_id, days=window_days, today=context.today)
        logger.info("Collected %s videos for %s", len(video_ids), slug)
        rows = []
        for video_id in video_ids:
            try:
                video = resolver.resolve_video(video_id)
                rows.append(analyze_video_heuristic(video, cache, fetcher, fundamentals))
            except Exception as exc:
                logger.warning("Skipping video %s for %s due to resolve/analyze failure: %s", video_id, slug, exc)
        validate_cross_stock_master_quality([stock for row in rows for stock in row["stocks"]])
        ranking = build_cross_video_ranking(rows)
        validation = ranking_validation(ranking, context.today)
        scorecard = quality_scorecard(rows, validation, ranking)
        json_path, txt_path = save_channel_artifacts(
            slug,
            channel["display_name"],
            channel["url"],
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
        accuracy_by_channel, leaderboard = enrich_comparison_with_signal_accuracy(comparison, tracker_db)
    except Exception as exc:
        logger.warning("Signal tracking failed (non-fatal): %s", exc)

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
        channel_comparison_data = comparison.get("channels", {})
        if not accuracy_by_channel:
            accuracy_by_channel, leaderboard = enrich_comparison_with_signal_accuracy(comparison, tracker_db)
        quality_scores = {item["slug"]: item["overall_quality_score"] for item in leaderboard}
        for slug, item in channel_payloads.items():
            ranking_dicts = [s.to_dict() for s in item["ranking"]]
            send_signal_alerts(
                config.notifications,
                ranking_dicts,
                channel_name=item["display_name"],
                channel_slug=slug,
                channel_quality_scores=quality_scores,
                min_score=config.strategy.signal_alert_min_score,
                min_channel_quality=config.strategy.signal_alert_min_channel_quality,
            )
    except Exception as exc:
        logger.warning("Signal alerts failed (non-fatal): %s", exc)

    payload = {
        "channels": {slug: {"json_path": item["json_path"], "txt_path": item["txt_path"]} for slug, item in channel_payloads.items()},
        "comparison_json": str(compare_json),
        "comparison_txt": str(compare_txt),
        "dashboard_markdown": dashboard_markdown,
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return payload


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
        lines.append(f"- 5일 표본: {overall_accuracy.get('signals_with_price', 0)}")
        lines.append(f"- 평균 시그널 점수: {_fmt_scalar(overall_accuracy.get('avg_signal_score'))}")
        lines.append(f"- 5일 적중률: {_fmt_percentage(overall_accuracy.get('hit_rate_5d'))}")
        lines.append(f"- 10일 적중률: {_fmt_percentage(overall_accuracy.get('hit_rate_10d'))}")
        lines.append(f"- 5일 평균수익률: {_fmt_percentage(overall_accuracy.get('avg_return_5d'))}")
        lines.append(f"- 10일 평균수익률: {_fmt_percentage(overall_accuracy.get('avg_return_10d'))}")
        lines.append(f"- 1일/3일/20일: {_fmt_window_stats(overall_accuracy.get('window_stats', {}), '1d')} | {_fmt_window_stats(overall_accuracy.get('window_stats', {}), '3d')} | {_fmt_window_stats(overall_accuracy.get('window_stats', {}), '20d')}")
        lines.append("")
    channel_leaderboard = signal_accuracy.get("channel_leaderboard", []) if isinstance(signal_accuracy, dict) else []
    if channel_leaderboard:
        lines.append("[채널 적중률 리더보드]")
        for idx, item in enumerate(channel_leaderboard[:10], start=1):
            lines.append(
                f"- {idx}. {item.get('display_name', item.get('slug', '-'))}"
                f" | quality={_fmt_scalar(item.get('overall_quality_score'))}"
                f" | 5d hit={_fmt_percentage(item.get('hit_rate_5d'))}"
                f" | 5d avg={_fmt_percentage(item.get('avg_return_5d'))}"
                f" | actionable={_fmt_ratio(item.get('actionable_ratio'))}"
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
        lines.append(f"- 5일 적중률: {_fmt_percentage(info.get('hit_rate_5d'))}")
        lines.append(f"- 10일 적중률: {_fmt_percentage(info.get('hit_rate_10d'))}")
        lines.append(f"- 5일 평균수익률: {_fmt_percentage(info.get('avg_return_5d'))}")
        lines.append(f"- 10일 평균수익률: {_fmt_percentage(info.get('avg_return_10d'))}")
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
