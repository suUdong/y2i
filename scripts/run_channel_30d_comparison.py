from __future__ import annotations

import argparse
import json
import logging
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.request import urlopen
from xml.etree import ElementTree as ET

from omx_brainstorm.app_config import AppConfig, load_app_config
from omx_brainstorm.comparison import RunContext, compare_channels, quality_scorecard, save_channel_artifacts
from omx_brainstorm.evaluation import ranking_validation
from omx_brainstorm.fundamentals import FundamentalsFetcher
from omx_brainstorm.heuristic_pipeline import analyze_video_heuristic
from omx_brainstorm.logging_utils import configure_logging
from omx_brainstorm.master_engine import validate_cross_stock_master_quality
from omx_brainstorm.research import build_cross_video_ranking
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
    compare_json, compare_txt = save_comparison_artifacts(comparison, context)
    dashboard_markdown = None
    try:
        from scripts.generate_dashboard import generate_dashboard

        dashboard_markdown = str(generate_dashboard(output_dir, output_dir.parent / "DASHBOARD.md"))
    except Exception as exc:
        logger.warning("Dashboard markdown generation failed: %s", exc)
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
    lines = [
        f"30일 채널 비교 ({context.run_id})",
        f"분석 가능 비율 최고: {comparison['more_actionable_channel']}",
        f"더 나은 랭킹 예측력: {comparison['better_ranking_channel']}",
        "",
    ]
    pipeline_summary = comparison.get("pipeline_summary", {})
    if pipeline_summary:
        lines.extend(
            [
                "[파이프라인 요약]",
                f"- total_channels: {pipeline_summary.get('total_channels', 0)}",
                f"- total_videos: {pipeline_summary.get('total_videos', 0)}",
                f"- actionable_videos: {pipeline_summary.get('actionable_videos', 0)}",
                f"- strict_actionable_videos: {pipeline_summary.get('strict_actionable_videos', 0)}",
                f"- skipped_videos: {pipeline_summary.get('skipped_videos', 0)}",
                f"- transcript_backed_videos: {pipeline_summary.get('transcript_backed_videos', 0)}",
                f"- metadata_fallback_videos: {pipeline_summary.get('metadata_fallback_videos', 0)}",
                f"- latest_published_at: {pipeline_summary.get('latest_published_at', '')}",
            ]
        )
        top_skip_reasons = pipeline_summary.get("top_skip_reasons", [])
        if top_skip_reasons:
            lines.append("- top_skip_reasons:")
            for item in top_skip_reasons:
                lines.append(f"  - {item['reason']} ({item['count']})")
        lines.append("")
    for slug, info in comparison["channels"].items():
        lines.extend(
            [
                f"[{slug}] {info['display_name']}",
                f"- total_videos: {info['total_videos']}",
                f"- actionable_videos: {info['actionable_videos']}",
                f"- strict_actionable_videos: {info.get('strict_actionable_videos', 0)}",
                f"- skipped_videos: {info.get('skipped_videos', 0)}",
                f"- actionable_ratio: {info['actionable_ratio']}",
                f"- transcript_backed_videos: {info.get('transcript_backed_videos', 0)}",
                f"- metadata_fallback_videos: {info.get('metadata_fallback_videos', 0)}",
                f"- latest_published_at: {info.get('latest_published_at', '')}",
                f"- ranking_top_1_return_pct: {info['ranking_top_1_return_pct']}",
                f"- ranking_top_3_return_pct: {info['ranking_top_3_return_pct']}",
                f"- ranking_spearman: {info['ranking_spearman']}",
                f"- ranking_eval_positions: {info['ranking_eval_positions']}",
                f"- quality_scorecard: {json.dumps(info['quality_scorecard'], ensure_ascii=False)}",
            ]
        )
        if info.get("top_skip_reasons"):
            lines.append(f"- top_skip_reasons: {json.dumps(info['top_skip_reasons'], ensure_ascii=False)}")
        if info.get("signal_breakdown"):
            lines.append(f"- signal_breakdown: {json.dumps(info['signal_breakdown'], ensure_ascii=False)}")
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
