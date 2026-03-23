from __future__ import annotations

import argparse
import json
from datetime import date, datetime, timezone
from pathlib import Path

from omx_brainstorm.app_config import load_app_config
from omx_brainstorm.comparison import RunContext, quality_scorecard, save_channel_artifacts
from omx_brainstorm.evaluation import ranking_validation
from omx_brainstorm.fundamentals import FundamentalsFetcher
from omx_brainstorm.heuristic_pipeline import analyze_video_heuristic, render_heuristic_dashboard
from omx_brainstorm.logging_utils import configure_logging
from omx_brainstorm.notifications import notify_all
from omx_brainstorm.master_engine import validate_cross_stock_master_quality
from omx_brainstorm.research import build_cross_video_ranking
from omx_brainstorm.transcript_cache import TranscriptCache
from omx_brainstorm.youtube import TranscriptFetcher, YoutubeResolver


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run one configured channel through the current 30-day heuristic pipeline.")
    parser.add_argument("slug")
    parser.add_argument("--config", default="config.toml")
    parser.add_argument("--verbose", action="store_true")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    config = load_app_config(args.config)
    configure_logging(verbose=args.verbose, json_logs=config.logging.json, log_dir=config.logging.log_dir, retention_days=config.logging.retention_days)
    channel = next(item for item in config.channels if item.slug == args.slug)

    resolver = YoutubeResolver()
    videos = resolver.resolve_channel_videos_since(channel.url, days=config.strategy.window_days, reference_date=date.today())
    cache = TranscriptCache()
    cache.warm_from_output_dir(Path(config.output_dir))
    fetcher = TranscriptFetcher()
    fundamentals = FundamentalsFetcher()

    rows = [analyze_video_heuristic(video, cache, fetcher, fundamentals) for video in videos]
    validate_cross_stock_master_quality([stock for row in rows for stock in row["stocks"]])
    ranking = build_cross_video_ranking(rows)
    context = RunContext(
        run_id=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"),
        today=date.today().isoformat(),
        output_dir=Path(config.output_dir),
        window_days=config.strategy.window_days,
    )
    validation = ranking_validation(ranking, context.today)
    scorecard = quality_scorecard(rows, validation, ranking)
    json_path, txt_path = save_channel_artifacts(
        channel.slug,
        channel.display_name,
        channel.url,
        rows,
        ranking,
        validation,
        scorecard,
        context,
    )
    dashboard_path = render_heuristic_dashboard(rows, Path(config.output_dir), label=f"{channel.slug}_30d_dashboard")
    summary = f"[OMX] {channel.display_name} 30일 분석 완료: {len(rows)}개 영상, {len(ranking)}개 종목 랭킹"
    notify_all(config.notifications, summary)
    print(json.dumps({
        "json_path": str(json_path),
        "txt_path": str(txt_path),
        "dashboard_path": str(dashboard_path) if dashboard_path else None,
        "video_count": len(rows),
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
