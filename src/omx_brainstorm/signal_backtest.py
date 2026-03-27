from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from .app_config import AppConfig, load_app_config
from .channel_quality import compute_channel_quality, compute_dynamic_weights, rank_channels
from .comparison import quality_scorecard, summarize_channel_run
from .evaluation import ranking_spearman, ranking_validation
from .fundamentals import FundamentalsFetcher
from .heuristic_pipeline import analyze_video_heuristic
from .research import build_cross_video_ranking
from .signal_tracker import (
    SignalTrackerDB,
    build_signal_backtest_summary,
    record_signals_from_ranking,
    record_signals_from_rows,
    save_signal_backtest_report,
    update_price_snapshots,
)
from .transcript_cache import TranscriptCache
from .youtube import TranscriptFetcher, VideoInput, YoutubeResolver

logger = logging.getLogger(__name__)


class _CacheOnlyTranscriptFetcher(TranscriptFetcher):
    """Historical backfill should prefer cache and fall back immediately on blocked live fetches."""

    def fetch(self, video_id: str, preferred_languages=None):  # type: ignore[override]
        raise RuntimeError(f"cache-only historical backfill for {video_id}")


def run_signal_backtest_workflow(
    *,
    config_path: str | Path = "config.toml",
    tracker_db_path: str | Path = ".omx/state/signal_tracker.json",
    output_dir: str | Path | None = None,
    lookback_days: int = 90,
    top_filters: int = 10,
    min_filter_sample: int = 3,
) -> dict[str, Any]:
    """Backfill recent signals, refresh returns, and persist a lookback report."""
    config = load_app_config(config_path)
    resolved_output_dir = Path(output_dir or config.output_dir)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    tracker_db = SignalTrackerDB(Path(tracker_db_path))

    channel_metadata, backfill_stats = backfill_signal_tracker(
        config,
        tracker_db,
        lookback_days=lookback_days,
    )
    summary = build_signal_backtest_summary(
        tracker_db,
        lookback_days=lookback_days,
        channel_metadata=channel_metadata,
        top_filters=top_filters,
        min_filter_sample=min_filter_sample,
    )
    summary["backfill"] = backfill_stats
    json_path, txt_path = save_signal_backtest_report(summary, resolved_output_dir, run_id)
    return {
        "generated_at": summary.get("generated_at", run_id),
        "tracker_db": str(Path(tracker_db_path)),
        "lookback_days": lookback_days,
        "json_path": str(json_path),
        "txt_path": str(txt_path),
        "total_signals": summary.get("overall", {}).get("total_signals", 0),
        "channel_count": len(summary.get("by_channel", {})),
        "top_filter_count": len(summary.get("filter_recommendations", [])),
        "backfill": backfill_stats,
    }


def backfill_signal_tracker(
    config: AppConfig,
    tracker_db: SignalTrackerDB,
    *,
    lookback_days: int,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    """Refresh tracker records by re-analyzing enabled channels over a lookback window."""
    output_dir = Path(config.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    resolver = YoutubeResolver()
    fetcher = _CacheOnlyTranscriptFetcher()
    fundamentals = FundamentalsFetcher()
    history_provider = None
    cache = TranscriptCache()
    cache.warm_from_output_dir(output_dir)
    today = date.today()

    channel_metadata: dict[str, dict[str, Any]] = {}
    failures: dict[str, str] = {}
    total_videos_seen = 0
    total_videos_analyzed = 0
    total_new_records = 0

    for channel in (item for item in config.channels if item.enabled):
        try:
            videos = resolver.resolve_channel_videos_since(
                channel.url,
                days=lookback_days,
                max_entries=max(config.strategy.max_scan, 80),
                reference_date=today,
            )
            total_videos_seen += len(videos)
            rows = _analyze_videos(
                videos,
                cache=cache,
                fetcher=fetcher,
                fundamentals=fundamentals,
                config=config,
            )
            total_videos_analyzed += len(rows)
            ranking = build_cross_video_ranking(rows)
            validation = ranking_validation(ranking, today.isoformat())
            scorecard = quality_scorecard(rows, validation, ranking)
            channel_run = summarize_channel_run(rows)
            channel_metadata[channel.slug] = {
                "display_name": channel.display_name,
                "url": channel.url,
                "total_videos": len(videos),
                "analyzed_videos": len(rows),
                "actionable_ratio": round(channel_run.get("analyzable_videos", 0) / len(rows), 4) if rows else 0.0,
                "ranking_spearman": ranking_spearman(ranking, validation),
                "quality_scorecard": scorecard,
                "signal_breakdown": channel_run.get("signal_breakdown", {}),
            }
            total_new_records += record_signals_from_rows(
                tracker_db,
                channel_slug=channel.slug,
                rows=rows,
                history_provider=history_provider,
            )
        except Exception as exc:
            logger.warning("Historical backfill failed for %s: %s", channel.slug, exc)
            failures[channel.slug] = str(exc)

    refreshed_records = update_price_snapshots(tracker_db, history_provider=history_provider)
    _enrich_channel_quality(channel_metadata, tracker_db)
    return channel_metadata, {
        "lookback_days": lookback_days,
        "videos_seen": total_videos_seen,
        "videos_analyzed": total_videos_analyzed,
        "new_records": total_new_records,
        "refreshed_records": refreshed_records,
        "failed_channels": failures,
    }


def _analyze_videos(
    videos: list[VideoInput],
    *,
    cache: TranscriptCache,
    fetcher: TranscriptFetcher,
    fundamentals: FundamentalsFetcher,
    config: AppConfig,
) -> list[dict[str, Any]]:
    if not videos:
        return []
    workers = max(1, min(len(videos), int(config.strategy.video_workers)))
    results: list[dict[str, Any] | None] = [None] * len(videos)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        future_to_index = {
            pool.submit(
                analyze_video_heuristic,
                video,
                cache,
                fetcher,
                fundamentals,
                max_fundamental_workers=config.strategy.fundamentals_workers,
            ): idx
            for idx, video in enumerate(videos)
        }
        for future in as_completed(future_to_index):
            idx = future_to_index[future]
            try:
                results[idx] = future.result()
            except Exception as exc:
                logger.warning("Skipping historical video %s due to analyze failure: %s", videos[idx].video_id, exc)
    return [row for row in results if row is not None]


def _enrich_channel_quality(
    channel_metadata: dict[str, dict[str, Any]],
    tracker_db: SignalTrackerDB,
) -> None:
    if not channel_metadata:
        return
    accuracy_by_channel = {
        slug: tracker_db.accuracy_report(slug).to_dict()
        for slug in channel_metadata
    }
    ranked_reports = rank_channels(compute_channel_quality(channel_metadata, accuracy_by_channel))
    weight_multipliers = compute_dynamic_weights(ranked_reports)
    by_slug = {report.slug: report for report in ranked_reports}
    for slug, info in channel_metadata.items():
        report = by_slug.get(slug)
        if report is None:
            continue
        info["overall_quality_score"] = report.overall_quality_score
        info["weight_multiplier"] = weight_multipliers.get(slug)
