from __future__ import annotations

from pathlib import Path
from typing import Any, Sequence

from .app_config import AppConfig
from .comparison_rows import reports_to_comparison_rows
from .pipeline import OMXPipeline
from .transcript_cache import TranscriptCache


def analyze_resolved_videos_to_reports(
    videos: Sequence[Any],
    *,
    config: AppConfig,
    transcript_cache: TranscriptCache | None = None,
    fetcher: Any | None = None,
    fundamentals_fetcher: Any | None = None,
    output_dir: str | Path | None = None,
    persist: bool = False,
) -> list[Any]:
    if not videos:
        return []
    pipeline = OMXPipeline(
        provider_name=config.provider,
        output_dir=Path(output_dir or config.output_dir),
        transcript_cache=transcript_cache,
        http_proxy_url=config.network.http_proxy_url,
        https_proxy_url=config.network.https_proxy_url,
    )
    if fetcher is not None:
        pipeline.fetcher = fetcher
    if fundamentals_fetcher is not None:
        pipeline.fundamentals = fundamentals_fetcher
    results = pipeline.analyze_resolved_videos(
        list(videos),
        max_workers=config.strategy.video_workers,
        persist=persist,
    )
    return [report for report, _paths in results]


def analyze_resolved_videos_to_rows(
    videos: Sequence[Any],
    *,
    config: AppConfig,
    transcript_cache: TranscriptCache | None = None,
    fetcher: Any | None = None,
    fundamentals_fetcher: Any | None = None,
    output_dir: str | Path | None = None,
    persist: bool = False,
) -> list[dict[str, Any]]:
    reports = analyze_resolved_videos_to_reports(
        videos,
        config=config,
        transcript_cache=transcript_cache,
        fetcher=fetcher,
        fundamentals_fetcher=fundamentals_fetcher,
        output_dir=output_dir,
        persist=persist,
    )
    return reports_to_comparison_rows(reports)
