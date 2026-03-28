from __future__ import annotations

import logging
from typing import Any

from .transcript_cache import TranscriptCache
from .youtube import TranscriptFetcher, describe_youtube_error


def _normalize_cached_language(language: str | None) -> str:
    normalized = (language or "unknown").strip()
    while normalized.startswith("cache:"):
        normalized = normalized[len("cache:") :]
    return normalized or "unknown"


def resolve_transcript_text(video, cache: TranscriptCache, fetcher: TranscriptFetcher, logger: logging.Logger) -> tuple[str, str, str, dict[str, Any] | None]:
    """Resolve transcript text from cache, live fetch, or metadata fallback."""
    metadata_text = " ".join(part for part in [video.title, video.description or "", " ".join(video.tags)] if part).strip()
    cached = cache.load(video.video_id)
    is_stale = cache.is_entry_stale(cached)
    if cached and cached.get("transcript_text") and not is_stale:
        cached_language = _normalize_cached_language(cached.get("transcript_language"))
        cached_source = str(cached.get("source") or "cache")
        if cached_source == "metadata_fallback" or cached_language == "metadata_fallback":
            return cached["transcript_text"], "cache:metadata_fallback", "metadata_fallback", cached
        return cached["transcript_text"], f"cache:{cached_language}", cached_source, cached
    try:
        segments, language = fetcher.fetch(video.video_id)
        transcript_text = fetcher.join_segments(segments)
        if not transcript_text:
            raise ValueError(f"Transcript fetch returned empty text for {video.video_id}")
        cache.save(video, transcript_text, language, "transcript_api")
        return transcript_text, language or "unknown", "transcript_api", cache.load(video.video_id)
    except Exception as exc:
        logger.warning("Transcript fetch failed for %s: %s", video.video_id, describe_youtube_error(exc))
        if cached and cached.get("transcript_text"):
            logger.info("Using cached transcript fallback for %s", video.video_id)
            cached_language = _normalize_cached_language(cached.get("transcript_language"))
            cached_source = str(cached.get("source") or "cache")
            if cached_source == "metadata_fallback" or cached_language == "metadata_fallback":
                return cached["transcript_text"], "cache:metadata_fallback", "metadata_fallback", cached
            return cached["transcript_text"], f"cache:{cached_language}", cached_source, cached
        if not metadata_text:
            logger.warning("Transcript fetch failed for %s and metadata fallback is empty", video.video_id)
        cache.save(video, metadata_text, "metadata_fallback", "metadata_fallback")
        logger.info("Using metadata fallback for %s", video.video_id)
        return metadata_text, "metadata_fallback", "metadata_fallback", cache.load(video.video_id)
