from __future__ import annotations

import logging
from typing import Any

from .transcript_cache import TranscriptCache
from .youtube import TranscriptFetcher, describe_youtube_error, is_permanent_transcript_error

METADATA_FALLBACK_PERMANENT_SOURCE = "metadata_fallback_permanent"
METADATA_FALLBACK_SOURCES = frozenset({"metadata_fallback", METADATA_FALLBACK_PERMANENT_SOURCE})


def is_metadata_fallback_source(source: str | None) -> bool:
    """True if the given transcript source string represents any metadata-only fallback."""
    if not source:
        return False
    return source.strip().lower() in METADATA_FALLBACK_SOURCES


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
        if hasattr(fetcher, "fetch_with_source"):
            segments, language, source = fetcher.fetch_with_source(video.video_id)  # type: ignore[attr-defined]
        else:
            segments, language = fetcher.fetch(video.video_id)
            source = "transcript_api"
        transcript_text = fetcher.join_segments(segments)
        if not transcript_text:
            raise ValueError(f"Transcript fetch returned empty text for {video.video_id}")
        cache.save(video, transcript_text, language, source)
        return transcript_text, language or "unknown", source, cache.load(video.video_id)
    except Exception as exc:
        logger.warning("Transcript fetch failed for %s: %s", video.video_id, describe_youtube_error(exc))
        permanent = is_permanent_transcript_error(exc)
        if cached and cached.get("transcript_text"):
            logger.info("Using cached transcript fallback for %s", video.video_id)
            cached_language = _normalize_cached_language(cached.get("transcript_language"))
            cached_source = str(cached.get("source") or "cache")
            if cached_source in ("metadata_fallback", METADATA_FALLBACK_PERMANENT_SOURCE) or cached_language == "metadata_fallback":
                return cached["transcript_text"], "cache:metadata_fallback", cached_source, cached
            return cached["transcript_text"], f"cache:{cached_language}", cached_source, cached
        if not metadata_text:
            logger.warning("Transcript fetch failed for %s and metadata fallback is empty", video.video_id)
        if permanent:
            cache.save(video, metadata_text, "metadata_fallback", METADATA_FALLBACK_PERMANENT_SOURCE)
            logger.info("Using permanent metadata fallback for %s (transcript unavailable)", video.video_id)
            return metadata_text, "metadata_fallback", METADATA_FALLBACK_PERMANENT_SOURCE, cache.load(video.video_id)
        else:
            logger.info("Transient transcript failure for %s — skipping cache so next run retries", video.video_id)
            return metadata_text, "metadata_fallback", "metadata_fallback", None
