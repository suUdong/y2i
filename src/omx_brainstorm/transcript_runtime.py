from __future__ import annotations

import logging
from typing import Any

from .transcript_cache import TranscriptCache
from .youtube import TranscriptFetcher


def resolve_transcript_text(video, cache: TranscriptCache, fetcher: TranscriptFetcher, logger: logging.Logger) -> tuple[str, str, str, dict[str, Any] | None]:
    """Resolve transcript text from cache, live fetch, or metadata fallback."""
    metadata_text = " ".join(part for part in [video.title, video.description or "", " ".join(video.tags)] if part).strip()
    cached = cache.load(video.video_id)
    is_stale = cache.is_entry_stale(cached)
    if cached and cached.get("transcript_text") and cached.get("source") != "metadata_fallback" and not is_stale:
        return cached["transcript_text"], f"cache:{cached.get('transcript_language') or 'unknown'}", cached.get("source", "cache"), cached
    try:
        segments, language = fetcher.fetch(video.video_id)
        transcript_text = fetcher.join_segments(segments)
        cache.save(video, transcript_text, language, "transcript_api")
        return transcript_text, language or "unknown", "transcript_api", cache.load(video.video_id)
    except Exception as exc:
        logger.warning("Transcript fetch failed for %s: %s", video.video_id, exc)
        if cached and cached.get("transcript_text"):
            return cached["transcript_text"], f"cache:{cached.get('transcript_language') or 'unknown'}", cached.get("source", "cache"), cached
        cache.save(video, metadata_text, "metadata_fallback", "metadata_fallback")
        return metadata_text, "metadata_fallback", "metadata_fallback", cache.load(video.video_id)
