from __future__ import annotations

from datetime import datetime, timezone
import re
from pathlib import Path
from typing import Any
import logging

from .errors import CacheError
from .models import VideoInput, utc_now_iso
from .utils import ensure_dir, read_json, write_json

logger = logging.getLogger(__name__)
SAFE_VIDEO_ID_RE = re.compile(r"[^A-Za-z0-9_-]")


DEFAULT_MAX_AGE_HOURS = 168  # 7 days


class TranscriptCache:
    """Persistent transcript and ticker-evidence cache keyed by video id."""

    def __init__(self, root: Path | None = None, max_age_hours: int = DEFAULT_MAX_AGE_HOURS):
        self.root = root or Path(".omx/cache/transcripts")
        self.max_age_hours = max_age_hours
        ensure_dir(self.root)

    def path_for(self, video_id: str) -> Path:
        safe_video_id = SAFE_VIDEO_ID_RE.sub("_", video_id)
        return self.root / f"{safe_video_id}.json"

    def load(self, video_id: str) -> dict[str, Any] | None:
        path = self.path_for(video_id)
        try:
            return read_json(path, None)
        except Exception as exc:
            logger.warning("Transcript cache load failed for %s: %s", video_id, exc)
            return None

    def is_stale(self, video_id: str, max_age_hours: int | None = None) -> bool:
        """Check if a cached entry is older than max_age_hours."""
        entry = self.load(video_id)
        if entry is None:
            return True
        cached_at = entry.get("cached_at")
        if not cached_at:
            return True
        try:
            cached_time = datetime.fromisoformat(cached_at)
            age = datetime.now(timezone.utc) - cached_time
            limit = max_age_hours if max_age_hours is not None else self.max_age_hours
            return age.total_seconds() > limit * 3600
        except (ValueError, TypeError):
            return True

    def save(
        self,
        video: VideoInput,
        transcript_text: str,
        transcript_language: str | None,
        source: str,
        ticker_mentions: list[dict[str, Any]] | None = None,
    ) -> Path:
        payload = {
            "video": {
                "video_id": video.video_id,
                "title": video.title,
                "url": video.url,
                "published_at": video.published_at,
                "description": video.description,
                "tags": list(video.tags),
            },
            "transcript_text": transcript_text,
            "transcript_language": transcript_language,
            "source": source,
            "ticker_mentions": ticker_mentions or [],
            "cached_at": utc_now_iso(),
        }
        path = self.path_for(video.video_id)
        write_json(path, payload)
        return path

    def warm_from_report_artifact(self, artifact_path: Path) -> bool:
        try:
            payload = read_json(artifact_path, None)
        except Exception as exc:
            logger.warning("Skipping corrupt artifact %s: %s", artifact_path, exc)
            return False
        if not payload or "video" not in payload or not payload.get("transcript_text"):
            return False
        video_payload = payload["video"]
        video = VideoInput(
            video_id=video_payload["video_id"],
            title=video_payload.get("title") or video_payload["video_id"],
            url=video_payload.get("url") or f"https://www.youtube.com/watch?v={video_payload['video_id']}",
            channel_id=video_payload.get("channel_id"),
            channel_title=video_payload.get("channel_title"),
            published_at=video_payload.get("published_at"),
            description=video_payload.get("description"),
            tags=list(video_payload.get("tags") or []),
        )
        self.save(
            video=video,
            transcript_text=payload.get("transcript_text", ""),
            transcript_language=payload.get("transcript_language"),
            source=payload.get("provider", "artifact_cache"),
            ticker_mentions=list(payload.get("ticker_mentions", []) or []),
        )
        return True

    def warm_from_output_dir(self, output_dir: Path) -> int:
        count = 0
        for artifact_path in sorted(output_dir.glob("*.json")):
            if self.warm_from_report_artifact(artifact_path):
                count += 1
        return count
