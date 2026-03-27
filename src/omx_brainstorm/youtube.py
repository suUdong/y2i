from __future__ import annotations

import re
from datetime import datetime, timezone
from datetime import date, timedelta
from dataclasses import asdict
from pathlib import Path
from threading import Lock
from typing import Iterable

from youtube_transcript_api import YouTubeTranscriptApi
from yt_dlp import YoutubeDL

from .models import TranscriptSegment, VideoInput, utc_now_iso
from .utils import ensure_dir, normalize_ws, read_json, write_json

VIDEO_ID_RE = re.compile(r"(?:v=|youtu\.be/|/shorts/)([A-Za-z0-9_-]{11})")
CHANNEL_ID_RE = re.compile(r"/channel/([A-Za-z0-9_-]+)")
SAFE_CACHE_KEY_RE = re.compile(r"[^A-Za-z0-9_-]")
DEFAULT_VIDEO_CACHE_HOURS = 24


class ChannelRegistry:
    def __init__(self, path: Path):
        self.path = path

    def load(self) -> list[dict]:
        return read_json(self.path, [])

    def save(self, rows: list[dict]) -> None:
        write_json(self.path, rows)

    def register(self, url: str, metadata: dict) -> dict:
        rows = self.load()
        normalized = {"url": url, **metadata}
        existing = [r for r in rows if r.get("url") != url]
        existing.append(normalized)
        self.save(existing)
        return normalized


def extract_video_id(url_or_id: str) -> str:
    if re.fullmatch(r"[A-Za-z0-9_-]{11}", url_or_id):
        return url_or_id
    match = VIDEO_ID_RE.search(url_or_id)
    if not match:
        raise ValueError(f"지원하지 않는 YouTube 영상 입력: {url_or_id}")
    return match.group(1)


class YoutubeResolver:
    def __init__(self, cache_root: Path | None = None, cache_max_age_hours: int = DEFAULT_VIDEO_CACHE_HOURS):
        self._ydl_opts = {
            "quiet": True,
            "no_warnings": True,
            "extract_flat": True,
            "skip_download": True,
        }
        self.cache_root = cache_root or Path(".omx/cache/video_metadata")
        self.cache_max_age_hours = cache_max_age_hours
        self._memory_cache: dict[str, dict] = {}
        self._cache_lock = Lock()
        ensure_dir(self.cache_root)

    def resolve_video(self, url_or_id: str) -> VideoInput:
        video_id = extract_video_id(url_or_id)
        cached = self._load_cached_video(video_id)
        if cached is not None and not self._is_cache_stale(cached):
            return self._video_from_payload(cached["video"])
        url = f"https://www.youtube.com/watch?v={video_id}"
        opts = {**self._ydl_opts, "extract_flat": False}
        try:
            with YoutubeDL(opts) as ydl:
                info = ydl.extract_info(url, download=False)
        except Exception:
            if cached is not None:
                return self._video_from_payload(cached["video"])
            raise
        video = VideoInput(
            video_id=video_id,
            title=info.get("title") or video_id,
            url=url,
            channel_id=info.get("channel_id"),
            channel_title=info.get("channel"),
            published_at=str(info.get("upload_date") or ""),
            description=info.get("description"),
            tags=list(info.get("tags") or []),
        )
        self._save_video_cache(video)
        return video

    def resolve_channel_videos_since(
        self,
        channel_url: str,
        days: int = 30,
        max_entries: int = 80,
        reference_date: date | None = None,
    ) -> list[VideoInput]:
        reference_date = reference_date or date.today()
        cutoff = reference_date - timedelta(days=days)
        entries = self._fetch_channel_entries(channel_url, max_entries=max_entries)
        videos: list[VideoInput] = []
        for entry in entries:
            video_id = entry.get("id")
            if not video_id:
                continue
            upload_date = str(entry.get("upload_date") or "")
            published = _parse_upload_date(upload_date)
            if published is not None and published < cutoff:
                break
            video = VideoInput(
                video_id=video_id,
                title=entry.get("title") or video_id,
                url=f"https://www.youtube.com/watch?v={video_id}",
                channel_id=entry.get("channel_id") or entry.get("uploader_id"),
                channel_title=entry.get("channel") or entry.get("uploader"),
                published_at=upload_date,
                description=entry.get("description"),
                tags=list(entry.get("tags") or []),
            )
            videos.append(video)
        return videos

    def resolve_channel_videos(self, channel_url: str, limit: int = 5) -> list[VideoInput]:
        opts = {**self._ydl_opts, "playlistend": limit}
        with YoutubeDL(opts) as ydl:
            info = ydl.extract_info(channel_url, download=False)
        entries = info.get("entries") or []
        videos: list[VideoInput] = []
        for entry in entries[:limit]:
            video_id = entry.get("id")
            if not video_id:
                continue
            videos.append(
                VideoInput(
                    video_id=video_id,
                    title=entry.get("title") or video_id,
                    url=f"https://www.youtube.com/watch?v={video_id}",
                    channel_id=entry.get("channel_id") or info.get("id"),
                    channel_title=entry.get("channel") or info.get("title"),
                    published_at=str(entry.get("upload_date") or ""),
                    description=entry.get("description"),
                    tags=list(entry.get("tags") or []),
                )
            )
        return videos

    def _fetch_channel_entries(self, channel_url: str, max_entries: int = 80) -> list[dict]:
        opts = {**self._ydl_opts, "playlistend": max_entries}
        with YoutubeDL(opts) as ydl:
            info = ydl.extract_info(channel_url, download=False)
        return info.get("entries") or []

    def _cache_path(self, video_id: str) -> Path:
        safe_video_id = SAFE_CACHE_KEY_RE.sub("_", video_id)
        return self.cache_root / f"{safe_video_id}.json"

    def _load_cached_video(self, video_id: str) -> dict | None:
        with self._cache_lock:
            cached = self._memory_cache.get(video_id)
        if cached is not None:
            return cached
        payload = read_json(self._cache_path(video_id), None)
        if payload is None:
            return None
        with self._cache_lock:
            self._memory_cache[video_id] = payload
        return payload

    def _save_video_cache(self, video: VideoInput) -> None:
        payload = {
            "cached_at": utc_now_iso(),
            "video": asdict(video),
        }
        with self._cache_lock:
            self._memory_cache[video.video_id] = payload
        write_json(self._cache_path(video.video_id), payload)

    def _is_cache_stale(self, payload: dict) -> bool:
        cached_at = payload.get("cached_at")
        if not isinstance(cached_at, str) or not cached_at:
            return True
        try:
            cached_time = datetime.fromisoformat(cached_at)
        except ValueError:
            return True
        age = datetime.now(timezone.utc) - cached_time
        return age.total_seconds() > self.cache_max_age_hours * 3600

    @staticmethod
    def _video_from_payload(payload: dict) -> VideoInput:
        return VideoInput(**payload)


class TranscriptFetcher:
    def fetch(self, video_id: str, preferred_languages: Iterable[str] | None = None) -> tuple[list[TranscriptSegment], str | None]:
        preferred_languages = list(preferred_languages or ["ko", "en"])
        api = YouTubeTranscriptApi()
        fetched = api.fetch(video_id, languages=preferred_languages)
        segments = [
            TranscriptSegment(start=item.start, duration=item.duration, text=normalize_ws(item.text))
            for item in fetched
            if normalize_ws(item.text)
        ]
        language = getattr(fetched, "language_code", None)
        return segments, language

    @staticmethod
    def join_segments(segments: list[TranscriptSegment]) -> str:
        return " ".join(segment.text for segment in segments)


def _parse_upload_date(value: str | None) -> date | None:
    if not value:
        return None
    value = value[:8]
    if len(value) != 8 or not value.isdigit():
        return None
    return date(int(value[:4]), int(value[4:6]), int(value[6:8]))
