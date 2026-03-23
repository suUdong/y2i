from __future__ import annotations

import re
from datetime import date, timedelta
from dataclasses import asdict
from pathlib import Path
from typing import Iterable

from youtube_transcript_api import YouTubeTranscriptApi
from yt_dlp import YoutubeDL

from .models import TranscriptSegment, VideoInput
from .utils import ensure_dir, normalize_ws, read_json, write_json

VIDEO_ID_RE = re.compile(r"(?:v=|youtu\.be/|/shorts/)([A-Za-z0-9_-]{11})")
CHANNEL_ID_RE = re.compile(r"/channel/([A-Za-z0-9_-]+)")


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
    def __init__(self):
        self._ydl_opts = {
            "quiet": True,
            "no_warnings": True,
            "extract_flat": True,
            "skip_download": True,
        }

    def resolve_video(self, url_or_id: str) -> VideoInput:
        video_id = extract_video_id(url_or_id)
        url = f"https://www.youtube.com/watch?v={video_id}"
        opts = {**self._ydl_opts, "extract_flat": False}
        with YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=False)
        return VideoInput(
            video_id=video_id,
            title=info.get("title") or video_id,
            url=url,
            channel_id=info.get("channel_id"),
            channel_title=info.get("channel"),
            published_at=str(info.get("upload_date") or ""),
            description=info.get("description"),
            tags=list(info.get("tags") or []),
        )

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
            video = self.resolve_video(video_id)
            published = _parse_upload_date(video.published_at)
            if published is None:
                continue
            if published < cutoff:
                break
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
