from __future__ import annotations

from collections import OrderedDict
import logging
import re
import time
from datetime import datetime, timezone
from datetime import date, timedelta
from dataclasses import asdict
from pathlib import Path
from threading import Lock
from typing import Any, Callable, Iterable

from youtube_transcript_api import IpBlocked, RequestBlocked, YouTubeTranscriptApi
from yt_dlp import DownloadError, YoutubeDL

from .models import TranscriptSegment, VideoInput, utc_now_iso
from .utils import ensure_dir, normalize_ws, read_json, write_json

VIDEO_ID_RE = re.compile(r"(?:v=|youtu\.be/|/shorts/)([A-Za-z0-9_-]{11})")
CHANNEL_ID_RE = re.compile(r"/channel/([A-Za-z0-9_-]+)")
CHANNEL_HANDLE_RE = re.compile(r"/@([^/?#]+)")
CHANNEL_SUFFIX_RE = re.compile(r"/(?:videos|featured|streams|shorts|live)/?$")
SAFE_CACHE_KEY_RE = re.compile(r"[^A-Za-z0-9_-]")
DEFAULT_VIDEO_CACHE_HOURS = 24
DEFAULT_YOUTUBE_FETCH_MAX_ATTEMPTS = 3
DEFAULT_YOUTUBE_FETCH_RETRY_BASE_SECONDS = 2.0
MAX_YOUTUBE_FETCH_RETRY_SECONDS = 12.0
RETRYABLE_YTDLP_MARKERS = (
    "sign in to confirm you're not a bot",
    "sign in to confirm you’re not a bot",
    "too many requests",
    "http error 429",
    "http error 500",
    "http error 502",
    "http error 503",
    "http error 504",
    "broken pipe",
    "connection reset by peer",
    "connection aborted",
    "remote end closed connection without response",
    "temporary failure in name resolution",
    "timed out",
    "temporarily unavailable",
)
RETRYABLE_TRANSCRIPT_MARKERS = (
    "youtube is blocking requests from your ip",
    "too many requests",
    "request blocked",
    "ip blocked",
    "broken pipe",
    "connection reset by peer",
    "connection aborted",
    "remote end closed connection without response",
    "temporarily unavailable",
    "timed out",
)

logger = logging.getLogger(__name__)


def describe_youtube_error(exc: Exception) -> str:
    """Collapse multiline extractor/transcript errors into a log-friendly summary."""
    message = normalize_ws(str(exc))
    return message or exc.__class__.__name__


def _retry_delay_seconds(attempt: int, base_delay_seconds: float = DEFAULT_YOUTUBE_FETCH_RETRY_BASE_SECONDS) -> float:
    attempt = max(1, int(attempt))
    return min(MAX_YOUTUBE_FETCH_RETRY_SECONDS, float(base_delay_seconds) * (2 ** (attempt - 1)))


def _sleep_before_retry(delay_seconds: float) -> None:
    time.sleep(delay_seconds)


def _exception_messages(exc: Exception) -> list[str]:
    messages: list[str] = []
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        message = describe_youtube_error(current if isinstance(current, Exception) else Exception(str(current)))
        if message:
            messages.append(message.lower())
        current = current.__cause__ or current.__context__
    return messages


def _has_retryable_marker(exc: Exception, markers: tuple[str, ...]) -> bool:
    return any(
        marker in message
        for message in _exception_messages(exc)
        for marker in markers
    )


def _is_retryable_ytdlp_error(exc: Exception) -> bool:
    if isinstance(exc, (BrokenPipeError, TimeoutError, ConnectionResetError, ConnectionAbortedError, EOFError)):
        return True
    if not isinstance(exc, (DownloadError, OSError)):
        return False
    return _has_retryable_marker(exc, RETRYABLE_YTDLP_MARKERS)


def _is_retryable_transcript_error(exc: Exception) -> bool:
    if isinstance(exc, (RequestBlocked, IpBlocked, BrokenPipeError, TimeoutError, ConnectionResetError, ConnectionAbortedError, EOFError)):
        return True
    return _has_retryable_marker(exc, RETRYABLE_TRANSCRIPT_MARKERS)


def _call_with_retry(
    operation: Callable[[], Any],
    *,
    context: str,
    is_retryable: Callable[[Exception], bool],
    max_attempts: int = DEFAULT_YOUTUBE_FETCH_MAX_ATTEMPTS,
) -> Any:
    max_attempts = max(1, int(max_attempts))
    last_exc: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            return operation()
        except Exception as exc:
            last_exc = exc
            if attempt >= max_attempts or not is_retryable(exc):
                raise
            delay_seconds = _retry_delay_seconds(attempt)
            logger.warning(
                "%s failed with retryable YouTube error on attempt %s/%s: %s; retrying in %.1fs",
                context,
                attempt,
                max_attempts,
                describe_youtube_error(exc),
                delay_seconds,
            )
            _sleep_before_retry(delay_seconds)
    if last_exc is not None:
        raise last_exc
    raise RuntimeError(f"{context} failed without raising an exception")


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
        channel_id = normalized.get("channel_id")
        existing = [
            row for row in rows
            if row.get("url") != url and (not channel_id or row.get("channel_id") != channel_id)
        ]
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
    def __init__(
        self,
        cache_root: Path | None = None,
        cache_max_age_hours: int = DEFAULT_VIDEO_CACHE_HOURS,
        max_memory_entries: int = 256,
        memory_cache_max_entries: int | None = None,
        memory_cache_size: int | None = None,
    ):
        self._ydl_opts = {
            "quiet": True,
            "no_warnings": True,
            "extract_flat": True,
            "skip_download": True,
        }
        self.cache_root = cache_root or Path(".omx/cache/video_metadata")
        self.cache_max_age_hours = cache_max_age_hours
        if memory_cache_max_entries is not None:
            max_memory_entries = memory_cache_max_entries
        if memory_cache_size is not None:
            max_memory_entries = memory_cache_size
        self.max_memory_entries = max(0, int(max_memory_entries))
        self._memory_cache: OrderedDict[str, dict] = OrderedDict()
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
            info = self._extract_info(url, opts=opts, context=f"yt-dlp video metadata fetch for {video_id}")
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
        info = self._extract_info(
            channel_url,
            opts={**self._ydl_opts, "playlistend": limit},
            context=f"yt-dlp channel fetch for {channel_url}",
        )
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

    def discover_channel(self, channel_url: str) -> dict[str, str | None]:
        info = self._extract_info(
            channel_url,
            opts={**self._ydl_opts, "playlistend": 1},
            context=f"yt-dlp channel discovery for {channel_url}",
        )
        first_entry = next((entry for entry in info.get("entries") or [] if isinstance(entry, dict)), {})
        channel_id = (
            info.get("channel_id")
            or info.get("id")
            or first_entry.get("channel_id")
        )
        uploader_id = info.get("uploader_id") or first_entry.get("uploader_id")
        channel_title = _clean_channel_title(
            info.get("channel")
            or info.get("uploader")
            or info.get("title")
            or first_entry.get("channel")
            or first_entry.get("uploader")
            or ""
        )
        canonical_url = canonical_channel_url(
            channel_url,
            channel_id=str(channel_id or "").strip() or None,
            uploader_id=str(uploader_id or "").strip() or None,
        )
        return {
            "url": canonical_url,
            "source_url": channel_url,
            "channel_id": str(channel_id or "").strip() or None,
            "channel_title": channel_title or None,
            "uploader_id": str(uploader_id or "").strip() or None,
        }

    def _fetch_channel_entries(self, channel_url: str, max_entries: int = 80) -> list[dict]:
        info = self._extract_info(
            channel_url,
            opts={**self._ydl_opts, "playlistend": max_entries},
            context=f"yt-dlp channel entries fetch for {channel_url}",
        )
        return info.get("entries") or []

    def _extract_info(self, target: str, *, opts: dict[str, Any], context: str) -> dict[str, Any]:
        return _call_with_retry(
            lambda: self._extract_info_once(target, opts),
            context=context,
            is_retryable=_is_retryable_ytdlp_error,
        )

    @staticmethod
    def _extract_info_once(target: str, opts: dict[str, Any]) -> dict[str, Any]:
        with YoutubeDL(opts) as ydl:
            return ydl.extract_info(target, download=False)

    def _cache_path(self, video_id: str) -> Path:
        safe_video_id = SAFE_CACHE_KEY_RE.sub("_", video_id)
        return self.cache_root / f"{safe_video_id}.json"

    def _load_cached_video(self, video_id: str) -> dict | None:
        cached = self._memory_cache_get(video_id)
        if cached is not None:
            return cached
        payload = read_json(self._cache_path(video_id), None)
        if payload is None:
            return None
        self._memory_cache_put(video_id, payload)
        return payload

    def _save_video_cache(self, video: VideoInput) -> None:
        payload = {
            "cached_at": utc_now_iso(),
            "video": asdict(video),
        }
        self._memory_cache_put(video.video_id, payload)
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

    def _memory_cache_get(self, key: str) -> dict | None:
        if self.max_memory_entries == 0:
            return None
        with self._cache_lock:
            cached = self._memory_cache.get(key)
            if cached is None:
                return None
            self._memory_cache.move_to_end(key)
            return cached

    def _memory_cache_put(self, key: str, payload: dict) -> None:
        if self.max_memory_entries == 0:
            return
        with self._cache_lock:
            self._memory_cache[key] = payload
            self._memory_cache.move_to_end(key)
            while len(self._memory_cache) > self.max_memory_entries:
                self._memory_cache.popitem(last=False)


class TranscriptFetcher:
    def fetch(self, video_id: str, preferred_languages: Iterable[str] | None = None) -> tuple[list[TranscriptSegment], str | None]:
        preferred_languages = list(preferred_languages or ["ko", "en"])
        api = YouTubeTranscriptApi()
        fetched = _call_with_retry(
            lambda: api.fetch(video_id, languages=preferred_languages),
            context=f"transcript fetch for {video_id}",
            is_retryable=_is_retryable_transcript_error,
        )
        segments = [
            TranscriptSegment(start=item.start, duration=item.duration, text=normalize_ws(item.text))
            for item in fetched
            if normalize_ws(item.text)
        ]
        if not segments:
            raise ValueError(f"Transcript fetch returned no non-empty segments for {video_id}")
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


def canonical_channel_url(
    url: str,
    *,
    channel_id: str | None = None,
    uploader_id: str | None = None,
) -> str:
    clean_url = url.strip().rstrip("/")
    if clean_url.endswith("/live"):
        clean_url = CHANNEL_SUFFIX_RE.sub("", clean_url)
    if channel_id:
        return f"https://www.youtube.com/channel/{channel_id}/videos"
    if uploader_id:
        handle = uploader_id if uploader_id.startswith("@") else f"@{uploader_id}"
        return f"https://www.youtube.com/{handle}/videos"
    if CHANNEL_SUFFIX_RE.search(clean_url):
        return clean_url
    if CHANNEL_ID_RE.search(clean_url) or CHANNEL_HANDLE_RE.search(clean_url) or "/c/" in clean_url or "/user/" in clean_url:
        return f"{CHANNEL_SUFFIX_RE.sub('', clean_url)}/videos"
    return clean_url


def _clean_channel_title(value: str | None) -> str:
    if not value:
        return ""
    clean_value = normalize_ws(value)
    if clean_value.endswith(" - Videos"):
        clean_value = clean_value[:-9]
    return clean_value.strip()
