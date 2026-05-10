from __future__ import annotations

from collections import OrderedDict
import json
import logging
import os
import random
import re
import time
from datetime import datetime, timezone
from datetime import date, timedelta
from dataclasses import asdict
from pathlib import Path
from threading import Lock
from typing import Any, Callable, Iterable

import requests
from youtube_transcript_api import (
    IpBlocked,
    NoTranscriptFound,
    RequestBlocked,
    TranscriptsDisabled,
    VideoUnavailable,
    YouTubeTranscriptApi,
)
try:  # top-level re-exports preferred; fall back to the private module on older releases
    from youtube_transcript_api import AgeRestricted, VideoUnplayable  # type: ignore[attr-defined]
except ImportError:  # pragma: no cover - depends on youtube_transcript_api version
    from youtube_transcript_api._errors import AgeRestricted, VideoUnplayable
from youtube_transcript_api.proxies import GenericProxyConfig
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
IP_BLOCK_STATE_PATH = Path(".omx/state/ip_block_state.json")
IP_BLOCK_CONSECUTIVE_LIMIT = 3
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


def _resolve_proxy_urls(
    *,
    http_proxy_url: str | None = None,
    https_proxy_url: str | None = None,
) -> tuple[str | None, str | None]:
    http_proxy = http_proxy_url or os.getenv("OMX_HTTP_PROXY_URL") or os.getenv("OMX_HTTP_PROXY") or os.getenv("OMX_YOUTUBE_PROXY_URL") or os.getenv("OMX_RESIDENTIAL_PROXY_URL")
    https_proxy = https_proxy_url or os.getenv("OMX_HTTPS_PROXY_URL") or os.getenv("OMX_HTTPS_PROXY") or os.getenv("OMX_YOUTUBE_PROXY_URL") or os.getenv("OMX_RESIDENTIAL_PROXY_URL")
    if http_proxy and not https_proxy:
        https_proxy = http_proxy
    if https_proxy and not http_proxy:
        http_proxy = https_proxy
    return http_proxy or None, https_proxy or None


def _proxy_dict(http_proxy_url: str | None, https_proxy_url: str | None) -> dict[str, str]:
    proxies: dict[str, str] = {}
    if http_proxy_url:
        proxies["http"] = http_proxy_url
    if https_proxy_url:
        proxies["https"] = https_proxy_url
    return proxies


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


_PERMANENT_TRANSCRIPT_EXC_TYPES = (
    TranscriptsDisabled,
    NoTranscriptFound,
    VideoUnavailable,
    VideoUnplayable,
    AgeRestricted,
)
# Concrete subclasses are listed explicitly rather than the `CouldNotRetrieveTranscript`
# base because sibling subclasses (e.g. YouTubeRequestFailed, CookiePathInvalid) are
# transient and must not be cached as permanent failures.
_PERMANENT_TRANSCRIPT_MARKERS = (
    "transcripts are disabled",
    "subtitles are disabled",
    "no transcript",
    "no transcripts",
    "video unavailable",
    "private video",
    "members-only",
    "age restricted",
    "this live event",
    "premiere",
)


def is_permanent_transcript_error(exc: Exception) -> bool:
    """True when the transcript is unlikely to ever be available (not an IP/rate issue)."""
    if isinstance(exc, (RequestBlocked, IpBlocked)):
        return False
    if isinstance(exc, _PERMANENT_TRANSCRIPT_EXC_TYPES):
        return True
    return _has_retryable_marker(exc, _PERMANENT_TRANSCRIPT_MARKERS)


def _is_retryable_transcript_error(exc: Exception) -> bool:
    if isinstance(exc, (RequestBlocked, IpBlocked, BrokenPipeError, TimeoutError, ConnectionResetError, ConnectionAbortedError, EOFError)):
        return True
    return _has_retryable_marker(exc, RETRYABLE_TRANSCRIPT_MARKERS)


def record_ip_block() -> int:
    """Record an IP block event and return the new consecutive count."""
    ensure_dir(IP_BLOCK_STATE_PATH.parent)
    state = read_json(IP_BLOCK_STATE_PATH, {"consecutive_blocks": 0})
    state["consecutive_blocks"] = state.get("consecutive_blocks", 0) + 1
    state["last_blocked_at"] = datetime.now(timezone.utc).isoformat()
    write_json(IP_BLOCK_STATE_PATH, state)
    logger.warning(
        "IP block recorded (consecutive: %s/%s)",
        state["consecutive_blocks"],
        IP_BLOCK_CONSECUTIVE_LIMIT,
    )
    return state["consecutive_blocks"]


def clear_ip_block_counter() -> None:
    """Reset the consecutive IP block counter after a successful fetch."""
    if IP_BLOCK_STATE_PATH.exists():
        state = read_json(IP_BLOCK_STATE_PATH, {})
        if state.get("consecutive_blocks", 0) > 0:
            state["consecutive_blocks"] = 0
            state["cleared_at"] = datetime.now(timezone.utc).isoformat()
            write_json(IP_BLOCK_STATE_PATH, state)


def ip_block_limit_reached() -> bool:
    """Check if consecutive IP blocks have hit the shutdown threshold."""
    if not IP_BLOCK_STATE_PATH.exists():
        return False
    state = read_json(IP_BLOCK_STATE_PATH, {})
    return state.get("consecutive_blocks", 0) >= IP_BLOCK_CONSECUTIVE_LIMIT


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
        http_proxy_url: str | None = None,
        https_proxy_url: str | None = None,
    ):
        self.http_proxy_url, self.https_proxy_url = _resolve_proxy_urls(
            http_proxy_url=http_proxy_url,
            https_proxy_url=https_proxy_url,
        )
        self._ydl_opts = {
            "quiet": True,
            "no_warnings": True,
            "extract_flat": True,
            "skip_download": True,
        }
        if self.https_proxy_url or self.http_proxy_url:
            self._ydl_opts["proxy"] = self.https_proxy_url or self.http_proxy_url
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
    def __init__(
        self,
        *,
        http_proxy_url: str | None = None,
        https_proxy_url: str | None = None,
        min_interval_seconds: float | None = None,
        jitter_seconds: float | None = None,
    ) -> None:
        self.http_proxy_url, self.https_proxy_url = _resolve_proxy_urls(
            http_proxy_url=http_proxy_url,
            https_proxy_url=https_proxy_url,
        )
        self._http_client = requests.Session()
        proxy_map = _proxy_dict(self.http_proxy_url, self.https_proxy_url)
        if proxy_map:
            self._http_client.proxies.update(proxy_map)
            self._proxy_config = GenericProxyConfig(
                http_url=self.http_proxy_url,
                https_url=self.https_proxy_url,
            )
        else:
            self._proxy_config = None
        if min_interval_seconds is None:
            min_interval_seconds = float(os.getenv("OMX_TRANSCRIPT_MIN_INTERVAL_SEC") or "4.0")
        if jitter_seconds is None:
            jitter_seconds = float(os.getenv("OMX_TRANSCRIPT_JITTER_SEC") or "2.0")
        self._min_interval_seconds = max(0.0, float(min_interval_seconds))
        self._jitter_seconds = max(0.0, float(jitter_seconds))
        self._throttle_lock = Lock()
        self._last_fetch_monotonic: float = 0.0

    def _throttle(self) -> None:
        """Pace outbound transcript requests to reduce IP-block risk.

        Shared across threads via a single lock so that the ThreadPoolExecutor
        in the pipeline cannot produce a burst of concurrent YouTube hits.
        The lock is intentionally held across ``time.sleep`` to serialize the
        worker pool — do not "optimize" it away.
        """
        if self._min_interval_seconds <= 0 and self._jitter_seconds <= 0:
            return
        with self._throttle_lock:
            now = time.monotonic()
            wait = 0.0
            if self._last_fetch_monotonic > 0:
                elapsed = now - self._last_fetch_monotonic
                if elapsed < self._min_interval_seconds:
                    wait = self._min_interval_seconds - elapsed
            if self._jitter_seconds > 0:
                wait += random.uniform(0.0, self._jitter_seconds)
            if wait > 0:
                time.sleep(wait)
            self._last_fetch_monotonic = time.monotonic()

    def fetch(self, video_id: str, preferred_languages: Iterable[str] | None = None) -> tuple[list[TranscriptSegment], str | None]:
        segments, language, _source = self.fetch_with_source(video_id, preferred_languages=preferred_languages)
        return segments, language

    def fetch_with_source(
        self,
        video_id: str,
        preferred_languages: Iterable[str] | None = None,
    ) -> tuple[list[TranscriptSegment], str | None, str]:
        preferred_languages = list(preferred_languages or ["ko", "en"])
        api = YouTubeTranscriptApi(proxy_config=self._proxy_config, http_client=self._http_client)

        def _fetch_once() -> Any:
            self._throttle()
            return api.fetch(video_id, languages=preferred_languages)

        try:
            fetched = _call_with_retry(
                _fetch_once,
                context=f"transcript fetch for {video_id}",
                is_retryable=_is_retryable_transcript_error,
            )
        except Exception as exc:
            is_ip_block = isinstance(exc, (RequestBlocked, IpBlocked)) or _has_retryable_marker(exc, ("ip blocked", "request blocked", "youtube is blocking requests from your ip"))
            if is_ip_block:
                record_ip_block()
            logger.warning("Transcript API fetch failed for %s: %s; trying yt-dlp subtitles", video_id, describe_youtube_error(exc))
            try:
                result = self._fetch_from_ytdlp(video_id, preferred_languages)
                clear_ip_block_counter()
                return result
            except Exception:
                if is_ip_block:
                    record_ip_block()
                raise

        clear_ip_block_counter()
        segments = [
            TranscriptSegment(start=item.start, duration=item.duration, text=normalize_ws(item.text))
            for item in fetched
            if normalize_ws(item.text)
        ]
        if not segments:
            raise ValueError(f"Transcript fetch returned no non-empty segments for {video_id}")
        language = getattr(fetched, "language_code", None)
        return segments, language, "transcript_api"

    @staticmethod
    def join_segments(segments: list[TranscriptSegment]) -> str:
        return " ".join(segment.text for segment in segments)

    def _fetch_from_ytdlp(
        self,
        video_id: str,
        preferred_languages: list[str],
    ) -> tuple[list[TranscriptSegment], str | None, str]:
        url = f"https://www.youtube.com/watch?v={video_id}"

        def _extract_once() -> dict[str, Any]:
            self._throttle()
            return self._extract_subtitle_info(url, preferred_languages)

        info = _call_with_retry(
            _extract_once,
            context=f"yt-dlp subtitle fetch for {video_id}",
            is_retryable=_is_retryable_ytdlp_error,
        )
        subtitle = _select_subtitle_track(info, preferred_languages)
        if subtitle is None:
            raise ValueError(f"yt-dlp subtitle fallback unavailable for {video_id}")
        payload = self._http_client.get(subtitle["url"], timeout=20)
        payload.raise_for_status()
        if subtitle["ext"] == "json3":
            segments = _parse_json3_segments(payload.text)
        else:
            segments = _parse_vtt_segments(payload.text)
        if not segments:
            raise ValueError(f"yt-dlp subtitle fallback returned no segments for {video_id}")
        return segments, subtitle["language"], subtitle["source"]

    def _extract_subtitle_info(self, target_url: str, preferred_languages: list[str]) -> dict[str, Any]:
        opts = {
            "quiet": True,
            "no_warnings": True,
            "skip_download": True,
            "extract_flat": False,
            "subtitleslangs": preferred_languages,
            "writesubtitles": True,
            "writeautomaticsub": True,
        }
        if self.https_proxy_url or self.http_proxy_url:
            opts["proxy"] = self.https_proxy_url or self.http_proxy_url
        with YoutubeDL(opts) as ydl:
            return ydl.extract_info(target_url, download=False)


def _select_subtitle_track(info: dict[str, Any], preferred_languages: list[str]) -> dict[str, str] | None:
    sources = (
        ("subtitles", info.get("subtitles") or {}, "yt_dlp_subtitles"),
        ("automatic_captions", info.get("automatic_captions") or {}, "yt_dlp_auto_captions"),
    )
    for _label, tracks, source_name in sources:
        if not isinstance(tracks, dict):
            continue
        for language in _subtitle_language_candidates(preferred_languages, tracks):
            formats = tracks.get(language) or []
            selected = _select_subtitle_format(formats)
            if selected is None:
                continue
            return {
                "url": str(selected["url"]),
                "ext": str(selected.get("ext") or "vtt").lower(),
                "language": language,
                "source": source_name,
            }
    return None


def _subtitle_language_candidates(preferred_languages: list[str], tracks: dict[str, Any]) -> list[str]:
    keys = list(tracks)
    ranked: list[str] = []
    for preferred in preferred_languages:
        pref = preferred.lower()
        ranked.extend(
            key for key in keys
            if key.lower() == pref or key.lower().startswith(f"{pref}-") or key.lower().endswith(f".{pref}")
        )
    ranked.extend(keys)
    deduped: list[str] = []
    seen: set[str] = set()
    for key in ranked:
        if key not in seen:
            seen.add(key)
            deduped.append(key)
    return deduped


def _select_subtitle_format(formats: list[dict[str, Any]]) -> dict[str, Any] | None:
    priorities = {"json3": 0, "srv3": 1, "vtt": 2, "ttml": 3}
    candidates = [
        item for item in formats
        if isinstance(item, dict) and item.get("url")
    ]
    if not candidates:
        return None
    candidates.sort(key=lambda item: priorities.get(str(item.get("ext") or "").lower(), 10))
    return candidates[0]


def _parse_json3_segments(payload: str) -> list[TranscriptSegment]:
    data = json.loads(payload)
    events = data.get("events", []) if isinstance(data, dict) else []
    segments: list[TranscriptSegment] = []
    for event in events:
        if not isinstance(event, dict):
            continue
        text = normalize_ws("".join(seg.get("utf8", "") for seg in event.get("segs", []) if isinstance(seg, dict)))
        if not text:
            continue
        start = float(event.get("tStartMs", 0) or 0) / 1000.0
        duration = float(event.get("dDurationMs", 0) or 0) / 1000.0
        segments.append(TranscriptSegment(start=start, duration=duration, text=text))
    return segments


_TIMECODE_RE = re.compile(r"^\d{2}:\d{2}:\d{2}\.\d{3}\s+-->\s+\d{2}:\d{2}:\d{2}\.\d{3}")


def _parse_vtt_segments(payload: str) -> list[TranscriptSegment]:
    segments: list[TranscriptSegment] = []
    block: list[str] = []
    start = 0.0
    duration = 0.0
    for raw_line in payload.splitlines():
        line = raw_line.strip("\ufeff").strip()
        if not line:
            if block:
                text = normalize_ws(" ".join(block))
                if text:
                    segments.append(TranscriptSegment(start=start, duration=duration, text=text))
            block = []
            start = 0.0
            duration = 0.0
            continue
        if line == "WEBVTT" or line.isdigit():
            continue
        if _TIMECODE_RE.match(line):
            start_text, end_text = [item.strip() for item in line.split("-->", 1)]
            start = _parse_vtt_timecode(start_text)
            end = _parse_vtt_timecode(end_text)
            duration = max(0.0, end - start)
            continue
        if line.startswith("NOTE"):
            continue
        block.append(line)
    if block:
        text = normalize_ws(" ".join(block))
        if text:
            segments.append(TranscriptSegment(start=start, duration=duration, text=text))
    return segments


def _parse_vtt_timecode(value: str) -> float:
    hours, minutes, seconds = value.split(":")
    sec, millis = seconds.split(".")
    return int(hours) * 3600 + int(minutes) * 60 + int(sec) + int(millis) / 1000.0


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
