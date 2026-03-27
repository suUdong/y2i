"""Extended tests for youtube.py: resolver, fetcher, parse helpers."""
from datetime import date
from unittest.mock import MagicMock, patch

from omx_brainstorm.models import TranscriptSegment, VideoInput
from omx_brainstorm.youtube import (
    ChannelRegistry,
    YoutubeResolver,
    TranscriptFetcher,
    extract_video_id,
    _parse_upload_date,
)


# --- extract_video_id ---

def test_extract_video_id_bare():
    assert extract_video_id("dQw4w9WgXcQ") == "dQw4w9WgXcQ"


def test_extract_video_id_full_url():
    assert extract_video_id("https://www.youtube.com/watch?v=dQw4w9WgXcQ") == "dQw4w9WgXcQ"


def test_extract_video_id_short_url():
    assert extract_video_id("https://youtu.be/dQw4w9WgXcQ") == "dQw4w9WgXcQ"


def test_extract_video_id_shorts():
    assert extract_video_id("https://youtube.com/shorts/dQw4w9WgXcQ") == "dQw4w9WgXcQ"


def test_extract_video_id_invalid():
    import pytest
    with pytest.raises(ValueError, match="지원하지 않는"):
        extract_video_id("not-a-valid-input-string")


# --- _parse_upload_date ---

def test_parse_upload_date_valid():
    assert _parse_upload_date("20260315") == date(2026, 3, 15)


def test_parse_upload_date_with_extra():
    assert _parse_upload_date("20260315T120000") == date(2026, 3, 15)


def test_parse_upload_date_none():
    assert _parse_upload_date(None) is None


def test_parse_upload_date_empty():
    assert _parse_upload_date("") is None


def test_parse_upload_date_short():
    assert _parse_upload_date("2026") is None


def test_parse_upload_date_non_digit():
    assert _parse_upload_date("abcdefgh") is None


# --- ChannelRegistry ---

def test_channel_registry_register(tmp_path):
    registry = ChannelRegistry(tmp_path / "channels.json")
    row = registry.register("https://youtube.com/@test", {"channel_id": "C1"})
    assert row["url"] == "https://youtube.com/@test"
    assert row["channel_id"] == "C1"
    loaded = registry.load()
    assert len(loaded) == 1


def test_channel_registry_deduplicates(tmp_path):
    registry = ChannelRegistry(tmp_path / "channels.json")
    registry.register("https://youtube.com/@test", {"channel_id": "C1"})
    registry.register("https://youtube.com/@test", {"channel_id": "C1_updated"})
    loaded = registry.load()
    assert len(loaded) == 1
    assert loaded[0]["channel_id"] == "C1_updated"


# --- YoutubeResolver with mocked yt_dlp ---

def test_resolve_video_mocked(monkeypatch):
    fake_info = {
        "title": "Test Video",
        "channel_id": "CH1",
        "channel": "Test Channel",
        "upload_date": "20260315",
        "description": "desc",
        "tags": ["tag1"],
    }

    class FakeYDL:
        def __init__(self, opts):
            pass
        def __enter__(self):
            return self
        def __exit__(self, *a):
            pass
        def extract_info(self, url, download=False):
            return fake_info

    monkeypatch.setattr("omx_brainstorm.youtube.YoutubeDL", FakeYDL)

    resolver = YoutubeResolver()
    video = resolver.resolve_video("dQw4w9WgXcQ")
    assert video.title == "Test Video"
    assert video.channel_id == "CH1"
    assert video.tags == ["tag1"]


def test_resolve_video_uses_cache_on_repeat(monkeypatch, tmp_path):
    calls = {"count": 0}
    fake_info = {
        "title": "Cached Video",
        "channel_id": "CH1",
        "channel": "Test Channel",
        "upload_date": "20260315",
        "description": "desc",
        "tags": ["tag1"],
    }

    class FakeYDL:
        def __init__(self, opts):
            pass
        def __enter__(self):
            return self
        def __exit__(self, *a):
            pass
        def extract_info(self, url, download=False):
            calls["count"] += 1
            return fake_info

    monkeypatch.setattr("omx_brainstorm.youtube.YoutubeDL", FakeYDL)

    resolver = YoutubeResolver(cache_root=tmp_path / "video-cache")
    first = resolver.resolve_video("dQw4w9WgXcQ")
    second = resolver.resolve_video("dQw4w9WgXcQ")

    assert first.title == "Cached Video"
    assert second.title == "Cached Video"
    assert calls["count"] == 1


def test_resolve_channel_videos_mocked(monkeypatch):
    fake_info = {
        "id": "CHANNEL_ID",
        "title": "Channel Title",
        "entries": [
            {"id": "vid1", "title": "Video 1", "channel_id": "CH1", "channel": "Ch", "upload_date": "20260320", "description": "d", "tags": []},
            {"id": "vid2", "title": "Video 2", "channel_id": "CH1", "channel": "Ch", "upload_date": "20260319", "description": "d", "tags": None},
        ],
    }

    class FakeYDL:
        def __init__(self, opts):
            pass
        def __enter__(self):
            return self
        def __exit__(self, *a):
            pass
        def extract_info(self, url, download=False):
            return fake_info

    monkeypatch.setattr("omx_brainstorm.youtube.YoutubeDL", FakeYDL)

    resolver = YoutubeResolver()
    videos = resolver.resolve_channel_videos("https://youtube.com/@test", limit=2)
    assert len(videos) == 2
    assert videos[0].video_id == "vid1"
    assert videos[1].tags == []


def test_resolve_channel_videos_skips_no_id(monkeypatch):
    fake_info = {
        "entries": [
            {"title": "No ID"},
            {"id": "vid1", "title": "Has ID"},
        ],
    }

    class FakeYDL:
        def __init__(self, opts): pass
        def __enter__(self): return self
        def __exit__(self, *a): pass
        def extract_info(self, url, download=False): return fake_info

    monkeypatch.setattr("omx_brainstorm.youtube.YoutubeDL", FakeYDL)

    resolver = YoutubeResolver()
    videos = resolver.resolve_channel_videos("https://youtube.com/@test", limit=5)
    assert len(videos) == 1


def test_resolve_channel_videos_since_mocked(monkeypatch):
    entries = [
        {"id": "vid1", "upload_date": "20260320", "title": "V1"},
        {"id": "vid2", "upload_date": "20260310", "title": "V2"},
        {"id": "vid3", "upload_date": "20260201", "title": "V3"},
    ]

    class FakeYDL:
        def __init__(self, opts): pass
        def __enter__(self): return self
        def __exit__(self, *a): pass
        def extract_info(self, url, download=False): return {"entries": entries}

    monkeypatch.setattr("omx_brainstorm.youtube.YoutubeDL", FakeYDL)

    resolver = YoutubeResolver()

    result = resolver.resolve_channel_videos_since(
        "https://youtube.com/@test",
        days=30,
        reference_date=date(2026, 3, 24),
    )
    # vid1 (Mar 20) and vid2 (Mar 10) are within 30 days, vid3 (Feb 1) triggers break
    assert len(result) == 2
    assert result[0].video_id == "vid1"
    assert result[1].video_id == "vid2"


def test_resolve_channel_videos_since_skips_no_date(monkeypatch):
    entries = [{"id": "vid1", "title": "V1"}]

    class FakeYDL:
        def __init__(self, opts): pass
        def __enter__(self): return self
        def __exit__(self, *a): pass
        def extract_info(self, url, download=False): return {"entries": entries}

    monkeypatch.setattr("omx_brainstorm.youtube.YoutubeDL", FakeYDL)

    resolver = YoutubeResolver()

    result = resolver.resolve_channel_videos_since("https://youtube.com/@test", days=30)
    # No upload_date means published is None, so date filter is skipped — video is included
    assert len(result) == 1


# --- TranscriptFetcher ---

def test_transcript_fetcher_join_segments():
    segments = [
        TranscriptSegment(0, 1, "hello"),
        TranscriptSegment(1, 1, "world"),
    ]
    assert TranscriptFetcher.join_segments(segments) == "hello world"
