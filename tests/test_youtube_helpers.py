"""Tests for youtube.py helper functions and ChannelRegistry."""
import json

from omx_brainstorm.youtube import ChannelRegistry, canonical_channel_url, extract_video_id, _parse_upload_date


def test_extract_video_id_from_bare_id():
    assert extract_video_id("abc123DEF_-") == "abc123DEF_-"


def test_extract_video_id_from_watch_url():
    assert extract_video_id("https://www.youtube.com/watch?v=abc123DEF_-") == "abc123DEF_-"


def test_extract_video_id_from_short_url():
    assert extract_video_id("https://youtu.be/abc123DEF_-") == "abc123DEF_-"


def test_extract_video_id_from_shorts_url():
    assert extract_video_id("https://www.youtube.com/shorts/abc123DEF_-") == "abc123DEF_-"


def test_extract_video_id_raises_on_invalid():
    import pytest
    with pytest.raises(ValueError, match="지원하지 않는"):
        extract_video_id("not-a-url")


def test_parse_upload_date_valid():
    result = _parse_upload_date("20260321")
    assert result is not None
    assert result.year == 2026
    assert result.month == 3
    assert result.day == 21


def test_parse_upload_date_none():
    assert _parse_upload_date(None) is None
    assert _parse_upload_date("") is None


def test_parse_upload_date_short_string():
    assert _parse_upload_date("2026") is None


def test_parse_upload_date_non_digit():
    assert _parse_upload_date("abcdefgh") is None


def test_parse_upload_date_truncates_long_string():
    result = _parse_upload_date("20260321T120000Z")
    assert result is not None
    assert result.day == 21


def test_channel_registry_save_load_roundtrip(tmp_path):
    registry = ChannelRegistry(tmp_path / "channels.json")
    assert registry.load() == []
    row = registry.register("https://youtube.com/channel/TEST", {"channel_title": "Test"})
    assert row["url"] == "https://youtube.com/channel/TEST"
    loaded = registry.load()
    assert len(loaded) == 1
    assert loaded[0]["channel_title"] == "Test"


def test_channel_registry_replaces_duplicate_url(tmp_path):
    registry = ChannelRegistry(tmp_path / "channels.json")
    registry.register("https://youtube.com/channel/A", {"title": "first"})
    registry.register("https://youtube.com/channel/A", {"title": "updated"})
    loaded = registry.load()
    assert len(loaded) == 1
    assert loaded[0]["title"] == "updated"


def test_channel_registry_keeps_different_urls(tmp_path):
    registry = ChannelRegistry(tmp_path / "channels.json")
    registry.register("https://youtube.com/channel/A", {"title": "A"})
    registry.register("https://youtube.com/channel/B", {"title": "B"})
    loaded = registry.load()
    assert len(loaded) == 2


def test_canonical_channel_url_adds_videos_suffix():
    assert canonical_channel_url("https://youtube.com/@demo") == "https://youtube.com/@demo/videos"


def test_channel_registry_replaces_same_channel_id_across_urls(tmp_path):
    registry = ChannelRegistry(tmp_path / "channels.json")
    registry.register("https://youtube.com/@demo/videos", {"channel_id": "UC123", "channel_title": "Demo"})
    registry.register("https://www.youtube.com/channel/UC123/videos", {"channel_id": "UC123", "channel_title": "Demo Updated"})
    loaded = registry.load()
    assert len(loaded) == 1
    assert loaded[0]["channel_title"] == "Demo Updated"
