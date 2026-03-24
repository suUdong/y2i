from datetime import date
from pathlib import Path

from omx_brainstorm.models import VideoInput
from omx_brainstorm.youtube import ChannelRegistry, YoutubeResolver, extract_video_id


def test_extract_video_id_from_url_and_raw_id(tmp_path: Path):
    assert extract_video_id("dQw4w9WgXcQ") == "dQw4w9WgXcQ"
    assert extract_video_id("https://youtu.be/dQw4w9WgXcQ") == "dQw4w9WgXcQ"


def test_channel_registry_deduplicates(tmp_path: Path):
    registry = ChannelRegistry(tmp_path / "channels.json")
    registry.register("https://youtube.com/@test", {"channel_title": "A"})
    registry.register("https://youtube.com/@test", {"channel_title": "B"})
    rows = registry.load()
    assert len(rows) == 1
    assert rows[0]["channel_title"] == "B"


class FakeResolver(YoutubeResolver):
    def __init__(self, entries):
        super().__init__()
        self._entries = entries

    def _fetch_channel_entries(self, channel_url: str, max_entries: int = 80):
        return self._entries


def test_resolve_channel_videos_since_filters_by_date():
    entries = [
        {"id": "new1", "title": "새 영상", "upload_date": "20260320"},
        {"id": "new2", "title": "조금 전 영상", "upload_date": "20260305"},
        {"id": "old1", "title": "오래된 영상", "upload_date": "20260201"},
    ]
    resolver = FakeResolver(entries)

    recent = resolver.resolve_channel_videos_since("https://youtube.com/@test", days=30, reference_date=date(2026, 3, 22))

    assert [video.video_id for video in recent] == ["new1", "new2"]
