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
    def __init__(self, videos):
        super().__init__()
        self.videos = videos

    def _fetch_channel_entries(self, channel_url: str, max_entries: int = 80):
        return [{"id": key} for key in self.videos]

    def resolve_video(self, url_or_id: str) -> VideoInput:
        return self.videos[url_or_id]


def test_resolve_channel_videos_since_filters_by_date():
    videos = {
        "new1": VideoInput(video_id="new1", title="새 영상", url="https://youtube.com/watch?v=new1", published_at="20260320"),
        "new2": VideoInput(video_id="new2", title="조금 전 영상", url="https://youtube.com/watch?v=new2", published_at="20260305"),
        "old1": VideoInput(video_id="old1", title="오래된 영상", url="https://youtube.com/watch?v=old1", published_at="20260201"),
    }
    resolver = FakeResolver(videos)

    recent = resolver.resolve_channel_videos_since("https://youtube.com/@test", days=30, reference_date=date(2026, 3, 22))

    assert [video.video_id for video in recent] == ["new1", "new2"]
