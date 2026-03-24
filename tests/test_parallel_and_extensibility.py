"""Tests for parallel video analysis and channel extensibility."""
import json
from pathlib import Path

from omx_brainstorm.app_config import load_app_config, AppConfig, ChannelConfig
from omx_brainstorm.models import VideoInput
from omx_brainstorm.pipeline import OMXPipeline
from omx_brainstorm.transcript_cache import TranscriptCache
from omx_brainstorm.youtube import ChannelRegistry


# --- Shared test helpers ---

class _MockProvider:
    def run(self, system_prompt, user_prompt):
        import json as _json
        from omx_brainstorm.llm import LLMResponse
        if "extract publicly traded stock tickers" in system_prompt.lower():
            payload = {"mentions": []}
        else:
            payload = {
                "ticker": "NVDA", "company_name": "NVIDIA",
                "basic_state": "test", "basic_signal_summary": "test",
                "basic_signal_verdict": "BUY",
                "master_opinions": [],
                "thesis_summary": "test", "framework_scores": [],
                "total_score": 70, "max_score": 100, "final_verdict": "BUY",
                "invalidation_triggers": [], "citations": [],
            }
        return LLMResponse(provider="mock", text=_json.dumps(payload, ensure_ascii=False))

    def run_json(self, system_prompt, user_prompt):
        import json as _json
        return _json.loads(self.run(system_prompt, user_prompt).text)


class _DummyFundamentals:
    def fetch(self, mention):
        from omx_brainstorm.models import FundamentalSnapshot
        return FundamentalSnapshot(ticker=mention.ticker, data_source="dummy")


class _DummyFetcher:
    def fetch(self, video_id, preferred_languages=None):
        from omx_brainstorm.models import TranscriptSegment
        return [TranscriptSegment(0, 1, "반도체 투자 전망 분석 종목")], "ko"

    def join_segments(self, segments):
        return " ".join(s.text for s in segments)


class _DummyResolver:
    def __init__(self, videos):
        self._videos = videos

    def resolve_video(self, url_or_id):
        return self._videos[0]

    def resolve_channel_videos(self, channel_url, limit=5):
        return self._videos[:limit]


def _make_pipeline(tmp_path, videos):
    pipeline = OMXPipeline(provider_name="mock", output_dir=tmp_path, transcript_cache=TranscriptCache(tmp_path / "cache"))
    pipeline.resolver = _DummyResolver(videos)
    pipeline.fetcher = _DummyFetcher()
    pipeline.fundamentals = _DummyFundamentals()
    return pipeline


# --- Parallel analysis tests ---

def test_parallel_analyze_channel(tmp_path):
    """Multiple videos analyzed in parallel produce correct results."""
    videos = [
        VideoInput(video_id=f"v{i}", title=f"금리 인하 전망 {i}", url=f"https://youtube.com/watch?v=v{i}")
        for i in range(4)
    ]
    pipeline = _make_pipeline(tmp_path, videos)
    results = pipeline.analyze_channel("https://youtube.com/@test", limit=4, max_workers=2)
    assert len(results) == 4
    # All reports created with correct video IDs
    video_ids = {report.video.video_id for report, _ in results}
    assert video_ids == {"v0", "v1", "v2", "v3"}


def test_sequential_fallback_single_video(tmp_path):
    """Single video doesn't use thread pool."""
    videos = [VideoInput(video_id="s1", title="금리 인하 전망", url="https://youtube.com/watch?v=s1")]
    pipeline = _make_pipeline(tmp_path, videos)
    results = pipeline.analyze_channel("https://youtube.com/@test", limit=1, max_workers=1)
    assert len(results) == 1


def test_parallel_resilient_to_failure(tmp_path, monkeypatch):
    """Parallel analysis skips failed videos and continues."""
    videos = [
        VideoInput(video_id="ok1", title="금리 인하 전망", url="https://youtube.com/watch?v=ok1"),
        VideoInput(video_id="fail1", title="fail video", url="https://youtube.com/watch?v=fail1"),
    ]
    pipeline = _make_pipeline(tmp_path, videos)

    original = pipeline._analyze_resolved_video

    def maybe_fail(video):
        if video.video_id == "fail1":
            raise RuntimeError("boom")
        return original(video)

    pipeline._analyze_resolved_video = maybe_fail
    results = pipeline.analyze_channel("https://youtube.com/@test", limit=2, max_workers=2)
    # One succeeded, one failed - should still return the successful one
    assert len(results) == 1
    assert results[0][0].video.video_id == "ok1"


def test_parallel_dashboard_generated(tmp_path):
    """Dashboard is still generated after parallel analysis."""
    videos = [
        VideoInput(video_id=f"d{i}", title=f"금리 인하 전망 {i}", url=f"https://youtube.com/watch?v=d{i}")
        for i in range(3)
    ]
    pipeline = _make_pipeline(tmp_path, videos)
    pipeline.analyze_channel("https://youtube.com/@test", limit=3, max_workers=2)
    dashboards = list(tmp_path.glob("channel_dashboard_*.md"))
    assert len(dashboards) == 1


# --- Channel extensibility tests ---

def test_register_new_channel_and_load(tmp_path):
    """New channel can be registered and loaded from registry."""
    registry = ChannelRegistry(tmp_path / "channels.json")
    registry.register("https://youtube.com/@newchannel", {
        "channel_id": "UC_NEW",
        "channel_title": "New Finance Channel",
    })
    channels = registry.load()
    assert len(channels) == 1
    assert channels[0]["channel_id"] == "UC_NEW"


def test_config_with_new_channel(tmp_path):
    """New channel in config.toml is properly loaded."""
    config_path = tmp_path / "config.toml"
    config_path.write_text("""
[app]
provider = "mock"

[[channels]]
slug = "new_fin"
display_name = "New Finance Channel"
url = "https://youtube.com/@newfinance/videos"
enabled = true

[[channels]]
slug = "old_ch"
display_name = "Old Channel"
url = "https://youtube.com/@oldchannel/videos"
enabled = false
""", encoding="utf-8")

    config = load_app_config(str(config_path))
    assert len(config.channels) == 2
    enabled = [ch for ch in config.channels if ch.enabled]
    assert len(enabled) == 1
    assert enabled[0].slug == "new_fin"


def test_config_defaults_when_no_channels(tmp_path):
    """Missing channels section falls back to DEFAULT_CHANNELS."""
    config_path = tmp_path / "config.toml"
    config_path.write_text("""
[app]
provider = "mock"
""", encoding="utf-8")

    config = load_app_config(str(config_path))
    assert len(config.channels) == 6  # DEFAULT_CHANNELS
    slugs = {ch.slug for ch in config.channels}
    assert "itgod" in slugs
    assert "sampro" in slugs


def test_config_invalid_channel_missing_field(tmp_path):
    """Config with missing required field raises ValueError."""
    config_path = tmp_path / "config.toml"
    config_path.write_text("""
[[channels]]
slug = "broken"
""", encoding="utf-8")

    import pytest
    with pytest.raises(ValueError, match="missing field"):
        load_app_config(str(config_path))


def test_new_channel_analyze_end_to_end(tmp_path):
    """A freshly configured channel can be analyzed end-to-end."""
    videos = [
        VideoInput(video_id="nc1", title="반도체 수혜주 종목 분석", url="https://youtube.com/watch?v=nc1"),
    ]
    pipeline = _make_pipeline(tmp_path, videos)
    results = pipeline.analyze_channel("https://youtube.com/@newchannel", limit=1)
    assert len(results) == 1
    report, (json_path, md_path, txt_path) = results[0]
    assert json_path.exists()
    assert md_path.exists()
    assert report.video.video_id == "nc1"
