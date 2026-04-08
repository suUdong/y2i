from __future__ import annotations

from pathlib import Path

from omx_brainstorm.analysis_rows import analyze_resolved_videos_to_rows
from omx_brainstorm.app_config import AppConfig, StrategyConfig
from omx_brainstorm.models import TranscriptSegment, FundamentalSnapshot, VideoInput
from omx_brainstorm.signal_backtest import _CacheOnlyTranscriptFetcher, _analyze_videos
from omx_brainstorm.transcript_cache import TranscriptCache


class _DummyFetcher:
    def fetch(self, video_id, preferred_languages=None):
        text = (
            "엔비디아가 아직 더 갈 수 있다. 데이터센터 수요가 강하다. "
            "AI 인프라와 반도체 CAPEX를 반복적으로 언급한다."
        )
        return [TranscriptSegment(0, 1, text)], "ko"

    def join_segments(self, segments):
        return " ".join(segment.text for segment in segments)


def test_analyze_resolved_videos_to_rows_uses_shared_pipeline(tmp_path: Path):
    config = AppConfig(
        provider="mock",
        output_dir=str(tmp_path),
        strategy=StrategyConfig(video_workers=1, fundamentals_workers=1),
    )
    video = VideoInput(
        video_id="abc123def45",
        title="엔비디아 데이터센터 투자",
        url="https://youtube.com/watch?v=abc123def45",
        description="엔비디아와 데이터센터 CAPEX를 다룬다.",
        published_at="20260408",
    )

    rows = analyze_resolved_videos_to_rows(
        [video],
        config=config,
        transcript_cache=TranscriptCache(tmp_path / "cache"),
        fetcher=_DummyFetcher(),
        output_dir=tmp_path,
        persist=False,
    )

    assert len(rows) == 1
    assert rows[0]["transcript_backed"] is True
    assert rows[0]["video_summary"]
    assert rows[0]["stocks"]
    assert rows[0]["stocks"][0]["evidence_snippets"]


def test_cache_only_transcript_fetcher_disables_live_fetch():
    fetcher = _CacheOnlyTranscriptFetcher()
    try:
        fetcher.fetch_with_source("abc123def45")
    except RuntimeError as exc:
        assert "cache-only historical backfill" in str(exc)
    else:
        raise AssertionError("expected RuntimeError")


def test_signal_backtest_analyze_videos_uses_shared_rows_helper(monkeypatch, tmp_path: Path):
    config = AppConfig(
        provider="mock",
        output_dir=str(tmp_path),
        strategy=StrategyConfig(video_workers=1, fundamentals_workers=1),
    )
    cache = TranscriptCache(tmp_path / "cache")
    videos = [VideoInput(video_id="abc123def45", title="제목", url="https://youtube.com/watch?v=abc123def45")]
    captured = {}

    def fake_analyze_resolved_videos_to_rows(videos_arg, *, config, transcript_cache, fetcher, output_dir, persist):
        captured["video_ids"] = [video.video_id for video in videos_arg]
        captured["fetcher_type"] = type(fetcher).__name__
        captured["persist"] = persist
        return [{"video_id": "abc123def45", "stocks": []}]

    monkeypatch.setattr("omx_brainstorm.signal_backtest.analyze_resolved_videos_to_rows", fake_analyze_resolved_videos_to_rows)

    rows = _analyze_videos(
        videos,
        cache=cache,
        fetcher=_CacheOnlyTranscriptFetcher(),
        config=config,
    )

    assert rows == [{"video_id": "abc123def45", "stocks": []}]
    assert captured["video_ids"] == ["abc123def45"]
    assert captured["fetcher_type"] == "_CacheOnlyTranscriptFetcher"
    assert captured["persist"] is False
