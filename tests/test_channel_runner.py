from pathlib import Path

from scripts.run_channel_30d_comparison import quality_scorecard, recent_feed_video_ids, run_comparison_job
from omx_brainstorm.app_config import AppConfig, ChannelConfig


def test_recent_feed_video_ids_returns_empty_on_failure(monkeypatch):
    def boom(*args, **kwargs):
        raise OSError("feed down")

    monkeypatch.setattr("scripts.run_channel_30d_comparison.urlopen", boom)
    assert recent_feed_video_ids("dummy", days=30) == []


def test_quality_scorecard_handles_empty_rows():
    scorecard = quality_scorecard([], {}, [])
    assert scorecard["overall"] == 0.0
    assert scorecard["transcript_coverage"] == 0.0
    assert scorecard["actionable_density"] == 0.0


def test_quality_scorecard_zero_predictive_power_without_positions():
    scorecard = quality_scorecard([{"should_analyze_stocks": True, "transcript_language": "ko"}], {}, [])
    assert scorecard["ranking_predictive_power"] == 0.0


def test_quality_scorecard_zero_horizon_without_positions():
    scorecard = quality_scorecard([{"should_analyze_stocks": True, "transcript_language": "ko"}], {}, [])
    assert scorecard["horizon_adequacy"] == 0.0


def test_recent_feed_video_ids_returns_empty_without_channel_id():
    assert recent_feed_video_ids("", days=30) == []


def test_quality_scorecard_transcript_coverage_counts_cache():
    scorecard = quality_scorecard([{"should_analyze_stocks": True, "transcript_language": "cache:ko"}], {}, [])
    assert scorecard["transcript_coverage"] == 100.0


def test_run_comparison_job_handles_empty_channel_set(tmp_path):
    config = AppConfig(output_dir=str(tmp_path / "out"), registry_path=str(tmp_path / "channels.json"), channels=[])
    payload = run_comparison_job(config)
    assert "channels" in payload
