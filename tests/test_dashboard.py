"""Tests for dashboard.data_loader module."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from dashboard.data_loader import (
    _latest_file,
    _load_json,
    build_overview_report,
    extract_actionable_signals,
    extract_cross_video_ranking,
    extract_expert_insights,
    extract_macro_signals,
    extract_per_video,
    extract_signal_distribution,
    extract_type_distribution,
    extract_videos,
    get_available_channels,
    get_last_update_time,
    get_pipeline_activity,
    get_recent_videos,
    load_30d_results,
    load_all_video_titles,
    load_channel_comparison,
    load_integration_report,
    load_video_titles,
)


@pytest.fixture
def tmp_output(tmp_path: Path) -> Path:
    """Create a temporary output directory with sample data files."""
    # Integration report
    integration = {
        "total_videos": 10,
        "type_distribution": {"STOCK_PICK": 3, "MACRO": 2, "OTHER": 5},
        "signal_distribution": {"ACTIONABLE": 4, "NOISE": 6},
        "analyzable_count": 4,
        "expert_extraction_rate": "2/3",
        "macro_coverage": "5/10",
        "per_video": [
            {"video_id": "v1", "title": "Test Video", "video_type": "STOCK_PICK", "signal_class": "ACTIONABLE", "signal_score": 70.0},
        ],
    }
    (tmp_path / "sampro_integration_report.json").write_text(json.dumps(integration), encoding="utf-8")

    # 30d results
    sampro_30d = {
        "channel_slug": "sampro",
        "channel_name": "Test Channel",
        "generated_at": "20260323T094413Z",
        "window_days": 30,
        "videos": [
            {
                "video_id": "v1",
                "title": "Test Video 1",
                "video_signal_class": "ACTIONABLE",
                "signal_score": 70.0,
                "published_at": "20260320",
                "stocks": [],
                "macro_insights": [
                    {"indicator": "interest_rate", "direction": "DOWN", "label": "Rate cut", "confidence": 0.8, "sentiment": "BULLISH"},
                ],
                "expert_insights": [
                    {"expert_name": "Dr. Kim", "affiliation": "ABC Securities", "key_claims": ["Rates will drop"], "topic": "Macro", "sentiment": "BULLISH"},
                ],
            },
            {
                "video_id": "v2",
                "title": "Test Video 2",
                "video_signal_class": "NOISE",
                "signal_score": 20.0,
                "published_at": "20260321",
                "stocks": [],
            },
        ],
        "cross_video_ranking": [
            {"ticker": "005930.KS", "company_name": "Samsung", "total_score": 85.0, "mention_count": 3, "final_verdict": "BUY"},
            {"ticker": "000660.KS", "company_name": "SK Hynix", "total_score": 72.0, "mention_count": 2, "final_verdict": "HOLD"},
        ],
        "quality_scorecard": {"overall": 0.6, "transcript_coverage": 0.8, "actionable_density": 0.4, "ranking_predictive_power": 0.5, "horizon_adequacy": 0.7},
    }
    (tmp_path / "sampro_30d_20260323T094413Z.json").write_text(json.dumps(sampro_30d), encoding="utf-8")

    # Another channel
    itgod_30d = {
        "channel_slug": "itgod",
        "channel_name": "IT God",
        "generated_at": "20260323T053248Z",
        "window_days": 30,
        "videos": [],
        "cross_video_ranking": [],
        "quality_scorecard": {"overall": 0.0},
    }
    (tmp_path / "itgod_30d_20260323T053248Z.json").write_text(json.dumps(itgod_30d), encoding="utf-8")

    # Channel comparison
    comparison = {
        "generated_at": "20260323T053248Z",
        "window_days": 30,
        "channels": {
            "sampro": {"display_name": "Test Channel", "total_videos": 10, "actionable_videos": 4, "actionable_ratio": 0.4, "quality_scorecard": {"overall": 0.6}},
            "itgod": {"display_name": "IT God", "total_videos": 0, "actionable_videos": 0, "actionable_ratio": 0.0, "quality_scorecard": {"overall": 0.0}},
        },
        "more_actionable_channel": "sampro",
        "better_ranking_channel": "sampro",
    }
    (tmp_path / "channel_comparison_30d_20260323T053248Z.json").write_text(json.dumps(comparison), encoding="utf-8")

    # Video titles
    titles = {
        "channel": "Test",
        "titles": [
            {"video_id": "v1", "title": "Test", "published_at": "2026-03-20", "labels": ["macro", "interview"]},
            {"video_id": "v2", "title": "Test2", "published_at": "2026-03-21", "labels": ["sector"]},
        ],
    }
    (tmp_path / "sampro_video_titles.json").write_text(json.dumps(titles), encoding="utf-8")
    other_titles = {
        "channel": "IT God",
        "titles": [
            {"video_id": "it1", "title": "AI Theme", "published_at": "2026-03-22", "labels": ["ai", "sector"]},
        ],
    }
    (tmp_path / "itgod_video_titles.json").write_text(json.dumps(other_titles), encoding="utf-8")

    return tmp_path


# ── _latest_file ─────────────────────────────────────────────────────────────

class TestLatestFile:
    def test_returns_file(self, tmp_output: Path):
        result = _latest_file(tmp_output, "sampro_30d_*.json")
        assert result is not None
        assert "sampro_30d_" in result.name

    def test_returns_none_for_no_match(self, tmp_output: Path):
        assert _latest_file(tmp_output, "nonexistent_*.json") is None


# ── _load_json ───────────────────────────────────────────────────────────────

class TestLoadJson:
    def test_loads_valid_json(self, tmp_output: Path):
        result = _load_json(tmp_output / "sampro_integration_report.json")
        assert isinstance(result, dict)
        assert result["total_videos"] == 10

    def test_returns_empty_dict_for_none(self):
        assert _load_json(None) == {}

    def test_returns_empty_dict_for_missing_file(self, tmp_path: Path):
        assert _load_json(tmp_path / "does_not_exist.json") == {}


# ── load_integration_report ──────────────────────────────────────────────────

class TestLoadIntegrationReport:
    def test_loads_report(self, tmp_output: Path):
        report = load_integration_report(tmp_output)
        assert report["total_videos"] == 10
        assert "type_distribution" in report

    def test_empty_when_missing(self, tmp_path: Path):
        assert load_integration_report(tmp_path) == {}


# ── load_30d_results ─────────────────────────────────────────────────────────

class TestLoad30dResults:
    def test_loads_sampro(self, tmp_output: Path):
        data = load_30d_results("sampro", tmp_output)
        assert data["channel_slug"] == "sampro"
        assert len(data["videos"]) == 2

    def test_loads_itgod(self, tmp_output: Path):
        data = load_30d_results("itgod", tmp_output)
        assert data["channel_slug"] == "itgod"

    def test_empty_for_unknown_channel(self, tmp_output: Path):
        assert load_30d_results("unknown_channel", tmp_output) == {}


# ── load_channel_comparison ──────────────────────────────────────────────────

class TestLoadChannelComparison:
    def test_loads(self, tmp_output: Path):
        comp = load_channel_comparison(tmp_output)
        assert "channels" in comp
        assert "sampro" in comp["channels"]

    def test_empty_when_missing(self, tmp_path: Path):
        assert load_channel_comparison(tmp_path) == {}


# ── load_video_titles ────────────────────────────────────────────────────────

class TestLoadVideoTitles:
    def test_loads(self, tmp_output: Path):
        titles = load_video_titles(tmp_output)
        assert "titles" in titles
        assert len(titles["titles"]) == 2

    def test_empty_when_missing(self, tmp_path: Path):
        assert load_video_titles(tmp_path) == {}


class TestLoadAllVideoTitles:
    def test_loads_and_merges_all_channels(self, tmp_output: Path):
        titles = load_all_video_titles(tmp_output)
        assert "titles" in titles
        assert len(titles["titles"]) == 3
        assert {row["_channel"] for row in titles["titles"]} == {"sampro", "itgod"}

    def test_empty_when_missing(self, tmp_path: Path):
        assert load_all_video_titles(tmp_path) == {}


# ── extract helpers ──────────────────────────────────────────────────────────

class TestExtractHelpers:
    def test_type_distribution(self, tmp_output: Path):
        report = load_integration_report(tmp_output)
        dist = extract_type_distribution(report)
        assert dist == {"STOCK_PICK": 3, "MACRO": 2, "OTHER": 5}

    def test_type_distribution_empty(self):
        assert extract_type_distribution({}) == {}

    def test_signal_distribution(self, tmp_output: Path):
        report = load_integration_report(tmp_output)
        dist = extract_signal_distribution(report)
        assert dist["ACTIONABLE"] == 4

    def test_signal_distribution_empty(self):
        assert extract_signal_distribution({}) == {}

    def test_per_video(self, tmp_output: Path):
        report = load_integration_report(tmp_output)
        pv = extract_per_video(report)
        assert len(pv) == 1
        assert pv[0]["video_id"] == "v1"

    def test_per_video_empty(self):
        assert extract_per_video({}) == []

    def test_cross_video_ranking(self, tmp_output: Path):
        data = load_30d_results("sampro", tmp_output)
        ranking = extract_cross_video_ranking(data)
        assert len(ranking) == 2
        assert ranking[0]["ticker"] == "005930.KS"

    def test_cross_video_ranking_empty(self):
        assert extract_cross_video_ranking({}) == []

    def test_extract_videos(self, tmp_output: Path):
        data = load_30d_results("sampro", tmp_output)
        videos = extract_videos(data)
        assert len(videos) == 2

    def test_extract_videos_empty(self):
        assert extract_videos({}) == []


# ── extract_expert_insights ──────────────────────────────────────────────────

class TestExtractExpertInsights:
    def test_extracts_insights(self, tmp_output: Path):
        data = load_30d_results("sampro", tmp_output)
        videos = extract_videos(data)
        insights = extract_expert_insights(videos)
        assert len(insights) == 1
        assert insights[0]["expert_name"] == "Dr. Kim"
        assert insights[0]["source_video"] == "Test Video 1"

    def test_empty_when_no_insights(self, tmp_output: Path):
        data = load_30d_results("itgod", tmp_output)
        videos = extract_videos(data)
        assert extract_expert_insights(videos) == []

    def test_empty_list(self):
        assert extract_expert_insights([]) == []


# ── extract_macro_signals ────────────────────────────────────────────────────

class TestExtractMacroSignals:
    def test_extracts_macros(self, tmp_output: Path):
        data = load_30d_results("sampro", tmp_output)
        videos = extract_videos(data)
        signals = extract_macro_signals(videos)
        assert len(signals) == 1
        assert signals[0]["indicator"] == "interest_rate"
        assert signals[0]["source_video"] == "Test Video 1"

    def test_empty_when_no_macros(self):
        assert extract_macro_signals([]) == []

    def test_videos_without_macro_key(self):
        videos = [{"video_id": "x", "title": "No macros"}]
        assert extract_macro_signals(videos) == []


# ── get_available_channels ───────────────────────────────────────────────────

class TestGetAvailableChannels:
    def test_finds_channels(self, tmp_output: Path):
        channels = get_available_channels(tmp_output)
        assert "sampro" in channels
        assert "itgod" in channels
        assert "channel_comparison" not in channels

    def test_empty_dir(self, tmp_path: Path):
        assert get_available_channels(tmp_path) == []

    def test_sorted(self, tmp_output: Path):
        channels = get_available_channels(tmp_output)
        assert channels == sorted(channels)


# ── get_last_update_time (US-002) ───────────────────────────────────────────

class TestGetLastUpdateTime:
    def test_returns_datetime(self, tmp_output: Path):
        result = get_last_update_time(tmp_output)
        assert result is not None
        assert hasattr(result, "tzinfo")
        assert result.tzinfo is not None

    def test_returns_none_for_empty_dir(self, tmp_path: Path):
        assert get_last_update_time(tmp_path) is None


# ── get_recent_videos (US-003) ──────────────────────────────────────────────

class TestGetRecentVideos:
    def test_returns_videos_from_recent_files(self, tmp_output: Path):
        # Files just created → mtime is now → within 24h
        videos = get_recent_videos(tmp_output, hours=24)
        assert len(videos) >= 2  # sampro has 2 videos
        assert all("_channel" in v for v in videos)

    def test_returns_empty_for_zero_hours(self, tmp_output: Path):
        videos = get_recent_videos(tmp_output, hours=0)
        assert videos == []

    def test_returns_empty_for_empty_dir(self, tmp_path: Path):
        assert get_recent_videos(tmp_path) == []

    def test_sorts_by_recent_date_then_score(self, tmp_output: Path):
        videos = get_recent_videos(tmp_output, hours=24)
        assert videos[0]["published_at"] >= videos[-1]["published_at"]


# ── extract_actionable_signals (US-006) ─────────────────────────────────────

class TestExtractActionableSignals:
    def test_finds_actionable(self, tmp_output: Path):
        signals = extract_actionable_signals(tmp_output)
        assert len(signals) >= 1
        assert all(s.get("title") for s in signals)
        assert all(s.get("channel") for s in signals)

    def test_sorted_by_score(self, tmp_output: Path):
        signals = extract_actionable_signals(tmp_output)
        if len(signals) >= 2:
            assert signals[0]["signal_score"] >= signals[1]["signal_score"]

    def test_empty_dir(self, tmp_path: Path):
        assert extract_actionable_signals(tmp_path) == []


# ── get_pipeline_activity (US-004) ──────────────────────────────────────────

class TestGetPipelineActivity:
    def test_returns_entries(self, tmp_output: Path):
        entries = get_pipeline_activity(tmp_output)
        assert len(entries) >= 1
        assert "channel" in entries[0]
        assert "timestamp" in entries[0]

    def test_respects_limit(self, tmp_output: Path):
        entries = get_pipeline_activity(tmp_output, limit=1)
        assert len(entries) == 1

    def test_empty_dir(self, tmp_path: Path):
        assert get_pipeline_activity(tmp_path) == []


class TestBuildOverviewReport:
    def test_aggregates_channels(self, tmp_output: Path):
        report = build_overview_report(tmp_output)
        assert report["channel_count"] == 2
        assert report["total_videos"] == 2
        assert report["analyzable_count"] == 1
        assert report["expert_video_count"] == 1
        assert report["macro_video_count"] == 1
        assert report["type_distribution"]["OTHER"] == 2

    def test_includes_channel_labels_in_per_video(self, tmp_output: Path):
        report = build_overview_report(tmp_output)
        assert report["per_video"][0]["channel"] in {"Test Channel", "IT God"}
