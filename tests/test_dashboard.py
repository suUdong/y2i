"""Tests for dashboard.data_loader module."""
from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest
from streamlit.testing.v1 import AppTest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import dashboard.data_loader as data_loader_module
from dashboard.data_loader import (
    _latest_file,
    _load_json,
    build_live_feed_events,
    build_overview_report,
    build_signal_timeline,
    extract_actionable_signals,
    extract_channel_leaderboard,
    extract_cross_video_ranking,
    extract_expert_insights,
    extract_macro_signals,
    extract_per_video,
    extract_recent_tracked_signals,
    extract_signal_distribution,
    extract_signal_accuracy_summary,
    extract_type_distribution,
    extract_videos,
    get_available_channels,
    get_all_rankings,
    get_last_update_time,
    get_live_feed_data,
    get_pipeline_activity,
    get_recent_videos,
    get_signal_chart_records,
    load_30d_results,
    load_all_video_titles,
    load_channel_comparison,
    load_integration_report,
    load_signal_accuracy_summary,
    load_tracker_records,
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
                "should_analyze_stocks": True,
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
                "should_analyze_stocks": False,
                "published_at": "20260321",
                "skip_reason": "종목 분석에 활용할 실질 신호가 부족함",
                "stocks": [],
            },
        ],
        "cross_video_ranking": [
            {
                "ticker": "005930.KS",
                "company_name": "Samsung",
                "aggregate_score": 85.0,
                "aggregate_verdict": "STRONG_BUY",
                "appearances": 2,
                "total_mentions": 3,
                "latest_price": 60320.0,
                "currency": "KRW",
                "first_signal_at": "2026-03-20",
                "last_signal_at": "2026-03-21",
                "latest_checked_at": "2026-03-23T00:00:00+00:00",
            },
            {
                "ticker": "000660.KS",
                "company_name": "SK Hynix",
                "aggregate_score": 72.0,
                "aggregate_verdict": "BUY",
                "appearances": 1,
                "total_mentions": 2,
                "latest_price": 120000.0,
                "currency": "KRW",
                "first_signal_at": "2026-03-18",
                "last_signal_at": "2026-03-18",
                "latest_checked_at": "2026-03-23T00:00:00+00:00",
            },
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
        "cross_video_ranking": [
            {
                "ticker": "005930.KS",
                "company_name": "Samsung",
                "aggregate_score": 74.0,
                "aggregate_verdict": "BUY",
                "appearances": 1,
                "total_mentions": 1,
                "latest_price": 60320.0,
                "currency": "KRW",
                "first_signal_at": "2026-03-22",
                "last_signal_at": "2026-03-22",
                "latest_checked_at": "2026-03-23T00:00:00+00:00",
            },
        ],
        "quality_scorecard": {"overall": 0.0},
    }
    (tmp_path / "itgod_30d_20260323T053248Z.json").write_text(json.dumps(itgod_30d), encoding="utf-8")

    # Channel comparison
    comparison = {
        "generated_at": "20260323T053248Z",
        "window_days": 30,
        "pipeline_summary": {
            "latest_reference_at": "20260323T053248Z",
            "latest_reference_kind": "generated_at",
        },
        "channels": {
            "sampro": {"display_name": "Test Channel", "total_videos": 10, "actionable_videos": 4, "strict_actionable_videos": 3, "actionable_ratio": 0.4, "latest_reference_at": "20260323T053248Z", "latest_reference_kind": "generated_at", "quality_scorecard": {"overall": 0.6}},
            "itgod": {"display_name": "IT God", "total_videos": 0, "actionable_videos": 0, "strict_actionable_videos": 0, "actionable_ratio": 0.0, "latest_reference_at": "", "latest_reference_kind": "unknown", "quality_scorecard": {"overall": 0.0}},
        },
        "more_actionable_channel": "sampro",
        "better_ranking_channel": "sampro",
    }
    (tmp_path / "channel_comparison_30d_20260323T053248Z.json").write_text(json.dumps(comparison), encoding="utf-8")

    tracker_data = {
        "signals": [
            {
                "ticker": "005930.KS",
                "company_name": "Samsung",
                "channel_slug": "sampro",
                "signal_date": "2026-03-20",
                "signal_score": 78.5,
                "verdict": "BUY",
                "entry_date": "2026-03-20",
                "entry_price": 58000.0,
                "latest_price": 60320.0,
                "latest_price_date": "2026-03-25",
                "price_path": [
                    {"date": "2026-03-20", "close": 58000.0, "days_from_signal": 0, "days_from_entry": 0, "return_pct": 0.0},
                    {"date": "2026-03-21", "close": 58696.0, "days_from_signal": 1, "days_from_entry": 1, "return_pct": 1.2},
                    {"date": "2026-03-25", "close": 60320.0, "days_from_signal": 5, "days_from_entry": 5, "return_pct": 4.0},
                ],
                "returns": {"1d": 1.2, "3d": 2.5, "5d": 4.0, "10d": 6.0, "20d": None},
                "recorded_at": "2026-03-23T00:00:00+00:00",
                "last_updated": "2026-03-23T00:00:00+00:00",
            },
            {
                "ticker": "000660.KS",
                "company_name": "SK Hynix",
                "channel_slug": "sampro",
                "signal_date": "2026-03-18",
                "signal_score": 55.0,
                "verdict": "WATCH",
                "entry_date": "2026-03-18",
                "entry_price": 120000.0,
                "returns": {"1d": -0.5, "3d": 1.0, "5d": -2.0, "10d": None, "20d": None},
                "recorded_at": "2026-03-23T00:00:00+00:00",
                "last_updated": "2026-03-23T00:00:00+00:00",
            },
        ],
        "updated_at": "2026-03-23T00:00:00+00:00",
    }
    tracker_dir = tmp_path / ".omx" / "state"
    tracker_dir.mkdir(parents=True, exist_ok=True)
    (tracker_dir / "signal_tracker.json").write_text(json.dumps(tracker_data), encoding="utf-8")

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
        assert data["videos"][1]["skip_reason"] == "종목 분석에 활용할 실질 신호가 부족함"

    def test_loads_itgod(self, tmp_output: Path):
        data = load_30d_results("itgod", tmp_output)
        assert data["channel_slug"] == "itgod"

    def test_empty_for_unknown_channel(self, tmp_output: Path):
        assert load_30d_results("unknown_channel", tmp_output) == {}

    def test_prefers_matching_run_id_when_available(self, tmp_output: Path):
        newer = {
            "channel_slug": "sampro",
            "channel_name": "Test Channel Newer",
            "generated_at": "20260324T000000Z",
            "window_days": 30,
            "videos": [],
            "cross_video_ranking": [],
            "quality_scorecard": {"overall": 0.0},
        }
        (tmp_output / "sampro_30d_20260324T000000Z.json").write_text(json.dumps(newer), encoding="utf-8")
        data = load_30d_results("sampro", tmp_output, preferred_run_id="20260323T094413Z")
        assert data["generated_at"] == "20260323T094413Z"


# ── load_channel_comparison ──────────────────────────────────────────────────

class TestLoadChannelComparison:
    def test_loads(self, tmp_output: Path):
        comp = load_channel_comparison(tmp_output)
        assert "channels" in comp
        assert "sampro" in comp["channels"]
        assert "pipeline_summary" in comp
        assert comp["signal_accuracy"]["overall"]["total_signals"] == 2
        assert comp["channels"]["sampro"]["hit_rate_5d"] == 50.0
        assert comp["channels"]["sampro"]["weight_multiplier"] is not None
        assert comp["channels"]["sampro"]["skipped_videos"] == 1
        assert comp["channels"]["sampro"]["strict_actionable_videos"] == 1
        assert comp["channels"]["sampro"]["latest_published_at"] == "20260321"
        assert comp["channels"]["sampro"]["latest_reference_at"] == "20260321"
        assert comp["channels"]["sampro"]["latest_reference_kind"] == "published_at"
        assert comp["pipeline_summary"]["skipped_videos"] == 1
        assert comp["pipeline_summary"]["strict_actionable_videos"] == 1
        assert comp["pipeline_summary"]["latest_reference_at"] == "20260323T053248Z"
        assert comp["pipeline_summary"]["latest_reference_kind"] == "generated_at"

    def test_prefers_channel_artifacts_aligned_to_comparison_run(self, tmp_output: Path):
        aligned = {
            "channel_slug": "sampro",
            "channel_name": "Test Channel",
            "generated_at": "20260323T053248Z",
            "window_days": 30,
            "videos": [
                {
                    "video_id": "v1",
                    "title": "Aligned Video",
                    "video_signal_class": "NOISE",
                    "should_analyze_stocks": False,
                    "signal_score": 20.0,
                    "published_at": "20260323",
                    "skip_reason": "aligned",
                    "stocks": [],
                }
            ],
            "cross_video_ranking": [],
            "quality_scorecard": {"overall": 1.0},
        }
        (tmp_output / "sampro_30d_20260323T053248Z.json").write_text(json.dumps(aligned), encoding="utf-8")
        newer = {
            "channel_slug": "sampro",
            "channel_name": "Test Channel Newer",
            "generated_at": "20260324T000000Z",
            "window_days": 30,
            "videos": [
                {
                    "video_id": "v3",
                    "title": "Later Video",
                    "video_signal_class": "ACTIONABLE",
                    "should_analyze_stocks": True,
                    "signal_score": 88.0,
                    "published_at": "20260324",
                    "stocks": [],
                }
            ],
            "cross_video_ranking": [],
            "quality_scorecard": {"overall": 99.0},
        }
        (tmp_output / "sampro_30d_20260324T000000Z.json").write_text(json.dumps(newer), encoding="utf-8")
        comp = load_channel_comparison(tmp_output)
        assert comp["channels"]["sampro"]["display_name"] == "Test Channel"
        assert comp["channels"]["sampro"]["strict_actionable_videos"] == 0
        assert comp["channels"]["sampro"]["latest_published_at"] == "20260323"
        assert comp["channels"]["sampro"]["latest_reference_at"] == "20260323"

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

    def test_extract_signal_accuracy_helpers(self, tmp_output: Path):
        comp = load_channel_comparison(tmp_output)
        accuracy = extract_signal_accuracy_summary(comp)
        leaderboard = extract_channel_leaderboard(comp)
        recent = extract_recent_tracked_signals(comp)
        assert accuracy["overall"]["hit_rate_5d"] == 50.0
        assert leaderboard[0]["slug"] == "sampro"
        assert recent[0]["ticker"] == "005930.KS"


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


class TestLoadSignalAccuracySummary:
    def test_loads_embedded_or_fallback_tracker_summary(self, tmp_output: Path):
        summary = load_signal_accuracy_summary(tmp_output, load_channel_comparison(tmp_output))
        assert summary["overall"]["total_signals"] == 2
        assert summary["overall"]["signals_with_price_3d"] == 2
        assert summary["overall"]["window_stats"]["5d"]["tracked"] == 2
        assert summary["channel_leaderboard"][0]["weight_multiplier"] is not None


class TestConsensusRanking:
    def test_get_all_rankings_uses_weighted_consensus(self, tmp_output: Path):
        rankings = get_all_rankings(tmp_output)
        assert rankings[0]["ticker"] == "005930.KS"
        assert rankings[0]["channel_count"] == 2
        assert rankings[0]["channel_weight_sum"] > 0
        assert rankings[0]["aggregate_score"] > rankings[0]["weighted_base_score"]


class TestTrackerDerivedLoaders:
    def test_load_tracker_records(self, tmp_output: Path):
        records = load_tracker_records(tmp_output)
        assert len(records) == 2
        assert records[0]["ticker"] == "005930.KS"

    def test_build_signal_timeline_prefers_price_path(self, tmp_output: Path):
        record = load_tracker_records(tmp_output)[0]
        timeline = build_signal_timeline(record)
        assert len(timeline) == 3
        assert timeline[-1]["close"] == 60320.0
        assert timeline[-1]["source"] == "price_path"

    def test_build_signal_timeline_falls_back_to_returns(self, tmp_output: Path):
        record = load_tracker_records(tmp_output)[1]
        timeline = build_signal_timeline(record)
        assert timeline[0]["close"] == 120000.0
        assert timeline[-1]["return_pct"] == -2.0
        assert timeline[-1]["source"] == "returns_fallback"

    def test_get_signal_chart_records(self, tmp_output: Path):
        records = get_signal_chart_records(tmp_output)
        assert records[0]["record_key"].startswith("005930.KS|sampro|")
        assert records[0]["channel_display"] == "Test Channel"
        assert records[0]["timeline"]

    def test_build_live_feed_events(self, tmp_output: Path):
        events = build_live_feed_events(tmp_output, hours=9999, limit=20)
        assert any(event["event_type"] == "video_analysis" for event in events)
        assert any(event["event_type"] == "signal_update" for event in events)
        assert events[0]["channel_display"] in {"Test Channel", "IT God"}


class TestGetLiveFeedData:
    def test_returns_expected_keys(self, tmp_output: Path):
        data = get_live_feed_data(tmp_output, hours=9999)
        assert "recent_videos" in data
        assert "recent_signals" in data
        assert "feed_events" in data
        assert "signal_chart_records" in data
        assert "last_update" in data

    def test_recent_videos_is_list(self, tmp_output: Path):
        data = get_live_feed_data(tmp_output, hours=9999)
        assert isinstance(data["recent_videos"], list)

    def test_recent_signals_is_list(self, tmp_output: Path):
        data = get_live_feed_data(tmp_output, hours=9999)
        assert isinstance(data["recent_signals"], list)

    def test_empty_dir_returns_empty_lists(self, tmp_path: Path):
        data = get_live_feed_data(tmp_path, hours=48)
        assert data["recent_videos"] == []
        assert data["recent_signals"] == []
        assert data["last_update"] is None


def test_streamlit_app_runs_without_session_errors(tmp_output: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(data_loader_module, "DEFAULT_OUTPUT_DIR", tmp_output)

    at = AppTest.from_file("dashboard/app.py", default_timeout=60)
    at.query_params["token"] = "6149ba10085f1be3"
    at.run(timeout=60)

    assert not at.exception
    assert at.title[0].value == "Y2I 투자 시그널"


def test_streamlit_app_blocks_without_auth(tmp_output: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(data_loader_module, "DEFAULT_OUTPUT_DIR", tmp_output)

    at = AppTest.from_file("dashboard/app.py", default_timeout=60)
    at.run(timeout=60)

    assert not at.exception
    assert len(at.title) == 0
    assert len(at.markdown) == 1
    assert "403 Forbidden" in at.markdown[0].value
