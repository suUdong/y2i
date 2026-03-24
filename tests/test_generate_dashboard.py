"""Tests for scripts/generate_dashboard.py"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

import generate_dashboard as gd


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def tmp_output(tmp_path):
    """Create a temporary output directory with sample data."""
    d = tmp_path / "output"
    d.mkdir()
    return d


@pytest.fixture
def sample_integration_report():
    return {
        "total_videos": 80,
        "type_distribution": {
            "OTHER": 18,
            "SECTOR": 8,
            "NEWS_EVENT": 12,
            "EXPERT_INTERVIEW": 22,
            "STOCK_PICK": 12,
            "MARKET_REVIEW": 4,
            "MACRO": 4,
        },
        "signal_distribution": {"NOISE": 43, "ACTIONABLE": 37},
        "analyzable_count": 37,
        "expert_extraction_rate": "18/22",
        "macro_coverage": "49/80",
        "per_video": [
            {
                "video_id": "v1",
                "title": "Test Video 1",
                "video_type": "EXPERT_INTERVIEW",
                "signal_class": "ACTIONABLE",
                "signal_score": 70.0,
                "should_analyze": True,
                "macro_count": 1,
                "expert_count": 1,
                "expert_names": ["김전문가"],
                "transcript_len": 2000,
            },
            {
                "video_id": "v2",
                "title": "Test Video 2",
                "video_type": "SECTOR",
                "signal_class": "NOISE",
                "signal_score": 12.0,
                "should_analyze": False,
                "macro_count": 0,
                "expert_count": 0,
                "expert_names": [],
                "transcript_len": 500,
            },
            {
                "video_id": "v3",
                "title": "Expert 김전문가 again",
                "video_type": "EXPERT_INTERVIEW",
                "signal_class": "NOISE",
                "signal_score": 20.0,
                "should_analyze": False,
                "macro_count": 0,
                "expert_count": 1,
                "expert_names": ["김전문가"],
                "transcript_len": 1500,
            },
        ],
    }


@pytest.fixture
def sample_30d():
    return {
        "channel_slug": "sampro",
        "channel_name": "삼프로TV",
        "generated_at": "20260323T094413Z",
        "window_days": 30,
        "videos": [
            {
                "video_id": "v1",
                "title": "반도체 전쟁 이란 트럼프",
                "signal_score": 70.0,
                "video_signal_class": "ACTIONABLE",
                "should_analyze_stocks": True,
                "signal_metrics": {
                    "macro_signal_count": 2,
                    "actionable_macro_count": 1,
                    "macro_sector_count": 3,
                },
                "stocks": [
                    {
                        "ticker": "005930.KS",
                        "company_name": "Samsung Electronics",
                        "final_score": 65.0,
                        "final_verdict": "WATCH",
                    }
                ],
            },
            {
                "video_id": "v2",
                "title": "금리 에너지 방산",
                "signal_score": 12.0,
                "video_signal_class": "NOISE",
                "should_analyze_stocks": False,
                "signal_metrics": {
                    "macro_signal_count": 1,
                    "actionable_macro_count": 0,
                    "macro_sector_count": 1,
                },
                "stocks": [],
            },
        ],
        "cross_video_ranking": [
            {
                "ticker": "005930.KS",
                "company_name": "Samsung Electronics Co., Ltd.",
                "aggregate_score": 61.9,
                "aggregate_verdict": "WATCH",
                "total_mentions": 27,
                "latest_price": 186300.0,
                "currency": "KRW",
            },
            {
                "ticker": "000660.KS",
                "company_name": "SK hynix Inc.",
                "aggregate_score": 50.0,
                "aggregate_verdict": "REJECT",
                "total_mentions": 11,
                "latest_price": 933000.0,
                "currency": "KRW",
            },
        ],
    }


@pytest.fixture
def sample_comparison():
    return {
        "generated_at": "20260323T053248Z",
        "window_days": 30,
        "channels": {
            "sampro": {
                "display_name": "삼프로TV",
                "total_videos": 80,
                "actionable_videos": 37,
                "actionable_ratio": 0.4625,
                "quality_scorecard": {
                    "overall": 45.0,
                    "transcript_coverage": 60.0,
                    "actionable_density": 46.2,
                    "ranking_predictive_power": 30.0,
                    "horizon_adequacy": 40.0,
                },
            },
            "itgod": {
                "display_name": "IT의 신",
                "total_videos": 0,
                "actionable_videos": 0,
                "actionable_ratio": 0.0,
                "quality_scorecard": {
                    "overall": 0.0,
                    "transcript_coverage": 0.0,
                    "actionable_density": 0.0,
                    "ranking_predictive_power": 0.0,
                    "horizon_adequacy": 0.0,
                },
            },
        },
        "more_actionable_channel": "sampro",
        "better_ranking_channel": "sampro",
    }


# ---------------------------------------------------------------------------
# Data loading tests
# ---------------------------------------------------------------------------

class TestLoadFunctions:
    def test_load_json_none_path(self):
        assert gd.load_json(None) is None

    def test_load_json_missing_file(self, tmp_path):
        assert gd.load_json(tmp_path / "nonexistent.json") is None

    def test_load_json_valid(self, tmp_path):
        p = tmp_path / "test.json"
        p.write_text('{"key": "value"}', encoding="utf-8")
        result = gd.load_json(p)
        assert result == {"key": "value"}

    def test_load_integration_report(self, tmp_output, sample_integration_report):
        p = tmp_output / "sampro_integration_report.json"
        p.write_text(json.dumps(sample_integration_report), encoding="utf-8")
        result = gd.load_integration_report(tmp_output)
        assert result["total_videos"] == 80

    def test_load_integration_report_missing(self, tmp_output):
        result = gd.load_integration_report(tmp_output)
        assert result is None

    def test_load_latest_30d(self, tmp_output, sample_30d):
        p = tmp_output / "sampro_30d_20260323T094413Z.json"
        p.write_text(json.dumps(sample_30d), encoding="utf-8")
        result = gd.load_latest_30d("sampro", tmp_output)
        assert result["channel_slug"] == "sampro"

    def test_load_latest_30d_picks_newest(self, tmp_output, sample_30d):
        old = tmp_output / "sampro_30d_20260322T000000Z.json"
        old.write_text(json.dumps({"old": True}), encoding="utf-8")
        import time; time.sleep(0.01)
        new = tmp_output / "sampro_30d_20260323T094413Z.json"
        new.write_text(json.dumps(sample_30d), encoding="utf-8")
        result = gd.load_latest_30d("sampro", tmp_output)
        assert result["channel_slug"] == "sampro"

    def test_load_latest_30d_missing(self, tmp_output):
        result = gd.load_latest_30d("sampro", tmp_output)
        assert result is None

    def test_load_latest_comparison(self, tmp_output, sample_comparison):
        p = tmp_output / "channel_comparison_30d_20260323T053248Z.json"
        p.write_text(json.dumps(sample_comparison), encoding="utf-8")
        result = gd.load_latest_comparison(tmp_output)
        assert "channels" in result

    def test_load_latest_comparison_missing(self, tmp_output):
        result = gd.load_latest_comparison(tmp_output)
        assert result is None


# ---------------------------------------------------------------------------
# Render section tests
# ---------------------------------------------------------------------------

class TestRenderHelpers:
    def test_bar_zero_max(self):
        assert gd._bar(5, 0) == ""

    def test_bar_full(self):
        result = gd._bar(10, 10, 10)
        assert result == "\u2588" * 10

    def test_bar_half(self):
        result = gd._bar(5, 10, 10)
        assert "\u2588" in result
        assert "\u2591" in result

    def test_pct_zero_total(self):
        assert gd._pct(5, 0) == "0.0%"

    def test_pct_normal(self):
        assert gd._pct(37, 80) == "46.2%"


class TestRenderPipelineSummary:
    def test_with_data(self, sample_integration_report):
        md = gd.render_pipeline_summary(sample_integration_report)
        assert "## Pipeline Summary (30 Days)" in md
        assert "**80**" in md
        assert "**37**" in md
        assert "46.2%" in md
        assert "ACTIONABLE" in md
        assert "NOISE" in md

    def test_no_data(self):
        md = gd.render_pipeline_summary(None)
        assert "No integration report data" in md


class TestRenderContentTypeDistribution:
    def test_with_data(self, sample_integration_report):
        md = gd.render_content_type_distribution(sample_integration_report)
        assert "## Content Type Distribution" in md
        assert "EXPERT_INTERVIEW" in md
        assert "27.5%" in md
        assert "| Type |" in md

    def test_no_data(self):
        md = gd.render_content_type_distribution(None)
        assert "No integration report data" in md

    def test_empty_distribution(self):
        md = gd.render_content_type_distribution({"type_distribution": {}})
        assert "No type distribution data" in md


class TestRenderStockRanking:
    def test_with_data(self, sample_30d):
        md = gd.render_stock_ranking(sample_30d)
        assert "## Stock Ranking" in md
        assert "005930.KS" in md
        assert "Samsung" in md
        assert "61.9" in md
        assert "WATCH" in md

    def test_sorted_descending(self, sample_30d):
        md = gd.render_stock_ranking(sample_30d)
        lines = md.split("\n")
        data_lines = [l for l in lines if l.startswith("| 1.") or l.startswith("| 2.")]
        assert len(data_lines) == 2
        assert "61.9" in data_lines[0]
        assert "50.0" in data_lines[1]

    def test_no_data(self):
        md = gd.render_stock_ranking(None)
        assert "No 30-day analysis data" in md

    def test_empty_ranking(self):
        md = gd.render_stock_ranking({"cross_video_ranking": []})
        assert "No stock ranking data" in md


class TestRenderMacroSignals:
    def test_with_data(self, sample_30d):
        md = gd.render_macro_signals(sample_30d)
        assert "## Macro Signals" in md
        assert "geopolitics" in md
        assert "semiconductor" in md

    def test_no_data(self):
        md = gd.render_macro_signals(None)
        assert "No 30-day analysis data" in md

    def test_no_macros(self):
        data = {"videos": [{"title": "boring video", "signal_metrics": {"macro_signal_count": 0}}]}
        md = gd.render_macro_signals(data)
        assert "No macro signals detected" in md


class TestRenderExpertInsights:
    def test_with_data(self, sample_integration_report):
        md = gd.render_expert_insights(sample_integration_report)
        assert "## Expert Insights" in md
        assert "김전문가" in md
        assert "2" in md  # appears twice

    def test_sorted_by_count(self, sample_integration_report):
        md = gd.render_expert_insights(sample_integration_report)
        assert "김전문가" in md

    def test_no_data(self):
        md = gd.render_expert_insights(None)
        assert "No integration report data" in md

    def test_empty_per_video(self):
        md = gd.render_expert_insights({"per_video": []})
        assert "No per-video data" in md

    def test_no_experts(self):
        md = gd.render_expert_insights({"per_video": [{"expert_names": []}]})
        assert "No expert data extracted" in md


class TestRenderChannelComparison:
    def test_with_data(self, sample_comparison):
        md = gd.render_channel_comparison(sample_comparison)
        assert "## Channel Comparison" in md
        assert "삼프로TV" in md
        assert "IT의 신" in md
        assert "Quality Scorecard" in md

    def test_no_data(self):
        md = gd.render_channel_comparison(None)
        assert "No channel comparison data" in md

    def test_empty_channels(self):
        md = gd.render_channel_comparison({"channels": {}})
        assert "No channel data" in md


# ---------------------------------------------------------------------------
# Integration test
# ---------------------------------------------------------------------------

class TestGenerateDashboard:
    def test_full_generation(self, tmp_output, sample_integration_report, sample_30d, sample_comparison):
        (tmp_output / "sampro_integration_report.json").write_text(
            json.dumps(sample_integration_report), encoding="utf-8"
        )
        (tmp_output / "sampro_30d_20260323T094413Z.json").write_text(
            json.dumps(sample_30d), encoding="utf-8"
        )
        (tmp_output / "channel_comparison_30d_20260323T053248Z.json").write_text(
            json.dumps(sample_comparison), encoding="utf-8"
        )
        dest = tmp_output.parent / "DASHBOARD.md"
        result = gd.generate_dashboard(tmp_output, dest)
        assert Path(result).exists()
        content = Path(result).read_text(encoding="utf-8")
        assert "# OMX Pipeline Dashboard" in content
        assert "## Pipeline Summary" in content
        assert "## Content Type Distribution" in content
        assert "## Stock Ranking" in content
        assert "## Macro Signals" in content
        assert "## Expert Insights" in content
        assert "## Channel Comparison" in content

    def test_empty_output_dir(self, tmp_output):
        dest = tmp_output.parent / "DASHBOARD.md"
        result = gd.generate_dashboard(tmp_output, dest)
        content = Path(result).read_text(encoding="utf-8")
        assert "# OMX Pipeline Dashboard" in content
        assert "No integration report data" in content

    def test_main_function(self, tmp_output, sample_integration_report):
        (tmp_output / "sampro_integration_report.json").write_text(
            json.dumps(sample_integration_report), encoding="utf-8"
        )
        dest = tmp_output.parent / "DASHBOARD.md"
        with patch.object(gd, "OUTPUT_DIR", tmp_output), \
             patch.object(gd, "DASHBOARD_PATH", dest):
            gd.main()
        assert dest.exists()


class TestAggregateMacroFromTitle:
    def test_multiple_keywords(self):
        agg = {}
        gd._aggregate_macro_from_title("반도체 전쟁 트럼프 금리", "ACTIONABLE", agg)
        assert "semiconductor" in agg
        assert "geopolitics" in agg
        assert "us_policy" in agg
        assert "interest_rate" in agg

    def test_accumulates_count(self):
        agg = {}
        gd._aggregate_macro_from_title("전쟁 뉴스", "ACTIONABLE", agg)
        gd._aggregate_macro_from_title("중동 전쟁", "NOISE", agg)
        assert agg["geopolitics"]["count"] == 3  # 전쟁x2 + 중동x1

    def test_no_keywords(self):
        agg = {}
        gd._aggregate_macro_from_title("일반 영상 제목", "NOISE", agg)
        assert len(agg) == 0
