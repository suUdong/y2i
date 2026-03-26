"""Tests for comparison module: quality scorecard, artifact saving, channel comparison."""
import json
from dataclasses import dataclass
from pathlib import Path

from omx_brainstorm.comparison import (
    RunContext,
    compare_channels,
    quality_scorecard,
    save_channel_artifacts,
)


@dataclass
class _FakeRankedStock:
    ticker: str
    company_name: str
    aggregate_score: float
    first_signal_at: str
    aggregate_verdict: str = "BUY"
    appearances: int = 1
    total_mentions: int = 2
    average_signal_strength: float = 0.7
    differentiation_score: float = 0.5
    average_final_score: float = 70.0
    best_final_score: float = 80.0
    last_signal_at: str | None = None
    latest_checked_at: str | None = None
    latest_price: float | None = 100.0
    currency: str | None = "USD"
    source_video_ids: list[str] | None = None
    source_video_titles: list[str] | None = None

    def to_dict(self):
        return {
            "ticker": self.ticker,
            "company_name": self.company_name,
            "aggregate_score": self.aggregate_score,
            "first_signal_at": self.first_signal_at,
        }


def _make_row(title="test video", should_analyze=True, stocks=None, transcript_language="ko"):
    return {
        "title": title,
        "url": "https://youtube.com/watch?v=t1",
        "published_at": "2026-03-01",
        "video_signal_class": "ACTIONABLE",
        "signal_score": 75.0,
        "should_analyze_stocks": should_analyze,
        "reason": "test",
        "skip_reason": "" if should_analyze else "test",
        "transcript_language": transcript_language,
        "stocks": stocks or [],
    }


def _make_validation(ranking_len=2):
    return {
        "top_1": {"portfolio_return_pct": 5.0, "positions": [{"ticker": "NVDA", "entry_date": "2026-03-01", "exit_date": "2026-03-15", "return_pct": 5.0}]},
        "top_3": {"portfolio_return_pct": 3.0, "positions": [{"ticker": "NVDA", "entry_date": "2026-03-01", "exit_date": "2026-03-15", "return_pct": 5.0}]},
        f"top_{ranking_len}": {
            "portfolio_return_pct": 2.0,
            "positions": [
                {"ticker": "NVDA", "entry_date": "2026-03-01", "exit_date": "2026-03-15", "return_pct": 5.0},
                {"ticker": "005930.KS", "entry_date": "2026-03-01", "exit_date": "2026-03-15", "return_pct": -1.0},
            ],
        },
    }


def _make_ranking():
    return [
        _FakeRankedStock("NVDA", "NVIDIA", 80.0, "2026-03-01"),
        _FakeRankedStock("005930.KS", "삼성전자", 70.0, "2026-03-02"),
    ]


def _make_context(tmp_path):
    return RunContext(run_id="20260323", today="2026-03-23", output_dir=tmp_path, window_days=30)


def test_quality_scorecard_basic():
    rows = [_make_row(should_analyze=True), _make_row(should_analyze=False)]
    ranking = _make_ranking()
    validation = _make_validation(len(ranking))
    sc = quality_scorecard(rows, validation, ranking)
    assert "overall" in sc
    assert "transcript_coverage" in sc
    assert "actionable_density" in sc
    assert "ranking_predictive_power" in sc
    assert "horizon_adequacy" in sc
    assert 0 <= sc["overall"] <= 100


def test_quality_scorecard_empty_rows():
    sc = quality_scorecard([], {}, [])
    assert sc["overall"] == 0.0
    assert sc["transcript_coverage"] == 0.0


def test_quality_scorecard_metadata_fallback():
    rows = [_make_row(transcript_language="metadata_fallback")]
    sc = quality_scorecard(rows, {}, [])
    assert sc["transcript_coverage"] == 0.0


def test_save_channel_artifacts_creates_files(tmp_path):
    rows = [_make_row()]
    ranking = _make_ranking()
    validation = _make_validation(len(ranking))
    scorecard = quality_scorecard(rows, validation, ranking)
    context = _make_context(tmp_path)

    json_path, txt_path = save_channel_artifacts(
        slug="test_ch",
        display_name="Test Channel",
        channel_url="https://youtube.com/channel/test",
        rows=rows,
        ranking=ranking,
        validation=validation,
        scorecard=scorecard,
        context=context,
    )
    assert json_path.exists()
    assert txt_path.exists()
    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert payload["channel_slug"] == "test_ch"
    assert len(payload["videos"]) == 1
    txt = txt_path.read_text(encoding="utf-8")
    assert "Test Channel" in txt


def test_save_channel_artifacts_with_stocks(tmp_path):
    stock = {
        "ticker": "NVDA",
        "company_name": "NVIDIA",
        "signal_strength_score": 0.8,
        "final_verdict": "BUY",
        "final_score": 76.0,
        "evidence_source": "transcript",
        "evidence_snippets": ["반도체"],
        "basic_state": "양호",
        "fundamentals": {
            "ticker": "NVDA", "company_name": "NVIDIA", "checked_at": None,
            "currency": "USD", "current_price": 900.0, "market_cap": None,
            "trailing_pe": None, "forward_pe": None, "price_to_book": None,
            "revenue_growth": None, "earnings_growth": None, "operating_margin": None,
            "return_on_equity": None, "debt_to_equity": None, "fifty_two_week_change": None,
            "data_source": "dummy", "notes": [],
        },
        "master_opinions": [
            {"master": "druckenmiller", "verdict": "BUY", "score": 80.0, "max_score": 100.0,
             "one_liner": "test", "rationale": [], "risks": [], "citations": []},
        ],
    }
    rows = [_make_row(stocks=[stock])]
    ranking = _make_ranking()
    validation = _make_validation(len(ranking))
    scorecard = quality_scorecard(rows, validation, ranking)
    context = _make_context(tmp_path)

    json_path, txt_path = save_channel_artifacts(
        slug="test_ch", display_name="Test", channel_url="https://test",
        rows=rows, ranking=ranking, validation=validation,
        scorecard=scorecard, context=context,
    )
    txt = txt_path.read_text(encoding="utf-8")
    assert "NVDA" in txt
    assert "druckenmiller" in txt


def test_compare_channels_empty():
    ctx = RunContext(run_id="test", today="2026-03-23", output_dir=Path("."), window_days=30)
    result = compare_channels({}, ctx)
    assert result["more_actionable_channel"] is None
    assert result["better_ranking_channel"] is None


def test_compare_channels_two_channels():
    ctx = RunContext(run_id="test", today="2026-03-23", output_dir=Path("."), window_days=30)
    ranking_a = _make_ranking()
    ranking_b = [_FakeRankedStock("AAPL", "Apple", 60.0, "2026-03-01")]
    payloads = {
        "chan_a": {
            "display_name": "Channel A",
            "rows": [_make_row(should_analyze=True), _make_row(should_analyze=True)],
            "ranking": ranking_a,
            "validation": _make_validation(len(ranking_a)),
            "scorecard": quality_scorecard(
                [_make_row(should_analyze=True)], _make_validation(len(ranking_a)), ranking_a
            ),
        },
        "chan_b": {
            "display_name": "Channel B",
            "rows": [_make_row(should_analyze=False)],
            "ranking": ranking_b,
            "validation": _make_validation(len(ranking_b)),
            "scorecard": quality_scorecard(
                [_make_row(should_analyze=False)], _make_validation(len(ranking_b)), ranking_b
            ),
        },
    }
    result = compare_channels(payloads, ctx)
    assert "channels" in result
    assert len(result["channels"]) == 2
    assert result["more_actionable_channel"] == "chan_a"
    assert result["better_ranking_channel"] is not None
    assert result["pipeline_summary"]["total_videos"] == 3
    assert result["pipeline_summary"]["skipped_videos"] == 1
    assert result["pipeline_summary"]["analyzable_videos"] == 2
    assert result["pipeline_summary"]["strict_actionable_videos"] == 3
    assert result["channels"]["chan_b"]["top_skip_reasons"][0]["reason"] == "test"
