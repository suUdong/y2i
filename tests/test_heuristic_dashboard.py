"""Tests for heuristic-to-dashboard adapter."""
from omx_brainstorm.heuristic_pipeline import heuristic_rows_to_reports, render_heuristic_dashboard
from omx_brainstorm.models import VideoAnalysisReport


def _make_heuristic_row(
    video_id="h1",
    title="금리 인하 전망",
    video_type="MACRO",
    should_analyze=True,
    stocks=None,
    macro_insights=None,
    market_review=None,
    expert_insights=None,
):
    return {
        "video_id": video_id,
        "title": title,
        "url": f"https://youtube.com/watch?v={video_id}",
        "published_at": "20260320",
        "description": "test",
        "tags": ["test"],
        "video_type": video_type,
        "signal_score": 75.0,
        "video_signal_class": "ACTIONABLE",
        "should_analyze_stocks": should_analyze,
        "reason": "test reason",
        "signal_metrics": {},
        "transcript_language": "ko",
        "macro_insights": macro_insights or [],
        "market_review": market_review,
        "expert_insights": expert_insights or [],
        "stocks": stocks or [],
    }


def _make_stock_dict():
    return {
        "ticker": "NVDA",
        "company_name": "NVIDIA",
        "mention_count": 3,
        "signal_timestamp": "20260320",
        "signal_strength_score": 0.8,
        "evidence_source": "transcript",
        "evidence_snippets": ["반도체"],
        "basic_state": "양호",
        "basic_signal_summary": "매출 성장 양호",
        "basic_signal_verdict": "BUY",
        "fundamentals": {
            "ticker": "NVDA", "company_name": "NVIDIA", "checked_at": None,
            "currency": "USD", "current_price": 900.0, "market_cap": None,
            "trailing_pe": None, "forward_pe": None, "price_to_book": None,
            "revenue_growth": 0.5, "earnings_growth": None, "operating_margin": 0.3,
            "return_on_equity": 0.8, "debt_to_equity": 40.0, "fifty_two_week_change": None,
            "data_source": "dummy", "notes": [],
        },
        "master_opinions": [
            {"master": "druckenmiller", "verdict": "BUY", "score": 80.0, "max_score": 100.0,
             "one_liner": "test", "rationale": ["r"], "risks": ["r"], "citations": ["c"]},
        ],
        "final_score": 76.0,
        "final_verdict": "BUY",
        "invalidation_triggers": ["가이던스 하향"],
    }


def _make_macro_dict():
    return {
        "indicator": "interest_rate",
        "direction": "DOWN",
        "label": "금리",
        "confidence": 0.7,
        "matched_keywords": ["금리 인하"],
        "sentiment": "BULLISH",
        "beneficiary_sectors": ["growth_tech"],
    }


def _make_expert_dict():
    return {
        "expert_name": "김영호",
        "affiliation": "삼성증권",
        "key_claims": ["반도체 상승 전망"],
        "topic": "반도체",
        "sentiment": "BULLISH",
        "mentioned_tickers": ["005930.KS"],
    }


# --- Conversion tests ---

def test_empty_rows():
    reports = heuristic_rows_to_reports([])
    assert reports == []


def test_basic_conversion():
    rows = [_make_heuristic_row()]
    reports = heuristic_rows_to_reports(rows)
    assert len(reports) == 1
    assert isinstance(reports[0], VideoAnalysisReport)
    assert reports[0].video.video_id == "h1"
    assert reports[0].signal_assessment.video_type == "MACRO"


def test_conversion_with_stocks():
    rows = [_make_heuristic_row(stocks=[_make_stock_dict()])]
    reports = heuristic_rows_to_reports(rows)
    assert len(reports[0].stock_analyses) == 1
    assert reports[0].stock_analyses[0].ticker == "NVDA"
    assert reports[0].stock_analyses[0].master_opinions[0].master == "druckenmiller"
    assert len(reports[0].ticker_mentions) == 1


def test_conversion_with_macro():
    rows = [_make_heuristic_row(macro_insights=[_make_macro_dict()])]
    reports = heuristic_rows_to_reports(rows)
    assert len(reports[0].macro_insights) == 1
    assert reports[0].macro_insights[0].indicator == "interest_rate"


def test_conversion_with_expert():
    rows = [_make_heuristic_row(expert_insights=[_make_expert_dict()])]
    reports = heuristic_rows_to_reports(rows)
    assert len(reports[0].expert_insights) == 1
    assert reports[0].expert_insights[0].expert_name == "김영호"


def test_conversion_with_market_review():
    mr = {
        "indices": [{"name": "코스피", "direction": "UP", "detail": "2600"}],
        "direction": "BULLISH",
        "risk_events": ["관세"],
        "sector_focus": ["반도체"],
        "key_points": ["코스피 상승"],
        "macro_insights": [_make_macro_dict()],
    }
    rows = [_make_heuristic_row(video_type="MARKET_REVIEW", market_review=mr)]
    reports = heuristic_rows_to_reports(rows)
    assert reports[0].market_review is not None
    assert reports[0].market_review.direction == "BULLISH"
    assert len(reports[0].market_review.macro_insights) == 1


def test_conversion_multiple_rows():
    rows = [
        _make_heuristic_row(video_id="v1", video_type="STOCK_PICK", stocks=[_make_stock_dict()]),
        _make_heuristic_row(video_id="v2", video_type="MACRO", macro_insights=[_make_macro_dict()]),
        _make_heuristic_row(video_id="v3", video_type="EXPERT_INTERVIEW", expert_insights=[_make_expert_dict()]),
    ]
    reports = heuristic_rows_to_reports(rows)
    assert len(reports) == 3


# --- Dashboard rendering ---

def test_render_heuristic_dashboard(tmp_path):
    rows = [
        _make_heuristic_row(video_id="d1", stocks=[_make_stock_dict()]),
        _make_heuristic_row(video_id="d2", macro_insights=[_make_macro_dict()]),
    ]
    path = render_heuristic_dashboard(rows, tmp_path)
    assert path is not None
    assert path.exists()
    content = path.read_text(encoding="utf-8")
    assert "OMX 통합 대시보드" in content
    assert "NVDA" in content
    assert "금리" in content


def test_render_heuristic_dashboard_empty(tmp_path):
    result = render_heuristic_dashboard([], tmp_path)
    assert result is None
