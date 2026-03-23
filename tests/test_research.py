"""Tests for research.py: cross-video ranking, timestamp helpers, signal strength, text rendering."""
from omx_brainstorm.research import (
    RankedStock,
    _is_earlier_timestamp,
    _is_newer_timestamp,
    _normalize_signal_date,
    _signal_strength,
    aggregate_verdict,
    build_cross_video_ranking,
    render_cross_video_ranking_text,
)


# --- timestamp helpers ---

def test_normalize_signal_date_none():
    assert _normalize_signal_date(None) is None
    assert _normalize_signal_date("") is None


def test_normalize_signal_date_8digit():
    assert _normalize_signal_date("20260301") == "2026-03-01"


def test_normalize_signal_date_iso():
    assert _normalize_signal_date("2026-03-01T12:00:00Z") == "2026-03-01"


def test_is_newer_timestamp_none_candidate():
    assert _is_newer_timestamp(None, "2026-03-01") is False


def test_is_newer_timestamp_none_current():
    assert _is_newer_timestamp("2026-03-01", None) is True


def test_is_newer_timestamp_comparison():
    assert _is_newer_timestamp("2026-03-05", "2026-03-01") is True
    assert _is_newer_timestamp("2026-03-01", "2026-03-05") is False


def test_is_earlier_timestamp_none_candidate():
    assert _is_earlier_timestamp(None, "2026-03-01") is False


def test_is_earlier_timestamp_none_current():
    assert _is_earlier_timestamp("2026-03-01", None) is True


def test_is_earlier_timestamp_comparison():
    assert _is_earlier_timestamp("2026-03-01", "2026-03-05") is True
    assert _is_earlier_timestamp("2026-03-05", "2026-03-01") is False


# --- signal strength ---

def test_signal_strength_with_explicit_score():
    stock = {"signal_strength_score": 85.0}
    video = {"signal_score": 50.0}
    assert _signal_strength(video, stock) == 85.0


def test_signal_strength_computed():
    stock = {"mention_count": 3, "master_opinions": []}
    video = {"signal_score": 70.0}
    result = _signal_strength(video, stock)
    # 70*0.7 + min(3*3, 18) + 0*0.8 = 49 + 9 + 0 = 58
    assert result == 58.0


# --- aggregate_verdict ---

def test_aggregate_verdict_all_levels():
    assert aggregate_verdict(85) == "STRONG_BUY"
    assert aggregate_verdict(80) == "STRONG_BUY"
    assert aggregate_verdict(72) == "BUY"
    assert aggregate_verdict(68) == "BUY"
    assert aggregate_verdict(60) == "WATCH"
    assert aggregate_verdict(55) == "WATCH"
    assert aggregate_verdict(40) == "REJECT"


# --- RankedStock.to_dict ---

def test_ranked_stock_to_dict():
    rs = RankedStock(
        ticker="NVDA", company_name="NVIDIA", aggregate_score=80.0,
        aggregate_verdict="STRONG_BUY", appearances=2, total_mentions=5,
        average_signal_strength=75.0, differentiation_score=0.5,
        average_final_score=78.0, best_final_score=82.0,
        first_signal_at="2026-03-01", last_signal_at="2026-03-10",
        latest_checked_at="2026-03-10", latest_price=900.0, currency="USD",
    )
    d = rs.to_dict()
    assert d["ticker"] == "NVDA"
    assert d["aggregate_score"] == 80.0


# --- build_cross_video_ranking ---

def _make_video(video_id="v1", title="Test", signal_score=70, should_analyze=True, stocks=None):
    return {
        "video_id": video_id,
        "title": title,
        "signal_score": signal_score,
        "should_analyze_stocks": should_analyze,
        "published_at": "2026-03-01",
        "stocks": stocks or [],
    }


def _make_stock(ticker="NVDA", mention_count=3, final_score=75.0, signal_strength=None):
    stock = {
        "ticker": ticker,
        "company_name": f"{ticker} Corp",
        "mention_count": mention_count,
        "final_score": final_score,
        "fundamentals": {"currency": "USD", "current_price": 900.0, "checked_at": "2026-03-10"},
        "master_opinions": [],
    }
    if signal_strength is not None:
        stock["signal_strength_score"] = signal_strength
    return stock


def test_build_cross_video_ranking_empty():
    assert build_cross_video_ranking([]) == []


def test_build_cross_video_ranking_skips_non_actionable():
    video = _make_video(should_analyze=False, stocks=[_make_stock()])
    assert build_cross_video_ranking([video]) == []


def test_build_cross_video_ranking_single_stock():
    video = _make_video(stocks=[_make_stock("NVDA", 3, 75.0, signal_strength=80.0)])
    ranking = build_cross_video_ranking([video])
    assert len(ranking) == 1
    assert ranking[0].ticker == "NVDA"
    assert ranking[0].appearances == 1
    assert ranking[0].total_mentions == 3


def test_build_cross_video_ranking_multiple_videos_same_stock():
    v1 = _make_video("v1", "Video 1", stocks=[_make_stock("NVDA", 3, 75.0, 80.0)])
    v2 = _make_video("v2", "Video 2", stocks=[_make_stock("NVDA", 2, 80.0, 85.0)])
    ranking = build_cross_video_ranking([v1, v2])
    assert len(ranking) == 1
    assert ranking[0].appearances == 2
    assert ranking[0].total_mentions == 5
    assert ranking[0].best_final_score == 80.0


def test_build_cross_video_ranking_sorts_by_score():
    v = _make_video(stocks=[
        _make_stock("AAPL", 1, 60.0, 50.0),
        _make_stock("NVDA", 5, 85.0, 90.0),
    ])
    ranking = build_cross_video_ranking([v])
    assert ranking[0].ticker == "NVDA"
    assert ranking[1].ticker == "AAPL"


def test_build_cross_video_ranking_timestamp_tracking():
    v1 = _make_video("v1", "Video 1", stocks=[_make_stock("NVDA")])
    v1["published_at"] = "2026-03-01"
    v2 = _make_video("v2", "Video 2", stocks=[_make_stock("NVDA")])
    v2["published_at"] = "2026-03-10"
    ranking = build_cross_video_ranking([v1, v2])
    assert ranking[0].first_signal_at == "2026-03-01"
    assert ranking[0].last_signal_at == "2026-03-10"


# --- render_cross_video_ranking_text ---

def test_render_cross_video_ranking_text_empty():
    text = render_cross_video_ranking_text([])
    assert "집계 대상 종목 없음" in text


def test_render_cross_video_ranking_text_with_items():
    rs = RankedStock(
        ticker="NVDA", company_name="NVIDIA", aggregate_score=80.0,
        aggregate_verdict="STRONG_BUY", appearances=2, total_mentions=5,
        average_signal_strength=75.0, differentiation_score=0.5,
        average_final_score=78.0, best_final_score=82.0,
        first_signal_at="2026-03-01", last_signal_at="2026-03-10",
        latest_checked_at="2026-03-10", latest_price=900.0, currency="USD",
    )
    text = render_cross_video_ranking_text([rs])
    assert "1. NVDA" in text
    assert "NVIDIA" in text
    assert "STRONG_BUY" in text
    assert "aggregate=80.0" in text
