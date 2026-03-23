"""Tests for the combined stock + macro signal dashboard report."""
from omx_brainstorm.models import (
    ExpertInsight,
    FundamentalSnapshot,
    MacroInsight,
    MarketReviewSummary,
    MasterOpinion,
    StockAnalysis,
    TickerMention,
    VideoAnalysisReport,
    VideoInput,
    VideoSignalAssessment,
)
from omx_brainstorm.reporting import render_combined_dashboard


def _make_report(
    title="테스트 영상",
    video_type="STOCK_PICK",
    signal_class="ACTIONABLE",
    stock_analyses=None,
    macro_insights=None,
    market_review=None,
    expert_insights=None,
):
    video = VideoInput(video_id="t1", title=title, url="https://youtube.com/watch?v=t1")
    signal = VideoSignalAssessment(
        signal_score=75.0,
        video_signal_class=signal_class,
        should_analyze_stocks=True,
        reason="test",
        video_type=video_type,
    )
    return VideoAnalysisReport(
        run_id="r1",
        created_at="2026-03-23T00:00:00Z",
        provider="mock",
        mode="ralph",
        video=video,
        signal_assessment=signal,
        transcript_text="test transcript",
        transcript_language="ko",
        ticker_mentions=[],
        stock_analyses=stock_analyses or [],
        macro_insights=macro_insights or [],
        market_review=market_review,
        expert_insights=expert_insights or [],
    )


def _make_stock(ticker="NVDA", verdict="BUY", score=76.0):
    return StockAnalysis(
        ticker=ticker,
        company_name="NVIDIA" if ticker == "NVDA" else ticker,
        extracted_from_video="test",
        fundamentals=FundamentalSnapshot(ticker=ticker, data_source="dummy"),
        basic_state="양호",
        basic_signal_summary="test",
        basic_signal_verdict="BUY",
        master_opinions=[
            MasterOpinion(master="druckenmiller", verdict="BUY", score=80, max_score=100, one_liner="test druckenmiller"),
            MasterOpinion(master="buffett", verdict="WATCH", score=65, max_score=100, one_liner="test buffett"),
            MasterOpinion(master="soros", verdict="BUY", score=75, max_score=100, one_liner="test soros"),
        ],
        thesis_summary="테스트 테제",
        framework_scores=[],
        total_score=score,
        max_score=100.0,
        final_verdict=verdict,
        invalidation_triggers=["가이던스 하향"],
    )


def _make_macro(indicator="interest_rate", direction="DOWN", label="금리", confidence=0.7):
    return MacroInsight(
        indicator=indicator,
        direction=direction,
        label=label,
        confidence=confidence,
        matched_keywords=["금리 인하"],
        sentiment="BULLISH",
        beneficiary_sectors=["growth_tech", "real_estate"],
    )


def test_dashboard_renders_stock_section():
    report = _make_report(stock_analyses=[_make_stock()])
    md = render_combined_dashboard([report])
    assert "# OMX 통합 대시보드" in md
    assert "NVDA" in md
    assert "종목 분석 요약" in md
    assert "druckenmiller" in md


def test_dashboard_renders_macro_section():
    report = _make_report(
        video_type="MACRO",
        macro_insights=[_make_macro()],
    )
    md = render_combined_dashboard([report])
    assert "매크로 시그널 요약" in md
    assert "금리" in md
    assert "BULLISH" in md


def test_dashboard_renders_market_review():
    review = MarketReviewSummary(
        indices=[{"name": "코스피", "direction": "UP", "detail": "2600 돌파"}],
        direction="BULLISH",
        risk_events=["관세"],
        sector_focus=["반도체"],
    )
    report = _make_report(video_type="MARKET_REVIEW", market_review=review)
    md = render_combined_dashboard([report])
    assert "시장 리뷰" in md
    assert "코스피" in md
    assert "BULLISH" in md


def test_dashboard_renders_expert_insights():
    expert = ExpertInsight(
        expert_name="김영호",
        affiliation="삼성증권",
        key_claims=["반도체 상승 전망", "메모리 수요 증가"],
        topic="반도체",
        sentiment="BULLISH",
        mentioned_tickers=["005930.KS"],
    )
    report = _make_report(video_type="EXPERT_INTERVIEW", expert_insights=[expert])
    md = render_combined_dashboard([report])
    assert "전문가 인사이트" in md
    assert "김영호" in md
    assert "삼성증권" in md
    assert "반도체 상승 전망" in md


def test_dashboard_combines_multiple_reports():
    r1 = _make_report(
        title="종목 분석",
        video_type="STOCK_PICK",
        stock_analyses=[_make_stock("NVDA", "BUY", 80)],
    )
    r2 = _make_report(
        title="매크로 분석",
        video_type="MACRO",
        macro_insights=[_make_macro()],
        stock_analyses=[_make_stock("005930.KS", "WATCH", 60)],
    )
    r3 = _make_report(
        title="전문가 인터뷰",
        video_type="EXPERT_INTERVIEW",
        expert_insights=[ExpertInsight(
            expert_name="이형수",
            affiliation="",
            key_claims=["AI 투자 확대"],
            topic="AI/데이터센터",
            sentiment="BULLISH",
        )],
    )
    md = render_combined_dashboard([r1, r2, r3])
    # All sections present
    assert "매크로 시그널 요약" in md
    assert "종목 분석 요약" in md
    assert "전문가 인사이트" in md
    assert "분석 영상 목록" in md
    # Both stocks
    assert "NVDA" in md
    assert "005930.KS" in md
    # Expert
    assert "이형수" in md
    # Video source list
    assert "종목 분석" in md
    assert "매크로 분석" in md


def test_dashboard_deduplicates_macro_insights():
    m1 = _make_macro("interest_rate", "DOWN", "금리", 0.65)
    m2 = _make_macro("interest_rate", "DOWN", "금리", 0.80)
    r1 = _make_report(macro_insights=[m1])
    r2 = _make_report(macro_insights=[m2])
    md = render_combined_dashboard([r1, r2])
    # Should only show one interest_rate row, with the higher confidence
    assert md.count("interest_rate") == 0  # We use label "금리", not indicator
    lines = [l for l in md.split("\n") if "금리" in l and "|" in l]
    assert len(lines) == 1
    assert "0.80" in lines[0]


def test_dashboard_deduplicates_stocks_by_ticker():
    s1 = _make_stock("NVDA", "BUY", 80)
    s2 = _make_stock("NVDA", "WATCH", 70)
    r1 = _make_report(stock_analyses=[s1])
    r2 = _make_report(stock_analyses=[s2])
    md = render_combined_dashboard([r1, r2])
    # Summary table should have NVDA only once (the higher-scored one)
    summary_section = md.split("## 종목 분석 요약")[1].split("##")[0]
    assert summary_section.count("NVDA") == 1


def test_dashboard_empty_reports():
    md = render_combined_dashboard([])
    assert "# OMX 통합 대시보드" in md
    assert "분석 영상 목록" in md


def test_dashboard_video_source_list():
    r1 = _make_report(title="영상A", video_type="STOCK_PICK")
    r2 = _make_report(title="영상B", video_type="MACRO")
    md = render_combined_dashboard([r1, r2])
    assert "영상A" in md
    assert "영상B" in md
    assert "STOCK_PICK" in md
    assert "MACRO" in md
