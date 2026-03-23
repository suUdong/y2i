from omx_brainstorm.models import (
    AnalysisScore,
    FundamentalSnapshot,
    MasterOpinion,
    StockAnalysis,
    TickerMention,
    VideoSignalAssessment,
    VideoAnalysisReport,
    VideoInput,
)
from omx_brainstorm.reporting import render_markdown, render_text


def test_render_text_contains_fundamentals_and_master_lines():
    report = VideoAnalysisReport(
        run_id="run1",
        created_at="2026-03-22T00:00:00+00:00",
        provider="mock",
        mode="ralph",
        video=VideoInput(video_id="abc123def45", title="제목", url="https://youtube.com/watch?v=abc123def45"),
        signal_assessment=VideoSignalAssessment(signal_score=80, video_signal_class="ACTIONABLE", should_analyze_stocks=True, reason="충분함"),
        transcript_text="text",
        transcript_language="ko",
        ticker_mentions=[TickerMention(ticker="NVDA", company_name="NVIDIA", confidence=0.9, reason="reason")],
        stock_analyses=[
            StockAnalysis(
                ticker="NVDA",
                company_name="NVIDIA",
                extracted_from_video="제목",
                fundamentals=FundamentalSnapshot(
                    ticker="NVDA",
                    company_name="NVIDIA",
                    checked_at="2026-03-22T00:00:00+00:00",
                    currency="USD",
                    current_price=100.0,
                    data_source="dummy",
                ),
                basic_state="우수",
                basic_signal_summary="성장 양호",
                basic_signal_verdict="BUY",
                master_opinions=[MasterOpinion(master="druckenmiller", verdict="BUY", score=80, max_score=100, one_liner="좋다")],
                thesis_summary="테제",
                framework_scores=[AnalysisScore(framework="basic_fundamentals", score=30, max_score=40, verdict="PASS", summary="ok")],
                total_score=75,
                max_score=100,
                final_verdict="BUY",
                invalidation_triggers=["가이던스 하향"],
            )
        ],
    )
    text = render_text(report)
    assert "기본재무상태" in text
    assert "거장 한줄평" in text
    assert "druckenmiller: BUY" in text
    assert "checked_at: 2026-03-22T00:00:00+00:00" in text


def test_render_markdown_contains_checked_at_and_master_table():
    report = VideoAnalysisReport(
        run_id="run1",
        created_at="2026-03-22T00:00:00+00:00",
        provider="mock",
        mode="ralph",
        video=VideoInput(video_id="abc123def45", title="제목", url="https://youtube.com/watch?v=abc123def45"),
        signal_assessment=VideoSignalAssessment(signal_score=80, video_signal_class="ACTIONABLE", should_analyze_stocks=True, reason="충분함"),
        transcript_text="text",
        transcript_language="ko",
        ticker_mentions=[],
        stock_analyses=[
            StockAnalysis(
                ticker="NVDA",
                company_name="NVIDIA",
                extracted_from_video="제목",
                fundamentals=FundamentalSnapshot(
                    ticker="NVDA",
                    company_name="NVIDIA",
                    checked_at="2026-03-22T00:00:00+00:00",
                    currency="USD",
                    current_price=100.0,
                    data_source="dummy",
                ),
                basic_state="우수",
                basic_signal_summary="성장 양호",
                basic_signal_verdict="BUY",
                master_opinions=[MasterOpinion(master="druckenmiller", verdict="BUY", score=80, max_score=100, one_liner="좋다")],
                thesis_summary="테제",
                framework_scores=[AnalysisScore(framework="basic_fundamentals", score=30, max_score=40, verdict="PASS", summary="ok")],
                total_score=75,
                max_score=100,
                final_verdict="BUY",
                invalidation_triggers=["가이던스 하향"],
            )
        ],
    )
    markdown = render_markdown(report)
    assert "| checked_at | 2026-03-22T00:00:00+00:00 |" in markdown
    assert "| Master | Verdict | Score | One-liner |" in markdown
