from omx_brainstorm.comparison_rows import report_to_comparison_row
from omx_brainstorm.models import (
    FundamentalSnapshot,
    MasterOpinion,
    StockAnalysis,
    TickerMention,
    VideoAnalysisReport,
    VideoInput,
    VideoSignalAssessment,
)


def test_report_to_comparison_row_preserves_evidence_first_fields():
    report = VideoAnalysisReport(
        run_id="run1",
        created_at="2026-04-08T00:00:00+00:00",
        provider="mock",
        mode="ralph",
        video=VideoInput(
            video_id="abc123def45",
            title="엔비디아 데이터센터 투자",
            url="https://youtube.com/watch?v=abc123def45",
            published_at="20260408",
            description="엔비디아와 데이터센터 CAPEX를 다룬다.",
            tags=["엔비디아", "데이터센터"],
        ),
        signal_assessment=VideoSignalAssessment(
            signal_score=82.0,
            video_signal_class="ACTIONABLE",
            should_analyze_stocks=True,
            reason="실자막 근거 충분",
            video_type="STOCK_PICK",
            metrics={"transcript_backed": True},
        ),
        transcript_text="엔비디아가 아직 더 갈 수 있다. 데이터센터 수요가 강하다.",
        transcript_language="ko",
        ticker_mentions=[
            TickerMention(
                ticker="NVDA",
                company_name="NVIDIA",
                confidence=0.92,
                reason="직접 언급",
                evidence=["엔비디아가 아직 더 갈 수 있다"],
            )
        ],
        stock_analyses=[
            StockAnalysis(
                ticker="NVDA",
                company_name="NVIDIA",
                extracted_from_video="엔비디아 데이터센터 투자",
                fundamentals=FundamentalSnapshot(
                    ticker="NVDA",
                    company_name="NVIDIA",
                    currency="USD",
                    current_price=100.0,
                    forward_pe=25.0,
                    revenue_growth=0.25,
                    data_source="dummy",
                ),
                basic_state="우수",
                basic_signal_summary="성장 양호",
                basic_signal_verdict="BUY",
                master_opinions=[
                    MasterOpinion(master="druckenmiller", verdict="BUY", score=84, max_score=100, one_liner="좋다"),
                    MasterOpinion(master="buffett", verdict="WATCH", score=70, max_score=100, one_liner="가격은 봐야 한다"),
                ],
                thesis_summary="AI 인프라 수요 지속",
                framework_scores=[],
                total_score=77.0,
                max_score=100.0,
                final_verdict="BUY",
                evidence_bullets=["엔비디아가 아직 더 갈 수 있다", "데이터센터 수요가 강하다"],
                reasoning_strength="STRONG",
                reasoning_strength_summary="실자막, 근거, 재무가 모두 확인됨",
            )
        ],
        transcript_backed=True,
        source_quality_note="",
        video_summary="엔비디아의 데이터센터 수요를 핵심 논거로 제시한다.",
    )

    row = report_to_comparison_row(report)

    assert row["transcript_backed"] is True
    assert row["video_summary"]
    assert row["stocks"][0]["evidence_snippets"]
    assert row["stocks"][0]["reasoning_strength"] == "STRONG"
    assert row["stocks"][0]["evidence_source"] == "transcript_api"
