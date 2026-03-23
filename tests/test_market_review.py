from omx_brainstorm.market_review import extract_market_review, render_market_review_md


class TestExtractMarketReview:
    def test_extracts_kospi_index(self):
        summary = extract_market_review(
            "마감시황 | 코스피 2600 돌파",
            "코스피가 2600을 돌파하며 상승 마감했다.",
        )
        names = [idx["name"] for idx in summary.indices]
        assert "코스피" in names
        kospi = next(idx for idx in summary.indices if idx["name"] == "코스피")
        assert kospi["direction"] == "UP"

    def test_extracts_nasdaq_decline(self):
        summary = extract_market_review(
            "밤시황 | 나스닥 급락",
            "나스닥이 급락하며 기술주 전반이 약세를 보였다.",
        )
        nasdaq = next(idx for idx in summary.indices if idx["name"] == "나스닥")
        assert nasdaq["direction"] == "DOWN"

    def test_extracts_multiple_indices(self):
        summary = extract_market_review(
            "마감시황 정리",
            "코스피 상승, 코스닥 하락, 나스닥 보합으로 마감.",
        )
        names = {idx["name"] for idx in summary.indices}
        assert "코스피" in names
        assert "코스닥" in names
        assert "나스닥" in names

    def test_overall_direction_bullish(self):
        summary = extract_market_review(
            "시장 강세 랠리",
            "상승 랠리가 이어지며 급등 종목이 쏟아졌다. 호재가 겹쳤다.",
        )
        assert summary.direction == "BULLISH"

    def test_overall_direction_bearish(self):
        summary = extract_market_review(
            "폭락장 분석",
            "급락과 약세가 지속되며 하락 폭이 커졌다. 악재가 쏟아졌다.",
        )
        assert summary.direction == "BEARISH"

    def test_risk_events_detected(self):
        summary = extract_market_review(
            "관세 전쟁 충격",
            "트럼프 관세 발표로 무역전쟁 우려가 확산되며 시장이 흔들렸다.",
        )
        assert "관세" in summary.risk_events
        assert "트럼프" in summary.risk_events

    def test_sector_focus_extracted(self):
        summary = extract_market_review(
            "반도체와 방산주 강세",
            "반도체 업종이 강세를 보이고 방산주도 동반 상승했다. AI 데이터센터 투자 확대.",
        )
        assert "반도체" in summary.sector_focus
        assert "방산/국방" in summary.sector_focus
        assert "AI/데이터센터" in summary.sector_focus

    def test_key_points_extracted(self):
        summary = extract_market_review(
            "마감시황",
            "코스피가 2600을 돌파했다. 나스닥 선물은 하락 중이다. 오늘 점심은 맛있었다.",
        )
        assert len(summary.key_points) >= 1
        assert any("코스피" in p for p in summary.key_points)

    def test_macro_insights_included(self):
        summary = extract_market_review(
            "마감시황 | 금리 인하 기대",
            "금리 인하 기대감에 성장주가 반등했다.",
        )
        assert len(summary.macro_insights) >= 1
        rate = next((i for i in summary.macro_insights if i.indicator == "interest_rate"), None)
        assert rate is not None
        assert rate.direction == "DOWN"

    def test_empty_content_returns_empty_summary(self):
        summary = extract_market_review("", "")
        assert summary.indices == []
        assert summary.direction == "NEUTRAL"
        assert summary.risk_events == []


class TestRenderMarketReviewMd:
    def test_renders_markdown_with_indices(self):
        summary = extract_market_review(
            "마감시황 | 코스피 2600 돌파, 반도체 강세",
            "코스피가 2600을 돌파했다. 반도체 업종이 주도했다.",
        )
        md = render_market_review_md(summary)
        assert "# 시장리뷰 요약" in md
        assert "코스피" in md
        assert "반도체" in md

    def test_renders_risk_events(self):
        summary = extract_market_review(
            "전쟁 리스크 분석",
            "전쟁 우려로 시장 변동성이 커졌다.",
        )
        md = render_market_review_md(summary)
        assert "리스크 이벤트" in md
        assert "전쟁" in md

    def test_renders_macro_insights_table(self):
        summary = extract_market_review(
            "금리 인하 기대",
            "금리 인하 전망에 성장주가 반등했다.",
        )
        md = render_market_review_md(summary)
        assert "매크로 인사이트" in md
        assert "금리" in md

    def test_empty_summary_still_renders(self):
        summary = extract_market_review("오늘의 일상", "카페에서 공부했다.")
        md = render_market_review_md(summary)
        assert "# 시장리뷰 요약" in md
        assert "NEUTRAL" in md
