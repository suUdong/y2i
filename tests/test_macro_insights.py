from omx_brainstorm.macro_signals import extract_macro_insights


class TestExtractMacroInsights:
    def test_rate_cut_direction_down(self):
        insights = extract_macro_insights(
            "금리 인하 시작되나",
            "기준금리 인하가 예상되면서 성장주에 대한 기대감이 커지고 있다.",
        )
        rate = next(i for i in insights if i.indicator == "interest_rate")
        assert rate.direction == "DOWN"
        assert rate.sentiment == "BULLISH"
        assert "growth_tech" in rate.beneficiary_sectors
        assert rate.confidence >= 0.65

    def test_rate_hike_direction_up(self):
        insights = extract_macro_insights(
            "금리 인상 지속",
            "고금리 환경이 longer for higher 기조로 이어질 전망이다.",
        )
        rate = next(i for i in insights if i.indicator == "interest_rate")
        assert rate.direction == "UP"
        assert rate.sentiment == "BEARISH"
        assert "banks" in rate.beneficiary_sectors

    def test_fx_strong_dollar(self):
        insights = extract_macro_insights(
            "달러 강세 언제까지",
            "원달러 상승 추세가 지속되며 수출주에 유리한 환경이다.",
        )
        fx = next(i for i in insights if i.indicator == "fx")
        assert fx.direction == "UP"
        assert "exporters" in fx.beneficiary_sectors

    def test_oil_price_decline(self):
        insights = extract_macro_insights(
            "유가 하락 전망",
            "국제유가 하락으로 항공주와 화학주에 긍정적 영향이 예상된다.",
        )
        oil = next(i for i in insights if i.indicator == "oil")
        assert oil.direction == "DOWN"
        assert "airlines" in oil.beneficiary_sectors

    def test_fomc_dovish(self):
        insights = extract_macro_insights(
            "FOMC 결과, 파월 비둘기파 전환",
            "파월 의장이 피봇 가능성을 시사하며 시장이 반등했다.",
        )
        fomc = next(i for i in insights if i.indicator == "fomc")
        assert fomc.direction == "DOWN"
        assert fomc.sentiment == "BULLISH"

    def test_cpi_inflation(self):
        insights = extract_macro_insights(
            "CPI 서프라이즈, 인플레이션 다시 고개",
            "물가 상승 압력이 재점화되면서 긴축 우려가 커졌다.",
        )
        cpi = next(i for i in insights if i.indicator == "cpi")
        assert cpi.direction == "UP"
        assert cpi.sentiment == "BEARISH"

    def test_employment_weakness(self):
        insights = extract_macro_insights(
            "고용지표 쇼크",
            "실업률 상승과 고용 둔화로 경기 침체 우려가 확산되고 있다.",
        )
        emp = next(i for i in insights if i.indicator == "employment")
        assert emp.direction == "DOWN"
        assert emp.sentiment == "BEARISH"

    def test_sector_rotation(self):
        insights = extract_macro_insights(
            "섹터 로테이션 시작",
            "자금 이동과 순환매가 진행 중이다.",
        )
        rot = next(i for i in insights if i.indicator == "sector_rotation")
        assert rot.direction == "UP"

    def test_neutral_when_only_generic_mention(self):
        insights = extract_macro_insights(
            "오늘의 환율 동향",
            "환율 안정 속에 시장은 관망세를 보였다.",
        )
        fx = next(i for i in insights if i.indicator == "fx")
        assert fx.direction == "NEUTRAL"

    def test_no_insights_for_unrelated(self):
        insights = extract_macro_insights(
            "오늘의 점심 추천",
            "맛있는 파스타를 먹었습니다.",
        )
        assert len(insights) == 0

    def test_multiple_indicators_extracted(self):
        insights = extract_macro_insights(
            "금리 인하와 유가 하락이 동시에",
            "금리 인하 기대감과 국제유가 하락이 동시에 나타나고 있다.",
        )
        indicators = {i.indicator for i in insights}
        assert "interest_rate" in indicators
        assert "oil" in indicators

    def test_confidence_increases_with_more_keywords(self):
        single = extract_macro_insights("금리 인하", "")
        multi = extract_macro_insights(
            "금리 인하 기대",
            "기준금리 인하와 rate cut 기대감이 커지고 있다. 금리 하락 전망이 우세하다.",
        )
        rate_single = next(i for i in single if i.indicator == "interest_rate")
        rate_multi = next(i for i in multi if i.indicator == "interest_rate")
        assert rate_multi.confidence > rate_single.confidence
