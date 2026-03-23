from omx_brainstorm.models import VideoType
from omx_brainstorm.title_taxonomy import classify_video_type
from omx_brainstorm.signal_gate import assess_video_signal


class TestClassifyVideoType:
    def test_market_review_from_title(self):
        assert classify_video_type("마감시황 | 나스닥 급락, 코스피 영향은?") == VideoType.MARKET_REVIEW

    def test_market_review_night_recap(self):
        assert classify_video_type("밤사이 글로벌 증시 정리") == VideoType.MARKET_REVIEW

    def test_macro_from_title(self):
        assert classify_video_type("FOMC 결과 분석, 금리 동결의 의미") == VideoType.MACRO

    def test_macro_rate(self):
        assert classify_video_type("기준금리 인하 시작되나? 파월 발언 해석") == VideoType.MACRO

    def test_expert_interview(self):
        assert classify_video_type("삼성증권 이사 특별출연, 하반기 전략은?") == VideoType.EXPERT_INTERVIEW

    def test_stock_pick(self):
        assert classify_video_type("지금 사야 할 수혜주 TOP 5") == VideoType.STOCK_PICK

    def test_sector(self):
        assert classify_video_type("반도체 업황 분석, HBM 수요 전망") == VideoType.SECTOR

    def test_news_event(self):
        assert classify_video_type("[속보] 트럼프 관세 발표, 시장 충격") == VideoType.NEWS_EVENT

    def test_other_fallback(self):
        assert classify_video_type("오늘의 브이로그") == VideoType.OTHER

    def test_description_helps_classify(self):
        result = classify_video_type(
            title="오늘의 핵심 정리",
            description="금리 환율 채권 시장 동향을 분석합니다",
        )
        assert result == VideoType.MACRO

    def test_tags_contribute(self):
        result = classify_video_type(
            title="전문가와 함께하는 분석",
            description="",
            tags=["인터뷰", "대담"],
        )
        assert result == VideoType.EXPERT_INTERVIEW


class TestSignalGateVideoType:
    def test_assess_includes_video_type(self):
        result = assess_video_signal(
            title="마감시황 | 코스피 2600 돌파",
            transcript_text="오늘 코스피가 2600을 돌파했습니다.",
        )
        assert result.video_type == "MARKET_REVIEW"

    def test_assess_macro_type(self):
        result = assess_video_signal(
            title="FOMC 금리 동결, 파월 발언 핵심 정리",
            transcript_text="파월 의장은 인플레이션이 아직 목표치에 도달하지 못했다고 밝혔다.",
        )
        assert result.video_type == "MACRO"

    def test_assess_stock_pick_type(self):
        result = assess_video_signal(
            title="지금 매수해야 할 종목 3선",
            transcript_text="삼성전자와 SK하이닉스가 반도체 수혜주로 주목받고 있다.",
        )
        assert result.video_type == "STOCK_PICK"

    def test_assess_defaults_to_other(self):
        result = assess_video_signal(
            title="일상 브이로그",
            transcript_text="오늘은 카페에서 공부했습니다.",
        )
        assert result.video_type == "OTHER"
