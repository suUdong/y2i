"""Tests for heuristic pipeline VideoType integration."""
from omx_brainstorm.heuristic_pipeline import analyze_video_heuristic
from omx_brainstorm.models import FundamentalSnapshot, TickerMention, TranscriptSegment, VideoInput
from omx_brainstorm.transcript_cache import TranscriptCache


class _DummyFetcher:
    def fetch(self, video_id, preferred_languages=None):
        text = "엔비디아가 아직 더 갈 수 있다. 반도체 투자와 메모리 수혜주를 점검한다. 삼성전자 SK하이닉스도 함께 다룬다." * 10
        return [TranscriptSegment(0, 1, text)], "ko"

    def join_segments(self, segments):
        return " ".join(s.text for s in segments)


class _DummyFundamentals:
    def fetch(self, mention):
        return FundamentalSnapshot(
            ticker=mention.ticker,
            company_name=mention.company_name or "TestCo",
            currency="USD",
            current_price=100.0,
            revenue_growth=0.2,
            operating_margin=0.15,
            return_on_equity=0.18,
            debt_to_equity=50.0,
            forward_pe=25.0,
            data_source="dummy",
        )


def test_heuristic_macro_video_includes_macro_insights(tmp_path):
    video = VideoInput(
        video_id="hm1",
        title="금리 인하와 환율 하락 전망",
        url="https://youtube.com/watch?v=hm1",
        description="금리 인하 수혜주",
        tags=["금리", "투자"],
    )
    result = analyze_video_heuristic(video, TranscriptCache(tmp_path / "cache"), _DummyFetcher(), _DummyFundamentals())
    assert result["video_type"] == "MACRO"
    assert len(result["macro_insights"]) > 0
    assert result["market_review"] is None
    assert result["expert_insights"] == []


def test_heuristic_market_review_includes_review(tmp_path):
    video = VideoInput(
        video_id="hmr1",
        title="마감시황 코스피 상승 나스닥 하락",
        url="https://youtube.com/watch?v=hmr1",
        description="오늘 시황 정리",
        tags=["시황", "코스피"],
    )
    result = analyze_video_heuristic(video, TranscriptCache(tmp_path / "cache"), _DummyFetcher(), _DummyFundamentals())
    assert result["video_type"] == "MARKET_REVIEW"
    assert result["market_review"] is not None
    assert "direction" in result["market_review"]


def test_heuristic_expert_interview_includes_insights(tmp_path):
    video = VideoInput(
        video_id="hei1",
        title="김영호 대표 인터뷰 반도체 전망",
        url="https://youtube.com/watch?v=hei1",
        description="김영호 삼성증권 대표와의 인터뷰",
        tags=["인터뷰", "반도체"],
    )
    result = analyze_video_heuristic(video, TranscriptCache(tmp_path / "cache"), _DummyFetcher(), _DummyFundamentals())
    assert result["video_type"] == "EXPERT_INTERVIEW"
    assert len(result["expert_insights"]) > 0
    assert result["expert_insights"][0]["expert_name"] == "김영호"


def test_heuristic_stock_pick_no_extra_fields(tmp_path):
    video = VideoInput(
        video_id="hsp1",
        title="반도체 수혜주 종목 분석",
        url="https://youtube.com/watch?v=hsp1",
        description="종목 분석",
        tags=["종목", "반도체"],
    )
    result = analyze_video_heuristic(video, TranscriptCache(tmp_path / "cache"), _DummyFetcher(), _DummyFundamentals())
    assert result["video_type"] == "STOCK_PICK"
    assert result["macro_insights"] == []
    assert result["market_review"] is None
    assert result["expert_insights"] == []


def test_heuristic_output_includes_video_type_field(tmp_path):
    video = VideoInput(
        video_id="hvt1",
        title="트럼프 관세 긴급 속보",
        url="https://youtube.com/watch?v=hvt1",
        description="속보",
        tags=["속보"],
    )
    result = analyze_video_heuristic(video, TranscriptCache(tmp_path / "cache"), _DummyFetcher(), _DummyFundamentals())
    assert "video_type" in result
    assert result["video_type"] == "NEWS_EVENT"
