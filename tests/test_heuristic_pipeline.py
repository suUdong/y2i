"""Tests for heuristic pipeline VideoType integration."""
from omx_brainstorm.heuristic_pipeline import (
    analyze_video_heuristic,
    basic_assessment,
    extract_mentions,
    final_verdict,
)
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


# --- extract_mentions ---

def test_extract_mentions_no_matches():
    mentions = extract_mentions("random title", "no company names here at all")
    assert mentions == []


def test_extract_mentions_skips_low_confidence_indirect(monkeypatch):
    """Line 39: indirect mentions with confidence < 0.55 should be skipped."""
    from omx_brainstorm.models import TickerMention as TM
    low_conf = TM(ticker="FAKE", company_name="FakeCo", confidence=0.3, reason="low")
    monkeypatch.setattr(
        "omx_brainstorm.heuristic_pipeline.indirect_macro_mentions",
        lambda title, text: [low_conf],
    )
    mentions = extract_mentions("random title", "no direct match")
    tickers = [m.ticker for m, _count in mentions]
    assert "FAKE" not in tickers


def test_extract_mentions_finds_companies():
    mentions = extract_mentions("엔비디아 분석", "엔비디아 실적이 좋다 엔비디아 전망")
    assert len(mentions) >= 1
    tickers = [m.ticker for m, _count in mentions]
    assert "NVDA" in tickers


# --- basic_assessment verdicts ---

def test_basic_assessment_buy_verdict():
    snapshot = FundamentalSnapshot(
        ticker="TEST",
        revenue_growth=0.25,
        operating_margin=0.25,
        return_on_equity=0.20,
        debt_to_equity=40.0,
        forward_pe=20.0,
    )
    score, verdict, state, summary = basic_assessment(snapshot)
    assert verdict == "BUY"
    assert score >= 72


def test_basic_assessment_watch_verdict():
    snapshot = FundamentalSnapshot(
        ticker="TEST",
        revenue_growth=0.08,
        operating_margin=0.12,
        return_on_equity=0.10,
        debt_to_equity=100.0,
        forward_pe=30.0,
    )
    score, verdict, state, summary = basic_assessment(snapshot)
    assert verdict == "WATCH"
    assert 58 <= score < 72


def test_basic_assessment_reject_verdict():
    snapshot = FundamentalSnapshot(
        ticker="TEST",
        revenue_growth=-0.10,
        operating_margin=-0.05,
        return_on_equity=0.03,
        debt_to_equity=200.0,
        forward_pe=50.0,
    )
    score, verdict, state, summary = basic_assessment(snapshot)
    assert verdict == "REJECT"
    assert score < 58


def test_basic_assessment_empty_snapshot():
    snapshot = FundamentalSnapshot(ticker="TEST")
    score, verdict, state, summary = basic_assessment(snapshot)
    assert verdict == "REJECT"
    assert score == 50.0


# --- final_verdict ---

def test_final_verdict_strong_buy():
    total, verdict = final_verdict([90.0, 85.0])
    assert verdict == "STRONG_BUY"
    assert total >= 80


def test_final_verdict_buy():
    total, verdict = final_verdict([70.0, 72.0])
    assert verdict == "BUY"
    assert 68 <= total < 80


def test_final_verdict_watch():
    total, verdict = final_verdict([58.0, 60.0])
    assert verdict == "WATCH"
    assert 55 <= total < 68


def test_final_verdict_reject():
    total, verdict = final_verdict([40.0, 45.0])
    assert verdict == "REJECT"
    assert total < 55


# --- early return when should_analyze_stocks is False ---

def test_heuristic_no_stock_analysis_when_low_signal(tmp_path):
    """Videos classified as NOISE should not analyze stocks."""
    video = VideoInput(
        video_id="hns1",
        title="구독자 이벤트 공지",
        url="https://youtube.com/watch?v=hns1",
        description="이벤트 공지입니다",
        tags=["공지"],
    )

    class _NoStockFetcher:
        def fetch(self, video_id, preferred_languages=None):
            return [TranscriptSegment(0, 1, "구독 좋아요 눌러주세요 이벤트 안내")], "ko"
        def join_segments(self, segments):
            return " ".join(s.text for s in segments)

    result = analyze_video_heuristic(video, TranscriptCache(tmp_path / "cache"), _NoStockFetcher(), _DummyFundamentals())
    assert result["stocks"] == []
    assert result["skip_reason"] == result["reason"]
