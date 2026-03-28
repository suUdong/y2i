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


class _MacroNewsOnlyFetcher:
    def fetch(self, video_id, preferred_languages=None):
        text = (
            "트럼프 발표와 중동 전쟁, 환율, 유가, 금리, 증시 충격 가능성을 빠르게 정리한다. "
            "거시 뉴스 흐름만 설명하고 특정 기업이나 개별 투자 전략은 다루지 않는다."
        ) * 8
        return [TranscriptSegment(0, 1, text)], "ko"

    def join_segments(self, segments):
        return " ".join(s.text for s in segments)


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


def test_heuristic_market_review_failure_returns_empty_fields(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "omx_brainstorm.heuristic_pipeline.extract_market_review",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    video = VideoInput(
        video_id="hmr-fail",
        title="마감시황 코스피 상승 나스닥 하락",
        url="https://youtube.com/watch?v=hmr-fail",
        description="오늘 시황 정리",
        tags=["시황", "코스피"],
    )
    result = analyze_video_heuristic(video, TranscriptCache(tmp_path / "cache"), _DummyFetcher(), _DummyFundamentals())
    assert result["video_type"] == "MARKET_REVIEW"
    assert result["market_review"] is None
    assert result["macro_insights"] == []


def test_heuristic_blocks_macro_news_without_company_or_sector_path(tmp_path):
    video = VideoInput(
        video_id="hmacro-news",
        title="트럼프 발표 15분전 누군가가 20조 배팅을 걸었다",
        url="https://youtube.com/watch?v=hmacro-news",
        description="전쟁과 거시 뉴스 브리핑",
        tags=["트럼프", "전쟁", "속보"],
    )
    result = analyze_video_heuristic(
        video,
        TranscriptCache(tmp_path / "cache"),
        _MacroNewsOnlyFetcher(),
        _DummyFundamentals(),
    )

    assert result["video_signal_class"] == "LOW_SIGNAL"
    assert result["should_analyze_stocks"] is False
    assert result["stocks"] == []
    assert result["signal_metrics"]["macro_stock_candidates"] >= 2
    assert result["signal_metrics"]["has_explicit_sector_path"] is False


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


def test_heuristic_falls_back_to_individual_fundamental_fetches_when_batch_fails(tmp_path):
    class BatchFailFundamentals(_DummyFundamentals):
        def fetch_many(self, mentions, max_workers=None):
            raise RuntimeError("batch boom")

    video = VideoInput(
        video_id="hfund1",
        title="엔비디아와 삼성전자 반도체 분석",
        url="https://youtube.com/watch?v=hfund1",
        description="반도체 종목 분석",
        tags=["엔비디아", "삼성전자"],
    )
    result = analyze_video_heuristic(video, TranscriptCache(tmp_path / "cache"), _DummyFetcher(), BatchFailFundamentals())
    assert result["stocks"]


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


def test_heuristic_skips_stock_analysis_for_non_stock_video_with_single_company_mention(tmp_path):
    class _SingleMentionFetcher:
        def fetch(self, video_id, preferred_languages=None):
            text = "영어 시험 난이도와 학습법을 설명한다. 삼성은 사내 문서를 영어로 쓴다는 사례만 짧게 언급한다."
            return [TranscriptSegment(0, 1, text)], "ko"

        def join_segments(self, segments):
            return " ".join(s.text for s in segments)

    video = VideoInput(
        video_id="h-single-mention",
        title="난이도 조절에 실패했다는 2026 수능 영어",
        url="https://youtube.com/watch?v=h-single-mention",
        description="시험 난이도와 영어 학습법을 다루는 영상",
        tags=["슈카", "경제", "시사", "주식"],
    )

    result = analyze_video_heuristic(
        video,
        TranscriptCache(tmp_path / "cache"),
        _SingleMentionFetcher(),
        _DummyFundamentals(),
    )

    assert result["should_analyze_stocks"] is False
    assert result["stocks"] == []
    assert result["signal_metrics"]["has_specific_stock_path"] is False


def test_heuristic_compacts_large_description_and_tags(tmp_path):
    video = VideoInput(
        video_id="hcompact1",
        title="반도체 수혜주 종목 분석",
        url="https://youtube.com/watch?v=hcompact1",
        description="메모리 수혜주 설명 " * 80,
        tags=[f"태그{i}" for i in range(20)],
    )
    result = analyze_video_heuristic(video, TranscriptCache(tmp_path / "cache"), _DummyFetcher(), _DummyFundamentals())
    assert len(result["description"]) <= 280
    assert len(result["tags"]) == 12


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


def test_extract_mentions_resolves_spaced_korean_company_names():
    mentions = extract_mentions("삼성 바이오 로직스 분석", "SK 하이닉스 실적과 삼성 바이오 로직스 모멘텀을 본다")
    tickers = [m.ticker for m, _count in mentions]
    assert "207940.KS" in tickers
    assert "000660.KS" in tickers


def test_extract_mentions_suppresses_ambiguous_group_alias_when_longer_company_matches():
    mentions = extract_mentions("삼성 바이오 로직스 실적", "삼성 바이오 로직스가 핵심이다")
    tickers = [m.ticker for m, _count in mentions]
    assert "207940.KS" in tickers
    assert "005930.KS" not in tickers


def test_extract_mentions_ignores_email_domain_noise():
    mentions = extract_mentions(
        "공지",
        "문의는 sample@naver.com 으로 주세요. 영상과 무관한 일반 안내문입니다.",
    )
    assert mentions == []


def test_extract_mentions_uses_metadata_when_transcript_body_is_generic():
    mentions = extract_mentions(
        "전력 인프라 업황 점검",
        "이번 영상은 업황과 수급만 간단히 본다.",
        metadata_text="AI 데이터센터 전력 인프라 수혜주로 HD현대일렉트릭과 효성중공업을 본다.",
    )
    tickers = [m.ticker for m, _count in mentions]
    assert "267260.KS" in tickers
    assert "298040.KS" in tickers


def test_extract_mentions_suppresses_single_ambiguous_group_alias_without_specific_company():
    mentions = extract_mentions("한화 지금 들어가도 되나?", "개인투자자 심리와 투자 원칙을 말합니다.")
    tickers = [m.ticker for m, _count in mentions]
    assert "000880.KS" not in tickers


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


def test_heuristic_downgrades_actionable_when_no_tickers_remain(tmp_path, monkeypatch):
    from omx_brainstorm.models import VideoSignalAssessment

    video = VideoInput(
        video_id="h-empty-actionable",
        title="반도체 매수 기회",
        url="https://youtube.com/watch?v=h-empty-actionable",
        description="강한 종목 신호처럼 보이지만 실제 언급 종목은 없다",
        tags=["반도체", "매수"],
    )
    monkeypatch.setattr(
        "omx_brainstorm.heuristic_pipeline.assess_video_signal",
        lambda *args, **kwargs: VideoSignalAssessment(
            signal_score=82.0,
            video_signal_class="ACTIONABLE",
            should_analyze_stocks=True,
            reason="pre-gate actionable",
            video_type="STOCK_PICK",
            metrics={"test": True},
        ),
    )
    monkeypatch.setattr("omx_brainstorm.heuristic_pipeline.extract_mentions", lambda *args, **kwargs: [])

    result = analyze_video_heuristic(video, TranscriptCache(tmp_path / "cache"), _DummyFetcher(), _DummyFundamentals())

    assert result["video_signal_class"] == "LOW_SIGNAL"
    assert result["should_analyze_stocks"] is False
    assert result["stocks"] == []
    assert result["skip_reason"]


def test_heuristic_cached_metadata_fallback_keeps_macro_only_video_out_of_stock_analysis(tmp_path):
    class _MetadataOnlyFetcher:
        def fetch(self, video_id, preferred_languages=None):
            raise RuntimeError("no transcript")

        def join_segments(self, segments):
            return " ".join(s.text for s in segments)

    video = VideoInput(
        video_id="h-meta-macro",
        title='[LIVE] 이란 전쟁은 안 끝나고, M7은 끝났다?',
        url="https://youtube.com/watch?v=h-meta-macro",
        description="중동 위기와 시장 충격을 다룬다.",
        tags=["전쟁", "중동", "M7"],
    )

    result = analyze_video_heuristic(
        video,
        TranscriptCache(tmp_path / "cache"),
        _MetadataOnlyFetcher(),
        _DummyFundamentals(),
    )

    assert result["transcript_language"] == "metadata_fallback"
    assert result["video_signal_class"] in {"NOISE", "LOW_SIGNAL", "SECTOR_ONLY"}
    assert result["should_analyze_stocks"] is False
    assert result["stocks"] == []


def test_heuristic_extracts_price_target(tmp_path, monkeypatch):
    from omx_brainstorm.models import VideoSignalAssessment

    class _TargetFetcher:
        def fetch(self, video_id, preferred_languages=None):
            text = "엔비디아 매수 관점에서 목표가 150달러까지 본다. 현재는 100달러 수준이라 업사이드가 남아 있다."
            return [TranscriptSegment(0, 1, text)], "ko"

        def join_segments(self, segments):
            return " ".join(s.text for s in segments)

    video = VideoInput(
        video_id="htarget1",
        title="엔비디아 매수 목표가 점검",
        url="https://youtube.com/watch?v=htarget1",
        description="매수 목표가 분석",
        tags=["엔비디아", "목표가", "매수"],
    )
    monkeypatch.setattr(
        "omx_brainstorm.heuristic_pipeline.assess_video_signal",
        lambda *args, **kwargs: VideoSignalAssessment(
            signal_score=88.0,
            video_signal_class="ACTIONABLE",
            should_analyze_stocks=True,
            reason="target test",
            video_type="STOCK_PICK",
        ),
    )
    result = analyze_video_heuristic(video, TranscriptCache(tmp_path / "cache"), _TargetFetcher(), _DummyFundamentals())
    nvda = next(stock for stock in result["stocks"] if stock["ticker"] == "NVDA")
    assert nvda["price_targets"][0]["target_price"] == 150.0
    assert nvda["price_target"]["target_price"] == 150.0
    assert nvda["price_target"]["status"] == "PENDING"
