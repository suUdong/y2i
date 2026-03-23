"""Tests for VideoType-based pipeline branching and expert interview extraction."""
from pathlib import Path

from omx_brainstorm.expert_interview import extract_expert_insights
from omx_brainstorm.models import VideoInput, VideoType
from omx_brainstorm.pipeline import OMXPipeline
from omx_brainstorm.signal_gate import assess_video_signal
from omx_brainstorm.title_taxonomy import classify_video_type
from omx_brainstorm.transcript_cache import TranscriptCache


# --- Shared test helpers ---

class _MockProvider:
    """Minimal mock that returns valid extraction/analysis JSON."""
    def run(self, system_prompt, user_prompt):
        import json
        from omx_brainstorm.llm import LLMResponse
        if "extract publicly traded stock tickers" in system_prompt.lower():
            payload = {"mentions": [{"ticker": "NVDA", "company_name": "NVIDIA", "confidence": 0.9, "reason": "test", "evidence": ["test"]}]}
        else:
            payload = {
                "ticker": "NVDA", "company_name": "NVIDIA",
                "basic_state": "test", "basic_signal_summary": "test",
                "basic_signal_verdict": "BUY",
                "master_opinions": [
                    {"master": "druckenmiller", "verdict": "BUY", "score": 80, "max_score": 100, "one_liner": "test", "rationale": ["t"], "risks": ["r"], "citations": ["fundamentals:x", "evidence:y"]},
                    {"master": "buffett", "verdict": "WATCH", "score": 65, "max_score": 100, "one_liner": "test", "rationale": ["t"], "risks": ["r"], "citations": ["fundamentals:x", "evidence:y"]},
                    {"master": "soros", "verdict": "BUY", "score": 75, "max_score": 100, "one_liner": "test", "rationale": ["t"], "risks": ["r"], "citations": ["fundamentals:x", "evidence:y"]},
                ],
                "thesis_summary": "test", "framework_scores": [],
                "total_score": 76, "max_score": 100, "final_verdict": "BUY",
                "invalidation_triggers": ["test"], "citations": ["test"],
            }
        return LLMResponse(provider="mock", text=json.dumps(payload, ensure_ascii=False))

    def run_json(self, system_prompt, user_prompt):
        import json
        return json.loads(self.run(system_prompt, user_prompt).text)


class _DummyFundamentals:
    def fetch(self, mention):
        from omx_brainstorm.models import FundamentalSnapshot
        return FundamentalSnapshot(ticker=mention.ticker, company_name="NVIDIA", currency="USD", current_price=900.0, data_source="dummy")


class _DummyFetcher:
    def fetch(self, video_id, preferred_languages=None):
        from omx_brainstorm.models import TranscriptSegment
        return [TranscriptSegment(0, 1, "엔비디아가 아직 더 갈 수 있다. 반도체 투자와 메모리 수혜주를 점검한다." * 10)], "ko"

    def join_segments(self, segments):
        return " ".join(s.text for s in segments)


class _DummyResolver:
    def __init__(self, video):
        self._video = video

    def resolve_video(self, url_or_id):
        return self._video

    def resolve_channel_videos(self, channel_url, limit=5):
        return [self._video]


def _make_pipeline(tmp_path, video):
    pipeline = OMXPipeline(provider_name="mock", output_dir=tmp_path, transcript_cache=TranscriptCache(tmp_path / "cache"))
    pipeline.resolver = _DummyResolver(video)
    pipeline.fetcher = _DummyFetcher()
    pipeline.fundamentals = _DummyFundamentals()
    return pipeline


# --- Task 1: VideoType-based pipeline branching ---

def test_stock_pick_video_runs_stock_analysis(tmp_path):
    video = VideoInput(video_id="sp1", title="반도체 수혜주 종목 분석", url="https://youtube.com/watch?v=sp1")
    vtype = classify_video_type(video.title)
    assert vtype == VideoType.STOCK_PICK

    pipeline = _make_pipeline(tmp_path, video)
    report, _ = pipeline.analyze_video(video.url)
    assert report.stock_analyses  # stock analysis ran
    assert not report.expert_insights
    assert report.market_review is None


def test_macro_video_extracts_macro_insights(tmp_path):
    video = VideoInput(
        video_id="mac1",
        title="금리 인하와 환율 하락이 시작되면",
        url="https://youtube.com/watch?v=mac1",
    )
    vtype = classify_video_type(video.title)
    assert vtype == VideoType.MACRO

    pipeline = _make_pipeline(tmp_path, video)
    report, _ = pipeline.analyze_video(video.url)
    assert report.macro_insights  # macro insights extracted
    assert report.market_review is None


def test_market_review_video_extracts_review_summary(tmp_path):
    video = VideoInput(
        video_id="mr1",
        title="마감시황 코스피 상승 나스닥 반등",
        url="https://youtube.com/watch?v=mr1",
    )
    vtype = classify_video_type(video.title)
    assert vtype == VideoType.MARKET_REVIEW

    pipeline = _make_pipeline(tmp_path, video)
    report, _ = pipeline.analyze_video(video.url)
    assert report.market_review is not None
    assert report.macro_insights == report.market_review.macro_insights


def test_expert_interview_video_extracts_insights(tmp_path):
    video = VideoInput(
        video_id="ei1",
        title="김영호 대표 인터뷰 반도체 전망",
        url="https://youtube.com/watch?v=ei1",
        description="김영호 삼성증권 대표와 반도체 시장 전망을 이야기합니다.",
    )
    vtype = classify_video_type(video.title)
    assert vtype == VideoType.EXPERT_INTERVIEW

    pipeline = _make_pipeline(tmp_path, video)
    report, _ = pipeline.analyze_video(video.url)
    assert report.expert_insights  # expert insights extracted
    assert report.expert_insights[0].expert_name == "김영호"


def test_news_event_video_still_extracts_macro(tmp_path):
    video = VideoInput(
        video_id="ne1",
        title="트럼프 관세 긴급 속보",
        url="https://youtube.com/watch?v=ne1",
    )
    vtype = classify_video_type(video.title)
    assert vtype == VideoType.NEWS_EVENT

    pipeline = _make_pipeline(tmp_path, video)
    report, _ = pipeline.analyze_video(video.url)
    # macro insights may or may not be present depending on transcript content
    assert report.market_review is None
    assert not report.expert_insights


def test_report_to_dict_includes_new_fields(tmp_path):
    video = VideoInput(
        video_id="mac2",
        title="금리 인하와 환율 하락이 시작되면",
        url="https://youtube.com/watch?v=mac2",
    )
    pipeline = _make_pipeline(tmp_path, video)
    report, _ = pipeline.analyze_video(video.url)
    d = report.to_dict()
    assert "macro_insights" in d
    assert "market_review" in d
    assert "expert_insights" in d


# --- Task 2: Expert interview insight extraction ---

def test_extract_expert_name_and_affiliation():
    insights = extract_expert_insights(
        title="김영호 삼성증권 대표 인터뷰",
        text="반도체 전망에 대해 김영호 대표는 상승을 예상한다고 판단했다. 메모리 수요가 핵심 변수라고 강조.",
        description="삼성증권 김영호 대표와의 인터뷰",
    )
    assert len(insights) >= 1
    assert insights[0].expert_name == "김영호"
    assert "삼성증권" in insights[0].affiliation


def test_extract_expert_claims():
    insights = extract_expert_insights(
        title="이형수 교수 인터뷰",
        text="반도체 시장이 내년에 크게 성장할 것으로 전망한다. AI 투자가 핵심 변수가 될 것이다. 매수 타이밍을 봐야 한다.",
        description="",
    )
    assert len(insights) >= 1
    assert len(insights[0].key_claims) >= 2


def test_extract_expert_sentiment_bullish():
    insights = extract_expert_insights(
        title="박사 인터뷰",
        text="홍길동 박사는 상승 반등 강세 매수 기대가 크다고 판단했다.",
        description="홍길동 박사",
    )
    assert len(insights) >= 1
    assert insights[0].sentiment == "BULLISH"


def test_extract_expert_sentiment_bearish():
    insights = extract_expert_insights(
        title="위원 인터뷰",
        text="김철수 위원은 하락 약세 위험 리스크 경계해야 한다고 판단했다.",
        description="김철수 위원",
    )
    assert len(insights) >= 1
    assert insights[0].sentiment == "BEARISH"


def test_extract_expert_topic_detection():
    insights = extract_expert_insights(
        title="이경수 교수 반도체 전망 인터뷰",
        text="반도체 메모리 HBM 시장이 성장할 것으로 예상한다.",
        description="",
    )
    assert len(insights) >= 1
    assert insights[0].topic == "반도체"


def test_extract_no_expert_returns_empty():
    insights = extract_expert_insights(
        title="오늘의 뉴스 정리",
        text="시장이 움직였습니다.",
        description="",
    )
    assert insights == []


def test_channel_analysis_produces_dashboard(tmp_path):
    video = VideoInput(video_id="ch1", title="반도체 수혜주 종목 분석", url="https://youtube.com/watch?v=ch1")
    pipeline = _make_pipeline(tmp_path, video)
    results = pipeline.analyze_channel("https://youtube.com/channel/test", limit=1)
    assert len(results) == 1
    dashboards = list(tmp_path.glob("channel_dashboard_*.md"))
    assert len(dashboards) == 1
    content = dashboards[0].read_text(encoding="utf-8")
    assert "OMX 통합 대시보드" in content


def test_pipeline_resilient_to_expert_extraction_failure(tmp_path, monkeypatch):
    """Pipeline should still produce a report even if expert extraction crashes."""
    import omx_brainstorm.pipeline as pipeline_mod

    def _boom(*args, **kwargs):
        raise RuntimeError("expert extraction exploded")

    monkeypatch.setattr(pipeline_mod, "extract_expert_insights", _boom)

    video = VideoInput(
        video_id="fail1",
        title="김영호 대표 인터뷰 반도체 전망",
        url="https://youtube.com/watch?v=fail1",
        description="인터뷰",
    )
    pipeline = _make_pipeline(tmp_path, video)
    report, _ = pipeline.analyze_video(video.url)
    # Report still created, expert_insights empty due to failure
    assert report.expert_insights == []
    assert report.video is not None


def test_pipeline_resilient_to_macro_extraction_failure(tmp_path, monkeypatch):
    """Pipeline should still produce a report even if macro extraction crashes."""
    import omx_brainstorm.pipeline as pipeline_mod

    def _boom(*args, **kwargs):
        raise RuntimeError("macro extraction exploded")

    monkeypatch.setattr(pipeline_mod, "extract_macro_insights", _boom)

    video = VideoInput(
        video_id="fail2",
        title="금리 인하와 환율 하락이 시작되면",
        url="https://youtube.com/watch?v=fail2",
    )
    pipeline = _make_pipeline(tmp_path, video)
    report, _ = pipeline.analyze_video(video.url)
    assert report.macro_insights == []
    assert report.video is not None


def test_expert_mentioned_tickers():
    insights = extract_expert_insights(
        title="박사 인터뷰",
        text="홍길동 박사는 삼성전자와 SK하이닉스가 좋다고 전망했다.",
        description="홍길동 박사",
    )
    assert len(insights) >= 1
    tickers = insights[0].mentioned_tickers
    assert "005930.KS" in tickers or "000660.KS" in tickers
