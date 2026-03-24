"""Additional pipeline.py coverage tests for error branches and analyze_channel_since."""
from pathlib import Path
from unittest.mock import MagicMock

from omx_brainstorm.models import VideoInput, TranscriptSegment
from omx_brainstorm.pipeline import OMXPipeline
from omx_brainstorm.transcript_cache import TranscriptCache


class _DummyFetcher:
    def fetch(self, video_id, preferred_languages=None):
        text = "엔비디아가 아직 더 갈 수 있다. 반도체 투자와 메모리 수혜주를 점검한다." * 10
        return [TranscriptSegment(0, 1, text)], "ko"

    def join_segments(self, segments):
        return " ".join(s.text for s in segments)


class _DummyFundamentals:
    def fetch(self, mention):
        from omx_brainstorm.models import FundamentalSnapshot
        return FundamentalSnapshot(ticker=mention.ticker, company_name="TestCo", currency="USD", current_price=100.0, data_source="dummy")


class _DummyResolver:
    def __init__(self, videos):
        self._videos = videos if isinstance(videos, list) else [videos]

    def resolve_video(self, url_or_id):
        return self._videos[0]

    def resolve_channel_videos(self, channel_url, limit=5):
        return self._videos[:limit]

    def resolve_channel_videos_since(self, channel_url, days=30, max_entries=80):
        return self._videos


def _make_pipeline(tmp_path, videos, provider_name="mock"):
    pipeline = OMXPipeline(provider_name=provider_name, output_dir=tmp_path, transcript_cache=TranscriptCache(tmp_path / "cache"))
    pipeline.resolver = _DummyResolver(videos)
    pipeline.fetcher = _DummyFetcher()
    pipeline.fundamentals = _DummyFundamentals()
    return pipeline


def test_market_review_extraction_failure(tmp_path, monkeypatch):
    """Lines 76-77: market_review extraction exception should be caught."""
    import omx_brainstorm.pipeline as pipeline_mod

    monkeypatch.setattr(pipeline_mod, "extract_market_review", lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("boom")))

    video = VideoInput(
        video_id="mr_fail",
        title="마감시황 코스피 상승 나스닥 반등",
        url="https://youtube.com/watch?v=mr_fail",
        description="시황",
        tags=["시황", "코스피"],
    )
    pipeline = _make_pipeline(tmp_path, video)
    report, _ = pipeline.analyze_video(video.url)
    assert report.market_review is None
    assert report.macro_insights == []


def test_expert_interview_with_non_mock_provider(tmp_path, monkeypatch):
    """Lines 80-81: non-mock provider uses extract_expert_insights_with_llm."""
    import omx_brainstorm.pipeline as pipeline_mod
    from omx_brainstorm.models import ExpertInsight
    from omx_brainstorm.llm import LLMResponse

    fake_insights = [ExpertInsight(expert_name="테스트", affiliation="테스트증권", key_claims=["test"])]
    monkeypatch.setattr(pipeline_mod, "extract_expert_insights_with_llm", lambda *a, **kw: fake_insights)
    # Mock the LLM provider to avoid needing a real CLI
    fake_llm_response = LLMResponse(provider="mock", text='{"tickers": []}')
    class FakeProvider:
        def run(self, s, u):
            return fake_llm_response
        def run_json(self, s, u):
            return {"tickers": []}
    monkeypatch.setattr(pipeline_mod, "resolve_provider", lambda name: FakeProvider())

    video = VideoInput(
        video_id="ei_llm",
        title="김영호 대표 인터뷰 반도체 전망",
        url="https://youtube.com/watch?v=ei_llm",
        description="김영호 삼성증권 대표",
        tags=["인터뷰"],
    )
    pipeline = _make_pipeline(tmp_path, video, provider_name="claude")
    report, _ = pipeline.analyze_video(video.url)
    assert len(report.expert_insights) == 1
    assert report.expert_insights[0].expert_name == "테스트"


def test_expert_interview_macro_extraction_failure(tmp_path, monkeypatch):
    """Lines 88-89: macro extraction failure inside EXPERT_INTERVIEW branch."""
    import omx_brainstorm.pipeline as pipeline_mod

    monkeypatch.setattr(pipeline_mod, "extract_macro_insights", lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("macro boom")))

    video = VideoInput(
        video_id="ei_macro_fail",
        title="김영호 대표 인터뷰 반도체 전망",
        url="https://youtube.com/watch?v=ei_macro_fail",
        description="김영호 삼성증권 대표",
        tags=["인터뷰"],
    )
    pipeline = _make_pipeline(tmp_path, video)
    report, _ = pipeline.analyze_video(video.url)
    assert report.macro_insights == []


def test_analyze_channel_since(tmp_path):
    """Lines 162-168: analyze_channel_since produces dashboard."""
    v1 = VideoInput(video_id="cs1", title="반도체 수혜주 종목 분석", url="https://youtube.com/watch?v=cs1")
    v2 = VideoInput(video_id="cs2", title="AI 관련주 점검", url="https://youtube.com/watch?v=cs2")
    pipeline = _make_pipeline(tmp_path, [v1, v2])
    results = pipeline.analyze_channel_since("https://youtube.com/channel/test", days=30)
    assert len(results) == 2
    dashboards = list(tmp_path.glob("channel_dashboard_*.md"))
    assert len(dashboards) == 1


def test_analyze_channel_since_empty(tmp_path):
    """analyze_channel_since with no videos returns empty list."""
    pipeline = _make_pipeline(tmp_path, [])
    pipeline.resolver = _DummyResolver([])
    results = pipeline.analyze_channel_since("https://youtube.com/channel/test", days=7)
    assert results == []
