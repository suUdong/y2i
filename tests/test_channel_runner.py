import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from scripts.run_channel_30d_comparison import (
    build_telegram_payload,
    discover_recent_video_ids,
    quality_scorecard,
    recent_feed_video_ids,
    run_comparison_job,
    save_comparison_artifacts,
)
from omx_brainstorm.app_config import AppConfig, ChannelConfig
from omx_brainstorm.comparison import RunContext


def test_recent_feed_video_ids_returns_empty_on_failure(monkeypatch):
    def boom(*args, **kwargs):
        raise OSError("feed down")

    monkeypatch.setattr("scripts.run_channel_30d_comparison.urlopen", boom)
    assert recent_feed_video_ids("dummy", days=30) == []


def test_quality_scorecard_handles_empty_rows():
    scorecard = quality_scorecard([], {}, [])
    assert scorecard["overall"] == 0.0
    assert scorecard["transcript_coverage"] == 0.0
    assert scorecard["actionable_density"] == 0.0


def test_quality_scorecard_zero_predictive_power_without_positions():
    scorecard = quality_scorecard([{"should_analyze_stocks": True, "transcript_language": "ko"}], {}, [])
    assert scorecard["ranking_predictive_power"] == 0.0


def test_quality_scorecard_zero_horizon_without_positions():
    scorecard = quality_scorecard([{"should_analyze_stocks": True, "transcript_language": "ko"}], {}, [])
    assert scorecard["horizon_adequacy"] == 0.0


def test_recent_feed_video_ids_returns_empty_without_channel_id():
    assert recent_feed_video_ids("", days=30) == []


def test_discover_recent_video_ids_falls_back_to_channel_listing(monkeypatch):
    class ResolverStub:
        def resolve_channel_videos_since(self, channel_url, days=30, reference_date=None):
            return [
                type("Video", (), {"video_id": "vid2"})(),
                type("Video", (), {"video_id": "vid2"})(),
                type("Video", (), {"video_id": "vid1"})(),
            ]

    monkeypatch.setattr("scripts.run_channel_30d_comparison.recent_feed_video_ids", lambda *args, **kwargs: [])
    video_ids = discover_recent_video_ids(
        "https://youtube.com/@test/videos",
        "UC123",
        days=30,
        today="2026-03-27",
        resolver=ResolverStub(),
    )
    assert video_ids == ["vid2", "vid1"]


def test_quality_scorecard_transcript_coverage_counts_cache():
    scorecard = quality_scorecard([{"should_analyze_stocks": True, "transcript_language": "cache:ko"}], {}, [])
    assert scorecard["transcript_coverage"] == 100.0


def test_run_comparison_job_handles_empty_channel_set(tmp_path):
    config = AppConfig(output_dir=str(tmp_path / "out"), registry_path=str(tmp_path / "channels.json"), channels=[])
    payload = run_comparison_job(config)
    assert "channels" in payload
    assert payload["dashboard_markdown"] is not None
    assert Path(payload["dashboard_markdown"]).exists()


def test_save_comparison_artifacts_writes_human_readable_labels(tmp_path):
    context = RunContext(run_id="20260326T000000Z", today="2026-03-26", output_dir=tmp_path, window_days=30)
    comparison = {
        "generated_at": context.run_id,
        "window_days": 30,
        "more_actionable_channel": "sampro",
        "better_ranking_channel": "itgod",
        "pipeline_summary": {
            "total_channels": 2,
            "total_videos": 30,
            "actionable_videos": 12,
            "strict_actionable_videos": 9,
            "skipped_videos": 18,
            "transcript_backed_videos": 21,
            "metadata_fallback_videos": 9,
            "latest_published_at": "20260325",
            "latest_reference_at": "20260326T000000Z",
            "latest_reference_kind": "generated_at",
            "top_skip_reasons": [{"reason": "근거 부족", "count": 5}],
        },
        "consensus_signals": [
            {
                "ticker": "NVDA",
                "company_name": "NVIDIA",
                "aggregate_score": 90.5,
                "consensus_strength": "STRONG",
                "cross_validation_status": "CONFIRMED",
                "cross_validation_score": 86.0,
                "channel_count": 2,
            }
        ],
        "channels": {
            "sampro": {
                "display_name": "삼프로TV",
                "total_videos": 15,
                "actionable_videos": 8,
                "strict_actionable_videos": 6,
                "skipped_videos": 7,
                "actionable_ratio": 0.5333,
                "transcript_backed_videos": 10,
                "metadata_fallback_videos": 5,
                "latest_published_at": "20260325",
                "latest_reference_at": "20260326T000000Z",
                "latest_reference_kind": "generated_at",
                "ranking_top_1_return_pct": 1.2,
                "ranking_top_3_return_pct": 2.3,
                "ranking_spearman": 0.45,
                "ranking_eval_positions": 4,
                "quality_scorecard": {"overall": 55.0},
                "top_skip_reasons": [{"reason": "근거 부족", "count": 3}],
                "signal_breakdown": {"ACTIONABLE": 6, "NOISE": 9},
            },
            "itgod": {
                "display_name": "IT의 신 이형수",
                "total_videos": 15,
                "actionable_videos": 4,
                "strict_actionable_videos": 3,
                "skipped_videos": 11,
                "actionable_ratio": 0.2667,
                "transcript_backed_videos": 11,
                "metadata_fallback_videos": 4,
                "latest_published_at": "20260324",
                "latest_reference_at": "20260324",
                "latest_reference_kind": "published_at",
                "ranking_top_1_return_pct": 3.4,
                "ranking_top_3_return_pct": 5.6,
                "ranking_spearman": 0.72,
                "ranking_eval_positions": 8,
                "quality_scorecard": {"overall": 61.0},
            },
        },
    }
    _json_path, txt_path = save_comparison_artifacts(comparison, context)
    txt = txt_path.read_text(encoding="utf-8")
    assert "분석 가능 비율 최고: 삼프로TV" in txt
    assert "랭킹 예측력 최고: IT의 신 이형수" in txt
    assert "- 스냅샷 run: 2026-03-26 00:00 UTC" in txt
    assert "- 채널 수: 2" in txt
    assert "- 최신 기준 출처: 스냅샷" in txt
    assert "- 분석 가능 비율: 53.3%" in txt
    assert "- 품질 점수표: 종합 55" in txt
    assert "- 시그널 분포: 엄격 액션 6 | 노이즈 9" in txt
    assert "- 최신 기준 시각: 2026-03-26 00:00 UTC" in txt
    assert "[합의 시그널]" in txt
    assert "NVIDIA (NVDA)" in txt
    assert "강한 합의" in txt
    assert "[삼프로TV]" in txt
    assert "- 채널 slug: sampro" in txt


def test_build_telegram_payload_includes_channel_summaries_and_leaderboard(tmp_path):
    class RankedStockStub:
        def __init__(self, ticker: str, company_name: str, score: float, verdict: str):
            self.ticker = ticker
            self.company_name = company_name
            self.aggregate_score = score
            self.aggregate_verdict = verdict

        def to_dict(self):
            return {
                "ticker": self.ticker,
                "company_name": self.company_name,
                "aggregate_score": self.aggregate_score,
                "aggregate_verdict": self.aggregate_verdict,
                "total_mentions": 2,
            }

    channel_payloads = {
        "sampro": {
            "display_name": "삼프로TV",
            "ranking": [RankedStockStub("NVDA", "NVIDIA", 88.0, "BUY")],
        },
        "itgod": {
            "display_name": "IT의 신",
            "ranking": [RankedStockStub("TSLA", "Tesla", 81.0, "WATCH")],
        },
    }
    leaderboard = [
        {"slug": "sampro", "display_name": "삼프로TV", "overall_quality_score": 77.5, "weight_multiplier": 1.2},
        {"slug": "itgod", "display_name": "IT의 신", "overall_quality_score": 71.2, "weight_multiplier": 0.95},
    ]
    context = RunContext(run_id="20260327T140000Z", today="2026-03-27", output_dir=tmp_path, window_days=30)

    payload = build_telegram_payload(channel_payloads, leaderboard, context)

    assert payload["generated_at"] == "20260327T140000Z"
    assert payload["daily_leaderboard"][0]["slug"] == "sampro"
    summaries = payload["analysis_summary"]["channel_signal_summaries"]
    assert summaries[0]["channel_name"] == "IT의 신"
    assert summaries[1]["signals"][0]["ticker"] == "NVDA"
    assert payload["analysis_summary"]["top_signals"][0]["ticker"] == "NVDA"
    assert payload["analysis_summary"]["top_signals"][0]["channel_count"] == 1
    assert payload["analysis_summary"]["consensus_signals"] == []


def test_build_telegram_payload_exposes_consensus_signals(tmp_path):
    class RankedStockStub:
        def __init__(self, ticker: str, company_name: str, score: float, verdict: str, *, target_price: float | None = None):
            self.ticker = ticker
            self.company_name = company_name
            self.aggregate_score = score
            self.aggregate_verdict = verdict
            self.target_price = target_price

        def to_dict(self):
            payload = {
                "ticker": self.ticker,
                "company_name": self.company_name,
                "aggregate_score": self.aggregate_score,
                "aggregate_verdict": self.aggregate_verdict,
                "total_mentions": 2,
                "appearances": 1,
                "latest_price": 100.0,
                "currency": "USD",
            }
            if self.target_price is not None:
                payload["price_target"] = {
                    "target_price": self.target_price,
                    "current_price": 100.0,
                    "currency": "USD",
                    "current_vs_target_pct": round((self.target_price - 100.0) / 100.0 * 100, 1),
                }
            return payload

    payload = build_telegram_payload(
        {
            "sampro": {"display_name": "삼프로TV", "ranking": [RankedStockStub("NVDA", "NVIDIA", 89.0, "STRONG_BUY", target_price=130.0)]},
            "itgod": {"display_name": "IT의 신", "ranking": [RankedStockStub("NVDA", "NVIDIA", 83.0, "BUY", target_price=150.0)]},
        },
        [
            {"slug": "sampro", "display_name": "삼프로TV", "overall_quality_score": 77.5, "weight_multiplier": 1.2},
            {"slug": "itgod", "display_name": "IT의 신", "overall_quality_score": 71.2, "weight_multiplier": 1.05},
        ],
        RunContext(run_id="20260327T141500Z", today="2026-03-27", output_dir=tmp_path, window_days=30),
    )

    consensus = payload["analysis_summary"]["consensus_signals"]
    assert consensus[0]["ticker"] == "NVDA"
    assert consensus[0]["consensus_signal"] is True
    assert consensus[0]["cross_validation_status"] == "CONFIRMED"
    assert consensus[0]["cross_validation_score"] >= 70.0
    assert consensus[0]["price_target"]["target_price"] == 140.0
