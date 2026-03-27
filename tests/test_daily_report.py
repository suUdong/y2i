from omx_brainstorm.comparison import RunContext
from omx_brainstorm.daily_report import (
    build_daily_report_payload,
    format_daily_report_telegram_caption,
    render_daily_report_markdown,
    save_daily_report,
)


def test_build_daily_report_payload_and_save(tmp_path):
    channel_payloads = {
        "sampro": {
            "display_name": "삼프로TV",
            "rows": [
                {
                    "stocks": [
                        {"ticker": "NVDA"},
                        {"ticker": "TSLA"},
                    ]
                },
                {
                    "stocks": [
                        {"ticker": "NVDA"},
                    ]
                },
            ],
        },
        "itgod": {
            "display_name": "IT의 신",
            "rows": [
                {
                    "stocks": [
                        {"ticker": "AAPL"},
                    ]
                }
            ],
        },
    }
    comparison = {
        "pipeline_summary": {"actionable_videos": 2, "strict_actionable_videos": 1},
        "consensus_signals": [
            {
                "ticker": "NVDA",
                "company_name": "NVIDIA",
                "aggregate_score": 91.0,
                "aggregate_verdict": "STRONG_BUY",
                "consensus_strength": "STRONG",
                "cross_validation_status": "CONFIRMED",
                "channel_count": 2,
            }
        ],
        "signal_accuracy": {"overall": {"total_signals": 12}},
        "channels": {
            "sampro": {
                "actionable_videos": 2,
                "strict_actionable_videos": 1,
                "tracked_signals": 8,
                "hit_rate_5d": 64.0,
                "target_hit_rate": 55.0,
                "overall_quality_score": 78.2,
                "signal_accuracy": {"hit_rate_3d": 60.0},
            },
            "itgod": {
                "actionable_videos": 1,
                "strict_actionable_videos": 0,
                "tracked_signals": 4,
                "hit_rate_5d": 51.0,
                "target_hit_rate": 40.0,
                "overall_quality_score": 70.1,
                "signal_accuracy": {"hit_rate_3d": 48.0},
            },
        },
    }
    leaderboard = [
        {
            "slug": "sampro",
            "display_name": "삼프로TV",
            "overall_quality_score": 78.2,
            "weight_multiplier": 1.16,
            "hit_rate_3d": 60.0,
            "hit_rate_5d": 64.0,
            "avg_return_5d": 2.7,
            "target_hit_rate": 55.0,
            "total_signals": 8,
            "actionable_ratio": 0.5,
        }
    ]
    context = RunContext(run_id="20260328T000500Z", today="2026-03-28", output_dir=tmp_path / "output", window_days=30)

    payload = build_daily_report_payload(channel_payloads, comparison, leaderboard, context)

    assert payload["totals"]["videos_analyzed"] == 3
    assert payload["totals"]["signal_count"] == 4
    assert payload["totals"]["unique_ticker_count"] == 3
    assert payload["totals"]["consensus_signal_count"] == 1

    markdown = render_daily_report_markdown(payload)
    assert "오늘 분석된 영상 수" in markdown
    assert "삼프로TV" in markdown
    assert "NVIDIA (NVDA)" in markdown

    report_path = save_daily_report(payload, tmp_path / "reports")
    assert report_path.exists()
    assert report_path.parent == tmp_path / "reports"

    caption = format_daily_report_telegram_caption(payload)
    assert "일일 마감 리포트" in caption
    assert "분석 영상 3개" in caption
