import json
import sys

from omx_brainstorm.backtest import BacktestReport
from omx_brainstorm.cli import build_parser, main, _report_summary
from omx_brainstorm.models import (
    ExpertInsight, MacroInsight, MarketReviewSummary,
    StockAnalysis, FundamentalSnapshot, MasterOpinion,
    TickerMention, VideoAnalysisReport, VideoInput, VideoSignalAssessment,
)


def test_cli_backtest_ranked_outputs_json(monkeypatch, tmp_path, capsys):
    payload = {
        "cross_video_ranking": [
            {"ticker": "NVDA", "company_name": "NVIDIA", "aggregate_score": 88.3, "first_signal_at": "2026-03-18"},
            {"ticker": "MU", "company_name": "Micron", "aggregate_score": 86.3, "first_signal_at": "2026-03-19"},
        ]
    }
    input_path = tmp_path / "ranking.json"
    input_path.write_text(json.dumps(payload), encoding="utf-8")

    def fake_run_buy_and_hold(self, ideas, start_date, end_date, initial_capital=10000.0, top_n=None):
        assert [item.ticker for item in ideas] == ["NVDA", "MU"]
        assert [item.signal_date for item in ideas] == ["2026-03-18", "2026-03-19"]
        assert start_date == "2026-01-02"
        assert end_date == "2026-03-21"
        assert top_n == 2
        assert initial_capital == 1000.0
        return BacktestReport(
            start_date=start_date,
            end_date=end_date,
            initial_capital=initial_capital,
            ending_capital=1100.0,
            portfolio_return_pct=10.0,
            hit_rate=1.0,
        )

    monkeypatch.setattr("omx_brainstorm.cli.BacktestEngine.run_buy_and_hold", fake_run_buy_and_hold)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "omx-brainstorm",
            "backtest-ranked",
            str(input_path),
            "--start-date",
            "2026-01-02",
            "--end-date",
            "2026-03-21",
            "--top-n",
            "2",
            "--initial-capital",
            "1000",
        ],
    )

    main()
    output = json.loads(capsys.readouterr().out)
    assert output["ending_capital"] == 1100.0
    assert output["portfolio_return_pct"] == 10.0


def test_cli_parser_has_helpful_descriptions():
    parser = build_parser()
    assert parser.description
    help_text = parser.format_help()
    assert "analyze-channel" in help_text
    assert "backtest-ranked" in help_text
    assert "backtest-artifact" in help_text
    assert "run-comparison" in help_text
    assert "run-scheduler" in help_text
    assert "run-healthcheck" in help_text
    assert "export-kindshot-feed" in help_text


def test_report_summary_includes_new_fields(tmp_path):
    from pathlib import Path
    video = VideoInput(video_id="t1", title="테스트", url="https://youtube.com/watch?v=t1")
    signal = VideoSignalAssessment(
        signal_score=75.0, video_signal_class="ACTIONABLE",
        should_analyze_stocks=True, reason="test", video_type="MACRO",
    )
    macro = MacroInsight(indicator="interest_rate", direction="DOWN", label="금리", confidence=0.7)
    expert = ExpertInsight(expert_name="김박사", affiliation="테스트증권", key_claims=["상승 전망"])
    report = VideoAnalysisReport(
        run_id="r1", created_at="2026-03-23", provider="mock", mode="ralph",
        video=video, signal_assessment=signal, transcript_text="t", transcript_language="ko",
        ticker_mentions=[TickerMention(ticker="NVDA")],
        stock_analyses=[],
        macro_insights=[macro],
        expert_insights=[expert],
    )
    paths = (tmp_path / "a.json", tmp_path / "a.md", tmp_path / "a.txt")
    summary = _report_summary(report, paths)
    assert summary["video_type"] == "MACRO"
    assert summary["signal_class"] == "ACTIONABLE"
    assert summary["signal_score"] == 75.0
    assert summary["macro_insights_count"] == 1
    assert summary["expert_insights_count"] == 1
    assert summary["has_market_review"] is False
    assert "NVDA" in summary["tickers"]


def test_cli_parser_has_analyze_all_command():
    parser = build_parser()
    args = parser.parse_args(["analyze-all", "--config", "config.toml", "--limit", "2"])
    assert args.command == "analyze-all"
    assert args.config == "config.toml"
    assert args.limit == 2
