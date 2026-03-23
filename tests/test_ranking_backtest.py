from omx_brainstorm.backtest import BacktestEngine, BacktestIdea, HistoricalPricePoint
from omx_brainstorm.research import build_cross_video_ranking, render_cross_video_ranking_text


def _stock(
    ticker: str,
    company_name: str,
    mention_count: int,
    signal_strength_score: float,
    final_score: float,
    final_verdict: str,
    checked_at: str,
):
    return {
        "ticker": ticker,
        "company_name": company_name,
        "mention_count": mention_count,
        "signal_strength_score": signal_strength_score,
        "final_score": final_score,
        "final_verdict": final_verdict,
        "master_opinions": [
            {"master": "druckenmiller", "score": 82.0},
            {"master": "buffett", "score": 64.0},
            {"master": "soros", "score": 78.0},
        ],
        "fundamentals": {
            "ticker": ticker,
            "company_name": company_name,
            "checked_at": checked_at,
            "currency": "USD",
            "current_price": 100.0,
        },
    }


def test_build_cross_video_ranking_aggregates_repeated_tickers():
    videos = [
        {
            "video_id": "v1",
            "title": "AI memory roadmap",
            "should_analyze_stocks": True,
            "published_at": "2026-03-20",
            "stocks": [
                _stock("MU", "Micron", 7, 84.0, 78.8, "BUY", "2026-03-21T22:08:57Z"),
                _stock("NVDA", "NVIDIA", 1, 70.0, 73.2, "BUY", "2026-03-21T22:08:57Z"),
            ],
        },
        {
            "video_id": "v2",
            "title": "Foundry beneficiaries",
            "should_analyze_stocks": True,
            "published_at": "2026-03-18",
            "stocks": [
                _stock("NVDA", "NVIDIA", 5, 78.0, 78.8, "BUY", "2026-03-21T23:00:00Z"),
                _stock("005930.KS", "Samsung Electronics", 15, 88.0, 75.2, "BUY", "2026-03-21T23:10:00Z"),
                _stock("MU", "Micron", 4, 80.0, 77.2, "BUY", "2026-03-21T23:10:00Z"),
            ],
        },
    ]

    ranking = build_cross_video_ranking(videos)

    assert [item.ticker for item in ranking[:3]] == ["005930.KS", "MU", "NVDA"]
    assert ranking[0].appearances == 1
    assert ranking[0].total_mentions == 15
    assert ranking[0].first_signal_at == "2026-03-18"
    assert ranking[0].latest_checked_at == "2026-03-21T23:10:00Z"
    assert ranking[0].differentiation_score > 0
    assert ranking[2].aggregate_score < ranking[1].aggregate_score

    text = render_cross_video_ranking_text(ranking)
    assert "통합 종목 랭킹" in text
    assert "first_signal_at=2026-03-18" in text


class DummyHistoryProvider:
    def __init__(self, series):
        self.series = series

    def get_price_history(self, ticker: str, start_date: str, end_date: str):
        return self.series[ticker]


def test_backtest_engine_runs_equal_weight_buy_and_hold():
    provider = DummyHistoryProvider(
        {
            "MU": [
                HistoricalPricePoint(date="2026-01-15", close=99.0),
                HistoricalPricePoint(date="2026-01-02", close=100.0),
                HistoricalPricePoint(date="2026-01-16", close=100.0),
                HistoricalPricePoint(date="2026-01-31", close=110.0),
            ],
            "NVDA": [
                HistoricalPricePoint(date="2026-01-02", close=200.0),
                HistoricalPricePoint(date="2026-01-15", close=195.0),
                HistoricalPricePoint(date="2026-01-16", close=200.0),
                HistoricalPricePoint(date="2026-01-31", close=180.0),
            ],
        }
    )
    engine = BacktestEngine(provider)
    report = engine.run_buy_and_hold(
        ideas=[
            BacktestIdea(ticker="MU", company_name="Micron", score=76.5, signal_date="2026-01-15"),
            BacktestIdea(ticker="NVDA", company_name="NVIDIA", score=76.1, signal_date="2026-01-15"),
        ],
        start_date="2026-01-01",
        end_date="2026-01-31",
        initial_capital=1000.0,
    )

    assert report.positions[0].ticker == "MU"
    assert report.positions[1].ticker == "NVDA"
    assert report.positions[0].entry_date == "2026-01-16"
    assert report.positions[1].entry_date == "2026-01-16"
    assert report.ending_capital == 1000.0
    assert report.portfolio_return_pct == 0.0
    assert report.hit_rate == 0.5
