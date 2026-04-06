from unittest.mock import MagicMock, patch, PropertyMock
import pandas as pd
from omx_brainstorm.fundamentals import FundamentalsFetcher
from omx_brainstorm.models import FundamentalSnapshot, TickerMention


def test_fundamental_snapshot_has_analyst_fields():
    snap = FundamentalSnapshot(
        ticker="AAPL",
        recommendation_key="buy",
        recommendation_mean=1.9,
        analyst_count=40,
        target_mean_price=295.0,
        target_median_price=300.0,
        analyst_strong_buy=6,
        analyst_buy=25,
        analyst_hold=15,
        analyst_sell=1,
        analyst_strong_sell=1,
        sector="Technology",
        industry="Consumer Electronics",
    )
    assert snap.recommendation_key == "buy"
    assert snap.recommendation_mean == 1.9
    assert snap.analyst_count == 40
    assert snap.target_mean_price == 295.0
    assert snap.target_median_price == 300.0
    assert snap.analyst_strong_buy == 6
    assert snap.analyst_buy == 25
    assert snap.analyst_hold == 15
    assert snap.analyst_sell == 1
    assert snap.analyst_strong_sell == 1
    assert snap.sector == "Technology"
    assert snap.industry == "Consumer Electronics"


def test_fundamental_snapshot_analyst_defaults():
    snap = FundamentalSnapshot(ticker="TEST")
    assert snap.recommendation_key is None
    assert snap.recommendation_mean is None
    assert snap.analyst_count is None
    assert snap.target_mean_price is None
    assert snap.target_median_price is None
    assert snap.analyst_strong_buy == 0
    assert snap.analyst_buy == 0
    assert snap.analyst_hold == 0
    assert snap.analyst_sell == 0
    assert snap.analyst_strong_sell == 0
    assert snap.sector is None
    assert snap.industry is None


def test_fetch_live_collects_analyst_data(tmp_path):
    mock_info = {
        "longName": "Apple Inc.",
        "currentPrice": 230.0,
        "currency": "USD",
        "recommendationKey": "buy",
        "recommendationMean": 1.9,
        "numberOfAnalystOpinions": 40,
        "targetMeanPrice": 295.0,
        "targetMedianPrice": 300.0,
        "sector": "Technology",
        "industry": "Consumer Electronics",
    }
    mock_rec = pd.DataFrame([{
        "period": "0m",
        "strongBuy": 6,
        "buy": 25,
        "hold": 15,
        "sell": 1,
        "strongSell": 1,
    }])

    mock_ticker = MagicMock()
    mock_ticker.info = mock_info
    type(mock_ticker).recommendations = PropertyMock(return_value=mock_rec)

    with patch("yfinance.Ticker", return_value=mock_ticker):
        fetcher = FundamentalsFetcher(cache_root=tmp_path, max_memory_entries=0)
        mention = TickerMention(ticker="AAPL", company_name="Apple")
        snap = fetcher._fetch_live(mention)

    assert snap.recommendation_key == "buy"
    assert snap.recommendation_mean == 1.9
    assert snap.analyst_count == 40
    assert snap.target_mean_price == 295.0
    assert snap.target_median_price == 300.0
    assert snap.analyst_strong_buy == 6
    assert snap.analyst_buy == 25
    assert snap.analyst_hold == 15
    assert snap.analyst_sell == 1
    assert snap.analyst_strong_sell == 1
    assert snap.sector == "Technology"
    assert snap.industry == "Consumer Electronics"


def test_fetch_live_handles_missing_recommendations(tmp_path):
    mock_info = {"longName": "Test Corp", "currentPrice": 100.0, "currency": "USD"}
    mock_ticker = MagicMock()
    mock_ticker.info = mock_info
    type(mock_ticker).recommendations = PropertyMock(return_value=None)

    with patch("yfinance.Ticker", return_value=mock_ticker):
        fetcher = FundamentalsFetcher(cache_root=tmp_path, max_memory_entries=0)
        mention = TickerMention(ticker="TEST")
        snap = fetcher._fetch_live(mention)

    assert snap.recommendation_key is None
    assert snap.analyst_count is None
    assert snap.analyst_strong_buy == 0
