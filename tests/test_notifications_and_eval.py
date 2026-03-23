"""Tests for notifications, evaluation, and fundamentals modules."""
from dataclasses import dataclass
from unittest.mock import MagicMock, patch

from omx_brainstorm.evaluation import ranking_spearman, ranking_validation
from omx_brainstorm.fundamentals import FundamentalsFetcher, _as_float
from omx_brainstorm.models import TickerMention
from omx_brainstorm.notifications import send_telegram_message


# --- notifications ---

@dataclass
class _FakeNotifConfig:
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""


def test_telegram_skipped_when_no_credentials():
    config = _FakeNotifConfig()
    assert send_telegram_message(config, "hello") is False


def test_telegram_success(monkeypatch):
    config = _FakeNotifConfig(telegram_bot_token="tok123", telegram_chat_id="chat456")
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"ok": True}
    mock_post = MagicMock(return_value=mock_resp)
    monkeypatch.setattr("omx_brainstorm.notifications.requests.post", mock_post)

    assert send_telegram_message(config, "test msg") is True
    mock_post.assert_called_once()
    call_args = mock_post.call_args
    assert call_args[1]["data"]["text"] == "test msg"


def test_telegram_failure(monkeypatch):
    config = _FakeNotifConfig(telegram_bot_token="tok", telegram_chat_id="chat")
    monkeypatch.setattr("omx_brainstorm.notifications.requests.post", MagicMock(side_effect=ConnectionError("fail")))
    assert send_telegram_message(config, "test") is False


# --- evaluation: ranking_spearman ---

@dataclass
class _FakeRankedItem:
    ticker: str
    company_name: str
    aggregate_score: float
    first_signal_at: str


def test_ranking_spearman_perfect_correlation():
    ranking = [
        _FakeRankedItem("A", "A Co", 90, "2026-01-01"),
        _FakeRankedItem("B", "B Co", 80, "2026-01-01"),
        _FakeRankedItem("C", "C Co", 70, "2026-01-01"),
    ]
    validation = {
        "top_3": {
            "positions": [
                {"ticker": "A", "return_pct": 10.0},
                {"ticker": "B", "return_pct": 5.0},
                {"ticker": "C", "return_pct": 1.0},
            ]
        }
    }
    result = ranking_spearman(ranking, validation)
    assert result == 1.0


def test_ranking_spearman_inverse_correlation():
    ranking = [
        _FakeRankedItem("A", "A Co", 90, "2026-01-01"),
        _FakeRankedItem("B", "B Co", 80, "2026-01-01"),
        _FakeRankedItem("C", "C Co", 70, "2026-01-01"),
    ]
    validation = {
        "top_3": {
            "positions": [
                {"ticker": "A", "return_pct": 1.0},
                {"ticker": "B", "return_pct": 5.0},
                {"ticker": "C", "return_pct": 10.0},
            ]
        }
    }
    result = ranking_spearman(ranking, validation)
    assert result == -1.0


def test_ranking_spearman_too_few_positions():
    ranking = [_FakeRankedItem("A", "A", 90, "2026-01-01")]
    validation = {"top_1": {"positions": [{"ticker": "A", "return_pct": 5.0}]}}
    assert ranking_spearman(ranking, validation) is None


def test_ranking_spearman_empty_validation():
    assert ranking_spearman([], {}) is None


def test_ranking_validation_empty():
    result = ranking_validation([], "2026-03-23")
    assert result == {}


def test_ranking_validation_runs_backtest(monkeypatch):
    @dataclass
    class _FakeReport:
        def to_dict(self):
            return {"portfolio_return_pct": 5.0, "positions": []}

    class _FakeEngine:
        def run_buy_and_hold(self, **kwargs):
            return _FakeReport()

    monkeypatch.setattr("omx_brainstorm.evaluation.BacktestEngine", _FakeEngine)

    ranking = [
        _FakeRankedItem("NVDA", "NVIDIA", 80.0, "2026-03-01"),
        _FakeRankedItem("AAPL", "Apple", 70.0, "2026-03-01"),
    ]
    result = ranking_validation(ranking, "2026-03-23")
    assert "top_1" in result
    assert "top_3" in result
    assert f"top_{len(ranking)}" in result


# --- fundamentals ---

def test_as_float_none():
    assert _as_float(None) is None


def test_as_float_valid():
    assert _as_float(42) == 42.0
    assert _as_float("3.14") == 3.14


def test_as_float_invalid():
    assert _as_float("not a number") is None


def test_fundamentals_no_yfinance(monkeypatch):
    import omx_brainstorm.fundamentals as mod
    original_import = __builtins__.__import__ if hasattr(__builtins__, '__import__') else __import__

    def mock_import(name, *args, **kwargs):
        if name == "yfinance":
            raise ImportError("no yfinance")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr("builtins.__import__", mock_import)
    # Clear any cached yfinance module
    import sys
    saved = sys.modules.pop("yfinance", None)
    try:
        fetcher = FundamentalsFetcher()
        mention = TickerMention(ticker="NVDA", company_name="NVIDIA")
        snapshot = fetcher.fetch(mention)
        assert snapshot.data_source == "unavailable"
        assert "yfinance_not_installed" in snapshot.notes
    finally:
        if saved is not None:
            sys.modules["yfinance"] = saved


def test_fundamentals_yfinance_error(monkeypatch):
    class _FakeTicker:
        def __init__(self, t):
            raise RuntimeError("API error")

    fake_yf = MagicMock()
    fake_yf.Ticker = _FakeTicker
    monkeypatch.setitem(__import__('sys').modules, "yfinance", fake_yf)

    fetcher = FundamentalsFetcher()
    mention = TickerMention(ticker="NVDA", company_name="NVIDIA")
    snapshot = fetcher.fetch(mention)
    assert snapshot.data_source == "yfinance_error"


def test_fundamentals_yfinance_success(monkeypatch):
    class _FakeFastInfo:
        last_price = 900.0
        market_cap = 2_000_000_000_000

    class _FakeTicker:
        def __init__(self, t):
            self.info = {
                "longName": "NVIDIA Corp",
                "currency": "USD",
                "currentPrice": 900.0,
                "marketCap": 2_000_000_000_000,
                "trailingPE": 60.0,
                "forwardPE": 40.0,
                "revenueGrowth": 0.5,
                "operatingMargins": 0.3,
                "returnOnEquity": 0.8,
                "debtToEquity": 40.0,
            }
            self.fast_info = _FakeFastInfo()

    fake_yf = MagicMock()
    fake_yf.Ticker = _FakeTicker
    monkeypatch.setitem(__import__('sys').modules, "yfinance", fake_yf)

    fetcher = FundamentalsFetcher()
    mention = TickerMention(ticker="NVDA", company_name="NVIDIA")
    snapshot = fetcher.fetch(mention)
    assert snapshot.data_source == "yfinance"
    assert snapshot.company_name == "NVIDIA Corp"
    assert snapshot.current_price == 900.0
    assert snapshot.revenue_growth == 0.5


def test_fundamentals_missing_price_adds_note(monkeypatch):
    class _FakeTicker:
        def __init__(self, t):
            self.info = {}
            self.fast_info = None

    fake_yf = MagicMock()
    fake_yf.Ticker = _FakeTicker
    monkeypatch.setitem(__import__('sys').modules, "yfinance", fake_yf)

    fetcher = FundamentalsFetcher()
    mention = TickerMention(ticker="TEST", company_name="Test Co")
    snapshot = fetcher.fetch(mention)
    assert "missing_current_price" in snapshot.notes
    assert "missing_revenue_growth" in snapshot.notes
