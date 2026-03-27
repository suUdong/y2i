"""Tests for stock_registry module."""
from omx_brainstorm.stock_registry import COMPANY_MAP, COMPANY_PATTERNS, SECTOR_STOCKS, resolve_kr_ticker


def test_company_map_has_required_entries():
    assert "엔비디아" in COMPANY_MAP
    assert "삼성전자" in COMPANY_MAP
    assert "sk하이닉스" in COMPANY_MAP


def test_company_map_returns_ticker_and_name():
    ticker, name = COMPANY_MAP["엔비디아"]
    assert ticker == "NVDA"
    assert name == "NVIDIA"


def test_company_map_korean_entries_have_krx_tickers():
    ticker, _ = COMPANY_MAP["삼성전자"]
    assert ticker.endswith(".KS") or ticker.endswith(".KQ")


def test_sector_stocks_has_required_sectors():
    required = ["growth_tech", "banks", "semiconductors", "defense", "construction"]
    for sector in required:
        assert sector in SECTOR_STOCKS, f"Missing sector: {sector}"
        assert len(SECTOR_STOCKS[sector]) >= 1


def test_sector_stocks_entries_are_ticker_name_tuples():
    for sector, stocks in SECTOR_STOCKS.items():
        for item in stocks:
            assert len(item) == 2, f"Bad entry in {sector}: {item}"
            ticker, name = item
            assert isinstance(ticker, str)
            assert isinstance(name, str)


def test_company_patterns_match_expected_strings():
    test_text = "엔비디아와 삼성 그리고 sk하이닉스"
    matches = [p for p in COMPANY_PATTERNS if p.search(test_text)]
    assert len(matches) >= 3


def test_company_patterns_case_insensitive():
    matches = [p for p in COMPANY_PATTERNS if p.search("NVIDIA")]
    assert len(matches) >= 1


def test_resolve_kr_ticker_exact_match():
    assert resolve_kr_ticker("삼성전자") == ("005930.KS", "Samsung Electronics")


def test_resolve_kr_ticker_alias_match():
    assert resolve_kr_ticker("현대차") == ("005380.KS", "Hyundai Motor")


def test_resolve_kr_ticker_normalizes_spacing():
    assert resolve_kr_ticker("SK 하이닉스") == ("000660.KS", "SK hynix")


def test_resolve_kr_ticker_unknown_name_returns_none():
    assert resolve_kr_ticker("unknown_company_xyz") is None
