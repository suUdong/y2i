from __future__ import annotations

from dataclasses import replace

from .models import FundamentalSnapshot, TickerMention, utc_now_iso


class FundamentalsFetcher:
    """Best-effort current fundamentals fetcher using Yahoo Finance."""

    def fetch(self, mention: TickerMention) -> FundamentalSnapshot:
        try:
            import yfinance as yf
        except ImportError:
            return FundamentalSnapshot(
                ticker=mention.ticker,
                company_name=mention.company_name,
                data_source="unavailable",
                notes=["yfinance_not_installed"],
            )

        try:
            ticker = yf.Ticker(mention.ticker)
            info = ticker.info or {}
            fast_info = getattr(ticker, "fast_info", None)
        except Exception as exc:
            return FundamentalSnapshot(
                ticker=mention.ticker,
                company_name=mention.company_name,
                data_source="yfinance_error",
                notes=[f"fetch_error:{type(exc).__name__}"],
            )

        def _pick(*keys: str) -> object | None:
            for key in keys:
                value = info.get(key)
                if value is not None:
                    return value
            return None

        current_price = _pick("currentPrice", "regularMarketPrice")
        if current_price is None and fast_info is not None:
            current_price = getattr(fast_info, "last_price", None)

        market_cap = _pick("marketCap")
        if market_cap is None and fast_info is not None:
            market_cap = getattr(fast_info, "market_cap", None)

        snapshot = FundamentalSnapshot(
            ticker=mention.ticker,
            company_name=_pick("longName", "shortName") or mention.company_name,
            checked_at=utc_now_iso(),
            currency=_pick("currency", "financialCurrency"),
            current_price=_as_float(current_price),
            market_cap=_as_float(market_cap),
            trailing_pe=_as_float(_pick("trailingPE")),
            forward_pe=_as_float(_pick("forwardPE")),
            price_to_book=_as_float(_pick("priceToBook")),
            revenue_growth=_as_float(_pick("revenueGrowth")),
            earnings_growth=_as_float(_pick("earningsGrowth")),
            operating_margin=_as_float(_pick("operatingMargins")),
            return_on_equity=_as_float(_pick("returnOnEquity")),
            debt_to_equity=_as_float(_pick("debtToEquity")),
            fifty_two_week_change=_as_float(_pick("52WeekChange", "fiftyTwoWeekChange")),
            data_source="yfinance",
            notes=[],
        )

        if snapshot.company_name is None:
            snapshot = replace(snapshot, company_name=mention.company_name)
        if snapshot.current_price is None:
            snapshot.notes.append("missing_current_price")
        if snapshot.revenue_growth is None:
            snapshot.notes.append("missing_revenue_growth")
        return snapshot

def _as_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
