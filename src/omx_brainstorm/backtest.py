from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date
from typing import Any, Protocol, Sequence


@dataclass(slots=True)
class HistoricalPricePoint:
    """Single dated close price used by the backtest engine."""
    date: str
    close: float


@dataclass(slots=True)
class BacktestIdea:
    """Backtest input idea with ranking score and optional signal date."""
    ticker: str
    company_name: str | None
    score: float
    signal_date: str | None = None


@dataclass(slots=True)
class BacktestPositionResult:
    """Resolved position outcome for one backtested idea."""
    ticker: str
    company_name: str | None
    weight: float
    signal_date: str | None
    entry_date: str
    exit_date: str
    entry_price: float
    exit_price: float
    return_pct: float
    starting_value: float
    ending_value: float


@dataclass(slots=True)
class BacktestReport:
    """Aggregate portfolio-level backtest result."""
    start_date: str
    end_date: str
    initial_capital: float
    ending_capital: float
    portfolio_return_pct: float
    hit_rate: float
    positions: list[BacktestPositionResult] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class PriceHistoryProvider(Protocol):
    def get_price_history(self, ticker: str, start_date: str, end_date: str) -> list[HistoricalPricePoint]: ...


class YFinanceHistoryProvider:
    def get_price_history(self, ticker: str, start_date: str, end_date: str) -> list[HistoricalPricePoint]:
        import yfinance as yf

        history = yf.Ticker(ticker).history(start=start_date, end=end_date, auto_adjust=False)
        if history.empty:
            return []
        return [
            HistoricalPricePoint(date=index.strftime("%Y-%m-%d"), close=float(row["Close"]))
            for index, row in history.iterrows()
            if row.get("Close") is not None
        ]


class BacktestEngine:
    """Run simple signal-date aware buy-and-hold backtests."""
    def __init__(self, history_provider: PriceHistoryProvider | None = None):
        self.history_provider = history_provider or YFinanceHistoryProvider()

    def run_buy_and_hold(
        self,
        ideas: Sequence[BacktestIdea],
        start_date: str,
        end_date: str,
        initial_capital: float = 10_000.0,
        top_n: int | None = None,
    ) -> BacktestReport:
        selected = sorted(ideas, key=lambda item: (-item.score, item.ticker))
        if top_n is not None:
            selected = selected[:top_n]

        materialized: list[tuple[BacktestIdea, list[HistoricalPricePoint]]] = []
        skipped: list[str] = []
        for idea in selected:
            history = self.history_provider.get_price_history(idea.ticker, start_date, end_date)
            entry_exit = _resolve_entry_exit(history, start_date, end_date, idea.signal_date)
            if entry_exit is None:
                skipped.append(idea.ticker)
                continue
            materialized.append((idea, entry_exit))

        if not materialized:
            return BacktestReport(
                start_date=start_date,
                end_date=end_date,
                initial_capital=initial_capital,
                ending_capital=initial_capital,
                portfolio_return_pct=0.0,
                hit_rate=0.0,
                positions=[],
                skipped=skipped,
            )

        weight = 1.0 / len(materialized)
        allocation = initial_capital * weight
        positions: list[BacktestPositionResult] = []
        ending_capital = 0.0
        wins = 0

        for idea, (entry, exit_) in materialized:
            return_pct = 0.0 if entry.close == 0 else (exit_.close - entry.close) / entry.close
            ending_value = allocation * (1.0 + return_pct)
            ending_capital += ending_value
            if return_pct > 0:
                wins += 1
            positions.append(
                BacktestPositionResult(
                    ticker=idea.ticker,
                    company_name=idea.company_name,
                    weight=round(weight, 4),
                    signal_date=idea.signal_date,
                    entry_date=entry.date,
                    exit_date=exit_.date,
                    entry_price=entry.close,
                    exit_price=exit_.close,
                    return_pct=round(return_pct * 100.0, 2),
                    starting_value=round(allocation, 2),
                    ending_value=round(ending_value, 2),
                )
            )

        portfolio_return_pct = 0.0 if initial_capital == 0 else ((ending_capital - initial_capital) / initial_capital) * 100.0
        return BacktestReport(
            start_date=start_date,
            end_date=end_date,
            initial_capital=round(initial_capital, 2),
            ending_capital=round(ending_capital, 2),
            portfolio_return_pct=round(portfolio_return_pct, 2),
            hit_rate=round(wins / len(materialized), 2),
            positions=positions,
            skipped=skipped,
        )


def _resolve_entry_exit(
    history: list[HistoricalPricePoint],
    start_date: str,
    end_date: str,
    signal_date: str | None,
) -> tuple[HistoricalPricePoint, HistoricalPricePoint] | None:
    sorted_history = sorted(history, key=lambda item: item.date)
    start_dt = _parse_date(start_date)
    end_dt = _parse_date(end_date)
    signal_dt = _parse_date(signal_date) if signal_date else None

    if signal_dt is not None:
        effective_start = max(start_dt, signal_dt)
        eligible = [item for item in sorted_history if _parse_date(item.date) > effective_start and _parse_date(item.date) <= end_dt]
    else:
        eligible = [item for item in sorted_history if start_dt <= _parse_date(item.date) <= end_dt]

    if len(eligible) < 2:
        return None
    return eligible[0], eligible[-1]


def _parse_date(value: str) -> date:
    return date.fromisoformat(value[:10])
