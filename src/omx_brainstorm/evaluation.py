from __future__ import annotations

from datetime import date
from statistics import mean
from typing import Any

from .backtest import BacktestEngine, BacktestIdea


def ranking_validation(ranking: list, end_date: str) -> dict[str, Any]:
    """Run signal-date aware backtests over ranked channel ideas."""
    if not ranking:
        return {}
    start_date = min(item.first_signal_at for item in ranking if item.first_signal_at)
    ideas = [
        BacktestIdea(
            ticker=item.ticker,
            company_name=item.company_name,
            score=item.aggregate_score,
            signal_date=item.first_signal_at,
        )
        for item in ranking
    ]
    engine = BacktestEngine()
    validation = {}
    for top_n in [1, 3, len(ideas)]:
        validation[f"top_{top_n}"] = engine.run_buy_and_hold(
            ideas=ideas,
            start_date=start_date,
            end_date=end_date,
            top_n=top_n,
            initial_capital=10_000.0,
        ).to_dict()
    return validation


def ranking_spearman(ranking: list, validation: dict[str, Any]) -> float | None:
    """Measure rank-order alignment between predicted ranking and realized returns."""
    top_all = validation.get(f"top_{len(ranking)}", {})
    positions = top_all.get("positions", [])
    if len(positions) < 2:
        return None
    returns_by_ticker = {item["ticker"]: item["return_pct"] for item in positions}
    ranked = [item.ticker for item in ranking if item.ticker in returns_by_ticker]
    realized = sorted(ranked, key=lambda ticker: returns_by_ticker[ticker], reverse=True)
    pos = {ticker: idx + 1 for idx, ticker in enumerate(realized)}
    n = len(ranked)
    d2 = sum(((idx + 1) - pos[ticker]) ** 2 for idx, ticker in enumerate(ranked))
    return round(1 - (6 * d2) / (n * (n * n - 1)), 4) if n > 1 else None
