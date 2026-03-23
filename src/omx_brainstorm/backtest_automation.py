from __future__ import annotations

import json
from dataclasses import asdict
from datetime import date
from pathlib import Path

from .backtest import BacktestEngine, BacktestIdea


def run_backtest_for_artifact(
    artifact_path: str | Path,
    *,
    end_date: str | None = None,
    top_n: int | None = None,
    initial_capital: float = 10_000.0,
) -> dict:
    """Run a signal-date aware backtest for a saved ranking artifact."""
    artifact_path = Path(artifact_path)
    payload = json.loads(artifact_path.read_text(encoding="utf-8"))
    ranking = payload.get("cross_video_ranking", [])
    if not ranking:
        return {
            "artifact_path": str(artifact_path),
            "backtest_report": None,
            "status": "no_ranking",
        }
    end_date = end_date or date.today().isoformat()
    signal_dates = [item["first_signal_at"] for item in ranking if item.get("first_signal_at")]
    if not signal_dates:
        return {
            "artifact_path": str(artifact_path),
            "backtest_report": None,
            "status": "missing_signal_dates",
        }
    start_date = min(signal_dates)
    ideas = [
        BacktestIdea(
            ticker=item["ticker"],
            company_name=item.get("company_name"),
            score=float(item.get("aggregate_score", 0.0)),
            signal_date=item.get("first_signal_at"),
        )
        for item in ranking
    ]
    report = BacktestEngine().run_buy_and_hold(
        ideas=ideas,
        start_date=start_date,
        end_date=end_date,
        top_n=top_n,
        initial_capital=initial_capital,
    )
    return {
        "artifact_path": str(artifact_path),
        "status": "ok",
        "backtest_report": report.to_dict(),
    }
