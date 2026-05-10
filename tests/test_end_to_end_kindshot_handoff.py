"""End-to-end contract test: y2i export → kindshot Y2iFeed consumption shape.

A fresh signal recorded today by ``record_signals_from_rows`` (the path the
pipeline uses for newly-analyzed videos) must:

1. Survive ``export_signals_for_kindshot`` selection.
2. Match the JSON contract expected by ``kindshot.feed.Y2iFeed.poll_once`` —
   i.e. a top-level ``signals`` list of objects with the required keys.
3. Pass the KRX/score/verdict filter that gates kindshot's BUY routing.

This test runs entirely in-process and does not require the kindshot
package; it locks the contract by replicating the Y2iFeed entry rules and
poll semantics (see kindshot/src/kindshot/feed.py Y2iFeed.poll_once).
"""
from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path

import pytest

from omx_brainstorm.kindshot_feed import export_signals_for_kindshot
from omx_brainstorm.signal_tracker import (
    SignalTrackerDB,
    record_signals_from_rows,
)
from omx_brainstorm.backtest import HistoricalPricePoint


class _FakeHistoryProvider:
    """Returns a deterministic entry price for any ticker.

    HistoricalPricePoint only carries ``date`` and ``close``; richer OHLCV
    is handled elsewhere in the pipeline.
    """

    def get_price_history(self, ticker, start, end):
        return [HistoricalPricePoint(date=start[:10], close=10000.0)]


_Y2I_VERDICT_RANK = {"REJECT": 0, "WATCH": 1, "BUY": 2, "STRONG_BUY": 3}


def _kindshot_y2i_passes(
    sig: dict,
    *,
    min_score: float = 65.0,
    min_verdict: str = "WATCH",
    lookback_days: int = 7,
    now: datetime | None = None,
) -> bool:
    """Replicate kindshot.feed.Y2iFeed._qualifies + KRX ticker + lookback filter.

    Pinning this shape catches breakage in the contract before it reaches
    production; kindshot's Y2iFeed test suite covers the reverse direction.
    """
    ticker = str(sig.get("ticker", ""))
    if not ticker.endswith((".KS", ".KQ")):
        return False
    code_part = ticker.split(".")[0]
    if not re.fullmatch(r"\d{6}", code_part):
        return False

    score = float(sig.get("signal_score") or sig.get("confidence", 0) * 100 or 0)
    if score < min_score:
        return False
    verdict = str(sig.get("verdict", "")).upper()
    if _Y2I_VERDICT_RANK.get(verdict, 0) < _Y2I_VERDICT_RANK.get(min_verdict.upper(), 1):
        return False

    signal_date = sig.get("signal_date", "")
    try:
        sig_date = datetime.strptime(signal_date, "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return False
    now = now or datetime.now()
    return (now.date() - sig_date).days <= lookback_days


@pytest.fixture
def db(tmp_path: Path) -> SignalTrackerDB:
    return SignalTrackerDB(tmp_path / "tracker.json")


def test_fresh_y2i_video_reaches_kindshot_buy_queue_contract(
    db: SignalTrackerDB, tmp_path: Path
) -> None:
    """A fresh BUY-class video → kindshot_feed.json → satisfies Y2iFeed."""
    # 1. Simulate the pipeline ingesting a newly-analyzed video with two
    #    qualifying KR stocks. record_signals_from_rows is the exact path
    #    pipeline.py uses on each new video.
    today = datetime.now().date().isoformat()
    fake_video_row = {
        "video_id": "VID_FRESH_001",
        "title": "삼성전자/하이닉스 비중확대 기회 - 사이클 반등 임박",
        "published_at": today,
        "video_signal_class": "ACTIONABLE",
        "signal_score": 72,
        "stocks": [
            {
                "ticker": "005930.KS",
                "company_name": "삼성전자",
                # STRONG_BUY at sweet-spot score: passes the strong-conviction
                # bypass without depending on consensus aggregation.
                "signal_strength_score": 68.0,
                "final_verdict": "STRONG_BUY",
                "price_target": {"target_price": 75000, "currency": "KRW"},
            },
            {
                "ticker": "000660.KS",
                "company_name": "SK hynix",
                "signal_strength_score": 73.5,
                "final_verdict": "STRONG_BUY",
                "price_target": {"target_price": 280000, "currency": "KRW"},
            },
        ],
    }

    # Add a second channel touching the same tickers to form a 2-channel
    # consensus (best-performing cohort per empirical analysis).
    added_a = record_signals_from_rows(
        db, "itgod", [fake_video_row], history_provider=_FakeHistoryProvider()
    )
    fake_video_row_b = dict(fake_video_row, video_id="VID_FRESH_002")
    added_b = record_signals_from_rows(
        db, "sosumonkey", [fake_video_row_b], history_provider=_FakeHistoryProvider()
    )
    assert added_a == 2 and added_b == 2

    # 2. Export to the kindshot contract location.
    output_path = tmp_path / "kindshot_feed.json"
    payload = export_signals_for_kindshot(db, output_path)
    feed = json.loads(output_path.read_text(encoding="utf-8"))

    assert payload["signal_count"] >= 2
    assert "generated_at" in feed
    assert isinstance(feed["signals"], list)

    # 3. Every emitted signal must carry the required Y2iFeed keys.
    required_keys = {
        "ticker", "company_name", "signal_source", "signal_date",
        "confidence", "verdict", "channel", "channel_weight",
        "consensus_signal", "consensus_strength", "consensus_channel_count",
        "evidence",
    }
    for sig in feed["signals"]:
        assert required_keys.issubset(sig.keys()), f"missing keys: {required_keys - sig.keys()}"
        assert sig["signal_source"] == "y2i"
        # confidence is a normalized [0,1] float — kindshot multiplies by 100
        assert 0.0 <= sig["confidence"] < 1.0

    # 4. At least one exported signal must pass the kindshot Y2iFeed gate.
    qualifying = [s for s in feed["signals"] if _kindshot_y2i_passes(s)]
    assert qualifying, "no exported signal passed the kindshot Y2iFeed filter"

    # 5. Both tickers from the fresh video should appear.
    exported_tickers = {s["ticker"] for s in qualifying}
    assert exported_tickers >= {"005930.KS", "000660.KS"}


def test_low_score_fresh_signal_is_dropped_before_kindshot(
    db: SignalTrackerDB, tmp_path: Path
) -> None:
    """Below the 65 sweet-spot floor — should not appear in the feed."""
    today = datetime.now().date().isoformat()
    row = {
        "video_id": "VID_LOW_001",
        "title": "약한 매수 의견",
        "published_at": today,
        "stocks": [
            {
                "ticker": "035420.KS",
                "company_name": "NAVER",
                "signal_strength_score": 60.0,  # below 65 gate
                "final_verdict": "BUY",
                "price_target": {"target_price": 250000, "currency": "KRW"},
            }
        ],
    }
    record_signals_from_rows(
        db, "itgod", [row], history_provider=_FakeHistoryProvider()
    )

    output_path = tmp_path / "kindshot_feed.json"
    export_signals_for_kindshot(db, output_path)
    feed = json.loads(output_path.read_text(encoding="utf-8"))

    assert all(s["ticker"] != "035420.KS" for s in feed["signals"])


def test_non_kr_ticker_skipped_before_kindshot(
    db: SignalTrackerDB, tmp_path: Path
) -> None:
    """US tickers in y2i tracker should not be exported to the KR consumer."""
    today = datetime.now().date().isoformat()
    row = {
        "video_id": "VID_US_001",
        "title": "AVGO 강력 매수",
        "published_at": today,
        "stocks": [
            {
                "ticker": "AVGO",
                "company_name": "Broadcom",
                "signal_strength_score": 85.0,
                "final_verdict": "STRONG_BUY",
                "price_target": {"target_price": 250, "currency": "USD"},
            }
        ],
    }
    record_signals_from_rows(
        db, "itgod", [row], history_provider=_FakeHistoryProvider()
    )

    output_path = tmp_path / "kindshot_feed.json"
    export_signals_for_kindshot(db, output_path)
    feed = json.loads(output_path.read_text(encoding="utf-8"))

    assert feed["signals"] == []
