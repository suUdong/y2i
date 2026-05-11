"""Kindshot export contract for y2i:reddit labels."""

from __future__ import annotations

import json
from pathlib import Path

from omx_brainstorm.kindshot_feed import export_signals_for_kindshot
from omx_brainstorm.signal_tracker import SignalRecord, SignalTrackerDB


def test_kindshot_export_labels_reddit_source_for_kr_match(tmp_path: Path) -> None:
    db = SignalTrackerDB(tmp_path / "tracker.json")
    db.add_record(
        SignalRecord(
            ticker="005930.KS",
            company_name="Samsung Electronics",
            channel_slug="reddit:stocks",
            signal_date="2026-05-10",
            signal_score=82.0,
            verdict="BUY",
            source_video_id="stocks:abc123",
            source_title="$005930 bullish Samsung HBM breakout",
            returns={"1d": 2.0, "3d": 3.0, "5d": 4.0, "10d": None, "20d": None},
        )
    )

    output_path = tmp_path / "kindshot.json"
    result = export_signals_for_kindshot(db, output_path)
    payload = json.loads(output_path.read_text(encoding="utf-8"))

    assert result["signal_count"] == 1
    assert payload["signals"][0]["signal_source"] == "y2i:reddit"
    assert payload["signals"][0]["channel"] == "reddit:stocks"
