"""Integration tests — twitter SignalRecords flow through kindshot_feed export."""

from __future__ import annotations

import json
from pathlib import Path

from omx_brainstorm.kindshot_feed import export_signals_for_kindshot
from omx_brainstorm.signal_tracker import SignalRecord, SignalTrackerDB


def _make_record(
    *,
    ticker: str,
    channel_slug: str,
    score: float,
    verdict: str = "BUY",
    company: str = "Sample Co",
    source_id: str | None = None,
) -> SignalRecord:
    return SignalRecord(
        ticker=ticker,
        company_name=company,
        channel_slug=channel_slug,
        signal_date="2026-05-10",
        signal_score=score,
        verdict=verdict,
        source_video_id=source_id or f"{channel_slug}:{ticker}",
        source_title=f"{company} tweet body",
    )


def test_twitter_signal_carries_y2i_twitter_label(tmp_path: Path) -> None:
    """High-conviction twitter BUY exports with signal_source='y2i:twitter'."""
    db = SignalTrackerDB(db_path=tmp_path / "tracker.json")
    db.add_record(_make_record(ticker="005930.KS", channel_slug="twitter:hankyung_news", score=80.0))

    output_path = tmp_path / "kindshot_feed.json"
    export_signals_for_kindshot(db, output_path)
    payload = json.loads(output_path.read_text(encoding="utf-8"))

    source_by_ticker = {item["ticker"]: item["signal_source"] for item in payload["signals"]}
    assert source_by_ticker.get("005930.KS") == "y2i:twitter"


def test_all_three_source_classes_export_with_distinct_labels(tmp_path: Path) -> None:
    """YouTube + news + twitter signals exported together carry the right tags."""
    db = SignalTrackerDB(db_path=tmp_path / "tracker.json")
    db.add_record(_make_record(ticker="005930.KS", channel_slug="twitter:hankyung_news", score=80.0))
    db.add_record(_make_record(ticker="000660.KS", channel_slug="news:hankyung", score=80.0))
    db.add_record(_make_record(ticker="042700.KS", channel_slug="sampro", score=80.0))

    output_path = tmp_path / "kindshot_feed.json"
    export_signals_for_kindshot(db, output_path)
    payload = json.loads(output_path.read_text(encoding="utf-8"))

    source_by_ticker = {item["ticker"]: item["signal_source"] for item in payload["signals"]}
    assert source_by_ticker.get("005930.KS") == "y2i:twitter"
    assert source_by_ticker.get("000660.KS") == "y2i:news"
    assert source_by_ticker.get("042700.KS") == "y2i:youtube"


def test_twitter_watch_records_are_filtered_out(tmp_path: Path) -> None:
    """WATCH verdict (low-confidence twitter) must not appear in the export."""
    db = SignalTrackerDB(db_path=tmp_path / "tracker.json")
    db.add_record(
        _make_record(ticker="005930.KS", channel_slug="twitter:wowtv_kr", score=64.0, verdict="WATCH")
    )

    output_path = tmp_path / "kindshot_feed.json"
    result = export_signals_for_kindshot(db, output_path)

    assert result["signal_count"] == 0


def test_twitter_record_below_score_threshold_is_filtered(tmp_path: Path) -> None:
    """Score < 65 (kindshot gate) must not appear regardless of BUY verdict."""
    db = SignalTrackerDB(db_path=tmp_path / "tracker.json")
    db.add_record(
        _make_record(ticker="005930.KS", channel_slug="twitter:mtnews_kr", score=60.0)
    )

    output_path = tmp_path / "kindshot_feed.json"
    result = export_signals_for_kindshot(db, output_path)

    assert result["signal_count"] == 0


def test_high_conviction_twitter_only_passes_gate(tmp_path: Path) -> None:
    """Score >= 72 unlocks the 'has_strong_conviction' branch (no target/history yet)."""
    db = SignalTrackerDB(db_path=tmp_path / "tracker.json")
    db.add_record(
        _make_record(ticker="005930.KS", channel_slug="twitter:hankyung_news", score=72.0)
    )
    db.add_record(
        _make_record(ticker="000660.KS", channel_slug="twitter:mtnews_kr", score=70.0)
    )

    output_path = tmp_path / "kindshot_feed.json"
    export_signals_for_kindshot(db, output_path)
    payload = json.loads(output_path.read_text(encoding="utf-8"))

    exported_tickers = {item["ticker"] for item in payload["signals"]}
    assert "005930.KS" in exported_tickers
    assert "000660.KS" not in exported_tickers
