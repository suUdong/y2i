"""Tests for src/omx_brainstorm/twitter_source.py — RSS resolver + classifier."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from omx_brainstorm.healthcheck import compute_health_summary, read_health_state
from omx_brainstorm.signal_tracker import SignalTrackerDB
from omx_brainstorm.twitter_source import (
    TWITTER_HEALTH_PATH,
    TweetItem,
    TwitterFetchError,
    TwitterResolver,
    classify_tweet_item,
    extract_kr_tickers,
    ingest_tweets_into_tracker,
    parse_rss_xml,
    run_twitter_ingestion,
)


# ──────────────────────────────────────────────────────────────────────────
# RSS parsing
# ──────────────────────────────────────────────────────────────────────────


_FIXTURE_RSS = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>hankyung_news / nitter</title>
    <link>https://nitter.example/hankyung_news</link>
    <description>tweets</description>
    <item>
      <title>삼성전자 HBM4 양산 가속 — 외인 매수 유입 $005930</title>
      <link>https://nitter.example/hankyung_news/status/1</link>
      <pubDate>Mon, 11 May 2026 03:14:00 +0900</pubDate>
      <guid>status-1</guid>
      <description>$005930 외국인 순매수 강세</description>
      <category>반도체</category>
    </item>
    <item>
      <title>오늘의 시장 메모</title>
      <link>https://nitter.example/hankyung_news/status/2</link>
      <guid>status-2</guid>
      <description>장중 보합권</description>
    </item>
    <item>
      <link>https://nitter.example/hankyung_news/status/3</link>
    </item>
  </channel>
</rss>
"""


def test_parse_rss_xml_parses_items_and_decodes_entities() -> None:
    items = parse_rss_xml(_FIXTURE_RSS, handle="hankyung_news")

    assert len(items) == 2
    first, second = items
    assert first.handle == "hankyung_news"
    assert "삼성전자" in first.title
    assert first.url.endswith("/status/1")
    assert first.source_id == "hankyung_news:status-1"
    parsed_dt = datetime.fromisoformat(first.published_at)
    assert parsed_dt.tzinfo is not None
    assert parsed_dt.astimezone(timezone.utc).hour == 18  # 03:14 KST → 18:14 UTC prior day
    assert first.raw_categories == ["반도체"]
    assert second.title.startswith("오늘의 시장 메모")


def test_parse_rss_xml_raises_on_invalid_xml() -> None:
    with pytest.raises(TwitterFetchError):
        parse_rss_xml("<not-xml", handle="hankyung_news")


def test_resolver_fetch_raises_on_http_error() -> None:
    resolver = TwitterResolver(handles={"hankyung_news": "https://nitter.example/feed"})

    class _FakeResponse:
        def __init__(self) -> None:
            self.status = 500

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def read(self) -> bytes:
            return b""

    def _fake_open(url, timeout):
        return _FakeResponse()

    with patch.object(resolver._opener, "open", side_effect=_fake_open):
        with pytest.raises(TwitterFetchError) as ctx:
            resolver.fetch("hankyung_news")
        assert ctx.value.handle == "hankyung_news"
        assert ctx.value.status == 500


def test_resolver_fetch_unknown_handle_raises() -> None:
    resolver = TwitterResolver(handles={"hankyung_news": "https://nitter.example/feed"})
    with pytest.raises(TwitterFetchError) as ctx:
        resolver.fetch("unknown_handle_xyz")
    assert ctx.value.status == "unknown_handle"


def test_resolver_fetch_returns_parsed_items_on_success() -> None:
    resolver = TwitterResolver(handles={"hankyung_news": "https://nitter.example/feed"})

    class _FakeResponse:
        def __init__(self) -> None:
            self.status = 200
            self._body = _FIXTURE_RSS.encode("utf-8")

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def read(self) -> bytes:
            return self._body

    with patch.object(resolver._opener, "open", return_value=_FakeResponse()):
        items = resolver.fetch("hankyung_news", max_items=10)

    assert len(items) == 2
    assert items[0].handle == "hankyung_news"


# ──────────────────────────────────────────────────────────────────────────
# Classification
# ──────────────────────────────────────────────────────────────────────────


def _make_item(title: str, summary: str = "", handle: str = "hankyung_news") -> TweetItem:
    return TweetItem(
        source_id=f"{handle}:{title}",
        handle=handle,
        title=title,
        summary=summary,
        url="",
        published_at="2026-05-11T03:14:00+00:00",
        raw_categories=[],
    )


def test_classify_actionable_tweet_with_known_ticker_passes_gate() -> None:
    item = _make_item("삼성전자 HBM 수주 확대, 반도체 슈퍼사이클 재점화")
    result = classify_tweet_item(item)
    assert result["should_analyze"] is True
    assert result["signal_score"] >= 60.0


def test_classify_cashtag_alone_bumps_score_into_actionable_band() -> None:
    item = _make_item("$005930 외국인 순매수 강세 반도체 매수세")
    result = classify_tweet_item(item)
    assert result["should_analyze"] is True
    # Cashtag bump (+6) + ticker-in-text (+18) + finance keywords lifts score
    # over the BUY threshold (68.0) for a tweet body that resolves a ticker.
    assert result["signal_score"] >= 68.0
    assert result["video_signal_class"] == "ACTIONABLE"


def test_classify_non_equity_tweet_is_skipped() -> None:
    item = _make_item("주말 먹방 일상 브이로그")
    result = classify_tweet_item(item)
    assert result["should_analyze"] is False
    assert result["video_signal_class"] == "NON_EQUITY"


def test_classify_low_signal_tweet_is_skipped() -> None:
    item = _make_item("오늘의 날씨 정보")
    result = classify_tweet_item(item)
    assert result["should_analyze"] is False
    assert result["video_signal_class"] == "LOW_SIGNAL"


def test_extract_kr_tickers_resolves_cashtag_and_name_in_same_text() -> None:
    pairs = extract_kr_tickers("$005930 강세 — 하이닉스 매수세 그리고 $000660")
    tickers = {ticker for ticker, _name in pairs}
    assert "005930.KS" in tickers
    assert "000660.KS" in tickers
    # All KR market only.
    assert all(t.endswith((".KS", ".KQ")) for t in tickers)


def test_extract_kr_tickers_ignores_unknown_cashtag() -> None:
    # 999999 is not a registered Korean ticker.
    pairs = extract_kr_tickers("$999999 알 수 없는 종목")
    assert pairs == []


# ──────────────────────────────────────────────────────────────────────────
# Tracker ingestion + idempotency
# ──────────────────────────────────────────────────────────────────────────


def test_ingest_tweets_into_tracker_writes_signal_record(tmp_path: Path) -> None:
    db = SignalTrackerDB(db_path=tmp_path / "tracker.json")
    item = _make_item(
        "삼성전자 HBM 수주 확대로 반도체 매수세 — 외국인 사들였다",
        summary="$005930 외국인 순매수",
    )

    summary = ingest_tweets_into_tracker(db, [item])

    assert summary["ingested"] >= 1
    assert summary["by_handle"].get("hankyung_news", 0) >= 1
    tickers = {r.ticker for r in db.records}
    assert "005930.KS" in tickers
    slugs = {r.channel_slug for r in db.records}
    assert "twitter:hankyung_news" in slugs


def test_ingest_tweets_into_tracker_is_idempotent(tmp_path: Path) -> None:
    db = SignalTrackerDB(db_path=tmp_path / "tracker.json")
    item = _make_item("삼성전자 HBM 수주 확대 반도체 $005930")
    first = ingest_tweets_into_tracker(db, [item])
    second = ingest_tweets_into_tracker(db, [item])

    assert first["ingested"] >= 1
    assert second["ingested"] == 0


def test_twitter_health_path_is_module_level_constant() -> None:
    assert isinstance(TWITTER_HEALTH_PATH, Path)
    assert str(TWITTER_HEALTH_PATH).endswith("twitter_ingestion_health.json")


# ──────────────────────────────────────────────────────────────────────────
# run_twitter_ingestion — health-file contract (74aeb5f pattern)
# ──────────────────────────────────────────────────────────────────────────


class _ScriptedResolver:
    """In-memory TwitterResolver double; emits a fixed item list per handle."""

    def __init__(self, handles: dict[str, list[TweetItem]]) -> None:
        self._handles_map = handles

    @property
    def handles(self) -> dict[str, str]:
        return {name: f"mem://{name}" for name in self._handles_map}

    # Parity alias with the real resolver.
    @property
    def outlets(self) -> dict[str, str]:
        return self.handles

    def fetch(self, handle: str, max_items: int | None = None) -> list[TweetItem]:
        items = self._handles_map.get(handle)
        if items is None:
            raise TwitterFetchError(handle, "unknown_handle", "unscripted handle")
        return list(items[: max_items or len(items)])


def test_run_twitter_ingestion_writes_health_file_ok(tmp_path: Path) -> None:
    tracker = SignalTrackerDB(db_path=tmp_path / "tracker.json")
    health_path = tmp_path / "twitter_health.json"
    resolver = _ScriptedResolver({
        "hankyung_news": [_make_item("삼성전자 HBM 수주 확대 반도체 매수세 $005930")],
        "mtnews_kr": [_make_item("SK하이닉스 HBM 수주 강세 반도체 $000660", handle="mtnews_kr")],
    })

    result = run_twitter_ingestion(
        handles=["hankyung_news", "mtnews_kr"],
        tracker_db_path=tmp_path / "tracker.json",
        health_path=health_path,
        resolver=resolver,
        tracker_db=tracker,
    )

    assert result["status"] == "ok"
    assert result["ingested"] >= 2
    state = json.loads(health_path.read_text(encoding="utf-8"))
    assert state["status"] == "ok"
    assert "last_success_at" in state
    assert state["ingested_count"] == result["ingested"]
    assert state["handles"] == ["hankyung_news", "mtnews_kr"]


def test_run_twitter_ingestion_marks_error_when_all_fetches_fail(tmp_path: Path) -> None:
    tracker = SignalTrackerDB(db_path=tmp_path / "tracker.json")
    health_path = tmp_path / "twitter_health.json"
    resolver = _ScriptedResolver({})  # nothing registered → every fetch raises

    result = run_twitter_ingestion(
        handles=["hankyung_news"],
        tracker_db_path=tmp_path / "tracker.json",
        health_path=health_path,
        resolver=resolver,
        tracker_db=tracker,
    )

    assert result["status"] == "error"
    state = json.loads(health_path.read_text(encoding="utf-8"))
    assert state["status"] == "error"
    assert state["error_count"] >= 1
    assert "last_error_at" in state


def test_run_twitter_ingestion_marks_degraded_when_some_handles_fail(tmp_path: Path) -> None:
    tracker = SignalTrackerDB(db_path=tmp_path / "tracker.json")
    health_path = tmp_path / "twitter_health.json"
    resolver = _ScriptedResolver({
        "hankyung_news": [_make_item("삼성전자 HBM 매수세 반도체 $005930")],
        # 'broken_handle' is not registered → fetch raises and reports the error
    })

    result = run_twitter_ingestion(
        handles=["hankyung_news", "broken_handle"],
        tracker_db_path=tmp_path / "tracker.json",
        health_path=health_path,
        resolver=resolver,
        tracker_db=tracker,
    )

    assert result["status"] == "degraded"
    state = json.loads(health_path.read_text(encoding="utf-8"))
    assert state["status"] == "degraded"
    assert state["last_success_at"] is not None
    assert state["error_count"] >= 1


def test_twitter_healthcheck_is_stale_when_last_success_is_24h_old(tmp_path: Path) -> None:
    state_path = tmp_path / "twitter_health.json"
    stale_iso = (
        datetime.now(timezone.utc).replace(microsecond=0) - timedelta(hours=24)
    ).isoformat()
    state_path.write_text(
        json.dumps({
            "status": "ok",
            "last_run_at": stale_iso,
            "last_success_at": stale_iso,
            "error_count": 0,
        }),
        encoding="utf-8",
    )

    summary = compute_health_summary(read_health_state(state_path), stale_threshold_hours=6.0)
    assert summary["is_stale"] is True
    assert summary["staleness_hours"] is not None and summary["staleness_hours"] >= 6.0


def test_twitter_healthcheck_not_stale_when_fresh(tmp_path: Path) -> None:
    state_path = tmp_path / "twitter_health.json"
    fresh_iso = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    state_path.write_text(
        json.dumps({
            "status": "ok",
            "last_run_at": fresh_iso,
            "last_success_at": fresh_iso,
            "error_count": 0,
        }),
        encoding="utf-8",
    )

    summary = compute_health_summary(read_health_state(state_path), stale_threshold_hours=6.0)
    assert summary["is_stale"] is False
