"""Tests for src/omx_brainstorm/news_source.py — RSS resolver + classifier."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from omx_brainstorm.news_source import (
    NEWS_HEALTH_PATH,
    NewsFetchError,
    NewsItem,
    NewsResolver,
    classify_news_item,
    extract_kr_tickers,
    ingest_news_into_tracker,
    parse_rss_xml,
)
from omx_brainstorm.signal_tracker import SignalTrackerDB


# ──────────────────────────────────────────────────────────────────────────
# RSS parsing
# ──────────────────────────────────────────────────────────────────────────


_FIXTURE_RSS = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>한경 증권</title>
    <link>https://www.hankyung.com/feed/finance</link>
    <description>증권 뉴스</description>
    <item>
      <title>삼성전자 HBM4 수주 확대 &amp; 외인 매수세</title>
      <link>https://example.com/article-1</link>
      <pubDate>Mon, 11 May 2026 03:14:00 +0900</pubDate>
      <guid>article-1</guid>
      <description>&lt;p&gt;HBM 공급 확대 기대&lt;/p&gt;</description>
      <category>반도체</category>
    </item>
    <item>
      <title>아침 시황 메모: 코스피 보합</title>
      <link>https://example.com/article-2</link>
      <guid>article-2</guid>
      <description>코스피 시황</description>
    </item>
    <item>
      <link>https://example.com/empty</link>
    </item>
  </channel>
</rss>
"""


def test_parse_rss_xml_parses_items_and_decodes_entities() -> None:
    items = parse_rss_xml(_FIXTURE_RSS, outlet="hankyung")

    assert len(items) == 2
    first, second = items
    assert first.title == "삼성전자 HBM4 수주 확대 & 외인 매수세"
    assert first.outlet == "hankyung"
    assert first.url == "https://example.com/article-1"
    assert first.source_id == "hankyung:article-1"
    assert first.published_at.endswith("+00:00")
    parsed_dt = datetime.fromisoformat(first.published_at)
    assert parsed_dt.tzinfo is not None
    assert parsed_dt.astimezone(timezone.utc).hour == 18  # 03:14 KST → 18:14 UTC prior day
    assert first.raw_categories == ["반도체"]

    # Missing pubDate -> still parses, summary stripped of HTML
    assert second.title.startswith("아침 시황 메모")
    assert second.published_at.endswith("+00:00")


def test_parse_rss_xml_raises_on_invalid_xml() -> None:
    with pytest.raises(NewsFetchError):
        parse_rss_xml("<not-xml", outlet="hankyung")


def test_resolver_fetch_raises_on_http_error() -> None:
    resolver = NewsResolver(outlets={"hankyung": "https://example.com/feed"})

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
        with pytest.raises(NewsFetchError) as ctx:
            resolver.fetch("hankyung")
        assert ctx.value.outlet == "hankyung"
        assert ctx.value.status == 500


def test_resolver_fetch_unknown_outlet_raises() -> None:
    resolver = NewsResolver(outlets={"hankyung": "https://example.com/feed"})
    with pytest.raises(NewsFetchError) as ctx:
        resolver.fetch("unknown_outlet_xyz")
    assert ctx.value.status == "unknown_outlet"


def test_resolver_fetch_returns_parsed_items_on_success() -> None:
    resolver = NewsResolver(outlets={"hankyung": "https://example.com/feed"})

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
        items = resolver.fetch("hankyung", max_items=10)

    assert len(items) == 2
    assert items[0].outlet == "hankyung"


# ──────────────────────────────────────────────────────────────────────────
# Classification
# ──────────────────────────────────────────────────────────────────────────


def _make_item(title: str, summary: str = "", outlet: str = "hankyung") -> NewsItem:
    return NewsItem(
        source_id=f"{outlet}:{title}",
        outlet=outlet,
        title=title,
        summary=summary,
        url="",
        published_at="2026-05-11T03:14:00+00:00",
        raw_categories=[],
    )


def test_classify_actionable_item_with_known_ticker_passes_gate() -> None:
    item = _make_item("삼성전자 HBM 수주 확대, 반도체 슈퍼사이클 재점화")
    result = classify_news_item(item)
    assert result["should_analyze"] is True
    assert result["signal_score"] >= 60.0
    assert result["video_signal_class"] in {"ACTIONABLE", "SECTOR_ONLY"}


def test_classify_non_equity_item_is_skipped() -> None:
    item = _make_item("주말 먹방 일상 브이로그")
    result = classify_news_item(item)
    assert result["should_analyze"] is False
    assert result["video_signal_class"] == "NON_EQUITY"


def test_classify_low_signal_item_is_skipped() -> None:
    item = _make_item("오늘의 날씨 정보")
    result = classify_news_item(item)
    assert result["should_analyze"] is False
    assert result["video_signal_class"] == "LOW_SIGNAL"


def test_extract_kr_tickers_returns_kospi_kosdaq_only() -> None:
    pairs = extract_kr_tickers("삼성전자 강세, 엔비디아 동조 — 하이닉스 매수세")
    tickers = {ticker for ticker, _name in pairs}
    assert "005930.KS" in tickers
    assert "000660.KS" in tickers
    # Foreign ticker mapped from "엔비디아" => NVDA is not .KS/.KQ and must be excluded.
    assert all(t.endswith((".KS", ".KQ")) for t in tickers)


# ──────────────────────────────────────────────────────────────────────────
# Tracker ingestion + idempotency
# ──────────────────────────────────────────────────────────────────────────


def test_ingest_news_into_tracker_writes_signal_record(tmp_path: Path) -> None:
    db = SignalTrackerDB(db_path=tmp_path / "tracker.json")
    item = _make_item(
        "삼성전자 HBM4 수주 확대로 반도체 매수세 — 외국인 사들였다",
        summary="삼성전자 실적 기대",
    )

    summary = ingest_news_into_tracker(db, [item])

    assert summary["ingested"] >= 1
    assert summary["by_outlet"].get("hankyung", 0) >= 1
    tickers = {r.ticker for r in db.records}
    assert "005930.KS" in tickers
    slugs = {r.channel_slug for r in db.records}
    assert "news:hankyung" in slugs


def test_ingest_news_into_tracker_is_idempotent(tmp_path: Path) -> None:
    db = SignalTrackerDB(db_path=tmp_path / "tracker.json")
    item = _make_item("삼성전자 HBM 수주 확대 반도체")
    first = ingest_news_into_tracker(db, [item])
    second = ingest_news_into_tracker(db, [item])

    assert first["ingested"] >= 1
    assert second["ingested"] == 0


def test_news_health_path_is_module_level_constant() -> None:
    assert isinstance(NEWS_HEALTH_PATH, Path)
    assert str(NEWS_HEALTH_PATH).endswith("news_ingestion_health.json")
