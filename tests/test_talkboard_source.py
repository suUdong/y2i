"""Tests for talkboard_source.py — Naver board parser + classifier."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from omx_brainstorm.healthcheck import compute_health_summary, read_health_state
from omx_brainstorm.signal_tracker import SignalTrackerDB
from omx_brainstorm.talkboard_source import (
    SOURCE_SELECTION_ANALYSIS,
    TALKBOARD_HEALTH_PATH,
    TalkboardFetchError,
    TalkboardPost,
    TalkboardResolver,
    classify_talkboard_post,
    extract_kr_tickers,
    ingest_talkboard_into_tracker,
    parse_naver_board_html,
    run_talkboard_ingestion,
)


_FIXTURE_HTML = """
<html><body>
<table class="type2">
  <tr><th>날짜</th><th>제목</th><th>글쓴이</th><th>조회</th><th>추천</th><th>비추천</th></tr>
  <tr>
    <td class="date">2026.05.11 07:39</td>
    <td class="title"><a href="/item/board_read.naver?code=005930&amp;nid=111">삼성전자 005930 오늘 상승 돌파 매수</a></td>
    <td class="p11">달려라주식</td><td>120</td><td>8</td><td>1</td>
  </tr>
  <tr>
    <td class="date">2026.05.11 07:38</td>
    <td class="title"><a href="/item/board_read.naver?code=005930&amp;nid=112">클린봇이 이용자 보호를 위해 숨긴 게시글입니다.</a></td>
    <td>hidden</td><td>1</td><td>0</td><td>0</td>
  </tr>
  <tr>
    <td class="date">2026.05.11 07:37</td>
    <td class="title"><a href="/item/board_read.naver?code=005930&amp;nid=113">파란불 손절 매도 급락 조심</a></td>
    <td>겁쟁이</td><td>60</td><td>1</td><td>5</td>
  </tr>
</table>
종목 토론 게시판
</body></html>
"""


def _make_post(
    title: str,
    *,
    board: str = "naver:005930",
    stock_code: str = "005930",
    views: int | None = 80,
    upvotes: int | None = 4,
    downvotes: int | None = 0,
) -> TalkboardPost:
    return TalkboardPost(
        source_id=f"{board}:{title}",
        board=board,
        stock_code=stock_code,
        title=title,
        author="tester",
        url="https://finance.naver.com/item/board_read.naver",
        published_at="2026-05-10T22:39:00+00:00",
        views=views,
        upvotes=upvotes,
        downvotes=downvotes,
    )


def test_source_selection_analysis_selects_naver_and_defers_app_surfaces() -> None:
    assert SOURCE_SELECTION_ANALYSIS["naver_finance"]["decision"] == "selected"
    assert SOURCE_SELECTION_ANALYSIS["toss_securities"]["decision"] == "defer"
    assert SOURCE_SELECTION_ANALYSIS["kakaostock"]["decision"] == "defer"
    assert "free" in SOURCE_SELECTION_ANALYSIS["naver_finance"]["cost"]


def test_parse_naver_board_html_extracts_posts_and_metadata() -> None:
    posts = parse_naver_board_html(
        _FIXTURE_HTML,
        "naver:005930",
        now=datetime(2026, 5, 11, 8, 0, tzinfo=timezone(timedelta(hours=9))),
    )

    assert len(posts) == 2
    first = posts[0]
    assert first.source_id == "naver:005930:111"
    assert first.title == "삼성전자 005930 오늘 상승 돌파 매수"
    assert first.stock_code == "005930"
    assert first.author == "달려라주식"
    assert first.views == 120
    assert first.upvotes == 8
    assert first.downvotes == 1
    assert first.url.startswith("https://finance.naver.com/item/board_read.naver")
    assert first.published_at == "2026-05-10T22:39:00+00:00"


def test_parse_naver_board_html_raises_on_non_board_html() -> None:
    with pytest.raises(TalkboardFetchError):
        parse_naver_board_html("<html><title>blocked</title></html>", "naver:005930")


def test_resolver_fetch_raises_on_http_error() -> None:
    resolver = TalkboardResolver(boards={"naver:005930": "https://finance.naver.com/item/board.naver?code=005930"})

    class _FakeResponse:
        status = 500

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def read(self) -> bytes:
            return b""

    with patch.object(resolver._opener, "open", return_value=_FakeResponse()):
        with pytest.raises(TalkboardFetchError) as ctx:
            resolver.fetch("naver:005930")
        assert ctx.value.status == 500


def test_resolver_fetch_unknown_board_raises() -> None:
    resolver = TalkboardResolver(boards={"naver:005930": "https://finance.naver.com/item/board.naver?code=005930"})
    with pytest.raises(TalkboardFetchError) as ctx:
        resolver.fetch("naver:999999")
    assert ctx.value.status == "unknown_board"


def test_resolver_rejects_non_naver_board_urls() -> None:
    with pytest.raises(TalkboardFetchError) as ctx:
        TalkboardResolver(boards={"other:005930": "https://example.com/board?code=005930"})
    assert ctx.value.status == "invalid_board"


def test_run_talkboard_ingestion_rejects_paginated_naver_board_url(tmp_path: Path) -> None:
    for url in (
        "https://finance.naver.com/item/board.naver?code=005930&page=2",
        "https://finance.naver.com/item/board.naver?code=005930&page=",
    ):
        with pytest.raises(TalkboardFetchError) as ctx:
            run_talkboard_ingestion(
                boards=[url],
                tracker_db_path=tmp_path / "tracker.json",
                health_path=tmp_path / "health.json",
                sleep_seconds=0,
            )
        assert ctx.value.status == "disallowed_board"


def test_resolver_fetch_returns_parsed_items_on_success() -> None:
    resolver = TalkboardResolver(
        boards={"naver:005930": "https://finance.naver.com/item/board.naver?code=005930"},
        sleep_seconds=0,
    )

    class _FakeResponse:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def read(self) -> bytes:
            return _FIXTURE_HTML.encode("euc-kr")

    with patch.object(resolver._opener, "open", return_value=_FakeResponse()):
        posts = resolver.fetch("naver:005930", max_items=10)

    assert len(posts) == 2
    assert posts[0].board == "naver:005930"


def test_classify_positive_talkboard_post_passes_gate() -> None:
    result = classify_talkboard_post(_make_post("삼성전자 005930 상승 돌파 매수"))
    assert result["should_analyze"] is True
    assert result["sentiment"] == "POSITIVE"
    assert result["signal_score"] >= 68.0


def test_classify_negative_talkboard_post_passes_gate() -> None:
    result = classify_talkboard_post(_make_post("삼성전자 005930 급락 손절 매도"))
    assert result["should_analyze"] is True
    assert result["sentiment"] == "NEGATIVE"
    assert result["signal_score"] >= 68.0


def test_classify_noise_without_sentiment_is_skipped() -> None:
    result = classify_talkboard_post(_make_post("비트코인 정치 이야기", views=1, upvotes=0))
    assert result["should_analyze"] is False
    assert result["video_signal_class"] == "NON_EQUITY"


def test_extract_kr_tickers_uses_board_code_code_mentions_and_names() -> None:
    pairs = extract_kr_tickers("하이닉스와 $005930 그리고 000660", board_stock_code="035420")
    tickers = {ticker for ticker, _name in pairs}
    assert {"035420.KS", "005930.KS", "000660.KS"} <= tickers
    assert all(t.endswith((".KS", ".KQ")) for t in tickers)


def test_ingest_talkboard_into_tracker_writes_buy_and_sell_records(tmp_path: Path) -> None:
    db = SignalTrackerDB(tmp_path / "tracker.json")
    posts = [
        _make_post("삼성전자 상승 돌파 매수"),
        _make_post("삼성전자 급락 손절 매도", upvotes=5, downvotes=0),
    ]

    summary = ingest_talkboard_into_tracker(db, posts)

    assert summary["ingested"] == 2
    assert summary["by_board"]["naver:005930"] == 2
    verdicts = {record.verdict for record in db.records}
    assert {"BUY", "SELL"} <= verdicts
    assert {record.channel_slug for record in db.records} == {"talkboard:naver:005930"}


def test_ingest_talkboard_into_tracker_is_idempotent(tmp_path: Path) -> None:
    db = SignalTrackerDB(tmp_path / "tracker.json")
    post = _make_post("삼성전자 상승 돌파 매수")
    first = ingest_talkboard_into_tracker(db, [post])
    second = ingest_talkboard_into_tracker(db, [post])

    assert first["ingested"] == 1
    assert second["ingested"] == 0


def test_talkboard_health_path_is_module_level_constant() -> None:
    assert isinstance(TALKBOARD_HEALTH_PATH, Path)
    assert str(TALKBOARD_HEALTH_PATH).endswith("talkboard_ingestion_health.json")


class _ScriptedResolver:
    def __init__(self, boards: dict[str, list[TalkboardPost]]) -> None:
        self._boards_map = boards

    @property
    def boards(self) -> dict[str, str]:
        return {name: f"mem://{name}" for name in self._boards_map}

    def fetch(self, board: str, max_items: int | None = None) -> list[TalkboardPost]:
        posts = self._boards_map.get(board)
        if posts is None:
            raise TalkboardFetchError(board, "unknown_board", "unscripted board")
        return list(posts[: max_items or len(posts)])


def test_run_talkboard_ingestion_writes_health_file_ok(tmp_path: Path) -> None:
    tracker = SignalTrackerDB(tmp_path / "tracker.json")
    health_path = tmp_path / "talkboard_health.json"
    resolver = _ScriptedResolver({
        "naver:005930": [_make_post("삼성전자 상승 돌파 매수")],
        "naver:000660": [_make_post("하이닉스 상승 돌파 매수", board="naver:000660", stock_code="000660")],
    })

    result = run_talkboard_ingestion(
        boards=["naver:005930", "naver:000660"],
        tracker_db_path=tmp_path / "tracker.json",
        health_path=health_path,
        resolver=resolver,
        tracker_db=tracker,
        sleep_seconds=0,
    )

    assert result["status"] == "ok"
    assert result["ingested"] == 2
    state = json.loads(health_path.read_text(encoding="utf-8"))
    assert state["status"] == "ok"
    assert state["boards"] == ["naver:005930", "naver:000660"]
    assert state["ingested_count"] == 2


def test_run_talkboard_ingestion_marks_error_when_all_fetches_fail(tmp_path: Path) -> None:
    tracker = SignalTrackerDB(tmp_path / "tracker.json")
    health_path = tmp_path / "talkboard_health.json"
    resolver = _ScriptedResolver({})

    result = run_talkboard_ingestion(
        boards=["naver:005930"],
        tracker_db_path=tmp_path / "tracker.json",
        health_path=health_path,
        resolver=resolver,
        tracker_db=tracker,
        sleep_seconds=0,
    )

    assert result["status"] == "error"
    state = json.loads(health_path.read_text(encoding="utf-8"))
    assert state["status"] == "error"
    assert state["error_count"] >= 1
    assert "last_error_at" in state


def test_run_talkboard_ingestion_marks_degraded_when_some_boards_fail(tmp_path: Path) -> None:
    tracker = SignalTrackerDB(tmp_path / "tracker.json")
    health_path = tmp_path / "talkboard_health.json"
    resolver = _ScriptedResolver({
        "naver:005930": [_make_post("삼성전자 상승 돌파 매수")],
    })

    result = run_talkboard_ingestion(
        boards=["naver:005930", "naver:000660"],
        tracker_db_path=tmp_path / "tracker.json",
        health_path=health_path,
        resolver=resolver,
        tracker_db=tracker,
        sleep_seconds=0,
    )

    assert result["status"] == "degraded"
    state = json.loads(health_path.read_text(encoding="utf-8"))
    assert state["status"] == "degraded"
    assert state["errors"][0]["board"] == "naver:000660"


def test_talkboard_health_summary_marks_stale(tmp_path: Path) -> None:
    stale_at = (datetime.now(timezone.utc) - timedelta(hours=8)).isoformat()
    path = tmp_path / "talkboard_health.json"
    path.write_text(json.dumps({"status": "ok", "last_success_at": stale_at}), encoding="utf-8")

    summary = compute_health_summary(read_health_state(path), stale_threshold_hours=6.0)

    assert summary["is_stale"] is True
    assert summary["stale_threshold_hours"] == 6.0
