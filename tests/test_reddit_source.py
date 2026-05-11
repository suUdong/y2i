"""Tests for reddit_source — old.reddit.com JSON + rule sentiment."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from omx_brainstorm.healthcheck import compute_health_summary, read_health_state
from omx_brainstorm.reddit_source import (
    REDDIT_API_OPTION_ANALYSIS,
    REDDIT_HEALTH_PATH,
    SUBREDDIT_CURATION,
    RedditFetchError,
    RedditPost,
    RedditResolver,
    classify_reddit_post,
    extract_tickers,
    ingest_reddit_into_tracker,
    parse_listing_json,
    run_reddit_ingestion,
)
from omx_brainstorm.signal_tracker import SignalTrackerDB


_FIXTURE_LISTING = {
    "kind": "Listing",
    "data": {
        "children": [
            {
                "kind": "t3",
                "data": {
                    "id": "abc123",
                    "subreddit": "wallstreetbets",
                    "title": "$SMCI bullish breakout, buying calls before earnings",
                    "selftext": "AI servers to the moon. Long SMCI and AVGO.",
                    "permalink": "/r/wallstreetbets/comments/abc123/smci/",
                    "url": "https://old.reddit.com/r/wallstreetbets/comments/abc123/smci/",
                    "created_utc": 1778457600,
                    "score": 320,
                    "num_comments": 85,
                    "upvote_ratio": 0.82,
                    "link_flair_text": "DD",
                },
            },
            {
                "kind": "t3",
                "data": {
                    "id": "def456",
                    "subreddit": "investing",
                    "title": "Micron $MU looks expensive, trim before cycle rollover",
                    "selftext": "Bearish on memory margins.",
                    "permalink": "/r/investing/comments/def456/mu/",
                    "created_utc": 1778454000,
                    "score": 20,
                    "num_comments": 4,
                    "upvote_ratio": 0.7,
                },
            },
            {
                "kind": "t1",
                "data": {"body": "comment ignored"},
            },
            {
                "kind": "t3",
                "data": {"id": "empty", "subreddit": "stocks"},
            },
        ]
    },
}


def _make_post(title: str, selftext: str = "", subreddit: str = "stocks") -> RedditPost:
    return RedditPost(
        source_id=f"{subreddit}:{title}",
        subreddit=subreddit,
        title=title,
        selftext=selftext,
        url="https://old.reddit.com/r/stocks/comments/1/example/",
        permalink="https://old.reddit.com/r/stocks/comments/1/example/",
        published_at="2026-05-10T12:00:00+00:00",
        score=120,
        num_comments=30,
    )


def test_api_option_analysis_selects_old_reddit_json() -> None:
    assert REDDIT_API_OPTION_ANALYSIS["old_reddit_json"]["decision"] == "selected for pilot"
    assert REDDIT_API_OPTION_ANALYSIS["praw_oauth"]["decision"].startswith("defer")
    assert REDDIT_API_OPTION_ANALYSIS["pushshift_archive"]["decision"].startswith("defer")


def test_subreddit_curation_has_signal_and_noise_tiers() -> None:
    assert SUBREDDIT_CURATION["SecurityAnalysis"]["tier"] == "high_signal"
    assert SUBREDDIT_CURATION["wallstreetbets"]["tier"] == "noise_filter"
    assert {"stocks", "investing", "wallstreetbets"} <= set(SUBREDDIT_CURATION)


def test_parse_listing_json_extracts_reddit_posts() -> None:
    posts = parse_listing_json(_FIXTURE_LISTING, "wallstreetbets")

    assert len(posts) == 2
    first = posts[0]
    assert first.source_id == "wallstreetbets:abc123"
    assert first.subreddit == "wallstreetbets"
    assert "$SMCI bullish" in first.title
    assert first.permalink.startswith("https://old.reddit.com/r/wallstreetbets/")
    assert first.score == 320
    assert first.num_comments == 85
    assert first.upvote_ratio == 0.82
    assert first.raw_categories == ["DD"]
    parsed = datetime.fromisoformat(first.published_at)
    assert parsed.tzinfo is not None
    assert parsed.astimezone(timezone.utc).year == 2026


def test_parse_listing_json_raises_on_invalid_payload() -> None:
    with pytest.raises(RedditFetchError):
        parse_listing_json("{not-json", "stocks")


def test_resolver_fetch_uses_json_endpoint_and_visible_headers() -> None:
    resolver = RedditResolver(subreddits={"stocks": "https://old.reddit.com/r/stocks/hot.json"})
    captured: dict[str, str] = {}

    class _FakeResponse:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def read(self) -> bytes:
            return json.dumps(_FIXTURE_LISTING).encode("utf-8")

    def _fake_open(request, timeout):
        captured["url"] = request.full_url
        captured["ua"] = request.headers.get("User-agent", "")
        captured["accept"] = request.headers.get("Accept", "")
        captured["timeout"] = str(timeout)
        return _FakeResponse()

    with patch.object(resolver._opener, "open", side_effect=_fake_open):
        posts = resolver.fetch("stocks", max_items=5)

    assert len(posts) == 2
    assert captured["url"].startswith("https://old.reddit.com/r/stocks/hot.json?")
    assert "limit=5" in captured["url"]
    assert captured["accept"] == "application/json"
    assert "y2i-reddit-ingest" in captured["ua"]


def test_resolver_fetch_unknown_subreddit_raises() -> None:
    resolver = RedditResolver(subreddits={"stocks": "https://old.reddit.com/r/stocks/hot.json"})
    with pytest.raises(RedditFetchError) as ctx:
        resolver.fetch("unknown")
    assert ctx.value.status == "unknown_subreddit"


def test_extract_tickers_handles_cashtags_and_uppercase_body_ner() -> None:
    pairs = extract_tickers("$SMCI and AVGO bullish; also $005930 and Micron MU")
    tickers = {ticker for ticker, _company in pairs}
    assert {"SMCI", "AVGO", "MU", "005930.KS"} <= tickers


def test_classify_positive_reddit_post_passes_stock_gate() -> None:
    result = classify_reddit_post(_make_post("$SMCI bullish breakout", "buy calls to the moon"))
    assert result["should_analyze_stocks"] is True
    assert result["sentiment"] == "POSITIVE"
    assert result["video_signal_class"] == "ACTIONABLE"


def test_classify_negative_reddit_post_passes_stock_gate() -> None:
    result = classify_reddit_post(_make_post("$MU bearish warning", "sell or short before the crash", "investing"))
    assert result["should_analyze_stocks"] is True
    assert result["sentiment"] == "NEGATIVE"


def test_classify_noise_without_ticker_is_skipped() -> None:
    result = classify_reddit_post(_make_post("crypto politics and gambling", "bitcoin sportsbook thread"))
    assert result["should_analyze_stocks"] is False
    assert result["video_signal_class"] == "NON_EQUITY"


def test_ingest_reddit_into_tracker_writes_buy_and_sell_records(tmp_path: Path) -> None:
    db = SignalTrackerDB(tmp_path / "tracker.json")
    posts = [
        _make_post("$SMCI bullish breakout", "buy calls to the moon", "wallstreetbets"),
        _make_post("$MU bearish warning", "sell or short before crash", "investing"),
    ]

    summary = ingest_reddit_into_tracker(db, posts)

    assert summary["ingested"] == 2
    assert summary["by_subreddit"]["wallstreetbets"] == 1
    assert summary["by_sentiment"]["POSITIVE"] == 1
    verdicts = {record.ticker: record.verdict for record in db.records}
    assert verdicts["SMCI"] == "BUY"
    assert verdicts["MU"] == "SELL"
    assert {record.channel_slug for record in db.records} == {"reddit:wallstreetbets", "reddit:investing"}


def test_fixture_listing_flows_through_parse_classify_and_ingest(tmp_path: Path) -> None:
    payload = json.loads(
        Path("tests/fixtures/reddit_listing_wallstreetbets.json").read_text(encoding="utf-8")
    )
    db = SignalTrackerDB(tmp_path / "tracker.json")

    posts = parse_listing_json(payload, "wallstreetbets")
    summary = ingest_reddit_into_tracker(db, posts)

    assert summary["ingested"] == 3
    tickers = {record.ticker for record in db.records}
    assert {"SMCI", "AVGO", "MU"} <= tickers


def test_ingest_reddit_into_tracker_is_idempotent(tmp_path: Path) -> None:
    db = SignalTrackerDB(tmp_path / "tracker.json")
    post = _make_post("$AVGO bullish breakout", "buy long AVGO")
    first = ingest_reddit_into_tracker(db, [post])
    second = ingest_reddit_into_tracker(db, [post])
    assert first["ingested"] == 1
    assert second["ingested"] == 0


def test_reddit_health_path_is_module_level_constant() -> None:
    assert isinstance(REDDIT_HEALTH_PATH, Path)
    assert str(REDDIT_HEALTH_PATH).endswith("reddit_ingestion_health.json")


class _ScriptedResolver:
    def __init__(self, subreddits: dict[str, list[RedditPost] | Exception]) -> None:
        self._subreddits_map = subreddits

    @property
    def subreddits(self) -> dict[str, str]:
        return {name: f"mem://{name}" for name in self._subreddits_map}

    def fetch(self, subreddit: str, max_items: int | None = None) -> list[RedditPost]:
        value = self._subreddits_map.get(subreddit)
        if isinstance(value, Exception):
            raise value
        if value is None:
            raise RedditFetchError(subreddit, "unknown_subreddit", "unscripted subreddit")
        return list(value[: max_items or len(value)])


def test_run_reddit_ingestion_writes_health_file_ok(tmp_path: Path) -> None:
    tracker = SignalTrackerDB(tmp_path / "tracker.json")
    health_path = tmp_path / "reddit_health.json"
    resolver = _ScriptedResolver({
        "stocks": [_make_post("$SMCI bullish breakout", "buy calls to the moon", "stocks")],
        "investing": [_make_post("$MU bearish warning", "sell or short", "investing")],
    })

    result = run_reddit_ingestion(
        subreddits=["stocks", "investing"],
        tracker_db_path=tmp_path / "tracker.json",
        health_path=health_path,
        resolver=resolver,  # type: ignore[arg-type]
        tracker_db=tracker,
        sleep_seconds=0,
    )

    assert result["status"] == "ok"
    assert result["ingested"] == 2
    state = json.loads(health_path.read_text(encoding="utf-8"))
    assert state["status"] == "ok"
    assert state["subreddits"] == ["stocks", "investing"]
    assert state["ingested_count"] == 2


def test_run_reddit_ingestion_marks_degraded_on_partial_fetch_failure(tmp_path: Path) -> None:
    health_path = tmp_path / "reddit_health.json"
    resolver = _ScriptedResolver({
        "stocks": [_make_post("$SMCI bullish breakout", "buy calls to the moon", "stocks")],
        "wallstreetbets": RedditFetchError("wallstreetbets", 429, "Too Many Requests"),
    })

    result = run_reddit_ingestion(
        subreddits=["stocks", "wallstreetbets"],
        tracker_db_path=tmp_path / "tracker.json",
        health_path=health_path,
        resolver=resolver,  # type: ignore[arg-type]
        sleep_seconds=0,
    )

    assert result["status"] == "degraded"
    assert result["errors"][0]["status"] == "429"
    summary = compute_health_summary(read_health_state(health_path), stale_threshold_hours=6)
    assert summary["status"] == "degraded"


def test_run_reddit_ingestion_marks_error_when_all_fetches_fail(tmp_path: Path) -> None:
    health_path = tmp_path / "reddit_health.json"
    resolver = _ScriptedResolver({
        "stocks": RedditFetchError("stocks", 403, "Forbidden"),
    })

    result = run_reddit_ingestion(
        subreddits=["stocks"],
        tracker_db_path=tmp_path / "tracker.json",
        health_path=health_path,
        resolver=resolver,  # type: ignore[arg-type]
        sleep_seconds=0,
    )

    assert result["status"] == "error"
    state = read_health_state(health_path)
    assert state["error_count"] == 1
