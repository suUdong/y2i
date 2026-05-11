"""Reddit ingestion for y2i English retail sentiment.

This is the fifth y2i source class, separate from YouTube, News RSS,
Twitter/X, and Korean talkboards. It uses Reddit's public old.reddit.com
listing JSON endpoint with stdlib urllib/json only:

    https://old.reddit.com/r/<subreddit>/hot.json?limit=<n>

No OAuth token, PRAW dependency, Pushshift archive, browser automation, or LLM
classification is required. Operators remain responsible for obeying Reddit
terms, using a visible user-agent, and keeping low request volume.
"""

from __future__ import annotations

import html
import json
import logging
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

from ..signal_gate import NON_EQUITY_KEYWORDS
from ..signal_tracker import DEFAULT_DB_PATH, SignalRecord, SignalTrackerDB
from ..stock_registry import COMPANY_MAP, resolve_kr_ticker
from ..utils import read_json, write_json

logger = logging.getLogger(__name__)

REDDIT_HEALTH_PATH = Path(".omx/state/reddit_ingestion_health.json")
DEFAULT_REDDIT_BASE = "https://old.reddit.com"
_DEFAULT_TIMEOUT_SECONDS = 12.0
_DEFAULT_SLEEP_SECONDS = 1.5
_DEFAULT_USER_AGENT = "y2i-reddit-ingest/1.0 (+https://github.com/y2i; contact: local-operator)"

REDDIT_API_OPTION_ANALYSIS: dict[str, dict[str, str]] = {
    "old_reddit_json": {
        "cost": "free, no API key",
        "dependency": "stdlib urllib/json",
        "coverage": "current subreddit listing pages only; no deep archive",
        "rate_limit": "must use visible User-Agent, low request volume, and tolerate 429/403",
        "decision": "selected for pilot",
    },
    "praw_oauth": {
        "cost": "free client registration but requires OAuth app credentials",
        "dependency": "third-party PRAW package",
        "coverage": "official API semantics and better pagination when authenticated",
        "rate_limit": "OAuth/API quotas apply; credentials must not be committed",
        "decision": "defer until stdlib endpoint is insufficient",
    },
    "pushshift_archive": {
        "cost": "external archive availability varies",
        "dependency": "external archive API/service",
        "coverage": "historical search when available, weaker for fresh production ingestion",
        "rate_limit": "service availability and terms are separate from Reddit",
        "decision": "defer; archive research only, not live source of truth",
    },
}

SUBREDDIT_CURATION: dict[str, dict[str, str]] = {
    "wallstreetbets": {
        "tier": "noise_filter",
        "rationale": "High retail velocity and meme/YOLO flow; require ticker and sentiment anchors.",
    },
    "stocks": {
        "tier": "high_signal",
        "rationale": "Broad equity discussion with lower meme density than WSB.",
    },
    "investing": {
        "tier": "high_signal",
        "rationale": "Longer horizon macro/equity discussion; useful for sector and quality framing.",
    },
    "SecurityAnalysis": {
        "tier": "high_signal",
        "rationale": "Lower volume but deeper fundamental writeups.",
    },
    "StockMarket": {
        "tier": "medium",
        "rationale": "Market-wide discussion; useful when ticker mentions are explicit.",
    },
    "options": {
        "tier": "noise_filter",
        "rationale": "Options positioning can lead price action but is ticker/noise heavy.",
    },
}

REDDIT_SUBREDDITS: dict[str, str] = {
    name: f"{DEFAULT_REDDIT_BASE}/r/{name}/hot.json"
    for name in SUBREDDIT_CURATION
}

_SUBREDDIT_TIER_BASE_SCORE = {
    "high_signal": 46.0,
    "medium": 42.0,
    "noise_filter": 36.0,
}
_MIN_SIGNAL_SCORE = 58.0
_BUY_VERDICT_SCORE = 68.0
_SELL_VERDICT_SCORE = 68.0

_CASHTAG_RE = re.compile(r"(?<![A-Za-z0-9_])\$([A-Za-z]{1,8}|[0-9]{6})(?![A-Za-z0-9_])")
_TOKEN_RE = re.compile(r"\b[A-Za-z][A-Za-z0-9.\-]{0,9}\b|[0-9]{6}|[가-힣A-Za-z0-9&]+")
_HTML_TAG_RE = re.compile(r"<[^>]+>")
_SUBREDDIT_RE = re.compile(r"^[A-Za-z0-9_]{2,32}$")

_COMMON_UPPERCASE_WORDS = {
    "A", "AI", "AM", "AN", "ARE", "ATH", "CEO", "CFO", "DD", "DCA", "EPS", "ETF",
    "EV", "FDA", "FOMC", "GDP", "IPO", "IR", "IT", "MOON", "NAV", "NYSE", "OTM",
    "PE", "PUT", "QQQ", "SEC", "SPY", "THE", "US", "USD", "YOLO",
}

_EXTRA_US_TICKERS: dict[str, str] = {
    "SMCI": "Super Micro Computer",
    "MU": "Micron",
    "AVGO": "Broadcom",
    "NVDA": "NVIDIA",
    "AMD": "AMD",
    "TSM": "TSMC",
    "TSLA": "Tesla",
    "AAPL": "Apple",
    "MSFT": "Microsoft",
    "AMZN": "Amazon",
    "GOOGL": "Alphabet",
    "META": "Meta Platforms",
    "PLTR": "Palantir",
    "ASML": "ASML",
    "MRVL": "Marvell",
    "INTC": "Intel",
    "ARM": "ARM Holdings",
}

_POSITIVE_KEYWORDS = {
    "accumulate", "beat", "beats", "breakout", "bull", "bullish", "buy", "calls",
    "cheap", "conviction", "long", "moon", "mooning", "outperform", "rebound",
    "rip", "ripping", "rocket", "squeeze", "upside",
}
_NEGATIVE_KEYWORDS = {
    "avoid", "bagholder", "bear", "bearish", "crash", "downgrade", "dump",
    "puts", "recession", "red flag", "rug", "sell", "short", "shorting",
    "tank", "tanking", "trim", "warning",
}
_NOISE_KEYWORDS = {
    "crypto", "bitcoin", "ethereum", "doge", "sportsbook", "gambling", "politics",
    "election", "meme only", "shitpost",
}


def _build_numeric_ticker_index() -> dict[str, tuple[str, str]]:
    index: dict[str, tuple[str, str]] = {}
    for ticker, english in COMPANY_MAP.values():
        if ticker.endswith((".KS", ".KQ")):
            numeric = ticker.split(".", 1)[0]
            index.setdefault(numeric, (ticker, english))
    return index


def _build_alpha_ticker_index() -> dict[str, tuple[str, str]]:
    index: dict[str, tuple[str, str]] = {
        ticker: (ticker, name) for ticker, name in _EXTRA_US_TICKERS.items()
    }
    for ticker, english in COMPANY_MAP.values():
        if ticker.endswith((".KS", ".KQ")):
            continue
        if ticker.isalpha() and len(ticker) >= 2:
            index.setdefault(ticker.upper(), (ticker.upper(), english))
    return index


_NUMERIC_TICKER_INDEX = _build_numeric_ticker_index()
_ALPHA_TICKER_INDEX = _build_alpha_ticker_index()


class RedditFetchError(RuntimeError):
    """Raised when a subreddit fetch or parse fails."""

    def __init__(self, subreddit: str, status: int | str, message: str = "") -> None:
        self.subreddit = subreddit
        self.status = status
        suffix = f": {message}" if message else ""
        super().__init__(f"reddit fetch failed for {subreddit} (status={status}){suffix}")


@dataclass(slots=True)
class RedditPost:
    """One Reddit link/self post from a subreddit listing."""

    source_id: str
    subreddit: str
    title: str
    selftext: str
    url: str
    permalink: str
    published_at: str
    score: int = 0
    num_comments: int = 0
    upvote_ratio: float | None = None
    flair: str = ""
    raw_categories: list[str] = field(default_factory=list)

    @property
    def summary(self) -> str:
        return self.selftext

    def to_dict(self) -> dict:
        return asdict(self)


def _strip_html(value: str) -> str:
    text = _HTML_TAG_RE.sub(" ", value or "")
    return " ".join(html.unescape(text).split())


def _coerce_created_iso(value: Any, *, now: datetime | None = None) -> str:
    try:
        created = float(value)
    except (TypeError, ValueError):
        return (now or datetime.now(timezone.utc)).isoformat()
    return datetime.fromtimestamp(created, tz=timezone.utc).isoformat()


def _subreddit_tier(subreddit: str) -> str:
    meta = SUBREDDIT_CURATION.get(subreddit) or SUBREDDIT_CURATION.get(subreddit.strip("/").lower())
    if meta:
        return str(meta.get("tier") or "medium")
    return "medium"


def _normalize_subreddit_entry(raw: str) -> tuple[str, str]:
    text = raw.strip()
    if not text:
        raise RedditFetchError(text, "invalid_subreddit", "empty subreddit")
    if text.startswith(("http://", "https://")):
        parsed = urllib.parse.urlparse(text)
        parts = [part for part in parsed.path.split("/") if part]
        if parsed.netloc != "old.reddit.com" or len(parts) < 2 or parts[0] != "r":
            raise RedditFetchError(text, "invalid_subreddit", "only old.reddit.com/r/<sub> listing URLs are supported")
        subreddit = parts[1]
        if not _SUBREDDIT_RE.fullmatch(subreddit):
            raise RedditFetchError(text, "invalid_subreddit", "bad subreddit name")
        return subreddit, f"{DEFAULT_REDDIT_BASE}/r/{subreddit}/hot.json"
    subreddit = text[2:] if text.lower().startswith("r/") else text
    if not _SUBREDDIT_RE.fullmatch(subreddit):
        raise RedditFetchError(text, "invalid_subreddit", "bad subreddit name")
    return subreddit, REDDIT_SUBREDDITS.get(subreddit, f"{DEFAULT_REDDIT_BASE}/r/{subreddit}/hot.json")


def _decode_listing_payload(raw: bytes | str, subreddit: str) -> dict[str, Any]:
    text = raw.decode("utf-8", errors="replace") if isinstance(raw, (bytes, bytearray)) else str(raw)
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise RedditFetchError(subreddit, "parse_error", str(exc)) from exc
    if not isinstance(payload, dict) or not isinstance(payload.get("data"), dict):
        raise RedditFetchError(subreddit, "parse_error", "missing listing data")
    return payload


def parse_listing_json(
    payload: dict[str, Any] | bytes | str,
    subreddit: str,
    *,
    max_items: int | None = None,
) -> list[RedditPost]:
    """Parse old.reddit.com listing JSON into RedditPost objects."""
    if not isinstance(payload, dict):
        payload = _decode_listing_payload(payload, subreddit)
    listing = payload.get("data", {})
    children = listing.get("children", []) if isinstance(listing, dict) else []
    posts: list[RedditPost] = []
    if not isinstance(children, list):
        raise RedditFetchError(subreddit, "parse_error", "listing children is not a list")
    for child in children:
        if not isinstance(child, dict) or child.get("kind") != "t3":
            continue
        data = child.get("data")
        if not isinstance(data, dict):
            continue
        title = _strip_html(str(data.get("title") or ""))
        selftext = _strip_html(str(data.get("selftext") or data.get("selftext_html") or ""))
        if not title and not selftext:
            continue
        permalink = str(data.get("permalink") or "")
        full_permalink = urllib.parse.urljoin(DEFAULT_REDDIT_BASE, permalink) if permalink else ""
        post_id = str(data.get("id") or data.get("name") or full_permalink or title)
        flair = _strip_html(str(data.get("link_flair_text") or ""))
        raw_subreddit = str(data.get("subreddit") or subreddit)
        posts.append(
            RedditPost(
                source_id=f"{raw_subreddit}:{post_id}"[:512],
                subreddit=raw_subreddit,
                title=title or selftext[:240],
                selftext=selftext,
                url=str(data.get("url") or full_permalink),
                permalink=full_permalink,
                published_at=_coerce_created_iso(data.get("created_utc")),
                score=int(data.get("score") or 0),
                num_comments=int(data.get("num_comments") or 0),
                upvote_ratio=float(data["upvote_ratio"]) if data.get("upvote_ratio") is not None else None,
                flair=flair,
                raw_categories=[flair] if flair else [],
            )
        )
        if max_items is not None and len(posts) >= max_items:
            break
    return posts


class RedditResolver:
    """Fetch public old.reddit.com listing JSON for registered subreddits."""

    def __init__(
        self,
        subreddits: dict[str, str] | None = None,
        *,
        http_proxy_url: str | None = None,
        https_proxy_url: str | None = None,
        timeout_seconds: float = _DEFAULT_TIMEOUT_SECONDS,
        user_agent: str = _DEFAULT_USER_AGENT,
        sleep_seconds: float = _DEFAULT_SLEEP_SECONDS,
        sort: str = "hot",
    ) -> None:
        self._subreddits = dict(subreddits or REDDIT_SUBREDDITS)
        self._timeout = float(timeout_seconds)
        self._user_agent = user_agent
        self._sleep_seconds = max(0.0, float(sleep_seconds))
        self._sort = sort if sort in {"hot", "new", "top"} else "hot"
        proxies: dict[str, str] = {}
        if http_proxy_url:
            proxies["http"] = http_proxy_url
        if https_proxy_url:
            proxies["https"] = https_proxy_url
        if proxies:
            self._opener = urllib.request.build_opener(urllib.request.ProxyHandler(proxies))
        else:
            self._opener = urllib.request.build_opener()

    @property
    def subreddits(self) -> dict[str, str]:
        return dict(self._subreddits)

    @property
    def outlets(self) -> dict[str, str]:
        return self.subreddits

    def fetch(self, subreddit: str, max_items: int | None = 30) -> list[RedditPost]:
        if subreddit not in self._subreddits:
            raise RedditFetchError(subreddit, "unknown_subreddit", f"subreddit not registered: {subreddit}")
        base_url = self._subreddits[subreddit]
        parsed = urllib.parse.urlparse(base_url)
        path = parsed.path
        if not path.endswith(".json"):
            path = path.rstrip("/") + f"/{self._sort}.json"
        query = dict(urllib.parse.parse_qsl(parsed.query, keep_blank_values=True))
        if max_items is not None:
            query["limit"] = str(max(1, min(int(max_items), 100)))
        url = urllib.parse.urlunparse(parsed._replace(path=path, query=urllib.parse.urlencode(query)))
        request = urllib.request.Request(
            url,
            headers={
                "User-Agent": self._user_agent,
                "Accept": "application/json",
            },
        )
        try:
            with self._opener.open(request, timeout=self._timeout) as resp:
                status = getattr(resp, "status", 200)
                if status != 200:
                    raise RedditFetchError(subreddit, status)
                body = resp.read()
        except urllib.error.HTTPError as exc:
            raise RedditFetchError(subreddit, exc.code, exc.reason or "") from exc
        except urllib.error.URLError as exc:
            raise RedditFetchError(subreddit, "url_error", str(exc.reason)) from exc
        return parse_listing_json(body, subreddit, max_items=max_items)

    def fetch_all(
        self,
        subreddits: Iterable[str] | None = None,
        max_items_per_subreddit: int = 30,
    ) -> list[RedditPost]:
        names = list(subreddits) if subreddits is not None else list(self._subreddits)
        collected: list[RedditPost] = []
        for idx, name in enumerate(names):
            try:
                collected.extend(self.fetch(name, max_items=max_items_per_subreddit))
            except RedditFetchError as exc:
                logger.warning("reddit fetch failed for %s: %s", name, exc)
            if idx < len(names) - 1 and self._sleep_seconds > 0:
                time.sleep(self._sleep_seconds)
        return collected


def _build_resolver_subreddits(
    subreddits: Sequence[str] | None,
) -> tuple[list[str] | None, dict[str, str] | None]:
    if subreddits is None:
        return None, None
    names: list[str] = []
    urls: dict[str, str] = {}
    for raw in subreddits:
        name, url = _normalize_subreddit_entry(raw)
        names.append(name)
        urls[name] = url
    return names, urls


def _lowercase_tokens(text: str) -> list[str]:
    return [token.lower() for token in _TOKEN_RE.findall(text or "")]


def _keyword_hits(text: str, keywords: set[str]) -> set[str]:
    lowered = f" {text.lower()} "
    token_set = set(_lowercase_tokens(text))
    hits: set[str] = set()
    for keyword in keywords:
        if " " in keyword:
            if f" {keyword.lower()} " in lowered:
                hits.add(keyword)
        elif keyword.lower() in token_set:
            hits.add(keyword)
    return hits


def extract_cashtag_tickers(text: str) -> list[tuple[str, str]]:
    """Pull known US/KR tickers from cashtags such as $SMCI or $005930."""
    found: dict[str, str] = {}
    for raw in _CASHTAG_RE.findall(text or ""):
        symbol = raw.upper()
        mapping: tuple[str, str] | None = None
        if symbol.isdigit():
            mapping = _NUMERIC_TICKER_INDEX.get(symbol)
        elif len(symbol) >= 2:
            mapping = _ALPHA_TICKER_INDEX.get(symbol)
        if mapping:
            found.setdefault(mapping[0], mapping[1])
    return list(found.items())


def extract_tickers(text: str) -> list[tuple[str, str]]:
    """Return unique (ticker, company) pairs from cashtags and simple token NER."""
    seen: dict[str, str] = {}
    for ticker, company in extract_cashtag_tickers(text):
        seen.setdefault(ticker, company)

    for raw in _TOKEN_RE.findall(text or ""):
        token = raw.strip()
        if not token:
            continue
        mapping: tuple[str, str] | None = None
        if token.isdigit() and len(token) == 6:
            mapping = _NUMERIC_TICKER_INDEX.get(token)
        elif token.isupper() and 2 <= len(token) <= 5 and token not in _COMMON_UPPERCASE_WORDS:
            mapping = _ALPHA_TICKER_INDEX.get(token)
        if mapping is None:
            lower = token.lower()
            mapping = COMPANY_MAP.get(lower)
            if mapping is None and len(lower) >= 2:
                mapping = resolve_kr_ticker(lower)
        if mapping:
            seen.setdefault(mapping[0], mapping[1])
    return list(seen.items())


def classify_reddit_post(post: RedditPost) -> dict[str, Any]:
    """Classify a post with deterministic ticker and sentiment rules."""
    text = f"{post.title} {post.selftext} {post.flair}".strip()
    token_set = set(_lowercase_tokens(text))
    non_equity_hits = (NON_EQUITY_KEYWORDS & token_set) | _keyword_hits(text, _NOISE_KEYWORDS)
    positive_hits = _keyword_hits(text, _POSITIVE_KEYWORDS)
    negative_hits = _keyword_hits(text, _NEGATIVE_KEYWORDS)
    tickers = extract_tickers(text)

    if non_equity_hits and not tickers:
        return {
            "signal_score": 25.0,
            "video_signal_class": "NON_EQUITY",
            "should_analyze": False,
            "should_analyze_stocks": False,
            "skip_reason": f"non-equity keyword: {sorted(non_equity_hits)[0]}",
            "sentiment": "NEUTRAL",
            "positive_hits": [],
            "negative_hits": [],
        }

    if positive_hits and len(positive_hits) >= len(negative_hits):
        sentiment = "POSITIVE"
    elif negative_hits:
        sentiment = "NEGATIVE"
    else:
        sentiment = "NEUTRAL"

    tier = _subreddit_tier(post.subreddit)
    score = _SUBREDDIT_TIER_BASE_SCORE.get(tier, 42.0)
    if tickers:
        score += 18.0
    if positive_hits or negative_hits:
        score += min(18.0, 8.0 * len(positive_hits | negative_hits))
    if post.score > 0:
        score += min(7.0, post.score / 150.0)
    if post.num_comments > 0:
        score += min(6.0, post.num_comments / 80.0)
    if tier == "noise_filter" and not (tickers and (positive_hits or negative_hits)):
        score -= 14.0
    score = max(0.0, min(95.0, score))

    should_analyze = bool(tickers) and sentiment != "NEUTRAL" and score >= _MIN_SIGNAL_SCORE
    if should_analyze:
        signal_class = "ACTIONABLE" if score >= _BUY_VERDICT_SCORE else "SECTOR_ONLY"
        skip_reason = ""
    elif tickers:
        signal_class = "LOW_SIGNAL"
        skip_reason = "sentiment/ticker anchors below threshold"
    else:
        signal_class = "LOW_SIGNAL"
        skip_reason = "no known ticker mention"

    return {
        "signal_score": round(score, 2),
        "video_signal_class": signal_class,
        "should_analyze": should_analyze,
        "should_analyze_stocks": should_analyze,
        "skip_reason": skip_reason,
        "sentiment": sentiment,
        "positive_hits": sorted(positive_hits),
        "negative_hits": sorted(negative_hits),
        "subreddit_tier": tier,
    }


def ingest_reddit_into_tracker(
    db: SignalTrackerDB,
    posts: Sequence[RedditPost],
    *,
    min_signal_score: float = _MIN_SIGNAL_SCORE,
) -> dict[str, Any]:
    """Classify, extract tickers, and write SignalRecords for Reddit posts."""
    ingested = 0
    skipped = 0
    by_subreddit: dict[str, int] = {}
    by_sentiment: dict[str, int] = {}
    for post in posts:
        classification = classify_reddit_post(post)
        score = float(classification["signal_score"])
        if not classification["should_analyze_stocks"] or score < min_signal_score:
            skipped += 1
            continue
        tickers = extract_tickers(f"{post.title} {post.selftext}")
        if not tickers:
            skipped += 1
            continue
        sentiment = str(classification.get("sentiment") or "NEUTRAL")
        if sentiment == "POSITIVE":
            verdict = "BUY" if score >= _BUY_VERDICT_SCORE else "WATCH"
        elif sentiment == "NEGATIVE":
            verdict = "SELL" if score >= _SELL_VERDICT_SCORE else "AVOID"
        else:
            verdict = "WATCH"

        signal_date = post.published_at[:10]
        wrote = 0
        for ticker, company in tickers:
            record = SignalRecord(
                ticker=ticker,
                company_name=company,
                channel_slug=f"reddit:{post.subreddit}",
                signal_date=signal_date,
                signal_score=score,
                verdict=verdict,
                source_video_id=post.source_id,
                source_title=post.title[:280],
            )
            if db.add_record(record):
                wrote += 1
        if wrote == 0:
            skipped += 1
            continue
        ingested += wrote
        by_subreddit[post.subreddit] = by_subreddit.get(post.subreddit, 0) + wrote
        by_sentiment[sentiment] = by_sentiment.get(sentiment, 0) + wrote
    return {
        "ingested": ingested,
        "skipped": skipped,
        "by_subreddit": by_subreddit,
        "by_sentiment": by_sentiment,
    }


def run_reddit_ingestion(
    *,
    subreddits: Sequence[str] | None = None,
    max_items_per_subreddit: int = 30,
    tracker_db_path: Path = DEFAULT_DB_PATH,
    health_path: Path = REDDIT_HEALTH_PATH,
    http_proxy_url: str | None = None,
    https_proxy_url: str | None = None,
    resolver: RedditResolver | None = None,
    tracker_db: SignalTrackerDB | None = None,
    sleep_seconds: float = _DEFAULT_SLEEP_SECONDS,
) -> dict[str, Any]:
    """Fetch posts, classify, ingest, and update the health file."""
    requested_subreddits, subreddit_urls = _build_resolver_subreddits(subreddits)
    resolver = resolver or RedditResolver(
        subreddits=subreddit_urls,
        http_proxy_url=http_proxy_url,
        https_proxy_url=https_proxy_url,
        sleep_seconds=sleep_seconds,
    )
    tracker = tracker_db if tracker_db is not None else SignalTrackerDB(tracker_db_path)

    state: dict[str, Any] = dict(read_json(health_path, {"error_count": 0}))
    state["last_run_at"] = datetime.now(timezone.utc).isoformat()

    requested = requested_subreddits if requested_subreddits is not None else list(resolver.subreddits)
    errors: list[dict[str, str | int]] = []
    posts: list[RedditPost] = []
    for idx, subreddit in enumerate(requested):
        try:
            posts.extend(resolver.fetch(subreddit, max_items=max_items_per_subreddit))
        except RedditFetchError as exc:
            errors.append({"subreddit": subreddit, "status": str(exc.status), "message": str(exc)})
        if idx < len(requested) - 1 and sleep_seconds > 0:
            time.sleep(sleep_seconds)

    summary = ingest_reddit_into_tracker(tracker, posts)

    finished_at = datetime.now(timezone.utc).isoformat()
    state["last_run_at"] = finished_at
    state["ingested_count"] = int(summary.get("ingested", 0))
    state["fetched_items"] = len(posts)
    state["subreddits"] = requested
    state["by_subreddit"] = summary.get("by_subreddit", {})
    state["by_sentiment"] = summary.get("by_sentiment", {})
    if errors and not posts:
        state["status"] = "error"
        state["last_error_at"] = finished_at
        state["error_count"] = int(state.get("error_count", 0)) + 1
        state["errors"] = errors
    elif errors:
        state["status"] = "degraded"
        state["last_success_at"] = finished_at
        state["errors"] = errors
        state["error_count"] = int(state.get("error_count", 0)) + 1
    else:
        state["status"] = "ok"
        state["last_success_at"] = finished_at
        state["errors"] = []

    write_json(health_path, state)
    return {
        "ingested": summary.get("ingested", 0),
        "skipped": summary.get("skipped", 0),
        "fetched_items": len(posts),
        "by_subreddit": summary.get("by_subreddit", {}),
        "by_sentiment": summary.get("by_sentiment", {}),
        "subreddits": requested,
        "errors": errors,
        "status": state["status"],
        "health_path": str(health_path),
        "api_option_analysis": REDDIT_API_OPTION_ANALYSIS,
        "subreddit_curation": SUBREDDIT_CURATION,
    }
