"""Twitter/X ingestion for y2i — Korean financial influencer feeds.

A third signal source alongside YouTube and Korean financial RSS news. The
shape mirrors ``news_source.py`` so consumers (signal_tracker, kindshot_feed,
healthcheck) can treat all three sources interchangeably.

Access strategy
---------------
The official X API v2 charges $200/month for the Basic tier (10K reads) — too
expensive for a pilot, and the free tier has no read entitlement. We instead
read each handle through a self-hostable RSS gateway. Two interchangeable
gateway shapes are supported with zero code changes:

    Nitter:  https://<host>/<handle>/rss
    RSSHub:  https://<host>/twitter/user/<handle>

The gateway base URL and URL template are configurable per-handle, so an
operator can self-host an RSSHub instance for durability without touching
this module. No Twitter credentials and no third-party Python dependency are
required — parsing uses stdlib ``urllib`` + ``xml.etree.ElementTree``.

Pipeline shape
--------------
    TwitterResolver.fetch -> TweetItem
    classify_tweet_item   -> {signal_score, video_signal_class, should_analyze, skip_reason}
    ingest_tweets_into_tracker -> SignalRecord rows with channel_slug='twitter:<handle>'

Health state is written to ``TWITTER_HEALTH_PATH`` in the same shape as
``scheduler_health.json`` (the 74aeb5f stale-detection contract), so
``healthcheck.compute_health_summary`` works unchanged.
"""

from __future__ import annotations

import html
import logging
import os
import re
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Iterable, Sequence

from .signal_gate import FINANCE_KEYWORDS, ACTIONABLE_TITLE_ANCHORS, NON_EQUITY_KEYWORDS
from .signal_tracker import DEFAULT_DB_PATH, SignalRecord, SignalTrackerDB
from .stock_registry import COMPANY_MAP, resolve_kr_ticker
from .utils import read_json, write_json

logger = logging.getLogger(__name__)

TWITTER_HEALTH_PATH = Path(".omx/state/twitter_ingestion_health.json")

# The default RSS gateway. Override globally via the ``Y2I_TWITTER_GATEWAY``
# environment variable when self-hosting a Nitter or RSSHub instance. The
# default points at a public Nitter mirror; the per-handle URL is built from
# ``DEFAULT_GATEWAY_TEMPLATE``.
DEFAULT_GATEWAY_BASE = os.environ.get(
    "Y2I_TWITTER_GATEWAY", "https://nitter.privacydev.net"
).rstrip("/")
DEFAULT_GATEWAY_TEMPLATE = os.environ.get(
    "Y2I_TWITTER_GATEWAY_TEMPLATE", "{base}/{handle}/rss"
)


def _default_gateway_url(handle: str) -> str:
    """Build the RSS URL for a handle from the configured gateway template."""
    return DEFAULT_GATEWAY_TEMPLATE.format(base=DEFAULT_GATEWAY_BASE, handle=handle)


# Curated Korean financial-press X handles. These are seed defaults — they
# point at public media-organization accounts that publish stock-relevant
# headlines in volume. Operators should refine this list with personal
# fintwit accounts based on the 14-day pilot freshness/hit-rate review
# (see ``scripts/twitter_signal_freshness.py``). Add or override entries
# via the operator config rather than editing this module.
TWITTER_INFLUENCERS: dict[str, str] = {
    "hankyung_news":  _default_gateway_url("hankyung_news"),
    "mtnews_kr":      _default_gateway_url("mtnews_kr"),
    "_yonhapnews":    _default_gateway_url("_yonhapnews"),
    "chosunbiz":      _default_gateway_url("chosunbiz"),
    "mkbusiness":     _default_gateway_url("mkbusiness"),
    "etoday_kr":      _default_gateway_url("etoday_kr"),
    "wowtv_kr":       _default_gateway_url("wowtv_kr"),
    "etnews_kr":      _default_gateway_url("etnews_kr"),
    "sedaily_kr":     _default_gateway_url("sedaily_kr"),
    "koreaherald":    _default_gateway_url("koreaherald"),
}

_TICKER_TOKEN_RE = re.compile(r"[0-9A-Za-z가-힣&]+")
_CASHTAG_RE = re.compile(r"\$([A-Za-z0-9]{2,8})")
_HTML_TAG_RE = re.compile(r"<[^>]+>")
_DEFAULT_TIMEOUT_SECONDS = 10.0
_DEFAULT_USER_AGENT = "y2i-twitter-ingest/1.0 (+https://github.com/y2i)"


# ── Numeric-cashtag lookup ($005930 → (005930.KS, "Samsung Electronics")) ─
# Built once from COMPANY_MAP so the cashtag matcher stays in sync with the
# Korean-name resolver. Only KR-suffixed tickers participate; non-KR entries
# in COMPANY_MAP (e.g. NVDA) are ignored at this layer.
def _build_numeric_ticker_index() -> dict[str, tuple[str, str]]:
    index: dict[str, tuple[str, str]] = {}
    for ticker, english in COMPANY_MAP.values():
        if ticker.endswith((".KS", ".KQ")):
            numeric = ticker.split(".", 1)[0]
            index.setdefault(numeric, (ticker, english))
    return index


_NUMERIC_TICKER_INDEX: dict[str, tuple[str, str]] = _build_numeric_ticker_index()


class TwitterFetchError(RuntimeError):
    """Raised when a gateway RSS fetch returns a non-success response."""

    def __init__(self, handle: str, status: int | str, message: str = "") -> None:
        self.handle = handle
        self.status = status
        suffix = f": {message}" if message else ""
        super().__init__(f"twitter fetch failed for {handle} (status={status}){suffix}")


@dataclass(slots=True)
class TweetItem:
    """One tweet (or Twitter-like post) lifted from an RSS gateway."""

    source_id: str
    handle: str
    title: str
    summary: str
    url: str
    published_at: str  # ISO-8601 UTC
    raw_categories: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)


def _strip_html(value: str) -> str:
    cleaned = _HTML_TAG_RE.sub("", value or "")
    return html.unescape(cleaned).strip()


def _coerce_published_iso(raw: str | None, *, now: datetime | None = None) -> str:
    """Parse RFC822 / ISO date strings into an ISO-8601 UTC string."""
    if raw:
        text = raw.strip()
        try:
            parsed = parsedate_to_datetime(text)
        except (TypeError, ValueError):
            parsed = None
        if parsed is None:
            try:
                parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
            except ValueError:
                parsed = None
        if parsed is not None:
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc).isoformat()
    return (now or datetime.now(timezone.utc)).isoformat()


def _findtext(entry: ET.Element, tag_names: Sequence[str]) -> str:
    for tag in tag_names:
        node = entry.find(tag)
        if node is not None and (node.text or "").strip():
            return node.text or ""
    return ""


def parse_rss_xml(xml_text: str, handle: str, *, max_items: int | None = None) -> list[TweetItem]:
    """Parse RSS/Atom XML from a Twitter gateway into ``TweetItem`` objects.

    Tolerant of feeds that omit guid, link, or pubDate. Items missing a
    title and description are skipped.
    """
    items: list[TweetItem] = []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        raise TwitterFetchError(handle, "parse_error", str(exc)) from exc

    channel = root.find("channel") if root.tag.endswith("rss") or root.tag == "rss" else root
    if channel is None:
        channel = root
    entries = channel.findall("item")
    if not entries:
        entries = channel.findall("{http://www.w3.org/2005/Atom}entry")

    for entry in entries:
        title = _strip_html(_findtext(entry, ("title",)))
        summary = _strip_html(
            _findtext(entry, ("description", "{http://www.w3.org/2005/Atom}summary"))
        )
        if not title and not summary:
            continue
        # On Nitter feeds the tweet body lives in description; the title is
        # often a truncated copy. Use whichever is longer as the canonical
        # title so the classifier sees the most context.
        if summary and len(summary) > len(title):
            title = summary
        link = _strip_html(_findtext(entry, ("link",)))
        if not link:
            link_el = entry.find("{http://www.w3.org/2005/Atom}link")
            if link_el is not None:
                link = link_el.attrib.get("href", "") or ""
        guid = _strip_html(_findtext(entry, ("guid", "{http://www.w3.org/2005/Atom}id"))) or link or title
        pub = _findtext(
            entry,
            ("pubDate", "{http://www.w3.org/2005/Atom}updated", "{http://www.w3.org/2005/Atom}published"),
        )
        categories = [_strip_html(c.text or "") for c in entry.findall("category") if (c.text or "").strip()]

        items.append(
            TweetItem(
                source_id=f"{handle}:{guid}"[:512],
                handle=handle,
                title=title,
                summary=summary,
                url=link,
                published_at=_coerce_published_iso(pub),
                raw_categories=categories,
            )
        )
        if max_items is not None and len(items) >= max_items:
            break

    return items


class TwitterResolver:
    """Fetch RSS for a registered handle via the configured gateway."""

    def __init__(
        self,
        handles: dict[str, str] | None = None,
        *,
        http_proxy_url: str | None = None,
        https_proxy_url: str | None = None,
        timeout_seconds: float = _DEFAULT_TIMEOUT_SECONDS,
        user_agent: str = _DEFAULT_USER_AGENT,
    ) -> None:
        self._handles = dict(handles or TWITTER_INFLUENCERS)
        self._timeout = float(timeout_seconds)
        self._user_agent = user_agent
        proxies: dict[str, str] = {}
        if http_proxy_url:
            proxies["http"] = http_proxy_url
        if https_proxy_url:
            proxies["https"] = https_proxy_url
        if proxies:
            opener = urllib.request.build_opener(urllib.request.ProxyHandler(proxies))
        else:
            opener = urllib.request.build_opener()
        opener.addheaders = [("User-Agent", self._user_agent)]
        self._opener = opener

    @property
    def handles(self) -> dict[str, str]:
        return dict(self._handles)

    # Parity alias so callers written against the news_source shape work.
    @property
    def outlets(self) -> dict[str, str]:
        return self.handles

    def fetch(self, handle: str, max_items: int | None = 30) -> list[TweetItem]:
        if handle not in self._handles:
            raise TwitterFetchError(handle, "unknown_handle", f"handle not registered: {handle}")
        url = self._handles[handle]
        try:
            with self._opener.open(url, timeout=self._timeout) as resp:
                status = getattr(resp, "status", 200)
                if status != 200:
                    raise TwitterFetchError(handle, status)
                body = resp.read()
        except urllib.error.HTTPError as exc:
            raise TwitterFetchError(handle, exc.code, exc.reason or "") from exc
        except urllib.error.URLError as exc:
            raise TwitterFetchError(handle, "url_error", str(exc.reason)) from exc

        text = body.decode("utf-8", errors="replace") if isinstance(body, (bytes, bytearray)) else str(body)
        return parse_rss_xml(text, handle, max_items=max_items)

    def fetch_all(self, handles: Iterable[str] | None = None, max_items_per_handle: int = 30) -> list[TweetItem]:
        names = list(handles) if handles is not None else list(self._handles)
        collected: list[TweetItem] = []
        for name in names:
            try:
                collected.extend(self.fetch(name, max_items=max_items_per_handle))
            except TwitterFetchError as exc:
                logger.warning("twitter fetch failed for %s: %s", name, exc)
        return collected


# ──────────────────────────────────────────────────────────────────────────
# Classification + tracker ingestion
# ──────────────────────────────────────────────────────────────────────────


_BUY_VERDICT_SCORE = 68.0


def _lowercase_tokens(text: str) -> list[str]:
    return [t.lower() for t in _TICKER_TOKEN_RE.findall(text or "")]


def _has_known_ticker(text: str) -> bool:
    """True if the text mentions a known Korean-name token *or* a KR cashtag."""
    tokens = _lowercase_tokens(text)
    if any(token in COMPANY_MAP for token in tokens):
        return True
    return bool(_extract_cashtag_kr_tickers(text))


def _extract_cashtag_kr_tickers(text: str) -> list[tuple[str, str]]:
    """Pull ``$005930``-style cashtags that resolve to a KR ticker."""
    out: list[tuple[str, str]] = []
    for match in _CASHTAG_RE.findall(text or ""):
        upper = match.upper()
        if upper.isdigit() and upper in _NUMERIC_TICKER_INDEX:
            out.append(_NUMERIC_TICKER_INDEX[upper])
    return out


def classify_tweet_item(item: TweetItem) -> dict:
    """Score a TweetItem with the same keyword inventory used for news.

    Cashtag presence is treated as a strong actionable anchor — a tweet that
    types ``$005930`` is almost certainly equity-relevant even if the rest of
    the keyword anchors are sparse.
    """
    text = f"{item.title} {item.summary}"
    tokens = _lowercase_tokens(text)
    token_set = set(tokens)

    non_equity_hits = NON_EQUITY_KEYWORDS & token_set
    if non_equity_hits:
        return {
            "signal_score": 25.0,
            "video_signal_class": "NON_EQUITY",
            "should_analyze": False,
            "skip_reason": f"non-equity keyword: {sorted(non_equity_hits)[0]}",
        }

    finance_hits = FINANCE_KEYWORDS & token_set
    anchor_hits = ACTIONABLE_TITLE_ANCHORS & token_set
    cashtag_hits = _extract_cashtag_kr_tickers(text)

    score = 40.0 + 6.0 * len(finance_hits) + 8.0 * len(anchor_hits)
    if _has_known_ticker(text):
        score += 18.0
    # Cashtag bump: each unique KR cashtag adds +6 (capped at +12 to avoid
    # runaway scores on threads that repeat one ticker many times).
    if cashtag_hits:
        score += min(12.0, 6.0 * len({t for t, _ in cashtag_hits}))
    score = max(0.0, min(95.0, score))

    if score >= 60.0:
        return {
            "signal_score": round(score, 2),
            "video_signal_class": "ACTIONABLE" if score >= _BUY_VERDICT_SCORE else "SECTOR_ONLY",
            "should_analyze": True,
            "skip_reason": "",
        }

    return {
        "signal_score": round(score, 2),
        "video_signal_class": "LOW_SIGNAL",
        "should_analyze": False,
        "skip_reason": "keyword anchors below threshold",
    }


def extract_kr_tickers(text: str) -> list[tuple[str, str]]:
    """Return unique (ticker, english_name) pairs, KR markets only.

    Combines three matchers in priority order:
        1. Cashtag pattern  — ``$005930``  → ``005930.KS``
        2. COMPANY_MAP exact-word match    (e.g. "삼성전자")
        3. ``resolve_kr_ticker`` fuzzy fallback for tokens ≥2 chars
    """
    seen: dict[str, str] = {}
    for ticker, english in _extract_cashtag_kr_tickers(text):
        seen.setdefault(ticker, english)
    tokens = _lowercase_tokens(text)
    for token in tokens:
        mapping = COMPANY_MAP.get(token)
        if mapping is None and len(token) >= 2:
            mapping = resolve_kr_ticker(token)
        if mapping is None:
            continue
        ticker, english = mapping
        if not ticker.endswith((".KS", ".KQ")):
            continue
        seen.setdefault(ticker, english)
    return list(seen.items())


def ingest_tweets_into_tracker(
    db: SignalTrackerDB,
    items: Sequence[TweetItem],
    *,
    min_signal_score: float = 60.0,
) -> dict:
    """Classify, extract tickers, and write SignalRecords for each TweetItem."""
    ingested = 0
    skipped = 0
    by_handle: dict[str, int] = {}
    for item in items:
        classification = classify_tweet_item(item)
        score = float(classification["signal_score"])
        if not classification["should_analyze"] or score < min_signal_score:
            skipped += 1
            continue
        text = f"{item.title} {item.summary}"
        tickers = extract_kr_tickers(text)
        if not tickers:
            skipped += 1
            continue
        verdict = "BUY" if score >= _BUY_VERDICT_SCORE else "WATCH"
        signal_date = item.published_at[:10]
        wrote = 0
        for ticker, english in tickers:
            record = SignalRecord(
                ticker=ticker,
                company_name=english,
                channel_slug=f"twitter:{item.handle}",
                signal_date=signal_date,
                signal_score=score,
                verdict=verdict,
                source_video_id=item.source_id,
                source_title=item.title[:280],
            )
            if db.add_record(record):
                wrote += 1
        if wrote == 0:
            skipped += 1
            continue
        ingested += wrote
        by_handle[item.handle] = by_handle.get(item.handle, 0) + wrote
    return {
        "ingested": ingested,
        "skipped": skipped,
        "by_handle": by_handle,
    }


# ──────────────────────────────────────────────────────────────────────────
# Runner — health-file aware (mirrors scheduler_health.json shape)
# ──────────────────────────────────────────────────────────────────────────


def run_twitter_ingestion(
    *,
    handles: Sequence[str] | None = None,
    max_items_per_handle: int = 30,
    tracker_db_path: Path = DEFAULT_DB_PATH,
    health_path: Path = TWITTER_HEALTH_PATH,
    http_proxy_url: str | None = None,
    https_proxy_url: str | None = None,
    resolver: TwitterResolver | None = None,
    tracker_db: SignalTrackerDB | None = None,
) -> dict:
    """Fetch tweets, classify, ingest, and update the health file.

    Health-file shape matches ``scheduler_health.json`` so the existing
    ``healthcheck.compute_health_summary`` works unchanged.
    """
    resolver = resolver or TwitterResolver(
        http_proxy_url=http_proxy_url,
        https_proxy_url=https_proxy_url,
    )
    tracker = tracker_db if tracker_db is not None else SignalTrackerDB(tracker_db_path)

    state: dict[str, Any] = dict(read_json(health_path, {"error_count": 0}))
    started_at = datetime.now(timezone.utc).isoformat()
    state["last_run_at"] = started_at

    requested = list(handles) if handles is not None else list(resolver.handles)
    errors: list[dict[str, str | int]] = []
    items: list[TweetItem] = []
    for name in requested:
        try:
            items.extend(resolver.fetch(name, max_items=max_items_per_handle))
        except TwitterFetchError as exc:
            errors.append({"handle": name, "status": str(exc.status), "message": str(exc)})

    summary = ingest_tweets_into_tracker(tracker, items)

    finished_at = datetime.now(timezone.utc).isoformat()
    state["last_run_at"] = finished_at
    state["ingested_count"] = int(summary.get("ingested", 0))
    state["fetched_items"] = len(items)
    state["handles"] = requested
    state["by_handle"] = summary.get("by_handle", {})
    if errors and not items:
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
        "fetched_items": len(items),
        "by_handle": summary.get("by_handle", {}),
        "handles": requested,
        "errors": errors,
        "status": state["status"],
        "health_path": str(health_path),
    }
