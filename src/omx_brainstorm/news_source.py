"""Korean financial news RSS ingestion for y2i.

Acts as a second signal source alongside YouTube. NewsItems are parsed from
stdlib (urllib + xml.etree) — no new third-party dependency required.

Pipeline shape mirrors the YouTube path:
    NewsResolver.fetch -> NewsItem
    classify_news_item  -> {signal_score, video_signal_class, should_analyze, ...}
    ingest_news_into_tracker -> SignalRecord rows written to SignalTrackerDB
        with channel_slug='news:<outlet>'

Health state is written to NEWS_HEALTH_PATH and consumed by
healthcheck.compute_health_summary using the same shape as scheduler_health.json
(the 74aeb5f stale-detection contract).
"""

from __future__ import annotations

import html
import logging
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

NEWS_HEALTH_PATH = Path(".omx/state/news_ingestion_health.json")

# Korean financial-press RSS endpoints. Picked for coverage of equity coverage
# desks (한경, 머투, 이데일리). All three publish category-level RSS feeds
# scoped to securities/companies that are direct y2i targets.
NEWS_OUTLETS: dict[str, str] = {
    "hankyung": "https://www.hankyung.com/feed/finance",
    "mt": "https://rss.mt.co.kr/mt_news.xml",
    "edaily": "https://rss.edaily.co.kr/stock_news.xml",
}

_TICKER_TOKEN_RE = re.compile(r"[0-9A-Za-z가-힣&]+")
_HTML_TAG_RE = re.compile(r"<[^>]+>")
_DEFAULT_TIMEOUT_SECONDS = 10.0
_DEFAULT_USER_AGENT = "y2i-news-ingest/1.0 (+https://github.com/y2i)"


class NewsFetchError(RuntimeError):
    """Raised when an outlet RSS fetch returns a non-success response."""

    def __init__(self, outlet: str, status: int | str, message: str = "") -> None:
        self.outlet = outlet
        self.status = status
        suffix = f": {message}" if message else ""
        super().__init__(f"news fetch failed for {outlet} (status={status}){suffix}")


@dataclass(slots=True)
class NewsItem:
    """One published news headline with parsed metadata."""

    source_id: str
    outlet: str
    title: str
    summary: str
    url: str
    published_at: str  # ISO-8601 UTC, e.g. "2026-05-11T03:14:00+00:00"
    raw_categories: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)


def _strip_html(value: str) -> str:
    cleaned = _HTML_TAG_RE.sub("", value or "")
    return html.unescape(cleaned).strip()


def _coerce_published_iso(raw: str | None, *, now: datetime | None = None) -> str:
    """Parse RFC822 / ISO date strings into an ISO-8601 UTC string.

    Returns the current UTC time when the input is missing or unparseable.
    Callers receive a stable string regardless of upstream feed quality.
    """
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


def parse_rss_xml(xml_text: str, outlet: str, *, max_items: int | None = None) -> list[NewsItem]:
    """Parse RSS 2.0 (or Atom-like) XML into NewsItem objects.

    Tolerant of feeds that omit guid, link, or pubDate. Title is required;
    items missing a title are skipped.
    """
    items: list[NewsItem] = []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        raise NewsFetchError(outlet, "parse_error", str(exc)) from exc

    channel = root.find("channel") if root.tag.endswith("rss") or root.tag == "rss" else root
    if channel is None:
        channel = root
    entries = channel.findall("item")
    if not entries:
        entries = channel.findall("{http://www.w3.org/2005/Atom}entry")

    for entry in entries:
        title = _strip_html(_findtext(entry, ("title",)))
        if not title:
            continue
        link = _strip_html(_findtext(entry, ("link",)))
        if not link:
            # Atom entries store link as an attribute on a <link> element.
            link_el = entry.find("{http://www.w3.org/2005/Atom}link")
            if link_el is not None:
                link = link_el.attrib.get("href", "") or ""
        guid = _strip_html(_findtext(entry, ("guid", "{http://www.w3.org/2005/Atom}id"))) or link or title
        pub = _findtext(entry, ("pubDate", "{http://www.w3.org/2005/Atom}updated", "{http://www.w3.org/2005/Atom}published"))
        summary = _strip_html(_findtext(entry, ("description", "{http://www.w3.org/2005/Atom}summary")))
        categories = [_strip_html(c.text or "") for c in entry.findall("category") if (c.text or "").strip()]

        items.append(
            NewsItem(
                source_id=f"{outlet}:{guid}"[:512],
                outlet=outlet,
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


def _findtext(entry: ET.Element, tag_names: Sequence[str]) -> str:
    for tag in tag_names:
        node = entry.find(tag)
        if node is not None and (node.text or "").strip():
            return node.text or ""
    return ""


class NewsResolver:
    """Fetches RSS for a registered outlet and returns parsed NewsItems."""

    def __init__(
        self,
        outlets: dict[str, str] | None = None,
        *,
        http_proxy_url: str | None = None,
        https_proxy_url: str | None = None,
        timeout_seconds: float = _DEFAULT_TIMEOUT_SECONDS,
        user_agent: str = _DEFAULT_USER_AGENT,
    ) -> None:
        self._outlets = dict(outlets or NEWS_OUTLETS)
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
    def outlets(self) -> dict[str, str]:
        return dict(self._outlets)

    def fetch(self, outlet: str, max_items: int | None = 30) -> list[NewsItem]:
        if outlet not in self._outlets:
            raise NewsFetchError(outlet, "unknown_outlet", f"outlet not registered: {outlet}")
        url = self._outlets[outlet]
        try:
            with self._opener.open(url, timeout=self._timeout) as resp:
                status = getattr(resp, "status", 200)
                if status != 200:
                    raise NewsFetchError(outlet, status)
                body = resp.read()
        except urllib.error.HTTPError as exc:
            raise NewsFetchError(outlet, exc.code, exc.reason or "") from exc
        except urllib.error.URLError as exc:
            raise NewsFetchError(outlet, "url_error", str(exc.reason)) from exc

        text = body.decode("utf-8", errors="replace") if isinstance(body, (bytes, bytearray)) else str(body)
        return parse_rss_xml(text, outlet, max_items=max_items)

    def fetch_all(self, outlets: Iterable[str] | None = None, max_items_per_outlet: int = 30) -> list[NewsItem]:
        names = list(outlets) if outlets is not None else list(self._outlets)
        collected: list[NewsItem] = []
        for name in names:
            try:
                collected.extend(self.fetch(name, max_items=max_items_per_outlet))
            except NewsFetchError as exc:
                logger.warning("news fetch failed for %s: %s", name, exc)
        return collected


# ──────────────────────────────────────────────────────────────────────────
# Classification + tracker ingestion
# ──────────────────────────────────────────────────────────────────────────


_BUY_VERDICT_SCORE = 68.0


def _lowercase_tokens(text: str) -> list[str]:
    return [t.lower() for t in _TICKER_TOKEN_RE.findall(text or "")]


def classify_news_item(item: NewsItem) -> dict:
    """Score a NewsItem using signal_gate keyword inventory.

    Returns the same field shape as VideoSignalAssessment.to_dict for
    downstream parity:
        {signal_score, video_signal_class, should_analyze, skip_reason}
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

    score = 40.0 + 6.0 * len(finance_hits) + 8.0 * len(anchor_hits)
    # Ticker presence is a strong signal even without keyword anchors.
    if _has_known_ticker(text):
        score += 18.0
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


def _has_known_ticker(text: str) -> bool:
    return any(token in COMPANY_MAP for token in _lowercase_tokens(text))


def extract_kr_tickers(text: str) -> list[tuple[str, str]]:
    """Return unique (ticker, english_name) pairs limited to KR markets."""
    seen: dict[str, str] = {}
    tokens = _lowercase_tokens(text)
    # Try direct COMPANY_MAP hits first, then fuzzy resolver fallback for
    # tokens longer than 2 characters (avoid noise on single jamo).
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


def ingest_news_into_tracker(
    db: SignalTrackerDB,
    items: Sequence[NewsItem],
    *,
    min_signal_score: float = 60.0,
) -> dict:
    """Classify, extract tickers, and write SignalRecords for each NewsItem.

    Returns a summary {ingested, skipped, by_outlet} suitable for both CLI
    JSON output and the news-ingestion health file.
    """
    ingested = 0
    skipped = 0
    by_outlet: dict[str, int] = {}
    for item in items:
        classification = classify_news_item(item)
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
                channel_slug=f"news:{item.outlet}",
                signal_date=signal_date,
                signal_score=score,
                verdict=verdict,
                source_video_id=item.source_id,
                source_title=item.title,
            )
            if db.add_record(record):
                wrote += 1
        if wrote == 0:
            skipped += 1
            continue
        ingested += wrote
        by_outlet[item.outlet] = by_outlet.get(item.outlet, 0) + wrote
    return {
        "ingested": ingested,
        "skipped": skipped,
        "by_outlet": by_outlet,
    }


# ──────────────────────────────────────────────────────────────────────────
# Runner — health-file aware (mirrors scheduler_health.json shape)
# ──────────────────────────────────────────────────────────────────────────


def run_news_ingestion(
    *,
    outlets: Sequence[str] | None = None,
    max_items_per_outlet: int = 30,
    tracker_db_path: Path = DEFAULT_DB_PATH,
    health_path: Path = NEWS_HEALTH_PATH,
    http_proxy_url: str | None = None,
    https_proxy_url: str | None = None,
    resolver: NewsResolver | None = None,
    tracker_db: SignalTrackerDB | None = None,
) -> dict:
    """Fetch RSS, classify, ingest into the tracker, and update the health file.

    Health file shape matches scheduler_health.json so the existing
    healthcheck.compute_health_summary works unchanged.
    """
    resolver = resolver or NewsResolver(
        http_proxy_url=http_proxy_url,
        https_proxy_url=https_proxy_url,
    )
    tracker = tracker_db if tracker_db is not None else SignalTrackerDB(tracker_db_path)

    state: dict[str, Any] = dict(read_json(health_path, {"error_count": 0}))
    started_at = datetime.now(timezone.utc).isoformat()
    state["last_run_at"] = started_at

    requested_outlets = list(outlets) if outlets is not None else list(resolver.outlets)
    errors: list[dict[str, str | int]] = []
    items: list[NewsItem] = []
    for name in requested_outlets:
        try:
            items.extend(resolver.fetch(name, max_items=max_items_per_outlet))
        except NewsFetchError as exc:
            errors.append({"outlet": name, "status": str(exc.status), "message": str(exc)})

    summary = ingest_news_into_tracker(tracker, items)

    finished_at = datetime.now(timezone.utc).isoformat()
    state["last_run_at"] = finished_at
    state["ingested_count"] = int(summary.get("ingested", 0))
    state["fetched_items"] = len(items)
    state["outlets"] = requested_outlets
    state["by_outlet"] = summary.get("by_outlet", {})
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
        "by_outlet": summary.get("by_outlet", {}),
        "outlets": requested_outlets,
        "errors": errors,
        "status": state["status"],
        "health_path": str(health_path),
    }
