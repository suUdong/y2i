"""Korean stock talkboard ingestion for y2i.

This source captures retail-investor sentiment from public Korean stock
discussion boards as a fourth y2i signal class. The first implementation is
Naver Finance's stock board first page:

    https://finance.naver.com/item/board.naver?code=<6-digit KR code>

Access strategy
---------------
Naver's finance robots file is restrictive for generic crawlers and explicitly
disallows paginated board URLs. This module therefore defaults to first-page
reads only, uses a visible user-agent, exposes a sleep interval between boards,
and keeps live access optional. Fixture tests cover the parser and ingestion
contract; operators remain responsible for running it only where their
user-agent and policy allow.

Toss Securities and KakaoStock community surfaces are not used here because
they are app/login/API-oriented and do not provide a stable free stdlib
surface comparable to Naver's public HTML board.

Pipeline shape
--------------
    TalkboardResolver.fetch -> TalkboardPost
    classify_talkboard_post -> {signal_score, video_signal_class, should_analyze, ...}
    ingest_talkboard_into_tracker -> SignalRecord rows with channel_slug='talkboard:<board>'

Health state is written to ``TALKBOARD_HEALTH_PATH`` in the same shape as
``scheduler_health.json`` (the 74aeb5f stale-detection contract).
"""

from __future__ import annotations

import html
import logging
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

from .signal_gate import NON_EQUITY_KEYWORDS
from .signal_tracker import DEFAULT_DB_PATH, SignalRecord, SignalTrackerDB
from .stock_registry import COMPANY_MAP, resolve_kr_ticker
from .utils import read_json, write_json

logger = logging.getLogger(__name__)

TALKBOARD_HEALTH_PATH = Path(".omx/state/talkboard_ingestion_health.json")

DEFAULT_NAVER_BOARD_BASE = "https://finance.naver.com/item/board.naver"
_KST = timezone(timedelta(hours=9))
_DEFAULT_TIMEOUT_SECONDS = 10.0
_DEFAULT_SLEEP_SECONDS = 1.2
_DEFAULT_USER_AGENT = "y2i-talkboard-ingest/1.0 (+https://github.com/y2i; first-page-only)"

# Conservative, high-volume KR boards. Operators can override with --boards.
TALKBOARD_BOARDS: dict[str, str] = {
    "naver:005930": f"{DEFAULT_NAVER_BOARD_BASE}?code=005930",  # Samsung Electronics
    "naver:000660": f"{DEFAULT_NAVER_BOARD_BASE}?code=000660",  # SK hynix
    "naver:035420": f"{DEFAULT_NAVER_BOARD_BASE}?code=035420",  # NAVER
    "naver:035720": f"{DEFAULT_NAVER_BOARD_BASE}?code=035720",  # Kakao
    "naver:086520": f"{DEFAULT_NAVER_BOARD_BASE}?code=086520",  # EcoPro
    "naver:196170": f"{DEFAULT_NAVER_BOARD_BASE}?code=196170",  # Alteogen
}

SOURCE_SELECTION_ANALYSIS: dict[str, dict[str, str]] = {
    "naver_finance": {
        "accessibility": "public first-page HTML per stock code; no key; stdlib fetchable",
        "signal_density": "high for KOSPI/KOSDAQ large caps, direct retail sentiment",
        "cost": "free",
        "caveat": "restrictive robots; use first page only, low rate, and operator policy review",
        "decision": "selected",
    },
    "toss_securities": {
        "accessibility": "app/web community surface is login/API oriented and unstable for stdlib HTML",
        "signal_density": "potentially high but not safely accessible without private app APIs",
        "cost": "unknown; no free public ingestion surface selected",
        "caveat": "avoid login/session scraping and paid/private APIs",
        "decision": "defer",
    },
    "kakaostock": {
        "accessibility": "community data is app/API oriented with weaker public HTML discoverability",
        "signal_density": "medium; retail focused but less direct default coverage than Naver",
        "cost": "unknown; no free public ingestion surface selected",
        "caveat": "avoid reverse-engineered app APIs",
        "decision": "defer",
    },
}

_HTML_TAG_RE = re.compile(r"<[^>]+>")
_ROW_RE = re.compile(r"<tr\b[^>]*>(.*?)</tr>", re.IGNORECASE | re.DOTALL)
_CELL_RE = re.compile(r"<td\b[^>]*>(.*?)</td>", re.IGNORECASE | re.DOTALL)
_HREF_RE = re.compile(r"""href=["']([^"']+)["']""", re.IGNORECASE)
_NID_RE = re.compile(r"[?&]nid=([0-9]+)")
_CODE_RE = re.compile(r"(?<![0-9])([0-9]{6})(?![0-9])")
_CASHTAG_RE = re.compile(r"\$([0-9]{6})")
_TOKEN_RE = re.compile(r"[0-9A-Za-z가-힣&]+")
_DATE_RE = re.compile(r"(?:(20[0-9]{2})\.)?([01][0-9])\.([0-3][0-9])\s+([0-2][0-9]):([0-5][0-9])")

_POSITIVE_KEYWORDS = {
    "매수", "상승", "돌파", "강세", "급등", "반등", "상한가", "호재", "수급", "추매",
    "외인매수", "기관매수", "신고가", "랠리", "간다", "간다아", "매집", "저평가",
}
_NEGATIVE_KEYWORDS = {
    "매도", "하락", "손절", "급락", "폭락", "악재", "물림", "탈출", "던져", "던진다",
    "팔아라", "폭망", "하한가", "공매도", "파란불", "조정", "고점", "물타기",
}
_NOISE_KEYWORDS = {
    "비트코인", "코인", "정치", "대통령", "먹방", "날씨", "전라도", "경상도", "종교",
}
_BUY_VERDICT_SCORE = 68.0
_SELL_VERDICT_SCORE = 68.0


def _build_numeric_ticker_index() -> dict[str, tuple[str, str]]:
    index: dict[str, tuple[str, str]] = {}
    for ticker, english in COMPANY_MAP.values():
        if ticker.endswith((".KS", ".KQ")):
            numeric = ticker.split(".", 1)[0]
            index.setdefault(numeric, (ticker, english))
    return index


_NUMERIC_TICKER_INDEX = _build_numeric_ticker_index()


class TalkboardFetchError(RuntimeError):
    """Raised when a talkboard fetch or parse fails."""

    def __init__(self, board: str, status: int | str, message: str = "") -> None:
        self.board = board
        self.status = status
        suffix = f": {message}" if message else ""
        super().__init__(f"talkboard fetch failed for {board} (status={status}){suffix}")


@dataclass(slots=True)
class TalkboardPost:
    """One public board post row."""

    source_id: str
    board: str
    stock_code: str
    title: str
    author: str
    url: str
    published_at: str
    views: int | None = None
    upvotes: int | None = None
    downvotes: int | None = None
    raw_categories: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)


def _strip_html(value: str) -> str:
    text = _HTML_TAG_RE.sub(" ", value or "")
    return " ".join(html.unescape(text).split())


def _decode_body(body: bytes | str) -> str:
    if isinstance(body, str):
        return body
    for encoding in ("utf-8", "euc-kr", "cp949"):
        try:
            return body.decode(encoding)
        except UnicodeDecodeError:
            continue
    return body.decode("utf-8", errors="replace")


def _parse_int(text: str) -> int | None:
    cleaned = re.sub(r"[^0-9]", "", text or "")
    if not cleaned:
        return None
    return int(cleaned)


def _coerce_board_datetime(raw: str, *, now: datetime | None = None) -> str:
    now = now or datetime.now(_KST)
    match = _DATE_RE.search(raw or "")
    if not match:
        return now.astimezone(timezone.utc).isoformat()
    year = int(match.group(1) or now.year)
    month = int(match.group(2))
    day = int(match.group(3))
    hour = int(match.group(4))
    minute = int(match.group(5))
    parsed = datetime(year, month, day, hour, minute, tzinfo=_KST)
    return parsed.astimezone(timezone.utc).isoformat()


def _stock_code_from_board(board: str) -> str:
    match = _CODE_RE.search(board)
    if match:
        return match.group(1)
    return ""


def _stock_name_for_code(code: str) -> str:
    mapping = _NUMERIC_TICKER_INDEX.get(code)
    return mapping[1] if mapping else ""


def _validate_naver_first_page_url(raw_url: str) -> str:
    parsed = urllib.parse.urlparse(raw_url)
    query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
    code = (query.get("code") or [""])[0]
    if parsed.scheme not in {"http", "https"}:
        raise TalkboardFetchError(raw_url, "invalid_board", "unsupported URL scheme")
    if parsed.netloc != "finance.naver.com" or parsed.path != "/item/board.naver":
        raise TalkboardFetchError(raw_url, "invalid_board", "only Naver Finance board-list URLs are supported")
    if not re.fullmatch(r"[0-9]{6}", code):
        raise TalkboardFetchError(raw_url, "invalid_board", "missing 6-digit code")
    if "page" in query:
        raise TalkboardFetchError(raw_url, "disallowed_board", "paginated Naver board URLs are not supported")
    return code


def _normalize_board_entry(raw: str) -> tuple[str, str]:
    text = raw.strip()
    if not text:
        raise TalkboardFetchError(text, "invalid_board", "empty board")
    if text.startswith(("http://", "https://")):
        code = _validate_naver_first_page_url(text)
        return f"naver:{code}", text
    if re.fullmatch(r"[0-9]{6}", text):
        return f"naver:{text}", f"{DEFAULT_NAVER_BOARD_BASE}?code={text}"
    if text in TALKBOARD_BOARDS:
        return text, TALKBOARD_BOARDS[text]
    raise TalkboardFetchError(text, "unknown_board", f"board not registered: {text}")


def parse_naver_board_html(
    html_text: str,
    board: str,
    *,
    max_items: int | None = None,
    now: datetime | None = None,
) -> list[TalkboardPost]:
    """Parse Naver Finance board HTML into post rows.

    The parser is intentionally tolerant: it scans table rows, finds rows with
    a date cell and an article link/title cell, then extracts the remaining
    metadata cells when present.
    """
    stock_code = _stock_code_from_board(board)
    posts: list[TalkboardPost] = []
    for row in _ROW_RE.findall(html_text or ""):
        cells = _CELL_RE.findall(row)
        if len(cells) < 2:
            continue
        cleaned = [_strip_html(cell) for cell in cells]
        if any("클린봇" in cell for cell in cleaned):
            continue
        date_idx = next((idx for idx, cell in enumerate(cleaned) if _DATE_RE.search(cell)), None)
        if date_idx is None:
            continue
        title_idx = next(
            (
                idx for idx in range(date_idx + 1, len(cleaned))
                if cleaned[idx] and "클린봇" not in cleaned[idx] and "공지" not in cleaned[idx]
            ),
            None,
        )
        if title_idx is None:
            continue
        title = cleaned[title_idx]
        if not title or title in {"제목", "글쓴이"}:
            continue
        href_match = _HREF_RE.search(cells[title_idx]) or _HREF_RE.search(row)
        href = html.unescape(href_match.group(1)) if href_match else ""
        url = urllib.parse.urljoin("https://finance.naver.com", href) if href else ""
        nid_match = _NID_RE.search(url)
        source_key = nid_match.group(1) if nid_match else f"{cleaned[date_idx]}:{title}"
        author = cleaned[title_idx + 1] if title_idx + 1 < len(cleaned) else ""
        views = _parse_int(cleaned[title_idx + 2]) if title_idx + 2 < len(cleaned) else None
        upvotes = _parse_int(cleaned[title_idx + 3]) if title_idx + 3 < len(cleaned) else None
        downvotes = _parse_int(cleaned[title_idx + 4]) if title_idx + 4 < len(cleaned) else None
        posts.append(
            TalkboardPost(
                source_id=f"{board}:{source_key}"[:512],
                board=board,
                stock_code=stock_code,
                title=title,
                author=author,
                url=url,
                published_at=_coerce_board_datetime(cleaned[date_idx], now=now),
                views=views,
                upvotes=upvotes,
                downvotes=downvotes,
                raw_categories=["naver_finance"],
            )
        )
        if max_items is not None and len(posts) >= max_items:
            break

    if not posts and "종목 토론 게시판" not in html_text and "토론" not in html_text:
        raise TalkboardFetchError(board, "parse_error", "no board table markers found")
    return posts


class TalkboardResolver:
    """Fetch public first-page board HTML for registered stock boards."""

    def __init__(
        self,
        boards: dict[str, str] | None = None,
        *,
        http_proxy_url: str | None = None,
        https_proxy_url: str | None = None,
        timeout_seconds: float = _DEFAULT_TIMEOUT_SECONDS,
        user_agent: str = _DEFAULT_USER_AGENT,
        sleep_seconds: float = _DEFAULT_SLEEP_SECONDS,
    ) -> None:
        self._boards = dict(boards or TALKBOARD_BOARDS)
        for board, url in self._boards.items():
            if url.startswith(("http://", "https://")):
                _validate_naver_first_page_url(url)
        self._timeout = float(timeout_seconds)
        self._user_agent = user_agent
        self._sleep_seconds = max(0.0, float(sleep_seconds))
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
    def boards(self) -> dict[str, str]:
        return dict(self._boards)

    @property
    def outlets(self) -> dict[str, str]:
        return self.boards

    def fetch(self, board: str, max_items: int | None = 30) -> list[TalkboardPost]:
        if board not in self._boards:
            raise TalkboardFetchError(board, "unknown_board", f"board not registered: {board}")
        url = self._boards[board]
        try:
            with self._opener.open(url, timeout=self._timeout) as resp:
                status = getattr(resp, "status", 200)
                if status != 200:
                    raise TalkboardFetchError(board, status)
                body = resp.read()
        except urllib.error.HTTPError as exc:
            raise TalkboardFetchError(board, exc.code, exc.reason or "") from exc
        except urllib.error.URLError as exc:
            raise TalkboardFetchError(board, "url_error", str(exc.reason)) from exc
        return parse_naver_board_html(_decode_body(body), board, max_items=max_items)

    def fetch_all(self, boards: Iterable[str] | None = None, max_items_per_board: int = 30) -> list[TalkboardPost]:
        names = list(boards) if boards is not None else list(self._boards)
        collected: list[TalkboardPost] = []
        for idx, name in enumerate(names):
            try:
                collected.extend(self.fetch(name, max_items=max_items_per_board))
            except TalkboardFetchError as exc:
                logger.warning("talkboard fetch failed for %s: %s", name, exc)
            if idx < len(names) - 1 and self._sleep_seconds > 0:
                time.sleep(self._sleep_seconds)
        return collected


def _lowercase_tokens(text: str) -> list[str]:
    return [t.lower() for t in _TOKEN_RE.findall(text or "")]


def _keyword_hits(text: str, keywords: set[str]) -> set[str]:
    compact = re.sub(r"\s+", "", text.lower())
    token_set = set(_lowercase_tokens(text))
    return {kw for kw in keywords if kw.lower() in compact or kw.lower() in token_set}


def _extract_numeric_tickers(text: str) -> list[tuple[str, str]]:
    found: dict[str, str] = {}
    for code in list(_CODE_RE.findall(text or "")) + list(_CASHTAG_RE.findall(text or "")):
        mapping = _NUMERIC_TICKER_INDEX.get(code)
        if mapping:
            found.setdefault(mapping[0], mapping[1])
    return list(found.items())


def extract_kr_tickers(text: str, *, board_stock_code: str | None = None) -> list[tuple[str, str]]:
    """Return unique KR tickers from board code, 6-digit codes, and names."""
    seen: dict[str, str] = {}
    if board_stock_code and board_stock_code in _NUMERIC_TICKER_INDEX:
        ticker, english = _NUMERIC_TICKER_INDEX[board_stock_code]
        seen.setdefault(ticker, english)
    for ticker, english in _extract_numeric_tickers(text):
        seen.setdefault(ticker, english)
    for token in _lowercase_tokens(text):
        mapping = COMPANY_MAP.get(token)
        if mapping is None and len(token) >= 2:
            mapping = resolve_kr_ticker(token)
        if mapping is None:
            continue
        ticker, english = mapping
        if ticker.endswith((".KS", ".KQ")):
            seen.setdefault(ticker, english)
    return list(seen.items())


def classify_talkboard_post(post: TalkboardPost) -> dict:
    """Rule-based retail sentiment classifier; no LLM calls."""
    text = f"{post.title} {_stock_name_for_code(post.stock_code)}"
    token_set = set(_lowercase_tokens(text))
    non_equity_hits = (NON_EQUITY_KEYWORDS & token_set) | _keyword_hits(text, _NOISE_KEYWORDS)
    positive_hits = _keyword_hits(text, _POSITIVE_KEYWORDS)
    negative_hits = _keyword_hits(text, _NEGATIVE_KEYWORDS)

    if non_equity_hits and not (positive_hits or negative_hits):
        return {
            "signal_score": 25.0,
            "video_signal_class": "NON_EQUITY",
            "should_analyze": False,
            "skip_reason": f"non-equity keyword: {sorted(non_equity_hits)[0]}",
            "sentiment": "NEUTRAL",
        }

    ticker_hits = extract_kr_tickers(text, board_stock_code=post.stock_code)
    engagement = 0.0
    if post.views is not None:
        engagement += min(8.0, post.views / 60.0)
    if post.upvotes is not None:
        engagement += min(8.0, post.upvotes * 1.5)
    if post.downvotes is not None:
        engagement -= min(4.0, post.downvotes * 0.6)

    if positive_hits or negative_hits:
        score = 42.0 + 10.0 * len(positive_hits | negative_hits) + engagement
        if ticker_hits:
            score += 14.0
    else:
        score = 35.0 + engagement
        if ticker_hits:
            score += 8.0
    score = max(0.0, min(95.0, score))

    if positive_hits and len(positive_hits) >= len(negative_hits):
        sentiment = "POSITIVE"
    elif negative_hits:
        sentiment = "NEGATIVE"
    else:
        sentiment = "NEUTRAL"

    should_analyze = bool(ticker_hits) and score >= 55.0 and sentiment != "NEUTRAL"
    return {
        "signal_score": round(score, 2),
        "video_signal_class": "ACTIONABLE" if should_analyze and score >= 60.0 else "LOW_SIGNAL",
        "should_analyze": should_analyze,
        "skip_reason": "" if should_analyze else "sentiment/ticker anchors below threshold",
        "sentiment": sentiment,
        "positive_hits": sorted(positive_hits),
        "negative_hits": sorted(negative_hits),
    }


def ingest_talkboard_into_tracker(
    db: SignalTrackerDB,
    posts: Sequence[TalkboardPost],
    *,
    min_signal_score: float = 55.0,
) -> dict:
    """Classify, extract tickers, and write SignalRecords for TalkboardPosts."""
    ingested = 0
    skipped = 0
    by_board: dict[str, int] = {}
    for post in posts:
        classification = classify_talkboard_post(post)
        score = float(classification["signal_score"])
        if not classification["should_analyze"] or score < min_signal_score:
            skipped += 1
            continue
        tickers = extract_kr_tickers(post.title, board_stock_code=post.stock_code)
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
        for ticker, english in tickers:
            record = SignalRecord(
                ticker=ticker,
                company_name=english,
                channel_slug=f"talkboard:{post.board}",
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
        by_board[post.board] = by_board.get(post.board, 0) + wrote
    return {
        "ingested": ingested,
        "skipped": skipped,
        "by_board": by_board,
    }


def _build_resolver_boards(boards: Sequence[str] | None) -> tuple[list[str] | None, dict[str, str] | None]:
    if boards is None:
        return None, None
    names: list[str] = []
    urls: dict[str, str] = {}
    for raw in boards:
        name, url = _normalize_board_entry(raw)
        names.append(name)
        urls[name] = url
    return names, urls


def run_talkboard_ingestion(
    *,
    boards: Sequence[str] | None = None,
    max_items_per_board: int = 30,
    tracker_db_path: Path = DEFAULT_DB_PATH,
    health_path: Path = TALKBOARD_HEALTH_PATH,
    http_proxy_url: str | None = None,
    https_proxy_url: str | None = None,
    resolver: TalkboardResolver | None = None,
    tracker_db: SignalTrackerDB | None = None,
    sleep_seconds: float = _DEFAULT_SLEEP_SECONDS,
) -> dict:
    """Fetch talkboards, classify, ingest, and update the health file."""
    requested_boards, board_urls = _build_resolver_boards(boards)
    resolver = resolver or TalkboardResolver(
        boards=board_urls,
        http_proxy_url=http_proxy_url,
        https_proxy_url=https_proxy_url,
        sleep_seconds=sleep_seconds,
    )
    tracker = tracker_db if tracker_db is not None else SignalTrackerDB(tracker_db_path)

    state: dict[str, Any] = dict(read_json(health_path, {"error_count": 0}))
    state["last_run_at"] = datetime.now(timezone.utc).isoformat()

    requested = requested_boards if requested_boards is not None else list(resolver.boards)
    errors: list[dict[str, str | int]] = []
    posts: list[TalkboardPost] = []
    for idx, board in enumerate(requested):
        try:
            posts.extend(resolver.fetch(board, max_items=max_items_per_board))
        except TalkboardFetchError as exc:
            errors.append({"board": board, "status": str(exc.status), "message": str(exc)})
        if idx < len(requested) - 1 and sleep_seconds > 0:
            time.sleep(sleep_seconds)

    summary = ingest_talkboard_into_tracker(tracker, posts)

    finished_at = datetime.now(timezone.utc).isoformat()
    state["last_run_at"] = finished_at
    state["ingested_count"] = int(summary.get("ingested", 0))
    state["fetched_items"] = len(posts)
    state["boards"] = requested
    state["by_board"] = summary.get("by_board", {})
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
        "by_board": summary.get("by_board", {}),
        "boards": requested,
        "errors": errors,
        "status": state["status"],
        "health_path": str(health_path),
        "source_selection": SOURCE_SELECTION_ANALYSIS,
    }
