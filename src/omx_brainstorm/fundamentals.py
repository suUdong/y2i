from __future__ import annotations

from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, replace
from datetime import datetime, timezone
from pathlib import Path
import re
from threading import Lock

from .models import FundamentalSnapshot, TickerMention, utc_now_iso
from .utils import ensure_dir, read_json, write_json


SAFE_CACHE_KEY_RE = re.compile(r"[^A-Za-z0-9._-]")
DEFAULT_MAX_AGE_HOURS = 12


class FundamentalsFetcher:
    """Best-effort current fundamentals fetcher using Yahoo Finance with file-backed caching."""

    def __init__(
        self,
        cache_root: Path | None = None,
        max_age_hours: int = DEFAULT_MAX_AGE_HOURS,
        max_workers: int = 4,
        max_memory_entries: int = 256,
        memory_cache_max_entries: int | None = None,
        memory_cache_size: int | None = None,
    ) -> None:
        self.cache_root = cache_root or Path(".omx/cache/fundamentals")
        self.max_age_hours = max_age_hours
        self.max_workers = max_workers
        if memory_cache_max_entries is not None:
            max_memory_entries = memory_cache_max_entries
        if memory_cache_size is not None:
            max_memory_entries = memory_cache_size
        self.max_memory_entries = max(0, int(max_memory_entries))
        self._memory_cache: OrderedDict[str, dict[str, object]] = OrderedDict()
        self._lock = Lock()
        ensure_dir(self.cache_root)

    def fetch(self, mention: TickerMention) -> FundamentalSnapshot:
        cache_key = self._cache_key(mention.ticker)
        cached = self._load_cache_entry(cache_key)
        if cached is not None and not self._is_stale(cached):
            return self._snapshot_from_entry(cached)

        stale_snapshot = self._snapshot_from_entry(cached) if cached is not None else None
        try:
            snapshot = self._fetch_live(mention)
        except Exception as exc:  # defensive wrapper around yfinance runtime behavior
            if stale_snapshot is not None:
                return self._with_note(stale_snapshot, f"stale_cache_fallback:{type(exc).__name__}")
            return FundamentalSnapshot(
                ticker=mention.ticker,
                company_name=mention.company_name,
                data_source="yfinance_error",
                notes=[f"fetch_error:{type(exc).__name__}"],
            )

        self._save_cache_entry(cache_key, snapshot)
        return snapshot

    def fetch_many(self, mentions: list[TickerMention], max_workers: int | None = None) -> dict[str, FundamentalSnapshot]:
        if not mentions:
            return {}

        unique_mentions: dict[str, TickerMention] = {}
        for mention in mentions:
            unique_mentions.setdefault(mention.ticker, mention)

        items = list(unique_mentions.items())
        workers = min(max_workers or self.max_workers, len(items))
        if workers <= 1:
            return {ticker: self.fetch(mention) for ticker, mention in items}

        results: dict[str, FundamentalSnapshot] = {}
        with ThreadPoolExecutor(max_workers=workers) as pool:
            future_to_ticker = {
                pool.submit(self.fetch, mention): ticker
                for ticker, mention in items
            }
            for future in as_completed(future_to_ticker):
                ticker = future_to_ticker[future]
                results[ticker] = future.result()
        return results

    def _fetch_live(self, mention: TickerMention) -> FundamentalSnapshot:
        try:
            import yfinance as yf
        except ImportError:
            return FundamentalSnapshot(
                ticker=mention.ticker,
                company_name=mention.company_name,
                data_source="unavailable",
                notes=["yfinance_not_installed"],
            )

        ticker = yf.Ticker(mention.ticker)
        info = ticker.info or {}
        fast_info = getattr(ticker, "fast_info", None)

        def _pick(*keys: str) -> object | None:
            for key in keys:
                value = info.get(key)
                if value is not None:
                    return value
            return None

        current_price = _pick("currentPrice", "regularMarketPrice")
        if current_price is None and fast_info is not None:
            current_price = getattr(fast_info, "last_price", None)

        market_cap = _pick("marketCap")
        if market_cap is None and fast_info is not None:
            market_cap = getattr(fast_info, "market_cap", None)

        snapshot = FundamentalSnapshot(
            ticker=mention.ticker,
            company_name=_pick("longName", "shortName") or mention.company_name,
            checked_at=utc_now_iso(),
            currency=_pick("currency", "financialCurrency"),
            current_price=_as_float(current_price),
            market_cap=_as_float(market_cap),
            trailing_pe=_as_float(_pick("trailingPE")),
            forward_pe=_as_float(_pick("forwardPE")),
            price_to_book=_as_float(_pick("priceToBook")),
            revenue_growth=_as_float(_pick("revenueGrowth")),
            earnings_growth=_as_float(_pick("earningsGrowth")),
            operating_margin=_as_float(_pick("operatingMargins")),
            return_on_equity=_as_float(_pick("returnOnEquity")),
            debt_to_equity=_as_float(_pick("debtToEquity")),
            fifty_two_week_change=_as_float(_pick("52WeekChange", "fiftyTwoWeekChange")),
            data_source="yfinance",
            notes=[],
        )

        if snapshot.company_name is None:
            snapshot = replace(snapshot, company_name=mention.company_name)
        if snapshot.current_price is None:
            snapshot.notes.append("missing_current_price")
        if snapshot.revenue_growth is None:
            snapshot.notes.append("missing_revenue_growth")
        return snapshot

    def _cache_key(self, ticker: str) -> str:
        return SAFE_CACHE_KEY_RE.sub("_", ticker.upper())

    def _cache_path(self, cache_key: str) -> Path:
        return self.cache_root / f"{cache_key}.json"

    def _load_cache_entry(self, cache_key: str) -> dict[str, object] | None:
        cached = self._memory_cache_get(cache_key)
        if cached is not None:
            return cached

        path = self._cache_path(cache_key)
        payload = read_json(path, None)
        if payload is None:
            return None
        self._memory_cache_put(cache_key, payload)
        return payload

    def _save_cache_entry(self, cache_key: str, snapshot: FundamentalSnapshot) -> None:
        payload = {
            "cached_at": utc_now_iso(),
            "snapshot": asdict(snapshot),
        }
        self._memory_cache_put(cache_key, payload)
        write_json(self._cache_path(cache_key), payload)

    def _snapshot_from_entry(self, entry: dict[str, object]) -> FundamentalSnapshot:
        snapshot_payload = entry.get("snapshot", {}) if isinstance(entry, dict) else {}
        return FundamentalSnapshot(**snapshot_payload)

    def _is_stale(self, entry: dict[str, object]) -> bool:
        cached_at = entry.get("cached_at")
        if not isinstance(cached_at, str) or not cached_at:
            return True
        try:
            cached_time = datetime.fromisoformat(cached_at)
        except ValueError:
            return True
        age = datetime.now(timezone.utc) - cached_time
        return age.total_seconds() > self.max_age_hours * 3600

    @staticmethod
    def _with_note(snapshot: FundamentalSnapshot, note: str) -> FundamentalSnapshot:
        notes = list(snapshot.notes)
        if note not in notes:
            notes.append(note)
        return replace(snapshot, notes=notes)

    def _memory_cache_get(self, cache_key: str) -> dict[str, object] | None:
        if self.max_memory_entries == 0:
            return None
        with self._lock:
            cached = self._memory_cache.get(cache_key)
            if cached is None:
                return None
            self._memory_cache.move_to_end(cache_key)
            return cached

    def _memory_cache_put(self, cache_key: str, payload: dict[str, object]) -> None:
        if self.max_memory_entries == 0:
            return
        with self._lock:
            self._memory_cache[cache_key] = payload
            self._memory_cache.move_to_end(cache_key)
            while len(self._memory_cache) > self.max_memory_entries:
                self._memory_cache.popitem(last=False)


def _as_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
