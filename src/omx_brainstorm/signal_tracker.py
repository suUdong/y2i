from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Sequence

logger = logging.getLogger(__name__)

TRACKING_WINDOWS = [1, 3, 5, 10, 20]  # days after signal
DEFAULT_DB_PATH = Path(".omx/state/signal_tracker.json")


@dataclass(slots=True)
class SignalRecord:
    """One tracked signal: a stock mentioned by a channel with price tracking."""
    ticker: str
    company_name: str | None
    channel_slug: str
    signal_date: str  # ISO date when video was published
    signal_score: float  # aggregate_score from ranking
    verdict: str  # aggregate_verdict
    entry_date: str | None = None  # first trading date on/after signal_date
    entry_price: float | None = None  # close price used as the tracking baseline
    returns: dict[str, float | None] = field(default_factory=dict)
    # returns keys: "1d", "3d", "5d", "10d", "20d" -> pct return or None if not yet available
    latest_price: float | None = None
    latest_price_date: str | None = None
    price_path: list[dict[str, Any]] = field(default_factory=list)
    price_target: dict[str, Any] | None = None
    target_progress_pct: float | None = None
    target_distance_pct: float | None = None
    target_hit: bool = False
    target_hit_date: str | None = None
    recorded_at: str = ""
    last_updated: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SignalRecord:
        return cls(**{k: v for k, v in data.items() if k in cls.__slots__})


@dataclass(slots=True)
class AccuracyStats:
    """Accuracy statistics for a group of signals."""
    total_signals: int = 0
    signals_with_price: int = 0
    signals_with_price_1d: int = 0
    signals_with_price_3d: int = 0
    signals_with_price_5d: int = 0
    hit_rate_1d: float | None = None
    hit_rate_3d: float | None = None
    hit_rate_5d: float | None = None  # % of signals with positive 5d return
    hit_rate_10d: float | None = None
    avg_return_1d: float | None = None
    avg_return_3d: float | None = None
    avg_return_5d: float | None = None
    avg_return_10d: float | None = None
    best_signal: str | None = None  # ticker of best performing signal
    worst_signal: str | None = None
    avg_signal_score: float | None = None
    window_stats: dict[str, dict[str, float | int | None]] = field(default_factory=dict)
    target_count: int = 0
    target_hits: int = 0
    pending_targets: int = 0
    target_hit_rate: float | None = None
    avg_target_progress_pct: float | None = None
    best_target_ticker: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class SignalTrackerDB:
    """Persistent signal→price tracking database."""

    def __init__(self, db_path: Path = DEFAULT_DB_PATH):
        self.db_path = db_path
        self._records: list[SignalRecord] = []
        self._load()

    def _load(self) -> None:
        if self.db_path.exists():
            try:
                data = json.loads(self.db_path.read_text(encoding="utf-8"))
                self._records = [SignalRecord.from_dict(r) for r in data.get("signals", [])]
            except (json.JSONDecodeError, KeyError):
                logger.warning("Corrupt signal tracker DB, starting fresh")
                self._records = []
        else:
            self._records = []

    def _save(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"signals": [r.to_dict() for r in self._records], "updated_at": datetime.now(timezone.utc).isoformat()}
        self.db_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    @property
    def records(self) -> list[SignalRecord]:
        return list(self._records)

    def _record_key(self, ticker: str, channel_slug: str, signal_date: str) -> str:
        return f"{ticker}|{channel_slug}|{signal_date}"

    def _existing_keys(self) -> set[str]:
        return {self._record_key(r.ticker, r.channel_slug, r.signal_date) for r in self._records}

    def add_record(self, record: SignalRecord) -> bool:
        """Add a signal record if not already tracked. Returns True if added."""
        key = self._record_key(record.ticker, record.channel_slug, record.signal_date)
        if key in self._existing_keys():
            return False
        now = datetime.now(timezone.utc).isoformat()
        record.recorded_at = now
        record.last_updated = now
        if not record.returns:
            record.returns = {f"{w}d": None for w in TRACKING_WINDOWS}
        self._records.append(record)
        self._save()
        return True

    def get_records_needing_update(self, today: date | None = None) -> list[SignalRecord]:
        """Return records that have unfilled price windows that should be fillable by now."""
        today = today or date.today()
        needs_update = []
        for record in self._records:
            if record.entry_date is None or record.entry_price is None or record.entry_price <= 0:
                needs_update.append(record)
                continue
            signal_dt = date.fromisoformat(record.signal_date[:10])
            days_elapsed = (today - signal_dt).days
            for window in TRACKING_WINDOWS:
                key = f"{window}d"
                if record.returns.get(key) is None and days_elapsed >= window:
                    needs_update.append(record)
                    break
        return needs_update

    def update_returns(self, ticker: str, channel_slug: str, signal_date: str, returns: dict[str, float | None]) -> bool:
        """Update return data for a specific record."""
        for record in self._records:
            if record.ticker == ticker and record.channel_slug == channel_slug and record.signal_date == signal_date:
                record.returns.update(returns)
                record.last_updated = datetime.now(timezone.utc).isoformat()
                self._save()
                return True
        return False

    def accuracy_report(self, channel_slug: str | None = None) -> AccuracyStats:
        """Compute accuracy stats, optionally filtered by channel."""
        filtered = [r for r in self._records if channel_slug is None or r.channel_slug == channel_slug]
        if not filtered:
            return AccuracyStats()

        window_stats: dict[str, dict[str, float | int | None]] = {}
        for window in TRACKING_WINDOWS:
            key = f"{window}d"
            with_window = [r for r in filtered if r.returns.get(key) is not None]
            if not with_window:
                window_stats[key] = {
                    "tracked": 0,
                    "coverage_pct": 0.0,
                    "hit_rate": None,
                    "avg_return": None,
                }
                continue
            hits = sum(1 for r in with_window if (r.returns.get(key) or 0) > 0)
            avg_return = sum(float(r.returns[key] or 0) for r in with_window) / len(with_window)
            window_stats[key] = {
                "tracked": len(with_window),
                "coverage_pct": round(len(with_window) / len(filtered) * 100, 1),
                "hit_rate": round(hits / len(with_window) * 100, 1),
                "avg_return": round(avg_return, 2),
            }

        def _window_value(window_key: str, field: str) -> float | int | None:
            return (window_stats.get(window_key, {}) or {}).get(field)

        signals_with_price_1d = int(_window_value("1d", "tracked") or 0)
        signals_with_price_3d = int(_window_value("3d", "tracked") or 0)
        signals_with_price_5d = int(_window_value("5d", "tracked") or 0)
        hit_rate_1d = _window_value("1d", "hit_rate")
        hit_rate_3d = _window_value("3d", "hit_rate")
        hit_rate_5d = window_stats["5d"]["hit_rate"]
        avg_return_5d = window_stats["5d"]["avg_return"]
        hit_rate_10d = window_stats["10d"]["hit_rate"]
        avg_return_10d = window_stats["10d"]["avg_return"]
        avg_return_1d = _window_value("1d", "avg_return")
        avg_return_3d = _window_value("3d", "avg_return")
        with_5d = [r for r in filtered if r.returns.get("5d") is not None]
        best = max(with_5d, key=lambda r: r.returns.get("5d") or -999) if with_5d else None
        worst = min(with_5d, key=lambda r: r.returns.get("5d") or 999) if with_5d else None
        target_records = [r for r in filtered if _target_price_from_record(r) is not None]
        progressed_targets = [r for r in target_records if r.target_progress_pct is not None]
        best_target = max(progressed_targets, key=lambda r: r.target_progress_pct or -999) if progressed_targets else None
        target_hits = sum(1 for r in target_records if r.target_hit)

        return AccuracyStats(
            total_signals=len(filtered),
            signals_with_price=signals_with_price_5d,
            signals_with_price_1d=signals_with_price_1d,
            signals_with_price_3d=signals_with_price_3d,
            signals_with_price_5d=signals_with_price_5d,
            hit_rate_1d=hit_rate_1d,
            hit_rate_3d=hit_rate_3d,
            hit_rate_5d=hit_rate_5d,
            hit_rate_10d=hit_rate_10d,
            avg_return_1d=avg_return_1d,
            avg_return_3d=avg_return_3d,
            avg_return_5d=avg_return_5d,
            avg_return_10d=avg_return_10d,
            best_signal=best.ticker if best else None,
            worst_signal=worst.ticker if worst else None,
            avg_signal_score=round(sum(r.signal_score for r in filtered) / len(filtered), 1) if filtered else None,
            window_stats=window_stats,
            target_count=len(target_records),
            target_hits=target_hits,
            pending_targets=sum(1 for r in target_records if not r.target_hit),
            target_hit_rate=round(target_hits / len(target_records) * 100, 1) if target_records else None,
            avg_target_progress_pct=(
                round(sum(float(r.target_progress_pct or 0) for r in progressed_targets) / len(progressed_targets), 2)
                if progressed_targets else None
            ),
            best_target_ticker=best_target.ticker if best_target else None,
        )

    def recent_records(self, limit: int = 10, channel_slug: str | None = None) -> list[dict[str, Any]]:
        """Return recent tracked signals, newest signal_date first."""
        filtered = [r for r in self._records if channel_slug is None or r.channel_slug == channel_slug]
        filtered.sort(
            key=lambda r: (
                r.signal_date,
                r.signal_score,
                r.recorded_at,
                r.ticker,
            ),
            reverse=True,
        )
        return [r.to_dict() for r in filtered[:limit]]


def record_signals_from_output(db: SignalTrackerDB, output_path: Path, history_provider: Any = None) -> int:
    """Ingest a channel JSON output file and create SignalRecords for ranked stocks.
    Returns number of new records added."""
    if history_provider is None:
        from .backtest import YFinanceHistoryProvider
        history_provider = YFinanceHistoryProvider()

    data = json.loads(output_path.read_text(encoding="utf-8"))
    channel_slug = data.get("channel_slug", "")
    ranking = data.get("cross_video_ranking", [])
    added = 0
    for stock in ranking:
        signal_date = stock.get("first_signal_at") or stock.get("last_signal_at")
        if not signal_date:
            continue
        normalized_signal_date = signal_date[:10]
        entry_point = _fetch_entry_point(history_provider, stock["ticker"], normalized_signal_date)
        record = SignalRecord(
            ticker=stock["ticker"],
            company_name=stock.get("company_name"),
            channel_slug=channel_slug,
            signal_date=normalized_signal_date,
            signal_score=float(stock.get("aggregate_score", 0)),
            verdict=stock.get("aggregate_verdict", ""),
            entry_date=entry_point.date if entry_point else None,
            entry_price=entry_point.close if entry_point else None,
            price_target=dict(stock.get("price_target") or {}) or None,
        )
        if db.add_record(record):
            added += 1
    return added


def update_price_snapshots(db: SignalTrackerDB, history_provider: Any = None) -> int:
    """Fetch current prices and update return fields for records with unfilled windows.
    Returns number of records updated."""
    if history_provider is None:
        from .backtest import YFinanceHistoryProvider
        history_provider = YFinanceHistoryProvider()

    records = db.get_records_needing_update()
    updated = 0
    today = date.today()
    dirty = False

    for record in records:
        signal_dt = date.fromisoformat(record.signal_date[:10])
        days_elapsed = (today - signal_dt).days

        # Fetch price history from signal date to today
        try:
            history = history_provider.get_price_history(
                record.ticker,
                record.signal_date[:10],
                today.isoformat(),
            )
        except Exception as exc:
            logger.warning("Price fetch failed for %s: %s", record.ticker, exc)
            continue

        if not history:
            continue

        entry_point = _resolve_entry_point(history, record.signal_date[:10])
        if entry_point is None or entry_point.close <= 0:
            continue

        entry_changed = (
            record.entry_price is None
            or record.entry_price <= 0
            or record.entry_date != entry_point.date
            or abs(record.entry_price - entry_point.close) > 1e-9
        )
        if entry_changed:
            record.entry_date = entry_point.date
            record.entry_price = entry_point.close

        price_path = _build_price_path(
            history,
            record.signal_date[:10],
            record.entry_date or entry_point.date,
            record.entry_price or entry_point.close,
        )
        latest_path_point = price_path[-1] if price_path else None
        path_changed = (
            price_path != record.price_path
            or record.latest_price != (latest_path_point.get("close") if latest_path_point else None)
            or record.latest_price_date != (latest_path_point.get("date") if latest_path_point else None)
        )

        prices_by_day = {}
        for point in history:
            dt = date.fromisoformat(point.date[:10])
            offset = (dt - signal_dt).days
            prices_by_day[offset] = point.close

        new_returns: dict[str, float | None] = {}
        for window in TRACKING_WINDOWS:
            key = f"{window}d"
            if record.returns.get(key) is not None and not entry_changed:
                continue
            if days_elapsed < window:
                continue
            # Find closest trading day to the window
            target_price = None
            for offset in range(window, window + 5):  # look up to 5 days forward for trading day
                if offset in prices_by_day:
                    target_price = prices_by_day[offset]
                    break
            if target_price is not None and record.entry_price and record.entry_price > 0:
                new_returns[key] = round((target_price - record.entry_price) / record.entry_price * 100, 2)

        if path_changed:
            record.price_path = price_path
            if latest_path_point:
                record.latest_price = float(latest_path_point["close"])
                record.latest_price_date = str(latest_path_point["date"])
        if latest_path_point and record.latest_price is not None:
            _update_target_tracking(record, latest_price=record.latest_price, latest_price_date=record.latest_price_date)

        if new_returns or entry_changed or path_changed:
            if new_returns:
                record.returns.update(new_returns)
            record.last_updated = datetime.now(timezone.utc).isoformat()
            dirty = True
            updated += 1

    if dirty:
        db._save()
    return updated


def _fetch_entry_point(history_provider: Any, ticker: str, signal_date: str) -> Any | None:
    """Resolve the first available close on/after the signal date."""
    try:
        history = history_provider.get_price_history(ticker, signal_date, date.today().isoformat())
    except Exception as exc:
        logger.warning("Entry price fetch failed for %s: %s", ticker, exc)
        return None
    return _resolve_entry_point(history, signal_date)


def _resolve_entry_point(history: Sequence[Any], signal_date: str) -> Any | None:
    signal_dt = date.fromisoformat(signal_date[:10])
    for point in sorted(history, key=lambda item: item.date):
        if date.fromisoformat(point.date[:10]) >= signal_dt:
            return point
    return None


def _build_price_path(
    history: Sequence[Any],
    signal_date: str,
    entry_date: str,
    entry_price: float,
) -> list[dict[str, Any]]:
    signal_dt = date.fromisoformat(signal_date[:10])
    entry_dt = date.fromisoformat(entry_date[:10])
    path: list[dict[str, Any]] = []

    for point in sorted(history, key=lambda item: item.date):
        point_dt = date.fromisoformat(point.date[:10])
        if point_dt < signal_dt:
            continue
        close = float(point.close)
        if close <= 0:
            continue
        return_pct = None
        if entry_price > 0 and point_dt >= entry_dt:
            return_pct = round((close - entry_price) / entry_price * 100, 2)
        path.append(
            {
                "date": point.date[:10],
                "close": round(close, 4),
                "days_from_signal": (point_dt - signal_dt).days,
                "days_from_entry": (point_dt - entry_dt).days,
                "return_pct": return_pct,
            }
        )

    return path


def _target_price_from_record(record: SignalRecord) -> float | None:
    payload = record.price_target or {}
    value = payload.get("target_price")
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _update_target_tracking(record: SignalRecord, *, latest_price: float | None, latest_price_date: str | None) -> None:
    target_price = _target_price_from_record(record)
    if target_price is None or latest_price is None or latest_price <= 0 or record.entry_price is None or record.entry_price <= 0:
        return

    if abs(target_price - record.entry_price) <= 1e-9:
        record.target_progress_pct = 100.0
        record.target_distance_pct = 0.0
        record.target_hit = True
        record.target_hit_date = record.target_hit_date or latest_price_date
        return

    if target_price > record.entry_price:
        progress = (latest_price - record.entry_price) / (target_price - record.entry_price) * 100
        hit = latest_price >= target_price
    else:
        progress = (record.entry_price - latest_price) / (record.entry_price - target_price) * 100
        hit = latest_price <= target_price

    record.target_progress_pct = round(progress, 2)
    record.target_distance_pct = round((target_price - latest_price) / latest_price * 100, 2)
    record.target_hit = hit
    if hit and not record.target_hit_date:
        record.target_hit_date = latest_price_date
