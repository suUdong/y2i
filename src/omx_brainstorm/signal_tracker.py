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
    entry_price: float | None = None  # price on signal_date
    returns: dict[str, float | None] = field(default_factory=dict)
    # returns keys: "1d", "3d", "5d", "10d", "20d" -> pct return or None if not yet available
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
    hit_rate_5d: float | None = None  # % of signals with positive 5d return
    hit_rate_10d: float | None = None
    avg_return_5d: float | None = None
    avg_return_10d: float | None = None
    best_signal: str | None = None  # ticker of best performing signal
    worst_signal: str | None = None

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

        with_5d = [r for r in filtered if r.returns.get("5d") is not None]
        with_10d = [r for r in filtered if r.returns.get("10d") is not None]

        hit_rate_5d = None
        avg_return_5d = None
        if with_5d:
            hits_5d = sum(1 for r in with_5d if (r.returns.get("5d") or 0) > 0)
            hit_rate_5d = round(hits_5d / len(with_5d) * 100, 1)
            avg_return_5d = round(sum(r.returns["5d"] for r in with_5d) / len(with_5d), 2)

        hit_rate_10d = None
        avg_return_10d = None
        if with_10d:
            hits_10d = sum(1 for r in with_10d if (r.returns.get("10d") or 0) > 0)
            hit_rate_10d = round(hits_10d / len(with_10d) * 100, 1)
            avg_return_10d = round(sum(r.returns["10d"] for r in with_10d) / len(with_10d), 2)

        best = max(with_5d, key=lambda r: r.returns.get("5d") or -999) if with_5d else None
        worst = min(with_5d, key=lambda r: r.returns.get("5d") or 999) if with_5d else None

        return AccuracyStats(
            total_signals=len(filtered),
            signals_with_price=len(with_5d),
            hit_rate_5d=hit_rate_5d,
            hit_rate_10d=hit_rate_10d,
            avg_return_5d=avg_return_5d,
            avg_return_10d=avg_return_10d,
            best_signal=best.ticker if best else None,
            worst_signal=worst.ticker if worst else None,
        )


def record_signals_from_output(db: SignalTrackerDB, output_path: Path) -> int:
    """Ingest a channel JSON output file and create SignalRecords for ranked stocks.
    Returns number of new records added."""
    data = json.loads(output_path.read_text(encoding="utf-8"))
    channel_slug = data.get("channel_slug", "")
    ranking = data.get("cross_video_ranking", [])
    added = 0
    for stock in ranking:
        signal_date = stock.get("first_signal_at") or stock.get("last_signal_at")
        if not signal_date:
            continue
        record = SignalRecord(
            ticker=stock["ticker"],
            company_name=stock.get("company_name"),
            channel_slug=channel_slug,
            signal_date=signal_date[:10],
            signal_score=float(stock.get("aggregate_score", 0)),
            verdict=stock.get("aggregate_verdict", ""),
            entry_price=stock.get("latest_price"),
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

    for record in records:
        if record.entry_price is None or record.entry_price <= 0:
            continue
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

        prices_by_day = {}
        for point in history:
            dt = date.fromisoformat(point.date[:10])
            offset = (dt - signal_dt).days
            prices_by_day[offset] = point.close

        new_returns: dict[str, float | None] = {}
        for window in TRACKING_WINDOWS:
            key = f"{window}d"
            if record.returns.get(key) is not None:
                continue
            if days_elapsed < window:
                continue
            # Find closest trading day to the window
            target_price = None
            for offset in range(window, window + 5):  # look up to 5 days forward for trading day
                if offset in prices_by_day:
                    target_price = prices_by_day[offset]
                    break
            if target_price is not None and record.entry_price > 0:
                new_returns[key] = round((target_price - record.entry_price) / record.entry_price * 100, 2)

        if new_returns:
            db.update_returns(record.ticker, record.channel_slug, record.signal_date, new_returns)
            updated += 1

    return updated
