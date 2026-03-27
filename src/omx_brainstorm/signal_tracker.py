from __future__ import annotations

import json
import logging
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone
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
    source_video_id: str | None = None
    source_title: str | None = None
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
        payload = {k: v for k, v in data.items() if k in cls.__slots__}
        if payload.get("target_hit_date") and not payload.get("target_hit"):
            payload["target_hit"] = True
        return cls(**payload)


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
    avg_directional_return_1d: float | None = None
    avg_directional_return_3d: float | None = None
    avg_directional_return_5d: float | None = None
    avg_directional_return_10d: float | None = None
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

    def _record_key(self, ticker: str, channel_slug: str, signal_date: str, source_video_id: str | None = None) -> str:
        return f"{ticker}|{channel_slug}|{signal_date}|{source_video_id or ''}"

    def _existing_keys(self) -> set[str]:
        return {self._record_key(r.ticker, r.channel_slug, r.signal_date, r.source_video_id) for r in self._records}

    def add_record(self, record: SignalRecord) -> bool:
        """Add a signal record if not already tracked. Returns True if added."""
        key = self._record_key(record.ticker, record.channel_slug, record.signal_date, record.source_video_id)
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
            target_price = _target_price_from_record(record)
            if target_price is not None:
                latest_date = (record.latest_price_date or "")[:10]
                if not record.target_hit or latest_date != today.isoformat():
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
        return _build_accuracy_stats(filtered)

    def ticker_accuracy_summary(self, *, limit: int | None = None, min_signals: int = 1) -> list[dict[str, Any]]:
        """Compute per-ticker accuracy summaries ranked by mature sample size and hit rate."""
        grouped: dict[str, list[SignalRecord]] = defaultdict(list)
        company_names: dict[str, str | None] = {}
        for record in self._records:
            grouped[record.ticker].append(record)
            company_names.setdefault(record.ticker, record.company_name)

        summaries: list[dict[str, Any]] = []
        for ticker, records in grouped.items():
            stats = _build_accuracy_stats(records).to_dict()
            total_signals = int(stats.get("total_signals", 0) or 0)
            if total_signals < min_signals:
                continue
            channels = sorted({record.channel_slug for record in records if record.channel_slug})
            stats.update(
                {
                    "ticker": ticker,
                    "company_name": company_names.get(ticker),
                    "channel_count": len(channels),
                    "channels": channels,
                    "bullish_signals": sum(1 for record in records if _signal_direction(record.verdict) > 0),
                    "bearish_signals": sum(1 for record in records if _signal_direction(record.verdict) < 0),
                    "first_signal_at": min((record.signal_date for record in records), default=None),
                    "last_signal_at": max((record.signal_date for record in records), default=None),
                }
            )
            summaries.append(stats)

        summaries.sort(
            key=lambda item: (
                -int(item.get("signals_with_price_5d", 0) or 0),
                -int(item.get("total_signals", 0) or 0),
                -(float(item.get("hit_rate_5d")) if item.get("hit_rate_5d") is not None else -1.0),
                -(float(item.get("avg_directional_return_5d")) if item.get("avg_directional_return_5d") is not None else -999.0),
                str(item.get("ticker", "")),
            )
        )
        return summaries[:limit] if limit is not None else summaries

    def recent_records(self, limit: int = 10, channel_slug: str | None = None, *, target_only: bool = False) -> list[dict[str, Any]]:
        """Return recent tracked signals, newest signal_date first."""
        filtered = [r for r in self._records if channel_slug is None or r.channel_slug == channel_slug]
        if target_only:
            filtered = [r for r in filtered if _target_price_from_record(r) is not None]
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
    return record_signals_from_ranking(
        db,
        channel_slug=str(data.get("channel_slug", "")),
        ranking=data.get("cross_video_ranking", []),
        history_provider=history_provider,
    )


def record_signals_from_ranking(
    db: SignalTrackerDB,
    channel_slug: str,
    ranking: Sequence[Any],
    history_provider: Any = None,
) -> int:
    """Create SignalRecords directly from ranking objects or dict payloads."""
    if history_provider is None:
        from .backtest import YFinanceHistoryProvider
        history_provider = YFinanceHistoryProvider()

    added = 0
    for stock in ranking:
        if hasattr(stock, "to_dict"):
            payload = dict(stock.to_dict())
        elif isinstance(stock, dict):
            payload = dict(stock)
        else:
            continue
        ticker = str(payload.get("ticker") or "").strip()
        signal_date = _normalize_signal_date(payload.get("first_signal_at") or payload.get("signal_date") or payload.get("last_signal_at"))
        if not ticker or not signal_date:
            continue
        entry_point = _fetch_entry_point(history_provider, ticker, signal_date)
        record = SignalRecord(
            ticker=ticker,
            company_name=payload.get("company_name"),
            channel_slug=channel_slug,
            signal_date=signal_date,
            signal_score=float(payload.get("aggregate_score", payload.get("signal_score", 0)) or 0),
            verdict=str(payload.get("aggregate_verdict", payload.get("verdict", "")) or ""),
            entry_date=entry_point.date if entry_point else None,
            entry_price=entry_point.close if entry_point else None,
            price_target=dict(payload.get("price_target") or {}) or None,
        )
        if db.add_record(record):
            added += 1
    return added


def record_signals_from_rows(
    db: SignalTrackerDB,
    channel_slug: str,
    rows: Sequence[dict[str, Any]],
    history_provider: Any = None,
) -> int:
    """Create SignalRecords from per-video heuristic rows for exhaustive backtests."""
    if history_provider is None:
        from .backtest import YFinanceHistoryProvider
        history_provider = YFinanceHistoryProvider()

    added = 0
    for row in rows:
        signal_date = _normalize_signal_date(row.get("published_at") or row.get("signal_date"))
        if not signal_date:
            continue
        source_video_id = str(row.get("video_id") or "").strip() or None
        source_title = str(row.get("title") or "").strip() or None
        row_signal_score = float(row.get("signal_score", 0) or 0)
        for stock in row.get("stocks", []) or []:
            ticker = str(stock.get("ticker") or "").strip()
            if not ticker:
                continue
            entry_point = _fetch_entry_point(history_provider, ticker, signal_date)
            record = SignalRecord(
                ticker=ticker,
                company_name=stock.get("company_name"),
                channel_slug=channel_slug,
                signal_date=signal_date,
                source_video_id=source_video_id,
                source_title=source_title,
                signal_score=float(stock.get("signal_strength_score", stock.get("final_score", row_signal_score)) or row_signal_score),
                verdict=str(stock.get("final_verdict", stock.get("basic_signal_verdict", row.get("video_signal_class", ""))) or ""),
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


def _record_target_hit(record: SignalRecord) -> bool:
    return bool(record.target_hit or record.target_hit_date)


def _signal_direction(verdict: str | None) -> int:
    normalized = str(verdict or "").strip().upper()
    if normalized in {"SELL", "STRONG_SELL", "UNDERPERFORM", "AVOID", "SHORT"}:
        return -1
    return 1


def _directional_return(record: SignalRecord, window_key: str) -> float | None:
    value = record.returns.get(window_key)
    if value is None:
        return None
    return round(float(value) * _signal_direction(record.verdict), 2)


def _window_value(window_stats: dict[str, dict[str, float | int | None]], window_key: str, field: str) -> float | int | None:
    return (window_stats.get(window_key, {}) or {}).get(field)


def _build_accuracy_stats(filtered: list[SignalRecord]) -> AccuracyStats:
    if not filtered:
        return AccuracyStats()

    window_stats: dict[str, dict[str, float | int | None]] = {}
    for window in TRACKING_WINDOWS:
        key = f"{window}d"
        with_window = [record for record in filtered if record.returns.get(key) is not None]
        if not with_window:
            window_stats[key] = {
                "tracked": 0,
                "coverage_pct": 0.0,
                "hit_rate": None,
                "avg_return": None,
                "avg_directional_return": None,
            }
            continue

        raw_returns = [float(record.returns.get(key) or 0) for record in with_window]
        directional_returns = [float(_directional_return(record, key) or 0) for record in with_window]
        hits = sum(1 for value in directional_returns if value > 0)
        window_stats[key] = {
            "tracked": len(with_window),
            "coverage_pct": round(len(with_window) / len(filtered) * 100, 1),
            "hit_rate": round(hits / len(with_window) * 100, 1),
            "avg_return": round(sum(raw_returns) / len(raw_returns), 2),
            "avg_directional_return": round(sum(directional_returns) / len(directional_returns), 2),
        }

    signals_with_price_1d = int(_window_value(window_stats, "1d", "tracked") or 0)
    signals_with_price_3d = int(_window_value(window_stats, "3d", "tracked") or 0)
    signals_with_price_5d = int(_window_value(window_stats, "5d", "tracked") or 0)
    with_5d = [record for record in filtered if record.returns.get("5d") is not None]
    best = max(with_5d, key=lambda record: _directional_return(record, "5d") or -999) if with_5d else None
    worst = min(with_5d, key=lambda record: _directional_return(record, "5d") or 999) if with_5d else None
    target_records = [record for record in filtered if _target_price_from_record(record) is not None]
    progressed_targets = [record for record in target_records if record.target_progress_pct is not None]
    best_target = max(progressed_targets, key=lambda record: record.target_progress_pct or -999) if progressed_targets else None
    target_hits = sum(1 for record in target_records if _record_target_hit(record))

    return AccuracyStats(
        total_signals=len(filtered),
        signals_with_price=signals_with_price_5d,
        signals_with_price_1d=signals_with_price_1d,
        signals_with_price_3d=signals_with_price_3d,
        signals_with_price_5d=signals_with_price_5d,
        hit_rate_1d=_window_value(window_stats, "1d", "hit_rate"),
        hit_rate_3d=_window_value(window_stats, "3d", "hit_rate"),
        hit_rate_5d=_window_value(window_stats, "5d", "hit_rate"),
        hit_rate_10d=_window_value(window_stats, "10d", "hit_rate"),
        avg_return_1d=_window_value(window_stats, "1d", "avg_return"),
        avg_return_3d=_window_value(window_stats, "3d", "avg_return"),
        avg_return_5d=_window_value(window_stats, "5d", "avg_return"),
        avg_return_10d=_window_value(window_stats, "10d", "avg_return"),
        avg_directional_return_1d=_window_value(window_stats, "1d", "avg_directional_return"),
        avg_directional_return_3d=_window_value(window_stats, "3d", "avg_directional_return"),
        avg_directional_return_5d=_window_value(window_stats, "5d", "avg_directional_return"),
        avg_directional_return_10d=_window_value(window_stats, "10d", "avg_directional_return"),
        best_signal=best.ticker if best else None,
        worst_signal=worst.ticker if worst else None,
        avg_signal_score=round(sum(record.signal_score for record in filtered) / len(filtered), 1) if filtered else None,
        window_stats=window_stats,
        target_count=len(target_records),
        target_hits=target_hits,
        pending_targets=sum(1 for record in target_records if not _record_target_hit(record)),
        target_hit_rate=round(target_hits / len(target_records) * 100, 1) if target_records else None,
        avg_target_progress_pct=(
            round(sum(float(record.target_progress_pct or 0) for record in progressed_targets) / len(progressed_targets), 2)
            if progressed_targets else None
        ),
        best_target_ticker=best_target.ticker if best_target else None,
    )


def build_signal_accuracy_summary(
    db: SignalTrackerDB,
    *,
    channel_metadata: dict[str, dict[str, Any]] | None = None,
    top_tickers: int = 20,
    recent_limit: int = 12,
    recent_targets_limit: int = 20,
) -> dict[str, Any]:
    """Build a report payload from tracked signals."""
    channel_metadata = channel_metadata or {}
    channel_slugs = sorted(channel_metadata) or sorted({record.channel_slug for record in db.records if record.channel_slug})
    accuracy_by_channel = {slug: db.accuracy_report(slug).to_dict() for slug in channel_slugs}
    ticker_summaries = db.ticker_accuracy_summary(limit=None)

    channel_leaderboard: list[dict[str, Any]] = []
    if channel_slugs:
        from .channel_quality import compute_channel_quality, compute_dynamic_weights, rank_channels

        quality_input = {
            slug: {
                "display_name": (channel_metadata.get(slug, {}) or {}).get("display_name", slug),
                "actionable_ratio": float((channel_metadata.get(slug, {}) or {}).get("actionable_ratio", 0) or 0),
                "ranking_spearman": (channel_metadata.get(slug, {}) or {}).get("ranking_spearman"),
                "quality_scorecard": dict((channel_metadata.get(slug, {}) or {}).get("quality_scorecard") or {}),
            }
            for slug in channel_slugs
        }
        ranked_reports = rank_channels(compute_channel_quality(quality_input, accuracy_by_channel))
        weight_multipliers = compute_dynamic_weights(ranked_reports)
        for report in ranked_reports:
            item = report.to_dict()
            item["weight_multiplier"] = weight_multipliers.get(report.slug)
            channel_leaderboard.append(item)

    updated_at = max((record.last_updated for record in db.records if record.last_updated), default="")
    return {
        "updated_at": updated_at,
        "overall": db.accuracy_report().to_dict(),
        "by_channel": accuracy_by_channel,
        "by_ticker": {item["ticker"]: item for item in ticker_summaries},
        "channel_leaderboard": channel_leaderboard,
        "ticker_leaderboard": ticker_summaries[:top_tickers],
        "recent_signals": db.recent_records(limit=recent_limit),
        "recent_targets": db.recent_records(limit=recent_targets_limit, target_only=True),
    }


def build_signal_backtest_summary(
    db: SignalTrackerDB,
    *,
    lookback_days: int = 90,
    as_of: date | None = None,
    channel_metadata: dict[str, dict[str, Any]] | None = None,
    top_filters: int = 10,
    min_filter_sample: int = 3,
) -> dict[str, Any]:
    """Build a lookback-bounded signal backtest summary from tracked signals."""
    as_of = as_of or date.today()
    channel_metadata = channel_metadata or {}
    start_date = as_of - timedelta(days=max(0, int(lookback_days) - 1))
    filtered = [
        record
        for record in db.records
        if _record_within_window(record, start_date=start_date, end_date=as_of)
    ]
    filtered.sort(
        key=lambda record: (
            record.signal_date,
            record.signal_score,
            record.channel_slug,
            record.ticker,
        ),
        reverse=True,
    )

    overall = _build_accuracy_stats(filtered).to_dict()
    overall.update(_build_roi_fields(filtered))

    by_channel: dict[str, dict[str, Any]] = {}
    channel_roi_leaderboard: list[dict[str, Any]] = []
    channel_slugs = sorted({record.channel_slug for record in filtered if record.channel_slug})
    for slug in channel_slugs:
        records = [record for record in filtered if record.channel_slug == slug]
        stats = _build_accuracy_stats(records).to_dict()
        stats.update(_build_roi_fields(records))
        metadata = dict(channel_metadata.get(slug, {}) or {})
        entry = {
            "slug": slug,
            "display_name": metadata.get("display_name", slug),
            "actionable_ratio": metadata.get("actionable_ratio"),
            "overall_quality_score": _channel_quality_value(metadata),
            "weight_multiplier": metadata.get("weight_multiplier"),
            "videos_seen": metadata.get("total_videos"),
            "videos_analyzed": metadata.get("analyzed_videos"),
            **stats,
        }
        by_channel[slug] = entry
        channel_roi_leaderboard.append(entry)
    channel_roi_leaderboard.sort(
        key=lambda item: (
            -(float(item.get("compounded_directional_roi_5d")) if item.get("compounded_directional_roi_5d") is not None else -999.0),
            -(float(item.get("avg_directional_return_5d")) if item.get("avg_directional_return_5d") is not None else -999.0),
            -(float(item.get("hit_rate_5d")) if item.get("hit_rate_5d") is not None else -999.0),
            -int(item.get("signals_with_price_5d", 0) or 0),
            str(item.get("slug", "")),
        )
    )

    signals = [_signal_record_to_backtest_row(record, channel_metadata=channel_metadata, as_of=as_of) for record in filtered]
    filter_recommendations = _optimize_signal_filters(
        filtered,
        channel_metadata=channel_metadata,
        top_filters=top_filters,
        min_filter_sample=min_filter_sample,
    )

    return {
        "as_of": as_of.isoformat(),
        "start_date": start_date.isoformat(),
        "lookback_days": int(lookback_days),
        "overall": overall,
        "by_channel": by_channel,
        "channel_roi_leaderboard": channel_roi_leaderboard,
        "filter_recommendations": filter_recommendations,
        "signals": signals,
        "recent_signals": signals[:20],
    }


def save_signal_accuracy_report(summary: dict[str, Any], output_dir: Path, run_id: str) -> tuple[Path, Path]:
    """Persist the signal accuracy summary as JSON and text."""
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / f"signal_accuracy_report_{run_id}.json"
    txt_path = output_dir / f"signal_accuracy_report_{run_id}.txt"
    payload = dict(summary)
    payload.setdefault("generated_at", run_id)
    payload["report_files"] = {
        "json_path": str(json_path),
        "txt_path": str(txt_path),
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    txt_path.write_text(render_signal_accuracy_report_text(payload), encoding="utf-8")
    summary.clear()
    summary.update(payload)
    return json_path, txt_path


def save_signal_backtest_report(summary: dict[str, Any], output_dir: Path, run_id: str) -> tuple[Path, Path]:
    """Persist the lookback-bounded signal backtest summary as JSON and text."""
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / f"signal_backtest_report_{run_id}.json"
    txt_path = output_dir / f"signal_backtest_report_{run_id}.txt"
    payload = dict(summary)
    payload.setdefault("generated_at", run_id)
    payload["report_files"] = {
        "json_path": str(json_path),
        "txt_path": str(txt_path),
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    txt_path.write_text(render_signal_backtest_report_text(payload), encoding="utf-8")
    summary.clear()
    summary.update(payload)
    return json_path, txt_path


def render_signal_accuracy_report_text(summary: dict[str, Any]) -> str:
    """Render a human-readable signal accuracy report."""
    overall = summary.get("overall", {}) if isinstance(summary, dict) else {}
    lines = [
        f"시그널 정확도 리포트 ({summary.get('generated_at', summary.get('updated_at', '-'))})",
        f"업데이트 시각: {summary.get('updated_at', '-')}",
        "",
        "[전체 정확도]",
        f"- 트래킹 신호 수: {int(overall.get('total_signals', 0) or 0)}",
        f"- 1일/3일/5일 표본: {int(overall.get('signals_with_price_1d', 0) or 0)} / {int(overall.get('signals_with_price_3d', 0) or 0)} / {int(overall.get('signals_with_price_5d', 0) or 0)}",
        f"- 1일 적중률: {_fmt_pct(overall.get('hit_rate_1d'))}",
        f"- 3일 적중률: {_fmt_pct(overall.get('hit_rate_3d'))}",
        f"- 5일 적중률: {_fmt_pct(overall.get('hit_rate_5d'))}",
        f"- 10일 적중률: {_fmt_pct(overall.get('hit_rate_10d'))}",
        f"- 5일 평균 실제수익률: {_fmt_pct(overall.get('avg_return_5d'))}",
        f"- 5일 평균 방향수익률: {_fmt_pct(overall.get('avg_directional_return_5d'))}",
        f"- 평균 시그널 점수: {_fmt_scalar(overall.get('avg_signal_score'))}",
        "",
        "[채널 리더보드]",
    ]

    channel_leaderboard = summary.get("channel_leaderboard", []) if isinstance(summary, dict) else []
    if channel_leaderboard:
        for idx, item in enumerate(channel_leaderboard[:10], start=1):
            lines.append(
                f"- {idx}. {item.get('display_name', item.get('slug', '-'))}"
                f" | 5d 적중률 {_fmt_pct(item.get('hit_rate_5d'))}"
                f" | 5d 방향수익률 {_fmt_pct(item.get('avg_directional_return_5d'))}"
                f" | 가중치 {_fmt_scalar(item.get('weight_multiplier'))}"
                f" | 표본 {int(item.get('signals_with_price_5d', 0) or 0)}"
            )
    else:
        lines.append("- 채널 데이터 없음")

    lines.extend(["", "[종목 리더보드]"])
    ticker_leaderboard = summary.get("ticker_leaderboard", []) if isinstance(summary, dict) else []
    if ticker_leaderboard:
        for idx, item in enumerate(ticker_leaderboard[:15], start=1):
            display_name = item.get("company_name") or item.get("ticker", "-")
            lines.append(
                f"- {idx}. {display_name} ({item.get('ticker', '-')})"
                f" | 5d 적중률 {_fmt_pct(item.get('hit_rate_5d'))}"
                f" | 5d 방향수익률 {_fmt_pct(item.get('avg_directional_return_5d'))}"
                f" | 채널 {int(item.get('channel_count', 0) or 0)}"
                f" | 표본 {int(item.get('signals_with_price_5d', 0) or 0)}"
            )
    else:
        lines.append("- 종목 데이터 없음")

    lines.extend(["", "[최근 추적 시그널]"])
    recent_signals = summary.get("recent_signals", []) if isinstance(summary, dict) else []
    if recent_signals:
        for item in recent_signals[:10]:
            returns = item.get("returns", {}) if isinstance(item.get("returns"), dict) else {}
            lines.append(
                f"- {item.get('ticker', '-')} | {item.get('channel_slug', '-')}"
                f" | {item.get('verdict', '-')}"
                f" | 5d {_fmt_pct(returns.get('5d'))}"
                f" | 날짜 {item.get('signal_date', '-')}"
            )
    else:
        lines.append("- 최근 시그널 없음")

    return "\n".join(lines).strip() + "\n"


def render_signal_backtest_report_text(summary: dict[str, Any]) -> str:
    """Render a human-readable lookback backtest report."""
    overall = summary.get("overall", {}) if isinstance(summary, dict) else {}
    lines = [
        f"시그널 백테스트 리포트 ({summary.get('generated_at', summary.get('as_of', '-'))})",
        f"평가 기준일: {summary.get('as_of', '-')}",
        f"룩백 기간: {summary.get('lookback_days', '-')}일",
        f"평가 시작일: {summary.get('start_date', '-')}",
        "",
        "[전체 성과]",
        f"- 시그널 수: {int(overall.get('total_signals', 0) or 0)}",
        f"- 1일/3일/5일 표본: {int(overall.get('signals_with_price_1d', 0) or 0)} / {int(overall.get('signals_with_price_3d', 0) or 0)} / {int(overall.get('signals_with_price_5d', 0) or 0)}",
        f"- 1일 적중률: {_fmt_pct(overall.get('hit_rate_1d'))}",
        f"- 3일 적중률: {_fmt_pct(overall.get('hit_rate_3d'))}",
        f"- 5일 적중률: {_fmt_pct(overall.get('hit_rate_5d'))}",
        f"- 1일 평균수익률: {_fmt_pct(overall.get('avg_return_1d'))}",
        f"- 3일 평균수익률: {_fmt_pct(overall.get('avg_return_3d'))}",
        f"- 5일 평균수익률: {_fmt_pct(overall.get('avg_return_5d'))}",
        f"- 1일 방향수익률: {_fmt_pct(overall.get('avg_directional_return_1d'))}",
        f"- 3일 방향수익률: {_fmt_pct(overall.get('avg_directional_return_3d'))}",
        f"- 5일 방향수익률: {_fmt_pct(overall.get('avg_directional_return_5d'))}",
        f"- 1일 복리 ROI: {_fmt_pct(overall.get('compounded_directional_roi_1d'))}",
        f"- 3일 복리 ROI: {_fmt_pct(overall.get('compounded_directional_roi_3d'))}",
        f"- 5일 복리 ROI: {_fmt_pct(overall.get('compounded_directional_roi_5d'))}",
        "",
        "[채널 ROI 리포트]",
    ]

    channel_rows = summary.get("channel_roi_leaderboard", []) if isinstance(summary, dict) else []
    if channel_rows:
        for idx, item in enumerate(channel_rows[:15], start=1):
            lines.append(
                f"- {idx}. {item.get('display_name', item.get('slug', '-'))}"
                f" | 5d ROI {_fmt_pct(item.get('compounded_directional_roi_5d'))}"
                f" | 5d 방향수익률 {_fmt_pct(item.get('avg_directional_return_5d'))}"
                f" | 5d 적중률 {_fmt_pct(item.get('hit_rate_5d'))}"
                f" | 표본 {int(item.get('signals_with_price_5d', 0) or 0)}"
                f" | 품질 {_fmt_scalar(item.get('overall_quality_score'))}"
            )
    else:
        lines.append("- 채널 데이터 없음")

    lines.extend(["", "[최적 필터 조건]"])
    filters = summary.get("filter_recommendations", []) if isinstance(summary, dict) else []
    if filters:
        for idx, item in enumerate(filters[:10], start=1):
            lines.append(
                f"- {idx}. {item.get('label', '-')}"
                f" | 5d ROI {_fmt_pct(item.get('compounded_directional_roi_5d'))}"
                f" | 5d 방향수익률 {_fmt_pct(item.get('avg_directional_return_5d'))}"
                f" | 5d 적중률 {_fmt_pct(item.get('hit_rate_5d'))}"
                f" | 5d 표본 {int(item.get('signals_with_price_5d', 0) or 0)}"
                f" | 전체 표본 {int(item.get('total_signals', 0) or 0)}"
            )
    else:
        lines.append("- 추천할 필터 없음")

    lines.extend(["", "[최근 시그널 샘플]"])
    recent = summary.get("recent_signals", []) if isinstance(summary, dict) else []
    if recent:
        for item in recent[:15]:
            lines.append(
                f"- {item.get('signal_date', '-')}"
                f" | {item.get('channel_slug', '-')}"
                f" | {item.get('ticker', '-')}"
                f" | score {_fmt_scalar(item.get('signal_score'))}"
                f" | verdict {item.get('verdict', '-')}"
                f" | 1d {_fmt_pct(item.get('returns', {}).get('1d'))}"
                f" | 3d {_fmt_pct(item.get('returns', {}).get('3d'))}"
                f" | 5d {_fmt_pct(item.get('returns', {}).get('5d'))}"
            )
    else:
        lines.append("- 시그널 없음")

    return "\n".join(lines).strip() + "\n"


def _fmt_pct(value: object) -> str:
    if value is None or value == "":
        return "미제공"
    return f"{float(value):.2f}%"


def _fmt_scalar(value: object) -> str:
    if value is None or value == "":
        return "미제공"
    if isinstance(value, float):
        return f"{value:.2f}".rstrip("0").rstrip(".")
    return str(value)


def _record_within_window(record: SignalRecord, *, start_date: date, end_date: date) -> bool:
    try:
        signal_dt = date.fromisoformat(record.signal_date[:10])
    except ValueError:
        return False
    return start_date <= signal_dt <= end_date


def _channel_quality_value(metadata: dict[str, Any]) -> float | None:
    raw = metadata.get("overall_quality_score")
    if raw is None:
        raw = (metadata.get("quality_scorecard", {}) or {}).get("overall")
    try:
        return float(raw) if raw is not None else None
    except (TypeError, ValueError):
        return None


def _build_roi_fields(records: list[SignalRecord]) -> dict[str, float | None]:
    return {
        "compounded_directional_roi_1d": _compounded_directional_roi(records, "1d"),
        "compounded_directional_roi_3d": _compounded_directional_roi(records, "3d"),
        "compounded_directional_roi_5d": _compounded_directional_roi(records, "5d"),
    }


def _compounded_directional_roi(records: list[SignalRecord], window_key: str) -> float | None:
    values = [float(value) for value in (_directional_return(record, window_key) for record in records) if value is not None]
    if not values:
        return None
    capital = 1.0
    for value in values:
        capital *= 1.0 + value / 100.0
    return round((capital - 1.0) * 100.0, 2)


def _signal_record_to_backtest_row(
    record: SignalRecord,
    *,
    channel_metadata: dict[str, dict[str, Any]],
    as_of: date,
) -> dict[str, Any]:
    metadata = dict(channel_metadata.get(record.channel_slug, {}) or {})
    try:
        signal_dt = date.fromisoformat(record.signal_date[:10])
        signal_age_days = (as_of - signal_dt).days
    except ValueError:
        signal_age_days = None
    return {
        "ticker": record.ticker,
        "company_name": record.company_name,
        "channel_slug": record.channel_slug,
        "channel_display_name": metadata.get("display_name", record.channel_slug),
        "channel_quality_score": _channel_quality_value(metadata),
        "signal_date": record.signal_date,
        "signal_age_days": signal_age_days,
        "signal_score": record.signal_score,
        "verdict": record.verdict,
        "source_video_id": record.source_video_id,
        "source_title": record.source_title,
        "has_price_target": _target_price_from_record(record) is not None,
        "returns": {
            "1d": record.returns.get("1d"),
            "3d": record.returns.get("3d"),
            "5d": record.returns.get("5d"),
        },
        "directional_returns": {
            "1d": _directional_return(record, "1d"),
            "3d": _directional_return(record, "3d"),
            "5d": _directional_return(record, "5d"),
        },
        "entry_date": record.entry_date,
        "entry_price": record.entry_price,
        "latest_price": record.latest_price,
        "latest_price_date": record.latest_price_date,
    }


def _optimize_signal_filters(
    records: list[SignalRecord],
    *,
    channel_metadata: dict[str, dict[str, Any]],
    top_filters: int,
    min_filter_sample: int,
) -> list[dict[str, Any]]:
    recommendations: list[dict[str, Any]] = []
    score_thresholds = [None, 65.0, 70.0, 75.0, 80.0]
    channel_quality_thresholds = [None, 55.0, 60.0, 70.0]
    for min_signal_score in score_thresholds:
        for min_channel_quality in channel_quality_thresholds:
            for require_conviction in (False, True):
                for require_price_target in (False, True):
                    if (
                        min_signal_score is None
                        and min_channel_quality is None
                        and not require_conviction
                        and not require_price_target
                    ):
                        continue
                    filtered = [
                        record
                        for record in records
                        if _record_matches_filter(
                            record,
                            channel_metadata=channel_metadata,
                            min_signal_score=min_signal_score,
                            min_channel_quality=min_channel_quality,
                            require_conviction=require_conviction,
                            require_price_target=require_price_target,
                        )
                    ]
                    stats = _build_accuracy_stats(filtered).to_dict()
                    mature_sample = int(stats.get("signals_with_price_5d", 0) or 0)
                    if mature_sample < min_filter_sample:
                        continue
                    recommendations.append(
                        {
                            "label": _filter_label(
                                min_signal_score=min_signal_score,
                                min_channel_quality=min_channel_quality,
                                require_conviction=require_conviction,
                                require_price_target=require_price_target,
                            ),
                            "conditions": {
                                "min_signal_score": min_signal_score,
                                "min_channel_quality": min_channel_quality,
                                "require_conviction": require_conviction,
                                "require_price_target": require_price_target,
                            },
                            **stats,
                            **_build_roi_fields(filtered),
                        }
                    )
    recommendations.sort(
        key=lambda item: (
            -(float(item.get("compounded_directional_roi_5d")) if item.get("compounded_directional_roi_5d") is not None else -999.0),
            -(float(item.get("avg_directional_return_5d")) if item.get("avg_directional_return_5d") is not None else -999.0),
            -(float(item.get("hit_rate_5d")) if item.get("hit_rate_5d") is not None else -999.0),
            -int(item.get("signals_with_price_5d", 0) or 0),
            str(item.get("label", "")),
        )
    )
    return recommendations[:top_filters]


def _record_matches_filter(
    record: SignalRecord,
    *,
    channel_metadata: dict[str, dict[str, Any]],
    min_signal_score: float | None,
    min_channel_quality: float | None,
    require_conviction: bool,
    require_price_target: bool,
) -> bool:
    if min_signal_score is not None and float(record.signal_score) < float(min_signal_score):
        return False
    if min_channel_quality is not None:
        metadata = dict(channel_metadata.get(record.channel_slug, {}) or {})
        channel_quality = _channel_quality_value(metadata)
        if channel_quality is None or channel_quality < float(min_channel_quality):
            return False
    if require_conviction:
        normalized = str(record.verdict or "").strip().upper()
        if normalized in {"", "WATCH", "HOLD", "NEUTRAL", "REJECT"}:
            return False
    if require_price_target and _target_price_from_record(record) is None:
        return False
    return True


def _filter_label(
    *,
    min_signal_score: float | None,
    min_channel_quality: float | None,
    require_conviction: bool,
    require_price_target: bool,
) -> str:
    parts: list[str] = []
    if min_signal_score is not None:
        parts.append(f"signal_score>={min_signal_score:.0f}")
    if min_channel_quality is not None:
        parts.append(f"channel_quality>={min_channel_quality:.0f}")
    if require_conviction:
        parts.append("conviction_only")
    if require_price_target:
        parts.append("has_price_target")
    return " AND ".join(parts) if parts else "baseline"


def _normalize_signal_date(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%Y%m%d", "%Y%m%dT%H%M%SZ"):
        try:
            return datetime.strptime(text[: len(fmt.replace("%", "").replace("-", ""))] if fmt == "%Y%m%d" else text, fmt).date().isoformat()
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date().isoformat()
    except ValueError:
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
        remaining_distance_pct = round((target_price - latest_price) / latest_price * 100, 2)
    else:
        progress = (record.entry_price - latest_price) / (record.entry_price - target_price) * 100
        hit = latest_price <= target_price
        remaining_distance_pct = round((latest_price - target_price) / latest_price * 100, 2)

    ever_hit = bool(record.target_hit or record.target_hit_date) or hit
    record.target_progress_pct = 100.0 if ever_hit else round(max(0.0, min(progress, 100.0)), 2)
    record.target_distance_pct = 0.0 if ever_hit else remaining_distance_pct
    record.target_hit = ever_hit
    if hit and not record.target_hit_date:
        record.target_hit_date = latest_price_date
