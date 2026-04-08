from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path


def cleanup_generated_artifacts(
    *,
    output_dir: str | Path,
    report_dir: str | Path,
    log_dir: str | Path,
    output_days: int,
    report_days: int,
    log_days: int,
    now: datetime | None = None,
) -> dict[str, int]:
    reference = now or datetime.now(timezone.utc)
    output_removed = _remove_older_than(Path(output_dir), reference - timedelta(days=max(1, output_days)))
    report_removed = _remove_older_than(Path(report_dir), reference - timedelta(days=max(1, report_days)))
    log_removed = _remove_older_than(Path(log_dir), reference - timedelta(days=max(1, log_days)), keep_predicate=_keep_log_file)
    return {
        "output_removed": output_removed,
        "report_removed": report_removed,
        "log_removed": log_removed,
    }


def _remove_older_than(root: Path, cutoff: datetime, keep_predicate=None) -> int:
    if not root.exists():
        return 0
    removed = 0
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        if keep_predicate is not None and keep_predicate(path):
            continue
        try:
            modified = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
        except FileNotFoundError:
            continue
        if modified >= cutoff:
            continue
        path.unlink(missing_ok=True)
        removed += 1
    _prune_empty_dirs(root)
    return removed


def _prune_empty_dirs(root: Path) -> None:
    if not root.exists():
        return
    for path in sorted((item for item in root.rglob("*") if item.is_dir()), reverse=True):
        try:
            next(path.iterdir())
        except StopIteration:
            path.rmdir()
        except FileNotFoundError:
            continue


def _keep_log_file(path: Path) -> bool:
    return path.suffix == ".pid"
