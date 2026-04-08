from __future__ import annotations

from pathlib import Path

from .utils import ensure_dir


def build_run_output_dir(base_output_dir: str | Path, run_id: str) -> Path:
    base = Path(base_output_dir)
    day = str(run_id)[:8] or "manual"
    return ensure_dir(base / "runs" / day / str(run_id))
