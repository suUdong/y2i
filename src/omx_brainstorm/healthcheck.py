from __future__ import annotations

from pathlib import Path

from .utils import read_json


def read_health_state(path: str | Path = ".omx/state/scheduler_health.json") -> dict:
    return read_json(Path(path), {"status": "unknown", "error_count": 0})
