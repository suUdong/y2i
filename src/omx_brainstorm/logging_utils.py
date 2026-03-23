from __future__ import annotations

import json
import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from datetime import datetime, timezone

from .utils import ensure_dir


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def configure_logging(
    verbose: bool = False,
    *,
    json_logs: bool = True,
    log_dir: str | Path = ".omx/logs",
    retention_days: int = 7,
) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()

    formatter = JsonFormatter() if json_logs else logging.Formatter("%(levelname)s %(name)s: %(message)s")

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    root.addHandler(stream_handler)

    log_path = ensure_dir(Path(log_dir)) / "omx-app.log"
    file_handler = TimedRotatingFileHandler(log_path, when="midnight", backupCount=retention_days, encoding="utf-8")
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)
