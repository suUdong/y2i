from __future__ import annotations

import json
import logging
import os
import sys
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


class PrettyConsoleFormatter(logging.Formatter):
    """Human-friendly colored formatter for interactive terminals."""

    _COLORS = {
        "DEBUG": "\033[37m",      # light gray
        "INFO": "\033[36m",       # cyan
        "WARNING": "\033[33m",    # yellow
        "ERROR": "\033[31m",      # red
        "CRITICAL": "\033[1;31m", # bold red
    }
    _RESET = "\033[0m"
    _DIM = "\033[2m"

    def __init__(self, *, use_color: bool) -> None:
        super().__init__()
        self.use_color = use_color

    def format(self, record: logging.LogRecord) -> str:
        ts = datetime.fromtimestamp(record.created).strftime("%H:%M:%S")
        level = record.levelname.ljust(7)
        name = record.name
        msg = record.getMessage()
        if self.use_color:
            color = self._COLORS.get(record.levelname, "")
            line = f"{self._DIM}{ts}{self._RESET} {color}{level}{self._RESET} {self._DIM}{name}{self._RESET} {msg}"
        else:
            line = f"{ts} {level} {name} {msg}"
        if record.exc_info:
            line += "\n" + self.formatException(record.exc_info)
        return line


def configure_logging(
    verbose: bool = False,
    *,
    json_logs: bool | None = None,
    log_dir: str | Path = ".omx/logs",
    retention_days: int = 7,
) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()

    is_tty = sys.stderr.isatty() if hasattr(sys.stderr, "isatty") else False
    # Default: pretty console in TTY, JSON when piped/redirected. Explicit flag wins.
    if json_logs is None:
        console_json = not is_tty
    else:
        console_json = json_logs

    console_formatter: logging.Formatter
    if console_json:
        console_formatter = JsonFormatter()
    else:
        use_color = is_tty and os.getenv("NO_COLOR") is None
        console_formatter = PrettyConsoleFormatter(use_color=use_color)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(console_formatter)
    root.addHandler(stream_handler)

    log_path = ensure_dir(Path(log_dir)) / "omx-app.log"
    file_handler = TimedRotatingFileHandler(log_path, when="midnight", backupCount=retention_days, encoding="utf-8")
    # File handler always stays JSON for machine parsing.
    file_handler.setFormatter(JsonFormatter())
    root.addHandler(file_handler)
