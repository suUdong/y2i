import json
import logging
from logging.handlers import TimedRotatingFileHandler

from omx_brainstorm.logging_utils import JsonFormatter, configure_logging


def test_json_formatter_emits_json():
    formatter = JsonFormatter()
    record = logging.LogRecord("x", logging.INFO, __file__, 1, "hello", (), None)
    payload = json.loads(formatter.format(record))
    assert payload["message"] == "hello"
    assert payload["level"] == "INFO"


def test_configure_logging_adds_handlers(tmp_path):
    configure_logging(log_dir=tmp_path)
    root = logging.getLogger()
    assert len(root.handlers) >= 2


def test_configure_logging_creates_log_dir(tmp_path):
    log_dir = tmp_path / "logs"
    configure_logging(log_dir=log_dir)
    assert log_dir.exists()


def test_configure_logging_sets_debug_when_verbose(tmp_path):
    configure_logging(verbose=True, log_dir=tmp_path)
    assert logging.getLogger().level == logging.DEBUG


def test_configure_logging_sets_info_when_not_verbose(tmp_path):
    configure_logging(verbose=False, log_dir=tmp_path)
    assert logging.getLogger().level == logging.INFO


def test_configure_logging_uses_plain_formatter_when_json_disabled(tmp_path):
    configure_logging(verbose=False, json_logs=False, log_dir=tmp_path)
    message = logging.getLogger().handlers[0].formatter.format(logging.LogRecord("x", logging.INFO, __file__, 1, "hello", (), None))
    assert "hello" in message
    assert not message.strip().startswith("{")


def test_configure_logging_uses_timed_rotation_handler(tmp_path):
    configure_logging(log_dir=tmp_path, retention_days=7)
    assert any(isinstance(handler, TimedRotatingFileHandler) and handler.backupCount == 7 for handler in logging.getLogger().handlers)
