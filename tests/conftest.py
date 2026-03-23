import logging

import pytest


@pytest.fixture(autouse=True)
def reset_root_logger_handlers():
    root = logging.getLogger()
    handlers = list(root.handlers)
    level = root.level
    yield
    root.handlers.clear()
    for handler in handlers:
        root.addHandler(handler)
    root.setLevel(level)
