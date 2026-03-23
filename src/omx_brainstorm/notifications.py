from __future__ import annotations

import json
import logging

import requests

from .app_config import NotificationConfig

logger = logging.getLogger(__name__)


def send_telegram_message(config: NotificationConfig, text: str) -> bool:
    if not config.telegram_bot_token or not config.telegram_chat_id:
        logger.info("Telegram notification skipped: credentials missing")
        return False
    url = f"https://api.telegram.org/bot{config.telegram_bot_token}/sendMessage"
    try:
        response = requests.post(url, data={"chat_id": config.telegram_chat_id, "text": text}, timeout=10)
        body = response.json()
        return bool(body.get("ok"))
    except Exception as exc:
        logger.warning("Telegram notification failed: %s", exc)
        return False
