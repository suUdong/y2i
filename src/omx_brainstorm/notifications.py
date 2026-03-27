from __future__ import annotations

import logging
from pathlib import Path

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


def send_telegram_document(config: NotificationConfig, file_path: str | Path, caption: str = "") -> bool:
    """Send a document to Telegram with an optional HTML caption."""
    if not config.telegram_bot_token or not config.telegram_chat_id:
        logger.info("Telegram document skipped: credentials missing")
        return False

    path = Path(file_path)
    if not path.exists():
        logger.warning("Telegram document skipped: file missing (%s)", path)
        return False

    url = f"https://api.telegram.org/bot{config.telegram_bot_token}/sendDocument"
    try:
        with path.open("rb") as handle:
            response = requests.post(
                url,
                data={
                    "chat_id": config.telegram_chat_id,
                    "caption": caption[:1024],
                    "parse_mode": "HTML",
                },
                files={"document": (path.name, handle, "text/markdown")},
                timeout=20,
            )
        body = response.json()
        return bool(body.get("ok"))
    except Exception as exc:
        logger.warning("Telegram document failed: %s", exc)
        return False


def send_discord_message(config: NotificationConfig, text: str) -> bool:
    if not config.discord_webhook_url:
        logger.info("Discord notification skipped: webhook URL missing")
        return False
    try:
        response = requests.post(
            config.discord_webhook_url,
            json={"content": text[:2000]},
            timeout=10,
        )
        return response.status_code in (200, 204)
    except Exception as exc:
        logger.warning("Discord notification failed: %s", exc)
        return False


def notify_all(config: NotificationConfig, text: str) -> dict[str, bool]:
    """Dispatch notification to all configured channels."""
    return {
        "telegram": send_telegram_message(config, text),
        "discord": send_discord_message(config, text),
    }
