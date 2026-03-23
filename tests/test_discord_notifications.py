"""Tests for Discord notification support and unified notify_all dispatch."""
from dataclasses import dataclass
from unittest.mock import MagicMock

from omx_brainstorm.notifications import send_discord_message, notify_all


@dataclass
class _FakeConfig:
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""
    discord_webhook_url: str = ""


def test_discord_skipped_when_no_webhook():
    config = _FakeConfig()
    assert send_discord_message(config, "hello") is False


def test_discord_success(monkeypatch):
    config = _FakeConfig(discord_webhook_url="https://discord.com/api/webhooks/test")
    mock_resp = MagicMock()
    mock_resp.status_code = 204
    mock_post = MagicMock(return_value=mock_resp)
    monkeypatch.setattr("omx_brainstorm.notifications.requests.post", mock_post)

    assert send_discord_message(config, "test msg") is True
    mock_post.assert_called_once()
    call_args = mock_post.call_args
    assert call_args[1]["json"]["content"] == "test msg"


def test_discord_success_200(monkeypatch):
    config = _FakeConfig(discord_webhook_url="https://discord.com/api/webhooks/test")
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    monkeypatch.setattr("omx_brainstorm.notifications.requests.post", MagicMock(return_value=mock_resp))
    assert send_discord_message(config, "ok") is True


def test_discord_failure(monkeypatch):
    config = _FakeConfig(discord_webhook_url="https://discord.com/api/webhooks/test")
    monkeypatch.setattr("omx_brainstorm.notifications.requests.post", MagicMock(side_effect=ConnectionError("fail")))
    assert send_discord_message(config, "test") is False


def test_discord_server_error(monkeypatch):
    config = _FakeConfig(discord_webhook_url="https://discord.com/api/webhooks/test")
    mock_resp = MagicMock()
    mock_resp.status_code = 500
    monkeypatch.setattr("omx_brainstorm.notifications.requests.post", MagicMock(return_value=mock_resp))
    assert send_discord_message(config, "test") is False


def test_discord_truncates_long_message(monkeypatch):
    config = _FakeConfig(discord_webhook_url="https://discord.com/api/webhooks/test")
    mock_resp = MagicMock()
    mock_resp.status_code = 204
    mock_post = MagicMock(return_value=mock_resp)
    monkeypatch.setattr("omx_brainstorm.notifications.requests.post", mock_post)

    long_msg = "x" * 3000
    send_discord_message(config, long_msg)
    sent_content = mock_post.call_args[1]["json"]["content"]
    assert len(sent_content) == 2000


def test_notify_all_dispatches_both(monkeypatch):
    config = _FakeConfig(
        telegram_bot_token="tok",
        telegram_chat_id="chat",
        discord_webhook_url="https://discord.com/api/webhooks/test",
    )
    mock_resp_tg = MagicMock()
    mock_resp_tg.json.return_value = {"ok": True}
    mock_resp_dc = MagicMock()
    mock_resp_dc.status_code = 204

    call_count = {"n": 0}
    def mock_post(url, **kwargs):
        call_count["n"] += 1
        if "telegram" in url:
            return mock_resp_tg
        return mock_resp_dc

    monkeypatch.setattr("omx_brainstorm.notifications.requests.post", mock_post)
    result = notify_all(config, "test")
    assert result == {"telegram": True, "discord": True}
    assert call_count["n"] == 2


def test_notify_all_partial_config(monkeypatch):
    config = _FakeConfig(discord_webhook_url="https://discord.com/api/webhooks/test")
    mock_resp = MagicMock()
    mock_resp.status_code = 204
    monkeypatch.setattr("omx_brainstorm.notifications.requests.post", MagicMock(return_value=mock_resp))

    result = notify_all(config, "test")
    assert result["telegram"] is False
    assert result["discord"] is True


def test_notify_all_no_config():
    config = _FakeConfig()
    result = notify_all(config, "test")
    assert result == {"telegram": False, "discord": False}


def test_config_loads_discord_webhook(monkeypatch, tmp_path):
    """Verify AppConfig loads discord_webhook_url from env."""
    from omx_brainstorm.app_config import load_app_config
    monkeypatch.setenv("DISCORD_WEBHOOK_URL", "https://discord.com/api/webhooks/123")
    config = load_app_config(tmp_path / "nonexistent.toml")
    assert config.notifications.discord_webhook_url == "https://discord.com/api/webhooks/123"
