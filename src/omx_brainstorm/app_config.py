from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(slots=True)
class ChannelConfig:
    """One configured YouTube channel target."""
    slug: str
    display_name: str
    url: str
    enabled: bool = True


@dataclass(slots=True)
class StrategyConfig:
    """High-level strategy parameters for scheduled runs."""
    window_days: int = 30
    max_scan: int = 80
    top_n: int = 3
    video_workers: int = 4
    fundamentals_workers: int = 4
    paper_trade_capital: float = 10_000.0
    signal_alert_min_score: float = 68.0
    signal_alert_min_channel_quality: float = 50.0
    high_confidence_min_score: float = 82.0


@dataclass(slots=True)
class NotificationConfig:
    """Notification destinations and credentials."""
    telegram_bot_token: str | None = None
    telegram_chat_id: str | None = None
    discord_webhook_url: str | None = None


@dataclass(slots=True)
class ScheduleConfig:
    """Daily scheduler settings."""
    daily_time: str = "09:00"
    timezone: str = "Asia/Seoul"
    enabled: bool = False
    poll_interval_minutes: int = 2
    poll_video_limit: int = 8
    job_max_attempts: int = 3
    retry_backoff_seconds: int = 60
    state_path: str = ".omx/state/scheduler_state.json"


@dataclass(slots=True)
class LoggingConfig:
    """Logging output and retention settings."""
    json: bool = True
    log_dir: str = ".omx/logs"
    retention_days: int = 7


@dataclass(slots=True)
class AppConfig:
    """Top-level application configuration."""
    config_path: str = "config.toml"
    provider: str = "auto"
    output_dir: str = "output"
    registry_path: str = "channels.json"
    paper_trading_mode: bool = True
    channels: list[ChannelConfig] = field(default_factory=list)
    strategy: StrategyConfig = field(default_factory=StrategyConfig)
    notifications: NotificationConfig = field(default_factory=NotificationConfig)
    schedule: ScheduleConfig = field(default_factory=ScheduleConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)


DEFAULT_CHANNELS = [
    ChannelConfig(slug="itgod", display_name="IT의 신 이형수", url="https://www.youtube.com/channel/UCQW05vzztAlwV54WL3pjGBQ/videos"),
    ChannelConfig(slug="kimjakgatv", display_name="김작가TV", url="https://www.youtube.com/@lucky_tv/videos"),
    ChannelConfig(slug="sampro", display_name="삼프로TV", url="https://www.youtube.com/@3protv/videos"),
    ChannelConfig(slug="syuka", display_name="슈카월드", url="https://www.youtube.com/@syukaworld/videos"),
    ChannelConfig(slug="hsacademy", display_name="이효석아카데미", url="https://www.youtube.com/@hs_academy/videos"),
    ChannelConfig(slug="sosumonkey", display_name="소수몽키", url="https://www.youtube.com/@sosumonkey/videos"),
]


def load_app_config(path: str | Path | None = None) -> AppConfig:
    path = Path(path or os.getenv("OMX_CONFIG_PATH", "config.toml"))
    payload = {}
    if path.exists():
        try:
            payload = tomllib.loads(path.read_text(encoding="utf-8"))
        except tomllib.TOMLDecodeError as exc:
            raise ValueError(f"Invalid config TOML: {path}") from exc

    app = payload.get("app", {})
    channels_payload = payload.get("channels", [])
    strategy_payload = payload.get("strategy", {})
    notifications_payload = payload.get("notifications", {})
    schedule_payload = payload.get("schedule", {})
    logging_payload = payload.get("logging", {})

    try:
        channels = [
            ChannelConfig(
                slug=item["slug"],
                display_name=item.get("display_name", item["slug"]),
                url=item["url"],
                enabled=bool(item.get("enabled", True)),
            )
            for item in channels_payload
        ] or list(DEFAULT_CHANNELS)
    except KeyError as exc:
        raise ValueError(f"Invalid channel config entry missing field: {exc}") from exc

    return AppConfig(
        config_path=str(path),
        provider=os.getenv("OMX_PROVIDER", app.get("provider", "auto")),
        output_dir=os.getenv("OMX_OUTPUT_DIR", app.get("output_dir", "output")),
        registry_path=os.getenv("OMX_REGISTRY_PATH", app.get("registry_path", "channels.json")),
        paper_trading_mode=_env_bool("OMX_PAPER_TRADING_MODE", app.get("paper_trading_mode", True)),
        channels=channels,
        strategy=StrategyConfig(
            window_days=int(os.getenv("OMX_WINDOW_DAYS", strategy_payload.get("window_days", 30))),
            max_scan=int(os.getenv("OMX_MAX_SCAN", strategy_payload.get("max_scan", 80))),
            top_n=int(os.getenv("OMX_TOP_N", strategy_payload.get("top_n", 3))),
            video_workers=int(os.getenv("OMX_VIDEO_WORKERS", strategy_payload.get("video_workers", 4))),
            fundamentals_workers=int(os.getenv("OMX_FUNDAMENTALS_WORKERS", strategy_payload.get("fundamentals_workers", 4))),
            paper_trade_capital=float(os.getenv("OMX_PAPER_TRADE_CAPITAL", strategy_payload.get("paper_trade_capital", 10_000.0))),
            signal_alert_min_score=float(os.getenv("OMX_SIGNAL_ALERT_MIN_SCORE", strategy_payload.get("signal_alert_min_score", 68.0))),
            signal_alert_min_channel_quality=float(os.getenv("OMX_SIGNAL_ALERT_MIN_CHANNEL_QUALITY", strategy_payload.get("signal_alert_min_channel_quality", 50.0))),
            high_confidence_min_score=float(os.getenv("OMX_HIGH_CONFIDENCE_MIN_SCORE", strategy_payload.get("high_confidence_min_score", 82.0))),
        ),
        notifications=NotificationConfig(
            telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", notifications_payload.get("telegram_bot_token")),
            telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID", notifications_payload.get("telegram_chat_id")),
            discord_webhook_url=os.getenv("DISCORD_WEBHOOK_URL", notifications_payload.get("discord_webhook_url")),
        ),
        schedule=ScheduleConfig(
            daily_time=os.getenv("OMX_DAILY_TIME", schedule_payload.get("daily_time", "09:00")),
            timezone=os.getenv("OMX_TIMEZONE", schedule_payload.get("timezone", "Asia/Seoul")),
            enabled=_env_bool("OMX_SCHEDULE_ENABLED", schedule_payload.get("enabled", False)),
            poll_interval_minutes=int(os.getenv("OMX_SCHEDULE_POLL_INTERVAL_MINUTES", schedule_payload.get("poll_interval_minutes", 10))),
            poll_video_limit=int(os.getenv("OMX_SCHEDULE_POLL_VIDEO_LIMIT", schedule_payload.get("poll_video_limit", 8))),
            job_max_attempts=int(os.getenv("OMX_SCHEDULE_JOB_MAX_ATTEMPTS", schedule_payload.get("job_max_attempts", 3))),
            retry_backoff_seconds=int(os.getenv("OMX_SCHEDULE_RETRY_BACKOFF_SECONDS", schedule_payload.get("retry_backoff_seconds", 60))),
            state_path=os.getenv("OMX_SCHEDULE_STATE_PATH", schedule_payload.get("state_path", ".omx/state/scheduler_state.json")),
        ),
        logging=LoggingConfig(
            json=_env_bool("OMX_JSON_LOGS", logging_payload.get("json", True)),
            log_dir=os.getenv("OMX_LOG_DIR", logging_payload.get("log_dir", ".omx/logs")),
            retention_days=int(os.getenv("OMX_LOG_RETENTION_DAYS", logging_payload.get("retention_days", 7))),
        ),
    )


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return bool(default)
    return value.lower() in {"1", "true", "yes", "on"}
