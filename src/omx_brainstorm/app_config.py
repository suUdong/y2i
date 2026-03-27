from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

from .utils import load_env_file


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
    ChannelConfig(slug="jeoningoo", display_name="전인구경제연구소", url="https://www.youtube.com/channel/UCznImSIaxZR7fdLCICLdgaQ/videos"),
    ChannelConfig(slug="moneyinside", display_name="머니인사이드", url="https://www.youtube.com/channel/UCxfko2YOD6DODYRGzeOPhIQ/videos"),
    ChannelConfig(slug="mickeypedia", display_name="미키피디아", url="https://www.youtube.com/channel/UCt9m3iBPn0e0z0B_t-Va7sw/videos"),
    ChannelConfig(slug="talentinvest", display_name="달란트투자", url="https://www.youtube.com/channel/UCBM86JVoHLqg9irpR2XKvGw/videos"),
    ChannelConfig(slug="kimdante", display_name="내일은 투자왕 - 김단테", url="https://www.youtube.com/channel/UCKTMvIu9a4VGSrpWy-8bUrQ/videos"),
]


def load_app_config(path: str | Path | None = None) -> AppConfig:
    path = Path(path or os.getenv("OMX_CONFIG_PATH", "config.toml"))
    dotenv_payload = load_env_file(os.getenv("OMX_ENV_PATH", str(path.with_name(".env"))))
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
        provider=_env_or_dotenv("OMX_PROVIDER", dotenv_payload, app.get("provider", "auto")),
        output_dir=_env_or_dotenv("OMX_OUTPUT_DIR", dotenv_payload, app.get("output_dir", "output")),
        registry_path=_env_or_dotenv("OMX_REGISTRY_PATH", dotenv_payload, app.get("registry_path", "channels.json")),
        paper_trading_mode=_env_or_dotenv_bool("OMX_PAPER_TRADING_MODE", dotenv_payload, app.get("paper_trading_mode", True)),
        channels=channels,
        strategy=StrategyConfig(
            window_days=int(_env_or_dotenv("OMX_WINDOW_DAYS", dotenv_payload, strategy_payload.get("window_days", 30))),
            max_scan=int(_env_or_dotenv("OMX_MAX_SCAN", dotenv_payload, strategy_payload.get("max_scan", 80))),
            top_n=int(_env_or_dotenv("OMX_TOP_N", dotenv_payload, strategy_payload.get("top_n", 3))),
            video_workers=int(_env_or_dotenv("OMX_VIDEO_WORKERS", dotenv_payload, strategy_payload.get("video_workers", 4))),
            fundamentals_workers=int(_env_or_dotenv("OMX_FUNDAMENTALS_WORKERS", dotenv_payload, strategy_payload.get("fundamentals_workers", 4))),
            paper_trade_capital=float(_env_or_dotenv("OMX_PAPER_TRADE_CAPITAL", dotenv_payload, strategy_payload.get("paper_trade_capital", 10_000.0))),
            signal_alert_min_score=float(_env_or_dotenv("OMX_SIGNAL_ALERT_MIN_SCORE", dotenv_payload, strategy_payload.get("signal_alert_min_score", 68.0))),
            signal_alert_min_channel_quality=float(_env_or_dotenv("OMX_SIGNAL_ALERT_MIN_CHANNEL_QUALITY", dotenv_payload, strategy_payload.get("signal_alert_min_channel_quality", 50.0))),
            high_confidence_min_score=float(_env_or_dotenv("OMX_HIGH_CONFIDENCE_MIN_SCORE", dotenv_payload, strategy_payload.get("high_confidence_min_score", 82.0))),
        ),
        notifications=NotificationConfig(
            telegram_bot_token=_env_or_dotenv("TELEGRAM_BOT_TOKEN", dotenv_payload, notifications_payload.get("telegram_bot_token")),
            telegram_chat_id=_env_or_dotenv("TELEGRAM_CHAT_ID", dotenv_payload, notifications_payload.get("telegram_chat_id")),
            discord_webhook_url=_env_or_dotenv("DISCORD_WEBHOOK_URL", dotenv_payload, notifications_payload.get("discord_webhook_url")),
        ),
        schedule=ScheduleConfig(
            daily_time=_env_or_dotenv("OMX_DAILY_TIME", dotenv_payload, schedule_payload.get("daily_time", "09:00")),
            timezone=_env_or_dotenv("OMX_TIMEZONE", dotenv_payload, schedule_payload.get("timezone", "Asia/Seoul")),
            enabled=_env_or_dotenv_bool("OMX_SCHEDULE_ENABLED", dotenv_payload, schedule_payload.get("enabled", False)),
            poll_interval_minutes=int(_env_or_dotenv("OMX_SCHEDULE_POLL_INTERVAL_MINUTES", dotenv_payload, schedule_payload.get("poll_interval_minutes", 10))),
            poll_video_limit=int(_env_or_dotenv("OMX_SCHEDULE_POLL_VIDEO_LIMIT", dotenv_payload, schedule_payload.get("poll_video_limit", 8))),
            job_max_attempts=int(_env_or_dotenv("OMX_SCHEDULE_JOB_MAX_ATTEMPTS", dotenv_payload, schedule_payload.get("job_max_attempts", 3))),
            retry_backoff_seconds=int(_env_or_dotenv("OMX_SCHEDULE_RETRY_BACKOFF_SECONDS", dotenv_payload, schedule_payload.get("retry_backoff_seconds", 60))),
            state_path=_env_or_dotenv("OMX_SCHEDULE_STATE_PATH", dotenv_payload, schedule_payload.get("state_path", ".omx/state/scheduler_state.json")),
        ),
        logging=LoggingConfig(
            json=_env_or_dotenv_bool("OMX_JSON_LOGS", dotenv_payload, logging_payload.get("json", True)),
            log_dir=_env_or_dotenv("OMX_LOG_DIR", dotenv_payload, logging_payload.get("log_dir", ".omx/logs")),
            retention_days=int(_env_or_dotenv("OMX_LOG_RETENTION_DAYS", dotenv_payload, logging_payload.get("retention_days", 7))),
        ),
    )


def _env_or_dotenv(name: str, dotenv_payload: dict[str, str], default: str | int | float | None) -> str | int | float | None:
    env_value = os.getenv(name)
    if env_value is not None:
        return env_value
    dotenv_value = dotenv_payload.get(name)
    if dotenv_value is not None and dotenv_value.strip():
        return dotenv_value
    return default


def _env_or_dotenv_bool(name: str, dotenv_payload: dict[str, str], default: bool) -> bool:
    env_value = os.getenv(name)
    if env_value is not None:
        return env_value.lower() in {"1", "true", "yes", "on"}
    dotenv_value = dotenv_payload.get(name)
    if dotenv_value is not None and dotenv_value.strip():
        return dotenv_value.lower() in {"1", "true", "yes", "on"}
    return bool(default)


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return bool(default)
    return value.lower() in {"1", "true", "yes", "on"}
