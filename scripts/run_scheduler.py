from __future__ import annotations

import argparse
import logging

from omx_brainstorm.app_config import load_app_config
from omx_brainstorm.logging_utils import configure_logging
from omx_brainstorm.scheduler import run_scheduled_job, run_scheduler_forever


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the daily OMX scheduler loop or a single scheduled job.")
    parser.add_argument("--config", default="config.toml")
    parser.add_argument("--once", action="store_true", help="Run one scheduled job immediately and exit")
    parser.add_argument("--verbose", action="store_true")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    config = load_app_config(args.config)
    configure_logging(
        verbose=args.verbose,
        json_logs=config.logging.json,
        log_dir=config.logging.log_dir,
        retention_days=config.logging.retention_days,
    )
    if args.once:
        raise SystemExit(run_scheduled_job(config))
    if not config.schedule.enabled:
        logging.getLogger(__name__).warning("Schedule is disabled in config; starting loop anyway because --once was not used")
    run_scheduler_forever(config)


if __name__ == "__main__":
    main()
