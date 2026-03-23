from __future__ import annotations

import argparse
import json

from omx_brainstorm.healthcheck import read_health_state


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Read scheduler health state.")
    parser.add_argument("--path", default=".omx/state/scheduler_health.json")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    print(json.dumps(read_health_state(args.path), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
