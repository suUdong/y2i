from __future__ import annotations

import argparse
import json

from omx_brainstorm.backtest_automation import run_backtest_for_artifact


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run an automated backtest report from a saved ranking artifact.")
    parser.add_argument("artifact_path")
    parser.add_argument("--end-date")
    parser.add_argument("--top-n", type=int)
    parser.add_argument("--initial-capital", type=float, default=10_000.0)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    payload = run_backtest_for_artifact(
        args.artifact_path,
        end_date=args.end_date,
        top_n=args.top_n,
        initial_capital=args.initial_capital,
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
