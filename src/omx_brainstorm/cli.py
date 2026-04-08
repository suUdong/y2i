from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from .app_config import load_app_config
from .backtest import BacktestEngine, BacktestIdea
from .backtest_automation import run_backtest_for_artifact
from .kindshot_feed import export_signals_for_kindshot
from .logging_utils import configure_logging
from .scheduler import run_scheduled_job, run_scheduler_forever
from .signal_backtest import run_signal_backtest_workflow
from .signal_tracker import SignalTrackerDB, build_signal_accuracy_summary, save_signal_accuracy_report
from .pipeline import OMXPipeline
from .youtube import ChannelRegistry, YoutubeResolver

logger = logging.getLogger(__name__)


def _network_proxy_kwargs(config) -> dict[str, str]:
    network = getattr(config, "network", None)
    http_proxy_url = getattr(network, "http_proxy_url", None)
    https_proxy_url = getattr(network, "https_proxy_url", None)
    kwargs: dict[str, str] = {}
    if http_proxy_url:
        kwargs["http_proxy_url"] = http_proxy_url
    if https_proxy_url:
        kwargs["https_proxy_url"] = https_proxy_url
    return kwargs


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="omx-brainstorm",
        description="Analyze YouTube finance videos into stock signals, reports, and backtests.",
    )
    parser.add_argument("--provider", default="auto", help="auto|codex|claude|gemini|mock")
    parser.add_argument("--output-dir", default="output")
    parser.add_argument("--mode", default="ralph", help="analysis mode, default=ralph")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose progress logging")
    sub = parser.add_subparsers(dest="command", required=True)

    p_register = sub.add_parser("register-channel", help="Register a YouTube channel URL")
    p_register.add_argument("url")
    p_register.add_argument("--registry", default="channels.json")

    p_list = sub.add_parser("list-channels", help="List registered channels")
    p_list.add_argument("--registry", default="channels.json")

    p_video = sub.add_parser("analyze-video", help="Analyze a single video URL")
    p_video.add_argument("url")

    p_channel = sub.add_parser("analyze-channel", help="Analyze the latest N videos from a channel")
    p_channel.add_argument("url")
    p_channel.add_argument("--limit", type=int, default=3)

    p_backtest = sub.add_parser("backtest-ranked", help="Run a backtest from a saved ranking artifact")
    p_backtest.add_argument("input_path", help="JSON artifact containing cross_video_ranking")
    p_backtest.add_argument("--start-date", required=True)
    p_backtest.add_argument("--end-date", required=True)
    p_backtest.add_argument("--top-n", type=int, default=5)
    p_backtest.add_argument("--initial-capital", type=float, default=10000.0)

    p_artifact = sub.add_parser("backtest-artifact", help="Run automated backtest evaluation on a saved channel artifact")
    p_artifact.add_argument("artifact_path")
    p_artifact.add_argument("--end-date")
    p_artifact.add_argument("--top-n", type=int)
    p_artifact.add_argument("--initial-capital", type=float, default=10000.0)

    p_compare = sub.add_parser("run-comparison", help="Run the configured multi-channel comparison job")
    p_compare.add_argument("--config", default="config.toml")

    p_scheduler = sub.add_parser("run-scheduler", help="Run the configured daily scheduler")
    p_scheduler.add_argument("--config", default="config.toml")
    p_scheduler.add_argument("--once", action="store_true")

    p_health = sub.add_parser("run-healthcheck", help="Read scheduler health state")
    p_health.add_argument("--path", default=".omx/state/scheduler_health.json")

    p_accuracy = sub.add_parser("signal-accuracy-report", help="Generate a tracked signal accuracy report")
    p_accuracy.add_argument("--tracker-db", default=".omx/state/signal_tracker.json")
    p_accuracy.add_argument("--top-tickers", type=int, default=20)

    p_kindshot = sub.add_parser("export-kindshot-feed", help="Export KR BUY/STRONG_BUY tracked signals for kindshot")
    p_kindshot.add_argument("--tracker-db", default=".omx/state/signal_tracker.json")
    p_kindshot.add_argument("--output", default=".omx/state/kindshot_feed.json")

    p_backtest_report = sub.add_parser("signal-backtest-report", help="Backfill recent signals and generate a lookback backtest report")
    p_backtest_report.add_argument("--config", default="config.toml")
    p_backtest_report.add_argument("--tracker-db", default=".omx/state/signal_backtest_tracker.json")
    p_backtest_report.add_argument("--lookback-days", type=int, default=90)
    p_backtest_report.add_argument("--top-filters", type=int, default=10)
    p_backtest_report.add_argument("--min-filter-sample", type=int, default=3)

    p_session_status = sub.add_parser("session-status", help="Inspect Codex session state, worktree drift, and handoff freshness")
    p_session_status.add_argument("--limit", type=int, default=12)

    p_session_open = sub.add_parser("session-open", help="Build a start-of-session brief from status, checkpoint, and handoff")
    p_session_open.add_argument("--limit", type=int, default=12)

    p_session_checkpoint = sub.add_parser("session-checkpoint", help="Persist the current Codex work slice and exact next step")
    p_session_checkpoint.add_argument("--task", required=True)
    p_session_checkpoint.add_argument("--next-step", required=True)
    p_session_checkpoint.add_argument("--verification", action="append", default=[])
    p_session_checkpoint.add_argument("--note", action="append", default=[])
    p_session_checkpoint.add_argument("--scope", action="append", default=[])

    p_session_handoff = sub.add_parser("session-handoff", help="Write a resumable SESSION_HANDOFF.md from the current Codex state")
    p_session_handoff.add_argument("--summary", required=True)
    p_session_handoff.add_argument("--next-step", required=True)
    p_session_handoff.add_argument("--decision", action="append", default=[])
    p_session_handoff.add_argument("--blocker", action="append", default=[])
    p_session_handoff.add_argument("--output", action="append", default=[])
    p_session_handoff.add_argument("--verification", action="append", default=[])
    p_session_handoff.add_argument("--path", default="SESSION_HANDOFF.md")

    p_harness = sub.add_parser("run-harness", help="Run an offline smoke harness against the refactored analysis flow")
    p_harness.add_argument("--scenario", default="basic")
    p_harness.add_argument("--list-scenarios", action="store_true")

    p_all = sub.add_parser("analyze-all", help="Analyze all enabled channels from config")
    p_all.add_argument("--config", default="config.toml")
    p_all.add_argument("--limit", type=int, default=3, help="Videos per channel")

    p_30d = sub.add_parser("analyze-channel-30d", help="Run 30-day heuristic analysis with dashboard for a channel slug")
    p_30d.add_argument("slug", help="Channel slug from config (e.g. sampro)")
    p_30d.add_argument("--config", default="config.toml")
    p_30d.add_argument("--days", type=int, default=30, help="Window in days")
    return parser


def _report_summary(report, paths) -> dict:
    """Build a JSON-serializable summary dict from a report and its output paths."""
    return {
        "video": report.video.title,
        "video_type": report.signal_assessment.video_type,
        "signal_class": report.signal_assessment.video_signal_class,
        "signal_score": round(report.signal_assessment.signal_score, 1),
        "tickers": [m.ticker for m in report.ticker_mentions],
        "final_verdicts": {s.ticker: s.final_verdict for s in report.stock_analyses},
        "macro_insights_count": len(report.macro_insights),
        "expert_insights_count": len(report.expert_insights),
        "has_market_review": report.market_review is not None,
        "transcript_backed": report.transcript_backed,
        "source_quality_note": report.source_quality_note,
        "video_summary": report.video_summary,
        "json_path": str(paths[0]),
        "markdown_path": str(paths[1]),
        "text_path": str(paths[2]),
    }


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    configure_logging(verbose=args.verbose)

    try:
        if args.command == "register-channel":
            resolver = YoutubeResolver()
            discovery = resolver.discover_channel(args.url)
            registry = ChannelRegistry(Path(args.registry))
            row = registry.register(
                str(discovery.get("url") or args.url),
                {
                    "channel_id": discovery.get("channel_id"),
                    "channel_title": discovery.get("channel_title"),
                    "source_url": args.url,
                },
            )
            print(json.dumps(row, ensure_ascii=False, indent=2))
            return

        if args.command == "list-channels":
            registry = ChannelRegistry(Path(args.registry))
            print(json.dumps(registry.load(), ensure_ascii=False, indent=2))
            return

        if args.command == "backtest-ranked":
            logger.info("Running ranked backtest from %s", args.input_path)
            payload = json.loads(Path(args.input_path).read_text(encoding="utf-8"))
            ideas = [
                BacktestIdea(
                    ticker=item["ticker"],
                    company_name=item.get("company_name"),
                    score=float(item.get("aggregate_score", 0.0)),
                    signal_date=item.get("first_signal_at"),
                )
                for item in payload.get("cross_video_ranking", [])
            ]
            report = BacktestEngine().run_buy_and_hold(
                ideas=ideas,
                start_date=args.start_date,
                end_date=args.end_date,
                top_n=args.top_n,
                initial_capital=args.initial_capital,
            )
            print(json.dumps(report.to_dict(), ensure_ascii=False, indent=2))
            return

        if args.command == "backtest-artifact":
            payload = run_backtest_for_artifact(
                args.artifact_path,
                end_date=args.end_date,
                top_n=args.top_n,
                initial_capital=args.initial_capital,
            )
            print(json.dumps(payload, ensure_ascii=False, indent=2))
            return

        if args.command == "run-comparison":
            from scripts.run_channel_30d_comparison import run_comparison_job

            config = load_app_config(args.config)
            configure_logging(
                verbose=args.verbose,
                json_logs=config.logging.json,
                log_dir=config.logging.log_dir,
                retention_days=config.logging.retention_days,
            )
            payload = run_comparison_job(config)
            print(json.dumps(payload, ensure_ascii=False, indent=2))
            return

        if args.command == "run-scheduler":
            config = load_app_config(args.config)
            configure_logging(
                verbose=args.verbose,
                json_logs=config.logging.json,
                log_dir=config.logging.log_dir,
                retention_days=config.logging.retention_days,
            )
            if args.once:
                raise SystemExit(run_scheduled_job(config))
            run_scheduler_forever(config)
            return

        if args.command == "run-healthcheck":
            from .healthcheck import read_health_state

            print(json.dumps(read_health_state(args.path), ensure_ascii=False, indent=2))
            return

        if args.command == "signal-accuracy-report":
            tracker_db = SignalTrackerDB(Path(args.tracker_db))
            comparison_payload: dict = {}
            try:
                from dashboard.data_loader import load_channel_comparison

                comparison_payload = load_channel_comparison(Path(args.output_dir))
            except Exception:
                comparison_payload = {}
            channels = comparison_payload.get("channels", {}) if isinstance(comparison_payload, dict) else {}
            summary = build_signal_accuracy_summary(
                tracker_db,
                channel_metadata=channels if isinstance(channels, dict) else {},
                top_tickers=args.top_tickers,
            )
            run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            json_path, txt_path = save_signal_accuracy_report(summary, Path(args.output_dir), run_id)
            print(json.dumps({
                "generated_at": summary.get("generated_at", run_id),
                "tracker_db": str(Path(args.tracker_db)),
                "json_path": str(json_path),
                "txt_path": str(txt_path),
                "total_signals": summary.get("overall", {}).get("total_signals", 0),
                "channel_count": len(summary.get("by_channel", {})),
                "ticker_count": len(summary.get("by_ticker", {})),
            }, ensure_ascii=False, indent=2))
            return

        if args.command == "export-kindshot-feed":
            tracker_db = SignalTrackerDB(Path(args.tracker_db))
            payload = export_signals_for_kindshot(tracker_db, Path(args.output))
            print(json.dumps(payload, ensure_ascii=False, indent=2))
            return

        if args.command == "signal-backtest-report":
            config = load_app_config(args.config)
            configure_logging(
                verbose=args.verbose,
                json_logs=config.logging.json,
                log_dir=config.logging.log_dir,
                retention_days=config.logging.retention_days,
            )
            payload = run_signal_backtest_workflow(
                config_path=args.config,
                tracker_db_path=args.tracker_db,
                output_dir=args.output_dir,
                lookback_days=args.lookback_days,
                top_filters=args.top_filters,
                min_filter_sample=args.min_filter_sample,
            )
            print(json.dumps(payload, ensure_ascii=False, indent=2))
            return

        if args.command == "session-status":
            from .session_harness import collect_session_status

            payload = collect_session_status(limit=args.limit)
            print(json.dumps(payload, ensure_ascii=False, indent=2))
            return

        if args.command == "session-open":
            from .session_harness import build_session_open_brief

            payload = build_session_open_brief(limit=args.limit)
            print(json.dumps(payload, ensure_ascii=False, indent=2))
            return

        if args.command == "session-checkpoint":
            from .session_harness import write_session_checkpoint

            payload = write_session_checkpoint(
                task=args.task,
                next_step=args.next_step,
                verification=args.verification,
                notes=args.note,
                scope=args.scope,
            )
            print(json.dumps(payload, ensure_ascii=False, indent=2))
            return

        if args.command == "session-handoff":
            from .session_harness import write_session_handoff

            payload = write_session_handoff(
                summary=args.summary,
                next_step=args.next_step,
                decisions=args.decision,
                blockers=args.blocker,
                outputs=args.output,
                verification=args.verification,
                path=args.path,
            )
            print(json.dumps(payload, ensure_ascii=False, indent=2))
            return

        if args.command == "run-harness":
            from .harness import describe_harness_scenarios, run_harness

            if args.list_scenarios:
                print(json.dumps(describe_harness_scenarios(), ensure_ascii=False, indent=2))
                return
            provider_name = "mock" if args.provider == "auto" else args.provider
            payload = run_harness(
                output_dir=args.output_dir,
                scenario=args.scenario,
                provider_name=provider_name,
            )
            print(json.dumps(payload, ensure_ascii=False, indent=2))
            return

        if args.command == "analyze-channel-30d":
            from datetime import date, timezone as tz
            from .analysis_rows import analyze_resolved_videos_to_reports
            from .artifact_cleanup import cleanup_generated_artifacts
            from .comparison_rows import reports_to_comparison_rows
            from .comparison import RunContext, quality_scorecard, save_channel_artifacts
            from .evaluation import ranking_validation
            from .master_engine import validate_cross_stock_master_quality
            from .output_layout import build_run_output_dir
            from .reporting import save_combined_dashboard
            from .research import build_cross_video_ranking
            from .transcript_cache import TranscriptCache

            config = load_app_config(args.config)
            configure_logging(verbose=args.verbose, json_logs=config.logging.json, log_dir=config.logging.log_dir, retention_days=config.logging.retention_days)
            channel = next((ch for ch in config.channels if ch.slug == args.slug), None)
            if not channel:
                logger.error("Channel slug '%s' not found in config", args.slug)
                raise SystemExit(1)
            resolver = YoutubeResolver(**_network_proxy_kwargs(config))
            videos = resolver.resolve_channel_videos_since(channel.url, days=args.days)
            cache = TranscriptCache()
            cache.warm_from_output_dir(Path(args.output_dir))
            reports = analyze_resolved_videos_to_reports(
                videos,
                config=config,
                transcript_cache=cache,
                output_dir=Path(args.output_dir),
                persist=False,
            )
            rows = reports_to_comparison_rows(reports)
            validate_cross_stock_master_quality([stock for row in rows for stock in row["stocks"]])
            ranking = build_cross_video_ranking(rows)
            run_id = datetime.now(tz.utc).strftime("%Y%m%dT%H%M%SZ")
            context = RunContext(
                run_id=run_id,
                today=date.today().isoformat(),
                output_dir=build_run_output_dir(args.output_dir, run_id),
                window_days=args.days,
            )
            validation = ranking_validation(ranking, context.today)
            scorecard = quality_scorecard(rows, validation, ranking)
            json_path, txt_path = save_channel_artifacts(
                channel.slug, channel.display_name, channel.url,
                rows, ranking, validation, scorecard, context,
            )
            dashboard_path = save_combined_dashboard(reports, Path(args.output_dir), label=f"{channel.slug}_30d_dashboard") if reports else None
            if config.retention.enabled:
                cleanup_generated_artifacts(
                    output_dir=Path(args.output_dir),
                    report_dir=Path(args.output_dir).parent / "reports",
                    log_dir=Path(config.logging.log_dir),
                    output_days=config.retention.output_days,
                    report_days=config.retention.report_days,
                    log_days=config.logging.retention_days,
                )
            print(json.dumps({
                "json_path": str(json_path),
                "txt_path": str(txt_path),
                "dashboard_path": str(dashboard_path) if dashboard_path else None,
                "video_count": len(rows),
                "scorecard": scorecard,
            }, ensure_ascii=False, indent=2))
            return

        if args.command == "analyze-all":
            config = load_app_config(args.config)
            enabled = [ch for ch in config.channels if ch.enabled]
            if not enabled:
                logger.warning("No enabled channels in config")
                print(json.dumps({"channels": [], "error": "no enabled channels"}, ensure_ascii=False, indent=2))
                return
            all_results = {}
            for ch in enabled:
                logger.info("Analyzing channel %s (%s) limit=%d", ch.slug, ch.display_name, args.limit)
                ch_output = Path(args.output_dir) / ch.slug
                ch_pipeline = OMXPipeline(
                    provider_name=config.provider,
                    output_dir=ch_output,
                    mode=args.mode,
                    **_network_proxy_kwargs(config),
                )
                try:
                    results = ch_pipeline.analyze_channel(ch.url, limit=args.limit)
                    all_results[ch.slug] = {
                        "display_name": ch.display_name,
                        "videos_analyzed": len(results),
                        "reports": [_report_summary(report, paths) for report, paths in results],
                    }
                except Exception as exc:
                    logger.error("Channel %s failed: %s", ch.slug, exc)
                    all_results[ch.slug] = {"display_name": ch.display_name, "error": str(exc)}
            print(json.dumps(all_results, ensure_ascii=False, indent=2))
            return

        pipeline = OMXPipeline(provider_name=args.provider, output_dir=Path(args.output_dir), mode=args.mode)

        if args.command == "analyze-video":
            logger.info("Analyzing video %s", args.url)
            report, (json_path, md_path, txt_path) = pipeline.analyze_video(args.url)
            print(json.dumps(_report_summary(report, (json_path, md_path, txt_path)), ensure_ascii=False, indent=2))
            return

        if args.command == "analyze-channel":
            logger.info("Analyzing channel %s (limit=%s)", args.url, args.limit)
            results = pipeline.analyze_channel(args.url, limit=args.limit)
            summary = [_report_summary(report, paths) for report, paths in results]
            print(json.dumps(summary, ensure_ascii=False, indent=2))
            return
    except Exception as exc:
        logger.error("Command failed: %s", exc)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
