from datetime import datetime, timedelta, timezone

from omx_brainstorm.artifact_cleanup import cleanup_generated_artifacts


def test_cleanup_generated_artifacts_removes_old_files(tmp_path):
    now = datetime(2026, 4, 8, tzinfo=timezone.utc)
    output_dir = tmp_path / "output"
    report_dir = tmp_path / "reports"
    log_dir = tmp_path / ".omx" / "logs"
    output_dir.mkdir(parents=True)
    report_dir.mkdir(parents=True)
    log_dir.mkdir(parents=True)

    fresh_output = output_dir / "fresh.json"
    stale_output = output_dir / "stale.json"
    fresh_report = report_dir / "fresh.md"
    stale_report = report_dir / "stale.md"
    fresh_log = log_dir / "fresh.log"
    stale_log = log_dir / "stale.log"
    pid_file = log_dir / "daemon.pid"

    for path in (fresh_output, stale_output, fresh_report, stale_report, fresh_log, stale_log, pid_file):
        path.write_text("x", encoding="utf-8")

    fresh_ts = (now - timedelta(days=1)).timestamp()
    stale_ts = (now - timedelta(days=40)).timestamp()
    for path in (fresh_output, fresh_report, fresh_log, pid_file):
        path.touch()
        import os
        os.utime(path, (fresh_ts, fresh_ts))
    for path in (stale_output, stale_report, stale_log):
        path.touch()
        import os
        os.utime(path, (stale_ts, stale_ts))

    summary = cleanup_generated_artifacts(
        output_dir=output_dir,
        report_dir=report_dir,
        log_dir=log_dir,
        output_days=14,
        report_days=30,
        log_days=7,
        now=now,
    )

    assert summary["output_removed"] == 1
    assert summary["report_removed"] == 1
    assert summary["log_removed"] == 1
    assert fresh_output.exists()
    assert fresh_report.exists()
    assert fresh_log.exists()
    assert pid_file.exists()
    assert not stale_output.exists()
    assert not stale_report.exists()
    assert not stale_log.exists()
