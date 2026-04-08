from __future__ import annotations

import json
from pathlib import Path

from omx_brainstorm.harness import available_harness_scenarios, describe_harness_scenarios, run_harness


def test_available_harness_scenarios_exposes_core_scenarios():
    scenarios = available_harness_scenarios()
    assert "basic" in scenarios
    assert "metadata_fallback" in scenarios


def test_describe_harness_scenarios_includes_descriptions():
    descriptions = {item["name"]: item["description"] for item in describe_harness_scenarios()}
    assert "basic" in descriptions
    assert "metadata_fallback" in descriptions
    assert "metadata-only fallback" in descriptions["metadata_fallback"]


def test_run_harness_basic_generates_offline_artifacts(tmp_path: Path):
    payload = run_harness(output_dir=tmp_path, scenario="basic", run_id="20260408T060000Z")

    assert payload["scenario"] == "basic"
    assert payload["provider"] == "mock"
    assert payload["video_count"] == 3
    assert payload["stock_count"] >= 1
    assert payload["ranking_count"] >= 1
    assert payload["video_type_counts"]["STOCK_PICK"] == 1
    assert payload["video_type_counts"]["MARKET_REVIEW"] == 1
    assert payload["video_type_counts"]["EXPERT_INTERVIEW"] == 1

    dashboard_path = Path(payload["dashboard_path"])
    summary_path = Path(payload["summary_path"])
    ranking_path = Path(payload["ranking_path"])
    summary_txt_path = Path(payload["summary_txt_path"])
    run_dir = Path(payload["run_dir"])

    assert dashboard_path.exists()
    assert summary_path.exists()
    assert ranking_path.exists()
    assert summary_txt_path.exists()
    assert run_dir.exists()

    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    ranking = json.loads(ranking_path.read_text(encoding="utf-8"))
    assert summary["top_ticker"] == payload["top_ticker"]
    assert ranking[0]["ticker"] == payload["top_ticker"]
    assert len(summary["reports"]) == 3
    assert "Harness scenario: basic" in summary_txt_path.read_text(encoding="utf-8")


def test_run_harness_metadata_fallback_captures_downgraded_reports(tmp_path: Path):
    payload = run_harness(output_dir=tmp_path, scenario="metadata_fallback", run_id="20260408T060100Z")

    assert payload["scenario"] == "metadata_fallback"
    assert payload["stock_count"] == 0
    assert payload["ranking_count"] == 0
    assert payload["signal_class_counts"]["LOW_SIGNAL"] >= 1
    assert payload["top_ticker"] is None
    assert all(item["transcript_backed"] is False for item in payload["reports"])
    assert any(item["signal_class"] == "LOW_SIGNAL" for item in payload["reports"])


def test_run_harness_rejects_unknown_scenario(tmp_path: Path):
    try:
        run_harness(output_dir=tmp_path, scenario="unknown")
    except ValueError as exc:
        assert "Unknown harness scenario" in str(exc)
    else:
        raise AssertionError("expected ValueError for unknown scenario")
