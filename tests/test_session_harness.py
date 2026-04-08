from __future__ import annotations

import json
import os
from pathlib import Path

from omx_brainstorm.session_harness import (
    build_session_open_brief,
    collect_session_status,
    render_session_handoff,
    write_session_checkpoint,
    write_session_handoff,
)


def _fake_git_output(_workspace_root: Path, *args: str) -> str | None:
    if args == ("rev-parse", "--abbrev-ref", "HEAD"):
        return "feat/codex-harness"
    if args == ("rev-parse", "--short", "HEAD"):
        return "abc1234"
    if args == ("status", "--short"):
        return " M src/demo.py\n?? tests/new_spec.py"
    return None


def test_collect_session_status_reports_handoff_staleness(monkeypatch, tmp_path: Path):
    handoff = tmp_path / "SESSION_HANDOFF.md"
    handoff.write_text("old handoff", encoding="utf-8")
    os.utime(handoff, (1, 1))

    src_file = tmp_path / "src" / "demo.py"
    src_file.parent.mkdir(parents=True, exist_ok=True)
    src_file.write_text("print('x')\n", encoding="utf-8")

    test_file = tmp_path / "tests" / "new_spec.py"
    test_file.parent.mkdir(parents=True, exist_ok=True)
    test_file.write_text("def test_x():\n    pass\n", encoding="utf-8")

    ralph_state = tmp_path / ".omx" / "state" / "ralph-state.json"
    ralph_state.parent.mkdir(parents=True, exist_ok=True)
    ralph_state.write_text(json.dumps({"active": True, "current_phase": "development"}), encoding="utf-8")

    latest_context = tmp_path / ".omx" / "context" / "recent.md"
    latest_context.parent.mkdir(parents=True, exist_ok=True)
    latest_context.write_text("ctx", encoding="utf-8")

    latest_plan = tmp_path / ".omx" / "plans" / "recent.md"
    latest_plan.parent.mkdir(parents=True, exist_ok=True)
    latest_plan.write_text("plan", encoding="utf-8")

    monkeypatch.setattr("omx_brainstorm.session_harness._git_output", _fake_git_output)
    status = collect_session_status(workspace_root=tmp_path, limit=5)

    assert status["git_branch"] == "feat/codex-harness"
    assert status["git_head"] == "abc1234"
    assert status["git_status_counts"]["dirty"] == 2
    assert status["git_status_counts"]["modified"] >= 1
    assert status["git_status_counts"]["untracked"] == 1
    assert status["handoff"]["exists"] is True
    assert status["handoff"]["stale_vs_worktree"] is True
    assert status["latest_context"]["path"] == ".omx/context/recent.md"
    assert status["latest_plan"]["path"] == ".omx/plans/recent.md"
    assert len(status["dirty_files"]) == 2


def test_write_session_checkpoint_persists_task_state(monkeypatch, tmp_path: Path):
    monkeypatch.setattr("omx_brainstorm.session_harness._git_output", _fake_git_output)
    payload = write_session_checkpoint(
        task="codex process harness",
        next_step="add handoff writer",
        workspace_root=tmp_path,
        verification=["pytest -q tests/test_session_harness.py"],
        notes=["status + checkpoint first"],
        scope=["src/omx_brainstorm/session_harness.py"],
    )

    checkpoint_path = tmp_path / ".omx" / "state" / "codex_harness.json"
    assert checkpoint_path.exists()
    written = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    assert written["task"] == "codex process harness"
    assert written["next_step"] == "add handoff writer"
    assert written["verification"] == ["pytest -q tests/test_session_harness.py"]
    assert written["scope"] == ["src/omx_brainstorm/session_harness.py"]
    assert payload["status"]["git_branch"] == "feat/codex-harness"


def test_render_and_write_session_handoff_include_required_sections(monkeypatch, tmp_path: Path):
    monkeypatch.setattr("omx_brainstorm.session_harness._git_output", _fake_git_output)
    write_session_checkpoint(
        task="codex process harness",
        next_step="wire CLI commands",
        workspace_root=tmp_path,
        notes=["checkpoint active"],
    )
    payload = write_session_handoff(
        summary="세션 처리용 하네스를 구축 중이다.",
        next_step="session-status와 session-handoff CLI를 검증한다.",
        workspace_root=tmp_path,
        decisions=["핸드오프와 체크포인트를 분리한다."],
        blockers=["없음"],
        outputs=[".omx/state/codex_harness.json"],
        verification=["pytest -q tests/test_session_harness.py"],
    )

    handoff_path = Path(payload["path"])
    text = handoff_path.read_text(encoding="utf-8")
    assert handoff_path.exists()
    assert "## Current progress" in text
    assert "## Important decisions" in text
    assert "## Blockers / limitations" in text
    assert "## Generated outputs" in text
    assert "## Exact next step" in text
    assert "세션 처리용 하네스를 구축 중이다." in text
    assert "session-status와 session-handoff CLI를 검증한다." in text

    rendered = render_session_handoff(
        status=collect_session_status(workspace_root=tmp_path),
        summary="요약",
        next_step="다음",
        decisions=["결정"],
        blockers=["제약"],
        outputs=["산출물"],
        verification=["검증"],
        checkpoint={"task": "t", "next_step": "n", "scope": ["a"], "notes": ["b"]},
    )
    assert "## Active checkpoint" in rendered


def test_build_session_open_brief_prioritizes_checkpoint_then_handoff(monkeypatch, tmp_path: Path):
    handoff = tmp_path / "SESSION_HANDOFF.md"
    handoff.write_text(
        "\n".join(
            [
                "# OMX Session Handoff",
                "",
                "## Current progress",
                "현재 progress 요약",
                "",
                "## Exact next step",
                "- handoff next step",
                "",
            ]
        ),
        encoding="utf-8",
    )
    checkpoint_path = tmp_path / ".omx" / "state" / "codex_harness.json"
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    checkpoint_path.write_text(
        json.dumps({"task": "t", "next_step": "checkpoint next step"}),
        encoding="utf-8",
    )
    monkeypatch.setattr("omx_brainstorm.session_harness._git_output", _fake_git_output)

    brief = build_session_open_brief(workspace_root=tmp_path, limit=4)

    assert brief["startup_focus"] == "checkpoint next step"
    assert brief["handoff_exact_next_step"] == "handoff next step"
    assert "현재 progress 요약" in brief["handoff_current_progress"]
    assert any(item["path"] == "AGENTS.md" for item in brief["must_read"])
    assert any("Active Codex checkpoint exists" == item for item in brief["alerts"])
    assert any("Resume from checkpoint next step: checkpoint next step" == item for item in brief["resume_actions"])


def test_build_session_open_brief_falls_back_to_handoff_when_no_checkpoint(monkeypatch, tmp_path: Path):
    handoff = tmp_path / "SESSION_HANDOFF.md"
    handoff.write_text(
        "\n".join(
            [
                "# OMX Session Handoff",
                "",
                "## Current progress",
                "handoff progress",
                "",
                "## Exact next step",
                "- handoff only next step",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr("omx_brainstorm.session_harness._git_output", _fake_git_output)

    brief = build_session_open_brief(workspace_root=tmp_path)

    assert brief["startup_focus"] == "handoff only next step"
    assert any("No active Codex checkpoint" == item for item in brief["alerts"])
    assert any("Resume from handoff next step: handoff only next step" == item for item in brief["resume_actions"])


def test_build_session_open_brief_prefers_newer_handoff_over_older_checkpoint(monkeypatch, tmp_path: Path):
    handoff = tmp_path / "SESSION_HANDOFF.md"
    handoff.write_text(
        "\n".join(
            [
                "# OMX Session Handoff",
                "",
                "## Current progress",
                "new handoff progress",
                "",
                "## Exact next step",
                "- newer handoff step",
            ]
        ),
        encoding="utf-8",
    )
    checkpoint_path = tmp_path / ".omx" / "state" / "codex_harness.json"
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    checkpoint_path.write_text(
        json.dumps(
            {
                "updated_at": "2026-04-08T05:00:00+00:00",
                "task": "t",
                "next_step": "older checkpoint step",
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr("omx_brainstorm.session_harness._git_output", _fake_git_output)

    brief = build_session_open_brief(workspace_root=tmp_path)

    assert brief["startup_focus"] == "newer handoff step"
    assert any("Codex checkpoint is older than SESSION_HANDOFF.md" == item for item in brief["alerts"])
    assert any("Checkpoint predates the latest handoff; refresh session-checkpoint after resuming." == item for item in brief["resume_actions"])
