from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import subprocess
from typing import Any

from .utils import read_json, write_json


CHECKPOINT_STATE_PATH = Path(".omx/state/codex_harness.json")
DEFAULT_STATUS_LIMIT = 12


def build_session_open_brief(
    *,
    workspace_root: str | Path = ".",
    limit: int = DEFAULT_STATUS_LIMIT,
) -> dict[str, Any]:
    status = collect_session_status(workspace_root=workspace_root, limit=limit)
    root = Path(status["workspace_root"])
    handoff_path = root / "SESSION_HANDOFF.md"
    checkpoint = status.get("checkpoint") or {}
    handoff_next_step = _extract_handoff_exact_next_step(handoff_path)
    handoff_progress = _extract_handoff_section_text(handoff_path, "## Current progress")
    handoff_updated_at = status.get("handoff", {}).get("updated_at")
    checkpoint_updated_at = checkpoint.get("updated_at")
    checkpoint_is_older_than_handoff = bool(
        checkpoint_updated_at
        and handoff_updated_at
        and checkpoint_updated_at < handoff_updated_at
    )

    must_read = [
        {"path": "AGENTS.md", "reason": "workspace operating contract", "exists": (root / "AGENTS.md").exists()},
        {"path": "SESSION_HANDOFF.md", "reason": "resume summary and exact next step", "exists": handoff_path.exists()},
        {"path": "README.md", "reason": "local commands and verification paths", "exists": (root / "README.md").exists()},
    ]
    latest_context = status.get("latest_context") or {}
    if latest_context.get("path"):
        must_read.append({"path": latest_context["path"], "reason": "latest execution context", "exists": True})
    latest_plan = status.get("latest_plan") or {}
    if latest_plan.get("path"):
        must_read.append({"path": latest_plan["path"], "reason": "latest saved plan/spec", "exists": True})

    alerts: list[str] = []
    dirty_count = int(status.get("git_status_counts", {}).get("dirty", 0) or 0)
    if dirty_count:
        alerts.append(f"Dirty worktree: {dirty_count} files")
    if status.get("handoff", {}).get("stale_vs_worktree"):
        alerts.append("SESSION_HANDOFF.md is older than the current worktree")
    if checkpoint:
        alerts.append("Active Codex checkpoint exists")
        if checkpoint_is_older_than_handoff:
            alerts.append("Codex checkpoint is older than SESSION_HANDOFF.md")
    else:
        alerts.append("No active Codex checkpoint")
    ralph_state = status.get("ralph_state") or {}
    if isinstance(ralph_state, dict) and ralph_state.get("current_phase"):
        alerts.append(f"Ralph state: {ralph_state.get('current_phase')}")

    resume_actions = [
        "Read AGENTS.md before editing.",
    ]
    if handoff_path.exists():
        resume_actions.append("Read SESSION_HANDOFF.md before expanding scope.")
    resume_actions.append("Review recent dirty files before changing code.")
    if checkpoint.get("next_step") and not checkpoint_is_older_than_handoff:
        resume_actions.append(f"Resume from checkpoint next step: {checkpoint['next_step']}")
    elif handoff_next_step:
        resume_actions.append(f"Resume from handoff next step: {handoff_next_step}")
    if checkpoint_is_older_than_handoff:
        resume_actions.append("Checkpoint predates the latest handoff; refresh session-checkpoint after resuming.")
    if status.get("handoff", {}).get("stale_vs_worktree"):
        resume_actions.append("Refresh session-checkpoint if the current slice has moved since the last handoff.")

    startup_focus = (
        (None if checkpoint_is_older_than_handoff else checkpoint.get("next_step"))
        or handoff_next_step
        or "Inspect worktree drift, then define the next bounded slice."
    )
    return {
        "generated_at": status.get("generated_at"),
        "workspace_root": status.get("workspace_root"),
        "startup_focus": startup_focus,
        "handoff_current_progress": handoff_progress,
        "handoff_exact_next_step": handoff_next_step,
        "must_read": must_read,
        "alerts": alerts,
        "resume_actions": resume_actions,
        "status": status,
    }


def collect_session_status(
    *,
    workspace_root: str | Path = ".",
    limit: int = DEFAULT_STATUS_LIMIT,
) -> dict[str, Any]:
    root = Path(workspace_root).resolve()
    handoff_path = root / "SESSION_HANDOFF.md"
    checkpoint_path = root / CHECKPOINT_STATE_PATH
    ralph_state_path = root / ".omx/state/ralph-state.json"
    context_dir = root / ".omx/context"
    plan_dir = root / ".omx/plans"

    git_branch = _git_output(root, "rev-parse", "--abbrev-ref", "HEAD")
    git_head = _git_output(root, "rev-parse", "--short", "HEAD")
    status_lines = _git_lines(root, "status", "--short")
    dirty_files = [_parse_git_status_line(line) for line in status_lines if line.strip()]
    dirty_files = [item for item in dirty_files if item]
    recent_dirty_files = _sort_paths_by_mtime(root, dirty_files, limit=limit)

    handoff_exists = handoff_path.exists()
    handoff_updated_at = _file_timestamp(handoff_path)
    latest_context_path = _latest_file(context_dir, "*.md")
    latest_plan_path = _latest_file(plan_dir, "*.md")
    latest_dirty_timestamp = _latest_timestamp(root, dirty_files)
    handoff_stale = bool(
        handoff_exists
        and handoff_updated_at
        and latest_dirty_timestamp
        and latest_dirty_timestamp > handoff_updated_at
    )

    checkpoint = read_json(checkpoint_path, None)
    ralph_state = read_json(ralph_state_path, {})
    counts = _status_counts(status_lines)

    return {
        "workspace_root": str(root),
        "generated_at": _now_iso(),
        "git_branch": git_branch,
        "git_head": git_head,
        "git_status_counts": counts,
        "dirty_files": recent_dirty_files,
        "handoff": {
            "path": str(handoff_path),
            "exists": handoff_exists,
            "updated_at": handoff_updated_at,
            "stale_vs_worktree": handoff_stale,
        },
        "checkpoint": checkpoint,
        "ralph_state": ralph_state if isinstance(ralph_state, dict) else {},
        "latest_context": _file_summary(root, latest_context_path),
        "latest_plan": _file_summary(root, latest_plan_path),
    }


def write_session_checkpoint(
    *,
    task: str,
    next_step: str,
    workspace_root: str | Path = ".",
    verification: list[str] | None = None,
    notes: list[str] | None = None,
    scope: list[str] | None = None,
) -> dict[str, Any]:
    root = Path(workspace_root).resolve()
    payload = {
        "updated_at": _now_iso(),
        "task": task,
        "next_step": next_step,
        "verification": list(verification or []),
        "notes": list(notes or []),
        "scope": list(scope or []),
        "status": collect_session_status(workspace_root=root),
    }
    write_json(root / CHECKPOINT_STATE_PATH, payload)
    return payload


def write_session_handoff(
    *,
    summary: str,
    next_step: str,
    workspace_root: str | Path = ".",
    decisions: list[str] | None = None,
    blockers: list[str] | None = None,
    outputs: list[str] | None = None,
    verification: list[str] | None = None,
    path: str | Path = "SESSION_HANDOFF.md",
) -> dict[str, Any]:
    root = Path(workspace_root).resolve()
    handoff_path = root / Path(path)
    status = collect_session_status(workspace_root=root)
    checkpoint = read_json(root / CHECKPOINT_STATE_PATH, None)
    content = render_session_handoff(
        status=status,
        summary=summary,
        next_step=next_step,
        decisions=list(decisions or []),
        blockers=list(blockers or []),
        outputs=list(outputs or []),
        verification=list(verification or []),
        checkpoint=checkpoint if isinstance(checkpoint, dict) else None,
    )
    handoff_path.write_text(content, encoding="utf-8")
    return {
        "path": str(handoff_path),
        "updated_at": _file_timestamp(handoff_path),
        "status": status,
    }


def render_session_handoff(
    *,
    status: dict[str, Any],
    summary: str,
    next_step: str,
    decisions: list[str],
    blockers: list[str],
    outputs: list[str],
    verification: list[str],
    checkpoint: dict[str, Any] | None,
) -> str:
    lines = [
        "# OMX Session Handoff",
        "",
        "## Resume rule",
        "다음 세션에서는 `AGENTS.md`와 `SESSION_HANDOFF.md`를 먼저 읽고 이어서 진행한다.",
        "",
        "## Workspace snapshot",
        f"- Workspace: `{status.get('workspace_root', '.')}`",
        f"- Generated at: `{status.get('generated_at', '-')}`",
        f"- Branch / HEAD: `{status.get('git_branch') or '-'} / {status.get('git_head') or '-'}`",
        f"- Dirty files: `{status.get('git_status_counts', {}).get('dirty', 0)}`",
        f"- Handoff stale vs worktree: `{status.get('handoff', {}).get('stale_vs_worktree', False)}`",
    ]
    latest_context = status.get("latest_context") or {}
    latest_plan = status.get("latest_plan") or {}
    if latest_context.get("path"):
        lines.append(f"- Latest context: `{latest_context['path']}`")
    if latest_plan.get("path"):
        lines.append(f"- Latest plan: `{latest_plan['path']}`")
    lines.extend(
        [
            "",
            "## Current progress",
            summary.strip(),
        ]
    )
    if checkpoint:
        lines.extend(
            [
                "",
                "## Active checkpoint",
                f"- Task: {checkpoint.get('task') or '-'}",
                f"- Next step in checkpoint: {checkpoint.get('next_step') or '-'}",
            ]
        )
        if checkpoint.get("scope"):
            lines.append(f"- Scope: {', '.join(str(item) for item in checkpoint.get('scope', []))}")
        if checkpoint.get("notes"):
            lines.append(f"- Notes: {' | '.join(str(item) for item in checkpoint.get('notes', []))}")
    lines.extend(["", "## Important decisions"])
    if decisions:
        for item in decisions:
            lines.append(f"- {item}")
    else:
        lines.append("- None recorded")
    lines.extend(["", "## Blockers / limitations"])
    if blockers:
        for item in blockers:
            lines.append(f"- {item}")
    else:
        lines.append("- None recorded")
    lines.extend(["", "## Generated outputs"])
    if outputs:
        for item in outputs:
            lines.append(f"- {item}")
    else:
        lines.append("- None recorded")
    lines.extend(["", "## Verification"])
    if verification:
        for item in verification:
            lines.append(f"- {item}")
    else:
        lines.append("- Not run")
    dirty_files = status.get("dirty_files", [])
    lines.extend(["", "## Dirty files"])
    if dirty_files:
        for item in dirty_files:
            lines.append(f"- {item['status']} {item['path']}")
    else:
        lines.append("- Clean worktree")
    lines.extend(["", "## Exact next step", f"- {next_step.strip()}"])
    return "\n".join(lines) + "\n"


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _extract_handoff_exact_next_step(path: Path) -> str | None:
    value = _extract_handoff_section_text(path, "## Exact next step")
    if not value:
        return None
    line = value.splitlines()[0].strip()
    return line[2:].strip() if line.startswith("- ") else line or None


def _extract_handoff_section_text(path: Path, heading: str) -> str | None:
    if not path.exists():
        return None
    lines = path.read_text(encoding="utf-8").splitlines()
    capture = False
    collected: list[str] = []
    for line in lines:
        if line.strip() == heading:
            capture = True
            continue
        if capture and line.startswith("## "):
            break
        if capture:
            if not collected and not line.strip():
                continue
            collected.append(line)
    text = "\n".join(collected).strip()
    return text or None


def _file_timestamp(path: Path) -> str | None:
    if not path.exists():
        return None
    return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).replace(microsecond=0).isoformat()


def _latest_file(root: Path, pattern: str) -> Path | None:
    if not root.exists():
        return None
    candidates = [path for path in root.glob(pattern) if path.is_file()]
    if not candidates:
        return None
    return max(candidates, key=lambda item: item.stat().st_mtime)


def _file_summary(workspace_root: Path, path: Path | None) -> dict[str, Any] | None:
    if path is None:
        return None
    return {
        "path": str(path.relative_to(workspace_root)),
        "updated_at": _file_timestamp(path),
    }


def _latest_timestamp(workspace_root: Path, dirty_files: list[dict[str, str]]) -> str | None:
    timestamps = []
    for item in dirty_files:
        path = workspace_root / item["path"]
        if path.exists():
            timestamps.append(_file_timestamp(path))
    timestamps = [item for item in timestamps if item]
    return max(timestamps) if timestamps else None


def _sort_paths_by_mtime(workspace_root: Path, dirty_files: list[dict[str, str]], *, limit: int) -> list[dict[str, str]]:
    enriched = []
    for item in dirty_files:
        path = workspace_root / item["path"]
        if path.exists():
            timestamp = _file_timestamp(path)
        else:
            timestamp = None
        enriched.append(
            {
                "status": item["status"],
                "path": item["path"],
                "updated_at": timestamp,
            }
        )
    enriched.sort(key=lambda item: item.get("updated_at") or "", reverse=True)
    return enriched[: max(1, limit)]


def _status_counts(lines: list[str]) -> dict[str, int]:
    counts = {"dirty": 0, "modified": 0, "added": 0, "deleted": 0, "untracked": 0}
    for line in lines:
        if not line.strip():
            continue
        counts["dirty"] += 1
        if line.startswith("??"):
            counts["untracked"] += 1
            continue
        x = line[0]
        y = line[1]
        for code in (x, y):
            if code == "M":
                counts["modified"] += 1
            elif code == "A":
                counts["added"] += 1
            elif code == "D":
                counts["deleted"] += 1
    return counts


def _parse_git_status_line(line: str) -> dict[str, str] | None:
    if not line.strip():
        return None
    status = line[:2]
    raw_path = line[3:].strip()
    path = raw_path.split(" -> ", 1)[-1]
    return {"status": status, "path": path}


def _git_lines(workspace_root: Path, *args: str) -> list[str]:
    output = _git_output(workspace_root, *args)
    if not output:
        return []
    return output.splitlines()


def _git_output(workspace_root: Path, *args: str) -> str | None:
    try:
        completed = subprocess.run(
            ["git", *args],
            cwd=workspace_root,
            text=True,
            capture_output=True,
            check=False,
        )
    except OSError:
        return None
    if completed.returncode != 0:
        return None
    value = completed.stdout.strip()
    return value or None
