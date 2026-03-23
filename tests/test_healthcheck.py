import json

from omx_brainstorm.healthcheck import read_health_state


def test_read_health_state_reads_existing_file(tmp_path):
    path = tmp_path / "health.json"
    path.write_text(json.dumps({"status": "ok", "error_count": 2}), encoding="utf-8")
    state = read_health_state(path)
    assert state["status"] == "ok"
    assert state["error_count"] == 2


def test_read_health_state_default_has_unknown_status(tmp_path):
    state = read_health_state(tmp_path / "missing.json")
    assert state["status"] == "unknown"


def test_read_health_state_default_has_zero_errors(tmp_path):
    state = read_health_state(tmp_path / "missing.json")
    assert state["error_count"] == 0


def test_read_health_state_works_with_string_path(tmp_path):
    path = tmp_path / "health.json"
    path.write_text(json.dumps({"status": "ok"}), encoding="utf-8")
    state = read_health_state(str(path))
    assert state["status"] == "ok"
