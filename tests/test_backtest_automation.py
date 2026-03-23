import json

from omx_brainstorm.backtest_automation import run_backtest_for_artifact


def test_run_backtest_for_artifact_handles_empty_ranking(tmp_path):
    artifact = tmp_path / "artifact.json"
    artifact.write_text(json.dumps({"cross_video_ranking": []}), encoding="utf-8")

    result = run_backtest_for_artifact(artifact)

    assert result["status"] == "no_ranking"
