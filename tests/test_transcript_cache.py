import json

from omx_brainstorm.transcript_cache import TranscriptCache
from omx_brainstorm.models import VideoInput


def test_transcript_cache_ingests_report_artifact(tmp_path):
    artifact = {
        "video": {
            "video_id": "abc123def45",
            "title": "제목",
            "url": "https://youtube.com/watch?v=abc123def45",
            "published_at": "20260318",
        },
        "transcript_text": "엔비디아가 아직 더 갈 수 있다.",
        "transcript_language": "ko",
        "ticker_mentions": [
            {"ticker": "NVDA", "company_name": "NVIDIA", "evidence": ["엔비디아가 아직 더 갈 수 있다."]},
        ],
    }
    artifact_path = tmp_path / "artifact.json"
    artifact_path.write_text(json.dumps(artifact, ensure_ascii=False), encoding="utf-8")

    cache = TranscriptCache(tmp_path / "cache")
    assert cache.warm_from_report_artifact(artifact_path) is True

    cached = cache.load("abc123def45")
    assert cached["transcript_text"] == "엔비디아가 아직 더 갈 수 있다."
    assert cached["ticker_mentions"][0]["ticker"] == "NVDA"


def test_transcript_cache_save_and_load_roundtrip(tmp_path):
    cache = TranscriptCache(tmp_path / "cache")
    video = VideoInput(video_id="vid1", title="제목", url="https://youtube.com/watch?v=vid1")
    cache.save(video, "text", "ko", "transcript_api", ticker_mentions=[{"ticker": "NVDA"}])
    payload = cache.load("vid1")
    assert payload["transcript_text"] == "text"
    assert payload["source"] == "transcript_api"


def test_transcript_cache_load_returns_none_on_corruption(tmp_path):
    cache = TranscriptCache(tmp_path / "cache")
    cache.path_for("bad").write_text("{not-json", encoding="utf-8")
    assert cache.load("bad") is None


def test_transcript_cache_warm_from_output_dir_counts_valid_files(tmp_path):
    cache = TranscriptCache(tmp_path / "cache")
    out = tmp_path / "out"
    out.mkdir()
    (out / "a.json").write_text(json.dumps({"video": {"video_id": "a", "title": "A", "url": "u"}, "transcript_text": "x"}), encoding="utf-8")
    (out / "b.json").write_text("broken", encoding="utf-8")
    assert cache.warm_from_output_dir(out) == 1


def test_transcript_cache_path_for_uses_json_extension(tmp_path):
    cache = TranscriptCache(tmp_path / "cache")
    assert cache.path_for("abc").name == "abc.json"


def test_is_stale_returns_true_for_missing_entry(tmp_path):
    cache = TranscriptCache(tmp_path / "cache")
    assert cache.is_stale("nonexistent") is True


def test_is_stale_returns_false_for_fresh_entry(tmp_path):
    cache = TranscriptCache(tmp_path / "cache", max_age_hours=24)
    video = VideoInput(video_id="fresh1", title="제목", url="https://youtube.com/watch?v=fresh1")
    cache.save(video, "text", "ko", "transcript_api")
    assert cache.is_stale("fresh1") is False


def test_is_stale_returns_true_for_old_entry(tmp_path):
    cache = TranscriptCache(tmp_path / "cache", max_age_hours=24)
    video = VideoInput(video_id="old1", title="제목", url="https://youtube.com/watch?v=old1")
    cache.save(video, "text", "ko", "transcript_api")
    # Manually set cached_at to 2 days ago
    path = cache.path_for("old1")
    data = json.loads(path.read_text(encoding="utf-8"))
    from datetime import datetime, timedelta, timezone
    old_time = (datetime.now(timezone.utc) - timedelta(hours=49)).isoformat()
    data["cached_at"] = old_time
    path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    assert cache.is_stale("old1") is True


def test_is_stale_with_custom_max_age(tmp_path):
    cache = TranscriptCache(tmp_path / "cache", max_age_hours=168)
    video = VideoInput(video_id="cust1", title="제목", url="https://youtube.com/watch?v=cust1")
    cache.save(video, "text", "ko", "transcript_api")
    # Fresh entry with override: 0 hours means always stale
    assert cache.is_stale("cust1", max_age_hours=0) is True
    # Fresh entry with large override: not stale
    assert cache.is_stale("cust1", max_age_hours=9999) is False


def test_is_stale_returns_true_for_missing_cached_at(tmp_path):
    cache = TranscriptCache(tmp_path / "cache")
    video = VideoInput(video_id="notime1", title="제목", url="https://youtube.com/watch?v=notime1")
    cache.save(video, "text", "ko", "transcript_api")
    # Remove cached_at field
    path = cache.path_for("notime1")
    data = json.loads(path.read_text(encoding="utf-8"))
    del data["cached_at"]
    path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    assert cache.is_stale("notime1") is True
