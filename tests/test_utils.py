import json

from omx_brainstorm.utils import chunk_text, read_json, split_sentences, unique_preserve


def test_chunk_text_splits_long_text():
    text = "A. " * 5000
    chunks = chunk_text(text, max_chars=1000)
    assert len(chunks) > 1
    assert all(len(c) <= 1000 for c in chunks)


def test_unique_preserve_keeps_order():
    assert unique_preserve(["A", "B", "A", "C"]) == ["A", "B", "C"]


def test_split_sentences_splits_on_punctuation():
    assert split_sentences("하나. 둘! 셋?") == ["하나.", "둘!", "셋?"]


def test_read_json_returns_default_when_missing(tmp_path):
    assert read_json(tmp_path / "missing.json", {"a": 1}) == {"a": 1}


def test_read_json_reads_existing_file(tmp_path):
    path = tmp_path / "data.json"
    path.write_text(json.dumps({"a": 1}), encoding="utf-8")
    assert read_json(path, {}) == {"a": 1}
