from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Iterable, TypeVar

from .models import TickerMention

T = TypeVar("T")


def ensure_dir(path: Path) -> Path:
    """Create a directory path if it does not exist and return it."""
    path.mkdir(parents=True, exist_ok=True)
    return path


def read_json(path: Path, default: T) -> T:
    """Read JSON from path or return the provided default when missing."""
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, data: object) -> None:
    """Write JSON data to disk with directory creation."""
    ensure_dir(path.parent)
    tmp_path = path.with_name(f".{path.name}.tmp")
    tmp_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_path.replace(path)


_whitespace = re.compile(r"\s+")


def normalize_ws(text: str) -> str:
    """Collapse repeated whitespace into single spaces."""
    return _whitespace.sub(" ", text).strip()


_sentence_split = re.compile(r"(?<=[.!?。！？])\s+|\n+")


def split_sentences(text: str) -> list[str]:
    """Split text into normalized sentence-like chunks."""
    return [normalize_ws(s) for s in _sentence_split.split(text) if normalize_ws(s)]


def chunk_text(text: str, max_chars: int = 12000) -> list[str]:
    """Chunk long text into sentence-preserving blocks."""
    sentences = split_sentences(text)
    chunks: list[str] = []
    current: list[str] = []
    current_len = 0
    for sentence in sentences:
        add_len = len(sentence) + (1 if current else 0)
        if current and current_len + add_len > max_chars:
            chunks.append(" ".join(current))
            current = [sentence]
            current_len = len(sentence)
        else:
            current.append(sentence)
            current_len += add_len
    if current:
        chunks.append(" ".join(current))
    return chunks or [normalize_ws(text)]


def unique_preserve(values: Iterable[str]) -> list[str]:
    """Return unique values while preserving first-seen order."""
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value not in seen:
            seen.add(value)
            result.append(value)
    return result


def merge_mention(mentions: dict[str, TickerMention], mention: TickerMention) -> None:
    """Merge a new ticker mention into an in-memory mention index."""
    if mention.ticker in mentions:
        prev = mentions[mention.ticker]
        prev.confidence = max(prev.confidence, mention.confidence)
        prev.evidence = unique_preserve(prev.evidence + mention.evidence)
        if not prev.reason:
            prev.reason = mention.reason
        if not prev.company_name:
            prev.company_name = mention.company_name
    else:
        mentions[mention.ticker] = mention
