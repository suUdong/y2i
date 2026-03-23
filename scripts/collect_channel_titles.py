from __future__ import annotations

import argparse
import json
from datetime import date, timedelta
from pathlib import Path

from yt_dlp import YoutubeDL

from omx_brainstorm.app_config import load_app_config
from omx_brainstorm.title_taxonomy import classify_title, summarize_title_classes


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Collect up to N years of video titles for a configured channel.")
    parser.add_argument("slug")
    parser.add_argument("--config", default="config.toml")
    parser.add_argument("--years", type=int, default=3)
    parser.add_argument("--max-entries", type=int, default=1200)
    parser.add_argument("--fast", action="store_true", help="Use title-only playlist extraction without per-video date resolution")
    return parser


def collect_titles(channel_url: str, years: int = 3, max_entries: int = 1200, fast: bool = False) -> list[dict]:
    cutoff = date.today() - timedelta(days=365 * years)
    opts = {"quiet": True, "no_warnings": True, "extract_flat": fast, "skip_download": True, "playlistend": max_entries}
    with YoutubeDL(opts) as ydl:
        info = ydl.extract_info(channel_url, download=False)
    entries = info.get("entries") or []
    rows = []
    for entry in entries:
        video_id = entry.get("id")
        title = entry.get("title")
        if not video_id or not title:
            continue
        published_at = str(entry.get("upload_date") or "")
        normalized = f"{published_at[:4]}-{published_at[4:6]}-{published_at[6:8]}" if len(published_at) >= 8 and published_at[:8].isdigit() else None
        if not fast and normalized is not None and date.fromisoformat(normalized) < cutoff:
            break
        rows.append(
            {
                "video_id": video_id,
                "title": title,
                "published_at": normalized,
                "url": f"https://www.youtube.com/watch?v={video_id}",
                "labels": classify_title(title),
            }
        )
    return rows


def main() -> None:
    args = build_parser().parse_args()
    config = load_app_config(args.config)
    channel = next(item for item in config.channels if item.slug == args.slug)
    rows = collect_titles(channel.url, years=args.years, max_entries=args.max_entries, fast=args.fast)

    output_dir = Path(config.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    md_path = output_dir / f"{args.slug}_video_titles.md"
    json_path = output_dir / f"{args.slug}_video_titles.json"
    summary = summarize_title_classes([row["title"] for row in rows])
    json_path.write_text(json.dumps({"channel": channel.display_name, "years": args.years, "titles": rows, "summary": summary}, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [
        f"# {channel.display_name} 영상 제목 목록",
        f"**수집 범위:** 최근 최대 {args.years}년",
        f"**수집 방식:** {'빠른 title-only 수집' if args.fast else '메타데이터 포함 수집'}",
        f"**수집 개수:** {len(rows)}개",
        "",
        "## 분류 요약",
    ]
    for key, value in summary.items():
        lines.append(f"- {key}: {value}")
    lines.extend(["", "## 제목 목록"])
    for idx, row in enumerate(rows, start=1):
        labels = ", ".join(row["labels"])
        lines.append(f"{idx}. `{row['published_at'] or '-'}' | [{row['title']}]({row['url']}) | {labels}")
    md_path.write_text("\n".join(lines), encoding="utf-8")
    print(json.dumps({"md_path": str(md_path), "json_path": str(json_path), "count": len(rows)}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
