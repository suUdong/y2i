from __future__ import annotations

import argparse
import json
from pathlib import Path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Summarize a saved channel title inventory.")
    parser.add_argument("inventory_json")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    path = Path(args.inventory_json)
    payload = json.loads(path.read_text(encoding="utf-8"))
    titles = payload["titles"]
    summary = payload["summary"]
    channel = payload["channel"]

    md_path = path.with_name(f"{path.stem}_analysis.md")
    lines = [
        f"# {channel} 제목 기반 콘텐츠 유형 분석",
        f"**분석 대상 제목 수:** {len(titles)}개",
        "",
        "## 유형 요약",
    ]
    for key, value in summary.items():
        lines.append(f"- {key}: {value}")
    lines.extend(["", "## 유형별 샘플"])
    for label in summary:
        samples = [item["title"] for item in titles if label in item["labels"]][:8]
        lines.append(f"### {label}")
        for idx, sample in enumerate(samples, start=1):
            lines.append(f"{idx}. {sample}")
        lines.append("")
    lines.extend(
        [
            "## 관찰",
            "- 삼프로TV는 단일 종목 추천 채널이라기보다 매크로/시황/인터뷰/섹터 콘텐츠가 혼합된 채널로 보인다.",
            "- `시장리뷰`, `전문가인터뷰`, `매크로` 비중이 높다.",
            "- 일부 제목은 직접 종목/산업 키워드를 포함하므로 기존 파이프라인으로도 일정 부분 분석이 가능하다.",
            "- 이후 확장은 실제 분석 결과에서 놓치는 패턴을 본 다음 제한적으로 설계하는 편이 맞다.",
        ]
    )
    md_path.write_text("\n".join(lines), encoding="utf-8")
    print(json.dumps({"analysis_md": str(md_path)}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
