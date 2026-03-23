from __future__ import annotations

import json
from collections import Counter
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import urlopen

from yt_dlp import YoutubeDL

from omx_brainstorm.fundamentals import FundamentalsFetcher
from omx_brainstorm.models import FundamentalSnapshot, MasterOpinion, TickerMention
from omx_brainstorm.reporting import render_master_line, render_fundamentals_lines
from omx_brainstorm.signal_gate import assess_video_signal

CHANNEL_URL = "https://www.youtube.com/channel/UCQW05vzztAlwV54WL3pjGBQ/videos"
OUTPUT_DIR = Path("output")
NOW = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

COMPANY_MAP = {
    "엔비디아": ("NVDA", "NVIDIA"),
    "nvidia": ("NVDA", "NVIDIA"),
    "삼성전자": ("005930.KS", "Samsung Electronics"),
    "삼성": ("005930.KS", "Samsung Electronics"),
    "sk하이닉스": ("000660.KS", "SK hynix"),
    "하이닉스": ("000660.KS", "SK hynix"),
    "마이크론": ("MU", "Micron"),
    "micron": ("MU", "Micron"),
    "브로드컴": ("AVGO", "Broadcom"),
    "broadcom": ("AVGO", "Broadcom"),
    "amd": ("AMD", "AMD"),
    "tsmc": ("TSM", "TSMC"),
    "asml": ("ASML", "ASML"),
    "한미반도체": ("042700.KS", "Hanmi Semiconductor"),
    "리노공업": ("058470.KQ", "Lino Industrial"),
    "이수페타시스": ("007660.KS", "ISU Petasys"),
    "원익ips": ("240810.KQ", "Wonik IPS"),
    "주성엔지니어링": ("036930.KQ", "Jusung Engineering"),
    "hpsp": ("403870.KQ", "HPSP"),
    "advantest": ("6857.T", "Advantest"),
    "coherent": ("COHR", "Coherent"),
    "marvell": ("MRVL", "Marvell"),
    "astera labs": ("ALAB", "Astera Labs"),
    "cisco": ("CSCO", "Cisco"),
    "arista": ("ANET", "Arista Networks"),
}


def fetch_recent_entries(limit: int = 5):
    opts = {"quiet": True, "extract_flat": True, "skip_download": True, "playlistend": limit}
    with YoutubeDL(opts) as ydl:
        info = ydl.extract_info(CHANNEL_URL, download=False)
    return info.get("entries") or []


def transcript_text(video_id: str) -> str:
    opts = {
        "quiet": True,
        "skip_download": True,
        "writeautomaticsub": True,
        "writesubtitles": True,
        "subtitleslangs": ["ko", "ko-orig", "en"],
        "subtitlesformat": "json3",
    }
    with YoutubeDL(opts) as ydl:
        info = ydl.extract_info(f"https://www.youtube.com/watch?v={video_id}", download=False)
    caps = info.get("subtitles") or {}
    auto = info.get("automatic_captions") or {}
    for lang in ("ko", "ko-orig", "en"):
        items = caps.get(lang) or auto.get(lang) or []
        for item in items:
            if item.get("ext") == "json3" and item.get("url"):
                with urlopen(item["url"]) as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
                events = payload.get("events", [])
                parts = []
                for event in events:
                    for seg in event.get("segs", []) or []:
                        txt = str(seg.get("utf8", "")).replace("\n", " ").strip()
                        if txt:
                            parts.append(txt)
                if parts:
                    return " ".join(parts)
    return ""


def extract_mentions(title: str, text: str) -> list[tuple[TickerMention, int]]:
    lower = f"{title} {text}".lower()
    counts: Counter[tuple[str, str]] = Counter()
    reasons: dict[tuple[str, str], list[str]] = {}
    for key, (ticker, company) in COMPANY_MAP.items():
        c = lower.count(key)
        if c > 0:
            counts[(ticker, company)] += c
            reasons.setdefault((ticker, company), []).append(key)
    mentions: list[tuple[TickerMention, int]] = []
    for (ticker, company), c in counts.most_common(4):
        mentions.append(
            (
                TickerMention(
                    ticker=ticker,
                    company_name=company,
                    confidence=min(0.99, 0.45 + c * 0.08),
                    reason=f"영상 제목/자막에서 {', '.join(sorted(set(reasons[(ticker, company)])))} 반복 언급",
                    evidence=[f"mention_count={c}"],
                ),
                c,
            )
        )
    return mentions


def safe_pct(value: float | None) -> float:
    return 0.0 if value is None else value * 100


def basic_assessment(snapshot: FundamentalSnapshot):
    score = 50.0
    notes = []
    if snapshot.revenue_growth is not None:
        rg = safe_pct(snapshot.revenue_growth)
        score += 8 if rg > 20 else 4 if rg > 5 else -4
        notes.append(f"매출성장률 {rg:.1f}%")
    if snapshot.operating_margin is not None:
        opm = safe_pct(snapshot.operating_margin)
        score += 8 if opm > 20 else 4 if opm > 10 else -4
        notes.append(f"영업이익률 {opm:.1f}%")
    if snapshot.return_on_equity is not None:
        roe = safe_pct(snapshot.return_on_equity)
        score += 6 if roe > 15 else 2 if roe > 8 else -3
        notes.append(f"ROE {roe:.1f}%")
    if snapshot.debt_to_equity is not None:
        de = snapshot.debt_to_equity
        score += 4 if de < 80 else 0 if de < 150 else -5
        notes.append(f"D/E {de:.1f}")
    if snapshot.forward_pe is not None:
        fpe = snapshot.forward_pe
        score += 2 if fpe < 25 else -2 if fpe > 40 else 0
        notes.append(f"Forward PE {fpe:.1f}")
    score = max(0.0, min(100.0, score))
    if score >= 72:
        verdict = "BUY"
        state = "기본 재무/수익성 지표가 전반적으로 양호한 상태"
    elif score >= 58:
        verdict = "WATCH"
        state = "기본 지표는 준수하지만 가격 또는 성장 지속성 확인이 필요한 상태"
    else:
        verdict = "REJECT"
        state = "기본 지표만으로는 적극적 진입 근거가 약한 상태"
    return score, verdict, state, " / ".join(notes[:5])


def master_opinions(snapshot: FundamentalSnapshot, mention_count: int, title: str):
    momentum = safe_pct(snapshot.fifty_two_week_change)
    rg = safe_pct(snapshot.revenue_growth)
    roe = safe_pct(snapshot.return_on_equity)
    opm = safe_pct(snapshot.operating_margin)
    de = snapshot.debt_to_equity or 0
    title_l = title.lower()
    cyc = any(k in title_l for k in ["cycle", "사이클", "memory", "메모리", "foundry", "파운드리", "gtc", "hbm"])

    dr_score = 55 + (10 if mention_count >= 5 else 4 if mention_count >= 2 else 0) + (8 if rg > 15 else 0) + (6 if cyc else 0) + (4 if momentum > 20 else 0)
    dr_score = max(0, min(100, dr_score))
    dr_verdict = "BUY" if dr_score >= 72 else "WATCH" if dr_score >= 58 else "REJECT"

    bf_score = 50 + (10 if roe > 15 else 0) + (8 if opm > 15 else 0) + (6 if de < 100 else -4) + (4 if (snapshot.forward_pe or 999) < 25 else -6 if (snapshot.forward_pe or 0) > 40 else 0)
    bf_score = max(0, min(100, bf_score))
    bf_verdict = "BUY" if bf_score >= 72 else "WATCH" if bf_score >= 58 else "REJECT"

    so_score = 52 + (10 if momentum > 25 else 4 if momentum > 0 else -6) + (8 if mention_count >= 4 else 4 if mention_count >= 2 else 0) + (6 if cyc else 0)
    so_score = max(0, min(100, so_score))
    so_verdict = "BUY" if so_score >= 72 else "WATCH" if so_score >= 58 else "REJECT"

    return [
        MasterOpinion("druckenmiller", dr_verdict, dr_score, 100, "수요 드라이버와 사이클 단서가 뚜렷해 보이지만 유동성/기대치 점검이 필요함"),
        MasterOpinion("buffett", bf_verdict, bf_score, 100, "사업 질과 수익성은 체크할 수 있으나 가격 매력과 안전마진은 별도 확인이 필요함"),
        MasterOpinion("soros", so_verdict, so_score, 100, "내러티브와 추세 지속 가능성은 있으나 반사성 꺾임에는 민감해야 함"),
    ]


def final_verdict(scores: list[float]) -> tuple[float, str]:
    total = sum(scores) / len(scores)
    if total >= 80:
        return total, "STRONG_BUY"
    if total >= 68:
        return total, "BUY"
    if total >= 55:
        return total, "WATCH"
    return total, "REJECT"


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    entries = fetch_recent_entries(5)
    ff = FundamentalsFetcher()
    results = []

    for entry in entries:
        video_id = entry["id"]
        title = entry["title"]
        text = transcript_text(video_id)
        signal = assess_video_signal(title, text)
        row = {
            "video_id": video_id,
            "title": title,
            "url": f"https://www.youtube.com/watch?v={video_id}",
            "signal_assessment": asdict(signal),
            "stocks": [],
        }
        if signal.should_analyze_stocks:
            for mention, mention_count in extract_mentions(title, text):
                snapshot = ff.fetch(mention)
                basic_score, basic_verdict, basic_state, basic_summary = basic_assessment(snapshot)
                masters = master_opinions(snapshot, mention_count, title)
                total_score, verdict = final_verdict([basic_score] + [m.score for m in masters])
                row["stocks"].append(
                    {
                        "ticker": mention.ticker,
                        "company_name": snapshot.company_name or mention.company_name,
                        "mention_count": mention_count,
                        "basic_state": basic_state,
                        "basic_signal_summary": basic_summary,
                        "basic_signal_verdict": basic_verdict,
                        "fundamentals": asdict(snapshot),
                        "master_opinions": [asdict(m) for m in masters],
                        "final_score": round(total_score, 1),
                        "final_verdict": verdict,
                        "invalidation_triggers": ["실적/가이던스 둔화", "메모리·AI CAPEX 약화", "멀티플 재조정"],
                    }
                )
        results.append(row)

    json_path = OUTPUT_DIR / f"itgod_recent5_results_{NOW}.json"
    txt_path = OUTPUT_DIR / f"itgod_recent5_results_{NOW}.txt"
    json_path.write_text(json.dumps({"channel_url": CHANNEL_URL, "generated_at": NOW, "videos": results}, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [f"IT의 신 이형수 최근 5개 영상 분석 결과 ({NOW})", f"채널: {CHANNEL_URL}", ""]
    for video in results:
        sig = video["signal_assessment"]
        lines.extend([
            f"영상: {video['title']}",
            f"URL: {video['url']}",
            f"Signal: {sig['video_signal_class']} ({sig['signal_score']:.1f}) | analyze={sig['should_analyze_stocks']}",
            f"Reason: {sig['reason']}",
        ])
        if not video["stocks"]:
            lines.extend(["종목 결과: 없음", ""])
            continue
        lines.append("종목 결과:")
        for stock in video["stocks"]:
            lines.extend([
                f"- {stock['ticker']} | {stock['company_name']} | 최종 {stock['final_verdict']} ({stock['final_score']})",
                f"  기본재무상태: {stock['basic_state']}",
                f"  기본지표요약: {stock['basic_signal_summary']}",
            ])
            snapshot = FundamentalSnapshot(**stock["fundamentals"])
            for fund_line in render_fundamentals_lines(snapshot)[:4]:
                lines.append(f"  {fund_line}")
            lines.append("  거장 한줄평:")
            for op in stock["master_opinions"]:
                lines.append(f"  {render_master_line(MasterOpinion(**op))}")
        lines.append("")
    txt_path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")
    print(json_path)
    print(txt_path)


if __name__ == "__main__":
    main()
