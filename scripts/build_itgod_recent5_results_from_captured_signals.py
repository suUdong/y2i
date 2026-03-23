from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

from omx_brainstorm.backtest import BacktestEngine, BacktestIdea
from omx_brainstorm.fundamentals import FundamentalsFetcher
from omx_brainstorm.master_engine import build_master_opinions, master_variance_score, validate_cross_stock_master_quality
from omx_brainstorm.models import FundamentalSnapshot, MasterOpinion, TickerMention
from omx_brainstorm.reporting import render_fundamentals_lines, render_master_line
from omx_brainstorm.research import RANKING_FORMULA, build_cross_video_ranking, render_cross_video_ranking_text
from omx_brainstorm.signal_features import stock_signal_strength
from omx_brainstorm.signal_gate import assess_video_signal
from omx_brainstorm.transcript_cache import TranscriptCache

NOW = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
OUT = Path('output')
CHANNEL_URL = 'https://www.youtube.com/channel/UCQW05vzztAlwV54WL3pjGBQ/videos'

VIDEOS = [
    {
        'video_id': 't3OfOPLy8cM',
        'title': '[몰아보기] AI 네트워크와 반도체의 콜라보, 4월 투자 전략의 핵심!!',
        'published_at': '20260321',
        'description': '[아바타 몰아보기] AI 네트워크와 반도체의 콜라보, 4월 투자 전략의 핵심!!',
        'tags': ['이형수', 'IT신이형수', '반도체종목', '반도체관련주', '초보투자', '주식초보', '재테크', '테크 주', '투자 분석'],
        'captured_mentions': [
            ('AVGO', 'Broadcom', 1),
            ('NVDA', 'NVIDIA', 1),
            ('MRVL', 'Marvell Technology', 1),
            ('267260.KS', 'HD Hyundai Electric', 2),
            ('298040.KS', 'Hyosung Heavy Industries', 2),
            ('103590.KS', 'Iljin Electric', 1),
        ],
        'stock_evidence': {
            'AVGO': ['엔비디아뿐 아니라 브로드컴, 마벨 등의 실적 발표에서 네트워크 사업 성장이 두드러지고'],
            'NVDA': ['M비디아 A이라는 칩', 'A100보다 훨씬 뛰어난 칩은 이제 B200, B300'],
            'MRVL': ['엔비디아뿐 아니라 브로드컴, 마벨 등의 실적 발표에서 네트워크 사업 성장이 두드러지고'],
            '267260.KS': ['HD 현대 엘렉트릭하고 효성 중공업이 있거든요'],
            '298040.KS': ['HD 현대 엘렉트릭하고 효성 중공업이 있거든요'],
            '103590.KS': ['일진 전기 뭐 이런 회사들이 좀 최근에 많이 주목을 받고 있고요'],
        },
    },
    {
        'video_id': 'O7-s_B-nRg8',
        'title': '엔비디아가 제시한 차세대 메모리 로드맵, 소부장 투자 전략은?',
        'published_at': '20260320',
        'description': '[제506회] 엔비디아가 제시한 차세대 메모리 로드맵, 소부장 투자 전략은?',
        'tags': ['이형수', 'IT신이형수', '반도체종목', '반도체관련주', '초보투자', '주식초보', '재테크', '테크 주', '투자 분석', '엔비디아', '삼성전자', '리노공업'],
        'captured_mentions': [('005930.KS', 'Samsung Electronics', 7), ('MU', 'Micron', 7), ('000660.KS', 'SK hynix', 1), ('NVDA', 'NVIDIA', 1), ('058470.KQ', 'LEENO Industrial', 1)],
        'stock_evidence': {
            '005930.KS': ['#엔비디아 #삼성전자', '고성능 스토리지 필수! 숨은 소부장 찾기'],
            'MU': ['낸드플래시 판도가 바뀐다', '고성능 스토리지 필수! 숨은 소부장 찾기'],
            '000660.KS': ['엔비디아가 제시한 차세대 메모리 로드맵'],
            'NVDA': ['엔비디아가 제시한 차세대 메모리 로드맵'],
            '058470.KQ': ['#리노공업', '소부장 투자 전략은?'],
        },
    },
    {
        'video_id': 'i1iVVhYKGXc',
        'title': '[아바타] 세대간 희비를 엇가르는 엄청난 사이클이 온다!!',
        'published_at': '20260319',
        'description': '[아바타 006회] 세대간 희비를 엇가르는 엄청난 사이클이 온다!!',
        'tags': ['이형수', 'IT신이형수', '반도체종목', '반도체관련주', '초보투자', '주식초보', '재테크', '테크 주', '투자 분석', 'AI인프라', 'AI반도체', 'AI버블'],
        'captured_mentions': [],
        'stock_evidence': {},
    },
    {
        'video_id': 'zvzflBi7vds',
        'title': '그록3 대박 난 삼성 파운드리, 수혜주 싹 정리!!',
        'published_at': '20260318',
        'description': '[제505회] 그록3 대박 난 삼성 파운드리, 수혜주 싹 정리!!',
        'tags': ['이형수', 'IT신이형수', '반도체종목', '반도체관련주', '초보투자', '주식초보', '재테크', '테크 주', '투자 분석', '삼성전자', '엔비디아', '삼성파운드리'],
        'captured_mentions': [('005930.KS', 'Samsung Electronics', 15), ('NVDA', 'NVIDIA', 5), ('MU', 'Micron', 4), ('000660.KS', 'SK hynix', 1)],
        'stock_evidence': {
            '005930.KS': ['그록3 대박 난 삼성 파운드리', '#삼성전자 #삼성파운드리'],
            'NVDA': ['GTC와 마이크론 실적, 테크주 반등의 신호탄 쏘나', '#엔비디아'],
            'MU': ['GTC와 마이크론 실적, 테크주 반등의 신호탄 쏘나'],
            '000660.KS': ['그록3 대박 난 삼성 파운드리, 수혜주 싹 정리!!'],
        },
    },
    {
        'video_id': 'am_yV0E-yZ0',
        'title': '[아바타] 이게 없으면 GPU도 못 돌려, AI 진화에 필요한 핵심 전력 기술은?',
        'published_at': '20260317',
        'description': '[아바타 005회] 이게 없으면 GPU도 못 돌려, AI 진화에 필요한 핵심 전력 기술은?',
        'tags': ['이형수', 'IT신이형수', '반도체종목', '반도체관련주', '초보투자', '주식초보', '재테크', '테크 주', '투자 분석', '전력인프라', 'hd현대일렉트릭', '효성중공업'],
        'captured_mentions': [('267260.KS', 'HD Hyundai Electric', 2), ('298040.KS', 'Hyosung Heavy Industries', 2)],
        'stock_evidence': {
            '267260.KS': ['#전력인프라 #hd현대일렉트릭', 'AI 진화에 필요한 핵심 전력 기술'],
            '298040.KS': ['#전력인프라 #효성중공업', 'GPU도 못 돌려, AI 진화에 필요한 핵심 전력 기술'],
        },
    },
]


def safe_pct(v):
    return 0.0 if v is None else v * 100


def basic(snapshot: FundamentalSnapshot):
    score = 50.0
    if snapshot.revenue_growth is not None:
        score += 8 if safe_pct(snapshot.revenue_growth) > 20 else 4 if safe_pct(snapshot.revenue_growth) > 5 else -4
    if snapshot.operating_margin is not None:
        score += 8 if safe_pct(snapshot.operating_margin) > 20 else 4 if safe_pct(snapshot.operating_margin) > 10 else -4
    if snapshot.return_on_equity is not None:
        score += 6 if safe_pct(snapshot.return_on_equity) > 15 else 2 if safe_pct(snapshot.return_on_equity) > 8 else -3
    if snapshot.debt_to_equity is not None:
        score += 4 if snapshot.debt_to_equity < 80 else 0 if snapshot.debt_to_equity < 150 else -5
    if snapshot.forward_pe is not None:
        score += 2 if snapshot.forward_pe < 25 else -2 if snapshot.forward_pe > 40 else 0
    score = max(0.0, min(100.0, score))
    verdict = 'BUY' if score >= 72 else 'WATCH' if score >= 58 else 'REJECT'
    summary = f"매출성장률 {safe_pct(snapshot.revenue_growth):.1f}% / 영업이익률 {safe_pct(snapshot.operating_margin):.1f}% / ROE {safe_pct(snapshot.return_on_equity):.1f}% / Forward PE {snapshot.forward_pe if snapshot.forward_pe is not None else '-'}"
    state = '기본 재무/수익성 지표가 전반적으로 양호한 상태' if verdict == 'BUY' else '기본 지표는 준수하지만 추가 확인이 필요한 상태' if verdict == 'WATCH' else '기본 지표만으로는 적극적 진입 근거가 약한 상태'
    return score, verdict, state, summary


def final(scores):
    total = sum(scores) / len(scores)
    verdict = 'STRONG_BUY' if total >= 80 else 'BUY' if total >= 68 else 'WATCH' if total >= 55 else 'REJECT'
    return round(total, 1), verdict


def ranking_validation(ranking, end_date: str):
    if not ranking:
        return {}
    engine = BacktestEngine()
    ideas = [
        BacktestIdea(
            ticker=item.ticker,
            company_name=item.company_name,
            score=item.aggregate_score,
            signal_date=item.first_signal_at,
        )
        for item in ranking
    ]
    start_date = min(item.first_signal_at for item in ranking if item.first_signal_at)
    summary = {}
    for top_n in [1, 3, len(ideas)]:
        label = f'top_{top_n}'
        report = engine.run_buy_and_hold(ideas=ideas, start_date=start_date, end_date=end_date, top_n=top_n, initial_capital=10000.0)
        summary[label] = report.to_dict()
    return summary


def main():
    OUT.mkdir(exist_ok=True)
    cache = TranscriptCache()
    cache.warm_from_output_dir(OUT)
    ff = FundamentalsFetcher()
    rows = []
    for video in VIDEOS:
        cached = cache.load(video['video_id']) or {}
        cached_transcript = cached.get('transcript_text', '')
        archived_evidence = {
            item['ticker']: list(item.get('evidence', []))
            for item in cached.get('ticker_mentions', [])
        } if cached else {}
        assessment = assess_video_signal(
            video['title'],
            cached_transcript,
            description=video.get('description', ''),
            tags=video.get('tags', []),
        )
        row = {
            'video_id': video['video_id'],
            'title': video['title'],
            'published_at': video.get('published_at'),
            'description': video.get('description', ''),
            'tags': list(video.get('tags', [])),
            'signal_score': assessment.signal_score,
            'video_signal_class': assessment.video_signal_class,
            'should_analyze_stocks': assessment.should_analyze_stocks,
            'reason': assessment.reason,
            'signal_metrics': assessment.metrics,
        }
        stocks = []
        if not assessment.should_analyze_stocks:
            row['stocks'] = stocks
            rows.append(row)
            continue
        for ticker, company, mention_count in video['captured_mentions']:
            snap = ff.fetch(TickerMention(ticker=ticker, company_name=company))
            bscore, bverdict, bstate, bsummary = basic(snap)
            evidence_snippets = archived_evidence.get(ticker) or video.get('stock_evidence', {}).get(ticker, [video['title']])
            evidence_source = cached.get('source', 'metadata_fallback') if archived_evidence.get(ticker) else 'metadata_fallback'
            mops = build_master_opinions(
                ticker=ticker,
                company_name=snap.company_name or company,
                snapshot=snap,
                mention_count=mention_count,
                video_title=video['title'],
                video_signal_score=assessment.signal_score,
                evidence_snippets=evidence_snippets,
            )
            total_score, verdict = final([bscore] + [m.score for m in mops])
            stocks.append({
                'ticker': ticker,
                'company_name': snap.company_name or company,
                'mention_count': mention_count,
                'signal_timestamp': row['published_at'],
                'signal_strength_score': stock_signal_strength(
                    ticker=ticker,
                    company_name=snap.company_name or company,
                    video_signal_score=assessment.signal_score,
                    mention_count=mention_count,
                    master_variance=master_variance_score(mops),
                    evidence_snippets=evidence_snippets,
                    evidence_source=evidence_source,
                ),
                'evidence_source': evidence_source,
                'basic_state': bstate,
                'basic_signal_summary': bsummary,
                'basic_signal_verdict': bverdict,
                'fundamentals': asdict(snap),
                'evidence_snippets': evidence_snippets,
                'master_opinions': [asdict(m) for m in mops],
                'final_score': total_score,
                'final_verdict': verdict,
                'invalidation_triggers': ['실적/가이던스 둔화', '메모리·AI CAPEX 약화', '멀티플 재조정'],
            })
        row['stocks'] = stocks
        rows.append(row)

    validate_cross_stock_master_quality([stock for row in rows for stock in row['stocks']])
    ranking = build_cross_video_ranking(rows)
    backtest_seed = [
        BacktestIdea(ticker=item.ticker, company_name=item.company_name, score=item.aggregate_score, signal_date=item.first_signal_at)
        for item in ranking
    ]
    validation = ranking_validation(ranking, datetime.now(timezone.utc).date().isoformat())

    payload = {
        'channel_url': CHANNEL_URL,
        'generated_at': NOW,
        'method_note': 'YouTube transcript direct refetch hit rate limits during run; this result uses earlier same-session captured transcript-derived signal/mention counts plus current fundamentals from yfinance.',
        'cross_video_ranking_formula': RANKING_FORMULA,
        'cross_video_ranking': [item.to_dict() for item in ranking],
        'ranking_validation': validation,
        'backtest_engine_seed': {
            'strategy': 'equal_weight_buy_and_hold',
            'ready_ideas': [asdict(item) for item in backtest_seed],
            'engine_module': 'omx_brainstorm.backtest.BacktestEngine',
        },
        'videos': rows,
    }
    json_path = OUT / f'itgod_recent5_results_{NOW}.json'
    txt_path = OUT / f'itgod_recent5_results_{NOW}.txt'
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')

    lines = [
        f'IT의 신 이형수 최근 5개 영상 결과 ({NOW})',
        f'채널: {CHANNEL_URL}',
        '주의: YouTube 자막 재요청이 차단된 영상은 title/description/tags 기반 fallback signal gate를 사용했고, 랭킹 산식에서는 미래 시점 fundamentals를 제외했다.',
        '',
        render_cross_video_ranking_text(ranking),
        '',
        '[랭킹 검증]',
        f"종료일: {datetime.now(timezone.utc).date().isoformat()}",
        f"top_1 수익률: {validation.get('top_1', {}).get('portfolio_return_pct', 0.0)}%",
        f"top_3 수익률: {validation.get('top_3', {}).get('portfolio_return_pct', 0.0)}%",
        f"top_{len(ranking)} 수익률: {validation.get(f'top_{len(ranking)}', {}).get('portfolio_return_pct', 0.0)}%",
        '',
    ]
    for video in rows:
        lines += [
            f"영상: {video['title']}",
            f"URL: https://www.youtube.com/watch?v={video['video_id']}",
            f"Published At: {video['published_at']}",
            f"Signal: {video['video_signal_class']} ({video['signal_score']}) | analyze={video['should_analyze_stocks']}",
            f"Reason: {video['reason']}",
        ]
        if not video['stocks']:
            lines += ['종목 결과: 없음', '']
            continue
        lines.append('종목 결과:')
        for stock in video['stocks']:
            checked_at = stock['fundamentals'].get('checked_at') or '-'
            lines += [
                f"- {stock['ticker']} | {stock['company_name']} | mention_count={stock['mention_count']} | signal_strength={stock['signal_strength_score']} | checked_at={checked_at} | 최종 {stock['final_verdict']} ({stock['final_score']})",
                f"  기본재무상태: {stock['basic_state']}",
                f"  기본지표요약: {stock['basic_signal_summary']}",
                f"  evidence_source: {stock['evidence_source']}",
                f"  evidence: {' | '.join(stock['evidence_snippets'])}",
            ]
            snap = FundamentalSnapshot(**stock['fundamentals'])
            for line in render_fundamentals_lines(snap)[:4]:
                lines.append(f'  {line}')
            lines.append('  거장 한줄평:')
            for op in stock['master_opinions']:
                lines.append(f"  {render_master_line(MasterOpinion(**op))}")
        lines.append('')
    txt_path.write_text('\n'.join(lines), encoding='utf-8')
    print(json_path)
    print(txt_path)

if __name__ == '__main__':
    main()
