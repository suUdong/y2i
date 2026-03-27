# OMX Brainstorm

경제/매크로 유튜브 채널을 수집하고, 자막과 메타데이터에서 종목 시그널을 추출한 뒤, 기본 재무·거장 평가·채널 비교·백테스트까지 연결하는 파이프라인이다.

## 구성
- 채널/영상 수집: `yt-dlp`, YouTube RSS
- 자막 수집: `youtube-transcript-api` + `.omx/cache/transcripts/` 영속 캐시
- 시그널 추출:
  - 직접 종목 언급
  - 매크로 → 섹터 → 종목 간접 연결
- 분석:
  - 기본 재무 스냅샷 (`yfinance`)
  - 드러큰밀러 / 버핏 / 소로스 one-line opinions
- 검증:
  - 채널별 cross-video ranking
  - signal-date aware backtest
  - 채널 간 비교 리포트

## 설치
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e . pytest
```

## 설정
기본 설정 파일은 [config.toml](/home/wdsr88/workspace/y2i/config.toml)이다. 샘플은 [config.example.toml](/home/wdsr88/workspace/y2i/config.example.toml), 환경변수 샘플은 [.env.example](/home/wdsr88/workspace/y2i/.env.example)에 있다.

우선순위:
1. 환경변수
2. `config.toml`
3. 코드 기본값

주요 설정:
- `[app]`: provider, output 경로, 레지스트리 경로, paper trading 모드
- `[[channels]]`: 채널 slug/display_name/url/enabled
- `[strategy]`: window days, scan depth, top N, paper trade capital
- `[notifications]`: Telegram bot/chat
- `[schedule]`: daily time, timezone, enabled, poll interval, poll video limit, scheduler state path
- `[logging]`: JSON logging 여부, 로그 디렉터리, 보관 일수

## CLI
```bash
omx-brainstorm register-channel "https://www.youtube.com/@lucky_tv/videos"
omx-brainstorm list-channels
omx-brainstorm analyze-video "https://www.youtube.com/watch?v=..."
omx-brainstorm analyze-channel "https://www.youtube.com/@lucky_tv/videos" --limit 5
omx-brainstorm backtest-ranked output/itgod_30d_*.json --start-date 2026-03-01 --end-date 2026-03-23
omx-brainstorm backtest-artifact output/itgod_30d_20260323T005353Z.json
omx-brainstorm run-comparison --config config.toml
omx-brainstorm run-scheduler --config config.toml --once
omx-brainstorm run-healthcheck
```

`--verbose`를 붙이면 로깅이 더 자세해진다.

## 채널 추가
1. `omx-brainstorm register-channel <url>`
2. `config.toml`의 `[[channels]]`에 채널 추가
3. `omx-brainstorm run-comparison --config config.toml` 실행

## 스케줄러
단발 실행:
```bash
python scripts/run_scheduler.py --config config.toml --once
```

루프 실행:
```bash
.venv/bin/python scripts/run_scheduler.py --config config.toml
```

스케줄러 동작:
- 10분 기본 폴링으로 각 채널의 최신 업로드를 확인한다.
- 새 영상이 감지되면 즉시 30일 비교 파이프라인을 다시 실행한다.
- `daily_time`을 지나면 하루 한 번 백스톱 비교 실행도 보장한다.
- 상태는 `.omx/state/scheduler_state.json`, 헬스 정보는 `.omx/state/scheduler_health.json`에 남는다.

성공/실패 요약은 Telegram/Discord 환경변수가 있으면 알림으로 전송된다.

크론 watchdog 예시:
```bash
crontab -l
```

```cron
@reboot /home/wdsr88/workspace/y2i/scripts/ensure_scheduler_daemon.sh
*/10 * * * * /home/wdsr88/workspace/y2i/scripts/ensure_scheduler_daemon.sh
```

## 로깅 / 헬스체크
- 기본 로그는 JSON 형식으로 `.omx/logs/omx-app.log`에 기록된다.
- 로그는 날짜 기준으로 회전되며 기본 7일 보관이다.
- 스케줄러 상태 확인:
```bash
python scripts/run_healthcheck.py
```

## Docker / paper trading
비교 실행:
```bash
docker compose run --rm omx-paper-trading
```

스케줄러 실행:
```bash
docker compose up -d omx-scheduler
```

필수 파일:
- `config.toml`
- `.env`
- `channels.json`

결과물과 캐시는 호스트에 유지된다:
- `./output`
- `./.omx`

## 백테스트 자동화
저장된 채널 artifact에서 자동 백테스트:
```bash
python scripts/run_backtest_report.py output/itgod_30d_20260323T005353Z.json
```

이 모듈은 저장된 `cross_video_ranking`을 읽고, `first_signal_at` 이후 수익률을 계산한다.

## 아키텍처
- [youtube.py](/home/wdsr88/workspace/y2i/src/omx_brainstorm/youtube.py): 채널/영상 해상도, 자막 fetch
- [transcript_cache.py](/home/wdsr88/workspace/y2i/src/omx_brainstorm/transcript_cache.py): 자막 캐시
- [signal_gate.py](/home/wdsr88/workspace/y2i/src/omx_brainstorm/signal_gate.py): ACTIONABLE / SECTOR_ONLY / LOW_SIGNAL / NOISE
- [macro_signals.py](/home/wdsr88/workspace/y2i/src/omx_brainstorm/macro_signals.py): 매크로 → 섹터 → 종목
- [extractors.py](/home/wdsr88/workspace/y2i/src/omx_brainstorm/extractors.py): 직접/간접 종목 병합
- [master_engine.py](/home/wdsr88/workspace/y2i/src/omx_brainstorm/master_engine.py): 거장 one-line opinions
- [research.py](/home/wdsr88/workspace/y2i/src/omx_brainstorm/research.py): ranking
- [backtest.py](/home/wdsr88/workspace/y2i/src/omx_brainstorm/backtest.py): signal-date aware backtest
- [backtest_automation.py](/home/wdsr88/workspace/y2i/src/omx_brainstorm/backtest_automation.py): artifact-based backtest automation
- [cli.py](/home/wdsr88/workspace/y2i/src/omx_brainstorm/cli.py): main CLI surface
- [run_channel_30d_comparison.py](/home/wdsr88/workspace/y2i/scripts/run_channel_30d_comparison.py): 운영용 비교 배치

## 테스트
```bash
pytest
```
