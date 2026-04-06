# Y2I: 애널리스트 컨센서스 + 쉬운 판단 근거 + 유연한 전문가 엔진

**날짜:** 2026-04-06
**상태:** 승인됨

## 목표

대시보드에서 종목을 볼 때 "왜 이 점수인지", "전문가들은 뭐라 하는지"를 누구나 이해할 수 있게 보여준다.

1. Yahoo Finance 애널리스트 컨센서스 시그널을 수집하여 팩트 기반 판단 근거 확보
2. 재무지표 + 컨센서스를 쉬운 자연어로 변환 (하이브리드: rule-based + LLM)
3. 전문가(master) 엔진을 설정 파일 기반으로 리팩토링하여 코드 수정 없이 전문가 추가/삭제 가능

## 1. FundamentalSnapshot 확장

### 추가 필드 (`models.py`)

```python
# 애널리스트 컨센서스
recommendation_key: str | None = None        # "strong_buy", "buy", "hold", "sell", "strong_sell"
recommendation_mean: float | None = None      # 1.0(Strong Buy) ~ 5.0(Strong Sell)
analyst_count: int | None = None
target_mean_price: float | None = None
target_median_price: float | None = None
analyst_strong_buy: int = 0
analyst_buy: int = 0
analyst_hold: int = 0
analyst_sell: int = 0
analyst_strong_sell: int = 0

# 업종 분류 (향후 업종 평균 비교 확장용)
sector: str | None = None
industry: str | None = None
```

### 수집 (`fundamentals.py` `_fetch_live`)

```python
# info에서
info.get("recommendationKey")      # "buy", "hold", etc.
info.get("recommendationMean")     # 1.0 ~ 5.0
info.get("numberOfAnalystOpinions")
info.get("targetMeanPrice")
info.get("targetMedianPrice")
info.get("sector")
info.get("industry")

# recommendations에서 (현재 월)
rec = ticker.recommendations
if rec is not None and not rec.empty:
    current = rec.iloc[0]  # period "0m"
    strong_buy = current.get("strongBuy", 0)
    buy = current.get("buy", 0)
    hold = current.get("hold", 0)
    sell = current.get("sell", 0)
    strong_sell = current.get("strongSell", 0)
```

## 2. 쉬운 언어 요약 (하이브리드)

### 새 모듈: `plain_summary.py`

**Rule-based 부분** — 재무지표 + 애널리스트 컨센서스를 절대 기준으로 자연어 변환.

#### 절대 기준 테이블

| 지표 | 구간 | 설명 |
|------|------|------|
| 영업이익률 | >= 20% | "돈을 아주 잘 버는 회사" |
| 영업이익률 | 10~20% | "돈을 꽤 잘 버는 회사" |
| 영업이익률 | 5~10% | "돈을 보통 수준으로 버는 회사" |
| 영업이익률 | < 5% | "돈 벌기가 어려운 회사" |
| ROE | >= 15% | "투자한 돈 대비 수익이 좋음" |
| ROE | 8~15% | "투자 대비 수익이 보통" |
| ROE | < 8% | "투자 대비 수익이 낮음" |
| D/E | < 50 | "빚이 아주 적음" |
| D/E | 50~100 | "빚은 적당한 편" |
| D/E | 100~200 | "빚이 좀 있는 편" |
| D/E | > 200 | "빚이 많은 편" |
| Forward PE | < 15 | "주가가 저렴한 편" |
| Forward PE | 15~25 | "주가가 적당한 편" |
| Forward PE | 25~40 | "주가가 비싼 편" |
| Forward PE | > 40 | "주가가 아주 비쌈" |
| 매출성장률 | >= 30% | "매출이 빠르게 성장 중" |
| 매출성장률 | 10~30% | "매출이 꾸준히 성장 중" |
| 매출성장률 | 0~10% | "매출이 조금씩 성장 중" |
| 매출성장률 | < 0% | "매출이 줄어들고 있음" |

#### 애널리스트 컨센서스 자연어

```
"37명 전문가 중 35명이 '사라'고 함 (목표가 ₩250,000)"
"애널리스트 의견이 없어요" (analyst_count == 0 or None)
```

비율 기반:
- buy+strong_buy >= 70% → "대부분의 전문가가 '사라'고 함"
- buy+strong_buy >= 50% → "전문가 절반 이상이 '사라'고 함"
- hold >= 50% → "전문가들이 '지켜보자'는 의견"
- sell+strong_sell >= 30% → "주의: 전문가들 사이에서 '팔아라' 의견이 꽤 있음"

#### 함수 인터페이스

```python
def build_plain_summary(snapshot: FundamentalSnapshot) -> str:
    """재무지표 + 애널리스트 컨센서스를 쉬운 한국어 요약으로 변환."""

def build_analyst_summary(snapshot: FundamentalSnapshot) -> str:
    """애널리스트 컨센서스만 자연어로 변환."""
```

### LLM 부분 — `analysis.py` 프롬프트 확장

`StockAnalysis`에 필드 추가:
```python
video_context_summary: str = ""  # LLM이 트랜스크립트에서 해당 종목 언급을 1~2문장 요약
```

프롬프트 출력 스키마에 `video_context_summary` 추가:
- "이 영상에서 해당 종목에 대해 언급한 핵심 내용을 트랜스크립트 원문을 인용하여 1~2문장으로 요약하라"
- 팩트 인용 제약 유지 (환각 방지)

### 최종 출력 합성

`StockAnalysis`에 추가:
```python
plain_summary: str = ""           # rule-based 재무+컨센서스 요약
video_context_summary: str = ""   # LLM 영상 맥락 요약
```

파이프라인 순서:
1. `FundamentalsFetcher.fetch()` → 재무 + 컨센서스 수집
2. `build_plain_summary(snapshot)` → rule-based 요약 생성
3. LLM 분석 → `video_context_summary` 생성
4. 둘 다 `StockAnalysis`에 저장

## 3. 설정 기반 전문가 엔진

### 설정 파일: `config/masters.toml`

```toml
[[masters]]
name = "druckenmiller"
display = "드러큰밀러"
focus = "유동성, 18~24개월 선행 드라이버"
key_metrics = ["revenue_growth", "forward_pe", "video_signal_score", "mention_count"]
base_score = 56

[masters.weights]
mention = 2.2
mention_cap = 12
revenue = 0.12
revenue_cap = 10
signal = 0.08
signal_cap = 8
theme = 6.0
valuation_penalty_threshold = 35
valuation_penalty_rate = 0.25

[masters.risks]
items = [
  "수요 드라이버 약화 또는 capex 피크아웃",
  "정책/유동성 변화로 멀티플 압축 위험"
]

[masters.templates]
one_liner = "{ticker}는 매출성장률 {revenue:.1f}%와 '{evidence}'로 드라이버는 강하지만, PER {forward_pe:.1f}배라 유동성 점검이 먼저다."
rationale = [
  "영상 신호 {video_signal_score:.1f}와 언급 {mention_count}회가 수급 지속을 지지",
  "매출성장률 {revenue:.1f}%와 영업이익률 {margin:.1f}%는 이익 레버리지 유지 중"
]

[[masters]]
name = "buffett"
display = "버핏"
focus = "사업 품질, 자본효율, 밸류에이션"
key_metrics = ["operating_margin", "return_on_equity", "forward_pe", "debt_to_equity"]
base_score = 48

[masters.weights]
margin = 0.18
margin_cap = 12
roe = 0.10
roe_cap = 10
leverage_low_threshold = 60
leverage_low_bonus = 6
leverage_mid_threshold = 120
leverage_mid_bonus = 2
leverage_high_penalty = -5
valuation_low_threshold = 22
valuation_low_bonus = 6
valuation_mid_threshold = 30
valuation_mid_bonus = 1
valuation_high_penalty = -6

[masters.risks]
items = [
  "밸류에이션이 장기 복리 수익률을 잠식할 위험",
  "산업 사이클과 고객 집중이 예측 가능성을 낮출 위험"
]

[masters.templates]
one_liner = "{ticker}는 영업이익률 {margin:.1f}%와 ROE {roe:.1f}%로 사업 질은 좋지만, 부채비율 {leverage:.1f}와 밸류에이션을 감안하면 안전마진이 충분하다고 보긴 어렵다."
rationale = [
  "ROE {roe:.1f}%와 영업이익률 {margin:.1f}%는 자본효율과 가격결정력을 뒷받침",
  "PER {forward_pe:.1f}배와 부채비율 {leverage:.1f}는 보수적 진입 여부를 가른다"
]

[[masters]]
name = "soros"
display = "소로스"
focus = "서사/반사성, 추세 지속, 레짐 타이밍"
key_metrics = ["fifty_two_week_change", "video_signal_score", "mention_count"]
base_score = 50

[masters.weights]
momentum = 0.10
momentum_cap = 10
signal = 0.10
signal_cap = 9
mention = 1.4
mention_cap = 8
theme = 4.0
valuation_penalty_threshold = 40
valuation_penalty_rate = 0.15

[masters.risks]
items = [
  "서사 약화 시 추세 반전이 급격할 수 있음",
  "포지셔닝 과열로 작은 악재에도 변동성이 커질 수 있음"
]

[masters.templates]
one_liner = "{ticker}는 52주 수익률 {momentum:.1f}%와 '{evidence}'가 맞물려 서사가 자기강화 구간이지만, 신호 점수 {video_signal_score:.1f}가 높을수록 반사성 꺾임도 빨라질 수 있다."
rationale = [
  "영상 제목과 근거는 현재 테마가 네트워크/메모리/전력 병목 해소 서사 위에 있음을 보여준다",
  "언급 {mention_count}회와 52주 변화율 {momentum:.1f}%는 추세 추종 자금이 붙기 쉬운 조건"
]
```

### `master_engine.py` 리팩토링

```python
@dataclass
class MasterConfig:
    name: str
    display: str
    focus: str
    key_metrics: list[str]
    base_score: float
    weights: dict[str, float]
    risks: list[str]
    templates: dict[str, str | list[str]]

def load_masters(path: Path | None = None) -> list[MasterConfig]:
    """TOML에서 전문가 목록 로드. path가 None이면 기본 경로."""

def build_opinion(config: MasterConfig, metrics: dict[str, float]) -> MasterOpinion:
    """공통 점수 엔진: base_score + sum(metric * weight, cap) - penalties."""

def format_template(template: str, metrics: dict[str, Any]) -> str:
    """템플릿 변수를 실제 값으로 치환."""
```

**공통 점수 공식:**
```
score = base_score
for metric_name, weight in weights.items():
    if metric_name ends with "_cap":
        continue  # cap은 별도 처리
    if metric_name ends with "_threshold" or "_rate" or "_bonus" or "_penalty":
        continue  # 특수 파라미터
    value = metrics.get(metric_name, 0)
    cap = weights.get(f"{metric_name}_cap")
    component = value * weight
    if cap is not None:
        component = min(component, cap)
    score += component

# valuation penalty (있으면)
threshold = weights.get("valuation_penalty_threshold")
rate = weights.get("valuation_penalty_rate")
if threshold and rate:
    score -= max(forward_pe - threshold, 0) * rate

# leverage tiers (있으면)
# ... 구간별 bonus/penalty 적용

score = clamp(0, 100, score)
```

**하위 호환:** 기존 `build_master_opinions()` 함수 시그니처 유지, 내부만 `load_masters()` + `build_opinion()` 호출로 교체.

### 새 전문가 추가 예시

`config/masters.toml`에 추가만 하면 됨:
```toml
[[masters]]
name = "peter_lynch"
display = "피터 린치"
focus = "합리적 가격의 성장주 (GARP)"
key_metrics = ["earnings_growth", "forward_pe", "revenue_growth"]
base_score = 50
[masters.weights]
earnings = 0.15
earnings_cap = 12
revenue = 0.10
revenue_cap = 8
peg_penalty_threshold = 1.5
peg_penalty_rate = 8.0
[masters.templates]
one_liner = "{ticker}는 이익성장률 {earnings_growth:.1f}%에 PER {forward_pe:.1f}배로 GARP 관점에서 {verdict_kr}."
```

코드 변경 없이 즉시 반영.

## 4. 대시보드 표시

### 랭킹 탭 expander 구조

```
📊 삼성전자 (005930.KS) — 78.5점 · 매수
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

💡 한눈에 보기
  돈을 아주 잘 버는 회사 (영업이익률 21.3%)
  빚은 적당한 편 (부채비율 76.8%)
  주가가 적당한 편 (PER 18.5배)
  매출이 꾸준히 성장 중 (매출성장률 15.2%)
  37명 전문가 중 35명이 "사라"고 함 (목표가 ₩250,000)

🧠 전문가 의견
  드러큰밀러 (72.1/100): "매출성장률 15.2%와 'HBM 수요'로..."
  버핏 (65.0/100): "영업이익률은 좋지만..."
  소로스 (68.3/100): "서사 자기강화 구간..."

📺 영상에서 이 종목은
  🎬 [IT의 신] "반도체 핵심주 분석" (BUY, 67점)
    → "HBM 수요 급증으로 2026년까지 매출 3배 성장 전망"
  🎬 [미키피디아] "투자 패권 지도" (WATCH, 59점)
    → "장기 성장주이나 단기 밸류에이션 부담"

📊 점수 구성
  기본 63.6 + 합의 +12.5 + 품질 +3.2 - 감점 0 = 78.5
```

### 데이터 소스 표시

각 항목 옆에 데이터 출처를 작게 표시하여 환각 여부 판단 가능:
- `(Yahoo Finance)` — 재무지표, 애널리스트 컨센서스
- `(영상 인용)` — LLM이 트랜스크립트에서 추출한 요약
- `(설정: druckenmiller)` — 전문가 의견

## 5. 영향 받는 파일

| 파일 | 변경 내용 |
|------|----------|
| `src/omx_brainstorm/models.py` | FundamentalSnapshot 필드 추가, StockAnalysis 필드 추가 |
| `src/omx_brainstorm/fundamentals.py` | `_fetch_live`에서 recommendation/sector 수집 |
| `src/omx_brainstorm/plain_summary.py` | **신규** — rule-based 자연어 요약 |
| `src/omx_brainstorm/master_engine.py` | 설정 기반 리팩토링 |
| `config/masters.toml` | **신규** — 전문가 프로필 설정 |
| `src/omx_brainstorm/prompts.py` | video_context_summary 필드 추가 |
| `src/omx_brainstorm/analysis.py` | video_context_summary 파싱 |
| `src/omx_brainstorm/heuristic_pipeline.py` | plain_summary 생성 호출 추가 |
| `dashboard/app.py` | 랭킹 expander UI 개선 |
| `dashboard/data_loader.py` | plain_summary/video_context backfill |
| `tests/` | 각 변경에 대한 테스트 추가 |

## 6. 절대 기준 (v1) vs 업종 평균 (향후)

v1은 절대 기준만 사용. 향후 확장 시:
- `sector`, `industry` 필드는 이미 수집
- `config/sector_benchmarks.toml`에 업종별 평균 정의
- `plain_summary.py`에서 업종 평균 대비 비교 문구 생성

## 7. 비용 영향

- Yahoo Finance API: `recommendations` 추가 호출 — 기존 `info` 호출에 포함, 추가 비용 없음
- LLM: `video_context_summary` 1~2문장 추가 — 기존 프롬프트에 필드 하나 추가, 토큰 미미
- 전문가 엔진: LLM 호출 없음 (rule-based + 템플릿)
