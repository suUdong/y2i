# Analyst Consensus + Plain Summary + Config-Based Masters Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Yahoo Finance analyst consensus signals, rule-based plain-language summaries, LLM video context summaries, and refactor the master engine to be config-driven — all surfaced in the dashboard ranking expander.

**Architecture:** FundamentalSnapshot gets analyst fields, a new `plain_summary.py` module converts metrics to plain Korean, `master_engine.py` reads `config/masters.toml` instead of hardcoded functions, and `dashboard/app.py` renders the enriched expander UI.

**Tech Stack:** Python 3.12, yfinance, tomllib (stdlib), Streamlit, pytest

**Spec:** `docs/superpowers/specs/2026-04-06-analyst-consensus-plain-summary-design.md`

**Project root:** `/home/wdsr88/workspace/y2i`

---

## File Map

| File | Action | Responsibility |
|------|--------|---------------|
| `src/omx_brainstorm/models.py` | Modify | Add analyst + sector fields to FundamentalSnapshot; add plain_summary + video_context_summary to StockAnalysis |
| `src/omx_brainstorm/fundamentals.py` | Modify | Collect recommendation/sector data in `_fetch_live` |
| `src/omx_brainstorm/plain_summary.py` | Create | Rule-based metric → plain Korean + analyst summary |
| `config/masters.toml` | Create | Master profiles with weights, risks, templates |
| `src/omx_brainstorm/master_engine.py` | Rewrite | Config-driven scoring engine, remove hardcoded functions |
| `src/omx_brainstorm/prompts.py` | Modify | Add video_context_summary to schema |
| `src/omx_brainstorm/analysis.py` | Modify | Parse video_context_summary from LLM payload |
| `src/omx_brainstorm/heuristic_pipeline.py` | Modify | Call build_plain_summary, attach to stock dict |
| `dashboard/app.py` | Modify | Enhanced expander with plain summary + analyst consensus + masters + video context |
| `dashboard/data_loader.py` | Modify | Backfill plain_summary/analyst data from legacy JSON |
| `tests/test_plain_summary.py` | Create | Tests for plain_summary module |
| `tests/test_master_engine.py` | Rewrite | Tests for config-driven master engine |
| `tests/test_fundamentals_analyst.py` | Create | Tests for analyst field collection |

---

### Task 1: FundamentalSnapshot — Add Analyst + Sector Fields

**Files:**
- Modify: `src/omx_brainstorm/models.py:106-124`
- Test: `tests/test_fundamentals_analyst.py` (create)

- [ ] **Step 1: Write the failing test**

Create `tests/test_fundamentals_analyst.py`:

```python
from omx_brainstorm.models import FundamentalSnapshot


def test_fundamental_snapshot_has_analyst_fields():
    snap = FundamentalSnapshot(
        ticker="AAPL",
        recommendation_key="buy",
        recommendation_mean=1.9,
        analyst_count=40,
        target_mean_price=295.0,
        target_median_price=300.0,
        analyst_strong_buy=6,
        analyst_buy=25,
        analyst_hold=15,
        analyst_sell=1,
        analyst_strong_sell=1,
        sector="Technology",
        industry="Consumer Electronics",
    )
    assert snap.recommendation_key == "buy"
    assert snap.recommendation_mean == 1.9
    assert snap.analyst_count == 40
    assert snap.target_mean_price == 295.0
    assert snap.target_median_price == 300.0
    assert snap.analyst_strong_buy == 6
    assert snap.analyst_buy == 25
    assert snap.analyst_hold == 15
    assert snap.analyst_sell == 1
    assert snap.analyst_strong_sell == 1
    assert snap.sector == "Technology"
    assert snap.industry == "Consumer Electronics"


def test_fundamental_snapshot_analyst_defaults():
    snap = FundamentalSnapshot(ticker="TEST")
    assert snap.recommendation_key is None
    assert snap.recommendation_mean is None
    assert snap.analyst_count is None
    assert snap.target_mean_price is None
    assert snap.target_median_price is None
    assert snap.analyst_strong_buy == 0
    assert snap.analyst_buy == 0
    assert snap.analyst_hold == 0
    assert snap.analyst_sell == 0
    assert snap.analyst_strong_sell == 0
    assert snap.sector is None
    assert snap.industry is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_fundamentals_analyst.py -v`
Expected: FAIL with `TypeError: FundamentalSnapshot.__init__() got an unexpected keyword argument 'recommendation_key'`

- [ ] **Step 3: Add fields to FundamentalSnapshot**

In `src/omx_brainstorm/models.py`, after the `notes` field (line 124), add:

```python
    # Analyst consensus (Yahoo Finance)
    recommendation_key: str | None = None
    recommendation_mean: float | None = None
    analyst_count: int | None = None
    target_mean_price: float | None = None
    target_median_price: float | None = None
    analyst_strong_buy: int = 0
    analyst_buy: int = 0
    analyst_hold: int = 0
    analyst_sell: int = 0
    analyst_strong_sell: int = 0
    # Sector classification
    sector: str | None = None
    industry: str | None = None
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_fundamentals_analyst.py -v`
Expected: 2 passed

- [ ] **Step 5: Run full test suite to check no regressions**

Run: `pytest tests/ -x -q`
Expected: All tests pass (689+)

- [ ] **Step 6: Commit**

```bash
git add src/omx_brainstorm/models.py tests/test_fundamentals_analyst.py
git commit -m "feat: add analyst consensus + sector fields to FundamentalSnapshot"
```

---

### Task 2: Fundamentals Fetcher — Collect Analyst Data

**Files:**
- Modify: `src/omx_brainstorm/fundamentals.py:89-145`
- Test: `tests/test_fundamentals_analyst.py` (append)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_fundamentals_analyst.py`:

```python
from unittest.mock import MagicMock, patch, PropertyMock
import pandas as pd
from omx_brainstorm.fundamentals import FundamentalsFetcher
from omx_brainstorm.models import TickerMention


def test_fetch_live_collects_analyst_data(tmp_path):
    mock_info = {
        "longName": "Apple Inc.",
        "currentPrice": 230.0,
        "currency": "USD",
        "recommendationKey": "buy",
        "recommendationMean": 1.9,
        "numberOfAnalystOpinions": 40,
        "targetMeanPrice": 295.0,
        "targetMedianPrice": 300.0,
        "sector": "Technology",
        "industry": "Consumer Electronics",
    }
    mock_rec = pd.DataFrame([{
        "period": "0m",
        "strongBuy": 6,
        "buy": 25,
        "hold": 15,
        "sell": 1,
        "strongSell": 1,
    }])

    mock_ticker = MagicMock()
    mock_ticker.info = mock_info
    type(mock_ticker).recommendations = PropertyMock(return_value=mock_rec)

    with patch("yfinance.Ticker", return_value=mock_ticker):
        fetcher = FundamentalsFetcher(cache_root=tmp_path, max_memory_entries=0)
        mention = TickerMention(ticker="AAPL", company_name="Apple")
        snap = fetcher._fetch_live(mention)

    assert snap.recommendation_key == "buy"
    assert snap.recommendation_mean == 1.9
    assert snap.analyst_count == 40
    assert snap.target_mean_price == 295.0
    assert snap.target_median_price == 300.0
    assert snap.analyst_strong_buy == 6
    assert snap.analyst_buy == 25
    assert snap.analyst_hold == 15
    assert snap.analyst_sell == 1
    assert snap.analyst_strong_sell == 1
    assert snap.sector == "Technology"
    assert snap.industry == "Consumer Electronics"


def test_fetch_live_handles_missing_recommendations(tmp_path):
    mock_info = {"longName": "Test Corp", "currentPrice": 100.0, "currency": "USD"}
    mock_ticker = MagicMock()
    mock_ticker.info = mock_info
    type(mock_ticker).recommendations = PropertyMock(return_value=None)

    with patch("yfinance.Ticker", return_value=mock_ticker):
        fetcher = FundamentalsFetcher(cache_root=tmp_path, max_memory_entries=0)
        mention = TickerMention(ticker="TEST")
        snap = fetcher._fetch_live(mention)

    assert snap.recommendation_key is None
    assert snap.analyst_count is None
    assert snap.analyst_strong_buy == 0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_fundamentals_analyst.py::test_fetch_live_collects_analyst_data -v`
Expected: FAIL — `recommendation_key` is None (not yet collected)

- [ ] **Step 3: Update `_fetch_live` to collect analyst data**

In `src/omx_brainstorm/fundamentals.py`, replace the `snapshot = FundamentalSnapshot(...)` block (lines 119-137) with:

```python
        # Analyst recommendations
        rec_key = info.get("recommendationKey")
        rec_mean = _as_float(info.get("recommendationMean"))
        analyst_count = info.get("numberOfAnalystOpinions")
        if isinstance(analyst_count, (int, float)):
            analyst_count = int(analyst_count)
        else:
            analyst_count = None
        target_mean = _as_float(info.get("targetMeanPrice"))
        target_median = _as_float(info.get("targetMedianPrice"))
        sector = info.get("sector")
        industry = info.get("industry")

        # Recommendation distribution from recommendations property
        a_strong_buy = 0
        a_buy = 0
        a_hold = 0
        a_sell = 0
        a_strong_sell = 0
        try:
            rec = ticker.recommendations
            if rec is not None and not rec.empty:
                current = rec.iloc[0]
                a_strong_buy = int(current.get("strongBuy", 0) or 0)
                a_buy = int(current.get("buy", 0) or 0)
                a_hold = int(current.get("hold", 0) or 0)
                a_sell = int(current.get("sell", 0) or 0)
                a_strong_sell = int(current.get("strongSell", 0) or 0)
        except Exception:
            pass  # recommendations not available for all tickers

        snapshot = FundamentalSnapshot(
            ticker=mention.ticker,
            company_name=_pick("longName", "shortName") or mention.company_name,
            checked_at=utc_now_iso(),
            currency=_pick("currency", "financialCurrency"),
            current_price=_as_float(current_price),
            market_cap=_as_float(market_cap),
            trailing_pe=_as_float(_pick("trailingPE")),
            forward_pe=_as_float(_pick("forwardPE")),
            price_to_book=_as_float(_pick("priceToBook")),
            revenue_growth=_as_float(_pick("revenueGrowth")),
            earnings_growth=_as_float(_pick("earningsGrowth")),
            operating_margin=_as_float(_pick("operatingMargins")),
            return_on_equity=_as_float(_pick("returnOnEquity")),
            debt_to_equity=_as_float(_pick("debtToEquity")),
            fifty_two_week_change=_as_float(_pick("52WeekChange", "fiftyTwoWeekChange")),
            data_source="yfinance",
            notes=[],
            recommendation_key=rec_key if isinstance(rec_key, str) else None,
            recommendation_mean=rec_mean,
            analyst_count=analyst_count,
            target_mean_price=target_mean,
            target_median_price=target_median,
            analyst_strong_buy=a_strong_buy,
            analyst_buy=a_buy,
            analyst_hold=a_hold,
            analyst_sell=a_sell,
            analyst_strong_sell=a_strong_sell,
            sector=sector if isinstance(sector, str) else None,
            industry=industry if isinstance(industry, str) else None,
        )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_fundamentals_analyst.py -v`
Expected: All 4 tests pass

- [ ] **Step 5: Run full test suite**

Run: `pytest tests/ -x -q`
Expected: All pass

- [ ] **Step 6: Commit**

```bash
git add src/omx_brainstorm/fundamentals.py tests/test_fundamentals_analyst.py
git commit -m "feat: collect analyst consensus + sector in fundamentals fetcher"
```

---

### Task 3: Plain Summary Module — Rule-Based Korean Summaries

**Files:**
- Create: `src/omx_brainstorm/plain_summary.py`
- Create: `tests/test_plain_summary.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_plain_summary.py`:

```python
from omx_brainstorm.models import FundamentalSnapshot
from omx_brainstorm.plain_summary import build_plain_summary, build_analyst_summary


def test_build_plain_summary_high_margin():
    snap = FundamentalSnapshot(
        ticker="TEST",
        operating_margin=0.30,
        return_on_equity=0.18,
        debt_to_equity=45.0,
        forward_pe=20.0,
        revenue_growth=0.25,
    )
    result = build_plain_summary(snap)
    assert "돈을 아주 잘 버는 회사" in result
    assert "영업이익률 30.0%" in result
    assert "투자한 돈 대비 수익이 좋음" in result
    assert "빚이 아주 적음" in result
    assert "주가가 적당한 편" in result
    assert "매출이 꾸준히 성장 중" in result


def test_build_plain_summary_low_margin():
    snap = FundamentalSnapshot(
        ticker="TEST",
        operating_margin=0.03,
        return_on_equity=0.05,
        debt_to_equity=250.0,
        forward_pe=50.0,
        revenue_growth=-0.1,
    )
    result = build_plain_summary(snap)
    assert "돈 벌기가 어려운 회사" in result
    assert "투자 대비 수익이 낮음" in result
    assert "빚이 많은 편" in result
    assert "주가가 아주 비쌈" in result
    assert "매출이 줄어들고 있음" in result


def test_build_plain_summary_none_metrics():
    snap = FundamentalSnapshot(ticker="TEST")
    result = build_plain_summary(snap)
    assert isinstance(result, str)
    # Should not crash, may be empty or minimal
    assert "데이터 부족" in result or result == ""


def test_build_analyst_summary_bullish():
    snap = FundamentalSnapshot(
        ticker="TEST",
        analyst_count=37,
        analyst_strong_buy=12,
        analyst_buy=23,
        analyst_hold=1,
        analyst_sell=0,
        analyst_strong_sell=0,
        target_median_price=250000.0,
        currency="KRW",
    )
    result = build_analyst_summary(snap)
    assert "37명" in result
    assert "35명" in result  # 12+23
    assert "사라" in result
    assert "250,000" in result


def test_build_analyst_summary_no_data():
    snap = FundamentalSnapshot(ticker="TEST")
    result = build_analyst_summary(snap)
    assert "애널리스트 의견이 없어요" in result


def test_build_analyst_summary_mixed():
    snap = FundamentalSnapshot(
        ticker="TEST",
        analyst_count=20,
        analyst_strong_buy=2,
        analyst_buy=6,
        analyst_hold=10,
        analyst_sell=1,
        analyst_strong_sell=1,
        target_median_price=50000.0,
        currency="KRW",
    )
    result = build_analyst_summary(snap)
    assert "지켜보자" in result


def test_build_analyst_summary_sell_warning():
    snap = FundamentalSnapshot(
        ticker="TEST",
        analyst_count=10,
        analyst_strong_buy=0,
        analyst_buy=2,
        analyst_hold=4,
        analyst_sell=3,
        analyst_strong_sell=1,
        target_median_price=30000.0,
        currency="KRW",
    )
    result = build_analyst_summary(snap)
    assert "팔아라" in result
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_plain_summary.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'omx_brainstorm.plain_summary'`

- [ ] **Step 3: Implement `plain_summary.py`**

Create `src/omx_brainstorm/plain_summary.py`:

```python
from __future__ import annotations

from .models import FundamentalSnapshot


def _pct(value: float | None) -> float | None:
    """Convert ratio (0.30) to percentage (30.0). Returns None if input is None."""
    return None if value is None else value * 100.0


def _describe_margin(pct: float) -> str:
    if pct >= 20:
        return f"돈을 아주 잘 버는 회사 (영업이익률 {pct:.1f}%)"
    if pct >= 10:
        return f"돈을 꽤 잘 버는 회사 (영업이익률 {pct:.1f}%)"
    if pct >= 5:
        return f"돈을 보통 수준으로 버는 회사 (영업이익률 {pct:.1f}%)"
    return f"돈 벌기가 어려운 회사 (영업이익률 {pct:.1f}%)"


def _describe_roe(pct: float) -> str:
    if pct >= 15:
        return f"투자한 돈 대비 수익이 좋음 (ROE {pct:.1f}%)"
    if pct >= 8:
        return f"투자 대비 수익이 보통 (ROE {pct:.1f}%)"
    return f"투자 대비 수익이 낮음 (ROE {pct:.1f}%)"


def _describe_debt(de: float) -> str:
    if de < 50:
        return f"빚이 아주 적음 (부채비율 {de:.1f}%)"
    if de < 100:
        return f"빚은 적당한 편 (부채비율 {de:.1f}%)"
    if de < 200:
        return f"빚이 좀 있는 편 (부채비율 {de:.1f}%)"
    return f"빚이 많은 편 (부채비율 {de:.1f}%)"


def _describe_pe(pe: float) -> str:
    if pe < 15:
        return f"주가가 저렴한 편 (PER {pe:.1f}배)"
    if pe < 25:
        return f"주가가 적당한 편 (PER {pe:.1f}배)"
    if pe < 40:
        return f"주가가 비싼 편 (PER {pe:.1f}배)"
    return f"주가가 아주 비쌈 (PER {pe:.1f}배)"


def _describe_revenue_growth(pct: float) -> str:
    if pct >= 30:
        return f"매출이 빠르게 성장 중 (매출성장률 {pct:.1f}%)"
    if pct >= 10:
        return f"매출이 꾸준히 성장 중 (매출성장률 {pct:.1f}%)"
    if pct >= 0:
        return f"매출이 조금씩 성장 중 (매출성장률 {pct:.1f}%)"
    return f"매출이 줄어들고 있음 (매출성장률 {pct:.1f}%)"


def _format_price(price: float, currency: str | None) -> str:
    if currency == "KRW":
        return f"\\{price:,.0f}"
    if currency == "USD":
        return f"${price:,.2f}"
    return f"{price:,.0f}"


def build_plain_summary(snapshot: FundamentalSnapshot) -> str:
    """Convert financial metrics to plain Korean summary lines."""
    lines: list[str] = []
    margin = _pct(snapshot.operating_margin)
    roe = _pct(snapshot.return_on_equity)
    de = snapshot.debt_to_equity
    pe = snapshot.forward_pe
    rev = _pct(snapshot.revenue_growth)

    if margin is not None:
        lines.append(_describe_margin(margin))
    if roe is not None:
        lines.append(_describe_roe(roe))
    if de is not None:
        lines.append(_describe_debt(de))
    if pe is not None and pe > 0:
        lines.append(_describe_pe(pe))
    if rev is not None:
        lines.append(_describe_revenue_growth(rev))

    if not lines:
        return "데이터 부족"

    analyst = build_analyst_summary(snapshot)
    if analyst and "없어요" not in analyst:
        lines.append(analyst)
    return "\n".join(lines)


def build_analyst_summary(snapshot: FundamentalSnapshot) -> str:
    """Convert analyst consensus to plain Korean."""
    count = snapshot.analyst_count
    if not count or count == 0:
        return "애널리스트 의견이 없어요"

    sb = snapshot.analyst_strong_buy
    b = snapshot.analyst_buy
    h = snapshot.analyst_hold
    s = snapshot.analyst_sell
    ss = snapshot.analyst_strong_sell
    total_votes = sb + b + h + s + ss

    if total_votes == 0:
        return "애널리스트 의견이 없어요"

    buy_count = sb + b
    sell_count = s + ss
    buy_pct = buy_count / total_votes * 100
    hold_pct = h / total_votes * 100
    sell_pct = sell_count / total_votes * 100

    target = snapshot.target_median_price or snapshot.target_mean_price
    target_str = ""
    if target:
        target_str = f" (목표가 {_format_price(target, snapshot.currency)})"

    if sell_pct >= 30:
        return f"주의: {count}명 전문가 중 {sell_count}명이 '팔아라' 의견{target_str}"
    if hold_pct >= 50:
        return f"{count}명 전문가들이 '지켜보자'는 의견{target_str}"
    if buy_pct >= 70:
        return f"{count}명 전문가 중 {buy_count}명이 '사라'고 함{target_str}"
    if buy_pct >= 50:
        return f"{count}명 전문가 절반 이상이 '사라'고 함{target_str}"
    return f"{count}명 전문가 의견 혼재 (매수 {buy_count}, 보유 {h}, 매도 {sell_count}){target_str}"
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_plain_summary.py -v`
Expected: All 7 tests pass

- [ ] **Step 5: Run full suite**

Run: `pytest tests/ -x -q`
Expected: All pass

- [ ] **Step 6: Commit**

```bash
git add src/omx_brainstorm/plain_summary.py tests/test_plain_summary.py
git commit -m "feat: add plain_summary module for rule-based Korean metric descriptions"
```

---

### Task 4: Config-Based Master Engine

**Files:**
- Create: `config/masters.toml`
- Rewrite: `src/omx_brainstorm/master_engine.py`
- Rewrite: `tests/test_master_engine.py`

- [ ] **Step 1: Create `config/masters.toml`**

Create `config/masters.toml` with the three existing masters. Copy the exact content from the spec (section 3, lines 141-238). This is the full file:

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
  "정책/유동성 변화로 멀티플 압축 위험",
]

[masters.templates]
one_liner = "{ticker}는 매출성장률 {revenue:.1f}%와 '{evidence}'로 드라이버는 강하지만, PER {forward_pe:.1f}배라 유동성 점검이 먼저다."
rationale = [
  "영상 신호 {video_signal_score:.1f}와 언급 {mention_count}회가 수급 지속을 지지",
  "매출성장률 {revenue:.1f}%와 영업이익률 {margin:.1f}%는 이익 레버리지 유지 중",
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
  "산업 사이클과 고객 집중이 예측 가능성을 낮출 위험",
]

[masters.templates]
one_liner = "{ticker}는 영업이익률 {margin:.1f}%와 ROE {roe:.1f}%로 사업 질은 좋지만, 부채비율 {leverage:.1f}와 밸류에이션을 감안하면 안전마진이 충분하다고 보긴 어렵다."
rationale = [
  "ROE {roe:.1f}%와 영업이익률 {margin:.1f}%는 자본효율과 가격결정력을 뒷받침",
  "PER {forward_pe:.1f}배와 부채비율 {leverage:.1f}는 보수적 진입 여부를 가른다",
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
  "포지셔닝 과열로 작은 악재에도 변동성이 커질 수 있음",
]

[masters.templates]
one_liner = "{ticker}는 52주 수익률 {momentum:.1f}%와 '{evidence}'가 맞물려 서사가 자기강화 구간이지만, 신호 점수 {video_signal_score:.1f}가 높을수록 반사성 꺾임도 빨라질 수 있다."
rationale = [
  "영상 제목과 근거는 현재 테마가 네트워크/메모리/전력 병목 해소 서사 위에 있음을 보여준다",
  "언급 {mention_count}회와 52주 변화율 {momentum:.1f}%는 추세 추종 자금이 붙기 쉬운 조건",
]
```

- [ ] **Step 2: Write failing tests for config-driven engine**

Rewrite `tests/test_master_engine.py`:

```python
import pytest
from pathlib import Path

from omx_brainstorm.master_engine import (
    MasterConfig,
    build_master_opinions,
    build_opinion,
    format_template,
    load_masters,
    master_variance_score,
    master_verdict,
    validate_cross_stock_master_quality,
    validate_master_opinions,
)
from omx_brainstorm.models import FundamentalSnapshot


def test_load_masters_from_toml():
    masters = load_masters()
    assert len(masters) >= 3
    names = [m.name for m in masters]
    assert "druckenmiller" in names
    assert "buffett" in names
    assert "soros" in names


def test_master_config_has_required_fields():
    masters = load_masters()
    for m in masters:
        assert m.name
        assert m.display
        assert m.focus
        assert m.base_score > 0
        assert isinstance(m.weights, dict)
        assert isinstance(m.risks, list)
        assert isinstance(m.templates, dict)
        assert "one_liner" in m.templates
        assert "rationale" in m.templates


def test_build_opinion_produces_valid_opinion():
    masters = load_masters()
    config = masters[0]  # druckenmiller
    metrics = {
        "ticker": "NVDA",
        "evidence": "AI 수요 급증",
        "mention_count": 5,
        "video_signal_score": 90.0,
        "revenue": 73.2,
        "margin": 65.0,
        "roe": 101.4,
        "momentum": 42.2,
        "forward_pe": 15.5,
        "leverage": 7.2,
        "theme_score": 1,
    }
    opinion = build_opinion(config, metrics)
    assert opinion.master == "druckenmiller"
    assert 0 <= opinion.score <= 100
    assert opinion.verdict in ("BUY", "WATCH", "REJECT")
    assert opinion.one_liner
    assert opinion.rationale
    assert any(c.startswith("fundamentals:") for c in opinion.citations)
    assert any(c.startswith("evidence:") for c in opinion.citations)


def test_build_master_opinions_backward_compatible():
    """build_master_opinions() should still work with old signature."""
    snapshot = FundamentalSnapshot(
        ticker="NVDA",
        company_name="NVIDIA Corporation",
        current_price=172.7,
        forward_pe=15.5,
        revenue_growth=0.732,
        operating_margin=0.6502,
        return_on_equity=1.014,
        debt_to_equity=7.2,
        fifty_two_week_change=0.422,
        currency="USD",
    )
    opinions = build_master_opinions(
        ticker="NVDA",
        company_name="NVIDIA Corporation",
        snapshot=snapshot,
        mention_count=5,
        video_title="엔비디아 차세대 메모리 로드맵",
        video_signal_score=90.0,
        evidence_snippets=["엔비디아가 제시한 차세대 메모리 로드맵"],
    )
    assert len(opinions) >= 3
    assert len({item.one_liner for item in opinions}) == len(opinions)
    for item in opinions:
        assert item.rationale
        assert any(c.startswith("fundamentals:") for c in item.citations)
        assert any(c.startswith("evidence:") for c in item.citations)


def test_format_template():
    template = "{ticker}는 매출 {revenue:.1f}%"
    result = format_template(template, {"ticker": "AAPL", "revenue": 25.3})
    assert result == "AAPL는 매출 25.3%"


def test_format_template_missing_key():
    template = "{ticker}는 {missing_key}를 보유"
    result = format_template(template, {"ticker": "AAPL"})
    assert "AAPL" in result  # should not crash


def test_cross_stock_master_quality_guard_rejects_repeated_sentences():
    stocks = [
        {
            "ticker": "AAA",
            "master_opinions": [
                {"master": "druckenmiller", "one_liner": "같은 문장", "citations": ["fundamentals:x", "evidence:y"]},
            ],
        },
        {
            "ticker": "BBB",
            "master_opinions": [
                {"master": "druckenmiller", "one_liner": "같은 문장", "citations": ["fundamentals:x", "evidence:y"]},
            ],
        },
    ]
    with pytest.raises(ValueError):
        validate_cross_stock_master_quality(stocks)
```

- [ ] **Step 3: Run tests to see what fails**

Run: `pytest tests/test_master_engine.py -v`
Expected: FAIL — `MasterConfig`, `build_opinion`, `format_template`, `load_masters` not found

- [ ] **Step 4: Rewrite `master_engine.py`**

Replace `src/omx_brainstorm/master_engine.py` entirely:

```python
from __future__ import annotations

import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from statistics import pstdev
from typing import Any

from .models import FundamentalSnapshot, MasterOpinion

_SKIP_SUFFIXES = ("_cap", "_threshold", "_rate", "_bonus", "_penalty")
_DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent.parent.parent / "config" / "masters.toml"


@dataclass(slots=True)
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
    """Load master profiles from TOML config."""
    config_path = path or _DEFAULT_CONFIG_PATH
    with open(config_path, "rb") as f:
        data = tomllib.load(f)
    masters = []
    for entry in data.get("masters", []):
        weights_raw = entry.get("weights", {})
        weights = {k: float(v) for k, v in weights_raw.items()}
        masters.append(MasterConfig(
            name=entry["name"],
            display=entry.get("display", entry["name"]),
            focus=entry.get("focus", ""),
            key_metrics=list(entry.get("key_metrics", [])),
            base_score=float(entry.get("base_score", 50)),
            weights=weights,
            risks=list(entry.get("risks", {}).get("items", [])),
            templates=dict(entry.get("templates", {})),
        ))
    return masters


def build_opinion(config: MasterConfig, metrics: dict[str, Any]) -> MasterOpinion:
    """Generic scoring engine: base_score + weighted components - penalties."""
    score = config.base_score
    weights = config.weights

    # Standard weighted components
    for key, weight in weights.items():
        if any(key.endswith(suffix) for suffix in _SKIP_SUFFIXES):
            continue
        value = float(metrics.get(key, 0))
        component = value * weight
        cap = weights.get(f"{key}_cap")
        if cap is not None:
            component = min(component, float(cap))
        score += component

    # Valuation penalty
    val_threshold = weights.get("valuation_penalty_threshold")
    val_rate = weights.get("valuation_penalty_rate")
    if val_threshold is not None and val_rate is not None:
        forward_pe = float(metrics.get("forward_pe", 0))
        score -= max(forward_pe - val_threshold, 0) * val_rate

    # Leverage tiers (buffett-style)
    lev_low_threshold = weights.get("leverage_low_threshold")
    if lev_low_threshold is not None:
        leverage = float(metrics.get("leverage", 0))
        lev_mid_threshold = float(weights.get("leverage_mid_threshold", 120))
        if leverage < lev_low_threshold:
            score += float(weights.get("leverage_low_bonus", 0))
        elif leverage < lev_mid_threshold:
            score += float(weights.get("leverage_mid_bonus", 0))
        else:
            score += float(weights.get("leverage_high_penalty", 0))

    # Valuation tiers (buffett-style)
    val_low_threshold = weights.get("valuation_low_threshold")
    if val_low_threshold is not None:
        forward_pe = float(metrics.get("forward_pe", 0))
        val_mid_threshold = float(weights.get("valuation_mid_threshold", 30))
        if 0 < forward_pe < val_low_threshold:
            score += float(weights.get("valuation_low_bonus", 0))
        elif forward_pe < val_mid_threshold:
            score += float(weights.get("valuation_mid_bonus", 0))
        else:
            score += float(weights.get("valuation_high_penalty", 0))

    score = _clamp(score)
    verdict = master_verdict(score)

    # Format templates
    ticker = str(metrics.get("ticker", ""))
    evidence = str(metrics.get("evidence", ""))
    template_vars = {**metrics, "verdict_kr": _verdict_kr(verdict)}
    one_liner = format_template(config.templates.get("one_liner", ""), template_vars)
    rationale_templates = config.templates.get("rationale", [])
    rationale = [format_template(t, template_vars) for t in rationale_templates]

    # Auto-generate citations from key_metrics
    citations = []
    for km in config.key_metrics:
        val = metrics.get(km)
        if val is not None:
            citations.append(f"fundamentals:{km}={val}")
            break
    if not citations:
        citations.append(f"fundamentals:base_score={config.base_score}")
    citations.append(f"evidence:{evidence}")

    return MasterOpinion(
        master=config.name,
        verdict=verdict,
        score=round(score, 1),
        max_score=100.0,
        one_liner=one_liner,
        rationale=rationale,
        risks=list(config.risks),
        citations=citations,
    )


def format_template(template: str, metrics: dict[str, Any]) -> str:
    """Format a template string with metrics, handling missing keys gracefully."""
    try:
        return template.format(**metrics)
    except (KeyError, ValueError, IndexError):
        # Fall back: replace known keys, leave unknown as-is
        result = template
        for key, value in metrics.items():
            try:
                result = result.replace(f"{{{key}}}", str(value))
                # Handle format specs like {revenue:.1f}
                for spec in [".1f", ".2f", ".0f", "d"]:
                    placeholder = f"{{{key}:{spec}}}"
                    if placeholder in result:
                        try:
                            result = result.replace(placeholder, f"{float(value):{spec}}")
                        except (TypeError, ValueError):
                            result = result.replace(placeholder, str(value))
            except Exception:
                continue
        return result


def build_master_opinions(
    ticker: str,
    company_name: str | None,
    snapshot: FundamentalSnapshot,
    mention_count: int,
    video_title: str,
    video_signal_score: float,
    evidence_snippets: list[str],
    masters_config_path: Path | None = None,
) -> list[MasterOpinion]:
    """Build stock-specific opinions using config-driven masters. Backward-compatible."""
    evidence_snippets = [item.strip() for item in evidence_snippets if item and item.strip()]
    primary_evidence = evidence_snippets[0] if evidence_snippets else video_title
    revenue = _pct(snapshot.revenue_growth)
    margin = _pct(snapshot.operating_margin)
    roe = _pct(snapshot.return_on_equity)
    momentum = _pct(snapshot.fifty_two_week_change)
    forward_pe = snapshot.forward_pe or 0.0
    leverage = snapshot.debt_to_equity or 0.0
    earnings_growth = _pct(snapshot.earnings_growth)
    theme_score = 1 if any(
        word in video_title.lower()
        for word in ["로드맵", "roadmap", "foundry", "gpu", "전력", "memory"]
    ) else 0

    metrics = {
        "ticker": ticker,
        "evidence": primary_evidence,
        "mention_count": mention_count,
        "mention": mention_count,
        "video_signal_score": video_signal_score,
        "signal": video_signal_score,
        "revenue": revenue,
        "margin": margin,
        "roe": roe,
        "momentum": momentum,
        "forward_pe": forward_pe,
        "leverage": leverage,
        "theme": theme_score,
        "theme_score": theme_score,
        "earnings_growth": earnings_growth,
    }

    masters = load_masters(masters_config_path)
    opinions = [build_opinion(config, metrics) for config in masters]
    return validate_master_opinions(opinions)


def master_variance_score(master_opinions: list[dict] | list[MasterOpinion]) -> float:
    """Measure disagreement across master scores."""
    scores = []
    for item in master_opinions:
        if isinstance(item, MasterOpinion):
            scores.append(float(item.score))
        else:
            scores.append(float(item.get("score", 0.0)))
    if len(scores) < 2:
        return 0.0
    return round(pstdev(scores), 2)


def validate_master_opinions(master_opinions: list[MasterOpinion]) -> list[MasterOpinion]:
    """Validate that each master opinion contains required non-boilerplate content."""
    one_liners = [item.one_liner.strip() for item in master_opinions if item.one_liner.strip()]
    if len(one_liners) != len(master_opinions):
        raise ValueError("master one-liner missing")
    if len(set(one_liners)) != len(one_liners):
        raise ValueError("master one-liners must be unique per stock")
    for item in master_opinions:
        if not item.rationale:
            raise ValueError(f"{item.master} rationale missing")
        if not any(citation.startswith("fundamentals:") for citation in item.citations):
            raise ValueError(f"{item.master} fundamentals citation missing")
        if not any(citation.startswith("evidence:") for citation in item.citations):
            raise ValueError(f"{item.master} evidence citation missing")
    return master_opinions


def validate_cross_stock_master_quality(stocks: list[dict]) -> None:
    """Reject repeated identical master sentences across different stocks."""
    seen: dict[tuple[str, str], str] = {}
    for stock in stocks:
        ticker = stock.get("ticker", "unknown")
        for item in stock.get("master_opinions", []):
            master = item["master"] if isinstance(item, dict) else item.master
            sentence = (item["one_liner"] if isinstance(item, dict) else item.one_liner).strip()
            if not sentence:
                raise ValueError(f"{ticker} missing master one-liner")
            key = (master, sentence)
            if key in seen and seen[key] != ticker:
                raise ValueError(f"Repeated master sentence across stocks: {master} -> {sentence}")
            seen[key] = ticker


def master_verdict(score: float) -> str:
    if score >= 80:
        return "BUY"
    if score >= 62:
        return "WATCH"
    return "REJECT"


def _clamp(score: float) -> float:
    return max(0.0, min(100.0, score))


def _pct(value: float | None) -> float:
    return 0.0 if value is None else value * 100.0


def _verdict_kr(verdict: str) -> str:
    return {"BUY": "매수", "WATCH": "관망", "REJECT": "회피"}.get(verdict, verdict)
```

- [ ] **Step 5: Run tests**

Run: `pytest tests/test_master_engine.py -v`
Expected: All 8 tests pass

- [ ] **Step 6: Run full suite**

Run: `pytest tests/ -x -q`
Expected: All pass

- [ ] **Step 7: Commit**

```bash
git add config/masters.toml src/omx_brainstorm/master_engine.py tests/test_master_engine.py
git commit -m "refactor: config-driven master engine with TOML profiles"
```

---

### Task 5: StockAnalysis Fields + Prompts + Analysis

**Files:**
- Modify: `src/omx_brainstorm/models.py:139-157` (StockAnalysis)
- Modify: `src/omx_brainstorm/prompts.py:42-101` (ANALYSIS_SYSTEM)
- Modify: `src/omx_brainstorm/analysis.py:71-89` (parse video_context_summary)
- Test: `tests/test_prompts.py` (verify schema includes new field)

- [ ] **Step 1: Add fields to StockAnalysis**

In `src/omx_brainstorm/models.py`, after `price_targets` field in StockAnalysis (line 157), add:

```python
    plain_summary: str = ""
    video_context_summary: str = ""
```

- [ ] **Step 2: Add `video_context_summary` to ANALYSIS_SYSTEM prompt**

In `src/omx_brainstorm/prompts.py`, in the JSON schema inside `ANALYSIS_SYSTEM` (after `"citations": [str]` on line 87), add:

```
  \"video_context_summary\": str
```

And add this rule after line 100:

```
- video_context_summary: Summarize what this video says about the stock in 1-2 sentences, quoting key phrases from the transcript. This must be factual — cite transcript content only, do not infer or speculate.
```

- [ ] **Step 3: Parse `video_context_summary` in `analysis.py`**

In `src/omx_brainstorm/analysis.py`, in the `return StockAnalysis(...)` block, after `price_targets=price_targets,` (line 88), add:

```python
            video_context_summary=payload.get("video_context_summary", ""),
```

- [ ] **Step 4: Run existing prompt test + full suite**

Run: `pytest tests/test_prompts.py tests/test_master_engine.py tests/ -x -q`
Expected: All pass

- [ ] **Step 5: Commit**

```bash
git add src/omx_brainstorm/models.py src/omx_brainstorm/prompts.py src/omx_brainstorm/analysis.py
git commit -m "feat: add plain_summary + video_context_summary to StockAnalysis and LLM prompt"
```

---

### Task 6: Pipeline Integration — Generate plain_summary

**Files:**
- Modify: `src/omx_brainstorm/heuristic_pipeline.py:314-346`

- [ ] **Step 1: Add import**

At the top of `src/omx_brainstorm/heuristic_pipeline.py`, add to imports (around line 13):

```python
from .plain_summary import build_plain_summary
```

- [ ] **Step 2: Add `plain_summary` and `video_context_summary` to stock dict**

In `src/omx_brainstorm/heuristic_pipeline.py`, in the `row["stocks"].append({...})` dict (after `"price_target"` on line 344), add:

```python
                    "plain_summary": build_plain_summary(snapshot),
                    "video_context_summary": "",  # filled by LLM pipeline
```

- [ ] **Step 3: Run full suite**

Run: `pytest tests/ -x -q`
Expected: All pass

- [ ] **Step 4: Commit**

```bash
git add src/omx_brainstorm/heuristic_pipeline.py
git commit -m "feat: generate plain_summary in heuristic pipeline"
```

---

### Task 7: Dashboard — Enhanced Expander with Plain Summary + Analyst

**Files:**
- Modify: `dashboard/app.py:1709-1800` (existing expander code)
- Modify: `dashboard/data_loader.py` (backfill plain_summary + analyst)

- [ ] **Step 1: Update `_backfill_video_summaries` in `data_loader.py`**

In `dashboard/data_loader.py`, in the `_backfill_video_summaries` function, add `plain_summary` and `fundamentals` to the per-stock summary dict. Change the inner loop to:

```python
    for video in videos:
        if not video.get("should_analyze_stocks"):
            continue
        for stock in video.get("stocks", []):
            ticker = stock.get("ticker", "")
            if not ticker:
                continue
            ticker_summaries.setdefault(ticker, []).append({
                "video_title": video.get("title", ""),
                "video_id": video.get("video_id", ""),
                "published_at": video.get("published_at"),
                "thesis_summary": stock.get("thesis_summary", ""),
                "signal_summary": stock.get("basic_signal_summary", ""),
                "verdict": stock.get(
                    "final_verdict", stock.get("basic_signal_verdict", "")
                ),
                "final_score": float(stock.get("final_score", 0.0) or 0.0),
                "mention_count": int(stock.get("mention_count", 0) or 0),
                "plain_summary": stock.get("plain_summary", ""),
                "video_context_summary": stock.get("video_context_summary", ""),
                "master_opinions": stock.get("master_opinions", []),
                "fundamentals": stock.get("fundamentals", {}),
            })
```

- [ ] **Step 2: Replace the expander UI in `dashboard/app.py`**

Replace the entire expander block (lines 1709-1800 approximately) with the enhanced version. The expander goes inside the `if ranking:` block, after the bar chart. Replace from `# ── 종목 상세: 점수 근거 + 영상별 요약 ──` through the end of `shown_tickers` loop:

```python
        # ── 종목 상세: 한눈에 보기 + 전문가 의견 + 영상별 요약 ──
        st.markdown("##### 종목 상세 분석")
        _rank_lookup: dict[str, dict[str, Any]] = {}
        for _ri in ranking:
            _tk = _ri.get("ticker", "")
            if _tk:
                _rank_lookup[_tk] = _ri

        shown_tickers = df_rank["종목"].tolist()
        for _display_name in shown_tickers[:20]:
            _match = None
            for _tk, _ri in _rank_lookup.items():
                _dn = format_ticker_display(_tk, _ri.get("company_name", ""))
                if _dn == _display_name:
                    _match = _ri
                    break
            if not _match:
                continue

            _score = _match.get("aggregate_score", 0)
            _verdict = translate_verdict(
                _match.get("aggregate_verdict", _match.get("final_verdict", ""))
            )
            with st.expander(
                f"{_display_name}  —  {_score}점 · {_verdict}",
                expanded=False,
            ):
                summaries = _match.get("source_video_summaries", [])

                # (A) 한눈에 보기 — plain summary
                _plain = ""
                if summaries:
                    _plain = summaries[0].get("plain_summary", "")
                    # Try to build from fundamentals if not pre-generated
                    if not _plain:
                        _fund = summaries[0].get("fundamentals", {})
                        if _fund:
                            _lines = []
                            _om = _fund.get("operating_margin")
                            if _om is not None:
                                _om_pct = float(_om) * 100
                                if _om_pct >= 20:
                                    _lines.append(f"돈을 아주 잘 버는 회사 (영업이익률 {_om_pct:.1f}%)")
                                elif _om_pct >= 10:
                                    _lines.append(f"돈을 꽤 잘 버는 회사 (영업이익률 {_om_pct:.1f}%)")
                                elif _om_pct >= 5:
                                    _lines.append(f"돈을 보통 수준으로 버는 회사 (영업이익률 {_om_pct:.1f}%)")
                                else:
                                    _lines.append(f"돈 벌기가 어려운 회사 (영업이익률 {_om_pct:.1f}%)")
                            _de = _fund.get("debt_to_equity")
                            if _de is not None:
                                _de_f = float(_de)
                                if _de_f < 50:
                                    _lines.append(f"빚이 아주 적음 (부채비율 {_de_f:.1f}%)")
                                elif _de_f < 100:
                                    _lines.append(f"빚은 적당한 편 (부채비율 {_de_f:.1f}%)")
                                elif _de_f < 200:
                                    _lines.append(f"빚이 좀 있는 편 (부채비율 {_de_f:.1f}%)")
                                else:
                                    _lines.append(f"빚이 많은 편 (부채비율 {_de_f:.1f}%)")
                            # Analyst consensus from fundamentals
                            _ac = _fund.get("analyst_count")
                            if _ac and int(_ac) > 0:
                                _sb = int(_fund.get("analyst_strong_buy", 0) or 0)
                                _ab = int(_fund.get("analyst_buy", 0) or 0)
                                _bc = _sb + _ab
                                _tp = _fund.get("target_median_price") or _fund.get("target_mean_price")
                                _cur = _fund.get("currency", "KRW")
                                _tp_str = ""
                                if _tp:
                                    if _cur == "KRW":
                                        _tp_str = f" (목표가 \\{float(_tp):,.0f})"
                                    else:
                                        _tp_str = f" (목표가 ${float(_tp):,.2f})"
                                _lines.append(f"{_ac}명 전문가 중 {_bc}명이 '사라'고 함{_tp_str}")
                            _plain = "\n".join(_lines)
                if _plain:
                    st.markdown(f"**한눈에 보기:**\n\n{_plain}")

                # (B) 전문가 의견
                if summaries:
                    all_masters: list[dict[str, Any]] = []
                    for _vs in summaries[:3]:
                        for _mo in _vs.get("master_opinions", []):
                            if isinstance(_mo, dict) and _mo.get("master"):
                                all_masters.append(_mo)
                    if all_masters:
                        st.markdown("**전문가 의견:**")
                        seen_masters: set[str] = set()
                        for _mo in all_masters:
                            _mn = _mo.get("master", "")
                            if _mn in seen_masters:
                                continue
                            seen_masters.add(_mn)
                            _ms = _mo.get("score", 0)
                            _mv = translate_verdict(_mo.get("verdict", ""))
                            _ml = _mo.get("one_liner", "")
                            if _ml:
                                st.markdown(f"  - **{_mn}** ({_ms}/100, {_mv}): {_ml}")

                # (C) 점수 구성
                _wb = _match.get("weighted_base_score")
                _cb = _match.get("consensus_bonus")
                if _wb is not None:
                    breakdown_parts = [
                        ("기본", _wb),
                        ("합의", _cb),
                        ("품질", _match.get("quality_weight_adjustment")),
                        ("밀도", _match.get("consensus_density_bonus")),
                        ("감점", _match.get("consensus_disagreement_penalty")),
                    ]
                    parts_str = " + ".join(
                        f"{label} {v:+.1f}" for label, v in breakdown_parts
                        if v is not None and v != 0
                    )
                    if parts_str:
                        st.markdown(f"**점수 구성:** {parts_str} = {_score}")

                # (D) 영상별 분석
                if summaries:
                    st.markdown("**영상에서 이 종목은:**")
                    for _vs in summaries:
                        _vt = _vs.get("video_title", "")
                        _vcs = _vs.get("video_context_summary", "")
                        _thesis = _vs.get("thesis_summary", "")
                        _sig = _vs.get("signal_summary", "")
                        _verd = translate_verdict(_vs.get("verdict", ""))
                        _fs = _vs.get("final_score", 0)
                        _text = _vcs or _thesis or _sig or ""
                        if _text:
                            st.markdown(
                                f"- **{_vt}** ({_verd}, {_fs:.0f}점)\n"
                                f"  > {_text}"
                            )
                        else:
                            st.markdown(f"- **{_vt}** ({_verd}, {_fs:.0f}점)")
                elif _match.get("source_video_titles"):
                    st.markdown("**출처 영상:**")
                    for _svt in _match["source_video_titles"]:
                        st.markdown(f"- {_svt}")
```

- [ ] **Step 3: Run full suite**

Run: `pytest tests/ -x -q`
Expected: All pass

- [ ] **Step 4: Verify dashboard loads**

Run: `cd /home/wdsr88/workspace/y2i && python3 -c "from dashboard.data_loader import get_all_rankings, DEFAULT_OUTPUT_DIR; r = get_all_rankings(DEFAULT_OUTPUT_DIR); s = r[0].get('source_video_summaries', []) if r else []; print(f'rankings={len(r)}, summaries={len(s)}, has_fundamentals={bool(s and s[0].get(\"fundamentals\"))}')"` (ignore streamlit cache warnings)
Expected: `rankings=N, summaries=M, has_fundamentals=True`

- [ ] **Step 5: Commit**

```bash
git add dashboard/app.py dashboard/data_loader.py
git commit -m "feat: enhanced dashboard expander with plain summary, analyst consensus, and master opinions"
```

---

### Task 8: Final Verification

- [ ] **Step 1: Run full test suite**

Run: `pytest tests/ -v`
Expected: All tests pass (700+)

- [ ] **Step 2: Type check (if mypy configured)**

Run: `mypy src/omx_brainstorm/plain_summary.py src/omx_brainstorm/master_engine.py --ignore-missing-imports` (if mypy is available)
Expected: No errors

- [ ] **Step 3: Lint check**

Run: `ruff check src/omx_brainstorm/plain_summary.py src/omx_brainstorm/master_engine.py src/omx_brainstorm/models.py src/omx_brainstorm/fundamentals.py` (if ruff is configured)
Expected: No errors (or fix any found)

- [ ] **Step 4: Manual dashboard smoke test**

Start: `cd /home/wdsr88/workspace/y2i && streamlit run dashboard/app.py`
Verify: Navigate to ranking tab, click a stock expander, check that:
1. "한눈에 보기" shows plain Korean descriptions
2. "전문가 의견" shows master opinions with scores
3. "영상에서 이 종목은" shows per-video summaries
4. "점수 구성" shows score breakdown

- [ ] **Step 5: Final commit if any fixes needed**

```bash
git add -A
git commit -m "fix: address lint/type/test issues from final verification"
```
