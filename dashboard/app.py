"""Y2I 투자 시그널 대시보드 — 한국어, 다크 테마, 모바일 최적화."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from streamlit_autorefresh import st_autorefresh

from dashboard.data_loader import (
    DEFAULT_OUTPUT_DIR,
    build_overview_report,
    extract_actionable_signals,
    extract_channel_leaderboard,
    extract_cross_video_ranking,
    extract_expert_insights,
    extract_macro_signals,
    extract_per_video,
    extract_recent_tracked_signals,
    extract_signal_distribution,
    extract_signal_accuracy_summary,
    extract_type_distribution,
    extract_videos,
    format_price,
    format_ticker_display,
    get_all_rankings,
    get_available_channels,
    get_channel_display_names,
    get_signal_chart_records,
    get_last_update_time,
    get_live_feed_data,
    get_pipeline_activity,
    get_recent_videos,
    load_30d_results,
    load_channel_comparison,
    load_all_video_titles,
)

# -- Page config ---------------------------------------------------------------

st.set_page_config(
    page_title="Y2I 투자 시그널",
    page_icon=":chart_with_upwards_trend:",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# -- Auto-refresh every 60s ----------------------------------------------------

st_autorefresh(interval=60_000, limit=None, key="auto_refresh")
st.markdown(
    '<div style="position:fixed;bottom:8px;right:10px;z-index:999;'
    'background:rgba(15,23,42,0.85);border:1px solid rgba(255,255,255,0.06);'
    'border-radius:8px;padding:3px 8px;font-size:0.65rem;color:#64748B;">'
    '60초 자동 새로고침</div>',
    unsafe_allow_html=True,
)

# -- Design System CSS ---------------------------------------------------------

st.markdown("""
<style>
/* ================================================================
   Y2I Design System — Dark Finance Theme (Korean)
   BG: #020617  Surface: #0F172A  Card: #1E293B
   Text: #F8FAFC  Muted: #94A3B8
   Green: #22C55E  Red: #EF4444  Amber: #F59E0B  Blue: #3B82F6
   ================================================================ */

/* -- Global ------------------------------------------------------------ */
html, body, [class*="css"] {
    font-size: 16px !important;
    color: #F8FAFC !important;
}
.stApp, [data-testid="stAppViewContainer"] {
    background: #020617 !important;
}
header[data-testid="stHeader"] {
    background: #0F172A !important;
    border-bottom: 1px solid rgba(255,255,255,0.06);
}
[data-testid="stSidebar"] {
    background: #0F172A !important;
}

/* -- Block container --------------------------------------------------- */
.block-container {
    padding: 1rem 1rem 2rem !important;
    max-width: 100% !important;
}
@media (min-width: 768px) {
    .block-container {
        padding: 1.5rem 2rem 3rem !important;
        max-width: 1200px !important;
    }
}

/* -- Card component ---------------------------------------------------- */
.omx-card {
    background: #0F172A;
    border: 1px solid rgba(255,255,255,0.06);
    border-radius: 16px;
    padding: 1.25rem;
    margin-bottom: 1rem;
    transition: border-color 0.2s;
}
.omx-card:hover {
    border-color: rgba(255,255,255,0.12);
}
.omx-card-actionable {
    border-left: 4px solid #22C55E;
}
.omx-card-noise {
    border-left: 4px solid #EF4444;
    opacity: 0.8;
}

/* -- KPI Metrics ------------------------------------------------------- */
[data-testid="stMetric"] {
    background: #0F172A !important;
    border: 1px solid rgba(255,255,255,0.06);
    border-radius: 16px;
    padding: 1rem 1.25rem !important;
}
[data-testid="stMetricValue"] {
    font-size: 2rem !important;
    font-weight: 800 !important;
    color: #F8FAFC !important;
    letter-spacing: -0.02em;
}
[data-testid="stMetricLabel"] {
    font-size: 0.75rem !important;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: #94A3B8 !important;
    font-weight: 600;
}
[data-testid="stMetricDelta"] {
    font-size: 0.85rem !important;
}

/* -- Tabs -------------------------------------------------------------- */
.stTabs [data-baseweb="tab-list"] {
    gap: 0;
    overflow-x: auto;
    -webkit-overflow-scrolling: touch;
    scrollbar-width: none;
    flex-wrap: nowrap;
    background: #0F172A;
    border-radius: 12px;
    padding: 4px;
    mask-image: linear-gradient(to right, transparent 0, black 12px, black calc(100% - 12px), transparent 100%);
    -webkit-mask-image: linear-gradient(to right, transparent 0, black 12px, black calc(100% - 12px), transparent 100%);
}
.stTabs [data-baseweb="tab-list"]::-webkit-scrollbar { display: none; }
.stTabs [data-baseweb="tab"] {
    min-height: 48px;
    padding: 0 20px;
    font-size: 0.85rem !important;
    font-weight: 600;
    white-space: nowrap;
    flex-shrink: 0;
    color: #94A3B8 !important;
    border-radius: 8px;
}
.stTabs [data-baseweb="tab"][aria-selected="true"] {
    background: #1E293B !important;
    color: #F8FAFC !important;
}

/* -- Signal Badges ----------------------------------------------------- */
.badge {
    display: inline-block;
    padding: 4px 12px;
    border-radius: 20px;
    font-size: 0.75rem;
    font-weight: 700;
    letter-spacing: 0.04em;
    text-transform: uppercase;
}
.badge-actionable {
    background: rgba(34,197,94,0.15);
    color: #22C55E;
    border: 1px solid rgba(34,197,94,0.3);
}
.badge-noise {
    background: rgba(239,68,68,0.12);
    color: #EF4444;
    border: 1px solid rgba(239,68,68,0.2);
}
.badge-low-signal {
    background: rgba(245,158,11,0.14);
    color: #F59E0B;
    border: 1px solid rgba(245,158,11,0.28);
}
.badge-sector-only {
    background: rgba(59,130,246,0.16);
    color: #60A5FA;
    border: 1px solid rgba(59,130,246,0.3);
}
.badge-non-equity,
.badge-unknown {
    background: rgba(100,116,139,0.16);
    color: #CBD5E1;
    border: 1px solid rgba(148,163,184,0.24);
}
.badge-new {
    background: rgba(59,130,246,0.15);
    color: #3B82F6;
    border: 1px solid rgba(59,130,246,0.3);
    animation: pulse-badge 2s ease-in-out infinite;
}
@keyframes pulse-badge {
    0%, 100% { opacity: 1; }
    50% { opacity: 0.6; }
}

/* -- Verdict Badges ---------------------------------------------------- */
.verdict-buy {
    background: rgba(34,197,94,0.15);
    color: #22C55E;
    border: 1px solid rgba(34,197,94,0.3);
    padding: 3px 10px;
    border-radius: 6px;
    font-size: 0.75rem;
    font-weight: 700;
}
.verdict-sell {
    background: rgba(239,68,68,0.15);
    color: #EF4444;
    border: 1px solid rgba(239,68,68,0.3);
    padding: 3px 10px;
    border-radius: 6px;
    font-size: 0.75rem;
    font-weight: 700;
}
.verdict-watch, .verdict-hold {
    background: rgba(245,158,11,0.15);
    color: #F59E0B;
    border: 1px solid rgba(245,158,11,0.3);
    padding: 3px 10px;
    border-radius: 6px;
    font-size: 0.75rem;
    font-weight: 700;
}
.verdict-reject {
    background: rgba(148,163,184,0.14);
    color: #CBD5E1;
    border: 1px solid rgba(148,163,184,0.3);
    padding: 3px 10px;
    border-radius: 6px;
    font-size: 0.75rem;
    font-weight: 700;
}

/* -- Alert Banner ------------------------------------------------------ */
.omx-alert {
    background: linear-gradient(135deg, rgba(34,197,94,0.08) 0%, rgba(245,158,11,0.08) 100%);
    border: 1px solid rgba(34,197,94,0.2);
    border-radius: 16px;
    padding: 1rem 1.25rem;
    margin-bottom: 1rem;
}
.omx-alert-title {
    color: #22C55E;
    font-size: 0.85rem;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    margin-bottom: 0.5rem;
}
.omx-alert-tickers {
    display: flex;
    flex-wrap: wrap;
    gap: 6px;
}
.hero-shell {
    background:
        radial-gradient(circle at top right, rgba(34,197,94,0.22), transparent 32%),
        radial-gradient(circle at bottom left, rgba(59,130,246,0.18), transparent 28%),
        linear-gradient(135deg, rgba(15,23,42,0.96) 0%, rgba(30,41,59,0.94) 100%);
    border: 1px solid rgba(255,255,255,0.08);
    border-radius: 20px;
    padding: 1.35rem;
    margin: 0.25rem 0 1rem;
}
.hero-kicker {
    color: #22C55E;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    font-size: 0.72rem;
    font-weight: 800;
}
.hero-title {
    color: #F8FAFC;
    font-size: 1.7rem;
    font-weight: 800;
    letter-spacing: -0.03em;
    margin-top: 0.15rem;
}
.hero-copy {
    color: #CBD5E1;
    font-size: 0.92rem;
    line-height: 1.55;
    margin-top: 0.55rem;
    max-width: 72ch;
}
.hero-chip-row {
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    margin-top: 0.95rem;
}
.hero-chip {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    background: rgba(255,255,255,0.05);
    border: 1px solid rgba(255,255,255,0.08);
    border-radius: 999px;
    padding: 7px 12px;
    color: #E2E8F0;
    font-size: 0.78rem;
    font-weight: 600;
}
.hero-chip strong {
    color: #F8FAFC;
    font-size: 0.86rem;
}
.section-kicker {
    color: #64748B;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    font-size: 0.7rem;
    font-weight: 800;
    margin-bottom: 0.35rem;
}
.podium-card {
    background: linear-gradient(180deg, rgba(30,41,59,0.95) 0%, rgba(15,23,42,0.95) 100%);
    border: 1px solid rgba(255,255,255,0.08);
    border-radius: 18px;
    padding: 1rem 1rem 1.1rem;
    min-height: 180px;
    position: relative;
    overflow: hidden;
}
.podium-card::after {
    content: "";
    position: absolute;
    inset: 0;
    background: linear-gradient(180deg, rgba(255,255,255,0.06), transparent 45%);
    pointer-events: none;
}
.podium-rank {
    font-size: 0.8rem;
    font-weight: 800;
    color: #94A3B8;
    letter-spacing: 0.08em;
    text-transform: uppercase;
}
.podium-rank-1 { color: #F8FAFC; }
.podium-rank-2 { color: #CBD5E1; }
.podium-rank-3 { color: #FBBF24; }
.podium-name {
    color: #F8FAFC;
    font-size: 1.05rem;
    font-weight: 800;
    margin-top: 0.35rem;
}
.podium-score {
    color: #22C55E;
    font-size: 2rem;
    font-weight: 900;
    letter-spacing: -0.03em;
    margin-top: 0.65rem;
}
.podium-meta {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 8px;
    margin-top: 0.8rem;
}
.podium-meta-item {
    background: rgba(255,255,255,0.04);
    border-radius: 12px;
    padding: 0.6rem 0.75rem;
}
.podium-meta-label {
    color: #64748B;
    font-size: 0.67rem;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    font-weight: 700;
}
.podium-meta-value {
    color: #F8FAFC;
    font-size: 0.92rem;
    font-weight: 700;
    margin-top: 0.2rem;
}
.feed-card {
    background: linear-gradient(180deg, rgba(15,23,42,0.96) 0%, rgba(30,41,59,0.94) 100%);
    border: 1px solid rgba(255,255,255,0.06);
    border-radius: 16px;
    padding: 1rem 1.05rem;
    margin-bottom: 0.75rem;
}
.feed-card-head {
    display: flex;
    justify-content: space-between;
    gap: 12px;
    margin-bottom: 0.4rem;
}
.feed-card-type {
    font-size: 0.68rem;
    font-weight: 800;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: #22C55E;
}
.feed-card-time {
    color: #64748B;
    font-size: 0.74rem;
    font-family: monospace;
}
.feed-card-title {
    color: #F8FAFC;
    font-size: 0.95rem;
    font-weight: 750;
    line-height: 1.45;
}
.feed-card-summary {
    color: #CBD5E1;
    font-size: 0.82rem;
    margin-top: 0.35rem;
}
.feed-card-detail {
    color: #94A3B8;
    font-size: 0.78rem;
    margin-top: 0.28rem;
    line-height: 1.5;
}
.feed-chip-row {
    display: flex;
    flex-wrap: wrap;
    gap: 6px;
    margin-top: 0.7rem;
}
.feed-chip {
    background: rgba(255,255,255,0.05);
    border: 1px solid rgba(255,255,255,0.07);
    border-radius: 999px;
    color: #CBD5E1;
    font-size: 0.72rem;
    padding: 4px 9px;
}
.ticker-chip {
    background: rgba(34,197,94,0.12);
    color: #22C55E;
    padding: 4px 10px;
    border-radius: 8px;
    font-size: 0.8rem;
    font-weight: 700;
    font-family: monospace;
}
.ticker-chip-count {
    color: #94A3B8;
    margin-left: 6px;
    font-size: 0.72rem;
    font-weight: 600;
}

/* -- Signal Hero Cards ------------------------------------------------- */
.signal-hero {
    background: linear-gradient(135deg, #0F172A 0%, #1E293B 100%);
    border: 1px solid rgba(255,255,255,0.08);
    border-radius: 16px;
    padding: 1.25rem;
    margin-bottom: 0.75rem;
    position: relative;
    overflow: hidden;
}
.signal-hero-buy { border-left: 4px solid #22C55E; }
.signal-hero-sell { border-left: 4px solid #EF4444; }
.signal-hero-watch { border-left: 4px solid #F59E0B; }
.signal-hero-hold { border-left: 4px solid #3B82F6; }
.signal-hero-reject { border-left: 4px solid #94A3B8; }
.signal-hero .ticker-name {
    font-size: 1.1rem;
    font-weight: 800;
    color: #F8FAFC;
    margin-bottom: 2px;
}
.signal-hero .ticker-code {
    font-size: 0.75rem;
    color: #94A3B8;
    font-family: monospace;
}
.signal-hero .hero-price {
    font-size: 1.3rem;
    font-weight: 700;
    color: #F8FAFC;
    margin-top: 8px;
}
.signal-hero .hero-score {
    position: absolute;
    top: 1rem;
    right: 1.25rem;
    font-size: 1.5rem;
    font-weight: 800;
}
.signal-hero .hero-meta {
    font-size: 0.75rem;
    color: #94A3B8;
    margin-top: 6px;
}
.signal-hero .hero-reason {
    font-size: 0.8rem;
    color: #CBD5E1;
    margin-top: 8px;
    line-height: 1.4;
    border-top: 1px solid rgba(255,255,255,0.06);
    padding-top: 8px;
}

/* -- Expanders --------------------------------------------------------- */
.streamlit-expanderHeader {
    font-size: 1rem !important;
    min-height: 48px;
    display: flex;
    align-items: center;
    background: #0F172A !important;
    border-radius: 12px;
}

/* -- DataFrames -------------------------------------------------------- */
[data-testid="stDataFrame"] {
    overflow-x: auto;
    -webkit-overflow-scrolling: touch;
    border-radius: 12px;
}

/* -- Buttons/Inputs ---------------------------------------------------- */
.stButton > button,
.stTextInput > div > div > input,
.stSelectbox > div > div {
    min-height: 48px !important;
    font-size: 1rem !important;
    background: #1E293B !important;
    border: 1px solid rgba(255,255,255,0.08) !important;
    border-radius: 12px !important;
    color: #F8FAFC !important;
}

/* -- Hide sidebar toggle on mobile ------------------------------------- */
[data-testid="collapsedControl"] { display: none; }

/* -- Video card -------------------------------------------------------- */
.video-card {
    background: #1E293B;
    border: 1px solid rgba(255,255,255,0.06);
    border-radius: 14px;
    padding: 1rem;
    margin-bottom: 0.75rem;
}
.video-card-actionable { border-left: 3px solid #22C55E; }
.video-card-sector-only { border-left: 3px solid #60A5FA; }
.video-card-low-signal { border-left: 3px solid #F59E0B; }
.video-card-non-equity,
.video-card-unknown { border-left: 3px solid #94A3B8; }
.video-card-noise { border-left: 3px solid #EF4444; }
.video-card-title {
    font-size: 0.95rem;
    font-weight: 700;
    color: #F8FAFC;
    margin-bottom: 4px;
}
.video-card-meta {
    font-size: 0.8rem;
    color: #94A3B8;
}

/* -- Status timeline --------------------------------------------------- */
.status-row {
    display: flex;
    align-items: center;
    gap: 12px;
    padding: 8px 0;
    border-bottom: 1px solid rgba(255,255,255,0.04);
}
.status-dot {
    width: 8px; height: 8px;
    border-radius: 50%;
    background: #22C55E;
    flex-shrink: 0;
}
.status-time {
    font-size: 0.8rem;
    color: #94A3B8;
    min-width: 140px;
    font-family: monospace;
}
.status-label {
    font-size: 0.85rem;
    color: #F8FAFC;
}

/* -- Desktop enhancements ---------------------------------------------- */
@media (min-width: 768px) {
    [data-testid="stMetricValue"] { font-size: 2.4rem !important; }
    .omx-card { padding: 1.5rem; }
}
@media (min-width: 1024px) {
    .block-container { max-width: 1400px !important; }
}

/* -- Galaxy Z Fold7 cover screen (375px) ------------------------------- */
@media (max-width: 600px) {
    .hero-title {
        font-size: 1.35rem;
    }
    .hero-copy {
        font-size: 0.84rem;
    }
    .hero-shell {
        padding: 1rem;
    }
    .podium-card {
        min-height: auto;
    }
    /* KPI 값 축소 */
    [data-testid="stMetricValue"] {
        font-size: 1.4rem !important;
    }
    [data-testid="stMetricLabel"] {
        font-size: 0.65rem !important;
    }
    [data-testid="stMetricDelta"] {
        font-size: 0.75rem !important;
    }
    /* 블록 패딩 최소화 */
    .block-container {
        padding: 0.5rem 0.5rem 1.5rem !important;
    }
    /* 시그널 히어로 카드 — 단일 열로 스택 */
    .signal-hero {
        margin-bottom: 0.5rem;
    }
    .signal-hero .hero-score {
        font-size: 1.2rem;
    }
    /* 비디오 카드 그리드 — 단일 열 */
    .video-card {
        margin-bottom: 0.5rem;
    }
    /* 알럿 배너 — 텍스트 줄바꿈 */
    .omx-alert {
        padding: 0.75rem 1rem;
    }
    .omx-alert-title {
        font-size: 0.75rem;
    }
    /* 티커 칩 — 모바일 축소 */
    .ticker-chip {
        font-size: 0.7rem;
        padding: 3px 7px;
    }
    /* 탭 바 — 375px 오버플로 방지 */
    .stTabs [data-baseweb="tab-list"] {
        padding: 2px;
    }
    .stTabs [data-baseweb="tab"] {
        min-height: 44px;
        padding: 0 12px;
        font-size: 0.78rem !important;
    }
    /* 엑스펜더 콘텐츠 전체 너비 */
    [data-testid="stExpander"] > div {
        padding: 0.5rem !important;
    }
    /* 상태 타임라인 */
    .status-time {
        min-width: 100px;
        font-size: 0.7rem;
    }
    /* 버튼/입력 최소 높이 완화 */
    .stButton > button,
    .stTextInput > div > div > input,
    .stSelectbox > div > div {
        min-height: 44px !important;
        font-size: 0.9rem !important;
    }
}

/* -- 데이터프레임/테이블 수평 스크롤 래퍼 -------------------------------- */
[data-testid="stDataFrame"] {
    overflow-x: auto !important;
    -webkit-overflow-scrolling: touch;
}
[data-testid="stDataFrame"] table {
    font-size: 12px;
}
@media (max-width: 600px) {
    [data-testid="stDataFrame"] table {
        font-size: 11px;
    }
}

/* -- 채널 비교 테이블 수평 스크롤 ---------------------------------------- */
.comp-table-wrapper {
    overflow-x: auto;
    -webkit-overflow-scrolling: touch;
    border-radius: 8px;
}
</style>
""", unsafe_allow_html=True)

# -- Header with timestamp + NEW badge ----------------------------------------

OUTPUT_DIR = DEFAULT_OUTPUT_DIR
KST = timezone(timedelta(hours=9))

last_update = get_last_update_time(OUTPUT_DIR)
header_cols = st.columns([3, 1])
with header_cols[0]:
    st.title("Y2I 투자 시그널")
with header_cols[1]:
    if last_update:
        now_utc = datetime.now(tz=timezone.utc)
        age_min = (now_utc - last_update).total_seconds() / 60
        ts_str = last_update.astimezone(KST).strftime("%Y-%m-%d %H:%M KST")
        badge_html = ""
        if age_min < 5:
            badge_html = ' <span class="badge badge-new">신규</span>'
        st.markdown(
            f'<div style="text-align:right;padding-top:1.2rem;">'
            f'<span style="color:#94A3B8;font-size:0.8rem;">최종 업데이트</span><br>'
            f'<span style="color:#F8FAFC;font-size:0.9rem;font-weight:600;">{ts_str}</span>'
            f'{badge_html}</div>',
            unsafe_allow_html=True,
        )

# -- Load channels -----------------------------------------------------------

available_channels = get_available_channels(OUTPUT_DIR)
if not available_channels:
    st.warning("분석 데이터가 없습니다. 파이프라인을 먼저 실행하세요.")
    st.stop()

channel_names = get_channel_display_names(OUTPUT_DIR)
comparison_snapshot = load_channel_comparison(OUTPUT_DIR)
accuracy_snapshot = extract_signal_accuracy_summary(comparison_snapshot or {})
pipeline_summary = comparison_snapshot.get("pipeline_summary", {}) if isinstance(comparison_snapshot, dict) else {}
overall_accuracy_snapshot = accuracy_snapshot.get("overall", {}) if isinstance(accuracy_snapshot, dict) else {}

hero_hit_rate = overall_accuracy_snapshot.get("hit_rate_5d")
hero_hit_rate_text = "미제공" if hero_hit_rate is None else f"{float(hero_hit_rate):.1f}%"

freshness_text = "업데이트 정보 없음"
if last_update:
    age_minutes = int((datetime.now(tz=timezone.utc) - last_update).total_seconds() // 60)
    freshness_text = "방금 반영" if age_minutes < 5 else f"{age_minutes}분 전 반영"

hero_chips = [
    ("채널", str(len(available_channels))),
    ("분석 영상", str(pipeline_summary.get("total_videos", 0) or 0)),
    ("액션 시그널", str(pipeline_summary.get("strict_actionable_videos", 0) or 0)),
    ("추적 신호", str(overall_accuracy_snapshot.get("total_signals", 0) or 0)),
    ("5일 적중률", hero_hit_rate_text),
    ("갱신", freshness_text),
]
hero_chip_html = "".join(
    f'<span class="hero-chip">{label} <strong>{value}</strong></span>'
    for label, value in hero_chips
)
st.markdown(
    f'<div class="hero-shell">'
    f'<div class="hero-kicker">Signal Intelligence Dashboard</div>'
    f'<div class="hero-title">채널 성과, 시그널 추적, 실시간 분석 흐름을 한 화면에서 검증</div>'
    f'<div class="hero-copy">'
    f'채널별 적중률과 수익률, 개별 시그널의 가격 추적, 최근 분석 활동을 함께 보도록 대시보드를 정리했습니다. '
    f'상단 요약에서 전체 상태를 확인하고, 정확도 탭에서 채널 품질과 시그널 후속 흐름을 바로 검증할 수 있습니다.'
    f'</div>'
    f'<div class="hero-chip-row">{hero_chip_html}</div>'
    f'</div>',
    unsafe_allow_html=True,
)

# -- Plotly theme helper -----------------------------------------------------

PLOTLY_TEMPLATE = {
    "layout": {
        "paper_bgcolor": "rgba(0,0,0,0)",
        "plot_bgcolor": "rgba(15,23,42,0.5)",
        "font": {"color": "#F8FAFC", "size": 14},
        "xaxis": {"gridcolor": "rgba(255,255,255,0.06)", "zerolinecolor": "rgba(255,255,255,0.06)"},
        "yaxis": {"gridcolor": "rgba(255,255,255,0.06)", "zerolinecolor": "rgba(255,255,255,0.06)"},
        "colorway": ["#3B82F6", "#22C55E", "#F59E0B", "#EF4444", "#8B5CF6", "#06B6D4"],
    }
}

SIGNAL_COLORS = {
    "ACTIONABLE": "#22C55E",
    "SECTOR_ONLY": "#60A5FA",
    "LOW_SIGNAL": "#F59E0B",
    "NON_EQUITY": "#94A3B8",
    "NOISE": "#EF4444",
    "UNKNOWN": "#64748B",
}
SIGNAL_CLASS_KR = {
    "ACTIONABLE": "엄격 액션",
    "SECTOR_ONLY": "섹터 참고",
    "LOW_SIGNAL": "저신호",
    "NON_EQUITY": "비주식",
    "NOISE": "노이즈",
    "UNKNOWN": "미분류",
}
EMPTY_TEXT = "미제공"
REFERENCE_KIND_KR = {"published_at": "게시", "generated_at": "스냅샷", "unknown": "미분류"}

VERDICT_KR = {"BUY": "매수", "SELL": "매도", "HOLD": "보유", "WATCH": "관망", "REJECT": "제외"}
VERDICT_CSS = {"BUY": "buy", "SELL": "sell", "HOLD": "hold", "WATCH": "watch", "REJECT": "reject"}
DIRECTION_KR = {"UP": "상승", "DOWN": "하락", "NEUTRAL": "중립", "BULLISH": "강세", "BEARISH": "약세"}
VIDEO_TYPE_KR = {
    "STOCK_PICK": "종목 분석",
    "SECTOR": "섹터 분석",
    "MACRO": "매크로",
    "EXPERT_INTERVIEW": "전문가 인터뷰",
    "MARKET_REVIEW": "시황 리뷰",
    "OTHER": "기타",
}


def render_chart(fig: go.Figure, key: str | None = None, height: int = 300) -> None:
    """Render a Plotly chart with dark theme."""
    fig.update_layout(
        template=PLOTLY_TEMPLATE,
        margin=dict(l=8, r=8, t=32, b=8),
        font=dict(size=11),
        hovermode="x unified",
        hoverlabel=dict(bgcolor="#0F172A", bordercolor="rgba(255,255,255,0.12)", font=dict(color="#F8FAFC", size=11)),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=-0.28,
            xanchor="center",
            x=0.5,
            font=dict(size=11, color="#94A3B8"),
        ),
        height=height,
    )
    fig.update_xaxes(showline=False, tickfont=dict(color="#CBD5E1", size=11))
    fig.update_yaxes(showline=False, tickfont=dict(color="#CBD5E1", size=11))
    st.plotly_chart(fig, use_container_width=True, key=key)


def render_metrics_row(metrics: list[tuple[str, str]], cols_desktop: int = 4) -> None:
    col_count = min(len(metrics), cols_desktop)
    cols = st.columns(col_count)
    for i, (label, value) in enumerate(metrics):
        cols[i % col_count].metric(label, value)


def render_podium_cards(rows: list[dict[str, object]]) -> None:
    if not rows:
        return
    cols = st.columns(min(len(rows), 3))
    for idx, row in enumerate(rows[:3], start=1):
        with cols[idx - 1]:
            st.markdown(
                f'<div class="podium-card">'
                f'<div class="podium-rank podium-rank-{idx}">Top {idx}</div>'
                f'<div class="podium-name">{row.get("채널", EMPTY_TEXT)}</div>'
                f'<div class="podium-score">{float(row.get("종합 품질", 0) or 0):.1f}</div>'
                f'<div class="podium-meta">'
                f'<div class="podium-meta-item"><div class="podium-meta-label">5일 적중률</div><div class="podium-meta-value">{format_percent_metric(row.get("5일 적중률"))}</div></div>'
                f'<div class="podium-meta-item"><div class="podium-meta-label">5일 평균수익률</div><div class="podium-meta-value">{format_percent_metric(row.get("5일 평균수익률"), digits=2)}</div></div>'
                f'<div class="podium-meta-item"><div class="podium-meta-label">추적 신호</div><div class="podium-meta-value">{int(row.get("추적 신호", 0) or 0)}</div></div>'
                f'<div class="podium-meta-item"><div class="podium-meta-label">표본</div><div class="podium-meta-value">{int(row.get("5일 표본", 0) or 0)}</div></div>'
                f'</div>'
                f'</div>',
                unsafe_allow_html=True,
            )


def render_feed_card(event: dict[str, object]) -> None:
    event_type = str(event.get("event_type", "video_analysis"))
    event_label = "신호 업데이트" if event_type == "signal_update" else "영상 분석"
    timestamp = format_signal_datetime(str(event.get("timestamp", "")))
    signal_class = str(event.get("signal_class", "UNKNOWN"))
    score = float(event.get("score", 0) or 0)
    chips = [
        str(event.get("channel_display", EMPTY_TEXT)),
        translate_signal_class(signal_class),
        f"점수 {score:.1f}" if score else "점수 미제공",
    ]
    chip_html = "".join(f'<span class="feed-chip">{chip}</span>' for chip in chips if chip)
    st.markdown(
        f'<div class="feed-card">'
        f'<div class="feed-card-head">'
        f'<div class="feed-card-type">{event_label}</div>'
        f'<div class="feed-card-time">{timestamp}</div>'
        f'</div>'
        f'<div class="feed-card-title">{event.get("headline", EMPTY_TEXT)}</div>'
        f'<div class="feed-card-summary">{event.get("summary", EMPTY_TEXT)}</div>'
        f'<div class="feed-card-detail">{event.get("detail", EMPTY_TEXT) or EMPTY_TEXT}</div>'
        f'<div class="feed-chip-row">{chip_html}</div>'
        f'</div>',
        unsafe_allow_html=True,
    )


def signal_badge(signal_class: str) -> str:
    """Return HTML for a signal badge (Korean)."""
    badge_css = {
        "ACTIONABLE": "badge-actionable",
        "SECTOR_ONLY": "badge-sector-only",
        "LOW_SIGNAL": "badge-low-signal",
        "NON_EQUITY": "badge-non-equity",
        "UNKNOWN": "badge-unknown",
    }.get(signal_class, "badge-noise")
    label = SIGNAL_CLASS_KR.get(signal_class, signal_class or EMPTY_TEXT)
    return f'<span class="badge {badge_css}">{label}</span>'


def verdict_badge(verdict: str) -> str:
    """Return HTML for a verdict badge (Korean)."""
    kr = VERDICT_KR.get(verdict, verdict)
    css = VERDICT_CSS.get(verdict, "reject")
    return f'<span class="verdict-{css}">{kr}</span>'


def video_card_class(signal_class: str) -> str:
    return {
        "ACTIONABLE": "video-card-actionable",
        "SECTOR_ONLY": "video-card-sector-only",
        "LOW_SIGNAL": "video-card-low-signal",
        "NON_EQUITY": "video-card-non-equity",
        "UNKNOWN": "video-card-unknown",
    }.get(signal_class, "video-card-noise")


def parse_timestamp_string(value: str) -> datetime | None:
    if not value:
        return None

    normalized = value.strip()
    if not normalized:
        return None

    for fmt in ("%Y%m%dT%H%M%SZ", "%Y%m%d", "%Y-%m-%d"):
        try:
            parsed = datetime.strptime(normalized, fmt)
            return parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            continue

    iso_candidate = normalized.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(iso_candidate)
    except ValueError:
        return None

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def format_signal_date(date_str: str) -> str:
    """Format date string for display across compact and ISO timestamps."""
    parsed = parse_timestamp_string(date_str)
    if parsed is not None:
        return parsed.astimezone(KST).strftime("%Y-%m-%d")
    if not date_str:
        return EMPTY_TEXT
    return date_str


def format_signal_datetime(date_str: str) -> str:
    parsed = parse_timestamp_string(date_str)
    if parsed is not None:
        return parsed.astimezone(KST).strftime("%Y-%m-%d %H:%M")
    return EMPTY_TEXT if not date_str else date_str


def format_timestamp_local(dt: datetime | None, include_tz: bool = True) -> str:
    if dt is None:
        return EMPTY_TEXT
    fmt = "%Y-%m-%d %H:%M KST" if include_tz else "%Y-%m-%d %H:%M"
    return dt.astimezone(KST).strftime(fmt)


def first_non_empty(*values: str | None) -> str:
    for value in values:
        if value:
            return value
    return ""


def format_reference_display(value: str | None, kind: str | None) -> str:
    formatted = format_signal_datetime(value or "")
    if formatted == EMPTY_TEXT:
        return formatted
    label = REFERENCE_KIND_KR.get(kind or "unknown", kind or "미분류")
    return f"{formatted} ({label})"


def format_run_id_display(value: str | None) -> str:
    return format_signal_datetime(value or "")


def format_reference_timing(
    last_signal_at: str | None,
    first_signal_at: str | None,
    latest_checked_at: str | None,
) -> str:
    signal_value = first_non_empty(last_signal_at, first_signal_at)
    if signal_value:
        return f"신호 {format_signal_date(signal_value)}"
    if latest_checked_at:
        return f"확인 {format_signal_date(latest_checked_at)}"
    return EMPTY_TEXT


def translate_signal_class(signal_class: str) -> str:
    return SIGNAL_CLASS_KR.get(signal_class, signal_class or EMPTY_TEXT)


def translate_verdict(verdict: str) -> str:
    return VERDICT_KR.get(verdict, verdict or EMPTY_TEXT)


def translate_video_type(video_type: str) -> str:
    return VIDEO_TYPE_KR.get(video_type, video_type or EMPTY_TEXT)


def translate_direction(direction: str) -> str:
    return DIRECTION_KR.get(direction, direction or EMPTY_TEXT)


def format_percent_metric(value: float | int | None, digits: int = 1) -> str:
    if value is None:
        return EMPTY_TEXT
    if pd.isna(value):
        return EMPTY_TEXT
    return f"{float(value):.{digits}f}%"


# -- Build tabs ---------------------------------------------------------------

fixed_tabs = ["요약", "실시간 피드", "종목 랭킹", "정확도", "매크로", "전문가"]
channel_tab_labels = [channel_names.get(ch, ch) for ch in available_channels]
tab_labels = fixed_tabs + channel_tab_labels + ["채널 비교", "상태"]

tabs = st.tabs(tab_labels)

# =============================================================================
# TAB 0 — 요약 (핵심 시그널)
# =============================================================================

with tabs[0]:
    # -- 핵심 시그널 카드 (Top ranked stocks across all channels) --
    try:
        all_rankings = get_all_rankings(OUTPUT_DIR)
    except Exception as _e:
        st.error(f"종목 랭킹 로딩 오류: {_e}")
        all_rankings = []
    if all_rankings:
        st.markdown("#### 핵심 종목 시그널")
        hero_cols = st.columns(min(len(all_rankings), 3))
        for i, stock in enumerate(all_rankings[:6]):
            ticker = stock.get("ticker", "")
            display_name = format_ticker_display(ticker, stock.get("company_name", ""))
            code = ticker.replace(".KS", "").replace(".KQ", "")
            price_str = format_price(stock.get("latest_price"), stock.get("currency", "KRW"))
            score = stock.get("aggregate_score", 0)
            verdict = stock.get("aggregate_verdict", "WATCH")
            verdict_css = VERDICT_CSS.get(verdict, "reject")
            score_color = "#22C55E" if score >= 65 else "#F59E0B" if score >= 50 else "#94A3B8" if verdict == "REJECT" else "#EF4444"
            timing_display = format_reference_timing(
                stock.get("last_signal_at"),
                stock.get("first_signal_at"),
                stock.get("latest_checked_at"),
            )
            appearances = stock.get("appearances", 0)
            source_channels_display = stock.get("_source_channels_display", [])
            channel_count = stock.get("channel_count", 1)
            source_label = ", ".join(source_channels_display[:3]) if source_channels_display else channel_names.get(stock.get("_source_channel", ""), "")
            if channel_count > 3:
                source_label += f" 외 {channel_count - 3}개"

            with hero_cols[i % len(hero_cols)]:
                # Get Korean name part only
                kr_parts = display_name.split(" ", 1)
                name_part = kr_parts[1] if len(kr_parts) > 1 else display_name

                st.markdown(
                    f'<div class="signal-hero signal-hero-{verdict_css}">'
                    f'<div class="ticker-name">{name_part}</div>'
                    f'<div class="ticker-code">{code} &middot; {verdict_badge(verdict)}</div>'
                    f'<div class="hero-price">{price_str}</div>'
                    f'<div class="hero-score" style="color:{score_color};">{score:.0f}</div>'
                    f'<div class="hero-meta">'
                    f'{channel_count}개 채널 &middot; {appearances}회 출현 &middot; {timing_display}'
                    f'</div>'
                    f'<div style="font-size:0.7rem;color:#64748B;margin-top:2px;">{source_label}</div>'
                    f'</div>',
                    unsafe_allow_html=True,
                )
    else:
        st.info("분석된 종목이 없습니다.")

    st.markdown("---")

    # -- Actionable Signal Alert Banner --
    try:
        actionable = extract_actionable_signals(OUTPUT_DIR)
    except Exception as _e:
        st.error(f"액션 시그널 로딩 오류: {_e}")
        actionable = []
    if actionable:
        ticker_stats: dict[str, dict[str, float | int]] = {}
        for sig in actionable:
            score = float(sig.get("signal_score", 0) or 0)
            for ticker in sig.get("tickers", []):
                if not ticker:
                    continue
                if ticker not in ticker_stats:
                    ticker_stats[ticker] = {"count": 0, "best_score": score}
                ticker_stats[ticker]["count"] = int(ticker_stats[ticker]["count"]) + 1
                ticker_stats[ticker]["best_score"] = max(float(ticker_stats[ticker]["best_score"]), score)

        ranked_tickers = sorted(
            ticker_stats.items(),
            key=lambda item: (
                int(item[1]["count"]),
                float(item[1]["best_score"]),
                item[0],
            ),
            reverse=True,
        )
        if ranked_tickers:
            ticker_chips = "".join(
                f'<span class="ticker-chip">{format_ticker_display(ticker)}'
                f'<span class="ticker-chip-count">{int(stats["count"])}회</span></span>'
                for ticker, stats in ranked_tickers[:20]
            )
            st.markdown(
                f'<div class="omx-alert">'
                f'<div class="omx-alert-title">액션 가능 시그널 상위 종목 ({len(actionable)}개 영상)</div>'
                f'<div class="omx-alert-tickers">{ticker_chips}</div>'
                f'</div>',
                unsafe_allow_html=True,
            )

    # -- 최근 분석 (24h) --
    try:
        recent_videos = get_recent_videos(OUTPUT_DIR, hours=24)
    except Exception as _e:
        st.error(f"최근 영상 로딩 오류: {_e}")
        recent_videos = []
    if recent_videos:
        st.markdown("#### 최근 분석 (24시간)")
        recent_sort = st.selectbox(
            "정렬 기준",
            ["최근 반영", "게시일", "점수"],
            key="recent_sort",
        )
        if recent_sort == "게시일":
            sorted_recent_videos = sorted(
                recent_videos,
                key=lambda rv: (
                    parse_timestamp_string(rv.get("published_at", "") or "") or datetime.min.replace(tzinfo=timezone.utc),
                    rv.get("_updated_at") or datetime.min.replace(tzinfo=timezone.utc),
                    rv.get("signal_score", 0),
                    rv.get("title", ""),
                ),
                reverse=True,
            )
        elif recent_sort == "점수":
            sorted_recent_videos = sorted(
                recent_videos,
                key=lambda rv: (
                    rv.get("signal_score", 0),
                    rv.get("_updated_at") or datetime.min.replace(tzinfo=timezone.utc),
                    parse_timestamp_string(rv.get("published_at", "") or "") or datetime.min.replace(tzinfo=timezone.utc),
                    rv.get("title", ""),
                ),
                reverse=True,
            )
        else:
            sorted_recent_videos = list(recent_videos)

        rcols = st.columns(min(len(sorted_recent_videos), 3))
        for i, rv in enumerate(sorted_recent_videos[:6]):
            sig_cls = rv.get("video_signal_class", "UNKNOWN")
            card_class = video_card_class(sig_cls)
            score = rv.get("signal_score", 0)
            pub_date = format_signal_date(rv.get("published_at", ""))
            updated_at = format_timestamp_local(rv.get("_updated_at"), include_tz=False)
            ch_name = channel_names.get(rv.get("_channel", ""), rv.get("_channel", ""))
            reason = rv.get("skip_reason") or rv.get("reason", "")
            title_full = rv.get("title", "제목 없음")
            title_short = title_full[:60] + ("..." if len(title_full) > 60 else "")
            with rcols[i % len(rcols)]:
                reason_html = ""
                if reason:
                    reason_preview = reason[:100] + ("..." if len(reason) > 100 else "")
                    reason_html = f'<div style="font-size:0.75rem;color:#CBD5E1;margin-top:4px;" title="{reason}">{reason_preview}</div>'
                st.markdown(
                    f'<div class="video-card {card_class}">'
                    f'<div class="video-card-title" title="{title_full}">{title_short}</div>'
                    f'<div class="video-card-meta">'
                    f'{ch_name} &middot; 점수: {score:.0f} &middot; 게시 {pub_date} &middot; 반영 {updated_at} &middot; '
                    f'{signal_badge(sig_cls)}'
                    f'</div>'
                    f'{reason_html}'
                    f'</div>',
                    unsafe_allow_html=True,
                )

    st.markdown("---")

    # -- 파이프라인 요약 KPIs --
    try:
        report = build_overview_report(OUTPUT_DIR)
    except Exception as _e:
        st.error(f"파이프라인 요약 로딩 오류: {_e}")
        report = None
    if report:
        st.markdown("#### 파이프라인 요약")
        render_metrics_row([
            ("분석 영상", str(report.get("total_videos", 0))),
            ("분석 가능", str(report.get("analyzable_count", 0))),
            ("전문가 포함 영상", str(report.get("expert_video_count", 0))),
            ("매크로 포함 영상", str(report.get("macro_video_count", 0))),
        ], cols_desktop=4)

        col_a, col_b = st.columns(2)
        with col_a:
            type_dist = extract_type_distribution(report)
            if type_dist:
                df_type = pd.DataFrame(
                    [
                        {"유형": translate_video_type(label), "건수": count}
                        for label, count in type_dist.items()
                    ]
                ).sort_values("건수", ascending=True)
                df_type["비중"] = (df_type["건수"] / df_type["건수"].sum()).map(lambda x: f"{x:.0%}")
                fig_type = px.bar(
                    df_type,
                    x="건수",
                    y="유형",
                    orientation="h",
                    title="영상 유형 분포",
                    text="비중",
                    color="유형",
                    color_discrete_sequence=["#1D4ED8", "#3B82F6", "#06B6D4", "#22C55E", "#F59E0B", "#94A3B8"],
                )
                fig_type.update_traces(textposition="outside", hovertemplate="%{y}: %{x}개 (%{text})<extra></extra>")
                fig_type.update_layout(showlegend=False)
                render_chart(fig_type, key="overview_type", height=300)

        with col_b:
            sig_dist = extract_signal_distribution(report)
            if sig_dist:
                df_sig = pd.DataFrame(
                    {
                        "시그널": [translate_signal_class(k) for k in sig_dist.keys()],
                        "건수": list(sig_dist.values()),
                    }
                ).sort_values("건수", ascending=True)
                df_sig["비중"] = (df_sig["건수"] / df_sig["건수"].sum()).map(lambda x: f"{x:.0%}")
                color_map = {translate_signal_class(k): v for k, v in SIGNAL_COLORS.items()}
                fig_bar = px.bar(
                    df_sig,
                    x="건수",
                    y="시그널",
                    orientation="h",
                    title="시그널 분포",
                    color="시그널",
                    color_discrete_map=color_map,
                    text="비중",
                )
                fig_bar.update_traces(
                    textposition="outside",
                    hovertemplate="%{y}: %{x}개 (%{text})<extra></extra>",
                )
                render_chart(fig_bar, key="overview_signal", height=300)

        with st.expander("영상별 상세", expanded=False):
            per_video = extract_per_video(report)
            if per_video:
                df_pv = pd.DataFrame(per_video)
                col_map = {
                    "channel": "채널",
                    "title": "제목",
                    "video_type": "유형",
                    "signal_class": "시그널",
                    "signal_score": "점수",
                    "published_at": "게시일",
                }
                mobile_cols = [c for c in ["channel", "title", "video_type", "signal_class", "signal_score", "published_at"] if c in df_pv.columns]
                display_df = df_pv[mobile_cols].rename(columns=col_map) if mobile_cols else df_pv
                if "유형" in display_df.columns:
                    display_df["유형"] = display_df["유형"].map(translate_video_type)
                if "시그널" in display_df.columns:
                    display_df["시그널"] = display_df["시그널"].map(translate_signal_class)
                if "게시일" in display_df.columns:
                    display_df["게시일"] = display_df["게시일"].map(format_signal_date)
                video_filter = st.text_input("영상 검색 (채널 / 제목 / 유형 / 시그널)", "", key="overview_video_filter")
                total_videos_count = len(display_df)
                if video_filter:
                    search_index = display_df.fillna("").astype(str).agg(" ".join, axis=1)
                    display_df = display_df[
                        search_index.str.upper().str.contains(video_filter.upper(), regex=False, na=False)
                    ]
                st.caption(f"표시 영상 {len(display_df)}개 / 전체 {total_videos_count}개")
                if display_df.empty:
                    st.info("검색 조건에 맞는 영상이 없습니다.")
                else:
                    st.dataframe(display_df, use_container_width=True, height=400)

    # -- 콘텐츠 라벨 --
    titles_data = load_all_video_titles(OUTPUT_DIR)
    if titles_data and "titles" in titles_data:
        all_labels: list[str] = []
        for t in titles_data["titles"]:
            all_labels.extend(t.get("labels", []))
        if all_labels:
            with st.expander("콘텐츠 라벨", expanded=False):
                label_counts = pd.Series(all_labels).value_counts().head(12).reset_index()
                label_counts.columns = ["라벨", "건수"]
                fig_labels = px.bar(
                    label_counts,
                    x="건수",
                    y="라벨",
                    orientation="h",
                    title="콘텐츠 라벨 분포",
                    text="건수",
                    color="건수",
                    color_continuous_scale=["#1D4ED8", "#06B6D4"],
                )
                fig_labels.update_traces(textposition="outside", hovertemplate="%{y}: %{x}개<extra></extra>")
                fig_labels.update_layout(coloraxis_showscale=False)
                render_chart(fig_labels, key="overview_labels", height=300)

# =============================================================================
# TAB 1 — 실시간 피드
# =============================================================================

with tabs[1]:
    st.markdown("#### 실시간 피드")
    try:
        live_data = get_live_feed_data(OUTPUT_DIR, hours=48)
    except Exception as _e:
        st.error(f"실시간 피드 로딩 오류: {_e}")
        live_data = {"recent_videos": [], "recent_signals": [], "feed_events": [], "last_update": None}

    if live_data.get("last_update"):
        st.caption(f"마지막 업데이트: {format_signal_datetime(str(live_data['last_update']))}")

    feed_events = live_data.get("feed_events", [])
    live_signals = live_data.get("recent_signals", [])
    live_videos = live_data.get("recent_videos", [])

    render_metrics_row([
        ("피드 이벤트", str(len(feed_events))),
        ("최근 분석 영상", str(len(live_videos))),
        ("추적 시그널", str(len(live_signals))),
    ], cols_desktop=3)

    feed_controls = st.columns([1, 1])
    with feed_controls[0]:
        feed_type = st.selectbox("이벤트 유형", ["전체", "영상 분석", "신호 업데이트"], key="feed_type")
    with feed_controls[1]:
        feed_channel_options = ["전체"] + sorted({str(item.get("channel_display", "")) for item in feed_events if item.get("channel_display")})
        feed_channel = st.selectbox("채널 필터", feed_channel_options, key="feed_channel")

    filtered_events = []
    for event in feed_events:
        event_type = event.get("event_type", "")
        if feed_type == "영상 분석" and event_type != "video_analysis":
            continue
        if feed_type == "신호 업데이트" and event_type != "signal_update":
            continue
        if feed_channel != "전체" and event.get("channel_display") != feed_channel:
            continue
        filtered_events.append(event)

    if filtered_events:
        st.markdown('<div class="section-kicker">Unified Feed</div>', unsafe_allow_html=True)
        for event in filtered_events:
            render_feed_card(event)
    else:
        st.info("선택한 조건에 맞는 실시간 이벤트가 없습니다.")

    with st.expander("원본 리스트 보기", expanded=False):
        raw_col_a, raw_col_b = st.columns(2)
        with raw_col_a:
            st.markdown("##### 최근 추적 시그널")
            if live_signals:
                recent_rows = []
                for sig in live_signals[:12]:
                    recent_rows.append(
                        {
                            "채널": channel_names.get(sig.get("channel_slug", ""), sig.get("channel_slug", EMPTY_TEXT)),
                            "종목": format_ticker_display(sig.get("ticker", ""), sig.get("company_name", "")),
                            "판단": translate_verdict(sig.get("verdict", "")),
                            "신호일": format_signal_date(sig.get("signal_date", "")),
                            "5일": format_percent_metric((sig.get("returns", {}) or {}).get("5d"), digits=2),
                            "10일": format_percent_metric((sig.get("returns", {}) or {}).get("10d"), digits=2),
                        }
                    )
                st.dataframe(pd.DataFrame(recent_rows), use_container_width=True, hide_index=True)
            else:
                st.info("추적 시그널이 없습니다.")

        with raw_col_b:
            st.markdown("##### 최근 분석 영상")
            if live_videos:
                recent_video_rows = []
                for vid in live_videos[:16]:
                    recent_video_rows.append(
                        {
                            "채널": channel_names.get(vid.get("_channel", ""), vid.get("_channel", EMPTY_TEXT)),
                            "제목": vid.get("title", EMPTY_TEXT),
                            "시그널": translate_signal_class(vid.get("video_signal_class", "UNKNOWN")),
                            "점수": round(float(vid.get("signal_score", 0) or 0), 1),
                            "게시일": format_signal_date(vid.get("published_at", "")),
                        }
                    )
                st.dataframe(pd.DataFrame(recent_video_rows), use_container_width=True, hide_index=True)
            else:
                st.info("최근 48시간 내 분석된 영상이 없습니다.")

# =============================================================================
# TAB 2 — 종목 랭킹
# =============================================================================

with tabs[2]:
    st.markdown("#### 종목 랭킹")
    rank_ch_options = ["전체 (통합)"] + [channel_names.get(ch, ch) for ch in available_channels]
    rank_ch_idx = st.selectbox("채널 선택", rank_ch_options, key="rank_ch")

    try:
        if rank_ch_idx == "전체 (통합)":
            ranking = get_all_rankings(OUTPUT_DIR)
        else:
            # Find the slug for the selected display name
            slug_map = {channel_names.get(ch, ch): ch for ch in available_channels}
            selected_slug = slug_map.get(rank_ch_idx, "")
            data_30d = load_30d_results(selected_slug, OUTPUT_DIR)
            ranking = extract_cross_video_ranking(data_30d)
    except Exception as _e:
        st.error(f"랭킹 데이터 로딩 오류: {_e}")
        ranking = []

    if ranking:
        filter_text = st.text_input("종목 검색 (코드 / 회사명)", "", key="rank_filter")
        rank_sort = st.selectbox(
            "정렬 기준",
            ["점수", "출현", "최근 시점"],
            key="rank_sort",
        )

        # Build display dataframe
        rows = []
        for i, item in enumerate(ranking):
            ticker = item.get("ticker", "")
            display = format_ticker_display(ticker, item.get("company_name", ""))
            score = item.get("aggregate_score", item.get("total_score", 0))
            verdict = item.get("aggregate_verdict", item.get("final_verdict", ""))
            verdict_label = translate_verdict(verdict)
            price = item.get("latest_price")
            currency = item.get("currency", "KRW")
            timing_display = format_reference_timing(
                item.get("last_signal_at"),
                item.get("first_signal_at"),
                item.get("latest_checked_at"),
            )
            appearances = item.get("appearances", item.get("mention_count", 0))
            checked_at = format_signal_datetime(item.get("latest_checked_at", ""))
            price_label = format_price(price, currency)
            timing_raw = first_non_empty(
                item.get("last_signal_at"),
                item.get("first_signal_at"),
                item.get("latest_checked_at"),
            )

            rows.append({
                "순위": i + 1,
                "종목": display,
                "점수": round(score, 1),
                "판단": verdict_label,
                "최근 시점": timing_display,
                "현재가": price_label,
                "출현": appearances,
                "가격확인": checked_at,
                "_검색": " ".join(
                    str(value)
                    for value in [
                        ticker,
                        display,
                        verdict,
                        verdict_label,
                        timing_display,
                        checked_at,
                        price_label,
                    ]
                ),
                "_점수": round(score, 1),
                "_출현": appearances,
                "_최근시점": parse_timestamp_string(timing_raw) or datetime.min.replace(tzinfo=timezone.utc),
            })

        df_rank = pd.DataFrame(rows)

        if filter_text:
            mask = df_rank["_검색"].str.upper().str.contains(filter_text.upper(), regex=False, na=False)
            df_rank = df_rank[mask]

        sort_columns = {
            "점수": ["_점수", "_출현", "종목"],
            "출현": ["_출현", "_점수", "종목"],
            "최근 시점": ["_최근시점", "_점수", "종목"],
        }
        df_rank = df_rank.sort_values(sort_columns[rank_sort], ascending=[False, False, True])

        render_metrics_row([
            ("전체 종목", str(len(rows))),
            ("현재 표시", str(len(df_rank))),
        ], cols_desktop=2)

        display_rank = df_rank.drop(columns=["_검색", "_점수", "_출현", "_최근시점"], errors="ignore")
        st.dataframe(display_rank, use_container_width=True, height=500, hide_index=True)

        # Confidence visualization below table
        if display_rank.empty:
            st.info("검색 조건에 맞는 종목이 없습니다.")
        else:
            st.markdown("##### 상위 종목 점수 분포")
            df_top_rank = pd.DataFrame({
                "종목": df_rank["종목"].astype(str).str.slice(0, 26),
                "점수": df_rank["_점수"],
                "판단": df_rank["판단"],
            }).head(12).sort_values("점수", ascending=True)
            verdict_colors = {
                translate_verdict("BUY"): "#22C55E",
                translate_verdict("WATCH"): "#F59E0B",
                translate_verdict("HOLD"): "#3B82F6",
                translate_verdict("SELL"): "#EF4444",
                translate_verdict("REJECT"): "#94A3B8",
            }
            fig_rank = px.bar(
                df_top_rank,
                x="점수",
                y="종목",
                orientation="h",
                color="판단",
                title="상위 종목 점수 비교",
                text="점수",
                color_discrete_map=verdict_colors,
            )
            fig_rank.update_traces(textposition="outside", hovertemplate="%{y}: %{x}점<extra>%{fullData.name}</extra>")
            render_chart(fig_rank, key="rank_scores", height=320)
    else:
        st.info("랭킹 데이터가 없습니다.")

# =============================================================================
# TAB 3 — 정확도
# =============================================================================

with tabs[3]:
    st.markdown("#### 시그널 정확도")
    try:
        accuracy_comp = load_channel_comparison(OUTPUT_DIR)
    except Exception as _e:
        st.error(f"정확도 데이터 로딩 오류: {_e}")
        accuracy_comp = {}

    accuracy_summary = extract_signal_accuracy_summary(accuracy_comp or {})
    overall_accuracy = accuracy_summary.get("overall", {}) if isinstance(accuracy_summary, dict) else {}
    channel_leaderboard = extract_channel_leaderboard(accuracy_comp or {})
    recent_tracked_signals = extract_recent_tracked_signals(accuracy_comp or {})

    if overall_accuracy:
        render_metrics_row([
            ("추적 신호", str(overall_accuracy.get("total_signals", 0))),
            ("5일 표본", str(overall_accuracy.get("signals_with_price", 0))),
            ("5일 적중률", format_percent_metric(overall_accuracy.get("hit_rate_5d"))),
            ("10일 적중률", format_percent_metric(overall_accuracy.get("hit_rate_10d"))),
            ("5일 평균수익", format_percent_metric(overall_accuracy.get("avg_return_5d"), digits=2)),
            (
                "평균 점수",
                f"{overall_accuracy.get('avg_signal_score', 0):.1f}"
                if overall_accuracy.get("avg_signal_score") is not None
                else EMPTY_TEXT,
            ),
        ], cols_desktop=6)

        if channel_leaderboard:
            st.markdown('<div class="section-kicker">Leaderboard Leaders</div>', unsafe_allow_html=True)
            leaderboard_preview = []
            by_channel_preview = accuracy_summary.get("by_channel", {}) if isinstance(accuracy_summary, dict) else {}
            for item in channel_leaderboard[:3]:
                channel_accuracy = by_channel_preview.get(item.get("slug", ""), {})
                leaderboard_preview.append(
                    {
                        "채널": item.get("display_name", item.get("slug", EMPTY_TEXT)),
                        "종합 품질": float(item.get("overall_quality_score", 0) or 0),
                        "5일 적중률": item.get("hit_rate_5d"),
                        "5일 평균수익률": item.get("avg_return_5d"),
                        "추적 신호": int(channel_accuracy.get("total_signals", 0) or 0),
                        "5일 표본": int(channel_accuracy.get("signals_with_price", 0) or 0),
                    }
                )
            render_podium_cards(leaderboard_preview)

        window_stats = overall_accuracy.get("window_stats", {})
        df_windows = pd.DataFrame(
            [
                {
                    "윈도우": key,
                    "표본": int((window_stats.get(key, {}) or {}).get("tracked", 0) or 0),
                    "커버리지": float((window_stats.get(key, {}) or {}).get("coverage_pct", 0) or 0),
                    "적중률": (window_stats.get(key, {}) or {}).get("hit_rate"),
                    "평균수익률": (window_stats.get(key, {}) or {}).get("avg_return"),
                }
                for key in ["1d", "3d", "5d", "10d", "20d"]
            ]
        )

        chart_col_a, chart_col_b = st.columns(2)
        with chart_col_a:
            df_hit = df_windows[df_windows["적중률"].notna()].copy()
            if not df_hit.empty:
                fig_hit = px.bar(
                    df_hit,
                    x="윈도우",
                    y="적중률",
                    text="적중률",
                    title="윈도우별 적중률",
                    color="적중률",
                    color_continuous_scale=["#1D4ED8", "#22C55E"],
                )
                fig_hit.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
                fig_hit.update_layout(coloraxis_showscale=False)
                fig_hit.update_yaxes(title="적중률 (%)", range=[0, 100])
                render_chart(fig_hit, key="accuracy_hit_rate", height=320)

        with chart_col_b:
            df_return = df_windows[df_windows["평균수익률"].notna()].copy()
            if not df_return.empty:
                fig_return = px.bar(
                    df_return,
                    x="윈도우",
                    y="평균수익률",
                    text="평균수익률",
                    title="윈도우별 평균수익률",
                    color="평균수익률",
                    color_continuous_scale=["#EF4444", "#94A3B8", "#22C55E"],
                )
                fig_return.update_traces(texttemplate="%{text:.2f}%", textposition="outside")
                fig_return.update_layout(coloraxis_showscale=False)
                fig_return.update_yaxes(title="수익률 (%)", zeroline=True, zerolinecolor="rgba(255,255,255,0.18)")
                render_chart(fig_return, key="accuracy_returns", height=320)

        st.markdown("##### 채널 적중률 리더보드")
        if channel_leaderboard:
            leaderboard_sort = st.selectbox(
                "정렬 기준",
                ["종합 품질", "5일 적중률", "10일 적중률", "5일 평균수익률", "추적 신호"],
                key="accuracy_leaderboard_sort",
            )
            by_channel = accuracy_summary.get("by_channel", {}) if isinstance(accuracy_summary, dict) else {}
            leaderboard_rows = []
            for idx, item in enumerate(channel_leaderboard, start=1):
                channel_accuracy = by_channel.get(item.get("slug", ""), {})
                leaderboard_rows.append({
                    "순위": idx,
                    "채널": item.get("display_name", item.get("slug", EMPTY_TEXT)),
                    "종합 품질": float(item.get("overall_quality_score", 0) or 0),
                    "5일 적중률": item.get("hit_rate_5d"),
                    "10일 적중률": item.get("hit_rate_10d"),
                    "5일 평균수익률": item.get("avg_return_5d"),
                    "10일 평균수익률": item.get("avg_return_10d"),
                    "추적 신호": int(channel_accuracy.get("total_signals", 0) or 0),
                    "5일 표본": int(channel_accuracy.get("signals_with_price", 0) or 0),
                    "분석 가능 비율": f"{float(item.get('actionable_ratio', 0) or 0):.0%}",
                })
            df_leaderboard = pd.DataFrame(leaderboard_rows)
            sort_map = {
                "종합 품질": ["종합 품질", "5일 적중률", "채널"],
                "5일 적중률": ["5일 적중률", "종합 품질", "채널"],
                "10일 적중률": ["10일 적중률", "종합 품질", "채널"],
                "5일 평균수익률": ["5일 평균수익률", "종합 품질", "채널"],
                "추적 신호": ["추적 신호", "종합 품질", "채널"],
            }
            df_leaderboard = df_leaderboard.sort_values(sort_map[leaderboard_sort], ascending=[False, False, True], na_position="last")

            display_leaderboard = df_leaderboard.copy()
            for col in ["5일 적중률", "10일 적중률"]:
                display_leaderboard[col] = display_leaderboard[col].map(format_percent_metric)
            for col in ["5일 평균수익률", "10일 평균수익률"]:
                display_leaderboard[col] = display_leaderboard[col].map(lambda value: format_percent_metric(value, digits=2))
            display_leaderboard["종합 품질"] = display_leaderboard["종합 품질"].map(lambda value: f"{value:.1f}")
            st.dataframe(display_leaderboard, use_container_width=True, hide_index=True)

            df_quality_chart = (
                df_leaderboard[["채널", "종합 품질", "5일 적중률"]]
                .copy()
                .head(10)
                .sort_values("종합 품질", ascending=True)
            )
            fig_quality = px.bar(
                df_quality_chart,
                x="종합 품질",
                y="채널",
                orientation="h",
                text="5일 적중률",
                color="5일 적중률",
                title="채널 종합 품질 / 5일 적중률",
                color_continuous_scale=["#1D4ED8", "#22C55E"],
            )
            if df_quality_chart["5일 적중률"].notna().any():
                fig_quality.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
            else:
                fig_quality.update_traces(textposition="none")
            fig_quality.update_layout(coloraxis_showscale=False)
            render_chart(fig_quality, key="accuracy_leaderboard_chart", height=360)

            scatter_rows = df_leaderboard.dropna(subset=["5일 적중률", "5일 평균수익률"]).copy()
            if not scatter_rows.empty:
                st.markdown("##### 채널 성과 포지셔닝")
                fig_scatter = px.scatter(
                    scatter_rows,
                    x="5일 적중률",
                    y="5일 평균수익률",
                    size="추적 신호",
                    color="종합 품질",
                    hover_name="채널",
                    text="채널",
                    color_continuous_scale=["#1D4ED8", "#06B6D4", "#22C55E"],
                )
                fig_scatter.update_traces(
                    textposition="top center",
                    marker=dict(line=dict(color="rgba(248,250,252,0.15)", width=1)),
                    hovertemplate="%{hovertext}<br>5일 적중률 %{x:.1f}%<br>5일 평균수익률 %{y:.2f}%<br>추적 신호 %{marker.size:.0f}<extra></extra>",
                )
                fig_scatter.update_xaxes(title="5일 적중률 (%)", range=[0, 100])
                fig_scatter.update_yaxes(title="5일 평균수익률 (%)", zeroline=True, zerolinecolor="rgba(255,255,255,0.18)")
                render_chart(fig_scatter, key="accuracy_positioning", height=360)
        else:
            st.info("채널 리더보드 데이터가 없습니다.")

        signal_chart_records = get_signal_chart_records(OUTPUT_DIR)
        st.markdown("##### 시그널 → 주가 추적")
        if signal_chart_records:
            selected_signal_label = st.selectbox(
                "추적 시그널 선택",
                [
                    f"{item['channel_display']} · {item['ticker_display']} · {format_signal_date(item.get('signal_date', ''))}"
                    for item in signal_chart_records
                ],
                key="accuracy_signal_chart_picker",
            )
            selected_signal = next(
                (
                    item
                    for item in signal_chart_records
                    if f"{item['channel_display']} · {item['ticker_display']} · {format_signal_date(item.get('signal_date', ''))}" == selected_signal_label
                ),
                signal_chart_records[0],
            )
            selected_timeline = selected_signal.get("timeline", [])
            if selected_timeline:
                df_timeline = pd.DataFrame(selected_timeline)
                baseline_close = float(df_timeline.iloc[0]["close"]) if not df_timeline.empty else 0.0
                if baseline_close > 0:
                    df_timeline["기준가 지수"] = df_timeline["close"].astype(float) / baseline_close * 100.0
                else:
                    df_timeline["기준가 지수"] = 100.0
                df_timeline["날짜"] = pd.to_datetime(df_timeline["date"])
                df_timeline["수익률"] = pd.to_numeric(df_timeline["return_pct"], errors="coerce")
                line_color = "#22C55E" if (selected_signal.get("latest_return_pct") or 0) >= 0 else "#EF4444"

                selected_metrics = st.columns(5)
                selected_metrics[0].metric("채널", str(selected_signal.get("channel_display", EMPTY_TEXT)))
                selected_metrics[1].metric("판단", translate_verdict(str(selected_signal.get("verdict", ""))))
                selected_metrics[2].metric("신호 점수", f"{float(selected_signal.get('signal_score', 0) or 0):.1f}")
                selected_metrics[3].metric("진입가", format_price(selected_signal.get("entry_price")))
                selected_metrics[4].metric("최신 수익률", format_percent_metric(selected_signal.get("latest_return_pct"), digits=2))

                fig_signal = go.Figure()
                fig_signal.add_trace(
                    go.Scatter(
                        x=df_timeline["날짜"],
                        y=df_timeline["기준가 지수"],
                        mode="lines+markers",
                        name="기준가 지수",
                        line=dict(color=line_color, width=3),
                        marker=dict(size=7, color=line_color),
                        customdata=df_timeline[["close", "수익률"]].values,
                        hovertemplate="날짜 %{x|%Y-%m-%d}<br>기준가 지수 %{y:.2f}<br>종가 %{customdata[0]:,.2f}<br>수익률 %{customdata[1]:+.2f}%<extra></extra>",
                    )
                )
                fig_signal.add_hline(y=100, line_dash="dot", line_color="rgba(255,255,255,0.24)")
                fig_signal.update_yaxes(title="기준가 지수 (진입가=100)")
                fig_signal.update_xaxes(title="날짜")
                render_chart(fig_signal, key="signal_tracking_chart", height=360)

                timeline_table = df_timeline[["date", "close", "수익률"]].copy()
                timeline_table.columns = ["날짜", "종가", "수익률"]
                timeline_table["날짜"] = timeline_table["날짜"].map(format_signal_date)
                timeline_table["종가"] = timeline_table["종가"].map(lambda value: format_price(float(value)))
                timeline_table["수익률"] = timeline_table["수익률"].map(lambda value: format_percent_metric(value, digits=2))
                st.dataframe(timeline_table, use_container_width=True, hide_index=True)
        else:
            st.info("차트로 볼 수 있는 추적 시그널이 없습니다.")

        with st.expander("최근 추적 신호", expanded=False):
            if recent_tracked_signals:
                recent_rows = []
                for item in recent_tracked_signals:
                    recent_rows.append({
                        "채널": channel_names.get(item.get("channel_slug", ""), item.get("channel_slug", EMPTY_TEXT)),
                        "종목": format_ticker_display(item.get("ticker", ""), item.get("company_name", "")),
                        "시그널일": format_signal_date(item.get("signal_date", "")),
                        "진입일": format_signal_date(item.get("entry_date", "")),
                        "판단": translate_verdict(item.get("verdict", "")),
                        "점수": round(float(item.get("signal_score", 0) or 0), 1),
                        "1일": format_percent_metric((item.get("returns", {}) or {}).get("1d"), digits=2),
                        "3일": format_percent_metric((item.get("returns", {}) or {}).get("3d"), digits=2),
                        "5일": format_percent_metric((item.get("returns", {}) or {}).get("5d"), digits=2),
                        "10일": format_percent_metric((item.get("returns", {}) or {}).get("10d"), digits=2),
                        "20일": format_percent_metric((item.get("returns", {}) or {}).get("20d"), digits=2),
                    })
                st.dataframe(pd.DataFrame(recent_rows), use_container_width=True, hide_index=True)
            else:
                st.info("최근 추적 신호가 없습니다.")
    else:
        st.info("시그널 정확도 데이터가 없습니다.")

# =============================================================================
# TAB 4 — 매크로
# =============================================================================

with tabs[4]:
    st.markdown("#### 매크로 시그널")
    macro_ch_options = ["전체 (통합)"] + [channel_names.get(ch, ch) for ch in available_channels]
    macro_ch_display = st.selectbox("채널 선택", macro_ch_options, key="macro_ch")
    macro_slug_map = {channel_names.get(ch, ch): ch for ch in available_channels}

    try:
        if macro_ch_display == "전체 (통합)":
            # Aggregate macro signals from all channels
            macro_signals = []
            for _slug in available_channels:
                _data = load_30d_results(_slug, OUTPUT_DIR)
                _videos = extract_videos(_data)
                for sig in extract_macro_signals(_videos):
                    sig["_channel"] = channel_names.get(_slug, _slug)
                    macro_signals.append(sig)
        else:
            macro_slug = macro_slug_map.get(macro_ch_display, "")
            macro_30d = load_30d_results(macro_slug, OUTPUT_DIR)
            videos = extract_videos(macro_30d)
            macro_signals = extract_macro_signals(videos)
    except Exception as _e:
        st.error(f"매크로 데이터 로딩 오류: {_e}")
        macro_signals = []

    if macro_signals:
        df_macro = pd.DataFrame(macro_signals)
        total_macro_count = len(df_macro)
        col_map = {
            "_channel": "채널",
            "direction": "방향",
            "confidence": "신뢰도",
            "sentiment": "센티멘트",
            "label": "라벨",
            "source_video": "출처 영상",
        }
        if "label" not in df_macro.columns and "indicator" in df_macro.columns:
            df_macro["label"] = df_macro["indicator"]
        df_macro = df_macro.sort_values(["confidence", "label"], ascending=[False, True])
        macro_filter = st.text_input("매크로 검색 (라벨 / 출처 영상 / 채널)", "", key="macro_filter")
        if macro_filter:
            macro_search_df = pd.DataFrame(index=df_macro.index)
            for column in ["_channel", "label", "source_video"]:
                if column in df_macro.columns:
                    macro_search_df[column] = df_macro[column].fillna("").astype(str)
            if "direction" in df_macro.columns:
                macro_search_df["direction"] = df_macro["direction"].map(translate_direction).fillna("")
            if "sentiment" in df_macro.columns:
                macro_search_df["sentiment"] = df_macro["sentiment"].map(translate_direction).fillna("")
            macro_index = macro_search_df.astype(str).agg(" ".join, axis=1)
            df_macro = df_macro[macro_index.str.upper().str.contains(macro_filter.upper(), regex=False, na=False)]
        display_cols = [c for c in ["_channel", "label", "direction", "confidence", "sentiment", "source_video"] if c in df_macro.columns]
        display_df = df_macro[display_cols].copy()
        render_metrics_row([
            ("전체 인사이트", str(total_macro_count)),
            ("현재 표시", str(len(df_macro))),
        ], cols_desktop=2)

        if df_macro.empty:
            st.info("검색 조건에 맞는 매크로 인사이트가 없습니다.")
        else:
            # Translate direction and sentiment to Korean
            if "direction" in display_df.columns:
                display_df["direction"] = display_df["direction"].map(translate_direction)
            if "sentiment" in display_df.columns:
                display_df["sentiment"] = display_df["sentiment"].map(translate_direction)
            if "confidence" in display_df.columns:
                display_df["confidence"] = display_df["confidence"].map(lambda x: f"{x:.0%}" if isinstance(x, (int, float)) else x)

            display_df = display_df.rename(columns=col_map)
            st.dataframe(display_df, use_container_width=True, height=400, hide_index=True)

            chart_col_a, chart_col_b = st.columns(2)

            with chart_col_a:
                label_counts = df_macro["label"].value_counts().head(10).reset_index()
                label_counts.columns = ["라벨", "건수"]
                fig_labels = px.bar(
                    label_counts.sort_values("건수", ascending=True),
                    x="건수",
                    y="라벨",
                    orientation="h",
                    title="주요 매크로 키워드",
                    text="건수",
                    color="건수",
                    color_continuous_scale=["#1D4ED8", "#22C55E"],
                )
                fig_labels.update_traces(textposition="outside", hovertemplate="%{y}: %{x}건<extra></extra>")
                fig_labels.update_layout(coloraxis_showscale=False)
                render_chart(fig_labels, key="macro_labels", height=300)

            with chart_col_b:
                if "direction" in df_macro.columns:
                    dir_counts = df_macro["direction"].value_counts().reset_index()
                    dir_counts.columns = ["방향", "건수"]
                    dir_counts["방향"] = dir_counts["방향"].map(translate_direction)
                    color_map = {"상승": "#22C55E", "하락": "#EF4444", "중립": "#94A3B8", "강세": "#22C55E", "약세": "#EF4444"}
                    dir_counts = dir_counts.sort_values("건수", ascending=True)
                    dir_counts["비중"] = (dir_counts["건수"] / dir_counts["건수"].sum()).map(lambda x: f"{x:.0%}")
                    fig_dir = px.bar(
                        dir_counts,
                        x="건수",
                        y="방향",
                        orientation="h",
                        title="매크로 시그널 방향 분포",
                        color="방향",
                        color_discrete_map=color_map,
                        text="비중",
                    )
                    fig_dir.update_traces(
                        textposition="outside",
                        hovertemplate="%{y}: %{x}건 (%{text})<extra></extra>",
                    )
                    render_chart(fig_dir, key="macro_pie", height=300)

            if "_channel" in df_macro.columns and macro_ch_display == "전체 (통합)":
                source_counts = df_macro["_channel"].value_counts().reset_index()
                source_counts.columns = ["채널", "건수"]
                fig_sources = px.bar(
                    source_counts.sort_values("건수", ascending=True),
                    x="건수",
                    y="채널",
                    orientation="h",
                    title="매크로 인사이트 출처 채널",
                    text="건수",
                    color="건수",
                    color_continuous_scale=["#1D4ED8", "#06B6D4"],
                )
                fig_sources.update_traces(textposition="outside", hovertemplate="%{y}: %{x}건<extra></extra>")
                fig_sources.update_layout(coloraxis_showscale=False)
                render_chart(fig_sources, key="macro_sources", height=280)
    else:
        if macro_ch_display == "전체 (통합)":
            st.info("매크로 시그널이 없습니다.")
        else:
            st.info(f"'{macro_ch_display}' 채널의 매크로 시그널이 없습니다.")

# =============================================================================
# TAB 5 — 전문가
# =============================================================================

with tabs[5]:
    st.markdown("#### 전문가 인사이트")
    expert_ch_options = ["전체 (통합)"] + [channel_names.get(ch, ch) for ch in available_channels]
    expert_ch_display = st.selectbox("채널 선택", expert_ch_options, key="expert_ch")
    expert_slug_map = {channel_names.get(ch, ch): ch for ch in available_channels}

    try:
        if expert_ch_display == "전체 (통합)":
            insights = []
            for _slug in available_channels:
                _data = load_30d_results(_slug, OUTPUT_DIR)
                _videos = extract_videos(_data)
                for ins in extract_expert_insights(_videos):
                    ins["_channel"] = channel_names.get(_slug, _slug)
                    insights.append(ins)
        else:
            expert_slug = expert_slug_map.get(expert_ch_display, "")
            expert_30d = load_30d_results(expert_slug, OUTPUT_DIR)
            expert_videos = extract_videos(expert_30d)
            insights = extract_expert_insights(expert_videos)
    except Exception as _e:
        st.error(f"전문가 인사이트 로딩 오류: {_e}")
        insights = []

    if insights:
        total_insight_count = len(insights)
        expert_filter = st.text_input("전문가 검색 (이름 / 주제 / 종목 / 출처)", "", key="expert_filter")
        if expert_filter:
            filtered_insights = []
            needle = expert_filter.upper()
            for insight in insights:
                sentiment_label = translate_direction(str(insight.get("sentiment", "")))
                display_tickers = " ".join(
                    format_ticker_display(str(t))
                    for t in insight.get("mentioned_tickers", [])
                    if t
                )
                haystack = " ".join(
                    [
                        str(insight.get("expert_name", "")),
                        str(insight.get("affiliation", "")),
                        str(insight.get("topic", "")),
                        str(insight.get("source_video", "")),
                        str(insight.get("_channel", "")),
                        sentiment_label,
                        display_tickers,
                    ]
                ).upper()
                if needle in haystack:
                    filtered_insights.append(insight)
            insights = filtered_insights

        render_metrics_row([
            ("전체 인사이트", str(total_insight_count)),
            ("현재 표시", str(len(insights))),
        ], cols_desktop=2)

        if not insights:
            st.info("검색 조건에 맞는 전문가 인사이트가 없습니다.")
        elif expert_ch_display == "전체 (통합)":
            render_metrics_row([
                ("출처 채널", str(len({item.get('_channel', '') for item in insights if item.get('_channel')}))),
            ], cols_desktop=1)

            df_expert_sources = pd.DataFrame(
                [{"채널": item.get("_channel", EMPTY_TEXT)} for item in insights if item.get("_channel")]
            )
            if not df_expert_sources.empty:
                source_counts = df_expert_sources.value_counts().reset_index(name="건수").sort_values("건수", ascending=True)
                fig_expert_sources = px.bar(
                    source_counts,
                    x="건수",
                    y="채널",
                    orientation="h",
                    title="전문가 인사이트 출처 채널",
                    text="건수",
                    color="건수",
                    color_continuous_scale=["#1D4ED8", "#8B5CF6"],
                )
                fig_expert_sources.update_traces(textposition="outside", hovertemplate="%{y}: %{x}건<extra></extra>")
                fig_expert_sources.update_layout(coloraxis_showscale=False)
                render_chart(fig_expert_sources, key="expert_sources", height=260)

        if insights:
            for i, insight in enumerate(insights):
                expert_name = insight.get("expert_name", "미상")
                affiliation = insight.get("affiliation", "")
                source_channel = insight.get("_channel", "")
                prefix = f"[{source_channel}] " if source_channel and expert_ch_display == "전체 (통합)" else ""
                label = f"{prefix}{expert_name} — {affiliation}" if affiliation else f"{prefix}{expert_name}"

                with st.expander(label, expanded=(i < 2)):
                    cols = st.columns([2, 1])
                    with cols[0]:
                        st.markdown(f"**주제:** {insight.get('topic', EMPTY_TEXT)}")
                        sentiment = insight.get("sentiment", "NEUTRAL")
                        sentiment_kr = translate_direction(sentiment)
                        sentiment_color = {"BULLISH": "#22C55E", "BEARISH": "#EF4444"}.get(sentiment, "#94A3B8")
                        st.markdown(f"**센티멘트:** <span style='color:{sentiment_color};font-weight:700;'>{sentiment_kr}</span>", unsafe_allow_html=True)
                    with cols[1]:
                        if source_channel and expert_ch_display == "전체 (통합)":
                            st.markdown(f"**채널:** {source_channel}")
                        st.markdown(f"**출처:** {insight.get('source_video', '')[:40]}")

                    claims = insight.get("key_claims", [])
                    if claims:
                        st.markdown("**핵심 주장:**")
                        for claim in claims:
                            st.markdown(f"- {claim}")

                    structured = insight.get("structured_claims", [])
                    if structured:
                        st.markdown("**구조화된 주장:**")
                        for sc in structured:
                            direction = sc.get("direction", "NEUTRAL")
                            direction_kr = translate_direction(direction)
                            icon = {"BULLISH": ":green_circle:", "BEARISH": ":red_circle:"}.get(direction, ":white_circle:")
                            conf = sc.get("confidence", 0)
                            st.markdown(f"{icon} **{sc.get('claim', '')}**")
                            st.caption(f"신뢰도: {conf:.0%} | {direction_kr}")
                            if sc.get("reasoning"):
                                st.caption(f"근거: {sc['reasoning']}")

                    tickers = insight.get("mentioned_tickers", [])
                    if tickers:
                        display_tickers = [format_ticker_display(t) for t in tickers]
                        st.markdown(f"**언급 종목:** {', '.join(display_tickers)}")
    else:
        if expert_ch_display == "전체 (통합)":
            st.info("전문가 인사이트가 없습니다.")
        else:
            st.info(f"'{expert_ch_display}' 채널의 전문가 인사이트가 없습니다.")

# =============================================================================
# CHANNEL TABS (dynamic, Korean)
# =============================================================================

for idx, ch_slug in enumerate(available_channels):
    tab_idx = len(fixed_tabs) + idx
    with tabs[tab_idx]:
        ch_display = channel_names.get(ch_slug, ch_slug)
        st.markdown(f"#### {ch_display}")
        try:
            ch_data = load_30d_results(ch_slug, OUTPUT_DIR)
        except Exception as _e:
            st.error(f"채널 데이터 로딩 오류: {_e}")
            st.info("해당 채널의 분석 데이터가 없습니다.")
            continue

        if not ch_data:
            st.info("해당 채널의 분석 데이터가 없습니다.")
            continue

        # Channel KPIs
        generated = ch_data.get("generated_at", EMPTY_TEXT)
        generated = format_signal_datetime(generated)
        render_metrics_row([
            ("채널", ch_data.get("channel_name", ch_slug)),
            ("분석 기간", f"{ch_data.get('window_days', 30)}일"),
            ("생성일", generated),
        ], cols_desktop=3)

        # Quality scorecard
        scorecard = ch_data.get("quality_scorecard", {})
        if scorecard:
            render_metrics_row([
                ("종합 점수", f"{scorecard.get('overall', 0):.1f}"),
                ("트랜스크립트", f"{scorecard.get('transcript_coverage', 0):.1f}"),
                ("액션 밀도", f"{scorecard.get('actionable_density', 0):.1f}"),
                ("랭킹 예측력", f"{scorecard.get('ranking_predictive_power', 0):.1f}"),
            ], cols_desktop=4)

        # Videos
        ch_videos = extract_videos(ch_data)
        if ch_videos:
            signal_counts = pd.Series([v.get("video_signal_class", "UNKNOWN") for v in ch_videos]).value_counts()
            summary_badges = []
            for signal_key in ["ACTIONABLE", "SECTOR_ONLY", "LOW_SIGNAL", "NON_EQUITY", "NOISE"]:
                count = int(signal_counts.get(signal_key, 0))
                if count:
                    summary_badges.append(f"{signal_badge(signal_key)} <span style='color:#CBD5E1;font-size:0.85rem;'>{count}</span>")
            badges_html = " &middot; ".join(summary_badges)
            st.markdown(
                f'영상: **{len(ch_videos)}**개'
                f'{f" &middot; {badges_html}" if badges_html else ""}',
                unsafe_allow_html=True,
            )

            col_a, col_b = st.columns(2)
            with col_a:
                signal_classes = [v.get("video_signal_class", "UNKNOWN") for v in ch_videos]
                sig_series = pd.Series(signal_classes).value_counts().reset_index()
                sig_series.columns = ["시그널", "건수"]
                sig_series["시그널"] = sig_series["시그널"].map(translate_signal_class)
                sig_series = sig_series.sort_values("건수", ascending=True)
                sig_series["비중"] = (sig_series["건수"] / sig_series["건수"].sum()).map(lambda x: f"{x:.0%}")
                color_map_kr = {translate_signal_class(k): v for k, v in SIGNAL_COLORS.items()}
                fig = px.bar(
                    sig_series,
                    x="건수",
                    y="시그널",
                    orientation="h",
                    title=f"{ch_display} 시그널 분포",
                    color="시그널",
                    color_discrete_map=color_map_kr,
                    text="비중",
                )
                fig.update_traces(
                    textposition="outside",
                    hovertemplate="%{y}: %{x}개 (%{text})<extra></extra>",
                )
                render_chart(fig, key=f"ch_{ch_slug}_signals", height=420)

            with col_b:
                dates = [v.get("published_at", "") for v in ch_videos]
                if any(dates):
                    df_timeline = pd.DataFrame({
                        "날짜": [format_signal_date(d) for d in dates],
                        "시그널": [translate_signal_class(sc) for sc in signal_classes],
                    })
                    df_timeline = df_timeline[df_timeline["날짜"] != EMPTY_TEXT]
                    if not df_timeline.empty:
                        timeline_counts = df_timeline.groupby(["날짜", "시그널"]).size().reset_index(name="건수")
                        fig_timeline = px.area(
                            timeline_counts.sort_values("날짜"),
                            x="날짜",
                            y="건수",
                            color="시그널",
                            title=f"{ch_display} — 30일 타임라인",
                            color_discrete_map=color_map_kr,
                        )
                        fig_timeline.update_traces(
                            mode="lines",
                            line=dict(width=2),
                            hovertemplate="%{x}: %{y}개<extra>%{fullData.name}</extra>",
                        )
                        render_chart(fig_timeline, key=f"ch_{ch_slug}_timeline", height=420)

            with st.expander("영상 상세", expanded=False):
                df_vids = pd.DataFrame(ch_videos)
                vid_col_map = {
                    "title": "제목",
                    "video_signal_class": "시그널",
                    "signal_score": "점수",
                    "published_at": "게시일",
                    "video_type": "유형",
                }
                mobile_cols = [c for c in ["title", "video_type", "video_signal_class", "signal_score", "published_at"] if c in df_vids.columns]
                display_df = df_vids[mobile_cols].copy()
                if "published_at" in display_df.columns:
                    display_df["published_at"] = display_df["published_at"].map(format_signal_date)
                if "video_type" in display_df.columns:
                    display_df["video_type"] = display_df["video_type"].map(translate_video_type)
                if "video_signal_class" in display_df.columns:
                    display_df["video_signal_class"] = display_df["video_signal_class"].map(translate_signal_class)
                display_df = display_df.rename(columns=vid_col_map)
                video_filter = st.text_input("영상 검색 (제목 / 유형 / 시그널)", "", key=f"video_filter_{ch_slug}")
                total_video_count = len(display_df)
                if video_filter:
                    search_index = display_df.fillna("").astype(str).agg(" ".join, axis=1)
                    display_df = display_df[
                        search_index.str.upper().str.contains(video_filter.upper(), regex=False, na=False)
                    ]
                st.caption(f"표시 영상 {len(display_df)}개 / 전체 {total_video_count}개")
                if display_df.empty:
                    st.info("검색 조건에 맞는 영상이 없습니다.")
                else:
                    st.dataframe(display_df, use_container_width=True, height=400, hide_index=True)

        # Ranking
        ch_ranking = extract_cross_video_ranking(ch_data)
        if ch_ranking:
            with st.expander("종목 랭킹", expanded=False):
                rank_filter = st.text_input("종목 검색 (코드 / 회사명)", "", key=f"rank_filter_{ch_slug}")
                rank_sort = st.selectbox(
                    "정렬 기준",
                    ["점수", "출현", "최근 시점"],
                    key=f"rank_sort_{ch_slug}",
                )
                rank_rows = []
                for ri, item in enumerate(ch_ranking):
                    ticker = item.get("ticker", "")
                    display = format_ticker_display(ticker, item.get("company_name", ""))
                    verdict = item.get("aggregate_verdict", item.get("final_verdict", ""))
                    verdict_label = translate_verdict(verdict)
                    price_label = format_price(item.get("latest_price"), item.get("currency", "KRW"))
                    timing_display = format_reference_timing(
                        item.get("last_signal_at"),
                        item.get("first_signal_at"),
                        item.get("latest_checked_at"),
                    )
                    timing_raw = first_non_empty(
                        item.get("last_signal_at"),
                        item.get("first_signal_at"),
                        item.get("latest_checked_at"),
                    )
                    appearances = item.get("appearances", item.get("mention_count", 0))
                    score_value = round(item.get("aggregate_score", item.get("total_score", 0)), 1)
                    rank_rows.append({
                        "순위": ri + 1,
                        "종목": display,
                        "점수": score_value,
                        "판단": verdict_label,
                        "최근 시점": timing_display,
                        "현재가": price_label,
                        "출현": appearances,
                        "_검색": " ".join(
                            str(value)
                            for value in [ticker, display, verdict, verdict_label, timing_display, price_label]
                        ),
                        "_점수": score_value,
                        "_출현": appearances,
                        "_최근시점": parse_timestamp_string(timing_raw) or datetime.min.replace(tzinfo=timezone.utc),
                    })
                df_rank_rows = pd.DataFrame(rank_rows)
                if rank_filter:
                    df_rank_rows = df_rank_rows[
                        df_rank_rows["_검색"].str.upper().str.contains(rank_filter.upper(), regex=False, na=False)
                    ]
                sort_columns = {
                    "점수": ["_점수", "_출현", "종목"],
                    "출현": ["_출현", "_점수", "종목"],
                    "최근 시점": ["_최근시점", "_점수", "종목"],
                }
                df_rank_rows = df_rank_rows.sort_values(sort_columns[rank_sort], ascending=[False, False, True])
                st.caption(f"표시 종목 {len(df_rank_rows)}개 / 전체 {len(rank_rows)}개")
                if df_rank_rows.empty:
                    st.info("검색 조건에 맞는 종목이 없습니다.")
                else:
                    st.dataframe(
                        df_rank_rows.drop(columns=["_검색", "_점수", "_출현", "_최근시점"], errors="ignore"),
                        use_container_width=True,
                        height=350,
                        hide_index=True,
                    )
                    verdict_colors = {
                        translate_verdict("BUY"): "#22C55E",
                        translate_verdict("WATCH"): "#F59E0B",
                        translate_verdict("HOLD"): "#3B82F6",
                        translate_verdict("SELL"): "#EF4444",
                        translate_verdict("REJECT"): "#94A3B8",
                    }
                    df_rank_chart = (
                        df_rank_rows[["종목", "점수", "판단"]]
                        .copy()
                        .head(8)
                        .sort_values("점수", ascending=True)
                    )
                    df_rank_chart["종목"] = df_rank_chart["종목"].astype(str).str.slice(0, 26)
                    fig_rank = px.bar(
                        df_rank_chart,
                        x="점수",
                        y="종목",
                        orientation="h",
                        color="판단",
                        title=f"{ch_display} 상위 종목 점수 비교",
                        text="점수",
                        color_discrete_map=verdict_colors,
                    )
                    fig_rank.update_traces(textposition="outside", hovertemplate="%{y}: %{x}점<extra>%{fullData.name}</extra>")
                    render_chart(fig_rank, key=f"rank_chart_{ch_slug}", height=380)

# =============================================================================
# 채널 비교
# =============================================================================

with tabs[-2]:
    st.markdown("#### 채널 비교")
    try:
        comp_data = load_channel_comparison(OUTPUT_DIR)
    except Exception as _e:
        st.error(f"채널 비교 데이터 로딩 오류: {_e}")
        comp_data = None

    if comp_data and "channels" in comp_data:
        channels_info = comp_data["channels"]

        rows = []
        scorecard_rows = []
        returns_rows = []
        for slug, info in channels_info.items():
            channel_label = info.get("display_name", slug)
            sc = info.get("quality_scorecard", {})
            overall_score = sc.get("overall", 0.0)
            actionable_ratio = info.get("actionable_ratio", 0.0)
            top1_return = info.get("ranking_top_1_return_pct", 0.0)
            top3_return = info.get("ranking_top_3_return_pct", 0.0)
            predictive_power = sc.get("ranking_predictive_power", 0.0)
            row = {"채널": channel_label}
            row["영상 수"] = info.get("total_videos", 0)
            row["분석 가능"] = info.get("actionable_videos", 0)
            row["엄격 액션"] = info.get("strict_actionable_videos", 0)
            row["스킵"] = info.get("skipped_videos", 0)
            row["가능 비율"] = f"{actionable_ratio:.0%}"
            row["메타 fallback"] = info.get("metadata_fallback_videos", 0)
            row["최신 기준 시각"] = format_reference_display(
                info.get("latest_reference_at", ""),
                info.get("latest_reference_kind", "unknown"),
            )
            row["순위상관"] = (
                f"{info.get('ranking_spearman', 0.0):.2f}"
                if info.get("ranking_spearman") is not None
                else "미제공"
            )
            row["평가 표본"] = info.get("ranking_eval_positions", 0)
            row["종합 점수"] = f"{overall_score:.1f}"
            row["품질 종합"] = (
                f"{float(info.get('overall_quality_score', 0) or 0):.1f}"
                if info.get("overall_quality_score") is not None
                else EMPTY_TEXT
            )
            row["추적 신호"] = info.get("tracked_signals", 0)
            row["5일 표본"] = info.get("tracked_signals_5d", 0)
            row["5일 적중률"] = format_percent_metric(info.get("hit_rate_5d"))
            row["10일 적중률"] = format_percent_metric(info.get("hit_rate_10d"))
            row["5일 평균수익률"] = format_percent_metric(info.get("avg_return_5d"), digits=2)
            row["상위 1개 수익률"] = f"{top1_return:.1f}%"
            row["상위 3개 수익률"] = f"{top3_return:.1f}%"
            row["_sort_overall"] = overall_score
            row["_sort_quality"] = float(info.get("overall_quality_score", 0) or 0)
            row["_sort_actionable_ratio"] = actionable_ratio
            row["_sort_hit_rate_5d"] = float(info.get("hit_rate_5d", -1) or -1)
            row["_sort_hit_rate_10d"] = float(info.get("hit_rate_10d", -1) or -1)
            row["_sort_avg_return_5d"] = float(info.get("avg_return_5d", -999) or -999)
            row["_sort_top1_return"] = top1_return
            row["_sort_top3_return"] = top3_return
            row["_sort_predictive_power"] = predictive_power
            top_skip_reasons = info.get("top_skip_reasons", [])
            row["대표 스킵 사유"] = top_skip_reasons[0]["reason"] if top_skip_reasons else EMPTY_TEXT
            rows.append(row)

            metric_kr = {
                "overall": "종합",
                "transcript_coverage": "트랜스크립트",
                "ranking_predictive_power": "랭킹 예측력",
                "actionable_density": "액션 밀도",
                "horizon_adequacy": "투자 기간 적합성",
            }
            for metric_key, metric_label in metric_kr.items():
                val = sc.get(metric_key)
                if val is not None:
                    scorecard_rows.append({
                        "채널": channel_label,
                        "지표": metric_label,
                        "점수": val,
                    })

            returns_rows.extend([
                {
                    "채널": channel_label,
                    "항목": "상위 1개 수익률",
                    "값": top1_return,
                },
                {
                    "채널": channel_label,
                    "항목": "상위 3개 수익률",
                    "값": top3_return,
                },
            ])

        df_comp = pd.DataFrame(rows)
        sort_options = {
            "품질 종합": "_sort_quality",
            "종합 점수": "_sort_overall",
            "5일 적중률": "_sort_hit_rate_5d",
            "10일 적중률": "_sort_hit_rate_10d",
            "5일 평균수익률": "_sort_avg_return_5d",
            "가능 비율": "_sort_actionable_ratio",
            "상위 3개 수익률": "_sort_top3_return",
            "상위 1개 수익률": "_sort_top1_return",
            "랭킹 예측력": "_sort_predictive_power",
        }
        compare_sort_label = st.selectbox("정렬 기준", list(sort_options.keys()), key="compare_sort")
        sort_column = sort_options[compare_sort_label]
        df_comp = df_comp.sort_values([sort_column, "_sort_overall"], ascending=[False, False])
        channel_order = df_comp["채널"].tolist()
        st.dataframe(
            df_comp.drop(
                columns=[
                    "_sort_overall",
                    "_sort_quality",
                    "_sort_actionable_ratio",
                    "_sort_hit_rate_5d",
                    "_sort_hit_rate_10d",
                    "_sort_avg_return_5d",
                    "_sort_top1_return",
                    "_sort_top3_return",
                    "_sort_predictive_power",
                ],
                errors="ignore",
            ),
            use_container_width=True,
            hide_index=True,
        )

        if len(rows) >= 2 and scorecard_rows:
            chart_col_a, chart_col_b = st.columns(2)
            with chart_col_a:
                df_scorecard = pd.DataFrame(scorecard_rows)
                fig_comp = px.bar(
                    df_scorecard,
                    x="지표",
                    y="점수",
                    color="채널",
                    barmode="group",
                    title="채널 품질 지표 비교",
                    text="점수",
                    category_orders={"채널": channel_order},
                )
                fig_comp.update_traces(texttemplate="%{text:.1f}", textposition="outside")
                fig_comp.update_yaxes(title="점수", range=[0, 100])
                render_chart(fig_comp, key="compare_scorecard", height=320)

            with chart_col_b:
                df_returns = pd.DataFrame(returns_rows)
                color_map_returns = {
                    "상위 1개 수익률": "#3B82F6",
                    "상위 3개 수익률": "#22C55E",
                }
                fig_returns = px.bar(
                    df_returns,
                    x="채널",
                    y="값",
                    color="항목",
                    barmode="group",
                    title="랭킹 수익률 비교",
                    text="값",
                    color_discrete_map=color_map_returns,
                    category_orders={"채널": channel_order},
                )
                fig_returns.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
                fig_returns.update_yaxes(title="수익률 (%)", zeroline=True, zerolinecolor="rgba(255,255,255,0.18)")
                render_chart(fig_returns, key="compare_returns", height=320)

        if scorecard_rows:
            with st.expander("품질 지표 히트맵", expanded=False):
                df_chart = pd.DataFrame(scorecard_rows)
                fig_heatmap = px.imshow(
                    df_chart.assign(채널=pd.Categorical(df_chart["채널"], categories=channel_order, ordered=True))
                    .sort_values("채널")
                    .pivot(index="채널", columns="지표", values="점수"),
                    text_auto=".1f",
                    aspect="auto",
                    color_continuous_scale=["#0F172A", "#1D4ED8", "#22C55E"],
                    title="채널 품질 지표 히트맵",
                )
                fig_heatmap.update_xaxes(side="top")
                render_chart(fig_heatmap, key="compare_heatmap", height=300)

        more_act = comp_data.get("more_actionable_channel", EMPTY_TEXT)
        better_rank = comp_data.get("better_ranking_channel", EMPTY_TEXT)
        more_act_name = channel_names.get(more_act, more_act)
        better_rank_name = channel_names.get(better_rank, better_rank)
        st.markdown(f"**분석 가능 비율 최고:** {more_act_name}")
        st.markdown(f"**랭킹 예측력 최고:** {better_rank_name}")
    else:
        st.info("채널 비교 데이터가 없습니다.")

# =============================================================================
# 상태 탭
# =============================================================================

with tabs[-1]:
    st.markdown("#### 파이프라인 상태")

    if last_update:
        now_utc = datetime.now(tz=timezone.utc)
        age_min = (now_utc - last_update).total_seconds() / 60
        status_text = "활성" if age_min < 120 else "대기"

        render_metrics_row([
            ("상태", status_text),
            ("마지막 실행", format_timestamp_local(last_update, include_tz=False)),
            ("경과 시간", f"{age_min:.0f}분 전"),
            ("채널 수", str(len(available_channels))),
        ], cols_desktop=4)

    try:
        comp_status = load_channel_comparison(OUTPUT_DIR)
    except Exception as _e:
        st.error(f"파이프라인 상태 로딩 오류: {_e}")
        comp_status = None

    pipeline_summary = (comp_status or {}).get("pipeline_summary", {})
    if pipeline_summary:
        st.markdown("#### 게이트 운영 요약")
        render_metrics_row([
            ("스냅샷 run", format_run_id_display((comp_status or {}).get("generated_at", ""))),
            ("스킵 영상", str(pipeline_summary.get("skipped_videos", 0))),
            ("엄격 액션", str(pipeline_summary.get("strict_actionable_videos", 0))),
            ("실자막 기반", str(pipeline_summary.get("transcript_backed_videos", 0))),
            ("메타 fallback", str(pipeline_summary.get("metadata_fallback_videos", 0))),
            (
                "최신 기준",
                format_reference_display(
                    pipeline_summary.get("latest_reference_at", ""),
                    pipeline_summary.get("latest_reference_kind", "unknown"),
                ),
            ),
        ], cols_desktop=6)

        top_skip_reasons = pipeline_summary.get("top_skip_reasons", [])
        if top_skip_reasons:
            st.markdown("#### 상위 스킵 사유")
            reason_rows = "".join(
                f'<div class="status-row">'
                f'<div class="status-dot"></div>'
                f'<div class="status-time">{item["count"]}건</div>'
                f'<div class="status-label">{item["reason"]}</div>'
                f'</div>'
                for item in top_skip_reasons
            )
            st.markdown(f'<div class="omx-card">{reason_rows}</div>', unsafe_allow_html=True)

    # Pipeline activity log
    activity = get_pipeline_activity(OUTPUT_DIR)
    if activity:
        st.markdown("#### 최근 파이프라인 활동")
        rows_html = ""
        for entry in activity:
            ts = format_timestamp_local(entry["timestamp"], include_tz=False)
            ch_name = channel_names.get(entry["channel"], entry["channel"])
            rows_html += (
                f'<div class="status-row">'
                f'<div class="status-dot"></div>'
                f'<div class="status-time">{ts}</div>'
                f'<div class="status-label">{ch_name}</div>'
                f'</div>'
            )
        st.markdown(f'<div class="omx-card">{rows_html}</div>', unsafe_allow_html=True)

        df_activity = pd.DataFrame(
            [{"채널": channel_names.get(entry["channel"], entry["channel"])} for entry in activity]
        )
        if not df_activity.empty:
            activity_counts = df_activity.value_counts().reset_index(name="건수").sort_values("건수", ascending=True)
            fig_activity = px.bar(
                activity_counts,
                x="건수",
                y="채널",
                orientation="h",
                title="최근 활동 채널 분포",
                text="건수",
                color="건수",
                color_continuous_scale=["#1D4ED8", "#22C55E"],
            )
            fig_activity.update_traces(textposition="outside", hovertemplate="%{y}: %{x}회<extra></extra>")
            fig_activity.update_layout(coloraxis_showscale=False)
            render_chart(fig_activity, key="status_activity", height=280)
    else:
        st.info("파이프라인 활동 기록이 없습니다.")

    st.markdown(
        '<div style="text-align:center;color:#64748B;font-size:0.75rem;margin-top:2rem;">'
        '60초마다 자동 새로고침</div>',
        unsafe_allow_html=True,
    )
