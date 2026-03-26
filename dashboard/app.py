"""Y2I 투자 시그널 대시보드 — 한국어, 다크 테마, 모바일 최적화."""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from streamlit_autorefresh import st_autorefresh

from data_loader import (
    DEFAULT_OUTPUT_DIR,
    build_overview_report,
    extract_actionable_signals,
    extract_cross_video_ranking,
    extract_expert_insights,
    extract_macro_signals,
    extract_per_video,
    extract_signal_distribution,
    extract_type_distribution,
    extract_videos,
    format_price,
    format_ticker_display,
    get_all_rankings,
    get_available_channels,
    get_channel_display_names,
    get_last_update_time,
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
.ticker-chip {
    background: rgba(34,197,94,0.12);
    color: #22C55E;
    padding: 4px 10px;
    border-radius: 8px;
    font-size: 0.8rem;
    font-weight: 700;
    font-family: monospace;
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
</style>
""", unsafe_allow_html=True)

# -- Auth gate --------------------------------------------------------------

DASHBOARD_TOKEN = os.environ.get("DASHBOARD_TOKEN", "jS-GpK2lpXoeLGGm17hRSmmPoAQxahs3")
query_token = st.query_params.get("token", "")
if query_token != DASHBOARD_TOKEN:
    st.error("접근이 거부되었습니다. URL에 ?token=<토큰>을 추가하세요.")
    st.stop()

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
    "ACTIONABLE": "분석 대상",
    "SECTOR_ONLY": "섹터 참고",
    "LOW_SIGNAL": "저신호",
    "NON_EQUITY": "비주식",
    "NOISE": "노이즈",
    "UNKNOWN": "미분류",
}
EMPTY_TEXT = "미제공"

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


def render_chart(fig: go.Figure, key: str | None = None, height: int = 400) -> None:
    """Render a Plotly chart with dark theme."""
    fig.update_layout(
        template=PLOTLY_TEMPLATE,
        margin=dict(l=16, r=16, t=48, b=24),
        font=dict(size=14),
        hovermode="x unified",
        hoverlabel=dict(bgcolor="#0F172A", bordercolor="rgba(255,255,255,0.12)", font=dict(color="#F8FAFC")),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=-0.24,
            xanchor="center",
            x=0.5,
            font=dict(size=12, color="#94A3B8"),
        ),
        height=height,
    )
    fig.update_xaxes(showline=False, tickfont=dict(color="#CBD5E1"))
    fig.update_yaxes(showline=False, tickfont=dict(color="#CBD5E1"))
    st.plotly_chart(fig, use_container_width=True, key=key)


def render_metrics_row(metrics: list[tuple[str, str]], cols_desktop: int = 4) -> None:
    col_count = min(len(metrics), cols_desktop)
    cols = st.columns(col_count)
    for i, (label, value) in enumerate(metrics):
        cols[i % col_count].metric(label, value)


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


def translate_signal_class(signal_class: str) -> str:
    return SIGNAL_CLASS_KR.get(signal_class, signal_class or EMPTY_TEXT)


def translate_verdict(verdict: str) -> str:
    return VERDICT_KR.get(verdict, verdict or EMPTY_TEXT)


def translate_video_type(video_type: str) -> str:
    return VIDEO_TYPE_KR.get(video_type, video_type or EMPTY_TEXT)


def translate_direction(direction: str) -> str:
    return DIRECTION_KR.get(direction, direction or EMPTY_TEXT)


# -- Build tabs ---------------------------------------------------------------

fixed_tabs = ["요약", "종목 랭킹", "매크로", "전문가"]
channel_tab_labels = [channel_names.get(ch, ch) for ch in available_channels]
tab_labels = fixed_tabs + channel_tab_labels + ["채널 비교", "상태"]

tabs = st.tabs(tab_labels)

# =============================================================================
# TAB 0 — 요약 (핵심 시그널)
# =============================================================================

with tabs[0]:
    # -- 핵심 시그널 카드 (Top ranked stocks across all channels) --
    all_rankings = get_all_rankings(OUTPUT_DIR)
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
            signal_date = format_signal_date(stock.get("last_signal_at", ""))
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
                    f'{channel_count}개 채널 &middot; {appearances}회 출현 &middot; {signal_date}'
                    f'</div>'
                    f'<div style="font-size:0.7rem;color:#64748B;margin-top:2px;">{source_label}</div>'
                    f'</div>',
                    unsafe_allow_html=True,
                )

    st.markdown("---")

    # -- Actionable Signal Alert Banner --
    actionable = extract_actionable_signals(OUTPUT_DIR)
    if actionable:
        all_tickers: list[str] = []
        for sig in actionable:
            all_tickers.extend(sig.get("tickers", []))
        unique_tickers = sorted(set(all_tickers))
        if unique_tickers:
            ticker_chips = "".join(
                f'<span class="ticker-chip">{format_ticker_display(t)}</span>' for t in unique_tickers[:20]
            )
            st.markdown(
                f'<div class="omx-alert">'
                f'<div class="omx-alert-title">액션 가능 시그널 발견 ({len(actionable)}개 영상)</div>'
                f'<div class="omx-alert-tickers">{ticker_chips}</div>'
                f'</div>',
                unsafe_allow_html=True,
            )

    # -- 최근 분석 (24h) --
    recent_videos = get_recent_videos(OUTPUT_DIR, hours=24)
    if recent_videos:
        st.markdown("#### 최근 분석 (24시간)")
        rcols = st.columns(min(len(recent_videos), 3))
        for i, rv in enumerate(recent_videos[:6]):
            sig_cls = rv.get("video_signal_class", "UNKNOWN")
            card_class = video_card_class(sig_cls)
            score = rv.get("signal_score", 0)
            pub_date = format_signal_date(rv.get("published_at", ""))
            updated_at = format_timestamp_local(rv.get("_updated_at"), include_tz=False)
            ch_name = channel_names.get(rv.get("_channel", ""), rv.get("_channel", ""))
            reason = rv.get("reason", "")
            with rcols[i % len(rcols)]:
                reason_html = ""
                if reason:
                    reason_html = f'<div style="font-size:0.75rem;color:#CBD5E1;margin-top:4px;">{reason[:100]}</div>'
                st.markdown(
                    f'<div class="video-card {card_class}">'
                    f'<div class="video-card-title">{rv.get("title", "제목 없음")[:60]}</div>'
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
    report = build_overview_report(OUTPUT_DIR)
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
                render_chart(fig_type, key="overview_type", height=420)

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
                render_chart(fig_bar, key="overview_signal", height=420)

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
                render_chart(fig_labels, key="overview_labels", height=420)

# =============================================================================
# TAB 1 — 종목 랭킹
# =============================================================================

with tabs[1]:
    st.markdown("#### 종목 랭킹")
    rank_ch_options = ["전체 (통합)"] + [channel_names.get(ch, ch) for ch in available_channels]
    rank_ch_idx = st.selectbox("채널 선택", rank_ch_options, key="rank_ch")

    if rank_ch_idx == "전체 (통합)":
        ranking = get_all_rankings(OUTPUT_DIR)
    else:
        # Find the slug for the selected display name
        slug_map = {channel_names.get(ch, ch): ch for ch in available_channels}
        selected_slug = slug_map.get(rank_ch_idx, "")
        data_30d = load_30d_results(selected_slug, OUTPUT_DIR)
        ranking = extract_cross_video_ranking(data_30d)

    if ranking:
        filter_text = st.text_input("종목 검색 (코드 / 회사명)", "", key="rank_filter")

        # Build display dataframe
        rows = []
        for i, item in enumerate(ranking):
            ticker = item.get("ticker", "")
            display = format_ticker_display(ticker, item.get("company_name", ""))
            score = item.get("aggregate_score", item.get("total_score", 0))
            verdict = item.get("aggregate_verdict", item.get("final_verdict", ""))
            price = item.get("latest_price")
            currency = item.get("currency", "KRW")
            last_signal = format_signal_date(item.get("last_signal_at", item.get("first_signal_at", "")))
            appearances = item.get("appearances", item.get("mention_count", 0))
            checked_at = format_signal_datetime(item.get("latest_checked_at", ""))

            rows.append({
                "순위": i + 1,
                "종목": display,
                "점수": round(score, 1),
                "판단": translate_verdict(verdict),
                "판단 시점": last_signal,
                "현재가": format_price(price, currency),
                "출현": appearances,
                "가격확인": checked_at,
                "_검색": " ".join(str(value) for value in [ticker, display, verdict, last_signal, checked_at]),
                "_점수": round(score, 1),
                "_판단_raw": verdict,
            })

        df_rank = pd.DataFrame(rows)

        if filter_text:
            mask = df_rank["_검색"].str.upper().str.contains(filter_text.upper(), regex=False, na=False)
            df_rank = df_rank[mask]

        render_metrics_row([
            ("전체 종목", str(len(rows))),
            ("현재 표시", str(len(df_rank))),
        ], cols_desktop=2)

        display_rank = df_rank.drop(columns=["_검색", "_점수", "_판단_raw"], errors="ignore")
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
                "판단_raw": df_rank["_판단_raw"],
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
            render_chart(fig_rank, key="rank_scores", height=460)
    else:
        st.info("랭킹 데이터가 없습니다.")

# =============================================================================
# TAB 2 — 매크로
# =============================================================================

with tabs[2]:
    st.markdown("#### 매크로 시그널")
    macro_ch_options = ["전체 (통합)"] + [channel_names.get(ch, ch) for ch in available_channels]
    macro_ch_display = st.selectbox("채널 선택", macro_ch_options, key="macro_ch")
    macro_slug_map = {channel_names.get(ch, ch): ch for ch in available_channels}

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

    if macro_signals:
        df_macro = pd.DataFrame(macro_signals)
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
        display_cols = [c for c in ["_channel", "label", "direction", "confidence", "sentiment", "source_video"] if c in df_macro.columns]
        display_df = df_macro[display_cols].copy()

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
            render_chart(fig_labels, key="macro_labels", height=420)

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
                render_chart(fig_dir, key="macro_pie", height=420)

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
            render_chart(fig_sources, key="macro_sources", height=360)
    else:
        st.info(f"'{macro_ch_display}' 채널의 매크로 시그널이 없습니다.")

# =============================================================================
# TAB 3 — 전문가
# =============================================================================

with tabs[3]:
    st.markdown("#### 전문가 인사이트")
    expert_ch_options = ["전체 (통합)"] + [channel_names.get(ch, ch) for ch in available_channels]
    expert_ch_display = st.selectbox("채널 선택", expert_ch_options, key="expert_ch")
    expert_slug_map = {channel_names.get(ch, ch): ch for ch in available_channels}

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

    if insights:
        if expert_ch_display == "전체 (통합)":
            render_metrics_row([
                ("전문가 인사이트", str(len(insights))),
                ("출처 채널", str(len({item.get('_channel', '') for item in insights if item.get('_channel')}))),
            ], cols_desktop=2)

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
                render_chart(fig_expert_sources, key="expert_sources", height=320)

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
        st.info(f"'{expert_ch_display}' 채널의 전문가 인사이트가 없습니다.")

# =============================================================================
# CHANNEL TABS (dynamic, Korean)
# =============================================================================

for idx, ch_slug in enumerate(available_channels):
    tab_idx = len(fixed_tabs) + idx
    with tabs[tab_idx]:
        ch_display = channel_names.get(ch_slug, ch_slug)
        st.markdown(f"#### {ch_display}")
        ch_data = load_30d_results(ch_slug, OUTPUT_DIR)

        if not ch_data:
            st.info(f"'{ch_display}' 채널의 30일 데이터가 없습니다.")
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
                st.dataframe(display_df, use_container_width=True, height=400, hide_index=True)

        # Ranking
        ch_ranking = extract_cross_video_ranking(ch_data)
        if ch_ranking:
            with st.expander("종목 랭킹", expanded=False):
                rank_rows = []
                for ri, item in enumerate(ch_ranking):
                    ticker = item.get("ticker", "")
                    rank_rows.append({
                        "순위": ri + 1,
                        "종목": format_ticker_display(ticker, item.get("company_name", "")),
                        "점수": round(item.get("aggregate_score", item.get("total_score", 0)), 1),
                        "판단": translate_verdict(item.get("aggregate_verdict", item.get("final_verdict", ""))),
                        "출현": item.get("appearances", item.get("mention_count", 0)),
                    })
                st.dataframe(pd.DataFrame(rank_rows), use_container_width=True, height=350, hide_index=True)

# =============================================================================
# 채널 비교
# =============================================================================

with tabs[-2]:
    st.markdown("#### 채널 비교")
    comp_data = load_channel_comparison(OUTPUT_DIR)

    if comp_data and "channels" in comp_data:
        channels_info = comp_data["channels"]

        rows = []
        scorecard_rows = []
        returns_rows = []
        for slug, info in channels_info.items():
            channel_label = info.get("display_name", slug)
            row = {"채널": channel_label}
            row["영상 수"] = info.get("total_videos", 0)
            row["분석 대상"] = info.get("actionable_videos", 0)
            row["대상 비율"] = f"{info.get('actionable_ratio', 0.0):.0%}"
            row["순위상관"] = (
                f"{info.get('ranking_spearman', 0.0):.2f}"
                if info.get("ranking_spearman") is not None
                else "미제공"
            )
            row["평가 표본"] = info.get("ranking_eval_positions", 0)
            sc = info.get("quality_scorecard", {})
            row["종합 점수"] = f"{sc.get('overall', 0.0):.1f}"
            row["상위 1개 수익률"] = f"{info.get('ranking_top_1_return_pct', 0.0):.1f}%"
            row["상위 3개 수익률"] = f"{info.get('ranking_top_3_return_pct', 0.0):.1f}%"
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
                    "값": info.get("ranking_top_1_return_pct", 0.0),
                },
                {
                    "채널": channel_label,
                    "항목": "상위 3개 수익률",
                    "값": info.get("ranking_top_3_return_pct", 0.0),
                },
            ])

        df_comp = pd.DataFrame(rows)
        st.dataframe(df_comp, use_container_width=True, hide_index=True)

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
                )
                fig_comp.update_traces(texttemplate="%{text:.1f}", textposition="outside")
                fig_comp.update_yaxes(title="점수", range=[0, 100])
                render_chart(fig_comp, key="compare_scorecard", height=500)

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
                )
                fig_returns.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
                fig_returns.update_yaxes(title="수익률 (%)", zeroline=True, zerolinecolor="rgba(255,255,255,0.18)")
                render_chart(fig_returns, key="compare_returns", height=500)

        if scorecard_rows:
            with st.expander("품질 지표 히트맵", expanded=False):
                df_chart = pd.DataFrame(scorecard_rows)
                fig_heatmap = px.imshow(
                    df_chart.pivot(index="채널", columns="지표", values="점수"),
                    text_auto=".1f",
                    aspect="auto",
                    color_continuous_scale=["#0F172A", "#1D4ED8", "#22C55E"],
                    title="채널 품질 지표 히트맵",
                )
                fig_heatmap.update_xaxes(side="top")
                render_chart(fig_heatmap, key="compare_heatmap", height=460)

        more_act = comp_data.get("more_actionable_channel", EMPTY_TEXT)
        better_rank = comp_data.get("better_ranking_channel", EMPTY_TEXT)
        more_act_name = channel_names.get(more_act, more_act)
        better_rank_name = channel_names.get(better_rank, better_rank)
        st.markdown(f"**액션 시그널 최다:** {more_act_name}")
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
            render_chart(fig_activity, key="status_activity", height=360)
    else:
        st.info("파이프라인 활동 기록이 없습니다.")

    st.markdown(
        '<div style="text-align:center;color:#64748B;font-size:0.75rem;margin-top:2rem;">'
        '60초마다 자동 새로고침</div>',
        unsafe_allow_html=True,
    )
