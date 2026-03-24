"""OMX Brainstorm Streamlit Dashboard — dark-theme, card-based, mobile-first."""
from __future__ import annotations

import os
from datetime import datetime, timezone

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from streamlit_autorefresh import st_autorefresh

from data_loader import (
    DEFAULT_OUTPUT_DIR,
    extract_actionable_signals,
    extract_cross_video_ranking,
    extract_expert_insights,
    extract_macro_signals,
    extract_per_video,
    extract_signal_distribution,
    extract_type_distribution,
    extract_videos,
    get_available_channels,
    get_last_update_time,
    get_pipeline_activity,
    get_recent_videos,
    load_30d_results,
    load_channel_comparison,
    load_integration_report,
    load_video_titles,
)

# -- Page config ---------------------------------------------------------------

st.set_page_config(
    page_title="OMX Dashboard",
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
   OMX Design System — Dark Finance Theme
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
    /* Scroll fade indicators */
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

/* -- New video card ---------------------------------------------------- */
.video-card {
    background: #1E293B;
    border: 1px solid rgba(255,255,255,0.06);
    border-radius: 14px;
    padding: 1rem;
    margin-bottom: 0.75rem;
}
.video-card-actionable { border-left: 3px solid #22C55E; }
.video-card-noise { border-left: 3px solid #64748B; }
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
    st.error("Access denied. Append ?token=<your-token> to the URL.")
    st.stop()

# -- Header with timestamp + NEW badge --------------------------------------

OUTPUT_DIR = DEFAULT_OUTPUT_DIR

last_update = get_last_update_time(OUTPUT_DIR)
header_cols = st.columns([3, 1])
with header_cols[0]:
    st.title("OMX Dashboard")
with header_cols[1]:
    if last_update:
        now_utc = datetime.now(tz=timezone.utc)
        age_min = (now_utc - last_update).total_seconds() / 60
        ts_str = last_update.strftime("%Y-%m-%d %H:%M UTC")
        badge_html = ""
        if age_min < 5:
            badge_html = ' <span class="badge badge-new">NEW</span>'
        st.markdown(
            f'<div style="text-align:right;padding-top:1.2rem;">'
            f'<span style="color:#94A3B8;font-size:0.8rem;">Last updated</span><br>'
            f'<span style="color:#F8FAFC;font-size:0.9rem;font-weight:600;">{ts_str}</span>'
            f'{badge_html}</div>',
            unsafe_allow_html=True,
        )

# -- Load channels -----------------------------------------------------------

available_channels = get_available_channels(OUTPUT_DIR)
if not available_channels:
    st.warning("No output data found. Run the pipeline first.")
    st.stop()

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

SIGNAL_COLORS = {"ACTIONABLE": "#22C55E", "NOISE": "#EF4444", "UNKNOWN": "#64748B"}


def render_chart(fig: go.Figure, key: str | None = None, height: int = 400) -> None:
    """Render a Plotly chart with dark theme and large labels."""
    fig.update_layout(
        template=PLOTLY_TEMPLATE,
        margin=dict(l=16, r=16, t=48, b=24),
        font=dict(size=14),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=-0.2,
            xanchor="center",
            x=0.5,
            font=dict(size=12, color="#94A3B8"),
        ),
        height=height,
    )
    st.plotly_chart(fig, use_container_width=True, key=key)


def render_metrics_row(metrics: list[tuple[str, str]], cols_desktop: int = 4) -> None:
    col_count = min(len(metrics), cols_desktop)
    cols = st.columns(col_count)
    for i, (label, value) in enumerate(metrics):
        cols[i % col_count].metric(label, value)


def signal_badge(signal_class: str) -> str:
    """Return HTML for a signal badge."""
    css_class = "badge-actionable" if signal_class == "ACTIONABLE" else "badge-noise"
    return f'<span class="badge {css_class}">{signal_class}</span>'


# -- Build tabs ---------------------------------------------------------------

tab_labels = ["Overview", "Signals", "Ranking", "Macro", "Expert"] + \
    [ch[:12] for ch in available_channels] + ["Compare", "Status"]

tabs = st.tabs(tab_labels)

# =============================================================================
# TAB 0 — Overview
# =============================================================================

with tabs[0]:
    # -- Actionable Signal Alert Banner (US-006) --
    actionable = extract_actionable_signals(OUTPUT_DIR)
    if actionable:
        all_tickers: list[str] = []
        for sig in actionable:
            all_tickers.extend(sig.get("tickers", []))
        unique_tickers = sorted(set(all_tickers))
        if unique_tickers:
            ticker_chips = "".join(f'<span class="ticker-chip">{t}</span>' for t in unique_tickers[:20])
            st.markdown(
                f'<div class="omx-alert">'
                f'<div class="omx-alert-title">Actionable Signals Found ({len(actionable)} videos)</div>'
                f'<div class="omx-alert-tickers">{ticker_chips}</div>'
                f'</div>',
                unsafe_allow_html=True,
            )

    # -- New Videos in last 24h (US-003) --
    recent_videos = get_recent_videos(OUTPUT_DIR, hours=24)
    if recent_videos:
        st.markdown("#### New Analysis (Last 24h)")
        rcols = st.columns(min(len(recent_videos), 3))
        for i, rv in enumerate(recent_videos[:6]):
            sig_cls = rv.get("video_signal_class", "UNKNOWN")
            card_class = "video-card-actionable" if sig_cls == "ACTIONABLE" else "video-card-noise"
            score = rv.get("signal_score", 0)
            with rcols[i % len(rcols)]:
                st.markdown(
                    f'<div class="video-card {card_class}">'
                    f'<div class="video-card-title">{rv.get("title", "Untitled")[:60]}</div>'
                    f'<div class="video-card-meta">'
                    f'{rv.get("_channel", "")} &middot; Score: {score:.0f} &middot; '
                    f'{signal_badge(sig_cls)}'
                    f'</div></div>',
                    unsafe_allow_html=True,
                )

    st.markdown("---")

    # -- Pipeline Summary KPIs --
    report = load_integration_report(OUTPUT_DIR)
    if report:
        st.markdown("#### Pipeline Summary")
        render_metrics_row([
            ("Total Videos", str(report.get("total_videos", 0))),
            ("Analyzable", str(report.get("analyzable_count", 0))),
            ("Expert Rate", str(report.get("expert_extraction_rate", "N/A"))),
            ("Macro Coverage", str(report.get("macro_coverage", "N/A"))),
        ], cols_desktop=4)

        col_a, col_b = st.columns(2)
        with col_a:
            type_dist = extract_type_distribution(report)
            if type_dist:
                df_type = pd.DataFrame(
                    {"type": list(type_dist.keys()), "count": list(type_dist.values())}
                )
                fig_pie = px.pie(
                    df_type, names="type", values="count",
                    title="Video Type Distribution",
                    color_discrete_sequence=["#3B82F6", "#22C55E", "#F59E0B", "#EF4444", "#8B5CF6"],
                )
                render_chart(fig_pie, key="overview_pie")

        with col_b:
            sig_dist = extract_signal_distribution(report)
            if sig_dist:
                df_sig = pd.DataFrame(
                    {"signal": list(sig_dist.keys()), "count": list(sig_dist.values())}
                )
                fig_bar = px.bar(
                    df_sig, x="signal", y="count",
                    title="Signal Distribution",
                    color="signal",
                    color_discrete_map=SIGNAL_COLORS,
                )
                render_chart(fig_bar, key="overview_bar")

        with st.expander("Per-Video Breakdown", expanded=False):
            per_video = extract_per_video(report)
            if per_video:
                df_pv = pd.DataFrame(per_video)
                mobile_cols = [c for c in ["title", "video_type", "signal_class", "signal_score"] if c in df_pv.columns]
                st.dataframe(df_pv[mobile_cols] if mobile_cols else df_pv, use_container_width=True, height=400)

    # -- Title Labels --
    titles_data = load_video_titles(OUTPUT_DIR)
    if titles_data and "titles" in titles_data:
        all_labels: list[str] = []
        for t in titles_data["titles"]:
            all_labels.extend(t.get("labels", []))
        if all_labels:
            with st.expander("Content Labels", expanded=False):
                label_counts = pd.Series(all_labels).value_counts().reset_index()
                label_counts.columns = ["label", "count"]
                fig_labels = px.bar(
                    label_counts, x="label", y="count",
                    title="Content Labels",
                    color="label",
                    color_discrete_sequence=["#3B82F6", "#22C55E", "#F59E0B", "#8B5CF6", "#06B6D4"],
                )
                render_chart(fig_labels, key="overview_labels")

# =============================================================================
# TAB 1 — Actionable Signals (US-006)
# =============================================================================

with tabs[1]:
    st.markdown("#### Actionable Signals")
    if actionable:
        render_metrics_row([
            ("Actionable Videos", str(len(actionable))),
            ("Unique Tickers", str(len(set(t for s in actionable for t in s.get("tickers", []))))),
            ("Top Score", f"{actionable[0].get('signal_score', 0):.0f}" if actionable else "N/A"),
        ], cols_desktop=3)

        for sig in actionable:
            tickers_str = ", ".join(sig.get("tickers", [])) or "No tickers"
            score = sig.get("signal_score", 0)
            st.markdown(
                f'<div class="omx-card omx-card-actionable">'
                f'<div style="display:flex;justify-content:space-between;align-items:center;">'
                f'<div>'
                f'<div style="font-weight:700;font-size:1rem;color:#F8FAFC;">{sig.get("title", "")[:70]}</div>'
                f'<div style="color:#94A3B8;font-size:0.85rem;margin-top:4px;">'
                f'{sig.get("channel", "")} &middot; {sig.get("published_at", "")}</div>'
                f'</div>'
                f'<div style="text-align:right;">'
                f'<div style="font-size:1.5rem;font-weight:800;color:#22C55E;">{score:.0f}</div>'
                f'<div style="font-size:0.7rem;color:#94A3B8;">SCORE</div>'
                f'</div></div>'
                f'<div style="margin-top:8px;display:flex;flex-wrap:wrap;gap:6px;">'
                + "".join(f'<span class="ticker-chip">{t}</span>' for t in sig.get("tickers", []))
                + f'</div></div>',
                unsafe_allow_html=True,
            )
    else:
        st.info("No actionable signals found.")

# =============================================================================
# TAB 2 — Stock Ranking
# =============================================================================

with tabs[2]:
    st.markdown("#### Stock Ranking")
    ranking_channel = st.selectbox("Channel", available_channels, key="rank_ch")
    data_30d = load_30d_results(ranking_channel, OUTPUT_DIR)
    ranking = extract_cross_video_ranking(data_30d)

    if ranking:
        df_rank = pd.DataFrame(ranking)
        filter_text = st.text_input("Filter ticker / company", "", key="rank_filter")
        if filter_text:
            mask = df_rank.apply(lambda row: filter_text.upper() in str(row.values).upper(), axis=1)
            df_rank = df_rank[mask]

        if "total_score" in df_rank.columns:
            df_rank = df_rank.sort_values(by="total_score", ascending=False)

        st.dataframe(df_rank, use_container_width=True, height=500)
    else:
        st.info(f"No ranking data for '{ranking_channel}'.")

# =============================================================================
# TAB 3 — Macro Signals
# =============================================================================

with tabs[3]:
    st.markdown("#### Macro Signals")
    macro_channel = st.selectbox("Channel", available_channels, key="macro_ch")
    macro_30d = load_30d_results(macro_channel, OUTPUT_DIR)
    videos = extract_videos(macro_30d)
    macro_signals = extract_macro_signals(videos)

    if macro_signals:
        df_macro = pd.DataFrame(macro_signals)
        mobile_cols = [c for c in ["indicator", "direction", "confidence", "sentiment"] if c in df_macro.columns]
        st.dataframe(df_macro[mobile_cols] if mobile_cols else df_macro, use_container_width=True, height=400)

        if "direction" in df_macro.columns:
            dir_counts = df_macro["direction"].value_counts().reset_index()
            dir_counts.columns = ["direction", "count"]
            fig_dir = px.pie(
                dir_counts, names="direction", values="count",
                title="Macro Signal Directions",
                color_discrete_map={"UP": "#22C55E", "DOWN": "#EF4444", "NEUTRAL": "#94A3B8"},
            )
            render_chart(fig_dir, key="macro_pie")
    else:
        st.info(f"No macro signals for '{macro_channel}'.")

# =============================================================================
# TAB 4 — Expert Insights
# =============================================================================

with tabs[4]:
    st.markdown("#### Expert Insights")
    expert_channel = st.selectbox("Channel", available_channels, key="expert_ch")
    expert_30d = load_30d_results(expert_channel, OUTPUT_DIR)
    expert_videos = extract_videos(expert_30d)
    insights = extract_expert_insights(expert_videos)

    if insights:
        for i, insight in enumerate(insights):
            expert_name = insight.get("expert_name", "Unknown")
            affiliation = insight.get("affiliation", "")
            label = f"{expert_name} -- {affiliation}" if affiliation else expert_name

            with st.expander(label, expanded=(i < 2)):
                st.markdown(f"**Topic:** {insight.get('topic', 'N/A')}")
                st.markdown(f"**Sentiment:** {insight.get('sentiment', 'NEUTRAL')}")
                st.markdown(f"**Source:** {insight.get('source_video', '')}")

                claims = insight.get("key_claims", [])
                if claims:
                    st.markdown("**Key Claims:**")
                    for claim in claims:
                        st.markdown(f"- {claim}")

                structured = insight.get("structured_claims", [])
                if structured:
                    st.markdown("**Structured Claims:**")
                    for sc in structured:
                        direction = sc.get("direction", "NEUTRAL")
                        icon = {"BULLISH": ":green_circle:", "BEARISH": ":red_circle:"}.get(direction, ":white_circle:")
                        conf = sc.get("confidence", 0)
                        st.markdown(f"{icon} **{sc.get('claim', '')}**")
                        st.caption(f"Confidence: {conf:.0%} | {direction}")
                        if sc.get("reasoning"):
                            st.caption(f"Reasoning: {sc['reasoning']}")

                tickers = insight.get("mentioned_tickers", [])
                if tickers:
                    st.markdown(f"**Tickers:** {', '.join(tickers)}")
    else:
        st.info(f"No expert insights for '{expert_channel}'.")

# =============================================================================
# CHANNEL TABS (dynamic)
# =============================================================================

for idx, ch_slug in enumerate(available_channels):
    tab_idx = 5 + idx
    with tabs[tab_idx]:
        st.markdown(f"#### Channel: {ch_slug}")
        ch_data = load_30d_results(ch_slug, OUTPUT_DIR)

        if not ch_data:
            st.info(f"No 30-day data for '{ch_slug}'.")
            continue

        # Channel KPIs
        render_metrics_row([
            ("Channel", ch_data.get("channel_name", ch_slug)),
            ("Window", f"{ch_data.get('window_days', 30)}d"),
            ("Generated", ch_data.get("generated_at", "N/A")),
        ], cols_desktop=3)

        # Quality scorecard
        scorecard = ch_data.get("quality_scorecard", {})
        if scorecard:
            render_metrics_row([
                ("Overall", f"{scorecard.get('overall', 0):.1f}"),
                ("Transcript", f"{scorecard.get('transcript_coverage', 0):.1f}"),
                ("Actionable", f"{scorecard.get('actionable_density', 0):.1f}"),
                ("Ranking", f"{scorecard.get('ranking_predictive_power', 0):.1f}"),
            ], cols_desktop=4)

        # Videos
        ch_videos = extract_videos(ch_data)
        if ch_videos:
            # Count actionable
            actionable_count = sum(1 for v in ch_videos if v.get("video_signal_class") == "ACTIONABLE")
            st.markdown(
                f'Videos: **{len(ch_videos)}** total &middot; '
                f'<span class="badge badge-actionable">ACTIONABLE: {actionable_count}</span>',
                unsafe_allow_html=True,
            )

            col_a, col_b = st.columns(2)
            with col_a:
                signal_classes = [v.get("video_signal_class", "UNKNOWN") for v in ch_videos]
                sig_series = pd.Series(signal_classes).value_counts().reset_index()
                sig_series.columns = ["signal", "count"]
                fig = px.bar(
                    sig_series, x="signal", y="count",
                    title=f"{ch_slug} Signals",
                    color="signal",
                    color_discrete_map=SIGNAL_COLORS,
                )
                render_chart(fig, key=f"ch_{ch_slug}_signals")

            with col_b:
                dates = [v.get("published_at", "") for v in ch_videos]
                if any(dates):
                    df_timeline = pd.DataFrame({"date": dates, "signal": signal_classes})
                    df_timeline = df_timeline[df_timeline["date"] != ""]
                    if not df_timeline.empty:
                        timeline_counts = df_timeline.groupby(["date", "signal"]).size().reset_index(name="count")
                        fig_timeline = px.bar(
                            timeline_counts, x="date", y="count", color="signal",
                            title=f"{ch_slug} -- 30-Day Timeline",
                            color_discrete_map=SIGNAL_COLORS,
                        )
                        render_chart(fig_timeline, key=f"ch_{ch_slug}_timeline")

            with st.expander("Video Details", expanded=False):
                df_vids = pd.DataFrame(ch_videos)
                mobile_cols = [c for c in ["title", "video_signal_class", "signal_score", "published_at"] if c in df_vids.columns]
                st.dataframe(df_vids[mobile_cols] if mobile_cols else df_vids, use_container_width=True, height=400)

        # Ranking
        ch_ranking = extract_cross_video_ranking(ch_data)
        if ch_ranking:
            with st.expander("Cross-Video Ranking", expanded=False):
                st.dataframe(pd.DataFrame(ch_ranking), use_container_width=True, height=350)

# =============================================================================
# Channel Comparison
# =============================================================================

with tabs[-2]:
    st.markdown("#### Channel Comparison")
    comp_data = load_channel_comparison(OUTPUT_DIR)

    if comp_data and "channels" in comp_data:
        channels_info = comp_data["channels"]

        rows = []
        for slug, info in channels_info.items():
            row = {"channel": info.get("display_name", slug)}
            row["videos"] = info.get("total_videos", 0)
            row["actionable"] = info.get("actionable_videos", 0)
            row["ratio"] = f"{info.get('actionable_ratio', 0.0):.0%}"
            sc = info.get("quality_scorecard", {})
            row["quality"] = f"{sc.get('overall', 0.0):.1f}"
            rows.append(row)

        df_comp = pd.DataFrame(rows)
        st.dataframe(df_comp, use_container_width=True)

        if len(rows) >= 2:
            chart_rows = []
            for slug, info in channels_info.items():
                sc = info.get("quality_scorecard", {})
                name = info.get("display_name", slug)
                for metric in ["overall", "transcript_coverage", "ranking_predictive_power"]:
                    chart_rows.append({
                        "channel": name,
                        "metric": metric.replace("_", " ").title(),
                        "score": sc.get(metric, 0.0),
                    })
            df_chart = pd.DataFrame(chart_rows)
            fig_comp = px.bar(
                df_chart, x="metric", y="score", color="channel",
                barmode="group", title="Channel Quality Metrics",
            )
            render_chart(fig_comp, key="compare_chart")

        st.markdown(f"**More Actionable:** {comp_data.get('more_actionable_channel', 'N/A')}")
        st.markdown(f"**Better Ranking:** {comp_data.get('better_ranking_channel', 'N/A')}")
    else:
        st.info("No channel comparison data found.")

# =============================================================================
# Status Tab (US-004)
# =============================================================================

with tabs[-1]:
    st.markdown("#### Pipeline Status")

    # Last update info
    if last_update:
        now_utc = datetime.now(tz=timezone.utc)
        age_min = (now_utc - last_update).total_seconds() / 60
        status_text = "Active" if age_min < 120 else "Idle"
        status_color = "#22C55E" if age_min < 120 else "#F59E0B"

        render_metrics_row([
            ("Status", status_text),
            ("Last Run", last_update.strftime("%Y-%m-%d %H:%M")),
            ("Age", f"{age_min:.0f} min ago"),
            ("Channels", str(len(available_channels))),
        ], cols_desktop=4)

    # Pipeline activity log
    activity = get_pipeline_activity(OUTPUT_DIR)
    if activity:
        st.markdown("#### Recent Pipeline Activity")
        rows_html = ""
        for entry in activity:
            ts = entry["timestamp"].strftime("%Y-%m-%d %H:%M")
            rows_html += (
                f'<div class="status-row">'
                f'<div class="status-dot"></div>'
                f'<div class="status-time">{ts}</div>'
                f'<div class="status-label">{entry["channel"]}</div>'
                f'</div>'
            )
        st.markdown(f'<div class="omx-card">{rows_html}</div>', unsafe_allow_html=True)
    else:
        st.info("No pipeline activity recorded yet.")

    # Auto-refresh indicator
    st.markdown(
        '<div style="text-align:center;color:#64748B;font-size:0.75rem;margin-top:2rem;">'
        'Auto-refreshing every 60 seconds</div>',
        unsafe_allow_html=True,
    )
