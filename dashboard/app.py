"""OMX Brainstorm Streamlit Dashboard — mobile-first responsive design."""
from __future__ import annotations

import os
import streamlit as st
import plotly.express as px
import pandas as pd

from data_loader import (
    DEFAULT_OUTPUT_DIR,
    extract_cross_video_ranking,
    extract_expert_insights,
    extract_macro_signals,
    extract_per_video,
    extract_signal_distribution,
    extract_type_distribution,
    extract_videos,
    get_available_channels,
    load_30d_results,
    load_channel_comparison,
    load_integration_report,
    load_video_titles,
)

# -- Page config (no wide layout for mobile) ----------------------------------

st.set_page_config(
    page_title="OMX Dashboard",
    page_icon=":chart_with_upwards_trend:",
    layout="centered",
    initial_sidebar_state="collapsed",
)

# -- Mobile-first CSS ---------------------------------------------------------

st.markdown("""
<style>
/* ── Base: mobile-first (Galaxy Z Fold7 cover: ~375px logical) ── */

/* Larger base font for touch readability */
html, body, [class*="css"] {
    font-size: 16px !important;
}

/* Streamlit block container: reduce padding on mobile */
.block-container {
    padding-left: 1rem !important;
    padding-right: 1rem !important;
    padding-top: 1rem !important;
    max-width: 100% !important;
}

/* Metric cards: larger text, better spacing */
[data-testid="stMetric"] {
    background: linear-gradient(135deg, #1e1e2e 0%, #2d2d44 100%);
    border: 1px solid rgba(255,255,255,0.08);
    border-radius: 12px;
    padding: 0.75rem 1rem;
    margin-bottom: 0.5rem;
}
[data-testid="stMetricValue"] {
    font-size: 1.4rem !important;
    font-weight: 700 !important;
}
[data-testid="stMetricLabel"] {
    font-size: 0.8rem !important;
    text-transform: uppercase;
    letter-spacing: 0.03em;
    opacity: 0.7;
}

/* Tabs: scrollable, larger touch targets */
.stTabs [data-baseweb="tab-list"] {
    gap: 0px;
    overflow-x: auto;
    -webkit-overflow-scrolling: touch;
    scrollbar-width: none;
    flex-wrap: nowrap;
}
.stTabs [data-baseweb="tab-list"]::-webkit-scrollbar {
    display: none;
}
.stTabs [data-baseweb="tab"] {
    min-height: 48px;
    padding: 0 16px;
    font-size: 0.85rem !important;
    white-space: nowrap;
    flex-shrink: 0;
}

/* Expanders: larger touch target */
.streamlit-expanderHeader {
    font-size: 1rem !important;
    min-height: 48px;
    display: flex;
    align-items: center;
}

/* DataFrames: horizontal scroll wrapper */
[data-testid="stDataFrame"] {
    overflow-x: auto;
    -webkit-overflow-scrolling: touch;
}

/* Plotly charts: minimum height on mobile */
.js-plotly-plot {
    min-height: 280px;
}

/* Buttons and inputs: 48px min touch target (WCAG) */
.stButton > button,
.stTextInput > div > div > input,
.stSelectbox > div > div {
    min-height: 48px !important;
    font-size: 1rem !important;
}

/* Hide sidebar toggle hint on mobile — we use tabs instead */
[data-testid="collapsedControl"] {
    display: none;
}

/* ── Tablet/desktop enhancements (Galaxy Z Fold7 inner: ~600px+) ── */
@media (min-width: 600px) {
    .block-container {
        padding-left: 2rem !important;
        padding-right: 2rem !important;
        max-width: 900px !important;
    }
    [data-testid="stMetricValue"] {
        font-size: 1.6rem !important;
    }
}

/* ── Desktop (>960px) ── */
@media (min-width: 960px) {
    .block-container {
        max-width: 1100px !important;
    }
}
</style>
""", unsafe_allow_html=True)

# -- Auth gate ----------------------------------------------------------------

DASHBOARD_TOKEN = os.environ.get("DASHBOARD_TOKEN", "jS-GpK2lpXoeLGGm17hRSmmPoAQxahs3")

query_token = st.query_params.get("token", "")
if query_token != DASHBOARD_TOKEN:
    st.error("Access denied. Append ?token=<your-token> to the URL.")
    st.stop()

st.title("OMX Dashboard")

OUTPUT_DIR = DEFAULT_OUTPUT_DIR

# -- Load channels (replaces sidebar) -----------------------------------------

available_channels = get_available_channels(OUTPUT_DIR)
if not available_channels:
    st.warning("No output data found. Run the pipeline first.")
    st.stop()

# -- Helper: responsive metrics ------------------------------------------------


def render_metrics_row(metrics: list[tuple[str, str]], cols_desktop: int = 4) -> None:
    """Render metrics in a 2-column grid (mobile) or N-column grid (desktop).

    Each item is a (label, value) tuple.
    Streamlit auto-stacks columns on very narrow viewports, but we help
    by using 2 columns as the base to keep metrics readable on ~375px screens.
    """
    col_count = min(len(metrics), cols_desktop)
    cols = st.columns(col_count)
    for i, (label, value) in enumerate(metrics):
        cols[i % col_count].metric(label, value)


def render_chart(fig, key: str | None = None) -> None:
    """Render a Plotly chart with mobile-friendly defaults."""
    fig.update_layout(
        margin=dict(l=16, r=16, t=40, b=16),
        font=dict(size=13),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=-0.25,
            xanchor="center",
            x=0.5,
            font=dict(size=11),
        ),
        height=320,
    )
    st.plotly_chart(fig, use_container_width=True, key=key)


# -- Build tab names (short labels for mobile) --------------------------------

tab_labels = [
    "Overview",
    "Ranking",
    "Macro",
    "Expert",
] + [ch[:12] for ch in available_channels] + ["Compare"]

tabs = st.tabs(tab_labels)

# ==============================================================================
# TAB 0 — Overview
# ==============================================================================

with tabs[0]:
    st.header("Overview")
    report = load_integration_report(OUTPUT_DIR)

    if report:
        # Pie chart — video type distribution (full width on mobile)
        type_dist = extract_type_distribution(report)
        if type_dist:
            df_type = pd.DataFrame(
                {"type": list(type_dist.keys()), "count": list(type_dist.values())}
            )
            fig_pie = px.pie(
                df_type, names="type", values="count",
                title="Video Type Distribution",
                color_discrete_sequence=px.colors.qualitative.Set2,
            )
            render_chart(fig_pie, key="overview_pie")

        # Bar chart — signal distribution (full width on mobile)
        sig_dist = extract_signal_distribution(report)
        if sig_dist:
            df_sig = pd.DataFrame(
                {"signal": list(sig_dist.keys()), "count": list(sig_dist.values())}
            )
            fig_bar = px.bar(
                df_sig, x="signal", y="count",
                title="Signal Distribution",
                color="signal",
                color_discrete_map={"ACTIONABLE": "#2ecc71", "NOISE": "#e74c3c"},
            )
            render_chart(fig_bar, key="overview_bar")

        # Summary metrics — 2x2 grid on mobile, 4-col on desktop
        st.subheader("Pipeline Summary")
        render_metrics_row([
            ("Total Videos", str(report.get("total_videos", 0))),
            ("Analyzable", str(report.get("analyzable_count", 0))),
            ("Expert Rate", str(report.get("expert_extraction_rate", "N/A"))),
            ("Macro Coverage", str(report.get("macro_coverage", "N/A"))),
        ], cols_desktop=4)

        # Per-video table — compact columns for mobile
        per_video = extract_per_video(report)
        if per_video:
            st.subheader("Per-Video Breakdown")
            df_pv = pd.DataFrame(per_video)
            mobile_cols = [c for c in ["title", "video_type", "signal_class", "signal_score"] if c in df_pv.columns]
            st.dataframe(df_pv[mobile_cols] if mobile_cols else df_pv, use_container_width=True, height=350)

    else:
        st.info("No integration report found. Run the sampro pipeline to generate data.")

    # Video titles label distribution
    titles_data = load_video_titles(OUTPUT_DIR)
    if titles_data and "titles" in titles_data:
        st.subheader("Title Labels")
        all_labels: list[str] = []
        for t in titles_data["titles"]:
            all_labels.extend(t.get("labels", []))
        if all_labels:
            label_counts = pd.Series(all_labels).value_counts().reset_index()
            label_counts.columns = ["label", "count"]
            fig_labels = px.bar(
                label_counts, x="label", y="count",
                title="Content Labels",
                color="label",
                color_discrete_sequence=px.colors.qualitative.Pastel,
            )
            render_chart(fig_labels, key="overview_labels")

# ==============================================================================
# TAB 1 — Stock Ranking
# ==============================================================================

with tabs[1]:
    st.header("Stock Ranking")
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

        st.dataframe(df_rank, use_container_width=True, height=450)
    else:
        st.info(f"No ranking data for '{ranking_channel}'.")

# ==============================================================================
# TAB 2 — Macro Signals
# ==============================================================================

with tabs[2]:
    st.header("Macro Signals")
    macro_channel = st.selectbox("Channel", available_channels, key="macro_ch")
    macro_30d = load_30d_results(macro_channel, OUTPUT_DIR)
    videos = extract_videos(macro_30d)
    macro_signals = extract_macro_signals(videos)

    if macro_signals:
        df_macro = pd.DataFrame(macro_signals)
        # Show fewer columns on mobile
        mobile_cols = [c for c in ["indicator", "direction", "confidence", "sentiment"] if c in df_macro.columns]
        st.dataframe(df_macro[mobile_cols] if mobile_cols else df_macro, use_container_width=True, height=350)

        if "direction" in df_macro.columns:
            st.subheader("Direction Breakdown")
            dir_counts = df_macro["direction"].value_counts().reset_index()
            dir_counts.columns = ["direction", "count"]
            fig_dir = px.pie(
                dir_counts, names="direction", values="count",
                title="Macro Signal Directions",
                color_discrete_map={"UP": "#2ecc71", "DOWN": "#e74c3c", "NEUTRAL": "#95a5a6"},
            )
            render_chart(fig_dir, key="macro_pie")
    else:
        st.info(f"No macro signals for '{macro_channel}'.")

# ==============================================================================
# TAB 3 — Expert Insights
# ==============================================================================

with tabs[3]:
    st.header("Expert Insights")
    expert_channel = st.selectbox("Channel", available_channels, key="expert_ch")
    expert_30d = load_30d_results(expert_channel, OUTPUT_DIR)
    expert_videos = extract_videos(expert_30d)
    insights = extract_expert_insights(expert_videos)

    if insights:
        for i, insight in enumerate(insights):
            expert_name = insight.get("expert_name", "Unknown")
            affiliation = insight.get("affiliation", "")
            label = f"{expert_name} — {affiliation}" if affiliation else expert_name

            with st.expander(label, expanded=(i < 2)):
                # Use vertical layout instead of cramped columns
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

# ==============================================================================
# CHANNEL TABS (dynamic)
# ==============================================================================

for idx, ch_slug in enumerate(available_channels):
    tab_idx = 4 + idx
    with tabs[tab_idx]:
        st.header(f"Channel: {ch_slug}")
        ch_data = load_30d_results(ch_slug, OUTPUT_DIR)

        if not ch_data:
            st.info(f"No 30-day data for '{ch_slug}'.")
            continue

        # Channel info — 2-col on mobile (3rd wraps)
        render_metrics_row([
            ("Channel", ch_data.get("channel_name", ch_slug)),
            ("Window", f"{ch_data.get('window_days', 30)}d"),
            ("Generated", ch_data.get("generated_at", "N/A")),
        ], cols_desktop=3)

        # Quality scorecard — 2-col on mobile, stacked
        scorecard = ch_data.get("quality_scorecard", {})
        if scorecard:
            st.subheader("Quality Scorecard")
            render_metrics_row([
                ("Overall", f"{scorecard.get('overall', 0):.1f}"),
                ("Transcript", f"{scorecard.get('transcript_coverage', 0):.1f}"),
                ("Actionable", f"{scorecard.get('actionable_density', 0):.1f}"),
                ("Ranking", f"{scorecard.get('ranking_predictive_power', 0):.1f}"),
            ], cols_desktop=4)

        # Videos
        ch_videos = extract_videos(ch_data)
        if ch_videos:
            st.subheader(f"Videos ({len(ch_videos)})")

            signal_classes = [v.get("video_signal_class", "UNKNOWN") for v in ch_videos]
            sig_series = pd.Series(signal_classes).value_counts().reset_index()
            sig_series.columns = ["signal", "count"]
            fig = px.bar(
                sig_series, x="signal", y="count",
                title=f"{ch_slug} Signals",
                color="signal",
                color_discrete_map={"ACTIONABLE": "#2ecc71", "NOISE": "#e74c3c"},
            )
            render_chart(fig, key=f"ch_{ch_slug}_signals")

            # Timeline
            dates = [v.get("published_at", "") for v in ch_videos]
            if any(dates):
                df_timeline = pd.DataFrame({"date": dates, "signal": signal_classes})
                df_timeline = df_timeline[df_timeline["date"] != ""]
                if not df_timeline.empty:
                    timeline_counts = df_timeline.groupby(["date", "signal"]).size().reset_index(name="count")
                    fig_timeline = px.bar(
                        timeline_counts, x="date", y="count", color="signal",
                        title=f"{ch_slug} — 30-Day Timeline",
                        color_discrete_map={"ACTIONABLE": "#2ecc71", "NOISE": "#e74c3c"},
                    )
                    render_chart(fig_timeline, key=f"ch_{ch_slug}_timeline")

            # Video table — mobile-friendly columns
            df_vids = pd.DataFrame(ch_videos)
            mobile_cols = [c for c in ["title", "video_signal_class", "signal_score", "published_at"] if c in df_vids.columns]
            st.dataframe(df_vids[mobile_cols] if mobile_cols else df_vids, use_container_width=True, height=350)

        # Ranking
        ch_ranking = extract_cross_video_ranking(ch_data)
        if ch_ranking:
            st.subheader("Cross-Video Ranking")
            st.dataframe(pd.DataFrame(ch_ranking), use_container_width=True, height=300)

# ==============================================================================
# LAST TAB — Channel Comparison
# ==============================================================================

with tabs[-1]:
    st.header("Channel Comparison")
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

        # Quality comparison chart
        if len(rows) >= 2:
            st.subheader("Quality Comparison")
            # Rebuild with numeric values for charting
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
