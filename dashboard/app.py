"""OMX Brainstorm Streamlit Dashboard — visualise output/ pipeline results."""
from __future__ import annotations

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

# ── Page config ──────────────────────────────────────────────────────────────

st.set_page_config(page_title="OMX Dashboard", page_icon=":chart_with_upwards_trend:", layout="wide")
st.title("OMX Brainstorm Dashboard")

OUTPUT_DIR = DEFAULT_OUTPUT_DIR

# ── Sidebar ──────────────────────────────────────────────────────────────────

st.sidebar.header("Settings")
available_channels = get_available_channels(OUTPUT_DIR)
if not available_channels:
    st.warning("No output data found. Run the pipeline first.")
    st.stop()

# ── Tabs ─────────────────────────────────────────────────────────────────────

tab_names = ["Overview", "Stock Ranking", "Macro Signals", "Expert Insights"] + [
    f"Channel: {ch}" for ch in available_channels
] + ["Channel Comparison"]

tabs = st.tabs(tab_names)

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 0 — Overview (content type distribution)
# ═══════════════════════════════════════════════════════════════════════════════

with tabs[0]:
    st.header("Content Type Distribution")
    report = load_integration_report(OUTPUT_DIR)

    if report:
        col1, col2 = st.columns(2)

        # Pie chart — video type distribution
        type_dist = extract_type_distribution(report)
        if type_dist:
            df_type = pd.DataFrame(
                {"type": list(type_dist.keys()), "count": list(type_dist.values())}
            )
            with col1:
                fig_pie = px.pie(
                    df_type, names="type", values="count",
                    title="Video Type Distribution",
                    color_discrete_sequence=px.colors.qualitative.Set2,
                )
                st.plotly_chart(fig_pie, use_container_width=True)

        # Bar chart — signal distribution
        sig_dist = extract_signal_distribution(report)
        if sig_dist:
            df_sig = pd.DataFrame(
                {"signal": list(sig_dist.keys()), "count": list(sig_dist.values())}
            )
            with col2:
                fig_bar = px.bar(
                    df_sig, x="signal", y="count",
                    title="Signal Distribution (ACTIONABLE vs NOISE)",
                    color="signal",
                    color_discrete_map={"ACTIONABLE": "#2ecc71", "NOISE": "#e74c3c"},
                )
                st.plotly_chart(fig_bar, use_container_width=True)

        # Summary metrics
        st.subheader("Pipeline Summary")
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Total Videos", report.get("total_videos", 0))
        m2.metric("Analyzable", report.get("analyzable_count", 0))
        m3.metric("Expert Extraction", report.get("expert_extraction_rate", "N/A"))
        m4.metric("Macro Coverage", report.get("macro_coverage", "N/A"))

        # Per-video table
        per_video = extract_per_video(report)
        if per_video:
            st.subheader("Per-Video Signal Breakdown")
            df_pv = pd.DataFrame(per_video)
            display_cols = [c for c in ["video_id", "title", "video_type", "signal_class", "signal_score", "should_analyze", "macro_count", "expert_count"] if c in df_pv.columns]
            st.dataframe(df_pv[display_cols], use_container_width=True, height=400)

    else:
        st.info("No integration report found. Run the sampro pipeline to generate data.")

    # Video titles label distribution (from sampro_video_titles.json)
    titles_data = load_video_titles(OUTPUT_DIR)
    if titles_data and "titles" in titles_data:
        st.subheader("Title-Based Label Distribution")
        all_labels: list[str] = []
        for t in titles_data["titles"]:
            all_labels.extend(t.get("labels", []))
        if all_labels:
            label_counts = pd.Series(all_labels).value_counts().reset_index()
            label_counts.columns = ["label", "count"]
            fig_labels = px.bar(
                label_counts, x="label", y="count",
                title="Content Labels from Video Titles",
                color="label",
                color_discrete_sequence=px.colors.qualitative.Pastel,
            )
            st.plotly_chart(fig_labels, use_container_width=True)

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 1 — Stock Ranking
# ═══════════════════════════════════════════════════════════════════════════════

with tabs[1]:
    st.header("Stock Ranking")
    ranking_channel = st.selectbox("Select channel for ranking", available_channels, key="rank_ch")
    data_30d = load_30d_results(ranking_channel, OUTPUT_DIR)
    ranking = extract_cross_video_ranking(data_30d)

    if ranking:
        df_rank = pd.DataFrame(ranking)
        # Filter
        filter_text = st.text_input("Filter by ticker or company name", "", key="rank_filter")
        if filter_text:
            mask = df_rank.apply(lambda row: filter_text.upper() in str(row.values).upper(), axis=1)
            df_rank = df_rank[mask]

        st.dataframe(
            df_rank.sort_values(by="total_score", ascending=False) if "total_score" in df_rank.columns else df_rank,
            use_container_width=True,
            height=500,
        )
    else:
        st.info(f"No ranking data for channel '{ranking_channel}'. Run the 30-day pipeline first.")

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 2 — Macro Signals
# ═══════════════════════════════════════════════════════════════════════════════

with tabs[2]:
    st.header("Macro Signal Summary")
    macro_channel = st.selectbox("Select channel", available_channels, key="macro_ch")
    macro_30d = load_30d_results(macro_channel, OUTPUT_DIR)
    videos = extract_videos(macro_30d)
    macro_signals = extract_macro_signals(videos)

    if macro_signals:
        df_macro = pd.DataFrame(macro_signals)
        display_cols = [c for c in ["indicator", "direction", "label", "confidence", "sentiment", "beneficiary_sectors", "source_video"] if c in df_macro.columns]
        st.dataframe(df_macro[display_cols] if display_cols else df_macro, use_container_width=True, height=400)

        # Direction summary
        if "direction" in df_macro.columns:
            st.subheader("Direction Breakdown")
            dir_counts = df_macro["direction"].value_counts().reset_index()
            dir_counts.columns = ["direction", "count"]
            fig_dir = px.pie(dir_counts, names="direction", values="count", title="Macro Signal Directions",
                             color_discrete_map={"UP": "#2ecc71", "DOWN": "#e74c3c", "NEUTRAL": "#95a5a6"})
            st.plotly_chart(fig_dir, use_container_width=True)
    else:
        st.info(f"No macro signals for '{macro_channel}'.")

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 3 — Expert Insights
# ═══════════════════════════════════════════════════════════════════════════════

with tabs[3]:
    st.header("Expert Insights")
    expert_channel = st.selectbox("Select channel", available_channels, key="expert_ch")
    expert_30d = load_30d_results(expert_channel, OUTPUT_DIR)
    expert_videos = extract_videos(expert_30d)
    insights = extract_expert_insights(expert_videos)

    if insights:
        for i, insight in enumerate(insights):
            with st.expander(f"{insight.get('expert_name', 'Unknown')} — {insight.get('affiliation', '')}", expanded=(i < 3)):
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
                        st.markdown(f"  {icon} **{sc.get('claim', '')}** (confidence: {conf:.0%}, {direction})")
                        if sc.get("reasoning"):
                            st.caption(f"    Reasoning: {sc['reasoning']}")

                tickers = insight.get("mentioned_tickers", [])
                if tickers:
                    st.markdown(f"**Mentioned Tickers:** {', '.join(tickers)}")
    else:
        st.info(f"No expert insights for '{expert_channel}'.")

# ═══════════════════════════════════════════════════════════════════════════════
# CHANNEL TABS (dynamic)
# ═══════════════════════════════════════════════════════════════════════════════

for idx, ch_slug in enumerate(available_channels):
    tab_idx = 4 + idx
    with tabs[tab_idx]:
        st.header(f"Channel: {ch_slug}")
        ch_data = load_30d_results(ch_slug, OUTPUT_DIR)

        if not ch_data:
            st.info(f"No 30-day data for '{ch_slug}'.")
            continue

        # Channel info
        c1, c2, c3 = st.columns(3)
        c1.metric("Channel", ch_data.get("channel_name", ch_slug))
        c2.metric("Window", f"{ch_data.get('window_days', 30)} days")
        c3.metric("Generated", ch_data.get("generated_at", "N/A"))

        # Quality scorecard
        scorecard = ch_data.get("quality_scorecard", {})
        if scorecard:
            st.subheader("Quality Scorecard")
            sc1, sc2, sc3, sc4, sc5 = st.columns(5)
            sc1.metric("Overall", f"{scorecard.get('overall', 0):.1f}")
            sc2.metric("Transcript", f"{scorecard.get('transcript_coverage', 0):.1f}")
            sc3.metric("Actionable Density", f"{scorecard.get('actionable_density', 0):.1f}")
            sc4.metric("Ranking Power", f"{scorecard.get('ranking_predictive_power', 0):.1f}")
            sc5.metric("Horizon Adequacy", f"{scorecard.get('horizon_adequacy', 0):.1f}")

        # Videos in this channel
        ch_videos = extract_videos(ch_data)
        if ch_videos:
            st.subheader(f"Videos ({len(ch_videos)})")

            # Signal distribution for this channel
            signal_classes = [v.get("video_signal_class", "UNKNOWN") for v in ch_videos]
            sig_series = pd.Series(signal_classes).value_counts().reset_index()
            sig_series.columns = ["signal", "count"]
            fig = px.bar(sig_series, x="signal", y="count", title=f"{ch_slug} Signal Distribution",
                         color="signal", color_discrete_map={"ACTIONABLE": "#2ecc71", "NOISE": "#e74c3c"})
            st.plotly_chart(fig, use_container_width=True)

            # Timeline: videos by published date
            dates = [v.get("published_at", "") for v in ch_videos]
            if any(dates):
                df_timeline = pd.DataFrame({"date": dates, "signal": signal_classes})
                df_timeline = df_timeline[df_timeline["date"] != ""]
                if not df_timeline.empty:
                    timeline_counts = df_timeline.groupby(["date", "signal"]).size().reset_index(name="count")
                    fig_timeline = px.bar(
                        timeline_counts, x="date", y="count", color="signal",
                        title=f"{ch_slug} — 30-Day Video Timeline",
                        color_discrete_map={"ACTIONABLE": "#2ecc71", "NOISE": "#e74c3c"},
                    )
                    st.plotly_chart(fig_timeline, use_container_width=True)

            # Video table
            df_vids = pd.DataFrame(ch_videos)
            display_cols = [c for c in ["video_id", "title", "video_signal_class", "signal_score", "published_at"] if c in df_vids.columns]
            st.dataframe(df_vids[display_cols] if display_cols else df_vids, use_container_width=True, height=400)

        # Ranking
        ch_ranking = extract_cross_video_ranking(ch_data)
        if ch_ranking:
            st.subheader("Cross-Video Ranking")
            st.dataframe(pd.DataFrame(ch_ranking), use_container_width=True, height=300)

# ═══════════════════════════════════════════════════════════════════════════════
# LAST TAB — Channel Comparison
# ═══════════════════════════════════════════════════════════════════════════════

with tabs[-1]:
    st.header("Channel Comparison (30-Day)")
    comp_data = load_channel_comparison(OUTPUT_DIR)

    if comp_data and "channels" in comp_data:
        channels_info = comp_data["channels"]

        # Comparison table
        rows = []
        for slug, info in channels_info.items():
            row = {"channel": info.get("display_name", slug)}
            row["total_videos"] = info.get("total_videos", 0)
            row["actionable_videos"] = info.get("actionable_videos", 0)
            row["actionable_ratio"] = info.get("actionable_ratio", 0.0)
            sc = info.get("quality_scorecard", {})
            row["overall_quality"] = sc.get("overall", 0.0)
            row["transcript_coverage"] = sc.get("transcript_coverage", 0.0)
            row["ranking_power"] = sc.get("ranking_predictive_power", 0.0)
            rows.append(row)

        df_comp = pd.DataFrame(rows)
        st.dataframe(df_comp, use_container_width=True)

        # Radar chart for quality scorecards
        if len(rows) >= 2:
            st.subheader("Quality Scorecard Comparison")
            categories = ["overall_quality", "transcript_coverage", "ranking_power"]
            fig_radar = px.bar(
                df_comp.melt(id_vars="channel", value_vars=categories, var_name="metric", value_name="score"),
                x="metric", y="score", color="channel", barmode="group",
                title="Channel Quality Metrics",
            )
            st.plotly_chart(fig_radar, use_container_width=True)

        st.markdown(f"**More Actionable Channel:** {comp_data.get('more_actionable_channel', 'N/A')}")
        st.markdown(f"**Better Ranking Channel:** {comp_data.get('better_ranking_channel', 'N/A')}")
    else:
        st.info("No channel comparison data found.")
