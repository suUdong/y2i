"""Insight-first Y2I dashboard."""
from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
from streamlit_autorefresh import st_autorefresh

import dashboard.data_loader as data_loader_runtime
from dashboard.auth import (
    AUTH_COOKIE_NAME,
    build_cookie_clear_html,
    build_cookie_sync_html,
    resolve_dashboard_auth,
)
from dashboard.data_loader import (
    extract_channel_leaderboard,
    get_all_rankings,
    get_available_channels,
    get_channel_display_names,
    get_live_feed_data,
    load_30d_results,
    load_channel_comparison,
    build_overview_report,
)
from dashboard.insight_view import (
    build_channel_focus_payload,
    build_header_metrics,
    build_priority_signal_rows,
    build_recent_video_rows,
    build_stock_insight_cards,
)
from omx_brainstorm.utils import load_env_file

st.set_page_config(
    page_title="Y2I 투자 시그널",
    page_icon=":chart_with_upwards_trend:",
    layout="wide",
    initial_sidebar_state="collapsed",
)

dotenv_payload = load_env_file(os.getenv("OMX_ENV_PATH", str(Path(__file__).resolve().parent.parent / ".env")))

DEFAULT_DASHBOARD_AUTH_TOKEN = "6149ba10085f1be3"
DASHBOARD_AUTH_TOKEN = os.getenv("DASHBOARD_AUTH_TOKEN") or dotenv_payload.get("DASHBOARD_AUTH_TOKEN") or DEFAULT_DASHBOARD_AUTH_TOKEN


def render_forbidden() -> None:
    st.markdown(
        """
        <style>
        .auth-shell {
            min-height: 100vh;
            display: flex;
            align-items: center;
            justify-content: center;
            padding: 2rem;
        }
        .auth-card {
            width: min(520px, 100%);
            border: 1px solid rgba(239,68,68,0.28);
            border-radius: 18px;
            padding: 2rem 1.5rem;
            background: rgba(15, 23, 42, 0.96);
            text-align: center;
            box-shadow: 0 12px 40px rgba(15, 23, 42, 0.35);
        }
        .auth-code {
            color: #ef4444;
            font-size: 0.8rem;
            font-weight: 800;
            letter-spacing: 0.12em;
            text-transform: uppercase;
        }
        .auth-title {
            font-size: 2rem;
            font-weight: 800;
            margin-top: 0.5rem;
            color: #f8fafc;
        }
        .auth-copy {
            margin-top: 0.75rem;
            color: #cbd5e1;
            line-height: 1.6;
        }
        </style>
        <div class="auth-shell">
            <div class="auth-card">
                <div class="auth-code">403 Forbidden</div>
                <div class="auth-title">접근이 차단되었습니다</div>
                <div class="auth-copy">유효한 대시보드 접근 토큰이 있을 때만 인사이트 화면을 렌더링합니다.</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.stop()


query_token = st.query_params.get("token")
cookie_token = st.context.cookies.get(AUTH_COOKIE_NAME)
auth_decision = resolve_dashboard_auth(
    query_token=query_token,
    cookie_token=cookie_token,
    expected_token=DASHBOARD_AUTH_TOKEN,
)

if auth_decision.should_clear_cookie:
    components.html(build_cookie_clear_html(AUTH_COOKIE_NAME), height=0, width=0)

if not auth_decision.is_authenticated:
    render_forbidden()

if auth_decision.should_set_cookie and auth_decision.token_to_persist is not None:
    components.html(
        build_cookie_sync_html(AUTH_COOKIE_NAME, auth_decision.token_to_persist),
        height=0,
        width=0,
    )

st_autorefresh(interval=60_000, limit=None, key="auto_refresh")

st.markdown(
    """
    <style>
    .block-container {
        max-width: 1200px !important;
        padding-top: 1.2rem !important;
        padding-bottom: 3rem !important;
    }
    .insight-note {
        padding: 0.9rem 1rem;
        border: 1px solid rgba(148,163,184,0.18);
        border-radius: 14px;
        background: rgba(15, 23, 42, 0.03);
        margin-bottom: 1rem;
    }
    .insight-card {
        border: 1px solid rgba(148,163,184,0.18);
        border-radius: 16px;
        padding: 1rem 1.1rem;
        background: linear-gradient(180deg, rgba(255,255,255,0.98), rgba(248,250,252,0.98));
        margin-bottom: 0.75rem;
    }
    .insight-eyebrow {
        color: #475569;
        font-size: 0.78rem;
        font-weight: 700;
        letter-spacing: 0.04em;
        text-transform: uppercase;
    }
    .insight-headline {
        font-size: 1.08rem;
        font-weight: 800;
        color: #0f172a;
        margin-top: 0.2rem;
    }
    .insight-copy {
        color: #334155;
        line-height: 1.6;
        margin-top: 0.35rem;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


def _render_stock_insight_cards(cards: list[dict], *, empty_message: str) -> None:
    if not cards:
        st.info(empty_message)
        return

    for idx, card in enumerate(cards):
        title = f"{card['ticker_display']} · {card['verdict']} · {card['score']:.1f}"
        with st.expander(title, expanded=(idx == 0)):
            st.markdown(
                (
                    "<div class='insight-card'>"
                    f"<div class='insight-eyebrow'>{card['signal_note']} · Conviction {card['conviction']}</div>"
                    f"<div class='insight-headline'>{card['ticker_display']}</div>"
                    f"<div class='insight-copy'>{card['why_now']}</div>"
                    "</div>"
                ),
                unsafe_allow_html=True,
            )
            metric_columns = st.columns(4)
            metric_columns[0].metric("Verdict", card["verdict"])
            metric_columns[1].metric("Score", f"{card['score']:.1f}")
            metric_columns[2].metric("Signal", card["signal_kind"])
            metric_columns[3].metric("Channels", str(card["channel_count"]))

            if card.get("video_context_summary"):
                st.markdown(f"**영상 문맥**  \n{card['video_context_summary']}")
            if card.get("plain_summary"):
                st.markdown(f"**쉽게 읽는 판단**  \n{card['plain_summary']}")

            left_col, right_col = st.columns([1.15, 0.85], gap="large")
            with left_col:
                if card.get("expert_views"):
                    st.markdown("**유튜브 전문가 의견**")
                    for view in card["expert_views"]:
                        st.markdown(
                            f"- **{view['expert']}** ({view['topic']}) · {view['sentiment']} · {view['summary']}"
                        )
                        for claim in view.get("claims", [])[:2]:
                            claim_text = claim.get("claim") or ""
                            if claim_text:
                                confidence = float(claim.get("confidence", 0) or 0)
                                st.caption(
                                    f"{claim.get('direction', 'NEUTRAL')} {confidence:.0%} · {claim_text}"
                                )
                if card.get("supporting_videos"):
                    st.markdown("**근거 영상**")
                    for video in card["supporting_videos"]:
                        st.markdown(
                            f"- {video['title']} · {video['verdict']} {video['score']:.1f} · {video['summary']}"
                        )

            with right_col:
                if card.get("master_views"):
                    st.markdown("**구루 관점**")
                    for view in card["master_views"]:
                        st.markdown(
                            f"- **{view['master']}** · {view['verdict']} {view['score']:.1f} · {view['one_liner']}"
                        )
                if card.get("analyst_summary") or card.get("price_target_summary") or card.get("fundamental_rows"):
                    st.markdown("**재무 / 컨센서스**")
                    if card.get("analyst_summary"):
                        st.markdown(f"- 컨센서스: {card['analyst_summary']}")
                    if card.get("price_target_summary"):
                        st.markdown(f"- 목표가: {card['price_target_summary']}")
                    for row in card.get("fundamental_rows", []):
                        st.markdown(f"- {row['label']}: {row['value']}")


OUTPUT_DIR = data_loader_runtime.DEFAULT_OUTPUT_DIR
comparison = load_channel_comparison(OUTPUT_DIR)
overview = build_overview_report(OUTPUT_DIR)
live_feed = get_live_feed_data(OUTPUT_DIR, hours=72)
channel_names = get_channel_display_names(OUTPUT_DIR)
rankings = get_all_rankings(OUTPUT_DIR)
leaderboard = extract_channel_leaderboard(comparison)
available_channels = get_available_channels(OUTPUT_DIR)

header_metrics = build_header_metrics(
    overview=overview if isinstance(overview, dict) else {},
    comparison=comparison if isinstance(comparison, dict) else {},
    live_feed=live_feed if isinstance(live_feed, dict) else {},
)
priority_rows = build_priority_signal_rows(rankings, limit=10)
recent_rows = build_recent_video_rows(
    live_feed.get("recent_videos", []) if isinstance(live_feed, dict) else [],
    channel_names,
    limit=12,
)
global_stock_cards = build_stock_insight_cards(rankings, limit=4)

st.title("Y2I 투자 시그널")
st.caption("유튜브를 오래 보지 않고도 지금 읽어야 할 투자 아이디어, 근거, 채널별 품질을 빠르게 훑는 화면")

st.markdown(
    "<div class='insight-note'>핵심 원칙: 결과보다 <b>왜 그런 판단이 나왔는지</b>, 어느 영상/전문가/매크로 근거가 붙는지를 먼저 보여줍니다.</div>",
    unsafe_allow_html=True,
)

metric_columns = st.columns(len(header_metrics))
for column, metric in zip(metric_columns, header_metrics):
    column.metric(metric["label"], metric["value"])

left, right = st.columns([1.35, 0.85], gap="large")

with left:
    st.subheader("지금 볼 신호")
    if priority_rows:
        st.dataframe(pd.DataFrame(priority_rows), use_container_width=True, hide_index=True)
    else:
        st.info("아직 우선순위 신호가 없습니다.")

    st.subheader("통합 인사이트 보드")
    _render_stock_insight_cards(global_stock_cards, empty_message="아직 읽을 만한 통합 인사이트가 없습니다.")

    st.subheader("최근 분석 영상")
    if recent_rows:
        st.dataframe(pd.DataFrame(recent_rows), use_container_width=True, hide_index=True)
    else:
        st.info("최근 분석 영상이 없습니다.")

with right:
    st.subheader("채널 품질")
    if leaderboard:
        channel_rows = [
            {
                "channel": item.get("display_name", item.get("slug", "-")),
                "quality": item.get("overall_quality_score"),
                "5d_hit": item.get("hit_rate_5d"),
                "5d_avg": item.get("avg_return_5d"),
                "weight": item.get("weight_multiplier"),
            }
            for item in leaderboard[:10]
        ]
        st.dataframe(pd.DataFrame(channel_rows), use_container_width=True, hide_index=True)
    else:
        st.info("채널 품질 데이터가 없습니다.")

    feed_events = live_feed.get("feed_events", []) if isinstance(live_feed, dict) else []
    st.subheader("라이브 피드")
    if feed_events:
        st.dataframe(
            pd.DataFrame(
                [
                    {
                        "channel": item.get("channel_display", item.get("channel_slug", "-")),
                        "headline": item.get("headline", ""),
                        "summary": item.get("summary", ""),
                    }
                    for item in feed_events[:8]
                ]
            ),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("라이브 피드가 없습니다.")

st.subheader("채널 집중 보기")
if available_channels:
    default_index = 0
    selected_channel = st.selectbox(
        "채널",
        available_channels,
        index=default_index,
        format_func=lambda slug: channel_names.get(slug, slug),
    )
    channel_data = load_30d_results(selected_channel, OUTPUT_DIR)
    focus = build_channel_focus_payload(selected_channel, channel_data if isinstance(channel_data, dict) else {})

    focus_metrics = st.columns(3)
    focus_metrics[0].metric("Channel", focus["channel_name"])
    focus_metrics[1].metric("Videos", str(focus["video_count"]))
    focus_metrics[2].metric("Top Stocks", str(len(focus["top_stocks"])))

    focus_left, focus_right = st.columns([1.1, 0.9], gap="large")
    with focus_left:
        st.markdown("**Top Stocks**")
        if focus["top_stocks"]:
            st.dataframe(pd.DataFrame(focus["top_stocks"]), use_container_width=True, hide_index=True)
        else:
            st.info("집중해서 볼 종목이 없습니다.")

        st.markdown("**Recent Video Notes**")
        if focus["recent_videos"]:
            st.dataframe(pd.DataFrame(focus["recent_videos"]), use_container_width=True, hide_index=True)
        else:
            st.info("최근 영상 요약이 없습니다.")

    with focus_right:
        st.markdown("**Macro Points**")
        if focus["macro_points"]:
            st.dataframe(pd.DataFrame(focus["macro_points"]), use_container_width=True, hide_index=True)
        else:
            st.info("매크로 포인트가 없습니다.")

        st.markdown("**Expert Points**")
        if focus["expert_points"]:
            st.dataframe(pd.DataFrame(focus["expert_points"]), use_container_width=True, hide_index=True)
        else:
            st.info("전문가 포인트가 없습니다.")

    st.markdown("**Stock Insight Deck**")
    _render_stock_insight_cards(
        focus.get("stock_cards", []),
        empty_message="이 채널에서 아직 종목별 인사이트를 만들 데이터가 없습니다.",
    )
else:
    st.info("표시할 채널 데이터가 없습니다.")
