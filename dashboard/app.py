"""Insight-first Y2I dashboard."""
from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

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
    build_overview_report,
    get_all_rankings,
    get_available_channels,
    get_channel_display_names,
    get_live_feed_data,
    load_30d_results,
    load_channel_comparison,
)
from dashboard.insight_view import (
    build_channel_focus_payload,
    build_feed_items,
    build_header_metrics,
    build_ranking_items,
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
        max-width: 1220px !important;
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
    .list-card {
        border: 1px solid rgba(148,163,184,0.18);
        border-radius: 14px;
        padding: 0.85rem 0.95rem;
        background: rgba(255,255,255,0.94);
        margin-bottom: 0.55rem;
    }
    .list-kicker {
        font-size: 0.76rem;
        color: #64748b;
        text-transform: uppercase;
        letter-spacing: 0.04em;
        font-weight: 700;
    }
    .list-summary {
        color: #475569;
        line-height: 1.45;
        margin-top: 0.25rem;
        font-size: 0.92rem;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


def _select_card(cards: list[dict], *, state_key: str) -> dict | None:
    if not cards:
        return None
    tickers = {card["ticker"] for card in cards}
    selected = st.session_state.get(state_key)
    if selected not in tickers:
        st.session_state[state_key] = cards[0]["ticker"]
        selected = cards[0]["ticker"]
    for card in cards:
        if card["ticker"] == selected:
            return card
    return cards[0]


def _render_compact_signal_list(
    items: list[dict],
    *,
    cards: list[dict],
    state_key: str,
    key_prefix: str,
    empty_message: str,
) -> dict | None:
    if not items or not cards:
        st.info(empty_message)
        return None

    selected = _select_card(cards, state_key=state_key)
    selected_ticker = selected["ticker"] if selected else ""
    for item in items:
        is_selected = item["ticker"] == selected_ticker
        st.markdown(
            (
                "<div class='list-card'>"
                f"<div class='list-kicker'>{item.get('signal_note', '')} · {item.get('freshness', '')}</div>"
                f"<div><strong>{item['ticker_display']}</strong> · {item['verdict']} · {float(item['score']):.1f}</div>"
                f"<div class='list-summary'>{item.get('summary', '')}</div>"
                f"<div class='list-summary'>{item.get('source', item.get('channels', ''))}</div>"
                "</div>"
            ),
            unsafe_allow_html=True,
        )
        if st.button(
            "상세 보기",
            key=f"{key_prefix}_{item['ticker']}",
            use_container_width=True,
            type="primary" if is_selected else "secondary",
        ):
            st.session_state[state_key] = item["ticker"]
            selected_ticker = item["ticker"]

    return _select_card(cards, state_key=state_key)


def _render_stock_detail(card: dict | None, *, empty_message: str) -> None:
    if card is None:
        st.info(empty_message)
        return

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
        st.markdown(f"**LLM + 재무 요약**  \n{card['plain_summary']}")

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
                        st.caption(f"{claim.get('direction', 'NEUTRAL')} {confidence:.0%} · {claim_text}")
        if card.get("supporting_videos"):
            st.markdown("**근거 영상 / 원문 맥락**")
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
available_channels = get_available_channels(OUTPUT_DIR)

header_metrics = build_header_metrics(
    overview=overview if isinstance(overview, dict) else {},
    comparison=comparison if isinstance(comparison, dict) else {},
    live_feed=live_feed if isinstance(live_feed, dict) else {},
)

st.title("Y2I 투자 시그널")
st.caption("피드에서 후보를 고르고, 랭킹에서 우선순위를 확인한 뒤, 상세 패널에서 전문가·LLM·구루·재무를 한 번에 읽는 화면")

st.markdown(
    "<div class='insight-note'>핵심 원칙: 피드는 짧게 스캔하고, 상세 패널에서 <b>전문가 의견 + LLM 해석 + 구루 관점 + 재무/컨센서스</b>를 한 번에 읽습니다.</div>",
    unsafe_allow_html=True,
)

metric_columns = st.columns(len(header_metrics))
for column, metric in zip(metric_columns, header_metrics):
    column.metric(metric["label"], metric["value"])

scope_options = ["__all__", *available_channels]
selected_scope = st.selectbox(
    "보기 범위",
    scope_options,
    format_func=lambda slug: "전체 채널" if slug == "__all__" else channel_names.get(slug, slug),
)

if selected_scope == "__all__":
    scope_label = "전체 채널"
    scope_cards = build_stock_insight_cards(rankings, limit=24)
else:
    channel_data = load_30d_results(selected_scope, OUTPUT_DIR)
    focus = build_channel_focus_payload(selected_scope, channel_data if isinstance(channel_data, dict) else {})
    scope_label = focus["channel_name"]
    scope_cards = focus.get("stock_cards", [])

st.markdown(f"**현재 범위:** {scope_label} · 종목 인사이트 {len(scope_cards)}개")

feed_items = build_feed_items(scope_cards, limit=12)
ranking_items = build_ranking_items(scope_cards, limit=12)
detail_state_key = f"selected_detail::{selected_scope}"

feed_tab, ranking_tab = st.tabs(["피드", "랭킹"])

with feed_tab:
    st.caption("피드는 지금 새로 보거나 변화가 생긴 종목을 짧게 스캔하는 영역이다.")
    feed_left, feed_right = st.columns([0.9, 1.1], gap="large")
    with feed_left:
        selected_feed_card = _render_compact_signal_list(
            feed_items,
            cards=scope_cards,
            state_key=detail_state_key,
            key_prefix=f"feed::{selected_scope}",
            empty_message="피드에 표시할 종목이 없습니다.",
        )
    with feed_right:
        st.subheader("상세 패널")
        _render_stock_detail(selected_feed_card, empty_message="피드에서 종목을 선택하면 상세가 표시됩니다.")

with ranking_tab:
    st.caption("랭킹은 지금 기준으로 가장 강한 아이디어를 우선순위대로 정리한 영역이다.")
    ranking_left, ranking_right = st.columns([0.9, 1.1], gap="large")
    with ranking_left:
        selected_ranking_card = _render_compact_signal_list(
            ranking_items,
            cards=scope_cards,
            state_key=detail_state_key,
            key_prefix=f"ranking::{selected_scope}",
            empty_message="랭킹에 표시할 종목이 없습니다.",
        )
    with ranking_right:
        st.subheader("상세 패널")
        _render_stock_detail(selected_ranking_card, empty_message="랭킹에서 종목을 선택하면 상세가 표시됩니다.")
