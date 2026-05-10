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
    build_header_metrics,
    build_ranking_items,
    build_stock_insight_cards,
    build_video_feed_items,
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
    .status-badge {
        display: inline-block;
        padding: 0.08rem 0.5rem;
        border-radius: 999px;
        font-size: 0.72rem;
        font-weight: 700;
        letter-spacing: 0.02em;
        margin-left: 0.35rem;
        vertical-align: middle;
    }
    .status-transcript { background: rgba(16,185,129,0.14); color: #047857; }
    .status-metadata   { background: rgba(245,158,11,0.16); color: #b45309; }
    .status-unknown    { background: rgba(148,163,184,0.18); color: #475569; }
    .scope-summary {
        display: flex;
        gap: 0.75rem;
        flex-wrap: wrap;
        padding: 0.65rem 0.9rem;
        border-radius: 12px;
        background: rgba(15,23,42,0.04);
        border: 1px solid rgba(148,163,184,0.16);
        margin: 0.5rem 0 1rem 0;
        font-size: 0.9rem;
        color: #334155;
    }
    .scope-summary strong { color: #0f172a; }
    hr.soft { border: none; border-top: 1px solid rgba(148,163,184,0.22); margin: 1rem 0 0.75rem 0; }
    </style>
    """,
    unsafe_allow_html=True,
)


def _translate_verdict(value: str) -> str:
    verdict = value.strip().upper()
    return {
        "STRONG_BUY": "강한 매수",
        "BUY": "매수",
        "WATCH": "관찰",
        "HOLD": "중립",
        "SELL": "매도",
        "REJECT": "제외",
    }.get(verdict, value)


def _translate_conviction(value: str) -> str:
    return {
        "HIGH": "높음",
        "MEDIUM": "중간",
        "WATCH": "관찰",
    }.get(value, value)


def _translate_signal_kind(value: str) -> str:
    kind = value.strip().upper()
    return {
        "CONSENSUS": "합의",
        "SINGLE_SOURCE": "단일 출처",
    }.get(kind, value)


def _transcript_badge_html(status: str | None, label: str | None = None) -> str:
    status = (status or "").lower()
    if status == "transcript_backed" or (status and status not in {"metadata_only", "unknown", ""}):
        klass, text = "status-transcript", label or "실자막 ✓"
    elif status == "metadata_only":
        klass, text = "status-metadata", label or "메타데이터만 ⚠"
    else:
        klass, text = "status-unknown", label or "미확인 ?"
    return f"<span class='status-badge {klass}'>{text}</span>"


def _translate_master(value: str) -> str:
    key = value.strip().lower()
    return {
        "druckenmiller": "드러큰밀러 Druckenmiller",
        "buffett": "버핏 Buffett",
        "soros": "소로스 Soros",
    }.get(key, value)


def _select_by_key(items: list[dict], *, state_key: str, item_key: str) -> dict | None:
    if not items:
        return None
    values = {item[item_key] for item in items}
    selected = st.session_state.get(state_key)
    if selected not in values:
        st.session_state[state_key] = items[0][item_key]
        selected = items[0][item_key]
    for item in items:
        if item[item_key] == selected:
            return item
    return items[0]


def _render_video_feed_list(
    items: list[dict],
    *,
    state_key: str,
    detail_kind_key: str,
    empty_message: str,
) -> dict | None:
    if not items:
        st.info(empty_message)
        return None

    selected = _select_by_key(items, state_key=state_key, item_key="video_id")
    selected_video_id = selected["video_id"] if selected else ""
    for item in items:
        is_selected = item["video_id"] == selected_video_id
        source_bits = [item["channel"], item["video_type_label"], item["signal_label"]]
        if item.get("lead_expert"):
            source_bits.append(item["lead_expert"])
        elif item.get("lead_stock"):
            source_bits.append(item["lead_stock"])
        badge = _transcript_badge_html(item.get("transcript_status"), item.get("transcript_status_label"))
        st.markdown(
            (
                "<div class='list-card'>"
                f"<div class='list-kicker'>새 영상 · {item.get('published_at', '-')}{badge}</div>"
                f"<div><strong>{item['title']}</strong></div>"
                f"<div class='list-summary'>{' · '.join(bit for bit in source_bits if bit)}</div>"
                f"<div class='list-summary'>점수 {item['signal_score']:.1f} · {item['summary']}</div>"
                "</div>"
            ),
            unsafe_allow_html=True,
        )
        if st.button(
            "✓ 선택됨 — 상세에서 보기" if is_selected else "이 영상 보기",
            key=f"feed_video_{item['video_id']}",
            use_container_width=True,
            type="primary" if is_selected else "secondary",
        ):
            st.session_state[state_key] = item["video_id"]
            st.session_state[detail_kind_key] = "video"
            selected_video_id = item["video_id"]

    return _select_by_key(items, state_key=state_key, item_key="video_id")


def _render_ranking_list(
    items: list[dict],
    *,
    state_key: str,
    detail_kind_key: str,
    empty_message: str,
) -> dict | None:
    if not items:
        st.info(empty_message)
        return None

    selected = _select_by_key(items, state_key=state_key, item_key="ticker")
    selected_ticker = selected["ticker"] if selected else ""
    for index, item in enumerate(items, start=1):
        is_selected = item["ticker"] == selected_ticker
        st.markdown(
            (
                "<div class='list-card'>"
                f"<div class='list-kicker'>랭킹 {index}위 · {_translate_signal_kind(item.get('signal_kind_label', item.get('signal_note', '')))}</div>"
                f"<div><strong>{item['ticker_display']}</strong> · {_translate_verdict(item['verdict'])} · {float(item['score']):.1f}</div>"
                f"<div class='list-summary'>확신도 {_translate_conviction(item.get('conviction', ''))} · {item.get('channels', '')}</div>"
                f"<div class='list-summary'>{item.get('summary', '')}</div>"
                "</div>"
            ),
            unsafe_allow_html=True,
        )
        if st.button(
            "✓ 선택됨 — 상세에서 보기" if is_selected else "이 종목 보기",
            key=f"ranking_stock_{item['ticker']}",
            use_container_width=True,
            type="primary" if is_selected else "secondary",
        ):
            st.session_state[state_key] = item["ticker"]
            st.session_state[detail_kind_key] = "stock"
            selected_ticker = item["ticker"]

    return _select_by_key(items, state_key=state_key, item_key="ticker")


def _render_stock_detail(card: dict | None, *, empty_message: str) -> None:
    if card is None:
        st.info(empty_message)
        return

    st.markdown(
        (
            "<div class='insight-card'>"
            f"<div class='insight-eyebrow'>{card['signal_note']} · 확신도 {_translate_conviction(card['conviction'])}{_transcript_badge_html(card.get('transcript_status'), card.get('transcript_status_label'))}</div>"
            f"<div class='insight-headline'>{card['ticker_display']}</div>"
            f"<div class='insight-copy'>{card['why_now']}</div>"
            "</div>"
        ),
        unsafe_allow_html=True,
    )
    metric_columns = st.columns(4)
    metric_columns[0].metric("판단", _translate_verdict(card["verdict"]))
    metric_columns[1].metric("Score", f"{card['score']:.1f}")
    metric_columns[2].metric("시그널", _translate_signal_kind(card["signal_kind"]))
    metric_columns[3].metric("근거 상태", card.get("transcript_status_label", "확인 필요"))

    if card.get("transcript_status") == "metadata_only":
        st.warning("이 종목 인사이트는 실자막 없이 메타데이터 기반 영상에서 만들어졌다. 정밀한 전문가 발언 해석으로 보면 안 된다.")
    elif card.get("transcript_status") == "unknown":
        st.info("이 종목 인사이트의 실자막 확보 여부를 현재 데이터만으로는 확정하지 못했다.")

    if card.get("video_context_summary"):
        st.markdown(f"**영상 문맥**  \n{card['video_context_summary']}")
    if card.get("plain_summary"):
        st.markdown(f"**LLM 판단 요약**  \n{card['plain_summary']}")

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

    if card.get("master_views"):
        st.markdown("**구루 관점**")
        for view in card["master_views"]:
            st.markdown(
                f"- **{_translate_master(view['master'])}** · {_translate_verdict(view['verdict'])} {view['score']:.1f} · {view['one_liner']}"
            )

    if card.get("analyst_summary") or card.get("price_target_summary") or card.get("fundamental_rows"):
        st.markdown("**재무 / 컨센서스**")
        if card.get("analyst_summary"):
            st.markdown(f"- 컨센서스: {card['analyst_summary']}")
        if card.get("price_target_summary"):
            st.markdown(f"- 목표가: {card['price_target_summary']}")
        for row in card.get("fundamental_rows", []):
            st.markdown(f"- {row['label']}: {row['value']}")

    if card.get("supporting_videos"):
        st.markdown("**근거 영상 / 원문 맥락**")
        for video in card["supporting_videos"]:
            st.markdown(
                f"- {video['title']} · {_translate_verdict(video['verdict'])} {video['score']:.1f} · {video['summary']}"
            )


def _render_video_detail(video: dict | None, *, empty_message: str) -> None:
    if video is None:
        st.info(empty_message)
        return

    st.markdown(
        (
            "<div class='insight-card'>"
            f"<div class='insight-eyebrow'>{video.get('_channel_display', video.get('_channel', ''))} · {video.get('published_at', '-')}{_transcript_badge_html(('metadata_only' if str(video.get('transcript_language','') or '').lower() == 'metadata_fallback' else ('transcript_backed' if video.get('transcript_language') else 'unknown')), video.get('transcript_status_label'))}</div>"
            f"<div class='insight-headline'>{video.get('title', '')}</div>"
            f"<div class='insight-copy'>{video.get('video_summary') or video.get('reason') or video.get('skip_reason') or ''}</div>"
            "</div>"
        ),
        unsafe_allow_html=True,
    )
    metric_columns = st.columns(4)
    metric_columns[0].metric("시그널", str(video.get("signal_label") or video.get("video_signal_class", "-")))
    metric_columns[1].metric("점수", f"{float(video.get('signal_score', 0) or 0):.1f}")
    metric_columns[2].metric("유형", str(video.get("video_type_label") or video.get("video_type", "-")))
    metric_columns[3].metric("근거 상태", str(video.get("transcript_status_label") or "확인 필요"))

    transcript_language = str(video.get("transcript_language", "") or "")
    if transcript_language.lower().endswith("metadata_fallback"):
        st.warning("이 영상은 실자막이 아니라 제목/설명/태그 기반으로만 해석됐다. 전문가 발언 인용은 신뢰하면 안 된다.")
    elif not transcript_language:
        st.info("이 영상의 실자막 확보 여부를 확인하지 못했다.")

    experts = [expert for expert in video.get("expert_insights", []) or [] if isinstance(expert, dict)]
    if experts:
        st.markdown("**영상 속 전문가 의견**")
        for expert in experts:
            st.markdown(
                f"- **{expert.get('expert_name', '-') }** ({expert.get('topic', '-')}) · {expert.get('summary') or ''}"
            )
            for claim in [claim for claim in expert.get("structured_claims", []) if isinstance(claim, dict)][:2]:
                claim_text = claim.get("claim") or ""
                if claim_text:
                    confidence = float(claim.get("confidence", 0) or 0)
                    st.caption(f"{claim.get('direction', 'NEUTRAL')} {confidence:.0%} · {claim_text}")

    stocks = [stock for stock in video.get("stocks", []) or [] if isinstance(stock, dict)]
    if stocks:
        st.markdown("**이 영상에서 본 종목**")
        for stock in stocks[:5]:
            ticker_display = stock.get("ticker")
            if stock.get("company_name"):
                ticker_display = f"{stock.get('ticker')} {stock.get('company_name')}"
            st.markdown(
                f"- **{ticker_display}** · {_translate_verdict(str(stock.get('final_verdict', '-')))} {float(stock.get('final_score', 0) or 0):.1f} · {stock.get('video_context_summary') or stock.get('plain_summary') or stock.get('basic_signal_summary') or ''}"
            )

    macro_points = [item for item in video.get("macro_insights", []) or [] if isinstance(item, dict)]
    if macro_points:
        st.markdown("**매크로 포인트**")
        for macro in macro_points[:4]:
            st.markdown(
                f"- {macro.get('label') or macro.get('indicator') or '-'} · {macro.get('direction', '-')} · 신뢰도 {float(macro.get('confidence', 0) or 0):.0%}"
            )


OUTPUT_DIR = data_loader_runtime.DEFAULT_OUTPUT_DIR
with st.spinner("파이프라인 데이터를 불러오는 중..."):
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
st.caption("피드는 새 영상을 보고, 랭킹은 종목 우선순위를 보고, 상세에서 전문가·LLM·구루·재무를 한 번에 읽는 화면")

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
    scope_videos = list(live_feed.get("recent_videos", []) if isinstance(live_feed, dict) else [])
else:
    channel_data = load_30d_results(selected_scope, OUTPUT_DIR)
    focus = build_channel_focus_payload(selected_scope, channel_data if isinstance(channel_data, dict) else {})
    scope_label = focus["channel_name"]
    scope_cards = focus.get("stock_cards", [])
    scope_videos = []
    for video in list(channel_data.get("videos", []) if isinstance(channel_data, dict) else []):
        row = dict(video)
        row["_channel"] = selected_scope
        row["_channel_display"] = focus["channel_name"]
        scope_videos.append(row)

for video in scope_videos:
    video.setdefault("_channel_display", channel_names.get(str(video.get("_channel", "")), str(video.get("_channel", ""))))

transcript_backed_count = sum(
    1 for v in scope_videos
    if str(v.get("transcript_language", "") or "").lower() not in {"", "metadata_fallback"}
)
metadata_only_count = sum(
    1 for v in scope_videos
    if str(v.get("transcript_language", "") or "").lower() == "metadata_fallback"
)
st.markdown(
    "<hr class='soft'/>"
    "<div class='scope-summary'>"
    f"<span>범위: <strong>{scope_label}</strong></span>"
    f"<span>새 영상 <strong>{len(scope_videos)}</strong></span>"
    f"<span>종목 인사이트 <strong>{len(scope_cards)}</strong></span>"
    f"<span>실자막 <strong>{transcript_backed_count}</strong></span>"
    f"<span>메타데이터만 <strong>{metadata_only_count}</strong></span>"
    "</div>",
    unsafe_allow_html=True,
)

feed_items = build_video_feed_items(scope_videos, channel_names, limit=12)
ranking_items = build_ranking_items(scope_cards, limit=12)
video_state_key = f"selected_video::{selected_scope}"
stock_state_key = f"selected_stock::{selected_scope}"
detail_kind_key = f"selected_detail_kind::{selected_scope}"

feed_tab, ranking_tab, detail_tab = st.tabs(["피드", "랭킹", "상세"])

with feed_tab:
    st.caption("피드는 기본적으로 새 영상이다. 어떤 영상이 방금 나왔고, 그 안에 어떤 전문가/종목 포인트가 있었는지만 짧게 본다.")
    selected_feed_video = _render_video_feed_list(
        feed_items,
        state_key=video_state_key,
        detail_kind_key=detail_kind_key,
        empty_message="피드에 표시할 새 영상이 없습니다.",
    )
    if selected_feed_video:
        st.info("영상을 선택했다. 상단의 `상세` 탭에서 전체 인사이트를 볼 수 있다.")

with ranking_tab:
    st.caption("랭킹은 지금 바로 읽어야 할 종목 우선순위다. 새 영상 여부보다 종목 판단 강도를 본다.")
    selected_ranking_card = _render_ranking_list(
        ranking_items,
        state_key=stock_state_key,
        detail_kind_key=detail_kind_key,
        empty_message="랭킹에 표시할 종목이 없습니다.",
    )
    if selected_ranking_card:
        st.info("종목을 선택했다. 상단의 `상세` 탭에서 전문가·LLM·구루·재무를 함께 볼 수 있다.")

with detail_tab:
    st.caption("상세는 마지막으로 고른 영상 또는 종목을 깊게 읽는 영역이다.")
    detail_kind = st.session_state.get(detail_kind_key, "video" if feed_items else "stock")
    selected_video = _select_by_key(scope_videos, state_key=video_state_key, item_key="video_id")
    selected_stock = _select_by_key(scope_cards, state_key=stock_state_key, item_key="ticker")
    if detail_kind == "video":
        _render_video_detail(selected_video, empty_message="피드에서 영상을 고르면 여기서 자세히 볼 수 있다.")
    else:
        _render_stock_detail(selected_stock, empty_message="랭킹에서 종목을 고르면 여기서 자세히 볼 수 있다.")
