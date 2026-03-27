from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dashboard.auth import (
    AUTH_COOKIE_MAX_AGE_SECONDS,
    AUTH_COOKIE_NAME,
    build_cookie_sync_html,
    resolve_dashboard_auth,
)


def test_valid_query_token_requests_cookie_persist() -> None:
    decision = resolve_dashboard_auth(
        query_token="6149ba10085f1be3",
        cookie_token=None,
        expected_token="6149ba10085f1be3",
    )

    assert decision.is_authenticated is True
    assert decision.source == "query_token"
    assert decision.should_set_cookie is True
    assert decision.token_to_persist == "6149ba10085f1be3"


def test_valid_cookie_allows_access_without_query_token() -> None:
    decision = resolve_dashboard_auth(
        query_token=None,
        cookie_token="6149ba10085f1be3",
        expected_token="6149ba10085f1be3",
    )

    assert decision.is_authenticated is True
    assert decision.source == "auth_cookie"
    assert decision.should_set_cookie is False


def test_invalid_query_token_rejects_even_with_valid_cookie() -> None:
    decision = resolve_dashboard_auth(
        query_token="wrong",
        cookie_token="6149ba10085f1be3",
        expected_token="6149ba10085f1be3",
    )

    assert decision.is_authenticated is False
    assert decision.source == "invalid_query_token"
    assert decision.should_clear_cookie is True


def test_missing_auth_state_is_rejected() -> None:
    decision = resolve_dashboard_auth(
        query_token=None,
        cookie_token=None,
        expected_token="6149ba10085f1be3",
    )

    assert decision.is_authenticated is False
    assert decision.source == "missing_or_invalid_cookie"
    assert decision.should_clear_cookie is False


def test_cookie_sync_html_sets_30_day_cookie_and_removes_token() -> None:
    html = build_cookie_sync_html(AUTH_COOKIE_NAME, "6149ba10085f1be3")

    assert AUTH_COOKIE_NAME in html
    assert "6149ba10085f1be3" in html
    assert f"Max-Age={AUTH_COOKIE_MAX_AGE_SECONDS}" in html
    assert 'url.searchParams.delete("token")' in html
    assert "window.location.replace" in html
