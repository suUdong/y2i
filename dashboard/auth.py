"""Dashboard authentication helpers."""
from __future__ import annotations

from dataclasses import dataclass
import json

AUTH_COOKIE_NAME = "y2i_dashboard_token"
AUTH_COOKIE_MAX_AGE_SECONDS = 30 * 24 * 60 * 60


@dataclass(frozen=True)
class DashboardAuthDecision:
    is_authenticated: bool
    source: str
    should_set_cookie: bool = False
    should_clear_cookie: bool = False
    token_to_persist: str | None = None


def _normalize_token(value: str | None) -> str | None:
    if value is None:
        return None

    normalized = value.strip()
    return normalized or None


def resolve_dashboard_auth(
    query_token: str | None,
    cookie_token: str | None,
    expected_token: str,
) -> DashboardAuthDecision:
    normalized_query = _normalize_token(query_token)
    normalized_cookie = _normalize_token(cookie_token)

    if normalized_query is not None:
        if normalized_query == expected_token:
            return DashboardAuthDecision(
                is_authenticated=True,
                source="query_token",
                should_set_cookie=True,
                token_to_persist=normalized_query,
            )
        return DashboardAuthDecision(
            is_authenticated=False,
            source="invalid_query_token",
            should_clear_cookie=normalized_cookie is not None,
        )

    if normalized_cookie == expected_token:
        return DashboardAuthDecision(
            is_authenticated=True,
            source="auth_cookie",
        )

    return DashboardAuthDecision(
        is_authenticated=False,
        source="missing_or_invalid_cookie",
        should_clear_cookie=normalized_cookie is not None,
    )


def build_cookie_sync_html(
    cookie_name: str,
    token: str,
    max_age_seconds: int = AUTH_COOKIE_MAX_AGE_SECONDS,
) -> str:
    cookie_name_json = json.dumps(cookie_name)
    token_json = json.dumps(token)

    return f"""
<script>
const cookieName = {cookie_name_json};
const tokenValue = {token_json};
const secureAttr = window.location.protocol === "https:" ? "; Secure" : "";
document.cookie = `${{cookieName}}=${{tokenValue}}; Max-Age={max_age_seconds}; Path=/; SameSite=Lax${{secureAttr}}`;

const url = new URL(window.location.href);
if (url.searchParams.has("token")) {{
  url.searchParams.delete("token");
  const nextSearch = url.searchParams.toString();
  const nextUrl = `${{url.pathname}}${{nextSearch ? `?${{nextSearch}}` : ""}}${{url.hash}}`;
  window.location.replace(nextUrl || "/");
}}
</script>
"""


def build_cookie_clear_html(cookie_name: str) -> str:
    cookie_name_json = json.dumps(cookie_name)
    return f"""
<script>
const cookieName = {cookie_name_json};
const secureAttr = window.location.protocol === "https:" ? "; Secure" : "";
document.cookie = `${{cookieName}}=; Max-Age=0; Path=/; SameSite=Lax${{secureAttr}}`;
</script>
"""
