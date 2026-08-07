from __future__ import annotations

from typing import Any

from connectors.models import ConnectorAccount


MASKED_SECRET = "*********************"


def google_fit_form_initial(account) -> dict[str, Any]:
    scopes = account.scopes if isinstance(account.scopes, dict) else {}
    initial: dict[str, Any] = {
        "client_id": scopes.get("client_id") or "",
    }
    if scopes.get("client_secret"):
        initial["client_secret"] = MASKED_SECRET
    if account.encrypted_refresh_token:
        initial["refresh_token"] = MASKED_SECRET
    if account.encrypted_access_token:
        initial["access_token"] = MASKED_SECRET
    return initial


def resolve_masked_credentials(account, cleaned_data: dict[str, Any]) -> dict[str, Any]:
    scopes = account.scopes if isinstance(account.scopes, dict) else {}
    resolved = dict(cleaned_data)
    if resolved.get("client_secret") == MASKED_SECRET:
        resolved["client_secret"] = scopes.get("client_secret")
    if resolved.get("refresh_token") == MASKED_SECRET:
        resolved["refresh_token"] = account.get_refresh_token()
    if resolved.get("access_token") == MASKED_SECRET:
        resolved["access_token"] = account.get_access_token()
    return resolved


def apply_credentials(account, cleaned_data: dict[str, Any]) -> None:
    access_token = cleaned_data.get("access_token")
    refresh_token = cleaned_data.get("refresh_token")
    if access_token and access_token != MASKED_SECRET:
        account.set_access_token(access_token)
    if refresh_token and refresh_token != MASKED_SECRET:
        account.set_refresh_token(refresh_token)
    account.external_account_id = None
    account.auth_type = ConnectorAccount.AUTH_OAUTH
    account.scopes = {
        "client_id": cleaned_data.get("client_id"),
        "client_secret": cleaned_data.get("client_secret"),
    }
    account.token_expires_at = None
