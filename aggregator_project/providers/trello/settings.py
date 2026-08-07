from __future__ import annotations

from typing import Any

from connectors.models import ConnectorAccount


MASKED_VALUE = "*********************"
DEFAULT_SETTINGS: dict[str, Any] = {
    "board_ids": [],
    "include_closed": True,
    "todo_list_name": "To Do",
    "in_progress_list_name": "Doing",
    "emit_task_created": True,
    "emit_task_updated": True,
    "emit_task_completed": True,
    "emit_task_state": False,
}


def get_trello_settings(scopes: dict[str, Any] | list[Any] | None) -> dict[str, Any]:
    merged = {**DEFAULT_SETTINGS, "board_ids": []}
    if not isinstance(scopes, dict):
        return merged
    stored = scopes.get("trello")
    if not isinstance(stored, dict):
        stored = scopes
    for key in DEFAULT_SETTINGS:
        if key in stored:
            merged[key] = stored[key]
    merged["board_ids"] = [
        str(value).strip()
        for value in merged.get("board_ids") or []
        if str(value).strip()
    ]
    for key in ("todo_list_name", "in_progress_list_name"):
        merged[key] = str(merged.get(key) or "").strip()
    return merged


def extract_trello_settings(cleaned_data: dict[str, Any]) -> dict[str, Any]:
    return {
        "board_ids": list(cleaned_data.get("board_ids") or []),
        "include_closed": bool(cleaned_data.get("include_closed")),
        "todo_list_name": str(cleaned_data.get("todo_list_name") or "").strip(),
        "in_progress_list_name": str(cleaned_data.get("in_progress_list_name") or "").strip(),
        "emit_task_created": bool(cleaned_data.get("emit_task_created")),
        "emit_task_updated": bool(cleaned_data.get("emit_task_updated")),
        "emit_task_completed": bool(cleaned_data.get("emit_task_completed")),
        "emit_task_state": bool(cleaned_data.get("emit_task_state")),
    }


def trello_form_initial(account) -> dict[str, Any]:
    settings = get_trello_settings(account.scopes)
    settings["board_ids"] = ", ".join(settings["board_ids"])
    if account.encrypted_refresh_token:
        settings["api_key"] = MASKED_VALUE
    if account.encrypted_access_token:
        settings["api_token"] = MASKED_VALUE
    return settings


def resolve_masked_credentials(account, cleaned_data: dict[str, Any]) -> dict[str, Any]:
    api_key = account.get_refresh_token() if cleaned_data.get("api_key") == MASKED_VALUE else cleaned_data.get("api_key")
    api_token = account.get_access_token() if cleaned_data.get("api_token") == MASKED_VALUE else cleaned_data.get("api_token")
    return {**cleaned_data, "api_key": api_key, "api_token": api_token}


def apply_credentials(account, cleaned_data: dict[str, Any]) -> None:
    api_key = cleaned_data.get("api_key")
    api_token = cleaned_data.get("api_token")
    if api_key and api_key != MASKED_VALUE:
        account.set_refresh_token(api_key)
    if api_token and api_token != MASKED_VALUE:
        account.set_access_token(api_token)
    account.auth_type = ConnectorAccount.AUTH_API_TOKEN
    account.external_account_id = None
    account.token_expires_at = None
    account.scopes = {"trello": extract_trello_settings(cleaned_data)}


def source_url(raw: dict[str, Any]) -> str | None:
    value = raw.get("url")
    return str(value) if value else None
