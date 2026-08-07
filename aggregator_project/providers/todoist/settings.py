from __future__ import annotations

from typing import Any

from connectors.models import ConnectorAccount

DEFAULT_SETTINGS: dict[str, bool] = {
    "sync_tasks": True,
    "include_completed": True,
    "include_archived": False,
    "emit_task_created": True,
    "emit_task_updated": True,
    "emit_task_completed": True,
    "emit_task_deleted": True,
    "task_state_created": False,
    "task_state_updated": False,
    "task_state_completed": False,
}

MASKED_TOKEN = "*********************"


def get_todoist_settings(scopes: dict[str, Any] | list[Any] | None) -> dict[str, bool]:
    if not isinstance(scopes, dict):
        return DEFAULT_SETTINGS.copy()
    stored = scopes.get("todoist")
    if not isinstance(stored, dict):
        stored = scopes
    merged = DEFAULT_SETTINGS.copy()
    for key, value in stored.items():
        if key in merged:
            merged[key] = bool(value)
    return merged


def extract_todoist_settings(cleaned_data: dict[str, Any]) -> dict[str, bool]:
    return {
        "sync_tasks": bool(cleaned_data.get("sync_tasks")),
        "include_completed": bool(cleaned_data.get("include_completed")),
        "include_archived": bool(cleaned_data.get("include_archived")),
        "emit_task_created": bool(cleaned_data.get("emit_task_created")),
        "emit_task_updated": bool(cleaned_data.get("emit_task_updated")),
        "emit_task_completed": bool(cleaned_data.get("emit_task_completed")),
        "emit_task_deleted": bool(cleaned_data.get("emit_task_deleted")),
        "task_state_created": bool(cleaned_data.get("task_state_created")),
        "task_state_updated": bool(cleaned_data.get("task_state_updated")),
        "task_state_completed": bool(cleaned_data.get("task_state_completed")),
    }


def apply_todoist_settings(account, cleaned_data: dict[str, Any]) -> None:
    settings = extract_todoist_settings(cleaned_data)
    account.scopes = {"todoist": settings}


def todoist_form_initial(account) -> dict[str, Any]:
    settings = get_todoist_settings(getattr(account, "scopes", None))
    if getattr(account, "encrypted_access_token", None):
        settings["api_token"] = MASKED_TOKEN
    return settings


def is_masked_token(value: Any) -> bool:
    return isinstance(value, str) and value.strip() == MASKED_TOKEN


def resolve_masked_credentials(account, cleaned_data: dict[str, Any]) -> dict[str, Any]:
    if is_masked_token(cleaned_data.get("api_token")):
        return {**cleaned_data, "api_token": account.get_access_token()}
    return cleaned_data


def apply_credentials(account, cleaned_data: dict[str, Any]) -> None:
    token = cleaned_data.get("api_token") or cleaned_data.get("access_token")
    if token and not is_masked_token(token):
        account.set_access_token(token)
    account.set_refresh_token(None)
    account.external_account_id = None
    account.auth_type = ConnectorAccount.AUTH_API_TOKEN
    account.token_expires_at = None
    apply_todoist_settings(account, cleaned_data)


def source_url(raw: dict[str, Any]) -> str | None:
    value = raw.get("url")
    if value:
        return str(value)
    task_id = raw.get("id") or raw.get("gid")
    return f"https://app.todoist.com/app/task/{task_id}" if task_id else None
