from __future__ import annotations

from typing import Any

DEFAULT_SETTINGS: dict[str, bool] = {
    "sync_habits": True,
    "sync_todos": True,
    "sync_dailies": True,
    "emit_history_occurrences": True,
    "emit_completion_occurrences": True,
    "task_state_created": False,
    "task_state_updated": False,
    "task_state_completed": False,
}
MASKED_TOKEN = "*********************"


def get_habitica_settings(scopes: dict[str, Any] | list[Any] | None) -> dict[str, bool]:
    if not isinstance(scopes, dict):
        return DEFAULT_SETTINGS.copy()
    stored = scopes.get("habitica")
    if not isinstance(stored, dict):
        return DEFAULT_SETTINGS.copy()
    merged = DEFAULT_SETTINGS.copy()
    for key, value in stored.items():
        if key in merged:
            merged[key] = bool(value)
    return merged


def extract_habitica_settings(cleaned_data: dict[str, Any]) -> dict[str, bool]:
    return {
        "sync_habits": bool(cleaned_data.get("sync_habits")),
        "sync_todos": bool(cleaned_data.get("sync_todos")),
        "sync_dailies": bool(cleaned_data.get("sync_dailies")),
        "emit_history_occurrences": bool(cleaned_data.get("emit_history_occurrences")),
        "emit_completion_occurrences": bool(cleaned_data.get("emit_completion_occurrences")),
        "task_state_created": bool(cleaned_data.get("task_state_created")),
        "task_state_updated": bool(cleaned_data.get("task_state_updated")),
        "task_state_completed": bool(cleaned_data.get("task_state_completed")),
    }


def apply_habitica_settings(account, cleaned_data: dict[str, Any]) -> None:
    settings = extract_habitica_settings(cleaned_data)
    account.scopes = {"habitica": settings}


def habitica_form_initial(account) -> dict[str, Any]:
    settings = get_habitica_settings(getattr(account, "scopes", None))
    user_id = getattr(account, "external_account_id", None)
    if user_id:
        settings["user_id"] = user_id
    if getattr(account, "get_access_token", None):
        existing = account.get_access_token()
        if existing:
            settings["api_token"] = MASKED_TOKEN
    return settings


def is_masked_token(value: Any) -> bool:
    return isinstance(value, str) and value.strip() == MASKED_TOKEN
