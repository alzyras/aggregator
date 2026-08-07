from __future__ import annotations


KEEP_KEYS = {
    "__clickup_planner_context",
    "id",
    "name",
    "description",
    "markdown_description",
    "status",
    "date_created",
    "date_updated",
    "date_done",
    "url",
    "list",
    "folder",
    "space",
    "priority",
}


def sanitize_raw(raw: dict) -> dict:
    return {key: value for key, value in raw.items() if key in KEEP_KEYS}
