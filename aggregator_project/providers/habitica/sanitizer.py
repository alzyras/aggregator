from __future__ import annotations

from typing import Any


KEEP_KEYS = {
    "__habitica_planner_context",
    "id",
    "_id",
    "text",
    "notes",
    "type",
    "completed",
    "createdAt",
    "updatedAt",
    "dateCreated",
    "startDate",
    "frequency",
    "repeat",
    "priority",
    "tags",
    "history",
}


def sanitize_raw(raw: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in raw.items() if key in KEEP_KEYS}
