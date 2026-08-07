from __future__ import annotations


KEEP_KEYS = {
    "__trello_created_at",
    "__trello_planner_context",
    "id",
    "idBoard",
    "idList",
    "name",
    "desc",
    "closed",
    "dateLastActivity",
    "due",
    "url",
    "labels",
}


def sanitize_raw(raw: dict) -> dict:
    return {key: value for key, value in raw.items() if key in KEEP_KEYS}
