from __future__ import annotations

from typing import Any


def planner_badges(_item, latest_event_raw: dict[str, Any]) -> list[str]:
    context = latest_event_raw.get("__habitica_planner_context") or {}
    badges = []
    task_type = str(context.get("task_type") or "").strip()
    if task_type:
        badges.append(task_type)
    tags = context.get("tags") or []
    for tag in tags:
        tag_name = str(tag or "").strip()
        if tag_name:
            badges.append(tag_name)
            break
    return badges
