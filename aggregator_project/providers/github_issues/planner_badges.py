from __future__ import annotations

from typing import Any


def planner_badges(_item, latest_event_raw: dict[str, Any]) -> list[str]:
    context = latest_event_raw.get("__github_planner_context") or {}
    badges: list[str] = []
    repository = str(context.get("repository") or "").strip()
    if repository:
        badges.append(repository)
    for label in context.get("labels") or []:
        value = str(label).strip()
        if value and value not in badges:
            badges.append(value)
        if len(badges) >= 3:
            break
    return badges
