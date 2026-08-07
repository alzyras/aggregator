from __future__ import annotations

from typing import Any


def planner_badges(_item, latest_event_raw: dict[str, Any]) -> list[str]:
    context = latest_event_raw.get("__linear_planner_context") or {}
    badges: list[str] = []
    for key in ("identifier", "team", "project", "priority"):
        value = str(context.get(key) or "").strip()
        if value and value not in badges:
            badges.append(value)
        if len(badges) >= 3:
            break
    return badges
