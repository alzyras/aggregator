from __future__ import annotations


def planner_badges(_item, latest_event_raw: dict) -> list[str]:
    context = latest_event_raw.get("__clickup_planner_context") or {}
    badges = []
    for key in ("space_name", "folder_name", "list_name", "priority"):
        value = str(context.get(key) or "").strip()
        if value and value not in badges:
            badges.append(value)
        if len(badges) >= 3:
            break
    return badges
