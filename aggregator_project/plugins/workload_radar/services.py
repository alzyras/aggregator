from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from django.conf import settings
from django.db.models import Q
from django.db.models.functions import Coalesce
from django.utils import timezone

from ingestion.services.cache import cache_get, cache_set, workspace_cache_key
from planner.models import PlannerItem, PlannerItemState, PlannerPlan
from planner.services.cache import planner_local_cache_token


WORKLOAD_RADAR_CACHE_NAMESPACE = "workload-radar-v1"
CAPACITY_MINUTES = 360
DAYS_TO_SHOW = 7
TASK_PREVIEW_LIMIT = 3
UNPLANNED_PREVIEW_LIMIT = 6


def build_workload_radar(
    *,
    workspace,
    user,
    cache_version: int | None = None,
    now=None,
) -> dict[str, Any]:
    """Summarize planned capacity without guessing time for unestimated work."""
    now = now or timezone.now()
    today = timezone.localdate(now)
    plan = (
        PlannerPlan.objects.filter(workspace=workspace, user=user)
        .order_by("id")
        .first()
    )
    local_token = planner_local_cache_token(
        workspace=workspace,
        user=user,
        plan=plan,
    )
    cache_key = workspace_cache_key(
        workspace,
        WORKLOAD_RADAR_CACHE_NAMESPACE,
        user.id,
        plan.id if plan else "none",
        today.isoformat(),
        local_token,
        cache_version=cache_version,
    )
    cached = cache_get(cache_key)
    if isinstance(cached, dict):
        return cached

    radar = _build_workload_radar(
        workspace=workspace,
        user=user,
        plan=plan,
        today=today,
    )
    cache_set(cache_key, radar, timeout=settings.CACHE_DEFAULT_TIMEOUT_SECONDS)
    return radar


def _build_workload_radar(*, workspace, user, plan: PlannerPlan | None, today: date) -> dict[str, Any]:
    days = [_empty_day(today + timedelta(days=offset), today) for offset in range(DAYS_TO_SHOW)]
    days_by_date = {row["date"]: row for row in days}
    states = []
    if plan:
        states = list(
            PlannerItemState.objects.filter(plan=plan, item__is_active=True)
            .select_related("item", "item__connector_account")
            .order_by("planned_order", "id")
        )

    assigned_item_ids = set()
    assigned_source_keys = set()
    unplanned_rows = []
    unplanned_count = 0
    unplanned_minutes = 0
    unestimated_count = 0
    scheduled_count = 0
    scheduled_minutes = 0

    for state in states:
        item = state.item
        assigned_item_ids.add(item.id)
        source_key = _source_key(item)
        if source_key:
            assigned_source_keys.add(source_key)
        if state.planner_status == PlannerItemState.PLANNER_STATUS_DONE:
            continue

        task = _state_task(state)
        scheduled_day = _planned_day(state.planned_start)
        if scheduled_day and scheduled_day in days_by_date:
            day = days_by_date[scheduled_day]
            minutes = state.estimated_minutes or 0
            day["task_count"] += 1
            day["planned_minutes"] += minutes
            day["unestimated_count"] += int(state.estimated_minutes is None)
            day["tasks"].append(task)
            scheduled_count += 1
            scheduled_minutes += minutes
            continue

        if scheduled_day:
            continue

        unplanned_count += 1
        unplanned_minutes += state.estimated_minutes or 0
        unestimated_count += int(state.estimated_minutes is None)
        priority = 0 if state.planner_status == PlannerItemState.PLANNER_STATUS_DOING else 1
        unplanned_rows.append(
            (
                priority,
                int(not state.pinned),
                state.planned_order,
                state.id,
                task,
            )
        )

    unplanned_items = (
        PlannerItem.objects.for_workspace(workspace)
        .filter(
            Q(user=user) | Q(user__isnull=True),
            is_active=True,
            last_synced_at__isnull=False,
        )
        .exclude(id__in=assigned_item_ids)
        .select_related("connector_account")
        .annotate(inbox_created_sort=Coalesce("source_created_at", "created_at"))
        .order_by("-inbox_created_sort", "-created_at", "-id")
    )
    for item in unplanned_items:
        source_key = _source_key(item)
        if source_key and source_key in assigned_source_keys:
            continue
        unplanned_count += 1
        unestimated_count += 1
        created_at = item.source_created_at or item.created_at
        created_sort = int(created_at.timestamp()) if created_at else 0
        unplanned_rows.append((2, 1, -created_sort, item.id, _item_task(item)))

    for day in days:
        day["hidden_task_count"] = max(
            len(day["tasks"]) - TASK_PREVIEW_LIMIT,
            0,
        )
        day["tasks"] = day["tasks"][:TASK_PREVIEW_LIMIT]
        day["load_percent"] = min(
            round((day["planned_minutes"] / CAPACITY_MINUTES) * 100),
            100,
        ) if day["planned_minutes"] else 0
        day["over_minutes"] = max(day["planned_minutes"] - CAPACITY_MINUTES, 0)
        day["is_over_capacity"] = day["over_minutes"] > 0
        day["remaining_minutes"] = max(CAPACITY_MINUTES - day["planned_minutes"], 0)

    overloaded_days = [day for day in days if day["is_over_capacity"]]
    unplanned_rows.sort(key=lambda row: row[:4])
    return {
        "today": today,
        "period_end": days[-1]["date"],
        "capacity_minutes": CAPACITY_MINUTES,
        "days": days,
        "scheduled_count": scheduled_count,
        "scheduled_minutes": scheduled_minutes,
        "unplanned_count": unplanned_count,
        "unplanned_minutes": unplanned_minutes,
        "unestimated_count": unestimated_count,
        "overloaded_days": overloaded_days,
        "unplanned": [row[-1] for row in unplanned_rows[:UNPLANNED_PREVIEW_LIMIT]],
        "has_tasks": bool(scheduled_count or unplanned_count),
        "insight": _insight(
            overloaded_days=overloaded_days,
            unplanned_count=unplanned_count,
            unestimated_count=unestimated_count,
        ),
    }


def _empty_day(value: date, today: date) -> dict[str, Any]:
    return {
        "date": value,
        "label": value.strftime("%a"),
        "full_label": f"{value.strftime('%A, %B')} {value.day}",
        "is_today": value == today,
        "task_count": 0,
        "planned_minutes": 0,
        "unestimated_count": 0,
        "tasks": [],
        "hidden_task_count": 0,
    }


def _insight(*, overloaded_days: list[dict[str, Any]], unplanned_count: int, unestimated_count: int) -> str:
    if overloaded_days:
        first = overloaded_days[0]
        return f"{first['label']} is {first['over_minutes']} min over your 6-hour planning capacity."
    if unestimated_count:
        return f"{unestimated_count} task{'s' if unestimated_count != 1 else ''} still need a time estimate."
    if unplanned_count:
        return f"{unplanned_count} task{'s' if unplanned_count != 1 else ''} are waiting for a place in the week."
    return "The next seven days are balanced and fully estimated."


def _state_task(state: PlannerItemState) -> dict[str, Any]:
    return {
        **_item_task(state.item),
        "collection": state.collection,
        "estimated_minutes": state.estimated_minutes,
        "planned_start": state.planned_start,
        "planner_status": state.planner_status,
    }


def _item_task(item: PlannerItem) -> dict[str, Any]:
    return {
        "item_id": item.id,
        "title": item.title,
        "source": item.source,
        "source_label": item.get_source_display(),
        "source_url": item.source_url or "",
        "created_at": item.source_created_at or item.created_at,
        "collection": "",
        "estimated_minutes": None,
        "planned_start": None,
        "planner_status": PlannerItemState.PLANNER_STATUS_INBOX,
    }


def _source_key(item: PlannerItem) -> tuple[str, str] | None:
    if not item.source or not item.source_entity_id:
        return None
    return item.source, item.source_entity_id


def _planned_day(value) -> date | None:
    if not value:
        return None
    if timezone.is_aware(value):
        return timezone.localtime(value).date()
    return value.date()
