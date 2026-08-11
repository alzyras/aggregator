from __future__ import annotations

from calendar import monthrange
from datetime import date, datetime, time, timedelta

from django.contrib.auth.decorators import login_required
from django.core.cache import cache
from django.views.decorators.csrf import ensure_csrf_cookie
from django.http import QueryDict
from django.core.paginator import Paginator
from django.db.models import Count, Q
from django.db.models.functions import Coalesce, TruncDate, TruncMonth
from django.shortcuts import get_object_or_404, render
from django.utils import timezone
from django.utils.dateparse import parse_date

from connectors.models import ConnectorAccount
from events.models import Event
from ingestion.models import Job
from ingestion.providers import get_provider_choices
from ingestion.services.cache import workspace_cache_key
from ingestion.services.refresh import get_workspace_refresh_snapshot
from planner.models import PlannerItem, PlannerItemState, PlannerPlan

LIFECYCLE_FILTER_ORDER = (
    "completed",
    "open",
    "deleted",
    "created",
    "updated",
    "started",
    "closed",
)

LIFECYCLE_EVENT_ACTIONS = (
    "created",
    "updated",
    "completed",
    "deleted",
    "started",
    "closed",
    "opened",
)

EXCLUDED_GENERIC_STATES = {
    "scored",
}

STATS_CACHE_TIMEOUT_SECONDS = 60
STATS_COMPLETION_WINDOW_DAYS = 30


@login_required
@ensure_csrf_cookie
def event_list(request):
    workspace = request.workspace
    base_events = Event.objects.for_workspace(workspace)
    source_choices = get_provider_choices()
    source_map = dict(source_choices)
    allowed_sources = set(source_map.keys())

    connector_accounts = list(
        ConnectorAccount.objects.for_workspace(workspace).order_by("source", "display_name")
    )
    connector_map = {str(account.id): account for account in connector_accounts}
    allowed_connector_ids = set(connector_map.keys())

    event_type_values = list(
        base_events.exclude(event_type__isnull=True)
        .exclude(event_type__exact="")
        .values_list("event_type", flat=True)
        .distinct()
        .order_by("event_type")[:200]
    )
    lifecycle_actions = _extract_available_lifecycle_actions(event_type_values)
    allowed_event_type_filters = set(lifecycle_actions)
    external_status_values = list(
        base_events.exclude(external_status__isnull=True)
        .exclude(external_status__exact="")
        .values_list("external_status", flat=True)
        .distinct()
        .order_by("external_status")[:200]
    )
    normalized_external_statuses = {
        value.strip().lower()
        for value in external_status_values
        if (value or "").strip()
    }
    allowed_states = _build_allowed_state_filters(
        normalized_external_statuses=normalized_external_statuses,
        lifecycle_actions=lifecycle_actions,
    )

    source = (request.GET.get("source") or "").strip()
    if source not in allowed_sources:
        source = ""
    source_entity_type = (request.GET.get("type") or "").strip()
    query = (request.GET.get("q") or "").strip()
    start_date = parse_date(request.GET.get("start", ""))
    end_date = parse_date(request.GET.get("end", ""))
    event_types = _sanitize_event_type_filters(
        request.GET.getlist("event_type"),
        allowed_event_type_filters,
    )
    external_statuses = _sanitize_multi_value(
        request.GET.getlist("external_status"),
        allowed_states,
    )
    connector_account_ids = _sanitize_multi_value(
        request.GET.getlist("connector_account"),
        allowed_connector_ids,
    )

    events = base_events.order_by("-start_time", "-created_at")
    if source:
        events = events.filter(source=source)
    if source_entity_type:
        events = events.filter(source_entity_type=source_entity_type)
    if event_types:
        events = events.filter(_build_event_type_query(event_types))
    if external_statuses:
        events = events.filter(_build_state_query(external_statuses))
    if connector_account_ids:
        events = events.filter(connector_account_id__in=connector_account_ids)
    if query:
        events = events.filter(
            Q(title__icontains=query)
            | Q(description__icontains=query)
            | Q(source_entity_id__icontains=query)
            | Q(external_actor_display_name__icontains=query)
            | Q(external_actor_id__icontains=query)
        )
    if start_date:
        events = events.filter(start_time__date__gte=start_date)
    if end_date:
        events = events.filter(start_time__date__lte=end_date)

    paginator = Paginator(events, 25)
    page_number = request.GET.get("page")
    page_obj = paginator.get_page(page_number)

    query_params = _build_sanitized_query_params(
        source=source,
        source_entity_type=source_entity_type,
        query=query,
        start_date=start_date.isoformat() if start_date else "",
        end_date=end_date.isoformat() if end_date else "",
        event_types=event_types,
        external_statuses=external_statuses,
        connector_account_ids=connector_account_ids,
    )
    pagination_query = query_params.urlencode()

    def _label_for_event_type(value: str) -> str:
        return value.replace("_", " ").replace("-", " ").title()

    ordered_states = []
    for key in LIFECYCLE_FILTER_ORDER:
        if key in allowed_states:
            ordered_states.append(key)
    for key in sorted(allowed_states):
        if key not in ordered_states:
            ordered_states.append(key)

    status_pills = [
        {
            "label": "All",
            "value": "",
            "url": _clear_filter(query_params, "external_status"),
            "active": not external_statuses,
        }
    ]
    for value in ordered_states:
        status_pills.append(
            {
                "label": _label_for_event_type(value),
                "value": value,
                "url": _toggle_filter_value(query_params, "external_status", value),
                "active": value in external_statuses,
            }
        )

    connector_pills = [
        {
            "label": "All",
            "url": _clear_filter(query_params, "connector_account"),
            "active": not connector_account_ids,
        }
    ]
    for account in connector_accounts:
        account_id = str(account.id)
        connector_pills.append(
            {
                "label": f"{account.get_source_display()} · {account.display_name}",
                "url": _toggle_filter_value(query_params, "connector_account", account_id),
                "active": account_id in connector_account_ids,
            }
        )

    active_filters = []
    if source:
        active_filters.append(
            {
                "label": f"Source: {source_map.get(source, source)}",
                "remove": _remove_filter(query_params, "source"),
            }
        )
    if query:
        active_filters.append(
            {
                "label": f"Search: {query}",
                "remove": _remove_filter(query_params, "q"),
            }
        )
    if source_entity_type:
        active_filters.append(
            {
                "label": f"Type: {source_entity_type}",
                "remove": _remove_filter(query_params, "type"),
            }
        )
    for value in event_types:
        active_filters.append(
            {
                "label": f"Event: {_label_for_event_type(value)}",
                "remove": _remove_filter(query_params, "event_type", value=value),
            }
        )
    for value in external_statuses:
        active_filters.append(
            {
                "label": f"State: {value.replace('_', ' ').title()}",
                "remove": _remove_filter(query_params, "external_status", value=value),
            }
        )
    for value in connector_account_ids:
        account = connector_map.get(value)
        account_label = f"{account.get_source_display()} · {account.display_name}" if account else value
        active_filters.append(
            {
                "label": f"Plugin: {account_label}",
                "remove": _remove_filter(query_params, "connector_account", value=value),
            }
        )
    if start_date or end_date:
        start_label = start_date.isoformat() if start_date else "Any"
        end_label = end_date.isoformat() if end_date else "Any"
        active_filters.append(
            {
                "label": f"Dates: {start_label} → {end_label}",
                "remove": _remove_filter(query_params, "start", remove_second="end"),
            }
        )

    context = {
        "refresh_state": get_workspace_refresh_snapshot(workspace=workspace),
        "page_obj": page_obj,
        "source_choices": source_choices,
        "event_type_groups": [
            ("Lifecycle", [(value, _label_for_event_type(value)) for value in lifecycle_actions]),
        ],
        "status_pills": status_pills,
        "connector_pills": connector_pills,
        "connector_accounts": connector_accounts,
        "pagination_query": pagination_query,
        "active_filters": active_filters,
        "has_advanced_filters": bool(
            source
            or source_entity_type
            or event_types
            or external_statuses
            or connector_account_ids
        ),
        "filters": {
            "source": source or "",
            "type": source_entity_type or "",
            "event_type": event_types,
            "external_status": external_statuses,
            "connector_account": connector_account_ids,
            "q": query,
            "start": start_date.isoformat() if start_date else "",
            "end": end_date.isoformat() if end_date else "",
        },
    }
    return render(request, "events_list.html", context)


@login_required
def event_detail(request, pk):
    event = get_object_or_404(
        Event.objects.for_workspace(request.workspace), pk=pk
    )
    context = {
        "event": event,
    }
    return render(request, "events_detail.html", context)


@login_required
@ensure_csrf_cookie
def stats_view(request):
    workspace = request.workspace
    source_label_map = {
        **dict(get_provider_choices()),
        **dict(PlannerItem.SOURCE_CHOICES),
    }
    base_events = Event.objects.for_workspace(workspace)
    base_connectors = ConnectorAccount.objects.for_workspace(workspace)
    base_sync_jobs = Job.objects.for_workspace(workspace).filter(
        job_type="sync",
        connector_account__isnull=False,
    )
    user_plan = (
        PlannerPlan.objects.for_workspace(workspace)
        .filter(user=request.user)
        .order_by("created_at")
        .first()
    )
    planner_sources = set()
    if user_plan:
        planner_sources = set(
            PlannerItemState.objects.filter(plan=user_plan, item__is_active=True)
            .values_list("item__source", flat=True)
            .distinct()
        )

    available_sources = sorted(
        set(base_events.values_list("source", flat=True).distinct())
        | set(base_connectors.values_list("source", flat=True).distinct())
        | set(base_sync_jobs.values_list("connector_account__source", flat=True).distinct())
        | planner_sources,
        key=lambda value: source_label_map.get(value, value).lower(),
    )
    allowed_sources = set(available_sources)
    selected_source = (request.GET.get("source") or "").strip().lower()
    if selected_source not in allowed_sources:
        selected_source = ""

    filtered_events = base_events
    if selected_source:
        filtered_events = filtered_events.filter(source=selected_source)

    bypass_cache = request.GET.get("nocache") == "1"
    cache_key = workspace_cache_key(
        workspace,
        "stats",
        selected_source or "all",
    )
    context = None if bypass_cache else cache.get(cache_key)
    if context is None:
        context = _build_stats_context(
            workspace=workspace,
            events=filtered_events,
            selected_source=selected_source,
        )
        if not bypass_cache:
            cache.set(cache_key, context, STATS_CACHE_TIMEOUT_SECONDS)

    source_filter_pills = [
        {
            "label": "All sources",
            "url": _build_stats_filter_url(),
            "active": not selected_source,
        }
    ]
    for source in available_sources:
        source_filter_pills.append(
            {
                "label": source_label_map.get(source, source.replace("_", " ").title()),
                "url": _build_stats_filter_url(source=source),
                "active": source == selected_source,
            }
        )

    render_context = {
        **context,
        "last_generated_at": timezone.now(),
        "cache_bypassed": bypass_cache,
        "refresh_state": get_workspace_refresh_snapshot(workspace=workspace),
        "rhythm": _build_rhythm_context(
            plan=user_plan,
            selected_source=selected_source,
            source_label_map=source_label_map,
        ),
        "selected_source": selected_source,
        "source_filter_pills": source_filter_pills,
    }
    return render(request, "stats.html", render_context)


def _remove_filter(query_params, key: str, *, value: str | None = None, remove_second: str | None = None) -> str:
    updated = query_params.copy()
    if value is None:
        updated.pop(key, None)
    else:
        values = updated.getlist(key)
        values = [item for item in values if item != value]
        if values:
            updated.setlist(key, values)
        else:
            updated.pop(key, None)
    if remove_second:
        updated.pop(remove_second, None)
    updated.pop("page", None)
    query = updated.urlencode()
    return f"?{query}" if query else "?"


def _toggle_filter_value(query_params, key: str, value: str) -> str:
    updated = query_params.copy()
    values = updated.getlist(key)
    if value in values:
        values = [item for item in values if item != value]
    else:
        values.append(value)
    if values:
        updated.setlist(key, values)
    else:
        updated.pop(key, None)
    updated.pop("page", None)
    query = updated.urlencode()
    return f"?{query}" if query else "?"


def _clear_filter(query_params, key: str) -> str:
    updated = query_params.copy()
    updated.pop(key, None)
    updated.pop("page", None)
    query = updated.urlencode()
    return f"?{query}" if query else "?"


def _sanitize_multi_value(raw_values: list[str], allowed_values: set[str]) -> list[str]:
    cleaned: list[str] = []
    for value in raw_values:
        normalized = (value or "").strip().lower()
        if not normalized or normalized not in allowed_values:
            continue
        if normalized not in cleaned:
            cleaned.append(normalized)
    return cleaned


def _sanitize_event_type_filters(raw_values: list[str], allowed_values: set[str]) -> list[str]:
    cleaned: list[str] = []
    for value in raw_values:
        normalized = (value or "").strip().lower().replace("-", "_")
        if not normalized:
            continue
        action = _extract_lifecycle_action(normalized)
        if action not in allowed_values:
            continue
        if action not in cleaned:
            cleaned.append(action)
    return cleaned


def _extract_available_lifecycle_actions(event_type_values: list[str]) -> list[str]:
    actions: list[str] = []
    for raw_value in event_type_values:
        action = _extract_lifecycle_action(raw_value)
        if action and action not in actions:
            actions.append(action)
    ordered_actions = [value for value in LIFECYCLE_EVENT_ACTIONS if value in actions]
    return ordered_actions


def _extract_lifecycle_action(event_type: str) -> str:
    normalized = (event_type or "").strip().lower().replace("-", "_")
    if not normalized:
        return ""
    for action in LIFECYCLE_EVENT_ACTIONS:
        if normalized == action or normalized.endswith(f"_{action}"):
            return action
    return ""


def _build_event_type_query(event_types: list[str]) -> Q:
    query = Q()
    for value in event_types:
        query |= Q(event_type__iexact=value) | Q(event_type__iendswith=f"_{value}")
    return query


def _build_allowed_state_filters(
    *,
    normalized_external_statuses: set[str],
    lifecycle_actions: list[str],
) -> set[str]:
    allowed_states = {
        value
        for value in normalized_external_statuses
        if value not in EXCLUDED_GENERIC_STATES
    }
    if "completed" in lifecycle_actions:
        allowed_states.add("completed")
    if "deleted" in lifecycle_actions:
        allowed_states.add("deleted")
    return allowed_states


def _build_state_query(selected_states: list[str]) -> Q:
    query = Q()
    for value in selected_states:
        if value == "completed":
            query |= Q(event_type__iexact="completed") | Q(event_type__iendswith="_completed")
            continue
        if value == "deleted":
            query |= (
                Q(event_type__iexact="deleted")
                | Q(event_type__iendswith="_deleted")
                | Q(external_status__iexact="deleted")
            )
            continue
        query |= Q(external_status__iexact=value)
    return query


def _build_sanitized_query_params(
    *,
    source: str,
    source_entity_type: str,
    query: str,
    start_date: str,
    end_date: str,
    event_types: list[str],
    external_statuses: list[str],
    connector_account_ids: list[str],
) -> QueryDict:
    params = QueryDict("", mutable=True)
    if source:
        params["source"] = source
    if source_entity_type:
        params["type"] = source_entity_type
    if query:
        params["q"] = query
    if start_date:
        params["start"] = start_date
    if end_date:
        params["end"] = end_date
    if event_types:
        params.setlist("event_type", event_types)
    if external_statuses:
        params.setlist("external_status", external_statuses)
    if connector_account_ids:
        params.setlist("connector_account", connector_account_ids)
    return params


def _build_stats_context(
    *,
    workspace,
    events,
    selected_source: str,
) -> dict[str, object]:
    source_label_map = dict(get_provider_choices())
    activity_events = events.annotate(activity_time=Coalesce("start_time", "created_at"))

    source_totals_rows = list(
        events.values("source")
        .annotate(event_count=Count("id"))
        .order_by("-event_count", "source")
    )
    source_totals = [
        {
            "source": row["source"],
            "source_label": source_label_map.get(row["source"], row["source"].replace("_", " ").title()),
            "event_count": row["event_count"],
            "color_hue": (index * 61 + 211) % 360,
        }
        for index, row in enumerate(source_totals_rows)
    ]

    completion_daily_series = _build_completion_daily_series(activity_events=activity_events)
    completion_monthly_series = _build_completion_monthly_series(activity_events=activity_events)
    completion_source_rows = (
        activity_events.filter(event_type__iexact="task_completed")
        .values("source")
        .annotate(completed_count=Count("id"))
        .order_by("-completed_count", "source")
    )
    source_color_by_key = {item["source"]: item["color_hue"] for item in source_totals}
    completion_source_totals = [
        {
            "source": row["source"],
            "source_label": source_label_map.get(row["source"], row["source"].replace("_", " ").title()),
            "completed_count": row["completed_count"],
            "color_hue": source_color_by_key.get(row["source"], 230),
        }
        for row in completion_source_rows
    ]
    sync_source_rows = _build_sync_source_rows(
        workspace=workspace,
        source_label_map=source_label_map,
        selected_source=selected_source,
    )

    return {
        "source_totals": source_totals,
        "completion_daily_series": completion_daily_series["series"],
        "completion_monthly_series": completion_monthly_series["series"],
        "completion_source_totals": completion_source_totals,
        "sync_source_rows": sync_source_rows,
        "stats_cache_timeout_seconds": STATS_CACHE_TIMEOUT_SECONDS,
    }


def _build_rhythm_context(
    *,
    plan: PlannerPlan | None,
    selected_source: str,
    source_label_map: dict[str, str],
) -> dict[str, object]:
    """Build fresh, personal weekly metrics without affecting cached event statistics."""
    today = timezone.localdate()
    week_start = today - timedelta(days=today.weekday())
    week_end = week_start + timedelta(days=6)
    week_start_at = timezone.make_aware(datetime.combine(week_start, time.min))
    week_end_at = timezone.make_aware(
        datetime.combine(week_end + timedelta(days=1), time.min)
    )
    weekday_series = [
        {
            "date": day_value,
            "date_iso": day_value.isoformat(),
            "label": day_value.strftime("%a"),
            "full_label": day_value.strftime("%A, %d %b"),
            "planned_count": 0,
            "completed_count": 0,
            "planned_minutes": 0,
            "is_today": day_value == today,
        }
        for day_value in (week_start + timedelta(days=offset) for offset in range(7))
    ]
    weekday_by_date = {row["date"]: row for row in weekday_series}
    source_totals: dict[str, dict[str, object]] = {}

    states = []
    if plan:
        states_query = (
            PlannerItemState.objects.select_related("item")
            .filter(plan=plan, item__is_active=True)
            .filter(
                Q(
                    planned_start__gte=week_start_at,
                    planned_start__lt=week_end_at,
                )
                | Q(
                    completed_at__gte=week_start_at,
                    completed_at__lt=week_end_at,
                )
            )
            .order_by("id")
        )
        if selected_source:
            states_query = states_query.filter(item__source=selected_source)
        states = list(states_query)

    planned_count = 0
    completed_count = 0
    planned_minutes = 0
    for state in states:
        planned_day = _stats_local_date(state.planned_start)
        completed_day = _stats_local_date(state.completed_at)
        planned_this_week = planned_day in weekday_by_date
        completed_this_week = completed_day in weekday_by_date

        if not planned_this_week and not completed_this_week:
            continue

        source = state.item.source
        source_row = source_totals.setdefault(
            source,
            {
                "source": source,
                "source_label": source_label_map.get(
                    source,
                    source.replace("_", " ").title(),
                ),
                "task_count": 0,
                "planned_count": 0,
                "completed_count": 0,
                "planned_minutes": 0,
            },
        )
        source_row["task_count"] += 1

        if planned_this_week:
            planned_count += 1
            planned_minutes += state.estimated_minutes or 0
            weekday_by_date[planned_day]["planned_count"] += 1
            weekday_by_date[planned_day]["planned_minutes"] += state.estimated_minutes or 0
            source_row["planned_count"] += 1
            source_row["planned_minutes"] += state.estimated_minutes or 0

        if completed_this_week:
            completed_count += 1
            weekday_by_date[completed_day]["completed_count"] += 1
            source_row["completed_count"] += 1

    has_activity = bool(planned_count or completed_count)
    is_first_week = bool(
        plan is None
        or (
            plan.created_at
            and _stats_local_date(plan.created_at) >= week_start
        )
    )
    source_total_count = sum(row["task_count"] for row in source_totals.values())
    source_breakdown = sorted(
        (
            {
                **row,
                "percent": round((row["task_count"] / source_total_count) * 100)
                if source_total_count
                else 0,
            }
            for row in source_totals.values()
        ),
        key=lambda row: (-row["task_count"], row["source_label"].lower()),
    )
    completion_rate = min(round((completed_count / planned_count) * 100), 100) if planned_count else 0
    empty_state = _build_rhythm_empty_state(
        has_activity=has_activity,
        is_first_week=is_first_week,
        selected_source=selected_source,
        source_label_map=source_label_map,
    )

    return {
        "week_start": week_start,
        "week_end": week_end,
        "week_label": f"{week_start.day} {week_start:%b} – {week_end.day} {week_end:%b}",
        "completed_count": completed_count,
        "planned_count": planned_count,
        "planned_minutes": planned_minutes,
        "completion_rate": completion_rate,
        "weekday_series": weekday_series,
        "source_breakdown": source_breakdown,
        "insight": _build_rhythm_insight(
            planned_count=planned_count,
            completed_count=completed_count,
            is_first_week=is_first_week,
        ),
        "has_activity": has_activity,
        "is_empty": empty_state["is_empty"],
        "is_first_week": empty_state["is_first_week"],
        "empty_state": empty_state,
    }


def _stats_local_date(value) -> date | None:
    if not value:
        return None
    if timezone.is_aware(value):
        return timezone.localtime(value).date()
    return value.date()


def _build_rhythm_empty_state(
    *,
    has_activity: bool,
    is_first_week: bool,
    selected_source: str,
    source_label_map: dict[str, str],
) -> dict[str, object]:
    if has_activity:
        return {
            "is_empty": False,
            "is_first_week": False,
            "title": "",
            "message": "",
        }

    source_label = source_label_map.get(
        selected_source,
        selected_source.replace("_", " ").title(),
    )
    if selected_source:
        title = f"No {source_label} tasks this week"
        message = "Try another source, or give a task a place in your plan."
    elif is_first_week:
        title = "Your first week starts here"
        message = "Plan one task to begin noticing a rhythm."
    else:
        title = "A quiet week is okay"
        message = "Give a task a time this week to see your planning rhythm."
    return {
        "is_empty": True,
        "is_first_week": is_first_week,
        "title": title,
        "message": message,
    }


def _build_rhythm_insight(
    *,
    planned_count: int,
    completed_count: int,
    is_first_week: bool,
) -> str:
    if not planned_count and not completed_count:
        return (
            "Your first week starts with one intentional task."
            if is_first_week
            else "A little space in the week can be useful."
        )
    if not planned_count:
        return (
            f"You completed {completed_count} task{'' if completed_count == 1 else 's'} "
            "without scheduling time for them."
        )
    if not completed_count:
        return (
            f"You have {planned_count} planned task{'' if planned_count == 1 else 's'} "
            "waiting for a first step."
        )
    if completed_count >= planned_count:
        return "You have completed everything you planned this week."
    return f"You have completed {completed_count} of {planned_count} planned tasks this week."


def _build_completion_daily_series(
    *,
    activity_events,
) -> dict[str, object]:
    end_day = timezone.localdate()
    start_day = end_day - timedelta(days=STATS_COMPLETION_WINDOW_DAYS - 1)
    rows = (
        activity_events.filter(
            activity_time__date__gte=start_day,
            activity_time__date__lte=end_day,
            event_type__iexact="task_completed",
        )
        .annotate(day=TruncDate("activity_time"))
        .values("day")
        .annotate(event_count=Count("id"))
        .order_by("day")
    )
    value_by_day = {
        row["day"].isoformat(): row["event_count"]
        for row in rows
        if row["day"]
    }
    series: list[dict[str, object]] = []
    for day_offset in range(STATS_COMPLETION_WINDOW_DAYS):
        day_value = start_day + timedelta(days=day_offset)
        day_iso = day_value.isoformat()
        series.append(
            {
                "label": day_iso,
                "value": value_by_day.get(day_iso, 0),
                "start_date": day_iso,
                "end_date": day_iso,
            }
        )

    return {
        "start_date": start_day,
        "end_date": end_day,
        "series": series,
    }


def _build_completion_monthly_series(
    *,
    activity_events,
) -> dict[str, object]:
    current_month = timezone.localdate().replace(day=1)
    months = []
    year_value = current_month.year
    month_value = current_month.month
    for _ in range(12):
        months.append((year_value, month_value))
        month_value -= 1
        if month_value == 0:
            month_value = 12
            year_value -= 1
    months.reverse()

    start_month = months[0]
    end_month = months[-1]
    start_date = date(start_month[0], start_month[1], 1)
    end_date = date(end_month[0], end_month[1], 1)

    rows = (
        activity_events.filter(
            activity_time__date__gte=start_date,
            activity_time__date__lte=timezone.localdate(),
            event_type__iexact="task_completed",
        )
        .annotate(month=TruncMonth("activity_time"))
        .values("month")
        .annotate(event_count=Count("id"))
        .order_by("month")
    )
    value_by_month = {
        row["month"].date().strftime("%Y-%m"): row["event_count"]
        for row in rows
        if row["month"]
    }
    series: list[dict[str, object]] = []
    for year_number, month_number in months:
        month_label = f"{year_number:04d}-{month_number:02d}"
        series.append(
            {
                "label": month_label,
                "value": value_by_month.get(month_label, 0),
                "start_date": month_label + "-01",
                "end_date": f"{year_number:04d}-{month_number:02d}-{monthrange(year_number, month_number)[1]:02d}",
            }
        )
    return {
        "start_date": start_date,
        "end_date": end_date,
        "series": series,
    }


def _build_stats_filter_url(
    *,
    source: str = "",
) -> str:
    params = QueryDict("", mutable=True)
    if source:
        params["source"] = source
    query = params.urlencode()
    return f"?{query}" if query else "?"


def _build_sync_source_rows(
    *,
    workspace,
    source_label_map: dict[str, str],
    selected_source: str,
) -> list[dict[str, object]]:
    connector_sources = set(
        ConnectorAccount.objects.for_workspace(workspace).values_list("source", flat=True).distinct()
    )
    event_sources = set(
        Event.objects.for_workspace(workspace).values_list("source", flat=True).distinct()
    )
    job_sources = set(
        Job.objects.for_workspace(workspace)
        .filter(job_type="sync", connector_account__isnull=False)
        .values_list("connector_account__source", flat=True)
        .distinct()
    )
    sources = connector_sources | event_sources | job_sources
    if selected_source:
        sources &= {selected_source}
    ordered_sources = sorted(
        sources,
        key=lambda value: source_label_map.get(value, value).lower(),
    )

    latest_jobs = (
        Job.objects.for_workspace(workspace)
        .filter(
            job_type="sync",
            finished_at__isnull=False,
            connector_account__isnull=False,
            connector_account__source__in=ordered_sources,
        )
        .select_related("connector_account")
        .order_by("connector_account__source", "-finished_at", "-queued_at")
    )
    latest_job_by_source: dict[str, Job] = {}
    for job in latest_jobs:
        source = job.connector_account.source
        if source not in latest_job_by_source:
            latest_job_by_source[source] = job

    rows: list[dict[str, object]] = []
    for source in ordered_sources:
        latest_job = latest_job_by_source.get(source)
        rows.append(
            {
                "source": source,
                "source_label": source_label_map.get(source, source.replace("_", " ").title()),
                "last_sync_at": latest_job.finished_at if latest_job else None,
                "last_sync_event_count": _extract_last_sync_event_count(
                    latest_job.output_summary if latest_job else {}
                ),
            }
        )
    return rows


def _extract_last_sync_event_count(summary: dict[str, object] | None) -> int | None:
    if not isinstance(summary, dict):
        return None

    for key in ("inserted", "total", "event_count", "processed"):
        value = _coerce_int(summary.get(key))
        if value is not None:
            return value

    results = summary.get("results")
    if isinstance(results, list):
        for item in results:
            if not isinstance(item, dict):
                continue
            for key in ("inserted", "total", "event_count", "processed"):
                value = _coerce_int(item.get(key))
                if value is not None:
                    return value
    return None


def _coerce_int(value) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.isdigit():
            return int(stripped)
    return None
