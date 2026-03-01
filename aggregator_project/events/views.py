from __future__ import annotations

from django.contrib.auth.decorators import login_required
from django.http import QueryDict
from django.core.paginator import Paginator
from django.db.models import Q
from django.shortcuts import get_object_or_404, render
from django.utils.dateparse import parse_date

from connectors.models import ConnectorAccount
from events.models import Event
from ingestion.providers import get_provider_choices

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


@login_required
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
