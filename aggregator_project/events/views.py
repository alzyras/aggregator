from __future__ import annotations

from django.contrib.auth.decorators import login_required
from django.core.paginator import Paginator
from django.db.models import Q
from django.shortcuts import get_object_or_404, render
from django.utils.dateparse import parse_date

from connectors.models import ConnectorAccount
from events.models import Event
from ingestion.providers import get_provider_choices


@login_required
def event_list(request):
    events = (
        Event.objects.for_workspace(request.workspace)
        .order_by("-start_time", "-created_at")
    )

    source = request.GET.get("source")
    source_entity_type = request.GET.get("type")
    event_types = [value for value in request.GET.getlist("event_type") if value]
    external_status = request.GET.get("external_status")
    connector_account_id = request.GET.get("connector_account")
    query = (request.GET.get("q") or "").strip()
    start_date = parse_date(request.GET.get("start", ""))
    end_date = parse_date(request.GET.get("end", ""))

    if source:
        events = events.filter(source=source)
    if source_entity_type:
        events = events.filter(source_entity_type=source_entity_type)
    if event_types:
        events = events.filter(event_type__in=event_types)
    if external_status:
        events = events.filter(external_status=external_status)
    if connector_account_id:
        events = events.filter(connector_account_id=connector_account_id)
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

    event_type_values = (
        Event.objects.for_workspace(request.workspace)
        .exclude(event_type__isnull=True)
        .exclude(event_type__exact="")
        .values_list("event_type", flat=True)
        .distinct()
        .order_by("event_type")[:200]
    )
    external_status_values = (
        Event.objects.for_workspace(request.workspace)
        .exclude(external_status__isnull=True)
        .exclude(external_status__exact="")
        .values_list("external_status", flat=True)
        .distinct()
        .order_by("external_status")[:200]
    )
    connector_accounts = (
        ConnectorAccount.objects.for_workspace(request.workspace)
        .order_by("source", "display_name")
    )
    lifecycle_suffixes = ("created", "completed", "deleted", "updated", "started", "closed")
    lifecycle_types = [value for value in event_type_values if value.endswith(lifecycle_suffixes)]
    other_types = [value for value in event_type_values if value not in lifecycle_types]

    query_params = request.GET.copy()
    query_params.pop("page", None)
    pagination_query = query_params.urlencode()

    def _label_for_event_type(value: str) -> str:
        return value.replace("_", " ").replace("-", " ").title()

    active_filters = []
    if source:
        active_filters.append(
            {
                "label": f"Source: {dict(get_provider_choices()).get(source, source)}",
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
    if external_status:
        active_filters.append(
            {
                "label": f"Status: {external_status}",
                "remove": _remove_filter(query_params, "external_status"),
            }
        )
    if connector_account_id:
        account = connector_accounts.filter(id=connector_account_id).first()
        account_label = (
            f"{account.get_source_display()} · {account.display_name}"
            if account
            else connector_account_id
        )
        active_filters.append(
            {
                "label": f"Plugin: {account_label}",
                "remove": _remove_filter(query_params, "connector_account"),
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
        "source_choices": get_provider_choices(),
        "event_type_groups": [
            ("Lifecycle", [(value, _label_for_event_type(value)) for value in lifecycle_types]),
            ("Other", [(value, _label_for_event_type(value)) for value in other_types]),
        ],
        "external_status_choices": list(external_status_values),
        "connector_accounts": connector_accounts,
        "pagination_query": pagination_query,
        "active_filters": active_filters,
        "filters": {
            "source": source or "",
            "type": source_entity_type or "",
            "event_type": event_types,
            "external_status": external_status or "",
            "connector_account": str(connector_account_id or ""),
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
        updated.setlist(key, [item for item in values if item != value])
    if remove_second:
        updated.pop(remove_second, None)
    query = updated.urlencode()
    return f"?{query}" if query else "?"
