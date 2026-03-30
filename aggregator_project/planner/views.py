from __future__ import annotations

import json
from datetime import timedelta

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.db import transaction
from django.db.models import Q
from django.http import HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone
from django.utils.dateparse import parse_datetime
from django.views.decorators.csrf import ensure_csrf_cookie
from django.views.decorators.http import require_POST

from connectors.models import ConnectorAccount
from events.models import Event
from planner.models import PlannerItem, PlannerItemState, PlannerPlan
from planner.services.reconcile import add_items_from_events, ensure_item_state


PLANNER_EVENT_TYPES = ["task_created", "task_updated", "task_state", "task_completed"]


@login_required
@ensure_csrf_cookie
def planner_list(request: HttpRequest) -> HttpResponse:
    plan = _get_or_create_plan(request)

    if request.method == "POST":
        if request.POST.get("add_from_sources"):
            added = _handle_add_from_sources(request, plan)
            if added:
                messages.success(request, f"Added {added} items from sources.")
            else:
                messages.info(request, "No new items found to add.")
            return redirect("planner_list")

    states = (
        PlannerItemState.objects
        .select_related("item", "item__connector_account")
        .filter(plan=plan, item__is_active=True)
        .order_by("planned_order", "id")
    )

    now = timezone.now()
    last_synced_at = (
        PlannerItem.objects
        .for_workspace(request.workspace)
        .filter(user=request.user, last_synced_at__isnull=False)
        .order_by("-last_synced_at")
        .values_list("last_synced_at", flat=True)
        .first()
    )
    stale_warning = False
    if last_synced_at:
        stale_warning = (now - last_synced_at) > timedelta(hours=24)

    source_choices = (
        ConnectorAccount.objects
        .for_workspace(request.workspace)
        .values_list("source", flat=True)
        .distinct()
    )

    grouped_states = {
        status: [] for status, _label in PlannerItemState.PLANNER_STATUS_CHOICES
    }
    for state in states:
        grouped_states.setdefault(state.planner_status, []).append(state)

    tab_items = []
    for status, label in PlannerItemState.PLANNER_STATUS_CHOICES:
        items = grouped_states.get(status, [])
        tab_items.append({
            "value": status,
            "label": label,
            "count": len(items),
            "items": items,
        })

    context = {
        "plan": plan,
        "tab_items": tab_items,
        "now": now,
        "last_synced_at": last_synced_at,
        "stale_warning": stale_warning,
        "status_choices": PlannerItemState.STATUS_CHOICES,
        "planner_status_choices": PlannerItemState.PLANNER_STATUS_CHOICES,
        "source_choices": [(source, source.replace("_", " ").title()) for source in source_choices],
    }
    return render(request, "planner/planner_list.html", context)


@login_required
def planner_calendar(request: HttpRequest) -> HttpResponse:
    plan = _get_or_create_plan(request)
    states = (
        PlannerItemState.objects
        .select_related("item", "item__connector_account")
        .filter(plan=plan, item__is_active=True)
        .order_by("planned_start", "planned_order")
    )
    now = timezone.now()
    week_start = now - timedelta(days=now.weekday())
    days = [week_start + timedelta(days=offset) for offset in range(7)]

    context = {
        "plan": plan,
        "states": states,
        "days": days,
        "now": now,
    }
    return render(request, "planner/planner_calendar.html", context)


@login_required
def update_planned_status(request: HttpRequest, item_id: str) -> JsonResponse:
    response = _assert_post(request)
    if response:
        return response
    payload = _parse_json(request)
    status = payload.get("planned_status")
    if status not in dict(PlannerItemState.STATUS_CHOICES):
        return JsonResponse({"error": "Invalid status."}, status=400)

    state = _get_state(request, item_id)
    state.planned_status = status
    state.last_planned_at = timezone.now()
    state.save(update_fields=["planned_status", "last_planned_at"])
    return JsonResponse({"status": "ok", "planned_status": state.planned_status})


@login_required
def update_planner_status(request: HttpRequest, item_id: str) -> JsonResponse:
    response = _assert_post(request)
    if response:
        return response
    payload = _parse_json(request)
    status = payload.get("planner_status")
    if status not in dict(PlannerItemState.PLANNER_STATUS_CHOICES):
        return JsonResponse({"error": "Invalid status."}, status=400)

    state = _get_state(request, item_id)
    state.planner_status = status
    state.last_planned_at = timezone.now()
    state.save(update_fields=["planner_status", "last_planned_at"])
    return JsonResponse({"status": "ok", "planner_status": state.planner_status})


@login_required
def update_planned_schedule(request: HttpRequest, item_id: str) -> JsonResponse:
    response = _assert_post(request)
    if response:
        return response
    payload = _parse_json(request)
    planned_start = _parse_datetime(payload.get("planned_start"))
    planned_end = _parse_datetime(payload.get("planned_end"))

    state = _get_state(request, item_id)
    state.planned_start = planned_start
    state.planned_end = planned_end
    state.last_planned_at = timezone.now()
    state.save(update_fields=["planned_start", "planned_end", "last_planned_at"])
    return JsonResponse({"status": "ok"})


@login_required
def toggle_pin(request: HttpRequest, item_id: str) -> JsonResponse:
    response = _assert_post(request)
    if response:
        return response
    state = _get_state(request, item_id)
    state.pinned = not state.pinned
    state.last_planned_at = timezone.now()
    state.save(update_fields=["pinned", "last_planned_at"])
    return JsonResponse({"status": "ok", "pinned": state.pinned})


@login_required
def reorder_items(request: HttpRequest) -> JsonResponse:
    response = _assert_post(request)
    if response:
        return response
    payload = _parse_json(request)
    block_order = payload.get("block_order")
    planner_status = payload.get("planner_status")
    moved_id = payload.get("moved_id")
    before_id = payload.get("before_id")
    after_id = payload.get("after_id")

    if block_order is None and moved_id is None:
        return JsonResponse({"error": "block_order or moved_id is required."}, status=400)
    if before_id and after_id:
        return JsonResponse({"error": "Provide only one of before_id or after_id."}, status=400)

    plan = _get_or_create_plan(request)
    now = timezone.now()

    if block_order is not None:
        if planner_status not in dict(PlannerItemState.PLANNER_STATUS_CHOICES):
            return JsonResponse({"error": "planner_status is required for block_order."}, status=400)
        if not isinstance(block_order, list) or not block_order:
            return JsonResponse({"error": "block_order must be a non-empty list."}, status=400)
        try:
            ordered_ids = [int(value) for value in block_order]
        except (TypeError, ValueError):
            return JsonResponse({"error": "Ids must be integers."}, status=400)
        if len(set(ordered_ids)) != len(ordered_ids):
            return JsonResponse({"error": "block_order contains duplicates."}, status=400)

        states = list(
            PlannerItemState.objects
            .filter(plan=plan, id__in=ordered_ids, item__is_active=True)
        )
        if len(states) != len(ordered_ids):
            return JsonResponse({"error": "block_order contains unknown ids."}, status=400)

        pinned_values = {state.pinned for state in states}
        if len(pinned_values) != 1:
            return JsonResponse({"error": "block_order must contain a single pinned block."}, status=400)
        pinned_value = pinned_values.pop()

        block_ids = list(
            PlannerItemState.objects
            .filter(
                plan=plan,
                pinned=pinned_value,
                planner_status=planner_status,
                item__is_active=True,
            )
            .values_list("id", flat=True)
        )
        if set(block_ids) != set(ordered_ids):
            return JsonResponse({"error": "block_order must include all items in the block."}, status=400)

        ordered_index = {state_id: idx for idx, state_id in enumerate(ordered_ids, start=1)}
        updated = []
        for state in states:
            planned_order = ordered_index[state.id]
            if state.planned_order != planned_order or state.last_planned_at != now:
                state.planned_order = planned_order
                state.last_planned_at = now
                updated.append(state)
        if updated:
            with transaction.atomic():
                PlannerItemState.objects.bulk_update(updated, ["planned_order", "last_planned_at"])
        return JsonResponse({"status": "ok"})

    try:
        moved_id = int(moved_id)
        before_id = int(before_id) if before_id is not None else None
        after_id = int(after_id) if after_id is not None else None
    except (TypeError, ValueError):
        return JsonResponse({"error": "Ids must be integers."}, status=400)

    moved_state = PlannerItemState.objects.filter(plan=plan, id=moved_id, item__is_active=True).first()
    if not moved_state:
        return JsonResponse({"error": "Unknown moved_id."}, status=400)

    neighbor_state = None
    neighbor_id = before_id if before_id is not None else after_id
    if neighbor_id is not None:
        neighbor_state = PlannerItemState.objects.filter(plan=plan, id=neighbor_id, item__is_active=True).first()
        if not neighbor_state:
            return JsonResponse({"error": "Unknown neighbor id."}, status=400)

    if neighbor_state and neighbor_state.pinned != moved_state.pinned:
        return JsonResponse({"error": "Cannot move items across pinned boundary."}, status=400)

    if planner_status and planner_status != moved_state.planner_status:
        return JsonResponse({"error": "Planner status mismatch."}, status=400)

    block_states = list(
        PlannerItemState.objects
        .filter(
            plan=plan,
            pinned=moved_state.pinned,
            planner_status=moved_state.planner_status,
            item__is_active=True,
        )
        .order_by("planned_order", "id")
    )

    block_lookup = {state.id: state for state in block_states}
    if moved_state.id not in block_lookup:
        return JsonResponse({"error": "Moved item not in reorderable block."}, status=400)

    block_states = [state for state in block_states if state.id != moved_state.id]
    insert_index = len(block_states)
    if neighbor_state:
        neighbor_index = next(
            (idx for idx, state in enumerate(block_states) if state.id == neighbor_state.id),
            None,
        )
        if neighbor_index is None:
            return JsonResponse({"error": "Neighbor not in reorderable block."}, status=400)
        insert_index = neighbor_index if before_id is not None else neighbor_index + 1
    block_states.insert(insert_index, moved_state)

    updated = []
    for index, state in enumerate(block_states, start=1):
        if state.planned_order != index or state.last_planned_at != now:
            state.planned_order = index
            state.last_planned_at = now
            updated.append(state)
    if updated:
        with transaction.atomic():
            PlannerItemState.objects.bulk_update(updated, ["planned_order", "last_planned_at"])

    return JsonResponse({"status": "ok"})


@login_required
def add_from_sources(request: HttpRequest) -> JsonResponse:
    response = _assert_post(request)
    if response:
        return response
    plan = _get_or_create_plan(request)
    added = _handle_add_from_sources(request, plan)
    return JsonResponse({"status": "ok", "added": added})


@login_required
@require_POST
def preview_sources(request: HttpRequest) -> JsonResponse:
    payload = _parse_json(request)
    sources = payload.get("sources") or []
    statuses = payload.get("statuses") or []
    days = payload.get("days", 30)

    try:
        days = int(days)
    except (TypeError, ValueError):
        return JsonResponse({"error": "days must be an integer."}, status=400)

    events = _latest_task_events(
        request=request,
        sources=sources,
        statuses=statuses,
        days=days,
    )

    results = []
    for event in events:
        results.append({
            "connector_account_id": event.connector_account_id,
            "source_entity_id": event.source_entity_id,
            "title": event.title or event.source_entity_id,
            "source": event.source,
            "connector_account": event.connector_account.display_name if event.connector_account else None,
            "external_status": event.external_status or event.event_type,
            "updated_at": event.start_time or event.created_at,
        })
    return JsonResponse({"results": results})


@login_required
@require_POST
def add_selected_sources(request: HttpRequest) -> JsonResponse:
    payload = _parse_json(request)
    selected_ids = payload.get("selected_ids") or []
    planner_status = payload.get("planner_status") or PlannerItemState.PLANNER_STATUS_INBOX
    planned_status = payload.get("planned_status") or PlannerItemState.STATUS_PLANNED
    pinned = bool(payload.get("pinned", False))
    placement = payload.get("placement") or "bottom"
    after_id = payload.get("after_id")

    if planner_status not in dict(PlannerItemState.PLANNER_STATUS_CHOICES):
        return JsonResponse({"error": "Invalid planner_status."}, status=400)
    if planned_status not in dict(PlannerItemState.STATUS_CHOICES):
        return JsonResponse({"error": "Invalid planned_status."}, status=400)
    if placement not in {"top", "bottom", "after_pinned", "after_id"}:
        return JsonResponse({"error": "Invalid placement."}, status=400)

    if not isinstance(selected_ids, list) or not selected_ids:
        return JsonResponse({"error": "selected_ids must be a non-empty list."}, status=400)

    plan = _get_or_create_plan(request)
    if placement == "after_id":
        if not after_id:
            return JsonResponse({"error": "after_id is required for placement after_id."}, status=400)
        if not PlannerItemState.objects.filter(
            plan=plan,
            id=after_id,
            pinned=pinned,
            planner_status=planner_status,
        ).exists():
            return JsonResponse({"error": "Invalid placement target."}, status=400)

    events = _events_from_selected(request, selected_ids)
    if not events:
        return JsonResponse({"error": "No matching events found."}, status=400)

    selected_states = []
    with transaction.atomic():
        for event in events:
            item, _created = _get_or_create_planner_item_from_event(
                request=request,
                event=event,
            )
            if not item:
                continue
            state = ensure_item_state(plan, item, planner_status=planner_status)
            state.planned_status = planned_status
            state.planner_status = planner_status
            state.pinned = pinned
            state.last_planned_at = timezone.now()
            state.save(update_fields=["planned_status", "planner_status", "pinned", "last_planned_at"])
            selected_states.append(state)

        if selected_states:
            placement_ok = _apply_block_placement(
                plan=plan,
                pinned=pinned,
                planner_status=planner_status,
                placement=placement,
                after_id=after_id,
                selected_states=selected_states,
            )
            if not placement_ok:
                return JsonResponse({"error": "Invalid placement target."}, status=400)

    return JsonResponse({"status": "ok", "added": len(selected_states)})


def _handle_add_from_sources(request: HttpRequest, plan: PlannerPlan) -> int:
    days = int(request.POST.get("days", 30))
    since = timezone.now() - timedelta(days=max(1, days))
    completed_statuses = {"completed", "done", "closed", "resolved"}

    events = (
        Event.objects.for_workspace(request.workspace)
        .filter(event_type__in=PLANNER_EVENT_TYPES, created_at__gte=since)
        .exclude(
            Q(event_type="task_completed")
            | Q(external_status__iexact="completed")
            | Q(external_status__iexact="done")
            | Q(external_status__iexact="closed")
            | Q(external_status__iexact="resolved")
        )
        .order_by("connector_account_id", "source_entity_id", "-created_at")
        .distinct("connector_account_id", "source_entity_id")
    )

    return add_items_from_events(
        workspace=request.workspace,
        user=request.user,
        events=events,
        plan=plan,
    )


def _latest_task_events(*, request: HttpRequest, sources: list, statuses: list, days: int):
    since = timezone.now() - timedelta(days=max(1, days))

    events = (
        Event.objects.for_workspace(request.workspace)
        .filter(event_type__in=PLANNER_EVENT_TYPES, created_at__gte=since)
        .select_related("connector_account")
    )

    if sources:
        events = events.filter(source__in=sources)

    status_values = {status.lower() for status in statuses if isinstance(status, str)}
    if status_values:
        status_query = Q()
        if "completed" in status_values:
            status_query |= (
                Q(event_type="task_completed")
                | Q(external_status__iexact="completed")
                | Q(external_status__iexact="done")
                | Q(external_status__iexact="closed")
                | Q(external_status__iexact="resolved")
            )
        if "in_progress" in status_values:
            status_query |= (
                Q(external_status__iexact="in progress")
                | Q(external_status__iexact="in_progress")
                | Q(external_status__iexact="doing")
            )
        if "open" in status_values:
            status_query |= (
                ~Q(event_type="task_completed")
                & ~Q(external_status__iexact="completed")
                & ~Q(external_status__iexact="done")
                & ~Q(external_status__iexact="closed")
                & ~Q(external_status__iexact="resolved")
                & ~Q(external_status__iexact="in progress")
                & ~Q(external_status__iexact="in_progress")
                & ~Q(external_status__iexact="doing")
            )
        events = events.filter(status_query)
    else:
        events = events.exclude(
            Q(event_type="task_completed")
            | Q(external_status__iexact="completed")
            | Q(external_status__iexact="done")
            | Q(external_status__iexact="closed")
            | Q(external_status__iexact="resolved")
        )

    return (
        events
        .order_by("connector_account_id", "source_entity_id", "-created_at")
        .distinct("connector_account_id", "source_entity_id")
    )


def _events_from_selected(request: HttpRequest, selected_ids: list) -> list[Event]:
    event_ids = []
    pairs = []
    for entry in selected_ids:
        if isinstance(entry, int):
            event_ids.append(entry)
        elif isinstance(entry, dict):
            connector_id = entry.get("connector_account_id")
            source_entity_id = entry.get("source_entity_id")
            if connector_id and source_entity_id:
                try:
                    pairs.append((int(connector_id), str(source_entity_id)))
                except (TypeError, ValueError):
                    continue
    if event_ids:
        return list(
            Event.objects.for_workspace(request.workspace)
            .filter(id__in=event_ids)
            .select_related("connector_account")
        )
    if pairs:
        query = Q()
        for connector_id, source_entity_id in pairs:
            query |= Q(connector_account_id=connector_id, source_entity_id=source_entity_id)
        return list(
            Event.objects.for_workspace(request.workspace)
            .filter(query)
            .order_by("connector_account_id", "source_entity_id", "-created_at")
            .distinct("connector_account_id", "source_entity_id")
            .select_related("connector_account")
        )
    return []


def _get_or_create_planner_item_from_event(
    *,
    request: HttpRequest,
    event: Event,
):
    return _get_or_create_item_from_event(event, request.workspace, request.user)


def _apply_block_placement(
    *,
    plan: PlannerPlan,
    pinned: bool,
    planner_status: str,
    placement: str,
    after_id: int | None,
    selected_states: list[PlannerItemState],
) -> bool:
    block_states = list(
        PlannerItemState.objects
        .filter(
            plan=plan,
            pinned=pinned,
            planner_status=planner_status,
            item__is_active=True,
        )
        .order_by("planned_order", "id")
    )
    selected_ids = {state.id for state in selected_states}
    block_states = [state for state in block_states if state.id not in selected_ids]

    insert_index = len(block_states)
    if placement == "top":
        insert_index = 0
    elif placement == "after_pinned":
        insert_index = 0
    elif placement == "after_id" and after_id:
        after_index = next((idx for idx, state in enumerate(block_states) if state.id == int(after_id)), None)
        if after_index is None:
            return False
        insert_index = after_index + 1

    block_states[insert_index:insert_index] = selected_states
    updated = []
    now = timezone.now()
    for index, state in enumerate(block_states, start=1):
        if state.planned_order != index or state.last_planned_at != now:
            state.planned_order = index
            state.last_planned_at = now
            updated.append(state)
    if updated:
        PlannerItemState.objects.bulk_update(updated, ["planned_order", "last_planned_at"])
    return True


def _get_or_create_plan(request: HttpRequest) -> PlannerPlan:
    plan = (
        PlannerPlan.objects
        .for_workspace(request.workspace)
        .filter(user=request.user)
        .order_by("created_at")
        .first()
    )
    if plan:
        return plan
    return PlannerPlan.objects.create(
        workspace=request.workspace,
        user=request.user,
        name="My Plan",
        timezone=timezone.get_current_timezone_name(),
    )


def _get_state(request: HttpRequest, item_id: str) -> PlannerItemState:
    plan = _get_or_create_plan(request)
    return get_object_or_404(
        PlannerItemState.objects.select_related("item"),
        plan=plan,
        item_id=item_id,
    )


def _parse_json(request: HttpRequest) -> dict:
    try:
        payload = json.loads(request.body.decode("utf-8")) if request.body else {}
    except json.JSONDecodeError:
        payload = {}
    return payload


def _parse_datetime(value: str | None):
    if not value:
        return None
    return parse_datetime(value)


def _assert_post(request: HttpRequest) -> JsonResponse | None:
    if request.method != "POST":
        return JsonResponse({"error": "POST required."}, status=405)
    return None
