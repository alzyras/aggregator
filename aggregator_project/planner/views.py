from __future__ import annotations

import json
from datetime import timedelta

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.db import DatabaseError, transaction
from django.db.models import Q
from django.http import HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone
from django.utils.dateparse import parse_datetime
from django.views.decorators.csrf import ensure_csrf_cookie

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
        .order_by("-pinned", "planned_order")
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

    context = {
        "plan": plan,
        "states": states,
        "now": now,
        "last_synced_at": last_synced_at,
        "stale_warning": stale_warning,
        "status_choices": PlannerItemState.STATUS_CHOICES,
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
    moved_id = payload.get("moved_id")
    before_id = payload.get("before_id")
    after_id = payload.get("after_id")

    if moved_id is None:
        return JsonResponse({"error": "moved_id is required."}, status=400)
    if before_id and after_id:
        return JsonResponse({"error": "Provide only one of before_id or after_id."}, status=400)

    try:
        moved_id = int(moved_id)
        before_id = int(before_id) if before_id is not None else None
        after_id = int(after_id) if after_id is not None else None
    except (TypeError, ValueError):
        return JsonResponse({"error": "Ids must be integers."}, status=400)

    plan = _get_or_create_plan(request)
    now = timezone.now()

    try:
        with transaction.atomic():
            try:
                moved_state = PlannerItemState.objects.select_for_update(nowait=True).get(
                    plan=plan,
                    id=moved_id,
                    item__is_active=True,
                )
            except PlannerItemState.DoesNotExist:
                return JsonResponse({"error": "Unknown moved_id."}, status=400)

            neighbor_state = None
            neighbor_id = before_id if before_id is not None else after_id
            if neighbor_id is not None:
                try:
                    neighbor_state = PlannerItemState.objects.select_for_update(nowait=True).get(
                        plan=plan,
                        id=neighbor_id,
                        item__is_active=True,
                    )
                except PlannerItemState.DoesNotExist:
                    return JsonResponse({"error": "Unknown neighbor id."}, status=400)

            if neighbor_state and neighbor_state.pinned != moved_state.pinned:
                return JsonResponse({"error": "Cannot move items across pinned boundary."}, status=400)

            block_states = list(
                PlannerItemState.objects
                .select_for_update(nowait=True)
                .filter(plan=plan, pinned=moved_state.pinned, item__is_active=True)
                .order_by("planned_order", "id")
            )
            block_lookup = {state.id: state for state in block_states}
            if moved_state.id not in block_lookup:
                return JsonResponse({"error": "Moved item not in reorderable block."}, status=400)
            if neighbor_state and neighbor_state.id not in block_lookup:
                return JsonResponse({"error": "Neighbor not in reorderable block."}, status=400)

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
                PlannerItemState.objects.bulk_update(updated, ["planned_order", "last_planned_at"])
    except DatabaseError:
        return JsonResponse({"error": "Planner is busy, please retry."}, status=409)

    return JsonResponse({"status": "ok"})


@login_required
def add_from_sources(request: HttpRequest) -> JsonResponse:
    response = _assert_post(request)
    if response:
        return response
    plan = _get_or_create_plan(request)
    added = _handle_add_from_sources(request, plan)
    return JsonResponse({"status": "ok", "added": added})


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
