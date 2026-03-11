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

from events.models import Event
from planner.models import PlannerItem, PlannerItemState, PlannerPlan
from planner.services.reconcile import add_items_from_events, ensure_item_state


PLANNER_EVENT_TYPES = ["task_created", "task_updated", "task_state", "task_completed"]


@login_required
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
    order = payload.get("order")
    if not isinstance(order, list):
        return JsonResponse({"error": "Invalid order payload."}, status=400)

    plan = _get_or_create_plan(request)
    ids = [str(value) for value in order]
    states = PlannerItemState.objects.filter(plan=plan, id__in=ids)
    if states.count() != len(ids):
        return JsonResponse({"error": "Unknown item in order list."}, status=400)

    with transaction.atomic():
        for index, state_id in enumerate(ids):
            PlannerItemState.objects.filter(plan=plan, id=state_id).update(
                planned_order=index + 1,
                last_planned_at=timezone.now(),
            )

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
