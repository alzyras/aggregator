from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.conf import settings
from django.shortcuts import redirect, render
from django.utils import timezone

from connectors.models import ConnectorAccount

from ingestion.models import Job
from ingestion.providers import get_provider_choices
from ingestion.services.jobs import queue_sync_jobs, run_job

RUN_STATUS_PRECEDENCE = (
    Job.STATUS_FAILED,
    Job.STATUS_RUNNING,
    Job.STATUS_QUEUED,
    Job.STATUS_SUCCESS,
    Job.STATUS_CANCELLED,
)

BUBBLE_STATUS_ALLOWED = {
    Job.STATUS_SUCCESS,
    Job.STATUS_FAILED,
    Job.STATUS_RUNNING,
    Job.STATUS_QUEUED,
    Job.STATUS_CANCELLED,
    "missing",
}


@login_required
def sync_view(request):
    if request.method == "POST":
        jobs = queue_sync_jobs(
            workspace=request.workspace,
            created_by=request.user,
        )
        if not jobs:
            messages.warning(
                request,
                "No active connector accounts to sync. Add a connector and try again.",
            )
            return redirect("sync_view")

        connector_labels = [_connector_label(job) for job in jobs]
        connector_summary = ", ".join(connector_labels)

        if settings.DEBUG and request.POST.get("run_immediately"):
            results = []
            for job in jobs:
                result = run_job(job.id)
                results.append((result, _connector_label(result)))
            status_parts = [
                f"{label} — {result.status}" for result, label in results
            ]
            if any(result.status == Job.STATUS_FAILED for result, _label in results):
                messages.warning(
                    request,
                    "Sync completed with errors: " + "; ".join(status_parts),
                )
            else:
                messages.success(
                    request,
                    "Sync completed: " + "; ".join(status_parts),
                )
            return redirect("jobs_list")
        messages.success(
            request,
            f"Queued {len(jobs)} sync jobs: {connector_summary}.",
        )
        return redirect("sync_view")

    status_filter = (request.GET.get("status") or "").strip()
    allowed_statuses = {value for value, _label in Job.STATUS_CHOICES}
    if status_filter not in allowed_statuses:
        status_filter = ""

    sync_jobs = list(
        Job.objects.for_workspace(request.workspace)
        .filter(job_type="sync")
        .select_related("connector_account", "created_by")
        .order_by("-queued_at")[:300]
    )
    dashboard = _build_runs_dashboard(
        workspace=request.workspace,
        sync_jobs=sync_jobs,
        status_filter=status_filter,
    )

    context = {
        "debug": settings.DEBUG,
        "source_choices": get_provider_choices(),
        "status_filter": status_filter,
        "status_choices": Job.STATUS_CHOICES,
        **dashboard,
    }
    return render(request, "sync.html", context)


@login_required
def jobs_list(request):
    status_filter = request.GET.get("status", "")
    jobs = Job.objects.for_workspace(request.workspace).select_related(
        "connector_account"
    )
    if status_filter:
        jobs = jobs.filter(status=status_filter)
    jobs = jobs.order_by("-queued_at")
    return render(
        request,
        "jobs_list.html",
        {"jobs": jobs, "status_filter": status_filter, "status_choices": Job.STATUS_CHOICES},
    )


def _connector_label(job: Job) -> str:
    account = job.connector_account
    if not account:
        return "Unknown connector"
    return f"{account.get_source_display()} ({account.display_name})"


def _connector_label_from_account(account: ConnectorAccount | None) -> str:
    if not account:
        return "Unknown connector"
    return f"{account.get_source_display()} ({account.display_name})"


def _build_runs_dashboard(
    *,
    workspace,
    sync_jobs: list[Job],
    status_filter: str,
    chart_limit: int = 20,
    run_overview_days: int = 30,
    table_limit: int = 40,
) -> dict[str, object]:
    workspace_connectors = list(
        ConnectorAccount.objects.for_workspace(workspace).order_by("source", "display_name")
    )
    connector_labels: dict[str, str] = {
        str(account.id): _connector_label_from_account(account)
        for account in workspace_connectors
    }

    grouped_jobs: dict[str, list[Job]] = defaultdict(list)
    for job in sync_jobs:
        run_id = _get_run_group_id(job)
        grouped_jobs[run_id].append(job)
        connector_key = _get_connector_key(job)
        connector_labels.setdefault(connector_key, _connector_label(job))

    run_groups: list[dict[str, object]] = []
    for run_id, jobs in grouped_jobs.items():
        statuses: list[str] = []
        status_counts = {
            Job.STATUS_QUEUED: 0,
            Job.STATUS_RUNNING: 0,
            Job.STATUS_SUCCESS: 0,
            Job.STATUS_FAILED: 0,
            Job.STATUS_CANCELLED: 0,
        }
        start_candidates: list[datetime] = []
        end_candidates: list[datetime] = []
        queued_candidates: list[datetime] = []
        connector_cells: dict[str, dict[str, object]] = {}
        connector_names: list[str] = []
        sources: set[str] = set()
        since_values: set[str] = set()
        full_sync_values: set[bool] = set()

        for job in jobs:
            display_status, _updated_at = _resolve_job_display(job)
            display_status = _normalize_bubble_status(display_status)
            statuses.append(display_status)
            status_counts[display_status] = status_counts.get(display_status, 0) + 1

            if job.started_at or job.queued_at:
                start_candidates.append(job.started_at or job.queued_at)
            if job.finished_at or job.started_at or job.queued_at:
                end_candidates.append(job.finished_at or job.started_at or job.queued_at)
            if job.queued_at:
                queued_candidates.append(job.queued_at)

            connector_key = _get_connector_key(job)
            connector_label = connector_labels.get(connector_key, _connector_label(job))
            if connector_label not in connector_names:
                connector_names.append(connector_label)

            job_duration_seconds = _job_duration_seconds(job)
            start_time = job.started_at or job.queued_at
            connector_cells[connector_key] = _build_run_bubble_cell(
                status=display_status,
                duration_seconds=job_duration_seconds,
                connector_label=connector_label,
                run_id=run_id,
                run_short_id=run_id[:8],
                start_time=start_time,
            )

            source = ""
            if job.connector_account:
                source = job.connector_account.get_source_display()
            elif isinstance(job.input_params, dict):
                source = str(job.input_params.get("source") or "")
            if source:
                sources.add(source)
            if isinstance(job.input_params, dict):
                since_value = str(job.input_params.get("since") or "").strip()
                if since_value:
                    since_values.add(since_value)
                full_sync_values.add(bool(job.input_params.get("full_sync")))

        run_status = _aggregate_run_status(statuses)
        start_time = min(start_candidates) if start_candidates else min(queued_candidates) if queued_candidates else None
        end_time = max(end_candidates) if end_candidates else start_time
        duration_seconds = (
            max(int((end_time - start_time).total_seconds()), 0)
            if start_time and end_time
            else 0
        )

        status_summary = ", ".join(
            f"{count} {status}"
            for status, count in status_counts.items()
            if count
        )
        run_groups.append(
            {
                "run_id": run_id,
                "short_run_id": run_id[:8],
                "start_time": start_time,
                "end_time": end_time,
                "duration_seconds": duration_seconds,
                "duration_label": _format_duration(duration_seconds),
                "status": run_status,
                "launched_by": _resolve_run_launched_by(jobs),
                "connector_cells": connector_cells,
                "connector_count": len(connector_cells),
                "connector_names": ", ".join(connector_names) if connector_names else "No connectors",
                "run_params": _build_group_params(
                    run_id=run_id,
                    sources=sources,
                    since_values=since_values,
                    full_sync_values=full_sync_values,
                ),
                "summary_text": status_summary,
                "sort_time": start_time or end_time,
            }
        )

    run_groups.sort(key=_run_sort_key, reverse=True)
    if status_filter:
        run_groups = [run for run in run_groups if run["status"] == status_filter]

    table_runs = run_groups[:table_limit]
    chart_runs = list(reversed(run_groups[:chart_limit]))
    day_columns = _build_day_columns(run_overview_days=run_overview_days)
    day_keys = {column["day_key"] for column in day_columns}

    durations = [run["duration_seconds"] for run in chart_runs]
    max_duration = max(durations) if durations else 1
    threshold_seconds = _percentile(durations, 0.9) if durations else 0
    threshold_height = (
        max(2, int((threshold_seconds / max_duration) * 100))
        if threshold_seconds and max_duration
        else 0
    )
    for run in chart_runs:
        duration_seconds = run["duration_seconds"]
        run["bar_height"] = (
            max(8, int((duration_seconds / max_duration) * 100))
            if duration_seconds and max_duration
            else 6
        )

    latest_cell_by_connector_day: dict[tuple[str, str], dict[str, str | int]] = {}
    for run in run_groups:
        run_start = run.get("start_time")
        if not run_start:
            continue
        day_key = run_start.date().isoformat()
        if day_key not in day_keys:
            continue
        for connector_key, connector_cell in run["connector_cells"].items():
            lookup_key = (connector_key, day_key)
            if lookup_key not in latest_cell_by_connector_day:
                latest_cell_by_connector_day[lookup_key] = connector_cell

    sorted_connectors = sorted(connector_labels.items(), key=lambda item: item[1].lower())
    connector_run_rows = []
    for connector_key, connector_label in sorted_connectors:
        cells = []
        has_data = False
        for column in day_columns:
            connector_cell = latest_cell_by_connector_day.get((connector_key, column["day_key"]))
            if connector_cell:
                has_data = True
                cells.append(connector_cell)
                continue
            cells.append(
                _build_run_bubble_cell(
                    status="missing",
                    duration_seconds=0,
                    connector_label=connector_label,
                    run_id=f"day-{column['day_key']}",
                    run_short_id=column["day_number"],
                    start_time=column["day_start_time"],
                )
            )
        connector_run_rows.append(
            {
                "connector_label": connector_label,
                "cells": cells,
                "has_data": has_data,
            }
        )

    return {
        "run_groups": table_runs,
        "duration_runs": chart_runs,
        "duration_threshold_label": _format_duration(threshold_seconds),
        "duration_threshold_height": threshold_height,
        "run_overview_runs": day_columns,
        "connector_run_rows": connector_run_rows,
    }


def _get_run_group_id(job: Job) -> str:
    if isinstance(job.input_params, dict):
        run_group_id = str(job.input_params.get("run_group_id") or "").strip()
        if run_group_id:
            return run_group_id
    return str(job.id)


def _get_connector_key(job: Job) -> str:
    if job.connector_account_id:
        return str(job.connector_account_id)
    return f"unknown-{job.id}"


def _job_duration_seconds(job: Job) -> int:
    if not job.started_at:
        return 0
    end_time = job.finished_at or job.started_at
    return max(int((end_time - job.started_at).total_seconds()), 0)


def _bubble_size_class(duration_seconds: int) -> str:
    if duration_seconds >= 900:
        return "run-bubble-size-large"
    if duration_seconds >= 180:
        return "run-bubble-size-medium"
    return "run-bubble-size-small"


def _normalize_bubble_status(raw_status: str | None) -> str:
    status = (raw_status or "").strip().lower()
    if status not in BUBBLE_STATUS_ALLOWED:
        return "missing"
    return status


def _build_run_bubble_cell(
    *,
    status: str | None,
    duration_seconds: int,
    connector_label: str,
    run_id: str,
    run_short_id: str,
    start_time,
) -> dict[str, str | int]:
    normalized_status = _normalize_bubble_status(status)
    duration_label = _format_duration(duration_seconds)
    date_text = start_time.date().isoformat() if start_time else "n/a"
    full_time_text = start_time.strftime("%Y-%m-%d %H:%M:%S %Z") if start_time else "n/a"
    status_label = normalized_status.title()
    return {
        "status": normalized_status,
        "duration_seconds": duration_seconds,
        "duration_label": duration_label,
        "size_class": _bubble_size_class(duration_seconds),
        "status_label": status_label,
        "tooltip": (
            f"Run {run_short_id} • {connector_label} • {status_label} • "
            f"{duration_label} • {date_text}"
        ),
        "title": (
            f"Run {run_id} • {connector_label} • {status_label} • "
            f"{duration_label} • {full_time_text}"
        ),
        "aria_label": (
            f"{connector_label}. Run {run_short_id}. Status {status_label}. "
            f"Duration {duration_label}. {date_text}."
        ),
    }


def _build_day_columns(*, run_overview_days: int) -> list[dict[str, object]]:
    today = timezone.localdate()
    columns: list[dict[str, object]] = []
    for day_offset in range(0, run_overview_days):
        day_value = today - timedelta(days=day_offset)
        columns.append(
            {
                "day_key": day_value.isoformat(),
                "day_number": f"{day_value.day:02d}",
                "day_title": day_value.strftime("%Y-%m-%d"),
                "day_start_time": timezone.make_aware(datetime.combine(day_value, datetime.min.time())),
            }
        )
    return columns


def _aggregate_run_status(statuses: list[str]) -> str:
    for status in RUN_STATUS_PRECEDENCE:
        if status in statuses:
            return status
    return Job.STATUS_QUEUED


def _resolve_run_launched_by(jobs: list[Job]) -> str:
    users = [job.created_by for job in jobs if job.created_by]
    if users:
        user = users[0]
        full_name = getattr(user, "get_full_name", lambda: "")()
        return full_name or getattr(user, "username", "User")
    return "Scheduler"


def _build_group_params(
    *,
    run_id: str,
    sources: set[str],
    since_values: set[str],
    full_sync_values: set[bool],
) -> dict[str, object]:
    params: dict[str, object] = {
        "run_group_id": run_id,
        "sources": sorted(sources),
    }
    if since_values:
        params["since"] = sorted(since_values) if len(since_values) > 1 else next(iter(since_values))
    if full_sync_values:
        params["full_sync"] = True in full_sync_values
    return params


def _format_duration(duration_seconds: int) -> str:
    if duration_seconds <= 0:
        return "0s"
    minutes, seconds = divmod(duration_seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}h {minutes}m {seconds}s"
    if minutes:
        return f"{minutes}m {seconds}s"
    return f"{seconds}s"


def _percentile(values: list[int], fraction: float) -> int:
    if not values:
        return 0
    ordered = sorted(values)
    index = int(round((len(ordered) - 1) * fraction))
    return ordered[max(0, min(index, len(ordered) - 1))]


def _run_sort_key(run: dict[str, object]) -> float:
    sort_time = run.get("sort_time")
    if sort_time is None:
        return float("-inf")
    return sort_time.timestamp()


def _resolve_job_display(job: Job) -> tuple[str, object | None]:
    if job.status == Job.STATUS_CANCELLED:
        return Job.STATUS_CANCELLED, job.finished_at or job.started_at or job.queued_at
    if job.status == Job.STATUS_FAILED:
        return Job.STATUS_FAILED, job.finished_at or job.started_at or job.queued_at
    if job.finished_at:
        return Job.STATUS_SUCCESS, job.finished_at
    if job.started_at:
        return Job.STATUS_RUNNING, job.started_at
    if job.queued_at:
        return Job.STATUS_QUEUED, job.queued_at
    return job.status, None
