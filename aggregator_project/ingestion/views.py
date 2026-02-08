from __future__ import annotations

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.shortcuts import redirect, render
from django.conf import settings

from ingestion.models import Job
from ingestion.providers import get_provider_choices
from ingestion.services.jobs import queue_sync_jobs, run_job


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

    status_filter = request.GET.get("status", "")
    recent_runs = (
        Job.objects.for_workspace(request.workspace)
        .filter(job_type="sync")
        .select_related("connector_account")
    )
    if status_filter:
        recent_runs = recent_runs.filter(status=status_filter)
    recent_runs = recent_runs.order_by("-queued_at")[:25]
    for run in recent_runs:
        display_status, updated_at = _resolve_job_display(run)
        run.display_status = display_status
        run.last_updated_at = updated_at
    context = {
        "recent_runs": recent_runs,
        "source_choices": get_provider_choices(),
        "status_filter": status_filter,
        "status_choices": Job.STATUS_CHOICES,
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
