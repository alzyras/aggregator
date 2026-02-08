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
            messages.warning(request, "No active connector accounts to sync.")
            return redirect("sync_view")

        if settings.DEBUG and request.POST.get("run_immediately"):
            for job in jobs:
                run_job(job.id)
            messages.success(request, f"Ran {len(jobs)} sync jobs.")
            return redirect("jobs_list")
        messages.success(request, f"Queued {len(jobs)} sync jobs.")
        return redirect("sync_view")

    recent_runs = (
        Job.objects.for_workspace(request.workspace)
        .filter(job_type="sync")
        .select_related("connector_account")
        .order_by("-queued_at")[:25]
    )
    context = {
        "recent_runs": recent_runs,
        "source_choices": get_provider_choices(),
    }
    return render(request, "sync.html", context)


@login_required
def jobs_list(request):
    jobs = (
        Job.objects.for_workspace(request.workspace)
        .select_related("connector_account")
        .order_by("-queued_at")
    )
    return render(request, "jobs_list.html", {"jobs": jobs})
