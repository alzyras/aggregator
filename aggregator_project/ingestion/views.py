from __future__ import annotations

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.shortcuts import redirect, render
from django.conf import settings
from django.utils import timezone

from ingestion.models import Job
from ingestion.providers import get_provider_choices
from ingestion.services.jobs import enqueue_job, run_job


@login_required
def sync_view(request):
    if request.method == "POST":
        job = Job.objects.create(
            workspace=request.workspace,
            job_type="sync",
            job_name="sync_all",
            input_params={},
            created_by=request.user,
            next_run_at=timezone.now(),
        )
        enqueue_job(job.id)
        if settings.DEBUG and request.POST.get("run_immediately"):
            run_job(job.id)
            messages.success(request, "Sync job executed.")
            return redirect("jobs_list")
        messages.success(request, "Sync job queued.")
        return redirect("sync_view")

    recent_runs = (
        Job.objects.for_workspace(request.workspace)
        .filter(job_type="sync")
        .order_by("-queued_at")[:25]
    )
    context = {
        "recent_runs": recent_runs,
        "source_choices": get_provider_choices(),
    }
    return render(request, "sync.html", context)


@login_required
def jobs_list(request):
    jobs = Job.objects.for_workspace(request.workspace).order_by("-queued_at")
    return render(request, "jobs_list.html", {"jobs": jobs})
