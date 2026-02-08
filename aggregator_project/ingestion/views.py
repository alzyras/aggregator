from __future__ import annotations

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.shortcuts import redirect, render

from ingestion.models import SyncRun
from ingestion.providers import get_provider_choices
from ingestion.services.sync import sync_all_sources


@login_required
def sync_view(request):
    if request.method == "POST":
        runs = sync_all_sources(request.workspace)
        messages.success(request, f"Sync complete. Runs: {len(runs)}")
        return redirect("sync_view")

    recent_runs = (
        SyncRun.objects.for_workspace(request.workspace)
        .order_by("-started_at")[:25]
    )
    context = {
        "recent_runs": recent_runs,
        "source_choices": get_provider_choices(),
    }
    return render(request, "sync.html", context)
