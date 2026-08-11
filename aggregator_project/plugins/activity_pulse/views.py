from django.contrib.auth.decorators import login_required
from django.shortcuts import render
from django.views.decorators.csrf import ensure_csrf_cookie

from ingestion.services.refresh import get_workspace_refresh_snapshot
from plugin_system.registry import plugin_required
from plugins.activity_pulse.services import build_activity_snapshot


@login_required
@ensure_csrf_cookie
@plugin_required("activity-pulse")
def index(request):
    refresh_state = get_workspace_refresh_snapshot(workspace=request.workspace)
    return render(
        request,
        "plugins/activity_pulse/index.html",
        {
            "activity": build_activity_snapshot(
                workspace=request.workspace,
                user=request.user,
                cache_version=refresh_state["policy"].cache_version,
            ),
            "refresh_state": refresh_state,
        },
    )
