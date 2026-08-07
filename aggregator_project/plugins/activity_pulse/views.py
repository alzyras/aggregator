from django.contrib.auth.decorators import login_required
from django.shortcuts import render

from plugin_system.registry import plugin_required
from plugins.activity_pulse.services import build_activity_snapshot


@login_required
@plugin_required("activity-pulse")
def index(request):
    return render(
        request,
        "plugins/activity_pulse/index.html",
        {
            "activity": build_activity_snapshot(
                workspace=request.workspace, user=request.user
            )
        },
    )
