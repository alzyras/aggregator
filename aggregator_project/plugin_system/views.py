from __future__ import annotations

import json

from django.contrib.auth.decorators import login_required
from django.http import HttpRequest, JsonResponse
from django.shortcuts import render
from django.urls import NoReverseMatch, reverse
from django.views.decorators.csrf import ensure_csrf_cookie
from django.views.decorators.http import require_POST

from plugin_system.registry import get_plugin_spec, get_plugin_specs
from plugin_system.services import activation_map, set_plugin_enabled


@login_required
@ensure_csrf_cookie
def plugin_catalog(request: HttpRequest):
    activations = activation_map(request.workspace)
    plugin_rows = []
    for spec in get_plugin_specs():
        enabled = activations.get(spec.plugin_id, spec.default_enabled)
        try:
            launch_url = reverse(spec.url_name)
        except NoReverseMatch:
            launch_url = ""
        plugin_rows.append({"spec": spec, "enabled": enabled, "launch_url": launch_url})
    return render(request, "plugin_system/catalog.html", {"plugin_rows": plugin_rows})


@login_required
@require_POST
def toggle_plugin(request: HttpRequest, plugin_id: str) -> JsonResponse:
    spec = get_plugin_spec(plugin_id)
    if spec is None:
        return JsonResponse({"error": "Unknown plugin."}, status=404)

    try:
        payload = json.loads(request.body or "{}")
    except json.JSONDecodeError:
        return JsonResponse({"error": "Invalid JSON."}, status=400)
    enabled = payload.get("enabled")
    if not isinstance(enabled, bool):
        return JsonResponse({"error": "enabled must be a boolean."}, status=400)

    activation = set_plugin_enabled(request.workspace, spec, enabled)
    return JsonResponse(
        {
            "plugin_id": spec.plugin_id,
            "enabled": activation.enabled,
            "nav": {
                "label": spec.nav_label,
                "icon": spec.icon,
                "url": reverse(spec.url_name),
                "url_name": spec.url_name,
            },
        }
    )
