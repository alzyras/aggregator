from __future__ import annotations

from dataclasses import dataclass
from functools import wraps
from typing import Callable

from django.apps import apps
from django.http import HttpRequest, JsonResponse
from django.shortcuts import redirect


@dataclass(frozen=True)
class PluginSpec:
    plugin_id: str
    label: str
    description: str
    nav_label: str
    url_name: str
    urlconf: str
    icon: str
    order: int = 100
    default_enabled: bool = False


def get_plugin_specs() -> list[PluginSpec]:
    specs = [
        spec
        for app_config in apps.get_app_configs()
        if (spec := getattr(app_config, "plugin_spec", None)) is not None
    ]
    return sorted(specs, key=lambda spec: (spec.order, spec.label.lower()))


def get_plugin_spec(plugin_id: str) -> PluginSpec | None:
    return next(
        (spec for spec in get_plugin_specs() if spec.plugin_id == plugin_id), None
    )


def plugin_required(plugin_id: str) -> Callable:
    def decorator(view_func: Callable) -> Callable:
        @wraps(view_func)
        def wrapped(request: HttpRequest, *args, **kwargs):
            from plugin_system.services import is_plugin_enabled

            workspace = getattr(request, "workspace", None)
            if workspace is not None and is_plugin_enabled(workspace, plugin_id):
                return view_func(request, *args, **kwargs)
            if request.headers.get("Accept", "").startswith("application/json"):
                return JsonResponse({"error": "Plugin is not enabled."}, status=403)
            return redirect("plugin_system:catalog")

        return wrapped

    return decorator
