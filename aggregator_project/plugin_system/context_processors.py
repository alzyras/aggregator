from __future__ import annotations

from django.urls import NoReverseMatch, reverse

from plugin_system.registry import get_plugin_specs
from plugin_system.services import activation_map


def plugin_navigation(request) -> dict:
    workspace = getattr(request, "workspace", None)
    user = getattr(request, "user", None)
    if workspace is None or user is None or not user.is_authenticated:
        return {"plugin_nav_items": []}

    activations = activation_map(workspace)
    items = []
    for spec in get_plugin_specs():
        enabled = activations.get(spec.plugin_id, spec.default_enabled)
        if not enabled:
            continue
        try:
            url = reverse(spec.url_name)
        except NoReverseMatch:
            continue
        items.append(
            {
                "plugin_id": spec.plugin_id,
                "label": spec.nav_label,
                "icon": spec.icon,
                "url": url,
                "url_name": spec.url_name,
            }
        )
    return {"plugin_nav_items": items}
