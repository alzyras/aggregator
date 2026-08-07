from __future__ import annotations

from plugin_system.models import PluginActivation
from plugin_system.registry import PluginSpec, get_plugin_spec
from workspaces.models import Workspace


def activation_map(workspace: Workspace) -> dict[str, bool]:
    return dict(
        PluginActivation.objects.for_workspace(workspace).values_list(
            "plugin_id", "enabled"
        )
    )


def is_plugin_enabled(workspace: Workspace, plugin_id: str) -> bool:
    activation = (
        PluginActivation.objects.for_workspace(workspace)
        .filter(plugin_id=plugin_id)
        .values_list("enabled", flat=True)
        .first()
    )
    if activation is not None:
        return activation
    spec = get_plugin_spec(plugin_id)
    return bool(spec and spec.default_enabled)


def set_plugin_enabled(
    workspace: Workspace, spec: PluginSpec, enabled: bool
) -> PluginActivation:
    activation, _created = PluginActivation.objects.update_or_create(
        workspace=workspace,
        plugin_id=spec.plugin_id,
        defaults={"enabled": enabled},
    )
    return activation
