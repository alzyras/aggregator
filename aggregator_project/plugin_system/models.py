from __future__ import annotations

from django.db import models

from core.models import TimestampedModel
from workspaces.models import Workspace


class PluginActivationQuerySet(models.QuerySet):
    def for_workspace(self, workspace: Workspace) -> "PluginActivationQuerySet":
        return self.filter(workspace=workspace)


class PluginActivation(TimestampedModel):
    workspace = models.ForeignKey(
        Workspace,
        on_delete=models.CASCADE,
        related_name="plugin_activations",
    )
    plugin_id = models.SlugField(max_length=64)
    enabled = models.BooleanField(default=False)
    settings = models.JSONField(default=dict, blank=True)

    objects = PluginActivationQuerySet.as_manager()

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["workspace", "plugin_id"],
                name="unique_workspace_plugin_activation",
            )
        ]
        indexes = [models.Index(fields=["workspace", "enabled"])]

    def __str__(self) -> str:
        state = "enabled" if self.enabled else "disabled"
        return f"{self.workspace_id}:{self.plugin_id} ({state})"
