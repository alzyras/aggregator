from __future__ import annotations

from django.conf import settings
from django.db import models
from django.utils import timezone

from connectors.models import ConnectorAccount
from core.constants import SOURCE_CHOICES
from core.models import TimestampedModel
from workspaces.models import Workspace


class WorkspaceQuerySet(models.QuerySet):
    def for_workspace(self, workspace: Workspace) -> "WorkspaceQuerySet":
        return self.filter(workspace=workspace)


class PlannerItem(TimestampedModel):
    workspace = models.ForeignKey(Workspace, on_delete=models.CASCADE)
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
    )
    connector_account = models.ForeignKey(
        ConnectorAccount,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="planner_items",
    )
    source = models.CharField(max_length=50, choices=SOURCE_CHOICES)
    source_entity_id = models.CharField(max_length=255)
    title = models.CharField(max_length=255)
    description = models.TextField(null=True, blank=True)
    source_url = models.URLField(null=True, blank=True)
    source_status = models.CharField(max_length=100, null=True, blank=True)
    source_updated_at = models.DateTimeField(null=True, blank=True)
    last_synced_at = models.DateTimeField(null=True, blank=True)
    external_completed = models.BooleanField(default=False)
    is_active = models.BooleanField(default=True)

    objects = WorkspaceQuerySet.as_manager()

    class Meta:
        indexes = [
            models.Index(fields=["workspace", "source", "source_entity_id"]),
            models.Index(fields=["workspace", "connector_account", "source_entity_id"]),
            models.Index(fields=["workspace", "last_synced_at"]),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=["workspace", "connector_account", "source", "source_entity_id"],
                name="planner_item_unique_source",
            )
        ]

    def __str__(self) -> str:
        return f"{self.source}:{self.source_entity_id}"


class PlannerPlan(TimestampedModel):
    workspace = models.ForeignKey(Workspace, on_delete=models.CASCADE)
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
    )
    name = models.CharField(max_length=255, default="My Plan")
    start_date = models.DateField(null=True, blank=True)
    end_date = models.DateField(null=True, blank=True)
    timezone = models.CharField(max_length=64, default="UTC")

    objects = WorkspaceQuerySet.as_manager()

    class Meta:
        indexes = [
            models.Index(fields=["workspace", "user"]),
        ]

    def __str__(self) -> str:
        return self.name


class PlannerItemState(models.Model):
    PLANNER_STATUS_INBOX = "inbox"
    PLANNER_STATUS_BACKLOG = "backlog"
    PLANNER_STATUS_DOING = "doing"
    PLANNER_STATUS_DONE = "done"

    PLANNER_STATUS_CHOICES = [
        (PLANNER_STATUS_INBOX, "inbox"),
        (PLANNER_STATUS_BACKLOG, "backlog"),
        (PLANNER_STATUS_DOING, "doing"),
        (PLANNER_STATUS_DONE, "done"),
    ]

    STATUS_PLANNED = "planned"
    STATUS_IN_PROGRESS = "in_progress"
    STATUS_DONE = "done"
    STATUS_DEFERRED = "deferred"

    STATUS_CHOICES = [
        (STATUS_PLANNED, "planned"),
        (STATUS_IN_PROGRESS, "in_progress"),
        (STATUS_DONE, "done"),
        (STATUS_DEFERRED, "deferred"),
    ]

    plan = models.ForeignKey(PlannerPlan, on_delete=models.CASCADE, related_name="item_states")
    item = models.ForeignKey(PlannerItem, on_delete=models.CASCADE, related_name="planner_states")
    planner_status = models.CharField(
        max_length=20,
        choices=PLANNER_STATUS_CHOICES,
        default=PLANNER_STATUS_INBOX,
    )
    planned_status = models.CharField(max_length=30, choices=STATUS_CHOICES, default=STATUS_PLANNED)
    planned_order = models.IntegerField(default=0)
    planned_start = models.DateTimeField(null=True, blank=True)
    planned_end = models.DateTimeField(null=True, blank=True)
    pinned = models.BooleanField(default=False)
    notes = models.TextField(null=True, blank=True)
    last_planned_at = models.DateTimeField(default=timezone.now)

    class Meta:
        indexes = [
            models.Index(fields=["plan", "planned_order"]),
            models.Index(fields=["plan", "planned_status"]),
            models.Index(fields=["plan", "planner_status"]),
        ]
        constraints = [
            models.UniqueConstraint(fields=["plan", "item"], name="planner_item_state_unique"),
        ]

    def __str__(self) -> str:
        return f"{self.plan_id}:{self.item_id}"
