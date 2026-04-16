from __future__ import annotations

from connectors.models import ConnectorAccount
from core.constants import SOURCE_CHOICES
from core.models import TimestampedModel
from django.conf import settings
from django.db import models
from django.utils import timezone
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

    WRITEBACK_STATUS_NONE = "none"
    WRITEBACK_STATUS_PENDING = "pending"
    WRITEBACK_STATUS_SYNCED = "synced"
    WRITEBACK_STATUS_FAILED = "failed"
    WRITEBACK_STATUS_UNSUPPORTED = "unsupported"

    WRITEBACK_STATUS_CHOICES = [
        (WRITEBACK_STATUS_NONE, "none"),
        (WRITEBACK_STATUS_PENDING, "pending"),
        (WRITEBACK_STATUS_SYNCED, "synced"),
        (WRITEBACK_STATUS_FAILED, "failed"),
        (WRITEBACK_STATUS_UNSUPPORTED, "unsupported"),
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
    external_status_requested = models.CharField(
        max_length=20,
        choices=PLANNER_STATUS_CHOICES,
        null=True,
        blank=True,
    )
    writeback_status = models.CharField(
        max_length=20,
        choices=WRITEBACK_STATUS_CHOICES,
        default=WRITEBACK_STATUS_NONE,
    )
    last_writeback_job_id = models.UUIDField(null=True, blank=True)
    last_writeback_error = models.TextField(blank=True)
    last_writeback_attempted_at = models.DateTimeField(null=True, blank=True)
    last_writeback_succeeded_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        indexes = [
            models.Index(fields=["plan", "planned_order"]),
            models.Index(fields=["plan", "planned_status"]),
            models.Index(fields=["plan", "planner_status"]),
            models.Index(fields=["plan", "writeback_status"]),
        ]
        constraints = [
            models.UniqueConstraint(fields=["plan", "item"], name="planner_item_state_unique"),
        ]

    def __str__(self) -> str:
        return f"{self.plan_id}:{self.item_id}"


class PlannerStatusIntent(models.Model):
    STATUS_PENDING = "pending"
    STATUS_SYNCED = "synced"
    STATUS_FAILED = "failed"
    STATUS_UNSUPPORTED = "unsupported"
    STATUS_STALE = "stale"

    STATUS_CHOICES = [
        (STATUS_PENDING, "pending"),
        (STATUS_SYNCED, "synced"),
        (STATUS_FAILED, "failed"),
        (STATUS_UNSUPPORTED, "unsupported"),
        (STATUS_STALE, "stale"),
    ]

    workspace = models.ForeignKey(Workspace, on_delete=models.CASCADE)
    plan = models.ForeignKey(PlannerPlan, on_delete=models.CASCADE, related_name="status_intents")
    item = models.ForeignKey(PlannerItem, on_delete=models.CASCADE, related_name="status_intents")
    state = models.ForeignKey(
        PlannerItemState,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="status_intents",
    )
    connector_account = models.ForeignKey(
        ConnectorAccount,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="planner_status_intents",
    )
    requested_planner_status = models.CharField(
        max_length=20,
        choices=PlannerItemState.PLANNER_STATUS_CHOICES,
    )
    provider_status_at_request = models.CharField(max_length=100, blank=True)
    provider_completed_at_request = models.BooleanField(default=False)
    resolved_provider_status = models.CharField(max_length=100, blank=True)
    resolved_external_completed = models.BooleanField(null=True, blank=True)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default=STATUS_PENDING)
    job = models.ForeignKey(
        "ingestion.Job",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="planner_status_intents",
    )
    attempts = models.IntegerField(default=0)
    last_error = models.TextField(blank=True)
    requested_at = models.DateTimeField(default=timezone.now)
    last_attempted_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        indexes = [
            models.Index(fields=["workspace", "status", "requested_at"]),
            models.Index(fields=["plan", "item", "status"]),
            models.Index(fields=["job"]),
        ]

    def __str__(self) -> str:
        return f"{self.item_id}:{self.requested_planner_status} ({self.status})"
