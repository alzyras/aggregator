from __future__ import annotations

import uuid

from django.conf import settings
from django.db import models
from django.utils import timezone
from django.core.exceptions import ValidationError

from connectors.models import ConnectorAccount
from workspaces.models import Workspace


class WorkspaceQuerySet(models.QuerySet):
    def for_workspace(self, workspace: Workspace) -> "WorkspaceQuerySet":
        return self.filter(workspace=workspace)


class Job(models.Model):
    STATUS_QUEUED = "queued"
    STATUS_RUNNING = "running"
    STATUS_SUCCESS = "success"
    STATUS_FAILED = "failed"
    STATUS_CANCELLED = "cancelled"

    STATUS_CHOICES = [
        (STATUS_QUEUED, "queued"),
        (STATUS_RUNNING, "running"),
        (STATUS_SUCCESS, "success"),
        (STATUS_FAILED, "failed"),
        (STATUS_CANCELLED, "cancelled"),
    ]

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    workspace = models.ForeignKey(Workspace, on_delete=models.CASCADE)
    connector_account = models.ForeignKey(
        ConnectorAccount,
        null=True,
        blank=True,
        on_delete=models.CASCADE,
    )
    job_type = models.CharField(
        max_length=50,
        help_text="Category, e.g. sync, aggregation, ai",
    )
    job_name = models.CharField(
        max_length=100,
        help_text="Specific job, e.g. sync_asana, daily_rollup",
    )
    status = models.CharField(
        max_length=20,
        choices=STATUS_CHOICES,
        default=STATUS_QUEUED,
    )
    priority = models.IntegerField(default=0)
    queued_at = models.DateTimeField(auto_now_add=True)
    next_run_at = models.DateTimeField(default=timezone.now)
    started_at = models.DateTimeField(null=True, blank=True)
    finished_at = models.DateTimeField(null=True, blank=True)
    locked_at = models.DateTimeField(null=True, blank=True)
    locked_by = models.CharField(max_length=100, null=True, blank=True)
    attempt_count = models.IntegerField(default=0)
    input_params = models.JSONField(default=dict, blank=True)
    output_summary = models.JSONField(default=dict, blank=True)
    error_message = models.TextField(blank=True)
    error_traceback = models.TextField(blank=True)
    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
    )

    objects = WorkspaceQuerySet.as_manager()

    class Meta:
        indexes = [
            models.Index(fields=["status", "next_run_at"]),
            models.Index(fields=["workspace", "connector_account", "status"]),
            models.Index(fields=["workspace", "status", "queued_at"]),
            models.Index(fields=["workspace", "job_type", "queued_at"]),
        ]

    def display_name(self) -> str:
        if self.job_type == "sync" and self.connector_account:
            account = self.connector_account
            return f"Sync {account.get_source_display()} ({account.display_name})"
        return self.job_name

    def clean(self) -> None:
        if self.job_type == "sync":
            if not self.connector_account_id:
                raise ValidationError("Sync jobs must include a connector account.")
            if self.connector_account and self.connector_account.workspace_id != self.workspace_id:
                raise ValidationError("Job workspace must match connector account workspace.")

    def save(self, *args, **kwargs):  # noqa: ANN002, ANN003
        self.full_clean()
        return super().save(*args, **kwargs)

    def __str__(self) -> str:
        return f"{self.job_type}:{self.job_name} ({self.status})"
