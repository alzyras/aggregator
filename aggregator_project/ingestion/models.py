from __future__ import annotations

import uuid

from django.conf import settings
from django.core.validators import MaxValueValidator, MinValueValidator
from django.db import models
from django.utils import timezone
from django.core.exceptions import ValidationError

from connectors.models import ConnectorAccount
from core.models import TimestampedModel
from workspaces.models import Workspace


class WorkspaceQuerySet(models.QuerySet):
    def for_workspace(self, workspace: Workspace) -> "WorkspaceQuerySet":
        return self.filter(workspace=workspace)


class WorkspaceRefreshPolicy(TimestampedModel):
    """Workspace-owned policy for keeping connector-backed data current."""

    workspace = models.OneToOneField(
        Workspace,
        on_delete=models.CASCADE,
        related_name="refresh_policy",
    )
    auto_refresh_enabled = models.BooleanField(default=True)
    refreshes_per_day = models.PositiveSmallIntegerField(
        default=12,
        validators=[MinValueValidator(1), MaxValueValidator(96)],
    )
    full_refresh_interval_days = models.PositiveSmallIntegerField(
        default=7,
        validators=[MinValueValidator(1), MaxValueValidator(31)],
    )
    cache_version = models.PositiveBigIntegerField(default=1)

    objects = WorkspaceQuerySet.as_manager()

    def __str__(self) -> str:
        return f"Refresh policy for {self.workspace}"


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
    idempotency_key = models.CharField(max_length=255, blank=True, default="")
    queued_at = models.DateTimeField(auto_now_add=True)
    next_run_at = models.DateTimeField(default=timezone.now)
    started_at = models.DateTimeField(null=True, blank=True)
    finished_at = models.DateTimeField(null=True, blank=True)
    locked_at = models.DateTimeField(null=True, blank=True)
    locked_by = models.CharField(max_length=100, null=True, blank=True)
    lease_expires_at = models.DateTimeField(null=True, blank=True)
    attempt_count = models.IntegerField(default=0)
    max_attempts = models.IntegerField(null=True, blank=True)
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
            models.Index(fields=["idempotency_key", "status"]),
            models.Index(fields=["status", "lease_expires_at"]),
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


class JobAttempt(TimestampedModel):
    STATUS_RUNNING = "running"
    STATUS_SUCCESS = "success"
    STATUS_FAILED = "failed"
    STATUS_RETRYING = "retrying"
    STATUS_CHOICES = [
        (STATUS_RUNNING, "running"),
        (STATUS_SUCCESS, "success"),
        (STATUS_FAILED, "failed"),
        (STATUS_RETRYING, "retrying"),
    ]

    job = models.ForeignKey(Job, on_delete=models.CASCADE, related_name="attempts")
    attempt_number = models.IntegerField()
    worker_id = models.CharField(max_length=100, blank=True)
    started_at = models.DateTimeField(default=timezone.now)
    finished_at = models.DateTimeField(null=True, blank=True)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default=STATUS_RUNNING)
    error_message = models.TextField(blank=True)
    output_summary = models.JSONField(default=dict, blank=True)

    class Meta:
        indexes = [
            models.Index(fields=["job", "attempt_number"]),
            models.Index(fields=["status", "started_at"]),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=["job", "attempt_number"],
                name="job_attempt_unique_attempt_number",
            )
        ]

    def __str__(self) -> str:
        return f"{self.job_id}:{self.attempt_number} ({self.status})"
