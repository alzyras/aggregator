from __future__ import annotations

import uuid

from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion
from django.utils import timezone


class Migration(migrations.Migration):
    initial = True

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ("connectors", "0001_initial"),
        ("workspaces", "0001_initial"),
    ]

    operations = [
        migrations.CreateModel(
            name="Job",
            fields=[
                (
                    "id",
                    models.UUIDField(
                        default=uuid.uuid4,
                        editable=False,
                        primary_key=True,
                        serialize=False,
                    ),
                ),
                (
                    "job_type",
                    models.CharField(
                        help_text="Category, e.g. sync, aggregation, ai",
                        max_length=50,
                    ),
                ),
                (
                    "job_name",
                    models.CharField(
                        help_text="Specific job, e.g. sync_asana, daily_rollup",
                        max_length=100,
                    ),
                ),
                (
                    "status",
                    models.CharField(
                        choices=[
                            ("queued", "queued"),
                            ("running", "running"),
                            ("success", "success"),
                            ("failed", "failed"),
                            ("cancelled", "cancelled"),
                        ],
                        default="queued",
                        max_length=20,
                    ),
                ),
                ("priority", models.IntegerField(default=0)),
                ("queued_at", models.DateTimeField(auto_now_add=True)),
                ("next_run_at", models.DateTimeField(default=timezone.now)),
                ("started_at", models.DateTimeField(blank=True, null=True)),
                ("finished_at", models.DateTimeField(blank=True, null=True)),
                ("locked_at", models.DateTimeField(blank=True, null=True)),
                ("locked_by", models.CharField(blank=True, max_length=100, null=True)),
                ("attempt_count", models.IntegerField(default=0)),
                ("input_params", models.JSONField(blank=True, default=dict)),
                ("output_summary", models.JSONField(blank=True, default=dict)),
                ("error_message", models.TextField(blank=True)),
                ("error_traceback", models.TextField(blank=True)),
                (
                    "connector_account",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.CASCADE,
                        to="connectors.connectoraccount",
                    ),
                ),
                (
                    "created_by",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
                (
                    "workspace",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        to="workspaces.workspace",
                    ),
                ),
            ],
            options={
                "indexes": [
                    models.Index(
                        fields=["status", "next_run_at"],
                        name="ingestion_status_next_run_idx",
                    ),
                    models.Index(
                        fields=["workspace", "connector_account", "status"],
                        name="ingestion_workspace_connector_status_idx",
                    ),
                    models.Index(
                        fields=["workspace", "status", "queued_at"],
                        name="ingestion_workspace_status_queued_idx",
                    ),
                    models.Index(
                        fields=["workspace", "job_type", "queued_at"],
                        name="ingestion_workspace_job_type_queued_idx",
                    ),
                ]
            },
        ),
    ]
