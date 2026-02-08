from __future__ import annotations

import uuid

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    initial = True

    dependencies = [
        ("workspaces", "0001_initial"),
    ]

    operations = [
        migrations.CreateModel(
            name="Event",
            fields=[
                (
                    "created_at",
                    models.DateTimeField(auto_now_add=True),
                ),
                ("updated_at", models.DateTimeField(auto_now=True)),
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
                    "source",
                    models.CharField(
                        choices=[
                            ("asana", "Asana"),
                            ("todoist", "Todoist"),
                            ("google_fit", "Google Fit"),
                            ("habitica", "Habitica"),
                            ("toggl", "Toggl"),
                            ("llm_summary", "LLM Summary"),
                        ],
                        max_length=50,
                    ),
                ),
                ("source_entity_type", models.CharField(max_length=100)),
                ("source_entity_id", models.CharField(max_length=255)),
                ("title", models.CharField(blank=True, max_length=255, null=True)),
                ("description", models.TextField(blank=True, null=True)),
                ("start_time", models.DateTimeField(blank=True, null=True)),
                ("end_time", models.DateTimeField(blank=True, null=True)),
                ("metric_type", models.CharField(blank=True, max_length=100, null=True)),
                (
                    "metric_value",
                    models.DecimalField(
                        blank=True, decimal_places=3, max_digits=12, null=True
                    ),
                ),
                ("metric_unit", models.CharField(blank=True, max_length=50, null=True)),
                ("status", models.CharField(blank=True, max_length=50, null=True)),
                ("raw", models.JSONField()),
                ("dedupe_hash", models.CharField(max_length=64)),
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
                        fields=["workspace", "source", "source_entity_id"],
                        name="events_workspace_source_entity_idx",
                    ),
                    models.Index(
                        fields=["workspace", "start_time"],
                        name="events_workspace_start_time_idx",
                    ),
                    models.Index(
                        fields=["workspace", "created_at"],
                        name="events_workspace_created_idx",
                    ),
                    models.Index(
                        fields=["workspace", "source", "created_at"],
                        name="events_workspace_source_created_idx",
                    ),
                    models.Index(
                        fields=["workspace", "source", "dedupe_hash"],
                        name="events_workspace_source_dedupe_idx",
                    ),
                ],
                "constraints": [
                    models.UniqueConstraint(
                        fields=("workspace", "source", "dedupe_hash"),
                        name="unique_event_dedupe",
                    )
                ],
            },
        ),
    ]
