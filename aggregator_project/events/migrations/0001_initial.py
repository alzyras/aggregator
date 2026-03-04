from __future__ import annotations

import uuid

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    initial = True

    dependencies = [
        ("connectors", "0001_initial"),
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
                            ("google_fit", "Google Fit"),
                            ("asana", "Asana"),
                            ("todoist", "Todoist"),
                            ("habitica", "Habitica"),
                        ],
                        max_length=50,
                    ),
                ),
                ("source_entity_type", models.CharField(max_length=100)),
                ("source_entity_id", models.CharField(max_length=255)),
                ("event_type", models.CharField(max_length=100)),
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
                (
                    "external_status",
                    models.CharField(
                        blank=True,
                        help_text="Status in the source system at the time of the event",
                        max_length=50,
                        null=True,
                    ),
                ),
                ("external_actor_id", models.CharField(blank=True, max_length=255, null=True)),
                ("external_actor_type", models.CharField(blank=True, max_length=50, null=True)),
                ("external_actor_display_name", models.CharField(blank=True, max_length=255, null=True)),
                ("external_actor_raw", models.JSONField(blank=True, null=True)),
                ("source_event_version", models.CharField(blank=True, max_length=255, null=True)),
                ("raw", models.JSONField()),
                ("dedupe_hash", models.CharField(max_length=64)),
                (
                    "connector_account",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="events",
                        to="connectors.connectoraccount",
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
                        fields=["workspace", "source", "source_entity_id"],
                        name="ev_ws_src_entity_idx",
                    ),
                    models.Index(
                        fields=["workspace", "start_time"],
                        name="ev_ws_start_idx",
                    ),
                    models.Index(
                        fields=["workspace", "created_at"],
                        name="ev_ws_created_idx",
                    ),
                    models.Index(
                        fields=["workspace", "source", "created_at"],
                        name="ev_ws_src_created_idx",
                    ),
                    models.Index(
                        fields=["workspace", "source", "dedupe_hash"],
                        name="ev_ws_src_dedupe_idx",
                    ),
                    models.Index(
                        fields=["workspace", "connector_account", "created_at"],
                        name="ev_ws_conn_created_idx",
                    ),
                    models.Index(
                        fields=["workspace", "source", "external_actor_id"],
                        name="ev_ws_src_actor_idx",
                    ),
                    models.Index(
                        fields=["workspace", "external_actor_id"],
                        name="ev_ws_actor_idx",
                    ),
                ],
                "constraints": [
                    models.UniqueConstraint(
                        fields=("workspace", "connector_account", "source", "dedupe_hash"),
                        name="unique_event_dedupe",
                    )
                ],
            },
        ),
    ]
