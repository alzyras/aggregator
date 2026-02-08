from __future__ import annotations

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    initial = True

    dependencies = [
        ("workspaces", "0001_initial"),
    ]

    operations = [
        migrations.CreateModel(
            name="SyncRun",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True, primary_key=True, serialize=False, verbose_name="ID"
                    ),
                ),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
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
                ("started_at", models.DateTimeField()),
                ("finished_at", models.DateTimeField(blank=True, null=True)),
                (
                    "status",
                    models.CharField(
                        choices=[
                            ("success", "Success"),
                            ("failure", "Failure"),
                            ("partial", "Partial"),
                        ],
                        max_length=20,
                    ),
                ),
                ("stats", models.JSONField(blank=True, default=dict)),
                ("error", models.TextField(blank=True, null=True)),
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
                        fields=["workspace", "source", "started_at"],
                        name="ingestion_workspace_source_started_idx",
                    )
                ]
            },
        ),
    ]
