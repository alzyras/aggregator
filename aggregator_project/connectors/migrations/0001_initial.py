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
            name="ConnectorAccount",
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
                    "provider",
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
                ("display_name", models.CharField(max_length=255)),
                (
                    "auth_type",
                    models.CharField(
                        choices=[("api_token", "API Token"), ("oauth", "OAuth")],
                        max_length=20,
                    ),
                ),
                ("encrypted_access_token", models.BinaryField()),
                ("encrypted_refresh_token", models.BinaryField(blank=True, null=True)),
                ("token_expires_at", models.DateTimeField(blank=True, null=True)),
                ("scopes", models.JSONField(blank=True, default=list)),
                ("external_account_id", models.CharField(blank=True, max_length=255, null=True)),
                ("revoked_at", models.DateTimeField(blank=True, null=True)),
                (
                    "status",
                    models.CharField(
                        choices=[
                            ("disconnected", "Disconnected"),
                            ("connecting", "Connecting"),
                            ("connected", "Connected"),
                            ("error", "Error"),
                        ],
                        default="disconnected",
                        max_length=20,
                    ),
                ),
                ("last_verified_at", models.DateTimeField(blank=True, null=True)),
                ("last_error", models.TextField(blank=True, null=True)),
                ("is_active", models.BooleanField(default=True)),
                ("last_sync_at", models.DateTimeField(blank=True, null=True)),
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
                        fields=["workspace", "provider", "is_active"],
                        name="connectors_workspace_provider_active_idx",
                    )
                ],
                "constraints": [
                    models.UniqueConstraint(
                        fields=("workspace", "provider"),
                        name="unique_connector_account",
                    )
                ],
            },
        ),
    ]
