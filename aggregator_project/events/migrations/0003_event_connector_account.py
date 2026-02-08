from __future__ import annotations

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    dependencies = [
        ("connectors", "0004_connector_account_status_sync"),
        ("events", "0002_rename_events_workspace_source_entity_idx_events_even_workspa_78d99e_idx_and_more"),
    ]

    operations = [
        migrations.AddField(
            model_name="event",
            name="connector_account",
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name="events", to="connectors.connectoraccount"),
        ),
        migrations.RemoveConstraint(
            model_name="event",
            name="unique_event_dedupe",
        ),
        migrations.AddConstraint(
            model_name="event",
            constraint=models.UniqueConstraint(fields=("workspace", "connector_account", "source", "dedupe_hash"), name="unique_event_dedupe"),
        ),
        migrations.AddIndex(
            model_name="event",
            index=models.Index(fields=["workspace", "connector_account", "created_at"], name="events_workspa_9ae6d0_idx"),
        ),
    ]
