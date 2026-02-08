from __future__ import annotations

from django.db import migrations


def drop_unique_connector_account(apps, schema_editor):
    if schema_editor.connection.vendor != "postgresql":
        return
    schema_editor.execute(
        "ALTER TABLE connectors_connectoraccount "
        "DROP CONSTRAINT IF EXISTS unique_connector_account"
    )


class Migration(migrations.Migration):
    dependencies = [
        ("connectors", "0002_rename_connectors_workspace_source_active_idx_connectors__workspa_959e7b_idx"),
    ]

    operations = [
        migrations.SeparateDatabaseAndState(
            database_operations=[migrations.RunPython(drop_unique_connector_account, migrations.RunPython.noop)],
            state_operations=[
                migrations.RemoveConstraint(
                    model_name="connectoraccount",
                    name="unique_connector_account",
                ),
            ],
        ),
    ]
