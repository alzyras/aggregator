from __future__ import annotations

from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("connectors", "0002_rename_connectors_workspace_source_active_idx_connectors__workspa_959e7b_idx"),
    ]

    operations = [
        migrations.RemoveConstraint(
            model_name="connectoraccount",
            name="unique_connector_account",
        ),
    ]
