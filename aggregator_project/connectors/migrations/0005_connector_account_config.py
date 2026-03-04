from __future__ import annotations

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("connectors", "0004_connector_account_status_sync"),
    ]

    operations = [
        migrations.AddField(
            model_name="connectoraccount",
            name="config",
            field=models.JSONField(blank=True, default=dict),
        ),
    ]
