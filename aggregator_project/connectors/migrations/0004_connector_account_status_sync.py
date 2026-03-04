from __future__ import annotations

from django.db import migrations, models


def forwards(apps, schema_editor):
    ConnectorAccount = apps.get_model("connectors", "ConnectorAccount")
    for account in ConnectorAccount.objects.all():
        if account.status == "connecting":
            account.status = "validating"
        elif account.status == "disconnected":
            account.status = "revoked"
        account.save(update_fields=["status"])


def backwards(apps, schema_editor):
    ConnectorAccount = apps.get_model("connectors", "ConnectorAccount")
    for account in ConnectorAccount.objects.all():
        if account.status == "validating":
            account.status = "connecting"
        elif account.status == "revoked":
            account.status = "disconnected"
        account.save(update_fields=["status"])


class Migration(migrations.Migration):
    dependencies = [
        ("connectors", "0003_remove_unique_connector_account"),
    ]

    operations = [
        migrations.AddField(
            model_name="connectoraccount",
            name="last_sync_status",
            field=models.CharField(blank=True, choices=[("success", "Success"), ("failed", "Failed")], max_length=20, null=True),
        ),
        migrations.AlterField(
            model_name="connectoraccount",
            name="status",
            field=models.CharField(choices=[("validating", "Validating"), ("connected", "Connected"), ("error", "Error"), ("revoked", "Revoked")], default="validating", max_length=20),
        ),
        migrations.RunPython(forwards, backwards),
    ]
