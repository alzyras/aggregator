from __future__ import annotations

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("events", "0001_initial"),
    ]

    operations = [
        migrations.AddField(
            model_name="event",
            name="event_type",
            field=models.CharField(default="event_recorded", max_length=100),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name="event",
            name="external_status",
            field=models.CharField(
                blank=True,
                help_text="Status in the source system at the time of the event",
                max_length=50,
                null=True,
            ),
        ),
        migrations.AddField(
            model_name="event",
            name="source_event_version",
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
        migrations.RemoveField(
            model_name="event",
            name="status",
        ),
    ]
