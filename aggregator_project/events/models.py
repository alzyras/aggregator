from __future__ import annotations

import uuid

from django.db import models

from core.constants import SOURCE_CHOICES
from core.models import TimestampedModel


class Event(TimestampedModel):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    source = models.CharField(max_length=50, choices=SOURCE_CHOICES)
    source_entity_type = models.CharField(max_length=100)
    source_entity_id = models.CharField(max_length=255)
    title = models.CharField(max_length=255, null=True, blank=True)
    description = models.TextField(null=True, blank=True)
    start_time = models.DateTimeField(null=True, blank=True)
    end_time = models.DateTimeField(null=True, blank=True)
    metric_type = models.CharField(max_length=100, null=True, blank=True)
    metric_value = models.DecimalField(max_digits=12, decimal_places=3, null=True, blank=True)
    metric_unit = models.CharField(max_length=50, null=True, blank=True)
    status = models.CharField(max_length=50, null=True, blank=True)
    raw = models.JSONField()
    dedupe_hash = models.CharField(max_length=64)

    class Meta:
        indexes = [
            models.Index(fields=["source", "source_entity_id"]),
            models.Index(fields=["start_time"]),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=["source", "dedupe_hash"], name="unique_event_dedupe"
            )
        ]

    def __str__(self) -> str:
        return f"{self.get_source_display()} - {self.title or self.source_entity_id}"
