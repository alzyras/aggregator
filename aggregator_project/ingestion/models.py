from __future__ import annotations

from django.db import models

from core.constants import SOURCE_CHOICES
from core.models import TimestampedModel


class SyncRun(TimestampedModel):
    STATUS_SUCCESS = "success"
    STATUS_FAILURE = "failure"
    STATUS_PARTIAL = "partial"

    STATUS_CHOICES = [
        (STATUS_SUCCESS, "Success"),
        (STATUS_FAILURE, "Failure"),
        (STATUS_PARTIAL, "Partial"),
    ]

    source = models.CharField(max_length=50, choices=SOURCE_CHOICES)
    started_at = models.DateTimeField()
    finished_at = models.DateTimeField(null=True, blank=True)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES)
    stats = models.JSONField(default=dict, blank=True)
    error = models.TextField(null=True, blank=True)

    class Meta:
        indexes = [
            models.Index(fields=["source", "started_at"]),
        ]

    def __str__(self) -> str:
        return f"{self.get_source_display()} - {self.status}"
