from __future__ import annotations

from django.core.management.base import BaseCommand
from django.db.models import Count
from django.utils import timezone

from ingestion.models import Job


class Command(BaseCommand):
    help = "Print a local queue health summary."

    def handle(self, *args, **options):
        now = timezone.now()
        counts = (
            Job.objects
            .values("status")
            .annotate(count=Count("id"))
            .order_by("status")
        )
        self.stdout.write("Queue health")
        for row in counts:
            self.stdout.write(f"- {row['status']}: {row['count']}")
        oldest_due = (
            Job.objects
            .filter(status=Job.STATUS_QUEUED, next_run_at__lte=now)
            .order_by("next_run_at")
            .values_list("next_run_at", flat=True)
            .first()
        )
        lag = max(int((now - oldest_due).total_seconds()), 0) if oldest_due else 0
        self.stdout.write(f"- queue_lag_seconds: {lag}")
