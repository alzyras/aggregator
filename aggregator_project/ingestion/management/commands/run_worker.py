from __future__ import annotations

from django.core.management.base import BaseCommand

from ingestion.services.worker import run_worker_loop


class Command(BaseCommand):
    help = "Continuously run queued jobs respecting concurrency limits."

    def add_arguments(self, parser):
        parser.add_argument("--poll-seconds", type=int, default=5)

    def handle(self, *args, **options):
        poll_seconds = options.get("poll_seconds")
        run_worker_loop(poll_seconds=poll_seconds)
