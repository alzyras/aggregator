from __future__ import annotations

from django.core.management.base import BaseCommand
from django.utils.dateparse import parse_datetime

from ingestion.services.sync import sync_all_sources


class Command(BaseCommand):
    help = "Sync all sources or a subset of sources."

    def add_arguments(self, parser):
        parser.add_argument("--since", type=str, help="ISO datetime to sync since")
        parser.add_argument(
            "--source",
            action="append",
            dest="sources",
            help="Source to sync (can be specified multiple times)",
        )

    def handle(self, *args, **options):
        since_raw = options.get("since")
        sources = options.get("sources")
        since = parse_datetime(since_raw) if since_raw else None
        runs = sync_all_sources(since=since, sources=sources)
        self.stdout.write(self.style.SUCCESS(f"Sync complete. Runs: {len(runs)}"))
