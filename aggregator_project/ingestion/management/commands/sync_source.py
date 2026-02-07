from __future__ import annotations

from django.core.management.base import BaseCommand, CommandError
from django.utils.dateparse import parse_datetime

from ingestion.providers import get_provider_sources
from ingestion.services.sync import sync_source


class Command(BaseCommand):
    help = "Sync a single source."

    def add_arguments(self, parser):
        parser.add_argument("--source", required=True, type=str, help="Source to sync")
        parser.add_argument("--since", type=str, help="ISO datetime to sync since")

    def handle(self, *args, **options):
        source = options.get("source")
        if not source:
            raise CommandError("--source is required")
        if source not in get_provider_sources():
            raise CommandError(f"Unknown source: {source}")
        since_raw = options.get("since")
        since = parse_datetime(since_raw) if since_raw else None
        run = sync_source(source, since=since)
        self.stdout.write(self.style.SUCCESS(f"Sync complete: {run.status}"))
