from __future__ import annotations

from django.core.management.base import BaseCommand, CommandError
from django.utils.dateparse import parse_datetime

from ingestion.services.sync import sync_all_sources
from workspaces.models import Workspace


class Command(BaseCommand):
    help = "Sync all sources or a subset of sources."

    def add_arguments(self, parser):
        parser.add_argument(
            "--workspace-id",
            required=True,
            type=int,
            help="Workspace id to sync",
        )
        parser.add_argument("--since", type=str, help="ISO datetime to sync since")
        parser.add_argument(
            "--source",
            action="append",
            dest="sources",
            help="Source to sync (can be specified multiple times)",
        )

    def handle(self, *args, **options):
        workspace_id = options.get("workspace_id")
        workspace = Workspace.objects.filter(id=workspace_id).first()
        if not workspace:
            raise CommandError(f"Unknown workspace id: {workspace_id}")
        since_raw = options.get("since")
        sources = options.get("sources")
        since = parse_datetime(since_raw) if since_raw else None
        runs = sync_all_sources(workspace, since=since, sources=sources)
        self.stdout.write(self.style.SUCCESS(f"Sync complete. Runs: {len(runs)}"))
