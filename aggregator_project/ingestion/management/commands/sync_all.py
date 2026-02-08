from __future__ import annotations

from django.core.management.base import BaseCommand, CommandError
from ingestion.services.jobs import queue_sync_jobs
from workspaces.models import Workspace


class Command(BaseCommand):
    help = "Queue a sync job for all sources or a subset of sources."

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
        jobs = queue_sync_jobs(
            workspace=workspace,
            sources=sources or None,
            since=since_raw,
        )
        if not jobs:
            self.stdout.write(self.style.WARNING("No active connector accounts to sync."))
            return
        self.stdout.write(self.style.SUCCESS(f"Queued {len(jobs)} sync jobs."))
