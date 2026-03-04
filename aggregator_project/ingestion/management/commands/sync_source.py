from __future__ import annotations

from django.core.management.base import BaseCommand, CommandError
from ingestion.services.jobs import queue_sync_jobs
from ingestion.providers import get_provider_sources
from workspaces.models import Workspace


class Command(BaseCommand):
    help = "Queue sync jobs for a source or a single connector account."

    def add_arguments(self, parser):
        parser.add_argument("--source", type=str, help="Source to sync")
        parser.add_argument(
            "--workspace-id",
            required=True,
            type=int,
            help="Workspace id to sync",
        )
        parser.add_argument(
            "--connector-account-id",
            type=int,
            help="Connector account id to sync",
        )
        parser.add_argument("--since", type=str, help="ISO datetime to sync since")

    def handle(self, *args, **options):
        source = options.get("source")
        workspace_id = options.get("workspace_id")
        connector_account_id = options.get("connector_account_id")
        if not source and not connector_account_id:
            raise CommandError("Either --source or --connector-account-id is required")
        workspace = Workspace.objects.filter(id=workspace_id).first()
        if not workspace:
            raise CommandError(f"Unknown workspace id: {workspace_id}")
        if source and source not in get_provider_sources():
            raise CommandError(f"Unknown source: {source}")
        since_raw = options.get("since")
        jobs = queue_sync_jobs(
            workspace=workspace,
            sources=[source] if source else None,
            connector_account_id=connector_account_id,
            since=since_raw,
        )
        if not jobs:
            self.stdout.write(self.style.WARNING("No active connector accounts to sync."))
            return
        self.stdout.write(self.style.SUCCESS(f"Queued {len(jobs)} sync jobs."))
