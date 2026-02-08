from __future__ import annotations

from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from ingestion.models import Job
from ingestion.services.jobs import enqueue_job
from ingestion.providers import get_provider_sources
from workspaces.models import Workspace


class Command(BaseCommand):
    help = "Queue a sync job for a single source."

    def add_arguments(self, parser):
        parser.add_argument("--source", required=True, type=str, help="Source to sync")
        parser.add_argument(
            "--workspace-id",
            required=True,
            type=int,
            help="Workspace id to sync",
        )
        parser.add_argument("--since", type=str, help="ISO datetime to sync since")

    def handle(self, *args, **options):
        source = options.get("source")
        workspace_id = options.get("workspace_id")
        if not source:
            raise CommandError("--source is required")
        workspace = Workspace.objects.filter(id=workspace_id).first()
        if not workspace:
            raise CommandError(f"Unknown workspace id: {workspace_id}")
        if source not in get_provider_sources():
            raise CommandError(f"Unknown source: {source}")
        since_raw = options.get("since")
        job = Job.objects.create(
            workspace=workspace,
            job_type="sync",
            job_name="sync_source",
            input_params={"source": source, "since": since_raw},
            next_run_at=timezone.now(),
        )
        enqueue_job(job.id)
        self.stdout.write(self.style.SUCCESS(f"Job queued: {job.id}"))
