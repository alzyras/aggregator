from __future__ import annotations

from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from ingestion.models import Job
from ingestion.services.jobs import enqueue_job
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
        job = Job.objects.create(
            workspace=workspace,
            job_type="sync",
            job_name="sync_all",
            input_params={"since": since_raw, "sources": sources or []},
            next_run_at=timezone.now(),
        )
        enqueue_job(job.id)
        self.stdout.write(self.style.SUCCESS(f"Job queued: {job.id}"))
