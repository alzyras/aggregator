from __future__ import annotations

from django.core.management.base import BaseCommand, CommandError

from ingestion.services.jobs import run_job


class Command(BaseCommand):
    help = "Run a single queued job by id."

    def add_arguments(self, parser):
        parser.add_argument("job_id", type=str, help="Job id to execute")

    def handle(self, *args, **options):
        job_id = options.get("job_id")
        if not job_id:
            raise CommandError("job_id is required")
        job = run_job(job_id)
        self.stdout.write(self.style.SUCCESS(f"Job {job.id} -> {job.status}"))
