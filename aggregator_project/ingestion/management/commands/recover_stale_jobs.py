from __future__ import annotations

from django.core.management.base import BaseCommand

from ingestion.services.jobs import recover_stale_jobs


class Command(BaseCommand):
    help = "Recover expired running jobs back to queued."

    def add_arguments(self, parser):
        parser.add_argument("--dry-run", action="store_true")

    def handle(self, *args, **options):
        if options["dry_run"]:
            self.stdout.write("Dry run only. Run without --dry-run to recover stale jobs.")
            return
        recovered = recover_stale_jobs()
        self.stdout.write(self.style.SUCCESS(f"Recovered {recovered} stale jobs."))
