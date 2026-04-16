from __future__ import annotations

from django.core.management.base import BaseCommand

from planner.models import PlannerStatusIntent
from planner.services.writeback import retry_failed_status_writeback
from workspaces.models import Workspace


class Command(BaseCommand):
    help = "Queue retries for failed planner status writebacks."

    def add_arguments(self, parser):
        parser.add_argument("--workspace-id", type=int, required=True)
        parser.add_argument("--source", type=str, default="")
        parser.add_argument("--limit", type=int, default=100)
        parser.add_argument("--dry-run", action="store_true")

    def handle(self, *args, **options):
        workspace = Workspace.objects.get(id=options["workspace_id"])
        intents = (
            PlannerStatusIntent.objects
            .select_related("state", "item")
            .filter(workspace=workspace, status=PlannerStatusIntent.STATUS_FAILED)
            .order_by("-requested_at")
        )
        if options["source"]:
            intents = intents.filter(item__source=options["source"])
        intents = list(intents[: max(int(options["limit"]), 1)])
        if options["dry_run"]:
            self.stdout.write(f"Would retry {len(intents)} failed writebacks.")
            return
        retried = 0
        for intent in intents:
            if not intent.state:
                continue
            retry_failed_status_writeback(state=intent.state)
            retried += 1
        self.stdout.write(self.style.SUCCESS(f"Queued {retried} failed writeback retries."))
