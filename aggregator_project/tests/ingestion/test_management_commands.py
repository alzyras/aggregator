from __future__ import annotations

from unittest.mock import patch

from django.core.management import call_command, CommandError
from django.test import TestCase

from workspaces.models import Workspace


class SyncSourceCommandTests(TestCase):
    def test_sync_source_unknown_workspace(self):
        with self.assertRaises(CommandError):
            call_command("sync_source", source="asana", workspace_id=999)

    def test_sync_source_unknown_source(self):
        workspace = Workspace.objects.create(name="Workspace")

        with self.assertRaises(CommandError):
            call_command(
                "sync_source", source="unknown", workspace_id=workspace.id
            )

    def test_sync_source_success(self):
        workspace = Workspace.objects.create(name="Workspace")

        with patch(
            "ingestion.management.commands.sync_source.queue_sync_jobs",
            return_value=[object()],
        ) as mock_queue:
            call_command("sync_source", source="asana", workspace_id=workspace.id)

        mock_queue.assert_called_once()


class SyncAllCommandTests(TestCase):
    def test_sync_all_unknown_workspace(self):
        with self.assertRaises(CommandError):
            call_command("sync_all", workspace_id=123)

    def test_sync_all_passes_sources_and_since(self):
        workspace = Workspace.objects.create(name="Workspace")

        with patch(
            "ingestion.management.commands.sync_all.queue_sync_jobs",
            return_value=[object()],
        ) as mock_queue:
            call_command(
                "sync_all",
                workspace_id=workspace.id,
                source=["asana", "todoist"],
                since="2025-01-01T00:00:00Z",
            )

        _args, kwargs = mock_queue.call_args
        self.assertEqual(kwargs["sources"], ["asana", "todoist"])
        self.assertEqual(kwargs["since"], "2025-01-01T00:00:00Z")
