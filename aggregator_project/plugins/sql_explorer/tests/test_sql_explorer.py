from __future__ import annotations

import json

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from events.models import Event
from plugin_system.models import PluginActivation
from plugins.sql_explorer.query_engine import QueryRejected, execute_workspace_query
from workspaces.models import Workspace, WorkspaceMember


class SqlExplorerTests(TestCase):
    def setUp(self) -> None:
        user_model = get_user_model()
        self.user = user_model.objects.create_user(
            username="sql-user",
            email="sql@example.com",
            password="password123",
        )
        self.workspace = Workspace.objects.create(name="SQL workspace")
        WorkspaceMember.objects.create(
            workspace=self.workspace,
            user=self.user,
            role=WorkspaceMember.ROLE_OWNER,
        )
        self.other_workspace = Workspace.objects.create(name="Other workspace")
        self.client.force_login(self.user)

    def _enable(self) -> None:
        PluginActivation.objects.create(
            workspace=self.workspace,
            plugin_id="sql-explorer",
            enabled=True,
        )

    def _event(self, workspace: Workspace, title: str, dedupe_hash: str) -> None:
        Event.objects.create(
            workspace=workspace,
            source="asana",
            source_entity_type="task",
            source_entity_id=dedupe_hash,
            event_type="task_created",
            title=title,
            raw={},
            dedupe_hash=dedupe_hash,
        )

    def test_disabled_plugin_redirects_to_catalog(self):
        response = self.client.get(reverse("sql_explorer:index"))

        self.assertRedirects(response, reverse("plugin_system:catalog"))

    def test_query_only_contains_current_workspace_data(self):
        self._event(self.workspace, "Visible", "visible")
        self._event(self.other_workspace, "Private", "private")

        result = execute_workspace_query(
            self.workspace, "SELECT title FROM events ORDER BY title"
        )

        self.assertEqual(result.rows, [["Visible"]])

    def test_mutating_query_is_rejected(self):
        with self.assertRaises(QueryRejected):
            execute_workspace_query(self.workspace, "DELETE FROM events")

    def test_query_endpoint_returns_columns_and_rows(self):
        self._enable()
        self._event(self.workspace, "Visible", "visible")
        response = self.client.post(
            reverse("sql_explorer:query"),
            data=json.dumps({"query": "SELECT title FROM events"}),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["columns"], ["title"])
        self.assertEqual(response.json()["rows"], [["Visible"]])

    def test_enabled_plugin_renders_shared_freshness_controls(self):
        self._enable()

        response = self.client.get(reverse("sql_explorer:index"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "data-workspace-refresh")
        self.assertContains(response, "Refresh now")
