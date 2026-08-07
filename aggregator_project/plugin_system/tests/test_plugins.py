from __future__ import annotations

import json

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from plugin_system.models import PluginActivation
from workspaces.models import Workspace, WorkspaceMember


class PluginCatalogTests(TestCase):
    def setUp(self) -> None:
        user_model = get_user_model()
        self.user = user_model.objects.create_user(
            username="plugin-user",
            email="plugin@example.com",
            password="password123",
        )
        self.workspace = Workspace.objects.create(name="Plugin workspace")
        WorkspaceMember.objects.create(
            workspace=self.workspace,
            user=self.user,
            role=WorkspaceMember.ROLE_OWNER,
        )
        self.client.force_login(self.user)

    def test_catalog_lists_installed_plugins(self):
        response = self.client.get(reverse("plugin_system:catalog"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "SQL Explorer")

    def test_toggle_is_workspace_scoped(self):
        other_workspace = Workspace.objects.create(name="Other")
        response = self.client.post(
            reverse("plugin_system:toggle", args=["sql-explorer"]),
            data=json.dumps({"enabled": True}),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 200)
        self.assertTrue(
            PluginActivation.objects.get(
                workspace=self.workspace,
                plugin_id="sql-explorer",
            ).enabled
        )
        self.assertFalse(
            PluginActivation.objects.filter(
                workspace=other_workspace,
                plugin_id="sql-explorer",
            ).exists()
        )

    def test_toggle_rejects_non_boolean_value(self):
        response = self.client.post(
            reverse("plugin_system:toggle", args=["sql-explorer"]),
            data=json.dumps({"enabled": "yes"}),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 400)
