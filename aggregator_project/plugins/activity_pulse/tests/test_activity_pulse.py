from __future__ import annotations

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from planner.models import PlannerItem, PlannerItemState, PlannerPlan
from plugin_system.models import PluginActivation
from plugins.activity_pulse.services import build_activity_snapshot
from workspaces.models import Workspace, WorkspaceMember


class ActivityPulseTests(TestCase):
    def setUp(self) -> None:
        self.user = get_user_model().objects.create_user(
            username="activity-user",
            password="password123",
        )
        self.workspace = Workspace.objects.create(name="Activity workspace")
        WorkspaceMember.objects.create(
            workspace=self.workspace,
            user=self.user,
            role=WorkspaceMember.ROLE_OWNER,
        )
        self.other_workspace = Workspace.objects.create(name="Other")
        self.client.force_login(self.user)

    def test_disabled_plugin_redirects_to_catalog(self):
        response = self.client.get(reverse("activity_pulse:index"))

        self.assertRedirects(response, reverse("plugin_system:catalog"))

    def test_snapshot_counts_only_current_workspace(self):
        plan = PlannerPlan.objects.create(
            workspace=self.workspace,
            user=self.user,
            name="My tasks",
        )
        item = PlannerItem.objects.create(
            workspace=self.workspace,
            source="todoist",
            source_entity_id="visible",
            title="Visible",
        )
        PlannerItemState.objects.create(
            plan=plan,
            item=item,
            planner_status=PlannerItemState.PLANNER_STATUS_DOING,
        )
        PlannerItem.objects.create(
            workspace=self.other_workspace,
            source="jira",
            source_entity_id="private",
            title="Private",
        )

        result = build_activity_snapshot(workspace=self.workspace, user=self.user)

        self.assertEqual(result["total"], 1)
        self.assertEqual(result["status_counts"]["doing"], 1)
        self.assertEqual(result["providers"][0]["source"], "todoist")

    def test_enabled_plugin_renders_dashboard(self):
        PluginActivation.objects.create(
            workspace=self.workspace,
            plugin_id="activity-pulse",
            enabled=True,
        )

        response = self.client.get(reverse("activity_pulse:index"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Activity Pulse")
