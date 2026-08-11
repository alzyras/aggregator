from __future__ import annotations

from datetime import timedelta

from django.contrib.auth import get_user_model
from django.core.cache import cache
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from planner.models import PlannerItem, PlannerItemState, PlannerPlan
from plugin_system.models import PluginActivation
from plugins.workload_radar.services import build_workload_radar
from workspaces.models import Workspace, WorkspaceMember


class WorkloadRadarTests(TestCase):
    def setUp(self) -> None:
        cache.clear()
        self.addCleanup(cache.clear)
        self.user = get_user_model().objects.create_user(
            username="workload-radar-user",
            password="password123",
        )
        self.workspace = Workspace.objects.create(name="Workload radar workspace")
        WorkspaceMember.objects.create(
            workspace=self.workspace,
            user=self.user,
            role=WorkspaceMember.ROLE_OWNER,
        )
        self.plan = PlannerPlan.objects.create(
            workspace=self.workspace,
            user=self.user,
            name="My plan",
        )
        self.client.force_login(self.user)

    def test_disabled_plugin_redirects_to_catalog(self):
        response = self.client.get(reverse("workload_radar:index"))

        self.assertRedirects(response, reverse("plugin_system:catalog"))

    def test_radar_groups_capacity_and_unplanned_work(self):
        now = timezone.now()
        today_start = timezone.localtime(now).replace(
            hour=12,
            minute=0,
            second=0,
            microsecond=0,
        )
        self._state_item("Heavy", "backlog", today_start, 420)
        self._state_item("Tomorrow", "backlog", today_start + timedelta(days=1), 30)
        self._state_item("Unplanned", "backlog", None, 15)
        PlannerItem.objects.create(
            workspace=self.workspace,
            user=None,
            source="todoist",
            source_entity_id="radar-inbox",
            title="Inbox work",
            last_synced_at=now,
        )
        other_workspace = Workspace.objects.create(name="Other radar workspace")
        PlannerItem.objects.create(
            workspace=other_workspace,
            source="asana",
            source_entity_id="private-radar",
            title="Private work",
            last_synced_at=now,
        )

        radar = build_workload_radar(
            workspace=self.workspace,
            user=self.user,
            now=now,
        )

        self.assertEqual(radar["scheduled_count"], 2)
        self.assertEqual(radar["scheduled_minutes"], 450)
        self.assertEqual(radar["unplanned_count"], 2)
        self.assertEqual(radar["unplanned_minutes"], 15)
        self.assertEqual(radar["unestimated_count"], 1)
        self.assertEqual(len(radar["overloaded_days"]), 1)
        self.assertEqual(radar["days"][0]["planned_minutes"], 420)
        self.assertEqual(radar["unplanned"][0]["title"], "Unplanned")
        self.assertEqual(radar["period_end"], timezone.localdate(now) + timedelta(days=6))

    def test_radar_limits_day_task_previews_and_keeps_hidden_count(self):
        today = timezone.localtime(timezone.now()).replace(
            hour=12,
            minute=0,
            second=0,
            microsecond=0,
        )
        for index in range(4):
            self._state_item(f"Today {index}", "backlog", today, 15)

        radar = build_workload_radar(
            workspace=self.workspace,
            user=self.user,
            now=today,
        )

        first_day = radar["days"][0]
        self.assertEqual(first_day["task_count"], 4)
        self.assertEqual(len(first_day["tasks"]), 3)
        self.assertEqual(first_day["hidden_task_count"], 1)

    def test_cached_radar_uses_a_new_local_key_after_schedule_change(self):
        item = self._state_item("Unplanned", "backlog", None, 30)
        first = build_workload_radar(workspace=self.workspace, user=self.user)
        state = PlannerItemState.objects.get(plan=self.plan, item=item)
        state.planned_start = timezone.now() + timedelta(days=1)
        state.last_planned_at = timezone.now() + timedelta(seconds=1)
        state.save(update_fields=["planned_start", "last_planned_at"])

        second = build_workload_radar(workspace=self.workspace, user=self.user)

        self.assertEqual(first["scheduled_count"], 0)
        self.assertEqual(second["scheduled_count"], 1)

    def test_enabled_plugin_renders_radar(self):
        PluginActivation.objects.create(
            workspace=self.workspace,
            plugin_id="workload-radar",
            enabled=True,
        )
        self._state_item("Heavy", "backlog", timezone.now(), 420)

        response = self.client.get(reverse("workload_radar:index"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Workload Radar")
        self.assertContains(response, "Heavy")

    def _state_item(
        self,
        title: str,
        planner_status: str,
        planned_start,
        estimated_minutes: int | None,
    ) -> PlannerItem:
        item = PlannerItem.objects.create(
            workspace=self.workspace,
            user=self.user,
            source="manual",
            source_entity_id=f"radar-{title.lower()}",
            title=title,
        )
        PlannerItemState.objects.create(
            plan=self.plan,
            item=item,
            planner_status=planner_status,
            planned_start=planned_start,
            estimated_minutes=estimated_minutes,
        )
        return item
