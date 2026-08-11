from __future__ import annotations

from datetime import timedelta

from django.contrib.auth import get_user_model
from django.core.cache import cache
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from planner.models import PlannerItem, PlannerItemState, PlannerPlan
from plugin_system.models import PluginActivation
from plugins.daily_brief.services import build_daily_brief
from workspaces.models import Workspace, WorkspaceMember


class DailyBriefTests(TestCase):
    def setUp(self) -> None:
        cache.clear()
        self.addCleanup(cache.clear)
        self.user = get_user_model().objects.create_user(
            username="daily-brief-user",
            password="password123",
        )
        self.workspace = Workspace.objects.create(name="Daily brief workspace")
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
        response = self.client.get(reverse("daily_brief:index"))

        self.assertRedirects(response, reverse("plugin_system:catalog"))

    def test_brief_groups_current_workspace_tasks(self):
        now = timezone.now()
        today_start = timezone.localtime(now).replace(
            hour=12,
            minute=0,
            second=0,
            microsecond=0,
        )
        self._state_item("Focus", "doing")
        self._state_item(
            "Today",
            "backlog",
            planned_start=today_start,
            estimated_minutes=30,
        )
        self._state_item(
            "Upcoming",
            "backlog",
            planned_start=now + timedelta(days=2),
        )
        PlannerItem.objects.create(
            workspace=self.workspace,
            user=None,
            source="todoist",
            source_entity_id="inbox-task",
            title="Inbox",
            last_synced_at=now,
        )
        other_workspace = Workspace.objects.create(name="Other daily brief workspace")
        PlannerItem.objects.create(
            workspace=other_workspace,
            source="asana",
            source_entity_id="private-task",
            title="Private",
            last_synced_at=now,
        )

        brief = build_daily_brief(
            workspace=self.workspace,
            user=self.user,
            now=now,
        )

        self.assertEqual(brief["focus_count"], 1)
        self.assertEqual(brief["today_count"], 1)
        self.assertEqual(brief["upcoming_count"], 1)
        self.assertEqual(brief["inbox_count"], 1)
        self.assertEqual(brief["focus"][0]["title"], "Focus")
        self.assertEqual(brief["triage"][0]["title"], "Inbox")

    def test_cached_brief_uses_a_new_local_key_after_planning_changes(self):
        item = self._state_item("Focus", "doing")
        first = build_daily_brief(workspace=self.workspace, user=self.user)
        state = PlannerItemState.objects.get(plan=self.plan, item=item)
        state.planner_status = PlannerItemState.PLANNER_STATUS_DONE
        state.last_planned_at = timezone.now() + timedelta(seconds=1)
        state.save(update_fields=["planner_status", "last_planned_at"])

        second = build_daily_brief(workspace=self.workspace, user=self.user)

        self.assertEqual(first["focus_count"], 1)
        self.assertEqual(second["focus_count"], 0)

    def test_enabled_plugin_renders_current_brief(self):
        PluginActivation.objects.create(
            workspace=self.workspace,
            plugin_id="daily-brief",
            enabled=True,
        )
        self._state_item("Focus", "doing")

        response = self.client.get(reverse("daily_brief:index"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Daily Brief")
        self.assertContains(response, "Focus")

    def _state_item(
        self,
        title: str,
        planner_status: str,
        *,
        planned_start=None,
        estimated_minutes: int | None = None,
    ) -> PlannerItem:
        item = PlannerItem.objects.create(
            workspace=self.workspace,
            user=self.user,
            source="manual",
            source_entity_id=f"daily-{title.lower()}",
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
