from __future__ import annotations

from datetime import datetime, timedelta, timezone

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from core.constants import SOURCE_ASANA, SOURCE_TODOIST
from events.models import Event
from workspaces.models import Workspace, WorkspaceMember


class EventViewsTests(TestCase):
    def setUp(self) -> None:
        self.user = self._create_user("tester")
        self.client.force_login(self.user)
        self.workspace = Workspace.objects.create(name="Workspace")
        WorkspaceMember.objects.create(
            workspace=self.workspace,
            user=self.user,
            role=WorkspaceMember.ROLE_OWNER,
        )

    def _create_user(self, username: str):
        user_model = get_user_model()
        return user_model.objects.create_user(
            username=username,
            email=f"{username}@example.com",
            password="password123",
        )

    def test_event_list_filters(self):
        now = datetime.now(tz=timezone.utc)
        Event.objects.create(
            workspace=self.workspace,
            source=SOURCE_ASANA,
            source_entity_type="task",
            source_entity_id="1",
            event_type="task_updated",
            title="A",
            start_time=now - timedelta(days=2),
            raw={},
            source_event_version="v1",
            dedupe_hash="hash-a",
        )
        Event.objects.create(
            workspace=self.workspace,
            source=SOURCE_TODOIST,
            source_entity_type="task",
            source_entity_id="2",
            event_type="task_updated",
            title="B",
            start_time=now - timedelta(days=1),
            raw={},
            source_event_version="v1",
            dedupe_hash="hash-b",
        )

        response = self.client.get(
            reverse("event_list"),
            data={
                "source": SOURCE_TODOIST,
                "type": "task",
                "start": (now - timedelta(days=2)).date().isoformat(),
                "end": (now - timedelta(days=1)).date().isoformat(),
            },
        )

        self.assertEqual(response.status_code, 200)
        page_obj = response.context["page_obj"]
        self.assertEqual(page_obj.paginator.count, 1)
        self.assertEqual(page_obj.object_list[0].source, SOURCE_TODOIST)

    def test_event_detail_for_workspace(self):
        event = Event.objects.create(
            workspace=self.workspace,
            source=SOURCE_ASANA,
            source_entity_type="task",
            source_entity_id="1",
            event_type="task_updated",
            title="Detail",
            raw={},
            source_event_version="v1",
            dedupe_hash="hash-detail",
        )

        response = self.client.get(reverse("event_detail", args=[event.id]))
        self.assertEqual(response.status_code, 200)
