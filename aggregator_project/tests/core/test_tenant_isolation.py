from __future__ import annotations

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from core.constants import SOURCE_ASANA
from events.models import Event
from workspaces.models import Workspace, WorkspaceMember


class TenantIsolationTests(TestCase):
    def setUp(self) -> None:
        self.user_a = self._create_user("user_a")
        self.user_b = self._create_user("user_b")

        self.workspace_a = Workspace.objects.create(name="Workspace A")
        WorkspaceMember.objects.create(
            workspace=self.workspace_a,
            user=self.user_a,
            role=WorkspaceMember.ROLE_OWNER,
        )

        self.workspace_b = Workspace.objects.create(name="Workspace B")
        WorkspaceMember.objects.create(
            workspace=self.workspace_b,
            user=self.user_b,
            role=WorkspaceMember.ROLE_OWNER,
        )

    def test_cross_workspace_isolation_by_id(self):
        event = Event.objects.create(
            workspace=self.workspace_a,
            source=SOURCE_ASANA,
            source_entity_type="task",
            source_entity_id="123",
            event_type="task_updated",
            title="Private",
            raw={},
            source_event_version="v1",
            dedupe_hash="hash-a",
        )

        self.client.force_login(self.user_b)
        response = self.client.get(reverse("event_detail", args=[event.id]))
        self.assertEqual(response.status_code, 404)

    def test_shared_workspace_access(self):
        user_c = self._create_user("user_c")
        WorkspaceMember.objects.create(
            workspace=self.workspace_a,
            user=user_c,
            role=WorkspaceMember.ROLE_MEMBER,
        )
        event = Event.objects.create(
            workspace=self.workspace_a,
            source=SOURCE_ASANA,
            source_entity_type="task",
            source_entity_id="456",
            event_type="task_updated",
            title="Shared",
            raw={},
            source_event_version="v1",
            dedupe_hash="hash-b",
        )

        self.client.force_login(user_c)
        response = self.client.get(reverse("event_detail", args=[event.id]))
        self.assertEqual(response.status_code, 200)

    def test_middleware_auto_creates_workspace(self):
        user_d = self._create_user("user_d")
        self.assertFalse(WorkspaceMember.objects.filter(user=user_d).exists())

        self.client.force_login(user_d)
        response = self.client.get(reverse("plugins_view"))
        self.assertEqual(response.status_code, 200)
        self.assertTrue(WorkspaceMember.objects.filter(user=user_d).exists())

        self.assertEqual(WorkspaceMember.objects.filter(user=self.user_b).count(), 1)

    def test_dedupe_isolated_by_workspace(self):
        Event.objects.create(
            workspace=self.workspace_a,
            source=SOURCE_ASANA,
            source_entity_type="task",
            source_entity_id="dup-1",
            event_type="task_updated",
            title="A",
            raw={},
            source_event_version="v1",
            dedupe_hash="same-hash",
        )
        Event.objects.create(
            workspace=self.workspace_b,
            source=SOURCE_ASANA,
            source_entity_type="task",
            source_entity_id="dup-2",
            event_type="task_updated",
            title="B",
            raw={},
            source_event_version="v1",
            dedupe_hash="same-hash",
        )

        count = Event.objects.filter(source=SOURCE_ASANA, dedupe_hash="same-hash").count()
        self.assertEqual(count, 2)

    def _create_user(self, username: str):
        user_model = get_user_model()
        return user_model.objects.create_user(
            username=username,
            email=f"{username}@example.com",
            password="password123",
        )
