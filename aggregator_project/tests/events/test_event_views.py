from __future__ import annotations

from datetime import datetime, timedelta, timezone

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from connectors.models import ConnectorAccount
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

    def _create_connector(self, source: str, display_name: str) -> ConnectorAccount:
        return ConnectorAccount.objects.create(
            workspace=self.workspace,
            source=source,
            display_name=display_name,
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            encrypted_access_token=b"token",
            status=ConnectorAccount.STATUS_CONNECTED,
            is_active=True,
        )

    def _create_event(
        self,
        *,
        source: str,
        connector_account: ConnectorAccount | None,
        source_entity_id: str,
        event_type: str,
        external_status: str | None = None,
    ) -> Event:
        return Event.objects.create(
            workspace=self.workspace,
            connector_account=connector_account,
            source=source,
            source_entity_type="task",
            source_entity_id=source_entity_id,
            event_type=event_type,
            external_status=external_status,
            title=f"Task {source_entity_id}",
            start_time=datetime.now(tz=timezone.utc),
            raw={},
            source_event_version=f"v-{source_entity_id}",
            dedupe_hash=f"hash-{source_entity_id}",
        )

    def test_state_pills_support_multi_select_or_filter(self):
        connector = self._create_connector(SOURCE_ASANA, "Work")
        self._create_event(
            source=SOURCE_ASANA,
            connector_account=connector,
            source_entity_id="1",
            event_type="task_updated",
            external_status="open",
        )
        self._create_event(
            source=SOURCE_ASANA,
            connector_account=connector,
            source_entity_id="2",
            event_type="task_completed",
            external_status="completed",
        )
        self._create_event(
            source=SOURCE_ASANA,
            connector_account=connector,
            source_entity_id="3",
            event_type="task_updated",
            external_status="deleted",
        )

        response = self.client.get(
            reverse("event_list"),
            data=[("external_status", "open"), ("external_status", "completed")],
        )

        self.assertEqual(response.status_code, 200)
        page_obj = response.context["page_obj"]
        self.assertEqual(page_obj.paginator.count, 2)
        event_types = {item.event_type for item in page_obj.object_list}
        self.assertEqual(event_types, {"task_updated", "task_completed"})

    def test_completed_state_filter_uses_completed_event_type(self):
        connector = self._create_connector(SOURCE_ASANA, "Work")
        completed_event = self._create_event(
            source=SOURCE_ASANA,
            connector_account=connector,
            source_entity_id="1",
            event_type="task_completed",
            external_status="completed",
        )
        self._create_event(
            source=SOURCE_ASANA,
            connector_account=connector,
            source_entity_id="2",
            event_type="task_state",
            external_status="completed",
        )

        response = self.client.get(reverse("event_list"), data={"external_status": "completed"})

        self.assertEqual(response.status_code, 200)
        page_obj = response.context["page_obj"]
        self.assertEqual(page_obj.paginator.count, 1)
        self.assertEqual(page_obj.object_list[0].id, completed_event.id)

    def test_event_type_filter_options_are_canonical_lifecycle_values(self):
        connector = self._create_connector(SOURCE_ASANA, "Work")
        self._create_event(
            source=SOURCE_ASANA,
            connector_account=connector,
            source_entity_id="1",
            event_type="daily_completed",
            external_status="completed",
        )
        self._create_event(
            source=SOURCE_ASANA,
            connector_account=connector,
            source_entity_id="2",
            event_type="task_completed",
            external_status="completed",
        )

        response = self.client.get(reverse("event_list"))

        self.assertEqual(response.status_code, 200)
        lifecycle_group = response.context["event_type_groups"][0][1]
        values = [value for value, _label in lifecycle_group]
        self.assertEqual(values, ["completed"])

    def test_connector_pills_include_all_workspace_connectors(self):
        self._create_connector(SOURCE_ASANA, "Personal")
        self._create_connector(SOURCE_ASANA, "Work")
        self._create_connector(SOURCE_TODOIST, "Main")

        response = self.client.get(reverse("event_list"))

        self.assertEqual(response.status_code, 200)
        labels = [pill["label"] for pill in response.context["connector_pills"]]
        self.assertIn("All", labels)
        self.assertIn("Asana · Personal", labels)
        self.assertIn("Asana · Work", labels)
        self.assertIn("Todoist · Main", labels)

    def test_combined_connector_and_state_filters_use_and_semantics(self):
        connector_a = self._create_connector(SOURCE_ASANA, "Personal")
        connector_b = self._create_connector(SOURCE_ASANA, "Work")
        self._create_event(
            source=SOURCE_ASANA,
            connector_account=connector_a,
            source_entity_id="1",
            event_type="task_updated",
            external_status="open",
        )
        self._create_event(
            source=SOURCE_ASANA,
            connector_account=connector_b,
            source_entity_id="2",
            event_type="task_updated",
            external_status="open",
        )
        self._create_event(
            source=SOURCE_ASANA,
            connector_account=connector_a,
            source_entity_id="3",
            event_type="task_updated",
            external_status="completed",
        )

        response = self.client.get(
            reverse("event_list"),
            data=[("external_status", "open"), ("connector_account", str(connector_a.id))],
        )

        self.assertEqual(response.status_code, 200)
        page_obj = response.context["page_obj"]
        self.assertEqual(page_obj.paginator.count, 1)
        self.assertEqual(page_obj.object_list[0].connector_account_id, connector_a.id)
        self.assertEqual(page_obj.object_list[0].external_status, "open")

    def test_pagination_preserves_filters(self):
        connector = self._create_connector(SOURCE_ASANA, "Personal")
        now = datetime.now(tz=timezone.utc)
        for index in range(30):
            Event.objects.create(
                workspace=self.workspace,
                connector_account=connector,
                source=SOURCE_ASANA,
                source_entity_type="task",
                source_entity_id=f"id-{index}",
                event_type="task_updated",
                title=f"Event {index}",
                start_time=now - timedelta(minutes=index),
                raw={},
                source_event_version=f"v-{index}",
                dedupe_hash=f"hash-page-{index}",
            )

        response = self.client.get(reverse("event_list"), data={"source": SOURCE_ASANA, "page": 2})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["pagination_query"], f"source={SOURCE_ASANA}")
        self.assertContains(
            response,
            f"?source={SOURCE_ASANA}&amp;page=1",
        )

    def test_reset_clears_all_filters(self):
        connector = self._create_connector(SOURCE_ASANA, "Personal")
        self._create_event(
            source=SOURCE_ASANA,
            connector_account=connector,
            source_entity_id="1",
            event_type="task_updated",
            external_status="open",
        )

        response = self.client.get(
            reverse("event_list"),
            data=[("external_status", "open"), ("connector_account", str(connector.id))],
        )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'class="btn ghost" href="?"')
        self.assertContains(response, "Reset all")

    def test_invalid_connector_ids_are_ignored(self):
        connector = self._create_connector(SOURCE_ASANA, "Personal")
        self._create_event(
            source=SOURCE_ASANA,
            connector_account=connector,
            source_entity_id="1",
            event_type="task_updated",
            external_status="open",
        )

        response = self.client.get(
            reverse("event_list"),
            data=[("connector_account", "999999"), ("connector_account", "invalid-id")],
        )

        self.assertEqual(response.status_code, 200)
        page_obj = response.context["page_obj"]
        self.assertEqual(page_obj.paginator.count, 1)
        self.assertEqual(response.context["filters"]["connector_account"], [])

    def test_event_detail_for_workspace(self):
        connector = self._create_connector(SOURCE_ASANA, "Personal")
        event = Event.objects.create(
            workspace=self.workspace,
            connector_account=connector,
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
