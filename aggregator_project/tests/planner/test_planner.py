from __future__ import annotations

import json

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from connectors.models import ConnectorAccount
from events.models import Event
from planner.models import PlannerItem, PlannerItemState, PlannerPlan
from planner.services.reconcile import reconcile_from_event
from workspaces.models import Workspace, WorkspaceMember


class PlannerTests(TestCase):
    def setUp(self) -> None:
        self.user = self._create_user("planner_user")
        self.workspace = Workspace.objects.create(name="Planner Workspace")
        WorkspaceMember.objects.create(
            workspace=self.workspace,
            user=self.user,
            role=WorkspaceMember.ROLE_OWNER,
        )
        self.client.force_login(self.user)

        self.account = ConnectorAccount.objects.create(
            workspace=self.workspace,
            source="asana",
            display_name="Main",
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            encrypted_access_token=b"token",
            status=ConnectorAccount.STATUS_CONNECTED,
        )

    def test_reconcile_creates_planner_item(self):
        event = Event.objects.create(
            workspace=self.workspace,
            connector_account=self.account,
            source="asana",
            source_entity_type="task",
            source_entity_id="task-1",
            event_type="task_state",
            title="Test Task",
            external_status="open",
            raw={},
            dedupe_hash="hash-1",
        )
        result = reconcile_from_event(event)
        self.assertTrue(result.created)
        self.assertIsNotNone(result.item)
        self.assertEqual(result.item.source_status, "open")
        self.assertFalse(result.item.external_completed)

    def test_reconcile_updates_existing_item(self):
        item = PlannerItem.objects.create(
            workspace=self.workspace,
            user=self.user,
            connector_account=self.account,
            source="asana",
            source_entity_id="task-2",
            title="Old",
        )
        event = Event.objects.create(
            workspace=self.workspace,
            connector_account=self.account,
            source="asana",
            source_entity_type="task",
            source_entity_id="task-2",
            event_type="task_completed",
            title="New",
            external_status="completed",
            raw={},
            dedupe_hash="hash-2",
        )
        reconcile_from_event(event)
        item.refresh_from_db()
        self.assertEqual(item.title, "New")
        self.assertTrue(item.external_completed)

    def test_status_endpoint_isolated(self):
        plan = PlannerPlan.objects.create(
            workspace=self.workspace,
            user=self.user,
            name="My Plan",
            timezone="UTC",
        )
        item = PlannerItem.objects.create(
            workspace=self.workspace,
            user=self.user,
            connector_account=self.account,
            source="asana",
            source_entity_id="task-3",
            title="Task",
        )
        state = PlannerItemState.objects.create(plan=plan, item=item)

        url = reverse("planner_item_status", args=[item.id])
        response = self.client.post(url, data="{\"planned_status\": \"done\"}", content_type="application/json")
        self.assertEqual(response.status_code, 200)
        state.refresh_from_db()
        self.assertEqual(state.planned_status, PlannerItemState.STATUS_DONE)

    def test_planner_status_defaults_to_inbox(self):
        plan = PlannerPlan.objects.create(
            workspace=self.workspace,
            user=self.user,
            name="My Plan",
            timezone="UTC",
        )
        item = PlannerItem.objects.create(
            workspace=self.workspace,
            user=self.user,
            connector_account=self.account,
            source="asana",
            source_entity_id="task-inbox",
            title="Inbox Task",
        )
        state = PlannerItemState.objects.create(plan=plan, item=item)
        self.assertEqual(state.planner_status, PlannerItemState.PLANNER_STATUS_INBOX)

    def test_planner_status_endpoint_updates(self):
        plan = PlannerPlan.objects.create(
            workspace=self.workspace,
            user=self.user,
            name="My Plan",
            timezone="UTC",
        )
        item = PlannerItem.objects.create(
            workspace=self.workspace,
            user=self.user,
            connector_account=self.account,
            source="asana",
            source_entity_id="task-status",
            title="Status Task",
        )
        state = PlannerItemState.objects.create(plan=plan, item=item)
        response = self.client.post(
            reverse("planner_item_planner_status", args=[item.id]),
            data=json.dumps({"planner_status": PlannerItemState.PLANNER_STATUS_DOING}),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 200)
        state.refresh_from_db()
        self.assertEqual(state.planner_status, PlannerItemState.PLANNER_STATUS_DOING)

    def test_status_endpoint_denies_other_workspace(self):
        other_user = self._create_user("other_user")
        other_workspace = Workspace.objects.create(name="Other Workspace")
        WorkspaceMember.objects.create(
            workspace=other_workspace,
            user=other_user,
            role=WorkspaceMember.ROLE_OWNER,
        )
        other_account = ConnectorAccount.objects.create(
            workspace=other_workspace,
            source="asana",
            display_name="Other",
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            encrypted_access_token=b"token",
            status=ConnectorAccount.STATUS_CONNECTED,
        )
        other_plan = PlannerPlan.objects.create(
            workspace=other_workspace,
            user=other_user,
            name="Other Plan",
            timezone="UTC",
        )
        other_item = PlannerItem.objects.create(
            workspace=other_workspace,
            user=other_user,
            connector_account=other_account,
            source="asana",
            source_entity_id="task-other",
            title="Other Task",
        )
        PlannerItemState.objects.create(plan=other_plan, item=other_item)

        url = reverse("planner_item_status", args=[other_item.id])
        response = self.client.post(url, data="{\"planned_status\": \"done\"}", content_type="application/json")
        self.assertEqual(response.status_code, 404)

    def test_reorder_updates_order(self):
        plan = PlannerPlan.objects.create(
            workspace=self.workspace,
            user=self.user,
            name="My Plan",
            timezone="UTC",
        )
        items = []
        for index in range(3):
            item = PlannerItem.objects.create(
                workspace=self.workspace,
                user=self.user,
                connector_account=self.account,
                source="asana",
                source_entity_id=f"task-{index}",
                title=f"Task {index}",
            )
            state = PlannerItemState.objects.create(plan=plan, item=item, planned_order=index + 1)
            items.append(state)

        order = [str(items[2].id), str(items[0].id), str(items[1].id)]
        response = self.client.post(
            reverse("planner_item_reorder"),
            data=json.dumps({"moved_id": items[2].id, "before_id": items[0].id}),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 200)
        items[2].refresh_from_db()
        self.assertEqual(items[2].planned_order, 1)

    def test_reorder_delta_updates_order(self):
        plan = PlannerPlan.objects.create(
            workspace=self.workspace,
            user=self.user,
            name="My Plan",
            timezone="UTC",
        )
        items = []
        for index in range(3):
            item = PlannerItem.objects.create(
                workspace=self.workspace,
                user=self.user,
                connector_account=self.account,
                source="asana",
                source_entity_id=f"task-delta-{index}",
                title=f"Task {index}",
            )
            state = PlannerItemState.objects.create(plan=plan, item=item, planned_order=index + 1)
            items.append(state)

        payload = {"moved_id": items[2].id, "before_id": items[0].id}
        response = self.client.post(
            reverse("planner_item_reorder"),
            data=json.dumps(payload),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 200)
        items[2].refresh_from_db()
        self.assertEqual(items[2].planned_order, 1)

    def test_reorder_respects_pinned_block(self):
        plan = PlannerPlan.objects.create(
            workspace=self.workspace,
            user=self.user,
            name="My Plan",
            timezone="UTC",
        )
        pinned_item = PlannerItem.objects.create(
            workspace=self.workspace,
            user=self.user,
            connector_account=self.account,
            source="asana",
            source_entity_id="task-pin",
            title="Pinned",
        )
        unpinned_item = PlannerItem.objects.create(
            workspace=self.workspace,
            user=self.user,
            connector_account=self.account,
            source="asana",
            source_entity_id="task-unpin",
            title="Unpinned",
        )
        pinned_item_two = PlannerItem.objects.create(
            workspace=self.workspace,
            user=self.user,
            connector_account=self.account,
            source="asana",
            source_entity_id="task-pin-two",
            title="Pinned Two",
        )
        pinned_state = PlannerItemState.objects.create(plan=plan, item=pinned_item, pinned=True, planned_order=1)
        pinned_state_two = PlannerItemState.objects.create(plan=plan, item=pinned_item_two, pinned=True, planned_order=2)
        unpinned_state = PlannerItemState.objects.create(plan=plan, item=unpinned_item, pinned=False, planned_order=3)

        response = self.client.post(
            reverse("planner_item_reorder"),
            data=json.dumps({"moved_id": pinned_state_two.id, "before_id": pinned_state.id}),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 200)
        pinned_state.refresh_from_db()
        pinned_state_two.refresh_from_db()
        unpinned_state.refresh_from_db()
        self.assertLess(pinned_state.planned_order, unpinned_state.planned_order)
        self.assertLess(pinned_state_two.planned_order, unpinned_state.planned_order)

    def test_reorder_rejects_cross_pinned(self):
        plan = PlannerPlan.objects.create(
            workspace=self.workspace,
            user=self.user,
            name="My Plan",
            timezone="UTC",
        )
        pinned_item = PlannerItem.objects.create(
            workspace=self.workspace,
            user=self.user,
            connector_account=self.account,
            source="asana",
            source_entity_id="task-pin-cross",
            title="Pinned",
        )
        unpinned_item = PlannerItem.objects.create(
            workspace=self.workspace,
            user=self.user,
            connector_account=self.account,
            source="asana",
            source_entity_id="task-unpin-cross",
            title="Unpinned",
        )
        pinned_state = PlannerItemState.objects.create(plan=plan, item=pinned_item, pinned=True, planned_order=1)
        unpinned_state = PlannerItemState.objects.create(plan=plan, item=unpinned_item, pinned=False, planned_order=2)

        payload = {"moved_id": pinned_state.id, "before_id": unpinned_state.id}
        response = self.client.post(
            reverse("planner_item_reorder"),
            data=json.dumps(payload),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 400)

    def test_reorder_rejects_unknown_ids(self):
        plan = PlannerPlan.objects.create(
            workspace=self.workspace,
            user=self.user,
            name="My Plan",
            timezone="UTC",
        )
        item = PlannerItem.objects.create(
            workspace=self.workspace,
            user=self.user,
            connector_account=self.account,
            source="asana",
            source_entity_id="task-x",
            title="Task",
        )
        PlannerItemState.objects.create(plan=plan, item=item, planned_order=1)

        payload = {"moved_id": 999999}
        response = self.client.post(
            reverse("planner_item_reorder"),
            data=json.dumps(payload),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 400)

    def test_reorder_rejects_non_integer_ids(self):
        plan = PlannerPlan.objects.create(
            workspace=self.workspace,
            user=self.user,
            name="My Plan",
            timezone="UTC",
        )
        item = PlannerItem.objects.create(
            workspace=self.workspace,
            user=self.user,
            connector_account=self.account,
            source="asana",
            source_entity_id="task-nonint",
            title="Task",
        )
        PlannerItemState.objects.create(plan=plan, item=item, planned_order=1)

        response = self.client.post(
            reverse("planner_item_reorder"),
            data=json.dumps({"moved_id": "abc"}),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 400)

    def test_reorder_block_order_updates_order(self):
        plan = PlannerPlan.objects.create(
            workspace=self.workspace,
            user=self.user,
            name="My Plan",
            timezone="UTC",
        )
        items = []
        for index in range(3):
            item = PlannerItem.objects.create(
                workspace=self.workspace,
                user=self.user,
                connector_account=self.account,
                source="asana",
                source_entity_id=f"task-block-{index}",
                title=f"Task {index}",
            )
            state = PlannerItemState.objects.create(plan=plan, item=item, planned_order=index + 1)
            items.append(state)

        payload = {
            "block_order": [items[2].id, items[0].id, items[1].id],
            "planner_status": PlannerItemState.PLANNER_STATUS_INBOX,
        }
        response = self.client.post(
            reverse("planner_item_reorder"),
            data=json.dumps(payload),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 200)
        items[2].refresh_from_db()
        self.assertEqual(items[2].planned_order, 1)

    def test_reorder_rapid_requests_end_with_last_order(self):
        plan = PlannerPlan.objects.create(
            workspace=self.workspace,
            user=self.user,
            name="My Plan",
            timezone="UTC",
        )
        items = []
        for index in range(3):
            item = PlannerItem.objects.create(
                workspace=self.workspace,
                user=self.user,
                connector_account=self.account,
                source="asana",
                source_entity_id=f"task-rapid-{index}",
                title=f"Task {index}",
            )
            state = PlannerItemState.objects.create(plan=plan, item=item, planned_order=index + 1)
            items.append(state)

        response_one = self.client.post(
            reverse("planner_item_reorder"),
            data=json.dumps({
                "block_order": [items[1].id, items[0].id, items[2].id],
                "planner_status": PlannerItemState.PLANNER_STATUS_INBOX,
            }),
            content_type="application/json",
        )
        response_two = self.client.post(
            reverse("planner_item_reorder"),
            data=json.dumps({
                "block_order": [items[2].id, items[1].id, items[0].id],
                "planner_status": PlannerItemState.PLANNER_STATUS_INBOX,
            }),
            content_type="application/json",
        )
        self.assertEqual(response_one.status_code, 200)
        self.assertEqual(response_two.status_code, 200)
        items[2].refresh_from_db()
        self.assertEqual(items[2].planned_order, 1)

    def test_add_from_sources_creates_items(self):
        Event.objects.create(
            workspace=self.workspace,
            connector_account=self.account,
            source="asana",
            source_entity_type="task",
            source_entity_id="task-4",
            event_type="task_state",
            title="Task 4",
            external_status="open",
            raw={},
            dedupe_hash="hash-4",
        )
        response = self.client.post(reverse("planner_add_from_sources"))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(PlannerItem.objects.count(), 1)

    def test_add_from_sources_uses_latest_event(self):
        Event.objects.create(
            workspace=self.workspace,
            connector_account=self.account,
            source="asana",
            source_entity_type="task",
            source_entity_id="task-5",
            event_type="task_updated",
            title="Task 5",
            external_status="open",
            raw={},
            dedupe_hash="hash-5",
        )
        response = self.client.post(reverse("planner_add_from_sources"))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(PlannerItem.objects.count(), 1)

    def _create_user(self, username: str):
        user_model = get_user_model()
        return user_model.objects.create_user(
            username=username,
            email=f"{username}@example.com",
            password="password123",
        )
