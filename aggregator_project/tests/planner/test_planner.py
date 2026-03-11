from __future__ import annotations

import json
import threading
import time

from django.contrib.auth import get_user_model
from django.db import connections, transaction
from django.test import TestCase, TransactionTestCase
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


class PlannerReorderLockTests(TransactionTestCase):
    reset_sequences = True

    def setUp(self) -> None:
        self.user = get_user_model().objects.create_user(
            username="lock-user",
            email="lock@example.com",
            password="password123",
        )
        self.workspace = Workspace.objects.create(name="Lock Workspace")
        WorkspaceMember.objects.create(
            workspace=self.workspace,
            user=self.user,
            role=WorkspaceMember.ROLE_OWNER,
        )
        self.client.force_login(self.user)

        self.account = ConnectorAccount.objects.create(
            workspace=self.workspace,
            source="asana",
            display_name="Lock",
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            encrypted_access_token=b"token",
            status=ConnectorAccount.STATUS_CONNECTED,
        )
        self.plan = PlannerPlan.objects.create(
            workspace=self.workspace,
            user=self.user,
            name="My Plan",
            timezone="UTC",
        )
        self.states = []
        for index in range(3):
            item = PlannerItem.objects.create(
                workspace=self.workspace,
                user=self.user,
                connector_account=self.account,
                source="asana",
                source_entity_id=f"lock-task-{index}",
                title=f"Task {index}",
            )
            self.states.append(
                PlannerItemState.objects.create(plan=self.plan, item=item, planned_order=index + 1)
            )

    def _lock_block(self, plan_id: int, pinned: bool, ready_event: threading.Event, release_event: threading.Event):
        conn = connections["default"]
        try:
            with transaction.atomic():
                list(
                    PlannerItemState.objects
                    .select_for_update()
                    .filter(plan_id=plan_id, pinned=pinned)
                    .order_by("planned_order")
                )
                ready_event.set()
                release_event.wait(5)
        finally:
            conn.close()

    def test_reorder_returns_busy_when_locked(self):
        ready_event = threading.Event()
        release_event = threading.Event()
        thread = threading.Thread(
            target=self._lock_block,
            args=(self.plan.id, False, ready_event, release_event),
        )
        thread.start()
        self.assertTrue(ready_event.wait(2))

        start_time = time.monotonic()
        response = self.client.post(
            reverse("planner_item_reorder"),
            data=json.dumps({"moved_id": self.states[2].id, "before_id": self.states[0].id}),
            content_type="application/json",
        )
        duration = time.monotonic() - start_time
        release_event.set()
        thread.join(timeout=2)

        self.assertEqual(response.status_code, 409)
        self.assertLess(duration, 2.0)

    def test_reorder_two_quick_requests_succeed(self):
        response_one = self.client.post(
            reverse("planner_item_reorder"),
            data=json.dumps({"moved_id": self.states[2].id, "before_id": self.states[0].id}),
            content_type="application/json",
        )
        response_two = self.client.post(
            reverse("planner_item_reorder"),
            data=json.dumps({"moved_id": self.states[1].id, "before_id": self.states[2].id}),
            content_type="application/json",
        )
        self.assertEqual(response_one.status_code, 200)
        self.assertEqual(response_two.status_code, 200)
