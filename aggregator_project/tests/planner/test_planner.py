from __future__ import annotations

import json
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import Mock, patch

from connectors.models import ConnectorAccount
from cryptography.fernet import Fernet
from django.contrib.auth import get_user_model
from django.test import TestCase, override_settings
from django.urls import reverse
from django.utils import timezone
from events.models import Event
from ingestion.models import Job
from ingestion.providers import STATUS_WRITEBACK_SUCCESS, StatusWritebackResult
from ingestion.services.jobs import run_job
from planner.models import PlannerItem, PlannerItemState, PlannerPlan
from planner.services.reconcile import reconcile_from_event
from workspaces.models import Workspace, WorkspaceMember


@override_settings(ENCRYPTION_KEY=Fernet.generate_key())
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
            encrypted_access_token=b"",
            status=ConnectorAccount.STATUS_CONNECTED,
        )
        self.account.set_access_token("token")
        self.account.save(update_fields=["encrypted_access_token"])

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

    def test_planner_list_renders_tabbed_list_rows_not_board_columns(self):
        PlannerItem.objects.create(
            workspace=self.workspace,
            user=None,
            connector_account=self.account,
            source="asana",
            source_entity_id="task-list-shape",
            title="List Shape Task",
            last_synced_at=timezone.now(),
        )

        response = self.client.get(reverse("planner_list"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "planner-tabs")
        self.assertContains(response, "planner-row")
        self.assertNotContains(response, "planner-board")
        self.assertNotContains(response, "planner-column")

    def test_planner_rows_show_created_date_not_sync_timestamp(self):
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
            source_entity_id="task-created-date",
            title="Created Date Task",
        )
        PlannerItemState.objects.create(plan=plan, item=item)

        response = self.client.get(reverse("planner_list"))

        item.refresh_from_db()
        self.assertContains(response, f"Created {item.created_at:%Y-%m-%d}")
        self.assertContains(response, item.created_at.strftime("%Y-%m-%d"))
        self.assertNotContains(response, "Never synced")
        self.assertNotContains(response, "Synced ")

    def test_planner_inbox_includes_unplanned_synced_tasks_newest_first(self):
        todoist_account = ConnectorAccount.objects.create(
            workspace=self.workspace,
            source="todoist",
            display_name="Todoist",
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            encrypted_access_token=b"",
            status=ConnectorAccount.STATUS_CONNECTED,
        )
        older = PlannerItem.objects.create(
            workspace=self.workspace,
            user=None,
            connector_account=self.account,
            source="asana",
            source_entity_id="task-older",
            title="Older Task",
            last_synced_at=timezone.now(),
        )
        newer = PlannerItem.objects.create(
            workspace=self.workspace,
            user=None,
            connector_account=todoist_account,
            source="todoist",
            source_entity_id="task-newer",
            title="Newer Task",
            last_synced_at=timezone.now(),
        )
        PlannerItem.objects.filter(id=older.id).update(created_at=timezone.now() - timedelta(hours=1))
        PlannerItem.objects.filter(id=newer.id).update(created_at=timezone.now())

        response = self.client.get(reverse("planner_list"))

        inbox = next(tab for tab in response.context["tab_items"] if tab["value"] == PlannerItemState.PLANNER_STATUS_INBOX)
        self.assertEqual([row.item.id for row in inbox["items"][:2]], [newer.id, older.id])

    def test_planner_inbox_excludes_items_assigned_to_status_tabs(self):
        plan = PlannerPlan.objects.create(
            workspace=self.workspace,
            user=self.user,
            name="My Plan",
            timezone="UTC",
        )
        assigned = PlannerItem.objects.create(
            workspace=self.workspace,
            user=self.user,
            connector_account=self.account,
            source="asana",
            source_entity_id="task-assigned",
            title="Assigned Task",
            last_synced_at=timezone.now(),
        )
        unplanned = PlannerItem.objects.create(
            workspace=self.workspace,
            user=None,
            connector_account=self.account,
            source="asana",
            source_entity_id="task-unplanned",
            title="Unplanned Task",
            last_synced_at=timezone.now(),
        )
        PlannerItemState.objects.create(
            plan=plan,
            item=assigned,
            planner_status=PlannerItemState.PLANNER_STATUS_BACKLOG,
        )

        response = self.client.get(reverse("planner_list"))

        tabs = {tab["value"]: tab for tab in response.context["tab_items"]}
        inbox_ids = [row.item.id for row in tabs[PlannerItemState.PLANNER_STATUS_INBOX]["items"]]
        backlog_ids = [row.item.id for row in tabs[PlannerItemState.PLANNER_STATUS_BACKLOG]["items"]]
        self.assertNotIn(assigned.id, inbox_ids)
        self.assertIn(unplanned.id, inbox_ids)
        self.assertIn(assigned.id, backlog_ids)

    def test_planner_inbox_excludes_duplicate_source_identity_assigned_to_status_tab(self):
        plan = PlannerPlan.objects.create(
            workspace=self.workspace,
            user=self.user,
            name="My Plan",
            timezone="UTC",
        )
        assigned = PlannerItem.objects.create(
            workspace=self.workspace,
            user=self.user,
            connector_account=self.account,
            source="asana",
            source_entity_id="task-source-duplicate",
            title="Same Provider Task",
            last_synced_at=timezone.now(),
        )
        duplicate = PlannerItem.objects.create(
            workspace=self.workspace,
            user=None,
            connector_account=None,
            source="asana",
            source_entity_id="task-source-duplicate",
            title="Same Provider Task",
            last_synced_at=timezone.now(),
        )
        PlannerItemState.objects.create(
            plan=plan,
            item=assigned,
            planner_status=PlannerItemState.PLANNER_STATUS_BACKLOG,
        )

        response = self.client.get(reverse("planner_list"))

        tabs = {tab["value"]: tab for tab in response.context["tab_items"]}
        inbox_ids = [row.item.id for row in tabs[PlannerItemState.PLANNER_STATUS_INBOX]["items"]]
        backlog_ids = [row.item.id for row in tabs[PlannerItemState.PLANNER_STATUS_BACKLOG]["items"]]
        self.assertNotIn(duplicate.id, inbox_ids)
        self.assertIn(assigned.id, backlog_ids)

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
        self.assertEqual(state.writeback_status, PlannerItemState.WRITEBACK_STATUS_PENDING)
        self.assertTrue(Job.objects.filter(job_type="planner_status_writeback").exists())

    def test_planner_status_endpoint_creates_state_for_unplanned_item(self):
        PlannerPlan.objects.create(
            workspace=self.workspace,
            user=self.user,
            name="My Plan",
            timezone="UTC",
        )
        item = PlannerItem.objects.create(
            workspace=self.workspace,
            user=None,
            connector_account=self.account,
            source="asana",
            source_entity_id="task-unplanned-status",
            title="Unplanned Status Task",
            last_synced_at=timezone.now(),
        )

        response = self.client.post(
            reverse("planner_item_planner_status", args=[item.id]),
            data=json.dumps({"planner_status": PlannerItemState.PLANNER_STATUS_BACKLOG}),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 200)
        state = PlannerItemState.objects.get(item=item)
        self.assertEqual(response.json()["state_id"], state.id)
        self.assertEqual(state.planner_status, PlannerItemState.PLANNER_STATUS_BACKLOG)
        self.assertEqual(state.writeback_status, PlannerItemState.WRITEBACK_STATUS_PENDING)
        self.assertTrue(Job.objects.filter(job_type="planner_status_writeback").exists())

    def test_planner_status_endpoint_reuses_state_for_duplicate_source_identity(self):
        plan = PlannerPlan.objects.create(
            workspace=self.workspace,
            user=self.user,
            name="My Plan",
            timezone="UTC",
        )
        assigned = PlannerItem.objects.create(
            workspace=self.workspace,
            user=self.user,
            connector_account=self.account,
            source="asana",
            source_entity_id="task-existing-source",
            title="Existing Provider Task",
            last_synced_at=timezone.now(),
        )
        duplicate = PlannerItem.objects.create(
            workspace=self.workspace,
            user=None,
            connector_account=None,
            source="asana",
            source_entity_id="task-existing-source",
            title="Existing Provider Task",
            last_synced_at=timezone.now(),
        )
        existing_state = PlannerItemState.objects.create(
            plan=plan,
            item=assigned,
            planner_status=PlannerItemState.PLANNER_STATUS_BACKLOG,
        )

        response = self.client.post(
            reverse("planner_item_planner_status", args=[duplicate.id]),
            data=json.dumps({"planner_status": PlannerItemState.PLANNER_STATUS_DOING}),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        existing_state.refresh_from_db()
        self.assertEqual(payload["item_id"], assigned.id)
        self.assertEqual(payload["state_id"], existing_state.id)
        self.assertEqual(existing_state.planner_status, PlannerItemState.PLANNER_STATUS_DOING)
        self.assertFalse(PlannerItemState.objects.filter(plan=plan, item=duplicate).exists())

    def test_planner_status_endpoint_marks_disconnected_account_unsupported(self):
        self.account.status = ConnectorAccount.STATUS_ERROR
        self.account.save(update_fields=["status"])
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
            source_entity_id="task-disconnected",
            title="Disconnected Task",
        )
        state = PlannerItemState.objects.create(plan=plan, item=item)

        response = self.client.post(
            reverse("planner_item_planner_status", args=[item.id]),
            data=json.dumps({"planner_status": PlannerItemState.PLANNER_STATUS_DONE}),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 200)
        state.refresh_from_db()
        self.assertEqual(state.planner_status, PlannerItemState.PLANNER_STATUS_DONE)
        self.assertEqual(state.writeback_status, PlannerItemState.WRITEBACK_STATUS_UNSUPPORTED)
        self.assertEqual(Job.objects.filter(job_type="planner_status_writeback").count(), 0)

    def test_planner_writeback_job_updates_external_fields(self):
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
            source_entity_id="task-writeback",
            title="Writeback Task",
            source_status="open",
        )
        state = PlannerItemState.objects.create(
            plan=plan,
            item=item,
            planner_status=PlannerItemState.PLANNER_STATUS_DONE,
            external_status_requested=PlannerItemState.PLANNER_STATUS_DONE,
            writeback_status=PlannerItemState.WRITEBACK_STATUS_PENDING,
        )
        job = Job.objects.create(
            workspace=self.workspace,
            connector_account=self.account,
            job_type="planner_status_writeback",
            job_name="planner_status_writeback",
            input_params={
                "planner_item_state_id": state.id,
                "planner_item_id": item.id,
                "planner_status": PlannerItemState.PLANNER_STATUS_DONE,
            },
            created_by=self.user,
        )
        writer = Mock()
        writer.apply_planner_status.return_value = StatusWritebackResult(
            status=STATUS_WRITEBACK_SUCCESS,
            source_status="completed",
            external_completed=True,
            message="ok",
        )
        spec = SimpleNamespace(status_writer_factory=lambda account: writer)

        with patch("planner.services.writeback.get_provider_spec", return_value=spec):
            run_job(job.id)

        item.refresh_from_db()
        state.refresh_from_db()
        job.refresh_from_db()
        self.assertEqual(job.status, Job.STATUS_SUCCESS)
        self.assertEqual(item.source_status, "completed")
        self.assertTrue(item.external_completed)
        self.assertEqual(state.writeback_status, PlannerItemState.WRITEBACK_STATUS_SYNCED)

    @override_settings(PLANNER_STATUS_WRITEBACK_MAX_RETRIES=1)
    def test_planner_writeback_job_failure_stays_pending_while_retrying(self):
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
            source_entity_id="task-failure",
            title="Failure Task",
        )
        state = PlannerItemState.objects.create(
            plan=plan,
            item=item,
            planner_status=PlannerItemState.PLANNER_STATUS_DONE,
            external_status_requested=PlannerItemState.PLANNER_STATUS_DONE,
            writeback_status=PlannerItemState.WRITEBACK_STATUS_PENDING,
        )
        job = Job.objects.create(
            workspace=self.workspace,
            connector_account=self.account,
            job_type="planner_status_writeback",
            job_name="planner_status_writeback",
            input_params={
                "planner_item_state_id": state.id,
                "planner_item_id": item.id,
                "planner_status": PlannerItemState.PLANNER_STATUS_DONE,
            },
            created_by=self.user,
        )
        writer = Mock()
        writer.apply_planner_status.side_effect = RuntimeError("provider down")
        spec = SimpleNamespace(status_writer_factory=lambda account: writer)

        with patch("planner.services.writeback.get_provider_spec", return_value=spec):
            run_job(job.id)

        state.refresh_from_db()
        job.refresh_from_db()
        self.assertEqual(state.writeback_status, PlannerItemState.WRITEBACK_STATUS_PENDING)
        self.assertEqual(state.last_writeback_error, "")
        self.assertEqual(job.status, Job.STATUS_QUEUED)
        self.assertEqual(job.attempt_count, 1)

    @override_settings(PLANNER_STATUS_WRITEBACK_MAX_RETRIES=0)
    def test_planner_writeback_job_failure_marks_state_after_retries_exhausted(self):
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
            source_entity_id="task-final-failure",
            title="Final Failure Task",
        )
        state = PlannerItemState.objects.create(
            plan=plan,
            item=item,
            planner_status=PlannerItemState.PLANNER_STATUS_DONE,
            external_status_requested=PlannerItemState.PLANNER_STATUS_DONE,
            writeback_status=PlannerItemState.WRITEBACK_STATUS_PENDING,
        )
        job = Job.objects.create(
            workspace=self.workspace,
            connector_account=self.account,
            job_type="planner_status_writeback",
            job_name="planner_status_writeback",
            input_params={
                "planner_item_state_id": state.id,
                "planner_item_id": item.id,
                "planner_status": PlannerItemState.PLANNER_STATUS_DONE,
            },
            created_by=self.user,
        )
        writer = Mock()
        writer.apply_planner_status.side_effect = RuntimeError("provider down")
        spec = SimpleNamespace(status_writer_factory=lambda account: writer)

        with patch("planner.services.writeback.get_provider_spec", return_value=spec):
            run_job(job.id)

        state.refresh_from_db()
        job.refresh_from_db()
        self.assertEqual(state.writeback_status, PlannerItemState.WRITEBACK_STATUS_FAILED)
        self.assertIn("provider down", state.last_writeback_error)
        self.assertEqual(job.status, Job.STATUS_FAILED)
        self.assertEqual(job.attempt_count, 1)

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
