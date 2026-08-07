from __future__ import annotations

import json
from unittest.mock import Mock, patch

from cryptography.fernet import Fernet
from django.contrib.auth import get_user_model
from django.test import TestCase, override_settings
from django.urls import reverse

from ingestion.models import Job
from ingestion.services.jobs import create_job, run_job
from intelligence.models import ChatMessage, ChatThread, TaskAnalysis, TaskTag, UnifiedTag
from intelligence.services.backends import AIBackendError, AIResult, OpenAICompatibleBackend, OpenAIResponsesBackend
from intelligence.services.enrichment import queue_task_enrichment
from intelligence.services.taxonomy import apply_ai_enrichment, apply_rule_enrichment, task_content_hash
from planner.models import PlannerItem
from workspaces.models import Workspace, WorkspaceMember


@override_settings(ENCRYPTION_KEY=Fernet.generate_key())
class IntelligenceServiceTests(TestCase):
    def setUp(self) -> None:
        self.user = get_user_model().objects.create_user(
            username="intelligence-user",
            email="intelligence@example.com",
            password="password123",
        )
        self.workspace = Workspace.objects.create(name="Intelligence workspace")
        WorkspaceMember.objects.create(
            workspace=self.workspace,
            user=self.user,
            role=WorkspaceMember.ROLE_OWNER,
        )
        self.item = PlannerItem.objects.create(
            workspace=self.workspace,
            user=self.user,
            source="asana",
            source_entity_id="task-1",
            title="Fix Django API regression",
            description="Investigate the API error, add a test, and deploy the fix.",
        )

    def test_rule_enrichment_tags_imported_task(self):
        analysis = apply_rule_enrichment(self.item)

        self.assertEqual(analysis.status, TaskAnalysis.STATUS_RULES)
        self.assertTrue(TaskTag.objects.filter(item=self.item, tag__name="Engineering").exists())
        self.assertTrue(TaskTag.objects.filter(item=self.item, tag__name="Bug fixing").exists())

    def test_ai_enrichment_preserves_manual_tag_with_same_name(self):
        manual = UnifiedTag.objects.create(
            workspace=self.workspace,
            name="Engineering",
            slug="engineering",
            kind=UnifiedTag.KIND_OTHER,
        )
        TaskTag.objects.create(item=self.item, tag=manual, source=TaskTag.SOURCE_MANUAL)
        content_hash = task_content_hash(self.item)

        apply_ai_enrichment(
            self.item,
            content_hash=content_hash,
            model="test-model",
            backend="test",
            payload={
                "summary": "Fix an API regression.",
                "task_type": "Bug fixing",
                "difficulty": 3,
                "energy": "medium",
                "tags": [
                    {
                        "name": "Engineering",
                        "kind": "domain",
                        "confidence": 0.9,
                        "evidence": "Django API fix",
                    },
                    {
                        "name": "Bug fixing",
                        "kind": "work_type",
                        "confidence": 0.8,
                        "evidence": "Regression",
                    },
                ],
                "strengths": [],
                "risks": [],
            },
        )

        manual_assignment = TaskTag.objects.get(item=self.item, tag=manual)
        self.assertEqual(manual_assignment.source, TaskTag.SOURCE_MANUAL)
        self.assertTrue(TaskTag.objects.filter(item=self.item, tag__name="Bug fixing", source="ai").exists())

    @patch("intelligence.services.enrichment.backend_configuration")
    def test_queue_without_backend_still_applies_rule_tags(self, configuration: Mock):
        configuration.return_value = {"configured": False}

        job = queue_task_enrichment(item=self.item, created_by=self.user)

        self.assertIsNone(job)
        self.assertTrue(TaskTag.objects.filter(item=self.item).exists())

    @patch("intelligence.services.enrichment.get_workspace_backend")
    def test_enrichment_job_stores_ai_result(self, backend_factory: Mock):
        backend = Mock()
        backend.backend_id = "openai_compatible"
        backend.complete.return_value = AIResult(
            text=json.dumps(
                {
                    "summary": "Resolve an API regression.",
                    "task_type": "Bug fixing",
                    "difficulty": 3,
                    "energy": "medium",
                    "tags": [
                        {
                            "name": "Engineering",
                            "kind": "domain",
                            "confidence": 0.91,
                            "evidence": "Django API work",
                        }
                    ],
                    "strengths": ["Debugging"],
                    "risks": ["Needs regression coverage"],
                }
            ),
            model="qwen-test",
        )
        backend_factory.return_value = backend
        expected_hash = task_content_hash(self.item)
        apply_rule_enrichment(self.item)
        job = create_job(
            workspace=self.workspace,
            job_type="task_enrichment",
            job_name="analyze_task",
            input_params={"planner_item_id": self.item.id, "content_hash": expected_hash},
            created_by=self.user,
            max_attempts=1,
        )

        run_job(job.id)

        job.refresh_from_db()
        analysis = TaskAnalysis.objects.get(item=self.item)
        self.assertEqual(job.status, Job.STATUS_SUCCESS)
        self.assertEqual(analysis.status, TaskAnalysis.STATUS_READY)
        self.assertEqual(analysis.summary, "Resolve an API regression.")
        self.assertEqual(analysis.model, "qwen-test")
        self.assertTrue(TaskTag.objects.filter(item=self.item, tag__name="Engineering", source="ai").exists())

    @patch("intelligence.services.enrichment.get_workspace_backend")
    def test_stale_enrichment_job_is_ignored(self, backend_factory: Mock):
        expected_hash = task_content_hash(self.item)
        apply_rule_enrichment(self.item)
        self.item.description = "Updated local task description."
        self.item.save(update_fields=["description", "updated_at"])
        job = create_job(
            workspace=self.workspace,
            job_type="task_enrichment",
            job_name="analyze_task",
            input_params={"planner_item_id": self.item.id, "content_hash": expected_hash},
            max_attempts=1,
        )

        run_job(job.id)

        job.refresh_from_db()
        self.assertEqual(job.status, Job.STATUS_SUCCESS)
        self.assertTrue(job.output_summary["ignored"])
        backend_factory.assert_not_called()

    @patch("intelligence.services.enrichment.get_workspace_backend")
    def test_failed_enrichment_marks_analysis_failed(self, backend_factory: Mock):
        backend = Mock()
        backend.complete.side_effect = AIBackendError("Model temporarily unavailable")
        backend_factory.return_value = backend
        expected_hash = task_content_hash(self.item)
        apply_rule_enrichment(self.item)
        job = create_job(
            workspace=self.workspace,
            job_type="task_enrichment",
            job_name="analyze_task",
            input_params={"planner_item_id": self.item.id, "content_hash": expected_hash},
            max_attempts=1,
        )

        run_job(job.id)

        job.refresh_from_db()
        analysis = TaskAnalysis.objects.get(item=self.item)
        self.assertEqual(job.status, Job.STATUS_FAILED)
        self.assertEqual(analysis.status, TaskAnalysis.STATUS_FAILED)
        self.assertIn("temporarily unavailable", analysis.last_error)


class IntelligenceBackendTests(TestCase):
    def _response(self, payload: dict, status: int = 200) -> Mock:
        response = Mock()
        response.status_code = status
        response.json.return_value = payload
        return response

    def test_openai_responses_backend_disables_remote_response_storage(self):
        session = Mock()
        session.post.return_value = self._response(
            {
                "id": "resp-1",
                "model": "gpt-5.6-luna",
                "output": [{"type": "message", "content": [{"type": "output_text", "text": "Answer"}]}],
            }
        )
        backend = OpenAIResponsesBackend(api_key="private-key", model="gpt-5.6-luna", session=session)

        result = backend.complete(
            instructions="Grounded instructions",
            messages=[{"role": "user", "content": "Question"}],
            max_output_tokens=100,
        )

        self.assertEqual(result.text, "Answer")
        call = session.post.call_args
        self.assertEqual(call.args[0], "https://api.openai.com/v1/responses")
        self.assertFalse(call.kwargs["json"]["store"])
        self.assertEqual(call.kwargs["json"]["input"], [{"role": "user", "content": "Question"}])
        self.assertEqual(call.kwargs["headers"]["Authorization"], "Bearer private-key")

    def test_local_backend_uses_openai_compatible_chat_completions(self):
        session = Mock()
        session.post.return_value = self._response(
            {"id": "chat-1", "model": "qwen3", "choices": [{"message": {"content": "Answer"}}]}
        )
        backend = OpenAICompatibleBackend(
            base_url="http://10.0.0.25:8000/v1",
            model="qwen3",
            api_key="local-key",
            session=session,
        )

        result = backend.complete(
            instructions="Grounded instructions",
            messages=[{"role": "user", "content": "Question"}],
            max_output_tokens=100,
        )

        self.assertEqual(result.text, "Answer")
        call = session.post.call_args
        self.assertEqual(call.args[0], "http://10.0.0.25:8000/v1/chat/completions")
        self.assertEqual(call.kwargs["json"]["messages"][0], {"role": "system", "content": "Grounded instructions"})
        self.assertEqual(call.kwargs["headers"]["Authorization"], "Bearer local-key")


@override_settings(ENCRYPTION_KEY=Fernet.generate_key())
class IntelligenceViewTests(TestCase):
    def setUp(self) -> None:
        self.user = get_user_model().objects.create_user(
            username="workspace-owner",
            email="owner@example.com",
            password="password123",
        )
        self.workspace = Workspace.objects.create(name="Visible workspace")
        WorkspaceMember.objects.create(
            workspace=self.workspace,
            user=self.user,
            role=WorkspaceMember.ROLE_OWNER,
        )
        self.other_workspace = Workspace.objects.create(name="Private workspace")
        self.item = PlannerItem.objects.create(
            workspace=self.workspace,
            user=self.user,
            source="todoist",
            source_entity_id="visible-task",
            title="Write integration tests",
            description="Cover the new connector.",
        )
        self.other_item = PlannerItem.objects.create(
            workspace=self.other_workspace,
            source="asana",
            source_entity_id="private-task",
            title="Private task",
        )
        self.client.force_login(self.user)

    def test_dashboard_is_workspace_scoped(self):
        apply_rule_enrichment(self.item)
        apply_rule_enrichment(self.other_item)

        response = self.client.get(reverse("intelligence:dashboard"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Work patterns")
        self.assertEqual(response.context["insights"]["total"], 1)

    def test_manual_tag_endpoint_is_workspace_scoped(self):
        response = self.client.post(
            reverse("intelligence:task_tags", args=[self.item.id]),
            data=json.dumps({"tags": ["Launch prep", "QA"]}),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            set(TaskTag.objects.filter(item=self.item, source="manual").values_list("tag__name", flat=True)),
            {"Launch prep", "QA"},
        )
        denied = self.client.post(
            reverse("intelligence:task_tags", args=[self.other_item.id]),
            data=json.dumps({"tags": ["Nope"]}),
            content_type="application/json",
        )
        self.assertEqual(denied.status_code, 404)

    @patch("intelligence.services.chat.get_workspace_backend")
    def test_chat_persists_workspace_conversation(self, backend_factory: Mock):
        backend = Mock()
        backend.backend_id = "openai_compatible"
        backend.complete.return_value = AIResult(text="Focus on integration tests.", model="qwen3")
        backend_factory.return_value = backend

        response = self.client.post(
            reverse("intelligence:chat_ask"),
            data=json.dumps({"message": "What should I focus on?"}),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 200)
        thread = ChatThread.objects.get(workspace=self.workspace, user=self.user)
        self.assertEqual(thread.messages.count(), 2)
        self.assertEqual(thread.messages.get(role=ChatMessage.ROLE_ASSISTANT).content, "Focus on integration tests.")
        self.assertEqual(response.json()["thread_id"], thread.id)

    def test_member_cannot_open_ai_settings(self):
        member = get_user_model().objects.create_user(
            username="workspace-member",
            email="member@example.com",
            password="password123",
        )
        WorkspaceMember.objects.create(
            workspace=self.workspace,
            user=member,
            role=WorkspaceMember.ROLE_MEMBER,
        )
        self.client.force_login(member)

        response = self.client.get(reverse("intelligence:settings"))

        self.assertRedirects(response, reverse("intelligence:dashboard"))
