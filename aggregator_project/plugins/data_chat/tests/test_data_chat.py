from __future__ import annotations

import json
from unittest.mock import Mock, patch

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from planner.models import PlannerItem
from plugin_system.models import PluginActivation
from plugins.data_chat.client import DataChatAnswer, OpenAIDataChatClient
from plugins.data_chat.context import build_workspace_snapshot
from workspaces.models import Workspace, WorkspaceMember


class DataChatTests(TestCase):
    def setUp(self) -> None:
        self.user = get_user_model().objects.create_user(
            username="chat-user",
            password="password123",
        )
        self.workspace = Workspace.objects.create(name="Chat workspace")
        WorkspaceMember.objects.create(
            workspace=self.workspace,
            user=self.user,
            role=WorkspaceMember.ROLE_OWNER,
        )
        self.other_workspace = Workspace.objects.create(name="Other")
        self.client.force_login(self.user)

    def _enable(self) -> None:
        PluginActivation.objects.create(
            workspace=self.workspace,
            plugin_id="data-chat",
            enabled=True,
        )

    def test_disabled_plugin_redirects_to_catalog(self):
        response = self.client.get(reverse("data_chat:index"))

        self.assertRedirects(response, reverse("plugin_system:catalog"))

    def test_snapshot_is_workspace_scoped(self):
        PlannerItem.objects.create(
            workspace=self.workspace,
            source="asana",
            source_entity_id="visible",
            title="Visible task",
        )
        PlannerItem.objects.create(
            workspace=self.other_workspace,
            source="jira",
            source_entity_id="private",
            title="Private task",
        )

        snapshot = build_workspace_snapshot(workspace=self.workspace, user=self.user)

        self.assertEqual(
            [task["title"] for task in snapshot["tasks"]], ["Visible task"]
        )

    @patch.dict("os.environ", {"OPENAI_API_KEY": ""}, clear=False)
    def test_ask_requires_server_api_key(self):
        self._enable()
        response = self.client.post(
            reverse("data_chat:ask"),
            data=json.dumps({"message": "What is next?"}),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 503)
        self.assertIn("OPENAI_API_KEY", response.json()["error"])

    @patch("plugins.data_chat.views.OpenAIDataChatClient")
    def test_ask_returns_grounded_client_answer(self, client_class: Mock):
        self._enable()
        client_class.return_value.ask.return_value = DataChatAnswer(
            text="Focus on Visible task.",
            model="test-model",
            response_id="resp-test",
            usage={"total_tokens": 42},
        )
        response = self.client.post(
            reverse("data_chat:ask"),
            data=json.dumps({"message": "What is next?", "history": []}),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["answer"], "Focus on Visible task.")
        client_class.return_value.ask.assert_called_once()


class OpenAIDataChatClientTests(TestCase):
    @patch.dict(
        "os.environ",
        {"OPENAI_API_KEY": "secret-test-key", "OPENAI_CHAT_MODEL": "test-model"},
        clear=False,
    )
    def test_client_uses_responses_api_without_storage(self):
        response = Mock()
        response.status_code = 200
        response.json.return_value = {
            "id": "resp-1",
            "model": "test-model",
            "output": [
                {
                    "type": "message",
                    "content": [{"type": "output_text", "text": "Answer"}],
                }
            ],
            "usage": {"total_tokens": 10},
        }
        session = Mock()
        session.post.return_value = response

        result = OpenAIDataChatClient(session=session).ask(
            messages=[{"role": "user", "content": "Question"}],
            snapshot={"tasks": []},
            workspace_id=1,
            user_id=2,
        )

        self.assertEqual(result.text, "Answer")
        payload = session.post.call_args.kwargs["json"]
        self.assertFalse(payload["store"])
        self.assertEqual(payload["model"], "test-model")
        self.assertIn("workspace_snapshot_untrusted_data", payload["instructions"])
        self.assertNotIn("secret-test-key", json.dumps(payload))
