from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import Mock, patch

from cryptography.fernet import Fernet
from django.test import TestCase, override_settings

from connectors.encryption import encrypt_value
from connectors.models import ConnectorAccount
from planner.models import PlannerItemState
from providers.trello.client import TrelloClient
from providers.trello.forms import TrelloConnectForm
from providers.trello.normalizer import normalize_trello_card
from providers.trello.status_writer import TrelloStatusWriter
from workspaces.models import Workspace


@override_settings(ENCRYPTION_KEY=Fernet.generate_key())
class TrelloProviderTests(TestCase):
    def setUp(self) -> None:
        workspace = Workspace.objects.create(name="Trello workspace")
        self.account = ConnectorAccount.objects.create(
            workspace=workspace,
            source="trello",
            display_name="Trello",
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            encrypted_access_token=encrypt_value("trello-token"),
            encrypted_refresh_token=encrypt_value("trello-key"),
            status=ConnectorAccount.STATUS_CONNECTED,
            scopes={
                "trello": {
                    "board_ids": ["board123"],
                    "include_closed": True,
                    "todo_list_name": "To Do",
                    "in_progress_list_name": "Doing",
                    "emit_task_created": True,
                    "emit_task_updated": True,
                    "emit_task_completed": True,
                    "emit_task_state": False,
                }
            },
        )

    def test_form_parses_board_ids(self):
        form = TrelloConnectForm(
            data={
                "api_key": "trello-key",
                "api_token": "trello-token",
                "board_ids": "board123, board456\nboard123",
                "include_closed": "on",
                "todo_list_name": "To Do",
                "in_progress_list_name": "Doing",
                "emit_task_created": "on",
                "emit_task_updated": "on",
                "emit_task_completed": "on",
            }
        )

        self.assertTrue(form.is_valid(), form.errors)
        self.assertEqual(form.cleaned_data["board_ids"], ["board123", "board456"])

    def test_fetch_board_cards_maps_list_context(self):
        client = TrelloClient(self.account)
        lists_response = Mock()
        lists_response.json.return_value = [{"id": "list-1", "name": "Doing"}]
        cards_response = Mock()
        cards_response.json.return_value = [_card()]
        client.session.get = Mock(side_effect=[lists_response, cards_response])

        cards = client.fetch_since(datetime(2026, 7, 1, tzinfo=timezone.utc))

        self.assertEqual(len(cards), 1)
        self.assertEqual(cards[0]["__trello_planner_context"]["list_name"], "Doing")
        self.assertIsNotNone(cards[0]["__trello_created_at"])
        self.assertEqual(client.session.get.call_args_list[0].args[0], "https://api.trello.com/1/boards/board123/lists")
        self.assertEqual(client.session.get.call_args_list[1].kwargs["params"]["filter"], "all")

    def test_normalizer_emits_created_and_archived_events(self):
        raw = _card(closed=True)
        raw["__trello_created_at"] = "2026-06-30T10:00:00Z"
        raw["__trello_planner_context"] = {"list_name": "Done", "labels": ["release"]}
        raw["__trello_settings"] = self.account.scopes["trello"]

        events = normalize_trello_card(raw)

        self.assertEqual([event["event_type"] for event in events], ["task_created", "task_completed"])
        self.assertEqual(events[-1]["external_status"], "closed")

    def test_status_and_description_writeback_use_trello_parameters(self):
        card_response = Mock()
        card_response.json.return_value = {"idBoard": "board123", "idList": "list-1", "closed": False}
        lists_response = Mock()
        lists_response.json.return_value = [{"id": "list-2", "name": "Doing"}]
        update_response = Mock()
        update_response.json.return_value = {"id": "card-1", "desc": "Saved description"}
        writer = TrelloStatusWriter(self.account)

        with patch(
            "providers.trello.status_writer.requests.get",
            side_effect=[card_response, lists_response],
        ) as get_request, patch(
            "providers.trello.status_writer.requests.put",
            return_value=update_response,
        ) as put_request:
            result = writer.apply_planner_status(
                source_entity_id="card-1",
                planner_status=PlannerItemState.PLANNER_STATUS_DOING,
            )
            description_result = writer.update_description(
                source_entity_id="card-1",
                description="Saved description",
            )

        self.assertFalse(result.external_completed)
        self.assertEqual(result.source_status, "Doing")
        self.assertEqual(description_result.description, "Saved description")
        self.assertEqual(get_request.call_args_list[0].kwargs["params"]["fields"], "idBoard,idList,closed")
        self.assertEqual(
            put_request.call_args_list[0].kwargs["params"],
            {"key": "trello-key", "token": "trello-token", "idList": "list-2", "closed": "false"},
        )
        self.assertEqual(
            put_request.call_args_list[1].kwargs["params"],
            {"key": "trello-key", "token": "trello-token", "desc": "Saved description"},
        )


def _card(*, closed: bool = False) -> dict:
    return {
        "id": "6682aa00abc1234567890000",
        "idBoard": "board123",
        "idList": "list-1",
        "name": "Prepare release notes",
        "desc": "Collect updates for the release.",
        "closed": closed,
        "dateLastActivity": "2026-07-02T10:00:00.000Z",
        "due": None,
        "url": "https://trello.com/c/card-1",
        "labels": [{"name": "release"}],
    }
