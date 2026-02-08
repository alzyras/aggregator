from __future__ import annotations

from dataclasses import replace
from unittest.mock import patch

from django.test import TestCase

from connectors.forms import AsanaConnectForm
from connectors.models import ConnectorAccount
from connectors.views import _build_provider_cards
from ingestion.providers import ProviderSpec, get_provider_specs
from workspaces.models import Workspace


class ProviderCardTests(TestCase):
    def _stub_spec(self) -> ProviderSpec:
        base_spec = next(spec for spec in get_provider_specs() if spec.source == "asana")
        return replace(base_spec, form_class=AsanaConnectForm)

    def test_provider_card_enabled_by_default(self):
        workspace = Workspace.objects.create(name="Workspace")
        spec = self._stub_spec()

        with patch("connectors.views.get_provider_specs", return_value=[spec]):
            cards = _build_provider_cards(workspace)

        self.assertEqual(len(cards), 1)
        self.assertTrue(cards[0]["enabled"])

    def test_provider_card_disabled_when_not_enabled(self):
        workspace = Workspace.objects.create(name="Workspace")
        spec = self._stub_spec()

        with patch("connectors.views.get_provider_specs", return_value=[spec]):
            with patch.dict("os.environ", {"ENABLED_PLUGINS": "todoist"}):
                cards = _build_provider_cards(workspace)

        self.assertFalse(cards[0]["enabled"])
        self.assertEqual(cards[0]["display_status"], "inactive")

    def test_provider_card_connected_status(self):
        workspace = Workspace.objects.create(name="Workspace")
        spec = self._stub_spec()
        ConnectorAccount.objects.create(
            workspace=workspace,
            source="asana",
            display_name="Asana",
            auth_type=ConnectorAccount.AUTH_API_TOKEN,
            credentials="",
            status=ConnectorAccount.STATUS_CONNECTED,
        )

        with patch("connectors.views.get_provider_specs", return_value=[spec]):
            cards = _build_provider_cards(workspace)

        self.assertEqual(cards[0]["display_status"], "connected")
