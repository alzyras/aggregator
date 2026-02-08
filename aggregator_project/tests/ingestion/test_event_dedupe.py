from __future__ import annotations

from unittest.mock import patch

from django.test import TestCase, override_settings

from connectors.forms import AsanaConnectForm
from core.constants import SOURCE_ASANA
from events.models import Event
from ingestion.providers import ProviderSpec
from ingestion.services import sync as sync_service
from workspaces.models import Workspace, WorkspaceMember


@override_settings(
    DATABASES={
        "default": {
            "ENGINE": "django.db.backends.sqlite3",
            "NAME": ":memory:",
        }
    }
)
class EventDedupeTests(TestCase):
    def test_dedupe_upsert(self):
        workspace = Workspace.objects.create(name="Test workspace")
        WorkspaceMember.objects.create(
            workspace=workspace,
            user=self._create_user(),
            role=WorkspaceMember.ROLE_OWNER,
        )
        raw_items = [{"gid": "123", "name": "Test Task", "completed": False}]

        class StubClient:
            def __init__(self, _workspace):
                self.workspace = _workspace

            def fetch_since(self, since=None):
                return raw_items

        def stub_normalizer(raw):
            return {
                "source": SOURCE_ASANA,
                "source_entity_type": "task",
                "source_entity_id": raw.get("gid"),
                "title": raw.get("name"),
                "description": None,
                "start_time": None,
                "end_time": None,
                "metric_type": None,
                "metric_value": None,
                "metric_unit": None,
                "status": "open",
            }

        def empty_credentials():
            return {}

        def ok_credentials(_credentials):
            return True, "ok"

        spec = ProviderSpec(
            source="asana",
            label="Asana",
            client_factory=lambda _workspace: StubClient(_workspace),
            normalizer=stub_normalizer,
            required_fields=[],
            auth_type="api_token",
            env_credentials=empty_credentials,
            validate_credentials=ok_credentials,
            form_class=AsanaConnectForm,
            icon="bi-kanban",
        )

        def stub_get_provider_spec(source: str):
            return spec if source == "asana" else None

        with patch("ingestion.services.sync.get_provider_spec", stub_get_provider_spec):
            sync_service.sync_source("asana", workspace)
        assert Event.objects.for_workspace(workspace).count() == 1

        with patch("ingestion.services.sync.get_provider_spec", stub_get_provider_spec):
            sync_service.sync_source("asana", workspace)
        assert Event.objects.for_workspace(workspace).count() == 1

    def _create_user(self):
        from django.contrib.auth import get_user_model

        user_model = get_user_model()
        return user_model.objects.create_user(
            username="tester",
            email="tester@example.com",
            password="password123",
        )
