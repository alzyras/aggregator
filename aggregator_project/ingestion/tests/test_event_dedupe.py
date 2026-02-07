from __future__ import annotations

from unittest.mock import patch

from django.test import TestCase, override_settings

from events.models import Event
from ingestion.providers import ProviderSpec
from ingestion.services import sync as sync_service


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
        raw_items = [{"gid": "123", "name": "Test Task", "completed": False}]

        class StubClient:
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

        spec = ProviderSpec(
            source="asana",
            label="Asana",
            client_factory=StubClient,
            normalizer=stub_normalizer,
        )

        def stub_get_provider_spec(source: str):
            return spec if source == "asana" else None

        with patch("ingestion.services.sync.get_provider_spec", stub_get_provider_spec):
            sync_service.sync_source("asana")
        assert Event.objects.count() == 1

        with patch("ingestion.services.sync.get_provider_spec", stub_get_provider_spec):
            sync_service.sync_source("asana")
        assert Event.objects.count() == 1
