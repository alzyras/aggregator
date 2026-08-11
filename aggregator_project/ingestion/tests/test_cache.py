from __future__ import annotations

from unittest.mock import patch

from django.test import TestCase

from ingestion.models import WorkspaceRefreshPolicy
from ingestion.services.cache import (
    cache_get,
    cache_set,
    invalidate_workspace_cache,
    workspace_cache_key,
)
from workspaces.models import Workspace


class WorkspaceCacheTests(TestCase):
    def setUp(self):
        self.workspace = Workspace.objects.create(name="Cache workspace")

    def test_workspace_cache_key_advances_after_invalidation(self):
        before = workspace_cache_key(self.workspace, "stats", "all")

        invalidate_workspace_cache(self.workspace)

        after = workspace_cache_key(self.workspace, "stats", "all")
        policy = WorkspaceRefreshPolicy.objects.get(workspace=self.workspace)
        self.assertNotEqual(before, after)
        self.assertIn(":v2:", after)
        self.assertEqual(policy.cache_version, 2)

    def test_cache_read_failure_returns_default(self):
        with patch("ingestion.services.cache.cache.get", side_effect=RuntimeError("offline")):
            result = cache_get("workspace:key", default={"fresh": False})

        self.assertEqual(result, {"fresh": False})

    def test_cache_write_failure_is_non_fatal(self):
        with patch("ingestion.services.cache.cache.set", side_effect=RuntimeError("offline")):
            result = cache_set("workspace:key", {"fresh": True})

        self.assertFalse(result)
