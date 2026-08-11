from __future__ import annotations

import logging

from django.core.cache import cache
from django.db.models import F

from ingestion.models import WorkspaceRefreshPolicy


logger = logging.getLogger(__name__)


def get_workspace_refresh_policy(workspace) -> WorkspaceRefreshPolicy:
    policy, _created = WorkspaceRefreshPolicy.objects.get_or_create(workspace=workspace)
    return policy


def workspace_cache_key(
    workspace,
    namespace: str,
    *parts: object,
    cache_version: int | None = None,
) -> str:
    """Build process-independent keys that are invalidated with workspace data."""
    if cache_version is None:
        cache_version = get_workspace_refresh_policy(workspace).cache_version
    suffix = ":".join(str(part) for part in parts) if parts else "all"
    return f"aggregator:workspace:{workspace.id}:v{cache_version}:{namespace}:{suffix}"


def cache_get(key: str, default=None):
    """Read derived data without making a cache outage user-visible."""
    try:
        return cache.get(key, default)
    except Exception:  # noqa: BLE001
        logger.warning("workspace_cache_read_failed", extra={"cache_key": key})
        return default


def cache_set(key: str, value, timeout: int | None = None) -> bool:
    """Write derived data opportunistically; the database remains authoritative."""
    try:
        cache.set(key, value, timeout)
    except Exception:  # noqa: BLE001
        logger.warning("workspace_cache_write_failed", extra={"cache_key": key})
        return False
    return True


def invalidate_workspace_cache(workspace) -> None:
    """Advance the shared cache version after imported workspace data changes."""
    policy = get_workspace_refresh_policy(workspace)
    WorkspaceRefreshPolicy.objects.filter(pk=policy.pk).update(
        cache_version=F("cache_version") + 1
    )
