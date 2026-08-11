from __future__ import annotations

from django.db.models import F

from ingestion.models import WorkspaceRefreshPolicy


def get_workspace_refresh_policy(workspace) -> WorkspaceRefreshPolicy:
    policy, _created = WorkspaceRefreshPolicy.objects.get_or_create(workspace=workspace)
    return policy


def workspace_cache_key(workspace, namespace: str, *parts: object) -> str:
    """Build process-independent keys that are invalidated with workspace data."""
    policy = get_workspace_refresh_policy(workspace)
    suffix = ":".join(str(part) for part in parts) if parts else "all"
    return f"aggregator:workspace:{workspace.id}:v{policy.cache_version}:{namespace}:{suffix}"


def invalidate_workspace_cache(workspace) -> None:
    """Advance the shared cache version after imported workspace data changes."""
    policy = get_workspace_refresh_policy(workspace)
    WorkspaceRefreshPolicy.objects.filter(pk=policy.pk).update(
        cache_version=F("cache_version") + 1
    )
