from __future__ import annotations

from datetime import datetime, timedelta, timezone as dt_timezone
import copy
import logging
import os

from django.conf import settings
from django.db import IntegrityError, transaction
from django.db.models import Max
from django.utils import timezone
from django.utils.dateparse import parse_datetime

from connectors.models import ConnectorAccount
from events.models import Event
from ingestion.normalizers.base import build_dedupe_hash
from ingestion.normalizers.utils import (
    CANONICAL_EVENT_TYPES,
    canonical_event_type,
    serialize_raw,
)
from ingestion.providers import get_provider_spec
from ingestion.services.cache import invalidate_workspace_cache
from planner.services.reconcile import reconcile_from_event

logger = logging.getLogger(__name__)


def _latest_event_timestamp(connector_account: ConnectorAccount) -> datetime | None:
    qs = Event.objects.filter(connector_account=connector_account)
    max_start = qs.aggregate(Max("start_time"))["start_time__max"]
    parsed_max = None
    for value in qs.values_list("source_event_version", flat=True):
        dt = parse_datetime(value) if value else None
        if dt and (parsed_max is None or dt > parsed_max):
            parsed_max = dt
    candidates = [dt for dt in (max_start, parsed_max) if dt]
    return max(candidates) if candidates else None


@transaction.atomic
def sync_connector_account(
    workspace,
    connector_account: ConnectorAccount,
    since: datetime | None = None,
    full_sync: bool = False,
) -> dict[str, object]:
    if connector_account.workspace_id != workspace.id:
        raise ValueError("Connector account does not belong to workspace.")
    if not connector_account.is_active or connector_account.revoked_at:
        raise ValueError("Connector account is inactive or revoked.")

    spec = get_provider_spec(connector_account.source)
    if not spec:
        raise ValueError(f"Unknown provider source: {connector_account.source}")

    sync_started_at = timezone.now()
    effective_full_sync = full_sync
    if since is None and not full_sync:
        since = _incremental_since(connector_account)
        effective_full_sync = since is None

    client = spec.client_factory(connector_account)
    raw_items = client.fetch_since(since)

    inserted = 0
    skipped = 0
    progress_enabled = os.getenv("SYNC_PROGRESS") == "1"
    progress_every = int(os.getenv("SYNC_PROGRESS_EVERY", "500"))
    for raw_index, raw in enumerate(raw_items, start=1):
        raw_for_normalizer = _raw_with_connector_settings(raw, connector_account)
        original_raw = copy.deepcopy(raw_for_normalizer)
        normalized_items = spec.normalizer(raw_for_normalizer)
        if isinstance(normalized_items, dict):
            normalized_items = [normalized_items]
        for normalized in normalized_items:
            raw_event_type = normalized.get("event_type")
            if not raw_event_type:
                skipped += 1
                continue
            normalized["event_type"] = canonical_event_type(raw_event_type)
            if normalized["event_type"] not in CANONICAL_EVENT_TYPES:
                skipped += 1
                continue
            if since and _normalized_at_or_before_since(normalized, since):
                skipped += 1
                continue
            if "raw" not in normalized:
                normalized["raw"] = original_raw
            raw_for_dedupe = serialize_raw(normalized["raw"])
            normalized["dedupe_hash"] = build_dedupe_hash({**normalized, "raw": raw_for_dedupe})
            if spec.raw_sanitizer is not None and isinstance(normalized["raw"], dict):
                normalized["raw"] = spec.raw_sanitizer(normalized["raw"])
            normalized["raw"] = serialize_raw(normalized["raw"])
            if not normalized.get("source_entity_id"):
                skipped += 1
                continue
            try:
                with transaction.atomic():
                    event = Event.objects.create(
                        workspace=workspace,
                        connector_account=connector_account,
                        **normalized,
                    )
                inserted += 1
                try:
                    reconcile_from_event(event)
                except Exception:  # noqa: BLE001
                    logger.exception("planner_reconcile_failed", extra={"event_id": str(event.id)})
            except IntegrityError:
                skipped += 1
            if progress_enabled and (inserted + skipped) % progress_every == 0:
                print(
                    f"[sync] processed {inserted + skipped} events "
                    f"(inserted={inserted}, skipped={skipped})"
                )
        if progress_enabled and raw_index % progress_every == 0:
            print(f"[sync] processed {raw_index} raw items")

    sync_finished_at = timezone.now()
    connector_account.last_sync_at = sync_finished_at
    connector_account.last_sync_status = ConnectorAccount.SYNC_STATUS_SUCCESS
    connector_account.sync_cursor_at = sync_started_at
    update_fields = [
        "last_sync_at",
        "last_sync_status",
        "sync_cursor_at",
    ]
    if effective_full_sync:
        connector_account.last_full_sync_at = sync_finished_at
        update_fields.append("last_full_sync_at")
    else:
        connector_account.last_incremental_sync_at = sync_finished_at
        update_fields.append("last_incremental_sync_at")
    connector_account.save(update_fields=update_fields)
    if inserted:
        invalidate_workspace_cache(workspace)

    return {
        "inserted": inserted,
        "skipped": skipped,
        "total": len(raw_items),
        "connector_account_id": str(connector_account.id),
        "mode": "full" if effective_full_sync else "incremental",
        "since": since.isoformat() if since else None,
    }


def _incremental_since(connector_account: ConnectorAccount) -> datetime | None:
    cursor = connector_account.sync_cursor_at or _latest_event_timestamp(connector_account)
    if cursor is None:
        return None
    lookback_minutes = max(
        int(getattr(settings, "SYNC_INCREMENTAL_LOOKBACK_MINUTES", 5)),
        0,
    )
    return cursor - timedelta(minutes=lookback_minutes)


def _normalized_at_or_before_since(normalized: dict, since: datetime) -> bool:
    candidates = []
    for field in ("source_event_version", "start_time", "end_time"):
        value = normalized.get(field)
        if isinstance(value, datetime):
            candidates.append(value)
        elif isinstance(value, str):
            parsed = parse_datetime(value)
            if parsed:
                candidates.append(parsed)
    if not candidates:
        return False
    normalized_candidates = [_ensure_aware(value) for value in candidates]
    return max(normalized_candidates) <= _ensure_aware(since)


def _ensure_aware(value: datetime) -> datetime:
    if timezone.is_naive(value):
        return timezone.make_aware(value, dt_timezone.utc)
    return value


def _raw_with_connector_settings(raw, connector_account: ConnectorAccount):
    if not isinstance(raw, dict) or not isinstance(connector_account.scopes, dict):
        return raw
    provider_settings = connector_account.scopes.get(connector_account.source)
    if not isinstance(provider_settings, dict):
        return raw
    enriched = copy.deepcopy(raw)
    enriched[f"__{connector_account.source}_settings"] = provider_settings
    enriched[f"_{connector_account.source}_settings"] = provider_settings
    return enriched
