# Asana → Event Mapping

## Occurrence events

- `task_created`: `created_at`
- `task_completed`: `completed_at`
- `task_reopened`: when `completed` is false but `completed_at` is present; timestamp uses `modified_at`
- `task_deleted`: when `archived` or `resource_subtype=archived`; timestamp uses `modified_at`
- `task_updated`: `modified_at`

Occurrence dedupe key uses `source + source_entity_id + event_type + occurred_at` (via `source_event_version`).

## State snapshot events

- One `task_state` per task per sync.
- `start_time`: `modified_at` (fallback to `created_at`, `start_at`, or `due_at`).
- `external_status`: `completed` or `open`.
- Dedupe key: `source + source_entity_id + task_state + modified_at`.

## Actor mapping

- `task_created`: `created_by`
- `task_completed`: `completed_by` (fallback to `last_modified_by`)
- `task_updated`/`task_reopened`/`task_deleted`: `last_modified_by`

Actor fields are provider-scoped and stored on each event.

## Timestamp rules

If a date-only value is provided (`due_at` without time), it is parsed as UTC midnight.
