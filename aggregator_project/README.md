# Aggregator Project

## Habitica ingestion (occurrence + state)

Habitica ingestion emits two classes of events into `events.Event`:

### Occurrence events

- Habits: `habit_scored` per history entry.
- Dailies: `daily_completed` per history entry (completed only), or `dateCompleted` fallback.
- Todos: `todo_completed` when `dateCompleted` is present.

Occurrence dedupe key: `source + source_entity_id + event_type + occurred_at` (stored in `source_event_version`).

### State snapshot events

- One `task_state` event per task per sync.
- `source_entity_type`: `habit` | `daily` | `todo` (matches Habitica type).
- `start_time`: `updatedAt` or `dateCreated` (UTC), fallback to `timezone.now()`.
- `status`: `completed` if `completed` else `open`.
- `metric_type`: `value` when `value` is present.
- Dedupe key: `source + source_entity_id + task_state + updatedAt` (or `dateCreated`).

Date-only timestamps are parsed as UTC midnight.
