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

## Jira ingestion

Jira connector stores non-secret options in `ConnectorAccount.config["jira"]` and secrets in encrypted token fields.

- Incremental by default, with configurable lookback (`incremental_lookback_minutes`).
- Full/backfill supported through full sync mode and `initial_backfill_days`.
- Lifecycle mapping: `task_created`, `task_updated`, `task_completed`, `task_reopened`, `task_deleted`, optional `task_state`.
- Worklogs can emit `metric_recorded` (`time_spent`, `seconds`).
- Changelog is the preferred source for precise completion/reopen transition timestamps.
