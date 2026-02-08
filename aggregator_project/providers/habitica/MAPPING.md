# Habitica → Event Mapping

This provider emits append-only per-occurrence events from Habitica tasks.

## Source data

- Habits: `/api/v3/tasks/user?type=habits` with `history` entries.
- Dailies: `/api/v3/tasks/user?type=dailys` with `history` entries (completed only), or `dateCompleted` fallback.
- Todos: `/api/v3/tasks/user?type=completedTodos` (and `todos` if completed).

## Event mapping

Each occurrence is converted into one `events.Event` row:

- `source`: `habitica`
- `source_entity_type`: `habit` | `daily` | `todo`
- `source_entity_id`: Habitica task id (`task.id` or `task._id`)
- `event_type`:
  - `habit_scored` for habit history entries
  - `daily_completed` for daily completion history / `dateCompleted`
  - `todo_completed` for todo completion
- `title`: task `text`
- `description`: task `notes`
- `start_time`: occurrence timestamp (history `date` or `dateCompleted`)
- `end_time`: `null`
- `metric_type`:
  - `score` for habits/dailies when a history value exists
- `metric_value`:
  - history `value` (score delta) when present
- `metric_unit`: `points` when `metric_type` is set
- `external_status`:
  - `scored` for habit events
  - `completed` for daily/todo events
- `source_event_version`: ISO timestamp of the occurrence
- `raw`: original task + occurrence payload

## Dedupe

Dedupe is per occurrence. The hash includes the occurrence timestamp via `source_event_version`, so multiple habit/daily history entries do not collapse into one event.
