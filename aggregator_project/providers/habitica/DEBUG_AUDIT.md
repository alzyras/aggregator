# Habitica Debug Audits

## Scripts

- `uv run python aggregator_project/providers/habitica/scripts/audit_payloads.py --connector-account-id <id> [--limit N] [--verbose] [--output report.json]`
  - Fetches tasks for the connector, groups by type, reports field presence (id/text/notes/value/priority/completed/dateCreated/dateCompleted/updatedAt/history keys).
  - Outputs console table (verbose) and optional JSON report.

- `uv run python aggregator_project/providers/habitica/scripts/audit_events.py --connector-account-id <id> [--limit N] [--force-all] [--verbose] [--output report.json]`
  - Normalizes tasks in-memory (no DB writes) and reports emitted event types per task type, timestamp source counts, and skip reasons (e.g., missing history/dateCompleted).
  - `--force-all` turns on all Habitica options to show maximum possible events.

Both scripts load `.env` automatically (dotenv) and use current connector credentials.

## Reading output

- Presence stats: percentage of tasks that include each field; history section shows how many tasks have history, average entries, and which keys appear in history entries.
- Event audit: counts per `source_entity_type` + `event_type`; timestamp source counts show how often we had a timestamp; skip reasons indicate missing data.

## Mapping → plugin options

- `emit_history_occurrences`: only enabled when history entries are present for habits.
- `emit_completion_occurrences`: for dailies/todos when `dateCompleted` (or completion history) is present.
- `task_state_created/updated/completed`: snapshot events gated by these toggles; use `dateCreated`, `updatedAt`, `dateCompleted` respectively.
- `sync_habits/sync_dailies/sync_todos`: gate fetch+normalize per type.

Use the audit reports to decide which toggles should remain on for a given connector.
