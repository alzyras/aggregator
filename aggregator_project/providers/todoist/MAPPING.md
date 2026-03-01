# Todoist → Event Mapping

Canonical events
- task_created: `added_at`/`created_at`
- task_updated: `updated_at` or `sync_updated_at` or `date_updated`
- task_completed: `completed_at` (when `completed` true)
- task_deleted: when `is_deleted` true (timestamp uses updated/completed/created fallback)
- task_state: snapshots using created/updated/completed timestamps per toggles

Timestamp precedence
- created: `added_at` → `created_at`
- updated: `updated_at` → `sync_updated_at` → `date_updated` → `due.datetime`/`due.date`
- completed: `completed_at`

Configurable options
- sync_tasks
- include_completed
- include_archived (best-effort; REST v2 tasks do not return completed items)
- emit_task_created/updated/completed/deleted
- task_state_created/updated/completed

Notes
- Todoist REST v2 `tasks` returns active tasks; completed tasks require another endpoint and are currently not fetched when `include_completed` is true (the flag is best-effort for future support).
- Subtasks are tasks with `parent_id` set; no separate toggle yet.
