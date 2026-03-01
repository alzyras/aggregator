# Todoist → Event Mapping

- task_created: `added_at`/`created_at` once.
- task_updated: only when meaningful fields change (content/title, description, completed, due, priority, labels, is_deleted/is_archived, parent/section/project).
- task_completed: when completed and `completed_at` exists.
- task_deleted: when `is_deleted` true.
- Task_state snapshots per toggles; completed snapshot skipped if same timestamp as completion occurrence.

Arbitration rule per timestamp per task: `task_completed` > `task_updated` > `task_created` > `task_state`; lower-priority at that timestamp is dropped.
