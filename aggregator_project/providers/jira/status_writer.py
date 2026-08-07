from __future__ import annotations

from ingestion.providers import (
    DescriptionWritebackResult,
    STATUS_WRITEBACK_FAILED,
    STATUS_WRITEBACK_SUCCESS,
    StatusWritebackResult,
)
from planner.models import PlannerItemState

from providers.jira.client import JiraClient

TARGET_CATEGORY_KEYS = {
    PlannerItemState.PLANNER_STATUS_INBOX: {"new", "todo"},
    PlannerItemState.PLANNER_STATUS_BACKLOG: {"new", "todo"},
    PlannerItemState.PLANNER_STATUS_DOING: {"indeterminate", "in_progress"},
    PlannerItemState.PLANNER_STATUS_DONE: {"done"},
}


class JiraStatusWriter:
    def __init__(self, account) -> None:
        self.client = JiraClient(account)

    def apply_planner_status(
        self,
        *,
        source_entity_id: str,
        planner_status: str,
        item=None,
        source_entity_type: str | None = None,
    ) -> StatusWritebackResult:
        target_keys = TARGET_CATEGORY_KEYS.get(planner_status)
        if not target_keys:
            return StatusWritebackResult(
                status=STATUS_WRITEBACK_FAILED,
                message=f"Unsupported planner status for Jira: {planner_status}",
            )

        transitions = self.client.get_issue_transitions(source_entity_id)
        transition = _choose_transition(transitions, target_keys)
        if not transition:
            return StatusWritebackResult(
                status=STATUS_WRITEBACK_FAILED,
                message="No valid Jira transition matches this planner status.",
            )

        self.client.transition_issue(source_entity_id, str(transition["id"]))
        target = transition.get("to") or {}
        category = (target.get("statusCategory") or {}).get("key")
        return StatusWritebackResult(
            status=STATUS_WRITEBACK_SUCCESS,
            source_status=target.get("name"),
            external_completed=category == "done",
            message="Saved to Jira.",
            raw=transition,
        )

    def update_description(
        self,
        *,
        source_entity_id: str,
        description: str,
        item=None,
        source_entity_type: str | None = None,
    ) -> DescriptionWritebackResult:
        data = self.client.update_issue_description(source_entity_id, description)
        return DescriptionWritebackResult(
            status=STATUS_WRITEBACK_SUCCESS,
            description=description,
            message="Description saved to Jira.",
            raw=data,
        )


def _choose_transition(transitions: list[dict], target_category_keys: set[str]) -> dict | None:
    for transition in transitions:
        target = transition.get("to") or {}
        category = target.get("statusCategory") or {}
        if str(category.get("key") or "").lower() in target_category_keys:
            return transition
    return None
