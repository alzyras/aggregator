from __future__ import annotations

from typing import Any

from ingestion.providers import (
    STATUS_WRITEBACK_NOOP,
    STATUS_WRITEBACK_SUCCESS,
    STATUS_WRITEBACK_UNSUPPORTED,
    DescriptionWritebackResult,
    StatusWritebackResult,
)
from planner.models import PlannerItemState
from providers.linear.api import LinearAPI


ISSUE_WORKFLOW_QUERY = """
query IssueWorkflow($id: String!) {
  issue(id: $id) {
    id
    state { id name type }
    team { id states { nodes { id name type position } } }
  }
}
"""

ISSUE_UPDATE_MUTATION = """
mutation IssueUpdate($id: String!, $input: IssueUpdateInput!) {
  issueUpdate(id: $id, input: $input) {
    success
    issue { id description state { id name type } }
  }
}
"""


class LinearStatusWriter:
    def __init__(self, account) -> None:
        self.token = account.get_access_token()
        self.api = LinearAPI(self.token)

    def apply_planner_status(
        self,
        *,
        source_entity_id: str,
        planner_status: str,
        item=None,
        source_entity_type: str | None = None,
    ) -> StatusWritebackResult:
        if not self.token:
            raise RuntimeError("Missing Linear API key.")
        issue = (
            self.api.request(ISSUE_WORKFLOW_QUERY, {"id": source_entity_id}).get(
                "issue"
            )
            or {}
        )
        current_state = issue.get("state") or {}
        states = ((issue.get("team") or {}).get("states") or {}).get("nodes") or []
        target_types = _target_state_types(planner_status)
        target = _choose_state(states, target_types)
        if not target:
            return StatusWritebackResult(
                status=STATUS_WRITEBACK_UNSUPPORTED,
                source_status=str(current_state.get("name") or ""),
                external_completed=(current_state.get("type") == "completed"),
                message="No matching Linear workflow state is available.",
            )
        if current_state.get("id") == target.get("id"):
            return StatusWritebackResult(
                status=STATUS_WRITEBACK_NOOP,
                source_status=str(target.get("name") or target.get("type") or ""),
                external_completed=target.get("type") == "completed",
                message="Linear is already in this state.",
            )
        payload = self.api.request(
            ISSUE_UPDATE_MUTATION,
            {"id": source_entity_id, "input": {"stateId": target["id"]}},
        )
        result = payload.get("issueUpdate") or {}
        if not result.get("success"):
            raise RuntimeError("Linear did not update the issue state.")
        updated = result.get("issue") or {}
        updated_state = updated.get("state") or target
        return StatusWritebackResult(
            status=STATUS_WRITEBACK_SUCCESS,
            source_status=str(
                updated_state.get("name") or updated_state.get("type") or ""
            ),
            external_completed=updated_state.get("type") == "completed",
            message="Saved to Linear.",
            raw=updated,
        )

    def update_description(
        self,
        *,
        source_entity_id: str,
        description: str,
        item=None,
        source_entity_type: str | None = None,
    ) -> DescriptionWritebackResult:
        if not self.token:
            raise RuntimeError("Missing Linear API key.")
        payload = self.api.request(
            ISSUE_UPDATE_MUTATION,
            {"id": source_entity_id, "input": {"description": description}},
        )
        result = payload.get("issueUpdate") or {}
        if not result.get("success"):
            raise RuntimeError("Linear did not update the issue description.")
        issue = result.get("issue") or {}
        return DescriptionWritebackResult(
            status=STATUS_WRITEBACK_SUCCESS,
            description=str(
                issue.get("description")
                if issue.get("description") is not None
                else description
            ),
            message="Description saved to Linear.",
            raw=issue,
        )


def _target_state_types(planner_status: str) -> tuple[str, ...]:
    if planner_status == PlannerItemState.PLANNER_STATUS_DONE:
        return ("completed",)
    if planner_status == PlannerItemState.PLANNER_STATUS_DOING:
        return ("started",)
    if planner_status == PlannerItemState.PLANNER_STATUS_INBOX:
        return ("backlog", "triage", "unstarted")
    return ("unstarted", "backlog")


def _choose_state(states: list[dict[str, Any]], state_types: tuple[str, ...]):
    for state_type in state_types:
        candidates = [state for state in states if state.get("type") == state_type]
        if candidates:
            return min(candidates, key=lambda state: float(state.get("position") or 0))
    return None
