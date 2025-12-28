from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class AsanaTask:
    task_id: str
    task_name: str
    project: str
    workspace_id: str
    workspace_name: str
    project_created_at: Optional[datetime]
    project_notes: Optional[str]
    project_owner: Optional[str]
    completed_by_name: Optional[str]
    completed_by_email: Optional[str]
    completed: bool
    task_description: Optional[str]
    date: Optional[datetime]
    created_by_name: Optional[str]
    created_by_email: Optional[str]
    type: str
    time_to_completion: Optional[float] = None
