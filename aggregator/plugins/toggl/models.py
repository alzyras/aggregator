from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class TogglEntry:
    id: int
    user_id: Optional[int]
    user_name: Optional[str]
    project_id: Optional[int]
    project_name: Optional[str]
    client_id: Optional[int]
    client_name: Optional[str]
    description: Optional[str]
    start_time: Optional[datetime]
    end_time: Optional[datetime]
    duration_minutes: Optional[float]
    tags: Optional[str]
    billable: Optional[bool]
    created_at: Optional[datetime]
