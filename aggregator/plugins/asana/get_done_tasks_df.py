import json
import os
import logging
from datetime import datetime

import pandas as pd
import requests


# Create a logger for this module
logger = logging.getLogger(__name__)


def get_personal_access_token():
    """Get personal access token from environment variable."""
    return os.environ.get("ASANA_PERSONAL_ACCESS_TOKEN")


def get_workspace_info(access_token, workspace_gid):
    """Gets workspace information by its GID."""
    url = f"https://app.asana.com/api/1.0/workspaces/{workspace_gid}"
    headers = {"Authorization": f"Bearer {access_token}"}
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        return response.json()["data"]
    except requests.exceptions.RequestException as e:
        logger.error(f"Error getting workspace info: {e}")
        return {"gid": workspace_gid, "name": "Unknown"}  # Return basic info if API call fails


def get_projects(access_token, workspace_gid):
    """Gets all projects in a workspace."""
    url = f"https://app.asana.com/api/1.0/workspaces/{workspace_gid}/projects"
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {"opt_fields": "name, created_at, modified_at, notes, owner.name"}
    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        projects = response.json()["data"]
        logger.info("Asana: found %s projects in workspace %s", len(projects), workspace_gid)
        return projects
    except requests.exceptions.RequestException as e:
        logger.error(f"Error getting projects: {e}")
        return []


def get_completed_tasks(access_token, project_gid, days_to_fetch=548):
    """Gets all completed tasks in a project."""
    # Calculate the date from which to fetch tasks
    from datetime import datetime, timedelta, timezone
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_to_fetch)
    cutoff_date_str = cutoff_date.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    
    url = f"https://app.asana.com/api/1.0/projects/{project_gid}/tasks"
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {
        "completed_since": cutoff_date_str,
        "opt_fields": "name,completed_at,assignee.name,assignee.email,completed,created_at, notes, created_by.name, created_by.email, subtasks",
        "limit": 100,
    }
    tasks = []
    next_page = None
    try:
        while True:
            if next_page:
                params["offset"] = next_page
            response = requests.get(url, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()["data"]
            for task in data:
                if task.get("completed"):
                    tasks.append(task)
            if "next_page" in response.json() and response.json()["next_page"]:
                next_page = response.json()["next_page"]["offset"]
            else:
                break
        logger.info("Asana: project %s completed tasks fetched (%s)", project_gid, len(tasks))
        return tasks
    except requests.exceptions.RequestException as e:
        logger.error(f"Error getting completed tasks: {e}")
        return []


def get_completed_subtasks(access_token, task_gid):
    """Gets all completed subtasks of a task."""
    url = f"https://app.asana.com/api/1.0/tasks/{task_gid}/subtasks"
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {
        "completed_since": "1970-01-01T00:00:00.000Z",
        "opt_fields": "name,completed_at,assignee.name,assignee.email,completed,created_at, notes, created_by.name, created_by.email",
        "limit": 100,
    }
    tasks = []
    next_page = None
    try:
        while True:
            if next_page:
                params["offset"] = next_page
            response = requests.get(url, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()["data"]
            for task in data:
                if task.get("completed"):
                    tasks.append(task)
            if "next_page" in response.json() and response.json()["next_page"]:
                next_page = response.json()["next_page"]["offset"]
            else:
                break
        logger.info("Asana: task %s completed subtasks fetched (%s)", task_gid, len(tasks))
        return tasks
    except requests.exceptions.RequestException as e:
        logger.error(f"Error getting completed subtasks: {e}")
        return []


def process_tasks_to_dataframe(access_token, workspace_gid, days_to_fetch=548):
    """Processes tasks and subtasks into a DataFrame."""
    # Get workspace information (ID and name)
    workspace_info = get_workspace_info(access_token, workspace_gid)
    workspace_name = workspace_info.get("name", "Unknown Workspace")
    workspace_id = workspace_info.get("gid", workspace_gid)
    
    projects = get_projects(access_token, workspace_gid)
    logger.info("Asana: starting fetch across %s projects", len(projects))
    all_tasks = []
    for project in projects:
        project_gid = project["gid"]
        project_name = project["name"]
        tasks = get_completed_tasks(access_token, project_gid, days_to_fetch)
        for task in tasks:
            completed_at = (
                datetime.fromisoformat(task["completed_at"].replace("Z", "+00:00"))
                if task.get("completed_at")
                else None
            )
            created_at = (
                datetime.fromisoformat(task["created_at"].replace("Z", "+00:00"))
                if task.get("created_at")
                else None
            )
            time_to_completion = (
                (completed_at - created_at).total_seconds()
                if completed_at and created_at
                else None
            )
            all_tasks.append(
                {
                    "task_id": task["gid"],
                    "task_name": task["name"],
                    "time_to_completion": time_to_completion,
                    "project": project_name,
                    "workspace_id": workspace_id,
                    "workspace_name": workspace_name,
                    "project_created_at": project.get("created_at"),
                    "project_notes": project.get("notes"),
                    "project_owner": project.get("owner", {}).get("name"),
                    "completed_by_name": task["assignee"]["name"]
                    if task.get("assignee")
                    else None,
                    "completed_by_email": task["assignee"]["email"]
                    if task.get("assignee")
                    else None,
                    "completed": task.get("completed"),
                    "task_description": task.get("notes"),
                    "date": completed_at,
                    "created_by_name": task.get("created_by", {}).get("name"),
                    "created_by_email": task.get("created_by", {}).get("email"),
                    "type": "task",
                }
            )
            if task.get("subtasks"):
                subtasks = get_completed_subtasks(access_token, task["gid"])
                for subtask in subtasks:
                    completed_at_sub = (
                        datetime.fromisoformat(
                            subtask["completed_at"].replace("Z", "+00:00")
                        )
                        if subtask.get("completed_at")
                        else None
                    )
                    created_at_sub = (
                        datetime.fromisoformat(
                            subtask["created_at"].replace("Z", "+00:00")
                        )
                        if subtask.get("created_at")
                        else None
                    )
                    time_to_completion_sub = (
                        (completed_at_sub - created_at_sub).total_seconds()
                        if completed_at_sub and created_at_sub
                        else None
                    )
                    all_tasks.append(
                        {
                            "task_id": subtask["gid"],
                            "task_name": subtask["name"],
                            "time_to_completion": time_to_completion_sub,
                            "project": project_name,
                            "workspace_id": workspace_id,
                            "workspace_name": workspace_name,
                            "project_created_at": project.get("created_at"),
                            "project_notes": project.get("notes"),
                            "project_owner": project.get("owner", {}).get("name"),
                            "completed_by_name": subtask["assignee"]["name"]
                            if subtask.get("assignee")
                            else None,
                            "completed_by_email": subtask["assignee"]["email"]
                            if subtask.get("assignee")
                            else None,
                            "completed": subtask.get("completed"),
                            "task_description": subtask.get("notes"),
                            "date": completed_at_sub,
                            "created_by_name": subtask.get("created_by", {}).get(
                                "name"
                            ),
                            "created_by_email": subtask.get("created_by", {}).get(
                                "email"
                            ),
                            "type": "subtask",
                        }
                    )
    return pd.DataFrame(all_tasks)


def get_asana_completed_tasks_df(personal_access_token, workspace_gid, days_to_fetch=548):
    """
    Gets the Asana completed tasks dataframe using a personal access token.
    
    This is simpler than OAuth and doesn't require browser authentication.
    """
    if not personal_access_token:
        logger.error("No personal access token found. Please set ASANA_PERSONAL_ACCESS_TOKEN in your environment.")
        return None

    try:
        df = process_tasks_to_dataframe(personal_access_token, workspace_gid, days_to_fetch)
        return df
    except Exception as e:
        logger.error(f"Error fetching Asana data: {e}")
        return None


def get_df(client_id, client_secret, workspace_gid):
    """
    Public function to get Asana data using personal access token.
    
    Args:
        client_id (str): Not used anymore, kept for compatibility
        client_secret (str): Not used anymore, kept for compatibility
        workspace_gid (str): Asana workspace GID
        
    Returns:
        pandas.DataFrame: DataFrame with completed tasks data
    """
    personal_access_token = get_personal_access_token()
    df = get_asana_completed_tasks_df(personal_access_token, workspace_gid)
    return df
