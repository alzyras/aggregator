import datetime
import io
import os

import pandas as pd
import requests

X_CLIENT_HEADER = "aggregator"


def fetch_tags(user_id, api_token):
    """Fetch all tags associated with the user."""
    url = "https://habitica.com/api/v3/tags"
    headers = {
        "x-api-user": user_id,
        "x-api-key": api_token,
        "x-client": X_CLIENT_HEADER,
    }
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        tags = response.json().get("data", [])
        tag_dict = {tag["id"]: tag["name"] for tag in tags}
        return tag_dict
    print(f"❌ Failed to fetch tags. Status code: {response.status_code}")
    return {}


def get_completed_todos(user_id, api_token, tag_dict):
    base_url = "https://habitica.com/api/v3/tasks/user"
    headers = {
        "x-api-user": user_id,
        "x-api-key": api_token,
        "x-client": X_CLIENT_HEADER,
    }

    params = {
        "type": "completedTodos",
    }

    response = requests.get(base_url, headers=headers, params=params)

    if response.status_code == 200:
        tasks = response.json().get("data", [])
        completed_todos = []
        for task in tasks:
            date_created_str = task.get("createdAt")
            date_completed_str = task.get("dateCompleted")

            date_created = None
            if date_created_str:
                try:
                    date_created = datetime.datetime.fromisoformat(
                        date_created_str.replace("Z", "+00:00")
                    ).strftime("%Y-%m-%d %H:%M:%S")
                except ValueError:
                    print(f"Error parsing date_created: {date_created_str}")

            date_completed = None
            if date_completed_str:
                try:
                    date_completed = datetime.datetime.fromisoformat(
                        date_completed_str.replace("Z", "+00:00")
                    ).strftime("%Y-%m-%d %H:%M:%S")
                except ValueError:
                    print(f"Error parsing date_completed: {date_completed_str}")

            tag_names = [
                tag_dict.get(tag_id, tag_id) for tag_id in task.get("tags", [])
            ]

            completed_todos.append(
                {
                    "item_id": task["id"],
                    "item_name": task.get("text", "Unknown Task"),
                    "item_type": task["type"],
                    "value": task.get("value", 0),
                    "date_created": date_created,
                    "date_completed": date_completed,
                    "notes": task.get("notes", ""),
                    "priority": task.get("priority", "Unknown"),
                    "tags": ", ".join(tag_names),
                    "completed": True,
                }
            )
        return completed_todos
    print(f"Failed to fetch tasks. Status code: {response.status_code}")
    return []


def create_dataframe(data, filename="habitica_completed_todos.csv"):
    if not data:
        return None

    df = pd.DataFrame(data)

    # Save DataFrame to CSV in memory (optional)
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_content = csv_buffer.getvalue()

    return df


def main(days_to_fetch=548):
    USER_ID = os.environ.get("HABITICA_USER_ID")
    API_TOKEN = os.environ.get("HABITICA_API_TOKEN")
    tag_dict = fetch_tags(USER_ID, API_TOKEN)
    task_data = get_completed_todos(USER_ID, API_TOKEN, tag_dict)

    if task_data:
        df = create_dataframe(task_data)
        return df


if __name__ == "__main__":
    main()
