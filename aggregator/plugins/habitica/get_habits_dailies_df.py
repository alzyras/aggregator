import os
from datetime import datetime

import pandas as pd
import requests

X_CLIENT_HEADER = "wellness_statistics"


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
    print(response.text)
    return {}


def fetch_completed_items(user_id, api_token, item_type, tag_dict):
    """Fetch completed tasks, habits, and dailies, including history items."""
    # Use the correct endpoint for all task types
    url = "https://habitica.com/api/v3/tasks/user"
    headers = {
        "x-api-user": user_id,
        "x-api-key": api_token,
        "x-client": X_CLIENT_HEADER,
    }
    
    # Set the correct type parameter for each item type
    type_param = item_type
    if item_type == "habit":
        type_param = "habits"
    elif item_type == "daily":
        type_param = "dailys"
    elif item_type == "todo":
        type_param = "todos"
    
    params = {
        "type": type_param,
    }
    
    response = requests.get(url, headers=headers, params=params)
    item_data = []

    if response.status_code == 200:
        items = response.json().get("data", [])
        for item in items:
            item_id = item["_id"]
            item_name = item["text"]
            item_type = item["type"]
            value = item.get("value", None)
            date_created = item.get("createdAt", None)
            notes = item.get("notes", None)
            priority = item.get("priority", None)
            tags = item.get("tags", [])
            tag_names = [tag_dict.get(tag, tag) for tag in tags]

            # For habits and dailies, check the history for completed items
            if item_type in ["habit", "daily"] and "history" in item:
                for history_item in item["history"]:
                    history_date = datetime.utcfromtimestamp(
                        history_item["date"] / 1000
                    ).strftime("%Y-%m-%d %H:%M:%S")

                    try:
                        if history_item["completed"] == False:
                            continue
                    except:
                        pass

                    item_data.append(
                        {
                            "item_id": item_id,
                            "item_name": item_name,
                            "item_type": item_type,
                            "value": history_item["value"],
                            "date_created": date_created,
                            "date_completed": history_date,
                            "notes": notes,
                            "priority": priority,
                            "tags": ", ".join(tag_names),
                            "completed": True,
                        }
                    )
            # For todos, check if they are completed
            elif item_type == "todo" and item.get("completed", False):
                item_data.append(
                    {
                        "item_id": item_id,
                        "item_name": item_name,
                        "item_type": item_type,
                        "value": value,
                        "date_created": date_created,
                        "date_completed": item.get("dateCompleted", None),
                        "notes": notes,
                        "priority": priority,
                        "tags": ", ".join(tag_names),
                        "completed": item.get("completed"),
                    }
                )
    else:
        print(f"❌ Failed to fetch {item_type}. Status code: {response.status_code}")

    return item_data


def fetch_all_data(user_id, api_token):
    """Fetch completed tasks, habits, and dailies and combine them into one DataFrame."""
    all_data = []
    tag_dict = fetch_tags(user_id, api_token)

    # Fetch all types of items using the correct endpoints
    habit_data = fetch_completed_items(user_id, api_token, "habit", tag_dict)
    daily_data = fetch_completed_items(user_id, api_token, "daily", tag_dict)
    todo_data = fetch_completed_items(user_id, api_token, "todo", tag_dict)

    all_data.extend(habit_data)
    all_data.extend(daily_data)
    all_data.extend(todo_data)

    df = pd.DataFrame(all_data)
    return df


def main(days_to_fetch=548):
    USER_ID = os.environ.get("HABITICA_USER_ID")
    API_TOKEN = os.environ.get("HABITICA_API_TOKEN")

    df_all_data = fetch_all_data(USER_ID, API_TOKEN)
    if not df_all_data.empty:
        df_all_data = df_all_data[
            [
                "item_id",
                "item_name",
                "item_type",
                "value",
                "date_created",
                "date_completed",
                "notes",
                "priority",
                "tags",
                "completed",
            ]
        ]

    return df_all_data


if __name__ == "__main__":
    main()
