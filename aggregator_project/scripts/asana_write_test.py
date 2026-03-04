from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import requests

BASE_URL = "https://app.asana.com/api/1.0"


def load_dotenv() -> None:
    dotenv_path = Path(__file__).resolve().parents[2] / ".env"
    if not dotenv_path.exists():
        return
    for line in dotenv_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("\"'"))


def require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        print(f"Missing required env var: {name}")
        sys.exit(1)
    return value


def create_task(token: str, workspace_gid: str, name: str, project_gid: str | None) -> dict:
    headers = {"Authorization": f"Bearer {token}"}
    data = {"name": name, "workspace": workspace_gid}
    if project_gid:
        data["projects"] = [project_gid]
    response = requests.post(
        f"{BASE_URL}/tasks",
        headers=headers,
        json={"data": data},
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def main() -> None:
    load_dotenv()

    parser = argparse.ArgumentParser(description="Create a test Asana task.")
    parser.add_argument("--name", default="Aggregator Asana write test", help="Task name")
    parser.add_argument("--confirm", action="store_true", help="Actually create the task")
    args = parser.parse_args()

    token = os.environ.get("ASANA_PERSONAL_ACCESS_TOKEN") or os.environ.get("ASANA_ACCESS_TOKEN")
    if not token:
        print("Missing ASANA_PERSONAL_ACCESS_TOKEN or ASANA_ACCESS_TOKEN")
        sys.exit(1)
    workspace_gid = require_env("ASANA_WORKSPACE_GID")
    project_gid = os.environ.get("ASANA_PROJECT_GID")

    if not args.confirm:
        print("Dry run. Use --confirm to create a task.")
        print(f"Would create task '{args.name}' in workspace {workspace_gid}")
        if project_gid:
            print(f"Project: {project_gid}")
        return

    payload = create_task(token, workspace_gid, args.name, project_gid)
    task = payload.get("data", {})
    print(f"Created task: {task.get('gid')} - {task.get('name')}")


if __name__ == "__main__":
    main()
