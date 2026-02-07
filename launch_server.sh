#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_ROOT"

if ! command -v uv >/dev/null 2>&1; then
  echo "uv is required but not installed. Install uv first: https://docs.astral.sh/uv/"
  exit 1
fi

if [ ! -d ".venv" ]; then
  uv venv .venv
fi

# shellcheck disable=SC1091
source .venv/bin/activate

uv pip install -r requirements.txt

if [ ! -f ".env" ]; then
  cp .env.example .env
fi

set -a
# shellcheck disable=SC1091
source .env
set +a

.venv/bin/python aggregator_project/manage.py ensure_db
.venv/bin/python aggregator_project/manage.py migrate

if [ -n "${DJANGO_SUPERUSER_USERNAME:-}" ] && [ -n "${DJANGO_SUPERUSER_PASSWORD:-}" ] && [ -n "${DJANGO_SUPERUSER_EMAIL:-}" ]; then
  .venv/bin/python aggregator_project/manage.py createsuperuser --noinput || true
fi

.venv/bin/python aggregator_project/manage.py runserver 0.0.0.0:8000
