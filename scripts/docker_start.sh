#!/usr/bin/env bash
set -euo pipefail

APP_ROLE="${APP_ROLE:-web}"
APP_PORT="${APP_PORT:-8000}"
WAIT_FOR_DB="${WAIT_FOR_DB:-1}"
DB_WAIT_TIMEOUT="${DB_WAIT_TIMEOUT:-120}"
DB_WAIT_INTERVAL="${DB_WAIT_INTERVAL:-2}"
AUTO_CREATE_DB="${AUTO_CREATE_DB:-0}"
RUN_MIGRATIONS="${RUN_MIGRATIONS:-1}"
COLLECT_STATIC="${COLLECT_STATIC:-1}"
WORKER_POLL_SECONDS="${WORKER_POLL_SECONDS:-5}"
GUNICORN_WORKERS="${GUNICORN_WORKERS:-2}"
GUNICORN_TIMEOUT="${GUNICORN_TIMEOUT:-120}"
VALIDATE_RUNTIME_CONFIG="${VALIDATE_RUNTIME_CONFIG:-1}"
STRICT_RUNTIME_CONFIG="${STRICT_RUNTIME_CONFIG:-0}"

if [ -z "${ENCRYPTION_KEY:-}" ]; then
  echo "ENCRYPTION_KEY is required. Set it in your environment or .env."
  exit 1
fi

if [ "${VALIDATE_RUNTIME_CONFIG}" = "1" ]; then
  if [ "${STRICT_RUNTIME_CONFIG}" = "1" ]; then
    python aggregator_project/manage.py validate_runtime_config --strict
  else
    python aggregator_project/manage.py validate_runtime_config
  fi
fi

if [ "${AUTO_CREATE_DB}" = "1" ]; then
  python aggregator_project/manage.py ensure_db
fi

if [ "${WAIT_FOR_DB}" = "1" ]; then
  python aggregator_project/manage.py wait_for_db --timeout "${DB_WAIT_TIMEOUT}" --interval "${DB_WAIT_INTERVAL}"
fi

if [ "${RUN_MIGRATIONS}" = "1" ]; then
  python aggregator_project/manage.py migrate --noinput
fi

if [ "${COLLECT_STATIC}" = "1" ]; then
  python aggregator_project/manage.py collectstatic --noinput
fi

if [ -n "${DJANGO_SUPERUSER_USERNAME:-}" ] && [ -n "${DJANGO_SUPERUSER_PASSWORD:-}" ] && [ -n "${DJANGO_SUPERUSER_EMAIL:-}" ]; then
  python aggregator_project/manage.py createsuperuser --noinput || true
fi

if [ "${APP_ROLE}" = "worker" ]; then
  exec python aggregator_project/manage.py run_worker --poll-seconds "${WORKER_POLL_SECONDS}"
fi

exec gunicorn \
  --chdir /app/aggregator_project \
  --bind "0.0.0.0:${APP_PORT}" \
  --workers "${GUNICORN_WORKERS}" \
  --timeout "${GUNICORN_TIMEOUT}" \
  aggregator_project.wsgi:application
