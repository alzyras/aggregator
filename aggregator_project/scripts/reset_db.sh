#!/usr/bin/env bash
set -euo pipefail

PGHOST=${PGHOST:-portainer.tailc8ebe2.ts.net}
PGUSER=${PGUSER:-appuser}
PGDATABASE=${PGDATABASE:-aggregator}

psql -h "$PGHOST" -U "$PGUSER" -c "DROP DATABASE IF EXISTS \"$PGDATABASE\";"
psql -h "$PGHOST" -U "$PGUSER" -c "CREATE DATABASE \"$PGDATABASE\";"
uv run python manage.py migrate
