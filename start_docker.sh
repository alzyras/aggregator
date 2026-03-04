#!/bin/bash

set -e

echo "Starting aggregator..."

# Check if .env file exists
if [ ! -f ".env" ]; then
    echo "Error: .env file not found!"
    echo "Please create a .env file based on .env.example"
    exit 1
fi

# Run Docker Compose stack with environment variables from .env
echo "Starting docker containers..."
docker compose --env-file .env up -d --build

echo "Aggregator started successfully!"
echo "Check logs with: docker compose logs -f"
