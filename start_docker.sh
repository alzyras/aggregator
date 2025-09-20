#!/bin/bash

# Script to start the aggregator using docker-compose
# This script reads environment variables from .env file and uses them with docker-compose

set -e

echo "Starting aggregator..."

# Check if .env file exists
if [ ! -f ".env" ]; then
    echo "Error: .env file not found!"
    echo "Please create a .env file based on .env.example"
    exit 1
fi

# Run docker-compose up with environment variables from .env
echo "Starting docker containers..."
docker-compose --env-file .env up -d

echo "Aggregator started successfully!"
echo "Check logs with: docker-compose logs -f"