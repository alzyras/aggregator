---
title: Aggregator - Track Productivity and Health Data
description: Modular application for collecting data from Asana, Habitica, Toggl, and Google Fit. Store data in MySQL database for analysis.
keywords: wellness, productivity, health tracking, asana, habitica, toggl, google fit, data collection, mysql
---

# Aggregator Application

A modular application for collecting and storing data from various sources.

## Quick Start

### Option 1: Using UV (Recommended)

```bash
# Install dependencies
uv sync

# Copy and configure environment variables
cp .env.example .env
# Edit .env with your credentials

# Run the application
uv run python aggregator/run_all.py
```

### Option 2: Using Docker

```bash
# Copy and configure environment variables
cp .env.example .env
# Edit .env with your credentials

# Start with Docker Compose using the provided script
./start_docker.sh
```

### Option 3: Direct Docker Compose

```bash
# Copy and configure environment variables
cp .env.example .env
# Edit .env with your credentials

# Start with Docker Compose
docker-compose up
```

## Features

- **Modular Design**: Enable/disable data sources as needed
- **Asana Integration**: Collect completed tasks and subtasks from Asana
- **Habitica Integration**: Collect completed habits, dailies, and todos from Habitica
- **Toggl Integration**: Track time spent on projects and tasks
- **Google Fit Integration**: Collect health and fitness data through Google Fit API
- **MySQL Storage**: Store collected data in a structured database

## Configuration

Create a `.env` file based on `.env.example` and configure your credentials.

## Plugin Documentation

For detailed information about each plugin, including configuration requirements and database schemas, see [Plugin Documentation](README_PLUGINS.md).