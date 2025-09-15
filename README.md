---
title: Wellness Statistics - Track Productivity and Health Data
description: Modular application for collecting wellness statistics from Asana, Habitica, Toggl, and Google Fit. Store data in MySQL database for analysis.
keywords: wellness, productivity, health tracking, asana, habitica, toggl, google fit, data collection, mysql
---

# Wellness Statistics Application

A modular application for collecting and storing wellness statistics from various sources.

## Features

- **Modular Design**: Enable/disable data sources as needed
- **Asana Integration**: Collect completed tasks and subtasks from Asana
- **Habitica Integration**: Collect completed habits, dailies, and todos from Habitica
- **Toggl Integration**: Track time spent on projects and tasks
- **Google Fit Integration**: Collect health and fitness data through Google Fit API
- **MySQL Storage**: Store collected data in a structured database

## Installation

To create and install virtual environment:

```bash
uv sync
```

During development, you can lint and format code using:

```bash
uv run poe x
```

To export requirements.txt:
```bash
uv export --no-hashes --no-dev --format requirements-txt > requirements.txt
```

## Configuration

Create a `.env` file based on `.env.example` and configure the following variables:

### General Settings
- `INTERVAL_SECONDS`: How often to collect data (in seconds)

### MySQL Settings
- `MYSQL_HOST`: MySQL server hostname
- `MYSQL_DB`: Database name
- `MYSQL_USER`: Database username
- `MYSQL_PASSWORD`: Database password

### Plugin Enablement
- `ENABLED_PLUGINS`: Comma-separated list of plugins to enable (e.g., `google_fit,habitica`)

## Plugin Documentation

For detailed information about each plugin, including configuration requirements and database schemas, see [Plugin Documentation](README_PLUGINS.md).

## Usage

Run the application with:

```bash
uv run python aggregator/run_all.py
```

## Development

### Adding New Modules

1. Create a new directory in `aggregator/plugins/` for your module
2. Implement the `PluginInterface` in `aggregator/plugins/your_plugin/plugin.py`
3. Add configuration variables to `aggregator/config/config.py`
4. Update `aggregator/plugin_manager.py` to load your plugin
5. Add any required SQL schema files to `aggregator/plugins/your_plugin/sql/`

