## Wellness Statistics Application

A modular application for collecting and storing wellness statistics from various sources.

## Features

- **Modular Design**: Enable/disable data sources as needed
- **Asana Integration**: Collect completed tasks and subtasks from Asana
- **Habitica Integration**: Collect completed habits, dailies, and todos from Habitica
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
- `ENABLED_PLUGINS`: Comma-separated list of plugins to enable (e.g., `asana,habitica`)

### Asana Settings (required if asana is in ENABLED_PLUGINS)
- `ASANA_PERSONAL_ACCESS_TOKEN`: Asana personal access token (preferred method)
- `ASANA_WORKSPACE_GID`: Asana workspace GID

### Habitica Settings (required if habitica is in ENABLED_PLUGINS)
- `HABITICA_USER_ID`: Habitica user ID
- `HABITICA_API_TOKEN`: Habitica API token

## Initial Setup

### Asana Setup
1. Go to Asana > My Profile Settings > Apps > Manage Developer Apps
2. Click "+ Create new personal access token"
3. Give it a name (e.g., "Wellness Statistics")
4. Copy the token and add it to your `.env` file as `ASANA_PERSONAL_ACCESS_TOKEN`

### Habitica Setup
1. Get your Habitica User ID and API Token from Habitica settings
2. Add them to your `.env` file

## Usage

Run the application with:

```bash
uv run python uv_app/run_all.py
```

## Database Schema

The application creates two tables:

1. `asana_items`: Stores completed Asana tasks and subtasks
2. `habitica_items`: Stores completed Habitica items (habits, dailies, todos)

## Modules

### Asana Module
Collects completed tasks and subtasks from Asana projects. Uses personal access tokens for simple authentication.

### Habitica Module
Collects completed habits, dailies, and todos from Habitica.

## Development

### Adding New Modules

1. Create a new directory in `uv_app/plugins/` for your module
2. Implement the `PluginInterface` in `uv_app/plugins/your_plugin/plugin.py`
3. Add configuration variables to `uv_app/config/config.py`
4. Update `uv_app/plugin_manager.py` to load your plugin
5. Add any required SQL schema files to `uv_app/plugins/your_plugin/sql/`

### Example Plugin Structure

```
uv_app/plugins/example/
├── __init__.py
├── plugin.py              # Implements PluginInterface
├── data_fetcher.py        # Handles data fetching logic
└── database_handler.py    # Handles database operations
```