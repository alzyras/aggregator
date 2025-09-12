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

### Toggl Plugin

The Toggl plugin fetches time tracking data from the Toggl API. To use it:

1. Obtain a Toggl API token from your Toggl account settings
2. Find your workspace ID in Toggl (you can see it in the URL when viewing your workspace)
3. Set the following environment variables in your `.env` file:
   - `TOGGL_API_TOKEN=your_api_token_here`
   - `TOGGL_WORKSPACE_ID=your_workspace_id_here`
4. Add `toggl` to the `ENABLED_PLUGINS` list

### Samsung Health Plugin

The Samsung Health plugin collects health and fitness data directly from Samsung Health through the Google Fit REST API. This allows you to retrieve health data from any PC without needing an Android device or ADB.

#### How It Works:

Samsung Health data is automatically synchronized with Google Fit when you use a Samsung device with Samsung Health app. The plugin uses the Google Fit REST API to access this data directly from your PC.

#### Setup Instructions:

1. **Google API Console Setup**:
   - Go to the [Google API Console](https://console.developers.google.com/)
   - Create a new project or select an existing one
   - Enable the Fitness API
   - Create OAuth 2.0 credentials (Client ID and Client Secret)
   - Make sure to add `http://localhost:8080/oauth2callback` as an authorized redirect URI

2. **Configuration**:
   Set the following environment variables in your `.env` file:
   - `GOOGLE_FIT_CLIENT_ID=your_google_fit_client_id`
   - `GOOGLE_FIT_CLIENT_SECRET=your_google_fit_client_secret`
   - `GOOGLE_FIT_REFRESH_TOKEN=your_google_fit_refresh_token` (optional - will be generated automatically)

3. **Enable the Plugin**:
   Add `samsung_health` to the `ENABLED_PLUGINS` list

#### How the OAuth Flow Works:

1. **Automatic OAuth Flow**: If no refresh token is provided, the plugin will automatically start the OAuth flow
2. **Browser Authorization**: The plugin will open your browser and ask you to authorize access to your Google Fit data
3. **Automatic Token Generation**: After authorization, the plugin will automatically obtain both access and refresh tokens
4. **Token Storage**: The refresh token will be displayed in the console - you should save it in your `.env` file for future use

#### First-Time Setup:

When you run the plugin for the first time:
1. Make sure you've set up your `GOOGLE_FIT_CLIENT_ID` and `GOOGLE_FIT_CLIENT_SECRET`
2. Leave `GOOGLE_FIT_REFRESH_TOKEN` empty or commented out
3. Run the application
4. Your browser will open automatically to the Google authorization page
5. Sign in with your Google account and grant the requested permissions
6. The plugin will automatically capture the refresh token and display it in the console
7. Copy the refresh token and add it to your `.env` file

#### Data Types Collected:

- **Steps**: Step count, distance, calories burned, speed
- **Heart Rate**: BPM readings
- **Sleep**: Sleep duration, sleep stages (deep, light, REM, awake)
- **Workouts**: Exercise type, duration
- **General Health**: Weight and other health metrics

#### Important Notes:

- Samsung Health data must be synchronized with Google Fit on your Samsung device
- This synchronization happens automatically when you use Samsung Health
- The plugin can only access data that has been synchronized to Google Fit
- Some Samsung-specific health metrics may not be available through Google Fit
- Data is retrieved from the last 7 days to avoid overwhelming the API

#### Security:

- All API requests use HTTPS
- Access tokens are obtained automatically and have limited lifetime
- Refresh tokens are stored securely in environment variables
- No personal health data is stored in the code or configuration files
- The OAuth flow uses localhost callback for security

### Example Plugin Structure

```
uv_app/plugins/example/
├── __init__.py
├── plugin.py              # Implements PluginInterface
├── data_fetcher.py        # Handles data fetching logic
└── database_handler.py    # Handles database operations
```