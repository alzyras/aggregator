# Aggregator Plugins

This document provides detailed information about each plugin in the Aggregator application, including their data schemas and configuration requirements.

## Table of Contents
- [Asana Plugin](#asana-plugin)
- [Habitica Plugin](#habitica-plugin)
- [Toggl Plugin](#toggl-plugin)
- [Google Fit Plugin](#google-fit-plugin)

## Asana Plugin

Collects completed tasks and subtasks from Asana projects.

### Configuration
To enable the Asana plugin, add `asana` to the `ENABLED_PLUGINS` list in your `.env` file and set the following variables:

- `ASANA_ACCESS_TOKEN`: Asana personal access token (preferred method)

### Setup Instructions
1. Go to Asana > My Profile Settings > Apps > Manage Developer Apps
2. Click "+ Create new personal access token"
3. Give it a name (e.g., "Aggregator")
4. Copy the token and add it to your `.env` file as `ASANA_ACCESS_TOKEN`

### Data Schema

| Column | Type | Description |
|--------|------|-------------|
| task_id | VARCHAR(255) | Unique task identifier (Primary Key) |
| task_name | TEXT | Name of the task |
| time_to_completion | REAL | Time taken to complete the task |
| project | TEXT | Project the task belongs to |
| project_created_at | DATETIME | When the project was created |
| project_notes | TEXT | Notes about the project |
| project_owner | TEXT | Owner of the project |
| completed_by_name | TEXT | Name of who completed the task |
| completed_by_email | TEXT | Email of who completed the task |
| completed | BOOLEAN | Whether the task is completed |
| task_description | TEXT | Description of the task |
| date | DATETIME | Date of the task |
| created_by_name | TEXT | Name of who created the task |
| created_by_email | TEXT | Email of who created the task |
| type | VARCHAR(10) | Type of the item |

## Habitica Plugin

Collects completed habits, dailies, and todos from Habitica.

### Configuration
To enable the Habitica plugin, add `habitica` to the `ENABLED_PLUGINS` list in your `.env` file and set the following variables:

- `HABITICA_USER_ID`: Habitica user ID
- `HABITICA_API_TOKEN`: Habitica API token

### Setup Instructions
1. Get your Habitica User ID and API Token from Habitica settings
2. Add them to your `.env` file

### Data Schema

| Column | Type | Description |
|--------|------|-------------|
| item_id | VARCHAR(36) | Unique item identifier |
| item_name | VARCHAR(255) | Name of the item |
| item_type | VARCHAR(50) | Type of the item (habit, daily, todo) |
| value | DECIMAL(10, 8) | Value of the item |
| date_created | DATETIME | When the item was created |
| date_completed | DATETIME | When the item was completed |
| notes | TEXT | Notes about the item |
| priority | DECIMAL(3, 1) | Priority level of the item |
| tags | TEXT | Tags associated with the item |
| completed | BOOLEAN | Whether the item is completed |

## Toggl Plugin

Fetches time tracking data from the Toggl API.

### Configuration
To enable the Toggl plugin, add `toggl` to the `ENABLED_PLUGINS` list in your `.env` file and set the following variables:

- `TOGGL_API_TOKEN`: Toggl API token

### Setup Instructions
1. Obtain a Toggl API token from your Toggl account settings
2. Set the environment variable in your `.env` file

### Data Schema

```sql
CREATE TABLE IF NOT EXISTS toggl_items (
    id BIGINT PRIMARY KEY,
    user_id BIGINT,
    user_name VARCHAR(255),
    project_id BIGINT,
    project_name VARCHAR(255),
    client_id BIGINT,
    client_name VARCHAR(255),
    description TEXT,
    start_time DATETIME,
    end_time DATETIME,
    duration_minutes DECIMAL(10, 2),
    tags TEXT,
    billable BOOLEAN,
    created_at DATETIME,
    INDEX idx_user_id (user_id),
    INDEX idx_project_id (project_id),
    INDEX idx_start_time (start_time),
    INDEX idx_end_time (end_time)
);
```

## Google Fit Plugin

Collects health and fitness data directly from Google Fit.

### Configuration
To enable the Google Fit plugin, add `google_fit` to the `ENABLED_PLUGINS` list in your `.env` file and set the following variables:

- `GOOGLE_FIT_CLIENT_ID`: Google Fit client ID
- `GOOGLE_FIT_CLIENT_SECRET`: Google Fit client secret

### How It Works
The plugin uses the Google Fit REST API to access health and fitness data directly from your Google account.

### Setup Instructions
1. **Google API Console Setup**:
   - Go to the [Google API Console](https://console.developers.google.com/)
   - Create a new project or select an existing one
   - Enable the Fitness API
   - Create OAuth 2.0 credentials (Client ID and Client Secret)
   - Make sure to add `http://localhost:8080/oauth2callback` as an authorized redirect URI

2. Set the environment variables in your `.env` file

3. **OAuth Flow**:
   - The plugin will automatically start the OAuth flow when run for the first time
   - The plugin will open your browser and ask you to authorize access to your Google Fit data
   - After authorization, the plugin will automatically obtain both access and refresh tokens
   - Tokens will be stored in `aggregator/plugins/google_fit/data/google_fit_tokens.json`

### Data Types Collected
- **Steps**: Daily step counts
- **Heart Rate**: Hourly heart rate readings
- **General Health**: Weight, height, and body fat percentage

### Steps Data Schema

| Column | Type | Description |
|--------|------|-------------|
| id | VARCHAR(255) | Unique identifier (Primary Key) |
| user_id | VARCHAR(255) | ID of the user |
| timestamp | DATETIME | Date of the steps (time component is 00:00:00) |
| steps | INT | Number of steps |
| created_at | DATETIME | When the record was created (default: CURRENT_TIMESTAMP) |
| updated_at | DATETIME | When the record was last updated (default: CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP) |

### Indexes
- idx_user_timestamp (user_id, timestamp)
- idx_timestamp (timestamp)
- uniq_user_date (user_id, timestamp) - Unique

### Heart Rate Data Schema

| Column | Type | Description |
|--------|------|-------------|
| id | VARCHAR(255) | Unique identifier (Primary Key) |
| user_id | VARCHAR(255) | ID of the user |
| timestamp | DATETIME | When the data was recorded |
| heart_rate | DECIMAL(5, 2) | Heart rate in BPM |
| created_at | DATETIME | When the record was created (default: CURRENT_TIMESTAMP) |
| updated_at | DATETIME | When the record was last updated (default: CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP) |

### Indexes
- idx_user_timestamp (user_id, timestamp)
- idx_timestamp (timestamp)
- uniq_user_hour (user_id, timestamp) - Unique

### General Health Data Schema

| Column | Type | Description |
|--------|------|-------------|
| id | VARCHAR(255) | Unique identifier (Primary Key) |
| user_id | VARCHAR(255) | ID of the user |
| date | DATE | Date of the measurement |
| data_type | VARCHAR(50) | Type of data (weight, height, body_fat_percentage) |
| value | DECIMAL(10, 2) | Measurement value |
| unit | VARCHAR(20) | Measurement unit (kg, cm, %) |
| source | VARCHAR(100) | Data source |
| created_at | DATETIME | When the record was created (default: CURRENT_TIMESTAMP) |
| updated_at | DATETIME | When the record was last updated (default: CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP) |

### Indexes
- idx_user_date (user_id, date)
- idx_date (date)
- idx_data_type (data_type)
- uniq_user_date_type (user_id, date, data_type) - Unique