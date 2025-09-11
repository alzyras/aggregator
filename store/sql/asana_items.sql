CREATE TABLE IF NOT EXISTS asana_items (
    task_id VARCHAR(255) PRIMARY KEY,
    task_name TEXT,
    time_to_completion REAL,
    project TEXT,
    project_created_at DATETIME,
    project_notes TEXT,
    project_owner TEXT,
    completed_by_name TEXT,
    completed_by_email TEXT,
    completed BOOLEAN,
    task_description TEXT,
    date DATETIME,
    created_by_name TEXT,
    created_by_email TEXT,
    type VARCHAR(10)
);