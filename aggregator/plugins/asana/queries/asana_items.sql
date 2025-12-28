-- For new installations: create table with the new schema
CREATE TABLE IF NOT EXISTS asana_items (
    task_id VARCHAR(255) PRIMARY KEY,
    task_name TEXT,
    time_to_completion REAL,
    project TEXT,
    workspace_id TEXT,
    workspace_name TEXT,
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

-- For existing installations: add new columns if they don't exist
-- These alter statements should be run once when updating the schema
-- ALTER TABLE asana_items ADD COLUMN workspace_id TEXT;
-- ALTER TABLE asana_items ADD COLUMN workspace_name TEXT;

-- Note: For existing tasks without workspace information, the fields will remain NULL
-- since there's no way to retroactively determine which workspace they came from