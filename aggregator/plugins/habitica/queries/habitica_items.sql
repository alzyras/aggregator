CREATE TABLE IF NOT EXISTS habitica_items (
    item_id VARCHAR(36),
    item_name VARCHAR(255),
    item_type VARCHAR(50),
    value DECIMAL(10, 8),
    date_created DATETIME,
    date_completed DATETIME,
    notes TEXT,
    priority DECIMAL(3, 1),
    tags TEXT,
    completed BOOLEAN
);

