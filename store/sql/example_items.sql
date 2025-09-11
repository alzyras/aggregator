CREATE TABLE IF NOT EXISTS example_items (
    id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255),
    value DECIMAL(10, 2),
    created_at DATETIME,
    updated_at DATETIME
);