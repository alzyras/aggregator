-- General health data table
CREATE TABLE IF NOT EXISTS google_fit_general (
    id VARCHAR(255) PRIMARY KEY,
    user_id VARCHAR(255),
    data_type VARCHAR(100),
    timestamp DATETIME,
    value DECIMAL(15, 6),
    unit VARCHAR(50),
    metadata JSON,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_user_timestamp (user_id, timestamp),
    INDEX idx_timestamp (timestamp),
    INDEX idx_data_type (data_type)
);