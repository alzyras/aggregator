-- Simplified heart data table with only essential columns
CREATE TABLE IF NOT EXISTS google_fit_heart (
    id VARCHAR(255) PRIMARY KEY,
    user_id VARCHAR(255),
    timestamp DATETIME,
    heart_rate DECIMAL(5, 2),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_user_timestamp (user_id, timestamp),
    INDEX idx_timestamp (timestamp),
    UNIQUE KEY uniq_user_hour (user_id, timestamp)
);