-- Steps data table
CREATE TABLE IF NOT EXISTS samsung_health_steps (
    id VARCHAR(255) PRIMARY KEY,
    user_id VARCHAR(255),
    timestamp DATETIME,
    steps INT,
    distance DECIMAL(10, 2),
    calories DECIMAL(10, 2),
    speed DECIMAL(10, 2),
    heart_rate DECIMAL(5, 2),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_user_timestamp (user_id, timestamp),
    INDEX idx_timestamp (timestamp),
    UNIQUE KEY uniq_user_date (user_id, timestamp)
);