-- Heart rate data table
CREATE TABLE IF NOT EXISTS samsung_health_heart_rate (
    id VARCHAR(255) PRIMARY KEY,
    user_id VARCHAR(255),
    timestamp DATETIME,
    heart_rate DECIMAL(5, 2),
    heart_rate_zone VARCHAR(50),
    measurement_type VARCHAR(50),
    context VARCHAR(50),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_user_timestamp (user_id, timestamp),
    INDEX idx_timestamp (timestamp)
);