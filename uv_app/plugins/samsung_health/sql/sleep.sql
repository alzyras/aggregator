-- Sleep data table
CREATE TABLE IF NOT EXISTS samsung_health_sleep (
    id VARCHAR(255) PRIMARY KEY,
    user_id VARCHAR(255),
    start_time DATETIME,
    end_time DATETIME,
    duration_minutes DECIMAL(10, 2),
    sleep_score DECIMAL(5, 2),
    deep_sleep_minutes DECIMAL(10, 2),
    light_sleep_minutes DECIMAL(10, 2),
    rem_sleep_minutes DECIMAL(10, 2),
    awake_minutes DECIMAL(10, 2),
    sleep_efficiency DECIMAL(5, 2),
    bed_time DATETIME,
    wake_up_time DATETIME,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_user_timestamp (user_id, start_time),
    INDEX idx_timestamp (start_time),
    UNIQUE KEY uniq_user_sleep_window (user_id, start_time, end_time)
);