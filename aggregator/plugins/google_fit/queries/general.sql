-- General health data table for Google Fit
CREATE TABLE IF NOT EXISTS google_fit_general (
    id VARCHAR(255) PRIMARY KEY,
    user_id VARCHAR(255),
    date DATE,
    data_type VARCHAR(50),
    value DECIMAL(10, 2),
    unit VARCHAR(20),
    source VARCHAR(100),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_user_date (user_id, date),
    INDEX idx_date (date),
    INDEX idx_data_type (data_type),
    UNIQUE KEY uniq_user_date_type (user_id, date, data_type)
);