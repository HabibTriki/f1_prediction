-- Create the historical_avg_laps table if it doesn't exist
CREATE TABLE IF NOT EXISTS historical_avg_laps (
    driver VARCHAR(255) NOT NULL,
    avg_lap_time REAL NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (driver)
);

-- Grant necessary permissions
GRANT ALL PRIVILEGES ON TABLE historical_avg_laps TO f1user;