-- Initialize Yelp Data Warehouse Database
-- This script runs automatically when PostgreSQL container starts

-- Create extensions if they don't exist
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";

-- Create schemas for better organization
CREATE SCHEMA IF NOT EXISTS raw_data;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS monitoring;

-- Grant permissions
GRANT ALL PRIVILEGES ON SCHEMA raw_data TO admin;
GRANT ALL PRIVILEGES ON SCHEMA analytics TO admin;
GRANT ALL PRIVILEGES ON SCHEMA monitoring TO admin;

-- Create data quality monitoring table
CREATE TABLE IF NOT EXISTS monitoring.data_quality_checks (
    id SERIAL PRIMARY KEY,
    check_name VARCHAR(100),
    table_name VARCHAR(100),
    check_query TEXT,
    expected_result TEXT,
    actual_result TEXT,
    status VARCHAR(20),
    execution_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create pipeline execution log table
CREATE TABLE IF NOT EXISTS monitoring.pipeline_execution_log (
    id SERIAL PRIMARY KEY,
    pipeline_name VARCHAR(100),
    stage VARCHAR(50),
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    duration_seconds INTEGER,
    records_processed INTEGER,
    status VARCHAR(20),
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert initial monitoring data
INSERT INTO monitoring.data_quality_checks (check_name, table_name, check_query, expected_result) VALUES
('business_not_null_check', 'business_analytics', 'SELECT COUNT(*) FROM business_analytics WHERE business_id IS NULL', '0'),
('user_not_null_check', 'user_analytics', 'SELECT COUNT(*) FROM user_analytics WHERE user_id IS NULL', '0'),
('city_summary_check', 'city_summary', 'SELECT COUNT(*) FROM city_summary WHERE total_businesses > 0', '>0'),
('business_stars_range', 'business_analytics', 'SELECT COUNT(*) FROM business_analytics WHERE avg_stars < 1 OR avg_stars > 5', '0')
ON CONFLICT DO NOTHING;