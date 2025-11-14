-- Setup script for database and schema
-- This script creates the database, schema, and warehouse if they don't exist

-- Create database (if it doesn't exist)
CREATE DATABASE IF NOT EXISTS FILE_EXTRACT_DB;

-- Use the database
USE DATABASE FILE_EXTRACT_DB;

-- Create schemas (if they don't exist)
CREATE SCHEMA IF NOT EXISTS PUBLIC;
CREATE SCHEMA IF NOT EXISTS CONVERTED_FILES;
CREATE SCHEMA IF NOT EXISTS LOGS;

-- Use the LOGS schema for log table
USE SCHEMA LOGS;

-- Note: Warehouse creation requires ACCOUNTADMIN role
-- If warehouse doesn't exist, create it (uncomment if needed)
-- CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
--     WITH WAREHOUSE_SIZE = 'X-SMALL'
--     AUTO_SUSPEND = 60
--     AUTO_RESUME = TRUE
--     INITIALLY_SUSPENDED = TRUE;

-- Note: Warehouse will start automatically when needed (AUTO_RESUME = TRUE)

-- Create audit log table in LOGS schema to track all operations
CREATE TABLE IF NOT EXISTS LOGS.FILE_OPERATION_LOG (
    LOG_ID NUMBER AUTOINCREMENT PRIMARY KEY,
    OPERATION_NAME VARCHAR(100) NOT NULL,
    FILE_NAME VARCHAR(500),
    USER_NAME VARCHAR(100),
    ROLE_NAME VARCHAR(100),
    SOURCE_STAGE VARCHAR(100),
    TARGET_STAGE VARCHAR(100),
    STATUS VARCHAR(50),
    START_TIME TIMESTAMP_NTZ NOT NULL,
    END_TIME TIMESTAMP_NTZ,
    DURATION_SECONDS NUMBER(10, 2),
    ERROR_MESSAGE VARCHAR(5000),
    ROWS_PROCESSED NUMBER,
    TABLE_NAME VARCHAR(100),
    SESSION_ID VARCHAR(100),
    CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

