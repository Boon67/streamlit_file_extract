-- Setup script for Snowflake stages
-- This script creates all necessary stages for file processing
-- Note: Run setup_database.sql first to ensure database and schema exist

-- Ensure we're using the correct database and schema
USE DATABASE FILE_EXTRACT_DB;
USE SCHEMA PUBLIC;

-- Create stages
CREATE OR REPLACE STAGE RAW_STAGE
    FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 0);

CREATE OR REPLACE STAGE PROCESSING_STAGE
    FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 0);

CREATE OR REPLACE STAGE COMPLETED_STAGE
    FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 0);

CREATE OR REPLACE STAGE ERROR_STAGE
    FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 0);

-- Grant necessary permissions (adjust role as needed)
-- GRANT USAGE ON STAGE RAW_STAGE TO ROLE <your_role>;
-- GRANT USAGE ON STAGE PROCESSING_STAGE TO ROLE <your_role>;
-- GRANT USAGE ON STAGE COMPLETED_STAGE TO ROLE <your_role>;
-- GRANT USAGE ON STAGE ERROR_STAGE TO ROLE <your_role>;

