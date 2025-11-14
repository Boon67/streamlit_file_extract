-- Stored Procedures for processing different file types
-- Note: These are optional - the Streamlit app processes files directly
-- Run setup_database.sql and setup_stages.sql first

-- Ensure we're using the correct database and schema
USE DATABASE FILE_EXTRACT_DB;
USE SCHEMA PUBLIC;

-- Simple procedure to process CSV/TXT files (SQL-based, no Python packages)
CREATE OR REPLACE PROCEDURE PROCESS_CSV_FILE_V2(FILENAME STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Clean filename for table name
    LET table_name VARCHAR := REPLACE(REPLACE(REPLACE(UPPER(SPLIT_PART(:FILENAME, '.', 1)), '-', '_'), ' ', '_'), '.', '_');
    table_name := REGEXP_REPLACE(table_name, '[^A-Z0-9_]', '_');

    -- Create table from CSV file in RAW_STAGE
    BEGIN
        EXECUTE IMMEDIATE 'CREATE OR REPLACE TABLE ' || table_name || ' AS 
            SELECT 
                *,
                ''' || :FILENAME || ''' AS SOURCE_FILE_NAME
            FROM @RAW_STAGE/' || :FILENAME || '
            (FILE_FORMAT => (TYPE = CSV, SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY = ''"'',
             ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE))';

        -- Remove file from RAW_STAGE after processing
        EXECUTE IMMEDIATE 'REMOVE @RAW_STAGE/' || :FILENAME;

        RETURN 'SUCCESS';
    EXCEPTION
        WHEN OTHER THEN
            -- Remove file on error
            BEGIN
                EXECUTE IMMEDIATE 'REMOVE @RAW_STAGE/' || :FILENAME;
            EXCEPTION
                WHEN OTHER THEN
                    NULL;
            END;
            RETURN 'ERROR: ' || SQLERRM;
    END;
END;
$$;

-- Simple procedure for Excel files (creates metadata table)
CREATE OR REPLACE PROCEDURE PROCESS_EXCEL_FILE_V2(FILENAME STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Clean filename for table name
    LET table_name VARCHAR := REPLACE(REPLACE(REPLACE(UPPER(SPLIT_PART(:FILENAME, '.', 1)), '-', '_'), ' ', '_'), '.', '_');
    table_name := REGEXP_REPLACE(table_name, '[^A-Z0-9_]', '_');

    -- For Excel files, create a metadata table
    -- Use the Streamlit app for full Excel processing
    BEGIN
        EXECUTE IMMEDIATE 'CREATE OR REPLACE TABLE ' || table_name || ' AS 
            SELECT 
                ''Excel file: ' || :FILENAME || ' - Use Streamlit app for full data processing'' AS note,
                ''' || :FILENAME || ''' AS SOURCE_FILE_NAME,
                CURRENT_TIMESTAMP() AS PROCESSED_AT';

        -- Remove from RAW_STAGE
        EXECUTE IMMEDIATE 'REMOVE @RAW_STAGE/' || :FILENAME;

        RETURN 'SUCCESS - Note: Use Streamlit app for full Excel processing';
    EXCEPTION
        WHEN OTHER THEN
            BEGIN
                EXECUTE IMMEDIATE 'REMOVE @RAW_STAGE/' || :FILENAME;
            EXCEPTION
                WHEN OTHER THEN
                    NULL;
            END;
            RETURN 'ERROR: ' || SQLERRM;
    END;
END;
$$;

-- Simple procedure for PDF files (creates metadata table)
CREATE OR REPLACE PROCEDURE PROCESS_PDF_FILE(FILENAME STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Clean filename for table name
    LET table_name VARCHAR := REPLACE(REPLACE(REPLACE(UPPER(SPLIT_PART(:FILENAME, '.', 1)), '-', '_'), ' ', '_'), '.', '_');
    table_name := REGEXP_REPLACE(table_name, '[^A-Z0-9_]', '_');

    -- For PDF files, create a metadata table
    -- Use the Streamlit app for full PDF processing
    BEGIN
        EXECUTE IMMEDIATE 'CREATE OR REPLACE TABLE ' || table_name || ' AS 
            SELECT 
                ''PDF file: ' || :FILENAME || ' - Use Streamlit app for full text extraction'' AS note,
                ''' || :FILENAME || ''' AS SOURCE_FILE_NAME,
                CURRENT_TIMESTAMP() AS PROCESSED_AT';

        -- Remove from RAW_STAGE
        EXECUTE IMMEDIATE 'REMOVE @RAW_STAGE/' || :FILENAME;

        RETURN 'SUCCESS - Note: Use Streamlit app for full PDF processing';
    EXCEPTION
        WHEN OTHER THEN
            BEGIN
                EXECUTE IMMEDIATE 'REMOVE @RAW_STAGE/' || :FILENAME;
            EXCEPTION
                WHEN OTHER THEN
                    NULL;
            END;
            RETURN 'ERROR: ' || SQLERRM;
    END;
END;
$$;
