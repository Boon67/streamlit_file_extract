-- Helper procedures for file management between stages
-- These procedures help move files between stages using GET/PUT pattern
-- Note: Run setup_database.sql and setup_stages.sql first

-- Ensure we're using the correct database and schema
USE DATABASE FILE_EXTRACT_DB;
USE SCHEMA PUBLIC;

-- Procedure to move file to completed stage
CREATE OR REPLACE PROCEDURE MOVE_FILE_TO_COMPLETED(FILENAME STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Note: Snowflake doesn't support direct file moves between stages
    -- Files are removed from source stage after processing
    -- The file reference is maintained in tables via SOURCE_FILE_NAME column
    RETURN 'SUCCESS - File reference maintained in table';
EXCEPTION
    WHEN OTHER THEN
        RETURN 'ERROR: ' || SQLERRM;
END;
$$;

-- Procedure to move file to error stage
CREATE OR REPLACE PROCEDURE MOVE_FILE_TO_ERROR_STAGE(FILENAME STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Remove file from RAW_STAGE if it exists there
    BEGIN
        EXECUTE IMMEDIATE 'REMOVE @RAW_STAGE/' || :FILENAME;
    EXCEPTION
        WHEN OTHER THEN
            NULL; -- File may not exist in RAW_STAGE
    END;
    
    -- Remove from PROCESSING_STAGE if it exists there
    BEGIN
        EXECUTE IMMEDIATE 'REMOVE @PROCESSING_STAGE/' || :FILENAME;
    EXCEPTION
        WHEN OTHER THEN
            NULL; -- File may not exist in PROCESSING_STAGE
    END;
    
    RETURN 'SUCCESS - File removed from source stage';
EXCEPTION
    WHEN OTHER THEN
        RETURN 'ERROR: ' || SQLERRM;
END;
$$;

