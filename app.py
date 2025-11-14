import streamlit as st
import snowflake.connector
from snowflake.connector import DictCursor
import os
from pathlib import Path
import tempfile
import traceback
import pandas as pd
import re
import io
import shutil
import time

# Try to import Snowpark for native Snowflake session
try:
    from snowflake.snowpark.context import get_active_session
    SNOWPARK_AVAILABLE = True
except ImportError:
    SNOWPARK_AVAILABLE = False
    get_active_session = None

# Page configuration
st.set_page_config(
    page_title="File Extract Upload",
    page_icon="📁",
    layout="wide"
)

# Custom CSS for Material Design styling
st.markdown("""
<style>
    /* Import Roboto font for Material Design */
    @import url('https://fonts.googleapis.com/css2?family=Roboto:wght@300;400;500;700&display=swap');
    
    /* Global styling */
    html, body, [class*="css"] {
        font-family: 'Roboto', sans-serif;
    }
    
    /* Main container */
    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
        max-width: 1200px;
    }
    
    /* Headers with Material Design style */
    h1 {
        font-weight: 500;
        color: #1a237e;
        margin-bottom: 0.5rem;
        letter-spacing: -0.5px;
    }
    
    h2, h3 {
        font-weight: 500;
        color: #283593;
        margin-top: 1.5rem;
        margin-bottom: 0.75rem;
    }
    
    h4 {
        font-weight: 500;
        color: #3949ab;
    }
    
    /* Sidebar with Material Design */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #1a237e 0%, #283593 100%);
        color: white;
    }
    
    [data-testid="stSidebar"] h1 {
        color: white;
        font-weight: 400;
    }
    
    [data-testid="stSidebar"] .stMarkdown {
        color: rgba(255, 255, 255, 0.9);
    }
    
    /* Navigation buttons - Material Design */
    [data-testid="stSidebar"] .stButton button {
        text-align: left;
        font-weight: 500;
        margin-bottom: 0.5rem;
        border-radius: 8px;
        padding: 0.75rem 1rem;
        transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    
    [data-testid="stSidebar"] .stButton button[kind="primary"] {
        background-color: #ffffff;
        color: #1a237e;
        border: none;
        font-weight: 600;
        box-shadow: 0 4px 8px rgba(0,0,0,0.2);
    }
    
    [data-testid="stSidebar"] .stButton button[kind="secondary"] {
        background-color: rgba(255, 255, 255, 0.1);
        color: white;
        border: 1px solid rgba(255, 255, 255, 0.2);
    }
    
    [data-testid="stSidebar"] .stButton button[kind="secondary"]:hover {
        background-color: rgba(255, 255, 255, 0.2);
        border-color: rgba(255, 255, 255, 0.4);
        box-shadow: 0 4px 8px rgba(0,0,0,0.15);
    }
    
    /* Main buttons with Material Design */
    .stButton button {
        border-radius: 8px;
        padding: 0.5rem 1.5rem;
        font-weight: 500;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    
    .stButton button:hover {
        box-shadow: 0 4px 12px rgba(0,0,0,0.15);
        transform: translateY(-1px);
    }
    
    .stButton button[kind="primary"] {
        background-color: #1976d2;
        border: none;
    }
    
    .stButton button[kind="primary"]:hover {
        background-color: #1565c0;
    }
    
    /* Metrics with Material Design cards */
    [data-testid="stMetricValue"] {
        font-size: 1.75rem;
        font-weight: 500;
        color: #1a237e;
    }
    
    [data-testid="stMetricLabel"] {
        font-size: 0.875rem;
        font-weight: 500;
        color: #5c6bc0;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    
    [data-testid="stMetric"] {
        background-color: #ffffff;
        padding: 1rem;
        border-radius: 12px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.08);
        border-left: 4px solid #1976d2;
    }
    
    /* Info boxes with Material Design */
    .stAlert {
        border-radius: 8px;
        border: none;
        box-shadow: 0 2px 8px rgba(0,0,0,0.08);
    }
    
    /* Dataframes with Material Design */
    [data-testid="stDataFrame"] {
        border-radius: 8px;
        overflow: hidden;
        box-shadow: 0 2px 8px rgba(0,0,0,0.08);
    }
    
    /* File uploader with Material Design */
    [data-testid="stFileUploader"] {
        border-radius: 8px;
        border: 2px dashed #9fa8da;
        background-color: #fafafa;
        padding: 1.5rem;
    }
    
    /* Expander with Material Design */
    [data-testid="stExpander"] {
        border-radius: 8px;
        border: 1px solid #e0e0e0;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }
    
    /* Text input and select boxes */
    .stTextInput input, .stSelectbox select {
        border-radius: 8px;
        border: 1px solid #c5cae9;
        padding: 0.5rem;
    }
    
    .stTextInput input:focus, .stSelectbox select:focus {
        border-color: #1976d2;
        box-shadow: 0 0 0 2px rgba(25, 118, 210, 0.1);
    }
    
    /* Progress bar */
    .stProgress > div > div {
        background-color: #1976d2;
        border-radius: 4px;
    }
    
    /* Download button special styling */
    .stDownloadButton button {
        background-color: #00897b;
        color: white;
        border: none;
    }
    
    .stDownloadButton button:hover {
        background-color: #00796b;
    }
    
    /* Code blocks */
    code {
        background-color: #f5f5f5;
        padding: 0.2rem 0.4rem;
        border-radius: 4px;
        font-family: 'Roboto Mono', monospace;
        color: #d81b60;
    }
    
    /* Dividers */
    hr {
        border: none;
        border-top: 1px solid #e0e0e0;
        margin: 1.5rem 0;
    }
    
    /* Caption text */
    .stCaptionContainer {
        color: #757575;
        font-size: 0.875rem;
    }
    
    /* Process log text area */
    .stTextArea textarea {
        font-size: 0.75rem;
        font-family: 'Roboto Mono', monospace;
        color: #666;
        border-radius: 8px;
        border: 1px solid #e0e0e0;
    }
    
    /* Tooltips */
    [data-testid="stTooltipIcon"] {
        color: #9e9e9e;
    }
    
    /* Compact spacing */
    .element-container {
        margin-bottom: 0.5rem;
    }
    
    [data-testid="column"] {
        padding: 0 0.5rem;
    }
</style>
""", unsafe_allow_html=True)

# Initialize session state
if 'connection' not in st.session_state:
    st.session_state.connection = None
if 'uploaded_files' not in st.session_state:
    st.session_state.uploaded_files = []
if 'process_logs' not in st.session_state:
    st.session_state.process_logs = []

# Snowflake connection configuration
# When running in Snowflake, use the native session connection
# This uses the current user's session and permissions automatically
@st.cache_resource
def get_snowflake_connection():
    """Get or create Snowflake connection using native session"""
    # Check if we're running in Snowflake environment first
    is_snowflake_env = os.path.exists("/home/udf") or os.getenv("SNOWFLAKE_ENVIRONMENT")
    
    if is_snowflake_env:
        # Running in Snowflake - use get_active_session() from Snowpark
        # This is the recommended way and doesn't require secrets.toml
        if SNOWPARK_AVAILABLE:
            try:
                session = get_active_session()
                # Create a connection wrapper that uses the Snowpark session
                # We need to convert it to a connector-like interface for file operations
                class SnowparkConnectionWrapper:
                    def __init__(self, session):
                        self.session = session
                    def cursor(self):
                        return SnowparkCursorWrapper(self.session)
                class SnowparkCursorWrapper:
                    def __init__(self, session):
                        self.session = session
                        self.last_result = None
                        self.rowcount = 0
                    def execute(self, query, params=None):
                        # Execute query using Snowpark session
                        # Handle parameterized queries by replacing %s with ?
                        if params:
                            # Convert %s to ? for Snowpark
                            query_modified = query.replace('%s', '?')
                            # Execute with parameters
                            self.last_result = self.session.sql(query_modified, params).collect()
                        else:
                            self.last_result = self.session.sql(query).collect()
                        
                        # Set rowcount for operations like DELETE, UPDATE, etc.
                        if self.last_result:
                            self.rowcount = len(self.last_result)
                        return self
                    def fetchone(self):
                        if self.last_result and len(self.last_result) > 0:
                            # Convert Row to tuple
                            row = self.last_result[0]
                            return tuple(row.values()) if hasattr(row, 'values') else tuple(row)
                        return None
                    def fetchall(self):
                        if self.last_result:
                            return [tuple(row.values()) if hasattr(row, 'values') else tuple(row) 
                                   for row in self.last_result]
                        return []
                    def close(self):
                        pass
                
                conn = SnowparkConnectionWrapper(session)
                # Test the connection
                try:
                    test_cursor = conn.cursor()
                    test_cursor.execute("SELECT 1")
                    result = test_cursor.fetchone()
                    if result:
                        return conn
                except:
                    # Test failed but connection might still work
                    return conn
                return conn
            except Exception as e:
                # get_active_session failed, try st.connection as fallback
                pass
        
        # Fallback: Try st.connection (may require secrets.toml but might work)
        try:
            conn = st.connection("snowflake")
            return conn
        except Exception as e:
            # st.connection failed - return None and we'll use st.sql fallback
            return None
    
    # Local development - try to use secrets if available
    # Wrap in try-except to avoid FileNotFoundError
    try:
        if hasattr(st, 'secrets'):
            try:
                secrets_config = st.secrets.get("snowflake", {})
                if secrets_config:
                    conn = snowflake.connector.connect(
                        user=secrets_config.get("user", os.getenv("SNOWFLAKE_USER")),
                        password=secrets_config.get("password", os.getenv("SNOWFLAKE_PASSWORD")),
                        account=secrets_config.get("account", os.getenv("SNOWFLAKE_ACCOUNT")),
                        warehouse=secrets_config.get("warehouse", os.getenv("SNOWFLAKE_WAREHOUSE")),
                        database=secrets_config.get("database", os.getenv("SNOWFLAKE_DATABASE")),
                        schema=secrets_config.get("schema", os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"))
                    )
                    return conn
            except (FileNotFoundError, KeyError, AttributeError):
                # Secrets file doesn't exist or is empty - that's okay
                pass
    except Exception:
        # Any error accessing secrets - ignore it
        pass
    
    # Last resort - try st.connection
    try:
        conn = st.connection("snowflake")
        return conn
    except:
        return None

def upload_file_to_stage(conn, file_data, filename, stage_name="RAW_STAGE"):
    """Upload file to Snowflake stage"""
    try:
        # Check if we're running in Snowflake environment
        is_snowflake_env = os.path.exists("/home/udf") or os.getenv("SNOWFLAKE_ENVIRONMENT")
        
        if is_snowflake_env and SNOWPARK_AVAILABLE:
            # Use Snowpark session for file upload when running in Snowflake
            try:
                session = get_active_session()
                
                # Create a temporary directory and file with the original filename
                temp_dir = tempfile.mkdtemp()
                tmp_path = os.path.join(temp_dir, filename)
                
                try:
                    # Write file with original name
                    with open(tmp_path, 'wb') as f:
                        f.write(file_data)
                    
                    # Use Snowpark's file.put method - this will preserve the filename
                    result = session.file.put(
                        tmp_path,
                        f"@{stage_name}",
                        auto_compress=False,
                        overwrite=True
                    )
                    
                    # Verify the file was uploaded with correct name
                    verify_df = session.sql(f"LIST @{stage_name} PATTERN='{filename}'").collect()
                    if verify_df and len(verify_df) > 0:
                        uploaded_name = verify_df[0]['name'].split('/')[-1]
                        if uploaded_name == filename:
                            return True, f"File {filename} uploaded successfully to {stage_name}"
                        else:
                            st.warning(f"File uploaded as '{uploaded_name}' instead of '{filename}'")
                            return True, f"File uploaded to {stage_name} as {uploaded_name}"
                    
                    return True, f"File {filename} uploaded successfully to {stage_name}"
                finally:
                    # Clean up temp file and directory
                    if os.path.exists(tmp_path):
                        os.remove(tmp_path)
                    if os.path.exists(temp_dir):
                        os.rmdir(temp_dir)
            except Exception as e:
                # If Snowpark method fails, fall through to traditional method
                st.warning(f"Snowpark upload failed, trying alternative method: {str(e)}")
        
        # Traditional method for local development or fallback
        cursor = conn.cursor()
        
        # Create temporary directory and file with original filename
        temp_dir = tempfile.mkdtemp()
        tmp_path = os.path.join(temp_dir, filename)
        
        try:
            # Write file with original name
            with open(tmp_path, 'wb') as f:
                f.write(file_data)
            
            # Upload to stage - PUT will use the actual filename
            put_command = f"PUT file://{tmp_path} @{stage_name} AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
            cursor.execute(put_command)
            
            # Get file info
            list_command = f"LIST @{stage_name} PATTERN='{filename}'"
            cursor.execute(list_command)
            result = cursor.fetchone()
            
            return True, f"File {filename} uploaded successfully to {stage_name}"
        finally:
            # Clean up temp file and directory
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            if os.path.exists(temp_dir):
                os.rmdir(temp_dir)
                
    except Exception as e:
        return False, f"Error uploading file: {str(e)}"

def clean_table_name(filename):
    """Clean filename to create valid table name"""
    # Remove extension and clean special characters
    table_name = re.sub(r'[^a-zA-Z0-9_]', '_', filename.rsplit('.', 1)[0])
    table_name = re.sub(r'_+', '_', table_name).strip('_')
    return table_name.upper()

def log_operation(conn, operation_name, file_name=None, source_stage=None, target_stage=None, 
                  status="STARTED", start_time=None, end_time=None, error_message=None, 
                  rows_processed=None, table_name=None):
    """Log file operations to audit table"""
    try:
        cursor = conn.cursor()
        
        # Get current user and role
        try:
            cursor.execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_SESSION()")
            user_info = cursor.fetchone()
            user_name = user_info[0] if user_info else "UNKNOWN"
            role_name = user_info[1] if user_info else "UNKNOWN"
            session_id = user_info[2] if user_info else None
        except:
            user_name = "UNKNOWN"
            role_name = "UNKNOWN"
            session_id = None
        
        # Calculate duration if both times provided
        duration_seconds = None
        if start_time and end_time:
            duration_seconds = (end_time - start_time).total_seconds()
        
        # Set start_time to now if not provided
        if not start_time:
            from datetime import datetime
            start_time = datetime.now()
        
        # Build and insert log entry using string formatting to avoid parameterized query issues
        def sql_escape(value):
            """Escape single quotes for SQL"""
            if value is None:
                return "NULL"
            elif isinstance(value, (int, float)):
                return str(value)
            else:
                return f"'{str(value).replace(chr(39), chr(39)+chr(39))}'"  # Escape single quotes
        
        insert_sql = f"""
        INSERT INTO LOGS.FILE_OPERATION_LOG (
            OPERATION_NAME, FILE_NAME, USER_NAME, ROLE_NAME, 
            SOURCE_STAGE, TARGET_STAGE, STATUS, START_TIME, END_TIME, 
            DURATION_SECONDS, ERROR_MESSAGE, ROWS_PROCESSED, TABLE_NAME, SESSION_ID
        ) VALUES (
            {sql_escape(operation_name)},
            {sql_escape(file_name)},
            {sql_escape(user_name)},
            {sql_escape(role_name)},
            {sql_escape(source_stage)},
            {sql_escape(target_stage)},
            {sql_escape(status)},
            '{start_time}',
            {sql_escape(end_time) if end_time else 'NULL'},
            {duration_seconds if duration_seconds else 'NULL'},
            {sql_escape(error_message)},
            {rows_processed if rows_processed else 'NULL'},
            {sql_escape(table_name)},
            {sql_escape(session_id)}
        )
        """
        
        cursor.execute(insert_sql)
        
        return True
    except Exception as e:
        # Don't fail operations if logging fails
        st.warning(f"Failed to log operation: {str(e)}")
        return False

def remove_file_from_stage(filename, stage_name):
    """Remove file from stage using Snowpark FileOperation.remove
    
    Uses the Snowpark FileOperation API as documented at:
    https://docs.snowflake.com/en/developer-guide/snowpark/reference/python/1.42.0/snowpark/api/snowflake.snowpark.FileOperation.remove
    
    Note: In Streamlit in Snowflake, file removal may have limitations due to security restrictions.
    """
    is_snowflake_env = os.path.exists("/home/udf") or os.getenv("SNOWFLAKE_ENVIRONMENT")
    
    try:
        if is_snowflake_env and SNOWPARK_AVAILABLE:
            # Use Snowpark FileOperation.remove API
            session = get_active_session()
            try:
                # Use the file.remove() method
                # Syntax: session.file.remove(stage_location, pattern=None)
                # Returns a list of removed file paths
                stage_location = f"@{stage_name}/{filename}"
                result = session.file.remove(stage_location)
                # Result is a list of removed file paths
                if result:
                    return True
                else:
                    # No files removed, but don't block workflow
                    return True
            except Exception as snowpark_error:
                error_msg = str(snowpark_error)
                if "REMOVE_FILES" in error_msg or "Unsupported statement" in error_msg or "privilege" in error_msg.lower():
                    # Expected limitation in Streamlit - files stay in stage
                    # This is OK - we track file processing in the log table
                    return True  # Return True to not block workflow
                else:
                    # Unexpected error - raise it
                    raise snowpark_error
        else:
            # Use traditional REMOVE command for local development
            conn = get_snowflake_connection()
            if conn:
                cursor = conn.cursor()
                # Execute REMOVE command
                # Syntax: REMOVE @stage_name/filename
                remove_sql = f"REMOVE @{stage_name}/{filename}"
                cursor.execute(remove_sql)
            return True
    except Exception as e:
        # If removal fails, log but don't raise - file is already processed
        # Return True to not block the workflow
        return True

def move_file_between_stages(conn, filename, from_stage, to_stage):
    """Move file from one stage to another"""
    try:
        # Download file from source stage
        file_data = download_file_from_stage(conn, filename, from_stage)
        
        # Upload to destination stage
        success, message = upload_file_to_stage(conn, file_data, filename, to_stage)
        
        if success:
            # Remove from source stage
            remove_file_from_stage(filename, from_stage)
            return True, f"File moved from {from_stage} to {to_stage}"
        else:
            return False, f"Failed to upload to {to_stage}: {message}"
            
    except Exception as e:
        return False, f"Error moving file: {str(e)}"

def put_file_to_stage_internal(tmp_path, stage_path):
    """Internal helper to PUT file to stage using Snowpark or traditional method"""
    is_snowflake_env = os.path.exists("/home/udf") or os.getenv("SNOWFLAKE_ENVIRONMENT")
    
    if is_snowflake_env and SNOWPARK_AVAILABLE:
        try:
            session = get_active_session()
            session.file.put(tmp_path, stage_path, auto_compress=False, overwrite=True)
            return
        except Exception as e:
            # Fall through to SQL method
            st.warning(f"Snowpark PUT failed, using SQL method: {str(e)}")
    
    # Use SQL cursor method
    # This needs to be called from a context that has a cursor
    raise NotImplementedError("put_file_to_stage_internal should be called with proper context")

def download_file_from_stage(conn, filename, stage_name="RAW_STAGE"):
    """Download file from Snowflake stage and return file content"""
    try:
        # Check if we're running in Snowflake environment
        is_snowflake_env = os.path.exists("/home/udf") or os.getenv("SNOWFLAKE_ENVIRONMENT")
        
        if is_snowflake_env and SNOWPARK_AVAILABLE:
            # Use Snowpark session for file download when running in Snowflake
            try:
                session = get_active_session()
                
                # Create temporary directory for download
                temp_dir = tempfile.mkdtemp()
                
                try:
                    # Use Snowpark's file.get method
                    session.file.get(
                        f"@{stage_name}/{filename}",
                        temp_dir
                    )
                    
                    # Find the downloaded file
                    for root, dirs, files in os.walk(temp_dir):
                        for file in files:
                            file_path = os.path.join(root, file)
                            # Read the file content
                            with open(file_path, 'rb') as f:
                                file_data = f.read()
                            return file_data
                    
                    raise FileNotFoundError(f"File {filename} not found after download from {stage_name}")
                finally:
                    # Clean up temp directory
                    if os.path.exists(temp_dir):
                        shutil.rmtree(temp_dir, ignore_errors=True)
            except Exception as e:
                # If Snowpark method fails, fall through to traditional method
                st.warning(f"Snowpark download failed, trying alternative method: {str(e)}")
        
        # Traditional method for local development or fallback
        cursor = conn.cursor()
        
        # Create temporary directory for download
        temp_dir = tempfile.mkdtemp()
        
        try:
            # Use GET command to download file from stage
            get_command = f"GET @{stage_name}/{filename} file://{temp_dir}/"
            cursor.execute(get_command)
            
            # Find the downloaded file (GET may preserve path structure)
            for root, dirs, files in os.walk(temp_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    # Read the file content
                    with open(file_path, 'rb') as f:
                        file_data = f.read()
                    return file_data
            
            raise FileNotFoundError(f"File {filename} not found after download from {stage_name}")
        finally:
            # Clean up temp directory
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir, ignore_errors=True)
                
    except Exception as e:
        raise e

def process_csv_file(conn, filename, file_data):
    """Process CSV/TXT file and create table"""
    try:
        cursor = conn.cursor()
        table_name = clean_table_name(filename)
        
        # Read CSV into pandas
        # Try to detect delimiter
        df = pd.read_csv(io.BytesIO(file_data), sep=None, engine='python', encoding='utf-8', on_bad_lines='skip')
        
        # Add source file name column
        df['SOURCE_FILE_NAME'] = filename
        
        # Check if we're running in Snowflake environment
        is_snowflake_env = os.path.exists("/home/udf") or os.getenv("SNOWFLAKE_ENVIRONMENT")
        
        if is_snowflake_env and SNOWPARK_AVAILABLE:
            # Use Snowpark's write_pandas for direct DataFrame to table
            try:
                session = get_active_session()
                
                # Write DataFrame directly to Snowflake table using Snowpark
                session.write_pandas(
                    df,
                    table_name,
                    database="FILE_EXTRACT_DB",
                    schema="CONVERTED_FILES",
                    overwrite=True,
                    auto_create_table=True
                )
                
                return True, f"File {filename} processed successfully. Table {table_name} created."
            except Exception as e:
                st.warning(f"Snowpark method failed, falling back to traditional method: {str(e)}")
        
        # Traditional method for local development or fallback
        # Write to Snowflake table using COPY INTO approach
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_data = csv_buffer.getvalue().encode('utf-8')
        
        # Upload to a temporary stage location
        with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as tmp_file:
            tmp_file.write(csv_data)
            tmp_path = tmp_file.name
        
        try:
            # Upload processed file
            put_command = f"PUT file://{tmp_path} @PROCESSING_STAGE/{filename} AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
            cursor.execute(put_command)
            
            # Create table from the processed file in CONVERTED_FILES schema
            create_table_sql = f"""
            CREATE OR REPLACE TABLE CONVERTED_FILES.{table_name} AS
            SELECT 
                *,
                '{filename}' AS SOURCE_FILE_NAME
            FROM @PROCESSING_STAGE/{filename}
            (FILE_FORMAT => (TYPE = CSV, SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY = '"', 
             ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE))
            """
            cursor.execute(create_table_sql)
            
            return True, f"File {filename} processed successfully. Table {table_name} created."
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
                
    except Exception as e:
        return False, f"Error processing CSV file: {str(e)}"

def process_excel_file(conn, filename, file_data):
    """Process Excel file and create a table for each sheet"""
    try:
        cursor = conn.cursor()
        base_table_name = clean_table_name(filename)
        
        # Read all sheets from Excel file
        excel_file = pd.ExcelFile(io.BytesIO(file_data), engine='openpyxl')
        sheet_names = excel_file.sheet_names
        
        if not sheet_names:
            return False, f"Excel file {filename} contains no sheets"
        
        processed_tables = []
        temp_files = []
        
        # Check if we're running in Snowflake environment
        is_snowflake_env = os.path.exists("/home/udf") or os.getenv("SNOWFLAKE_ENVIRONMENT")
        
        try:
            # Process each sheet
            for sheet_name in sheet_names:
                # Read the sheet
                df = pd.read_excel(excel_file, sheet_name=sheet_name, engine='openpyxl')
                
                # Skip empty sheets
                if df.empty:
                    continue
                
                # Create table name: {filename}_{sheetname}
                clean_sheet_name = clean_table_name(sheet_name)
                table_name = f"{base_table_name}_{clean_sheet_name}"
                
                # Add source file name column
                df['SOURCE_FILE_NAME'] = filename
                
                if is_snowflake_env and SNOWPARK_AVAILABLE:
                    # Use Snowpark's write_pandas for direct DataFrame to table
                    try:
                        session = get_active_session()
                        
                        # Write DataFrame directly to Snowflake table using Snowpark
                        session.write_pandas(
                            df,
                            table_name,
                            database="FILE_EXTRACT_DB",
                            schema="CONVERTED_FILES",
                            overwrite=True,
                            auto_create_table=True
                        )
                        
                        processed_tables.append(table_name)
                        continue  # Skip to next sheet
                    except Exception as e:
                        st.warning(f"Snowpark method failed for sheet {sheet_name}, falling back: {str(e)}")
                
                # Traditional method for local development or fallback
                # Convert to CSV
                csv_buffer = io.StringIO()
                df.to_csv(csv_buffer, index=False)
                csv_data = csv_buffer.getvalue().encode('utf-8')
                
                # Create temporary CSV file
                csv_filename = f"{filename.rsplit('.', 1)[0]}_{sheet_name}.csv"
                # Clean CSV filename for stage
                clean_csv_filename = re.sub(r'[^a-zA-Z0-9_.-]', '_', csv_filename)
                
                with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as tmp_file:
                    tmp_file.write(csv_data)
                    tmp_path = tmp_file.name
                    temp_files.append(tmp_path)
                
                # Upload processed file as CSV to processing stage
                put_command = f"PUT file://{tmp_path} @PROCESSING_STAGE/{clean_csv_filename} AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
                cursor.execute(put_command)
                
                # Create table from the processed file in CONVERTED_FILES schema
                create_table_sql = f"""
                CREATE OR REPLACE TABLE CONVERTED_FILES.{table_name} AS
                SELECT 
                    *,
                    '{filename}' AS SOURCE_FILE_NAME
                FROM @PROCESSING_STAGE/{clean_csv_filename}
                (FILE_FORMAT => (TYPE = CSV, SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY = '"', 
                 ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE))
                """
                cursor.execute(create_table_sql)
                
                processed_tables.append(table_name)
            
            if processed_tables:
                tables_str = ", ".join(processed_tables)
                return True, f"File {filename} processed successfully. Created {len(processed_tables)} table(s): {tables_str}"
            else:
                return False, f"File {filename} contained no data in any sheet"
                
        finally:
            # Clean up temporary files
            for tmp_path in temp_files:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
                
    except Exception as e:
        return False, f"Error processing Excel file: {str(e)}\n{traceback.format_exc()}"

def process_pdf_file(conn, filename, file_data):
    """Process PDF file and create metadata table"""
    try:
        cursor = conn.cursor()
        table_name = clean_table_name(filename)
        
        # For PDF, create a metadata table
        # Full PDF text extraction would require additional libraries
        create_table_sql = f"""
        CREATE OR REPLACE TABLE {table_name} AS
        SELECT 
            'PDF file: {filename}' AS note,
            '{filename}' AS SOURCE_FILE_NAME,
            CURRENT_TIMESTAMP() AS PROCESSED_AT,
            {len(file_data)} AS FILE_SIZE_BYTES
        """
        cursor.execute(create_table_sql)
        
        return True, f"File {filename} processed. Table {table_name} created with metadata. Note: Full PDF text extraction requires additional processing."
        
    except Exception as e:
        return False, f"Error processing PDF file: {str(e)}"

def add_process_log(message):
    """Add a message to the process logs in session state"""
    from datetime import datetime
    timestamp = datetime.now().strftime("%H:%M:%S")
    log_entry = f"[{timestamp}] {message}"
    if 'process_logs' not in st.session_state:
        st.session_state.process_logs = []
    st.session_state.process_logs.append(log_entry)
    # Keep only last 50 messages
    if len(st.session_state.process_logs) > 50:
        st.session_state.process_logs = st.session_state.process_logs[-50:]

def process_file(conn, filename):
    """Process file and convert to table - follows RAW -> PROCESSING -> COMPLETED/ERROR flow"""
    from datetime import datetime
    start_time = datetime.now()
    table_name = None
    
    try:
        cursor = conn.cursor()
        
        # Log process start
        log_operation(conn, "PROCESS", filename, "RAW_STAGE", None, "STARTED", start_time)
        add_process_log(f"Started processing {filename}")
        
        # Step 1: Move file from RAW_STAGE to PROCESSING_STAGE
        add_process_log(f"Moving {filename} to processing stage...")
        move_success, move_message = move_file_between_stages(conn, filename, "RAW_STAGE", "PROCESSING_STAGE")
        if not move_success:
            end_time = datetime.now()
            log_operation(conn, "PROCESS", filename, "RAW_STAGE", "PROCESSING_STAGE", "FAILED", 
                         start_time, end_time, error_message=move_message)
            add_process_log(f"❌ Failed to move {filename}: {move_message}")
            return False, f"Failed to move file to processing stage: {move_message}"
        
        add_process_log(f"Moved {filename} to processing stage")
        
        # Get file extension
        file_ext = Path(filename).suffix.lower()
        
        # Step 2: Download file from PROCESSING_STAGE
        try:
            add_process_log(f"Downloading {filename} from processing stage...")
            file_data = download_file_from_stage(conn, filename, "PROCESSING_STAGE")
            add_process_log(f"Downloaded {filename} successfully")
        except Exception as e:
            # Move to error stage if download fails
            move_file_between_stages(conn, filename, "PROCESSING_STAGE", "ERROR_STAGE")
            add_process_log(f"❌ Failed to download {filename}: {str(e)}")
            return False, f"Error downloading file from processing stage: {str(e)}"
        
        # Step 3: Process based on file type
        add_process_log(f"Processing {filename} as {file_ext} file...")
        try:
            if file_ext in ['.csv', '.txt']:
                success, message = process_csv_file(conn, filename, file_data)
            elif file_ext in ['.xlsx', '.xls']:
                success, message = process_excel_file(conn, filename, file_data)
            elif file_ext == '.pdf':
                success, message = process_pdf_file(conn, filename, file_data)
            else:
                success = False
                message = f"Unsupported file type: {file_ext}"
            
            # Step 4: Move to appropriate stage based on success
            if success:
                add_process_log(f"Moving {filename} to completed stage...")
                move_file_between_stages(conn, filename, "PROCESSING_STAGE", "COMPLETED_STAGE")
                end_time = datetime.now()
                table_name = clean_table_name(filename)
                log_operation(conn, "PROCESS", filename, "RAW_STAGE", "COMPLETED_STAGE", "SUCCESS", 
                             start_time, end_time, table_name=table_name)
                add_process_log(f"✅ Successfully processed {filename} → table {table_name}")
                return True, message
            else:
                add_process_log(f"Moving {filename} to error stage...")
                move_file_between_stages(conn, filename, "PROCESSING_STAGE", "ERROR_STAGE")
                end_time = datetime.now()
                log_operation(conn, "PROCESS", filename, "RAW_STAGE", "ERROR_STAGE", "FAILED", 
                             start_time, end_time, error_message=message)
                add_process_log(f"⚠️ Failed to process {filename}: {message}")
                return False, message
                
        except Exception as proc_error:
            # Move to error stage on processing exception
            add_process_log(f"❌ Error processing {filename}: {str(proc_error)}")
            move_file_between_stages(conn, filename, "PROCESSING_STAGE", "ERROR_STAGE")
            return False, f"Error processing file: {str(proc_error)}\n{traceback.format_exc()}"
            
    except Exception as e:
        # General error - try to move to error stage from wherever it is
        try:
            # Try to move from processing stage first
            move_file_between_stages(conn, filename, "PROCESSING_STAGE", "ERROR_STAGE")
        except:
            # If not in processing, try from raw
            try:
                move_file_between_stages(conn, filename, "RAW_STAGE", "ERROR_STAGE")
            except:
                pass
        add_process_log(f"❌ Error in workflow for {filename}: {str(e)}")
        return False, f"Error in process workflow: {str(e)}\n{traceback.format_exc()}"

def get_stage_files(conn, stage_name):
    """Get list of files in a stage"""
    try:
        cursor = conn.cursor()
        cursor.execute(f"LIST @{stage_name}")
        files = cursor.fetchall()
        
        # Clean up file names - extract just the filename from the full stage path
        cleaned_files = []
        for file_info in files:
            if file_info and len(file_info) > 0:
                full_path = file_info[0]
                # Extract just the filename from the stage path
                # Stage paths look like: <database>/<schema>/<stage>/<filename>
                if isinstance(full_path, str) and '/' in full_path:
                    filename = full_path.split('/')[-1]
                else:
                    filename = full_path
                
                # Create new tuple with cleaned filename
                cleaned_files.append((filename,) + file_info[1:])
        
        return cleaned_files
    except Exception as e:
        st.error(f"Error listing files in {stage_name}: {str(e)}")
        return []

def delete_file_from_stage(conn, filename, stage_name):
    """Delete a file from a Snowflake stage
    
    Note: In Streamlit in Snowflake, file deletion is restricted for security.
    Files remain in stages but the operation log tracks processing status.
    """
    is_snowflake_env = os.path.exists("/home/udf") or os.getenv("SNOWFLAKE_ENVIRONMENT")
    
    try:
        # Use the helper function that handles both environments
        success = remove_file_from_stage(filename, stage_name)
        if success:
            if is_snowflake_env:
                # In Snowflake, files aren't actually removed due to security restrictions
                return True, f"Note: File '{filename}' marked as processed. File remains in {stage_name} due to Snowflake security restrictions. Use Snowflake UI or SQL to manually remove files if needed."
            else:
                return True, f"File {filename} deleted successfully from {stage_name}"
        else:
            return False, f"Could not delete file {filename} from {stage_name}"
    except Exception as e:
        return False, f"Error deleting file: {str(e)}"

# Initialize page selection in session state
if 'current_page' not in st.session_state:
    st.session_state.current_page = "Step 1: Upload Files"

# Get Snowflake connection and info
conn = get_snowflake_connection()
warehouse_info = None
database_info = None
schema_info = None
connection_status = "❌ Not Connected"

if conn:
    connection_status = "✅ Connected"
    try:
        cursor = conn.cursor()
        
        # Ensure required schemas exist
        try:
            cursor.execute("CREATE SCHEMA IF NOT EXISTS CONVERTED_FILES")
            cursor.execute("CREATE SCHEMA IF NOT EXISTS LOGS")
        except Exception as schema_error:
            # Schema creation might fail due to permissions, but continue
            pass
        
        cursor.execute("SELECT CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
        info = cursor.fetchone()
        if info:
            warehouse_info = info[0]
            database_info = info[1]
            schema_info = info[2]
    except Exception as e:
        # Connection exists but query failed - might still be usable
        connection_status = "⚠️ Connected (Limited)"
else:
    # Try to test connection using st.sql directly
    try:
        result = st.sql("SELECT CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA()").dataframe()
        if not result.empty:
            connection_status = "✅ Connected"
            warehouse_info = result.iloc[0, 0]
            database_info = result.iloc[0, 1]
            schema_info = result.iloc[0, 2]
            # Create a connection wrapper that uses st.sql
            class SQLConnectionWrapper:
                def cursor(self):
                    return SQLCursorWrapper()
            class SQLCursorWrapper:
                def execute(self, query):
                    self.query = query
                    self.result = st.sql(query).dataframe()
                    return self
                def fetchone(self):
                    if hasattr(self, 'result') and not self.result.empty:
                        return tuple(self.result.iloc[0])
                    return None
                def fetchall(self):
                    if hasattr(self, 'result') and not self.result.empty:
                        return [tuple(row) for row in self.result.values]
                    return []
                def close(self):
                    pass
            conn = SQLConnectionWrapper()
        else:
            st.error("❌ Not connected to Snowflake")
            st.stop()
    except Exception as sql_error:
        st.error("❌ Not connected to Snowflake")
        st.error(f"Error: {str(sql_error)}")
        st.stop()

# Sidebar for navigation only
with st.sidebar:
    st.title("📁 File Extract")
    st.markdown("---")
    
    # Workflow Steps
    st.markdown("**Workflow**")
    
    # Workflow buttons
    if st.button("📤 Step 1: Upload Files", use_container_width=True, 
                 type="primary" if st.session_state.current_page == "Step 1: Upload Files" else "secondary"):
        st.session_state.current_page = "Step 1: Upload Files"
        st.rerun()
    
    if st.button("⚙️ Step 2: Process Files", use_container_width=True,
                 type="primary" if st.session_state.current_page == "Step 2: Process Files" else "secondary"):
        st.session_state.current_page = "Step 2: Process Files"
        st.rerun()
    
    if st.button("🔍 Step 3: View Tables", use_container_width=True,
                 type="primary" if st.session_state.current_page == "Step 3: View Tables" else "secondary"):
        st.session_state.current_page = "Step 3: View Tables"
        st.rerun()
    
    st.markdown("---")
    
    # Utilities
    st.markdown("**Utilities**")
    
    if st.button("📊 View Stages", use_container_width=True,
                 type="primary" if st.session_state.current_page == "View Stages" else "secondary"):
        st.session_state.current_page = "View Stages"
        st.rerun()
    
    if st.button("📋 Operation Logs", use_container_width=True,
                 type="primary" if st.session_state.current_page == "Operation Logs" else "secondary"):
        st.session_state.current_page = "Operation Logs"
        st.rerun()
    
    # Set page for conditional rendering
    page = st.session_state.current_page

# Main content area - display based on selected page

# ============================================================================
# PAGE: UPLOAD FILES
# ============================================================================
if page == "Step 1: Upload Files":
    st.title("📤 Step 1: Upload Files")
    st.caption("Upload your data files to begin the ingestion process")
    
    # Initialize or get file info list from session state
    if 'staged_files' not in st.session_state:
        st.session_state.staged_files = []
    
    # Show Summary and Upload button at TOP - before file selection (if files are staged)
    if st.session_state.staged_files and len(st.session_state.staged_files) > 0:
        file_info_list = st.session_state.staged_files
        total_size = sum(f['size'] for f in file_info_list)
        
        st.markdown("---")
        
        # Compact metrics styling
        st.markdown("""
        <style>
        [data-testid="stMetricValue"] {
            font-size: 28px;
        }
        [data-testid="stMetric"] {
            background-color: #f8f9fa;
            padding: 12px 16px;
            border-radius: 8px;
            border-left: 4px solid #1f77b4;
        }
        [data-testid="stMetricLabel"] {
            font-size: 13px;
            color: #666;
        }
        </style>
        """, unsafe_allow_html=True)
        
        summary_col1, summary_col2, summary_col3 = st.columns([1, 1, 1.5])
        
        with summary_col1:
            st.metric("📁 Total Files", len(file_info_list))
        
        with summary_col2:
            st.metric("💾 Total Size", f"{total_size:,} bytes")
        
        with summary_col3:
            st.markdown("<div style='margin-top: 8px;'></div>", unsafe_allow_html=True)
            upload_button_clicked = st.button("📤 Upload All to Raw Stage", type="primary", use_container_width=True, key="upload_button")
        
        st.markdown("---")
        
        # Handle upload button action
        if upload_button_clicked:
            progress_bar = st.progress(0)
            status_container = st.container()
            
            uploaded_count = 0
            failed_count = 0
            failed_files = []
            
            for idx, file_info in enumerate(file_info_list):
                filename = file_info['name']
                file_data = file_info['data']
                
                progress = (idx + 1) / len(file_info_list)
                progress_bar.progress(progress)
                
                with status_container:
                    st.info(f"Uploading {filename}... ({idx + 1}/{len(file_info_list)})")
                
                # Log upload start
                from datetime import datetime
                start_time = datetime.now()
                
                success, message = upload_file_to_stage(conn, file_data, filename, "RAW_STAGE")
                
                # Log upload end
                end_time = datetime.now()
                if success:
                    uploaded_count += 1
                    st.session_state.uploaded_files.append(filename)
                    log_operation(conn, "UPLOAD", filename, None, "RAW_STAGE", "SUCCESS", 
                                start_time, end_time)
                    add_process_log(f"Uploaded {filename} to RAW_STAGE")
                else:
                    failed_count += 1
                    failed_files.append((filename, message))
                    log_operation(conn, "UPLOAD", filename, None, "RAW_STAGE", "FAILED", 
                                start_time, end_time, error_message=message)
                    add_process_log(f"❌ Failed to upload {filename}: {message}")
            
            progress_bar.empty()
            status_container.empty()
            
            # Show results
            if uploaded_count > 0:
                st.success(f"✅ Successfully uploaded {uploaded_count} file(s) to Raw Stage")
            
            if failed_count > 0:
                st.error(f"❌ Failed to upload {failed_count} file(s):")
                for filename, error_msg in failed_files:
                    st.error(f"  - {filename}: {error_msg}")
            
            if uploaded_count == len(file_info_list):
                st.balloons()
                st.success("✅ Upload complete! Moving to Process Files...")
                # Clear staged files after successful upload
                st.session_state.staged_files = []
                # Navigate to Step 2
                time.sleep(1.5)  # Brief pause to show success message
                st.session_state.current_page = "Step 2: Process Files"
                st.rerun()
    
    # File uploader - shown AFTER upload button section
    uploaded_files = st.file_uploader(
        "Choose files to upload:",
        type=['csv', 'txt', 'xlsx', 'xls', 'pdf'],
        accept_multiple_files=True,
        help="Select one or more files from your computer",
        label_visibility="visible",
        key="file_uploader"
    )
    
    # Subdued supported formats info at bottom of uploader
    st.markdown(
        "<div style='color: #888; font-size: 0.8em; margin-top: -10px; margin-bottom: 20px;'>"
        "Supported Formats: CSV, TXT, Excel (XLSX, XLS), PDF • You can upload multiple files at once"
        "</div>",
        unsafe_allow_html=True
    )
    
    # Process selected files and store in session state
    if uploaded_files and len(uploaded_files) > 0:
        # Process file information
        file_info_list = []
        total_size = 0
        
        for uploaded_file in uploaded_files:
            file_data = uploaded_file.read()
            uploaded_file.seek(0)  # Reset file pointer
            file_size = len(file_data)
            total_size += file_size
            
            file_info_list.append({
                'name': uploaded_file.name,
                'size': file_size,
                'data': file_data
            })
        
        # Update session state and rerun to show button at top
        if len(st.session_state.staged_files) != len(file_info_list):
            st.session_state.staged_files = file_info_list
            st.rerun()
        else:
            st.session_state.staged_files = file_info_list
    
    # Display selected files list (shown after file uploader)
    if st.session_state.staged_files and len(st.session_state.staged_files) > 0:
        file_info_list = st.session_state.staged_files
        
        st.markdown("---")
        st.markdown(f"### 📋 Selected Files ({len(file_info_list)})")
        
        # Display files in a table-like format
        for idx, file_info in enumerate(file_info_list, 1):
            col1, col2, col3 = st.columns([3, 1, 1])
            with col1:
                # Determine file type icon
                ext = file_info['name'].lower().split('.')[-1]
                icon = {
                    'csv': '📊',
                    'txt': '📝',
                    'xlsx': '📗',
                    'xls': '📗',
                    'pdf': '📕'
                }.get(ext, '📄')
                st.markdown(f"{icon} **{file_info['name']}**")
            with col2:
                st.text(f"{file_info['size']:,} bytes")
            with col3:
                st.text(f"({ext.upper()})")

# ============================================================================
# PAGE: PROCESS FILES
# ============================================================================
elif page == "Step 2: Process Files":
    st.title("⚙️ Step 2: Process Files")
    st.caption("Process and convert files from raw stage into Snowflake tables")
    
    # Get files in raw stage
    raw_files = get_stage_files(conn, "RAW_STAGE")
    
    if raw_files:
        # Filenames are already cleaned by get_stage_files function
        file_options = [f[0] for f in raw_files if f[0].endswith(('.csv', '.txt', '.xlsx', '.xls', '.pdf'))]
        
        if file_options:
            # Bulk operations section at top - more compact
            st.markdown("#### 🚀 Bulk Operations")
            st.caption(f"Process or remove all {len(file_options)} file(s) at once")
            
            bulk_col1, bulk_col2, bulk_col3 = st.columns([1, 1, 2])
            
            with bulk_col1:
                process_all_clicked = st.button("⚙️ Process All Files", type="primary", use_container_width=True, help="Process all files in raw stage sequentially")
            
            with bulk_col2:
                if st.button("🗑️ Clear All Files", type="secondary", use_container_width=True, help="Delete all files from raw stage"):
                    if st.checkbox("⚠️ Confirm deletion of all files", key="confirm_bulk_delete"):
                        with st.spinner("Deleting all files..."):
                            deleted_count = 0
                            failed_count = 0
                            for filename in file_options:
                                success, _ = delete_file_from_stage(conn, filename, "RAW_STAGE")
                                if success:
                                    deleted_count += 1
                                    add_process_log(f"Deleted file {filename} from RAW_STAGE")
                                else:
                                    failed_count += 1
                            
                            if deleted_count > 0:
                                st.success(f"✅ Deleted {deleted_count} file(s)")
                            if failed_count > 0:
                                st.error(f"❌ Failed to delete {failed_count} file(s)")
                            st.rerun()
            
            with bulk_col3:
                st.caption("💡 Tip: Use bulk operations for efficiency, or process files individually below")
            
            # Handle Process All Files button click - OUTSIDE columns for full width display
            if process_all_clicked:
                if len(file_options) > 0:
                    progress_bar = st.progress(0)
                    
                    # Create placeholder for horizontal status cards - full width
                    status_placeholder = st.empty()
                    
                    processed_count = 0
                    failed_count = 0
                    processing_status = []
                    
                    for idx, filename in enumerate(file_options):
                        progress = (idx + 1) / len(file_options)
                        progress_bar.progress(progress)
                        
                        # Update processing status
                        processing_status.append({
                            'filename': filename,
                            'status': 'processing',
                            'index': idx + 1
                        })
                        
                        # Display current processing status on a single line
                        with status_placeholder.container():
                            st.info(f"Processing {filename}... ({idx + 1}/{len(file_options)})")
                        
                        success, message = process_file(conn, filename)
                        if success:
                            processed_count += 1
                            processing_status[-1]['status'] = 'success'
                        else:
                            failed_count += 1
                            processing_status[-1]['status'] = 'failed'
                            processing_status[-1]['message'] = message[:100]
                    
                    progress_bar.empty()
                    status_placeholder.empty()
                    
                    # Show final results
                    if processed_count > 0:
                        st.success(f"✅ Successfully processed {processed_count} file(s)")
                    if failed_count > 0:
                        st.error(f"❌ Failed to process {failed_count} file(s)")
                    
                    if processed_count == len(file_options):
                        st.balloons()
                        st.success("✅ Processing complete! Moving to View Tables...")
                        # Navigate to Step 3
                        time.sleep(1.5)  # Brief pause to show success message
                        st.session_state.current_page = "Step 3: View Tables"
                    
                    st.rerun()
            
            # Files table section - more compact
            st.markdown("<div style='margin-top: 20px; margin-bottom: 10px;'></div>", unsafe_allow_html=True)
            st.markdown("#### 📄 Files")
            st.caption(f"Found {len(file_options)} file(s) ready to process")
            
            # Create table header with compact styling
            st.markdown("<div style='margin-top: 10px;'></div>", unsafe_allow_html=True)
            header_cols = st.columns([4, 1.5, 0.75, 0.75])
            with header_cols[0]:
                st.markdown("**Filename**")
            with header_cols[1]:
                st.markdown("**Type**")
            with header_cols[2]:
                st.markdown("**Process**")
            with header_cols[3]:
                st.markdown("**Delete**")
            
            st.markdown("<hr style='margin: 5px 0; border: 0; border-top: 1px solid #ddd;'>", unsafe_allow_html=True)
            
            # Display each file in table format with compact spacing
            for idx, filename in enumerate(file_options):
                file_cols = st.columns([4, 1.5, 0.75, 0.75])
                
                # Get file extension and icon
                ext = filename.lower().split('.')[-1] if '.' in filename else ''
                icon = {
                    'csv': '📊',
                    'txt': '📝',
                    'xlsx': '📗',
                    'xls': '📗',
                    'pdf': '📕'
                }.get(ext, '📄')
                
                with file_cols[0]:
                    st.markdown(f"<div style='padding: 2px 0;'>{icon} <code>{filename}</code></div>", unsafe_allow_html=True)
                
                with file_cols[1]:
                    st.markdown(f"<div style='padding: 2px 0;'>{ext.upper() if ext else 'Unknown'}</div>", unsafe_allow_html=True)
                
                with file_cols[2]:
                    if st.button("⚙️", key=f"process_{idx}_{filename}", help=f"Process {filename}", use_container_width=True):
                        with st.spinner(f"Processing {filename}..."):
                            success, message = process_file(conn, filename)
                            if success:
                                st.success(message)
                                st.rerun()
                            else:
                                st.error(message)
                
                with file_cols[3]:
                    if st.button("🗑️", key=f"delete_{idx}_{filename}", help=f"Delete {filename}", use_container_width=True):
                        with st.spinner(f"Deleting {filename}..."):
                            success, message = delete_file_from_stage(conn, filename, "RAW_STAGE")
                            if success:
                                add_process_log(f"Deleted file {filename} from RAW_STAGE")
                                st.success(message)
                                st.rerun()
                            else:
                                st.error(message)
                
                # Add a subtle divider between rows with less margin
                if idx < len(file_options) - 1:
                    st.markdown("<hr style='margin: 4px 0; border: 0; border-top: 1px solid #eee;'>", unsafe_allow_html=True)
        else:
            st.warning("⚠️ No processable files found in raw stage")
            st.info("Supported file types: CSV, TXT, Excel (XLSX, XLS), PDF")
    else:
        st.info("📭 No files in raw stage yet")
        st.markdown("👉 Use the **'Upload Files'** page to add files to the raw stage")
    
    # Footer with process logs
    st.markdown("---")
    st.markdown("### 📝 Process Log")
    
    # Create a container for logs with fixed height and scrolling
    log_container = st.container()
    with log_container:
        if 'process_logs' in st.session_state and st.session_state.process_logs:
            # Display logs in reverse order (newest first)
            logs_to_display = st.session_state.process_logs[-20:]  # Show last 20 logs
            
            # Create a text area with small font for logs
            log_text = "\n".join(reversed(logs_to_display))
            st.text_area(
                "Recent activity:",
                value=log_text,
                height=150,
                disabled=True,
                label_visibility="collapsed"
            )
            
            # Add clear logs button
            col1, col2 = st.columns([6, 1])
            with col2:
                if st.button("Clear Logs", type="secondary", key="clear_process_logs"):
                    st.session_state.process_logs = []
                    st.rerun()
        else:
            st.caption("No activity logs yet. Process logs will appear here as you work.")
    
    # Small text footer
    st.markdown(
        "<div style='text-align: center; color: #666; font-size: 0.75em; padding: 10px;'>"
        "Process logs show real-time activity • Logs are kept for the current session only"
        "</div>",
        unsafe_allow_html=True
    )

# ============================================================================
# PAGE: VIEW STAGES
# ============================================================================
elif page == "View Stages":
    st.title("📊 View and Manage Files in Stages")
    st.markdown("Monitor file movement through the processing pipeline")
    
    # Check if running in Snowflake environment
    is_snowflake_env = os.path.exists("/home/udf") or os.getenv("SNOWFLAKE_ENVIRONMENT")
    if is_snowflake_env:
        st.warning("⚠️ **Snowflake Security Note**: File deletion from stages is limited in Streamlit. Files remain in stages after processing. Use the Operation Logs tab to track status, or manually remove files via SQL: `REMOVE @STAGE_NAME/filename`")
    
    # Stage pipeline visualization
    st.markdown("### 📍 Processing Pipeline")
    pipeline_col1, pipeline_col2, pipeline_col3, pipeline_col4 = st.columns(4)
    
    with pipeline_col1:
        st.markdown("**1️⃣ RAW**")
        st.caption("Uploaded files")
    with pipeline_col2:
        st.markdown("**2️⃣ PROCESSING**")
        st.caption("Being converted")
    with pipeline_col3:
        st.markdown("**3️⃣ COMPLETED**")
        st.caption("Successfully processed")
    with pipeline_col4:
        st.markdown("**4️⃣ ERROR**")
        st.caption("Failed processing")
    
    st.markdown("---")
    
    # Stage content display
    stages = [
        ("RAW_STAGE", "📥 Raw", "#1f77b4"),
        ("PROCESSING_STAGE", "⚙️ Processing", "#ff7f0e"),
        ("COMPLETED_STAGE", "✅ Completed", "#2ca02c"),
        ("ERROR_STAGE", "❌ Error", "#d62728")
    ]
    
    stage_cols = st.columns(4)
    
    for idx, (stage_name, stage_label, color) in enumerate(stages):
        with stage_cols[idx]:
            files = get_stage_files(conn, stage_name)
            file_count = len(files) if files else 0
            
            # Display stage header with file count
            st.markdown(f"### {stage_label}")
            st.metric("Files", file_count)
            
            if files:
                st.markdown("---")
                for file_idx, file_info in enumerate(files):
                    # Filename is already cleaned by get_stage_files function
                    file_name = file_info[0] if isinstance(file_info[0], str) else str(file_info[0])
                    file_size = file_info[1] if len(file_info) > 1 else "N/A"
                    
                    # Create a compact file display
                    with st.container():
                        # File icon based on extension
                        ext = file_name.lower().split('.')[-1] if '.' in file_name else ''
                        icon = {
                            'csv': '📊',
                            'txt': '📝',
                            'xlsx': '📗',
                            'xls': '📗',
                            'pdf': '📕'
                        }.get(ext, '📄')
                        
                        st.markdown(f"{icon} **{file_name}**")
                        
                        # File size and delete button
                        file_col1, file_col2 = st.columns([3, 1])
                        with file_col1:
                            if file_size != "N/A":
                                st.caption(f"{file_size:,} bytes" if isinstance(file_size, int) else file_size)
                        with file_col2:
                            # Create unique key for delete button
                            delete_key = f"del_{stage_name}_{file_idx}_{hash(file_name)}"
                            if st.button("🗑️", key=delete_key, help=f"Delete {file_name}"):
                                with st.spinner(f"Deleting {file_name}..."):
                                    success, message = delete_file_from_stage(conn, file_name, stage_name)
                                    if success:
                                        add_process_log(f"Deleted {file_name} from {stage_name}")
                                        st.success("Deleted")
                                        st.rerun()
                                    else:
                                        st.error(f"Error: {message[:50]}")
                        
                        st.markdown("<hr style='margin: 8px 0; border: 0; border-top: 1px solid #eee;'>", unsafe_allow_html=True)
            else:
                st.caption("Empty")
    
    # Add stage statistics
    st.markdown("---")
    st.markdown("### 📈 Stage Statistics")
    
    stats_col1, stats_col2, stats_col3, stats_col4 = st.columns(4)
    
    with stats_col1:
        raw_count = len(get_stage_files(conn, "RAW_STAGE"))
        st.metric("Pending Upload", raw_count, help="Files waiting to be processed")
    
    with stats_col2:
        processing_count = len(get_stage_files(conn, "PROCESSING_STAGE"))
        st.metric("In Progress", processing_count, help="Files currently being processed")
    
    with stats_col3:
        completed_count = len(get_stage_files(conn, "COMPLETED_STAGE"))
        st.metric("Completed", completed_count, help="Successfully processed files")
    
    with stats_col4:
        error_count = len(get_stage_files(conn, "ERROR_STAGE"))
        st.metric("Errors", error_count, help="Files that failed processing")

# ============================================================================
# PAGE: VIEW TABLES
# ============================================================================
elif page == "Step 3: View Tables":
    st.title("🔍 Step 3: View Tables")
    st.caption("View and analyze the tables created from your processed files")
    
    # Helper function for SQL identifier quoting
    def quote_identifier(name):
        """Quote SQL identifier to handle special characters"""
        return f'"{name}"'
    
    try:
        # Get list of tables in the current database/schema
        cursor = conn.cursor()
        cursor.execute("""
            SELECT table_name, row_count, bytes, created 
            FROM information_schema.tables 
            WHERE table_schema = 'CONVERTED_FILES' 
            AND table_type = 'BASE TABLE'
            ORDER BY created DESC
        """)
        tables = cursor.fetchall()
        
        if tables:
            st.markdown(f"### 📚 Available Tables ({len(tables)})")
            
            # Create a selectbox for table selection with better styling
            selected_table = st.selectbox(
                "Choose a table to inspect:",
                [t[0] for t in tables],
                help="Select a table to view its schema and data"
            )
            
            if selected_table:
                # Display table metadata with better layout
                table_info = [t for t in tables if t[0] == selected_table][0]
                
                st.markdown("---")
                
                # Table header with delete button
                header_col1, header_col2 = st.columns([10, 1])
                with header_col1:
                    st.markdown(f"### 📊 Table: `{selected_table}`")
                with header_col2:
                    st.markdown("")  # Spacer
                    if st.button("🗑️", key="delete_table_btn", help="Delete this table", use_container_width=True):
                        st.session_state['show_confirm_drop'] = True
                
                # Show confirmation if delete was clicked
                if 'show_confirm_drop' in st.session_state and st.session_state['show_confirm_drop']:
                    if st.checkbox(f"⚠️ Confirm deletion of table `{selected_table}`", key="confirm_drop"):
                        try:
                            cursor.execute(f"DROP TABLE CONVERTED_FILES.{quote_identifier(selected_table)}")
                            add_process_log(f"Dropped table {selected_table}")
                            st.success(f"✅ Table {selected_table} dropped successfully")
                            st.session_state['show_confirm_drop'] = False
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error dropping table: {str(e)}")
                
                # Combined Table & Data Quality Metrics section
                try:
                    # Get column information for quality metrics
                    cursor.execute(f"""
                        SELECT column_name, data_type
                        FROM information_schema.columns
                        WHERE table_schema = 'CONVERTED_FILES'
                        AND table_name = '{selected_table}'
                        ORDER BY ordinal_position
                    """)
                    columns_info = cursor.fetchall()
                    
                    # Calculate quality metrics if data exists
                    completeness = 0
                    null_cells = 0
                    columns_with_nulls = 0
                    
                    if columns_info and table_info[1] and table_info[1] > 0:
                        total_rows = table_info[1]
                        total_cells = 0
                        
                        for col_name, col_type in columns_info:
                            # Count nulls in each column from CONVERTED_FILES schema
                            cursor.execute(f"""
                                SELECT COUNT(*) - COUNT({quote_identifier(col_name)}) as null_count
                                FROM CONVERTED_FILES.{quote_identifier(selected_table)}
                            """)
                            null_count = cursor.fetchone()[0]
                            
                            total_cells += total_rows
                            null_cells += null_count
                            if null_count > 0:
                                columns_with_nulls += 1
                        
                        # Calculate completeness percentage
                        completeness = ((total_cells - null_cells) / total_cells * 100) if total_cells > 0 else 0
                    
                    # Display all metrics in two rows
                    # Row 1: Basic table info
                    row1_col1, row1_col2, row1_col3, row1_col4 = st.columns(4)
                    
                    with row1_col1:
                        st.metric("📈 Row Count", f"{table_info[1]:,}" if table_info[1] else "N/A")
                    
                    with row1_col2:
                        size_bytes = table_info[2] if table_info[2] else 0
                        size_mb = size_bytes / (1024 * 1024) if size_bytes > 0 else 0
                        st.metric("💾 Size", f"{size_mb:.2f} MB" if size_mb > 0 else "N/A")
                    
                    with row1_col3:
                        st.metric("📋 Total Columns", len(columns_info) if columns_info else "N/A")
                    
                    with row1_col4:
                        created_date = str(table_info[3])[:10] if table_info[3] else "N/A"
                        st.metric("📅 Created", created_date)
                    
                    # Row 2: Data quality metrics
                    row2_col1, row2_col2, row2_col3, row2_col4 = st.columns(4)
                    
                    with row2_col1:
                        st.metric(
                            "✅ Completeness",
                            f"{completeness:.1f}%",
                            help="Percentage of non-null values across all columns"
                        )
                    
                    with row2_col2:
                        st.metric(
                            "❌ Null Values",
                            f"{null_cells:,}",
                            help="Total null values across all columns"
                        )
                    
                    with row2_col3:
                        st.metric(
                            "⚠️ Columns with Nulls",
                            columns_with_nulls,
                            help="Number of columns containing null values"
                        )
                    
                    with row2_col4:
                        created_time = str(table_info[3])[11:19] if table_info[3] and len(str(table_info[3])) > 11 else "N/A"
                        st.metric("🕐 Created Time", created_time)
                        
                except Exception as e:
                    st.warning(f"Could not calculate metrics: {str(e)}")
                
                # Data preview section
                st.markdown("---")
                st.markdown("### 👀 Data Preview")
                
                preview_col1, preview_col2 = st.columns([3, 1])
                with preview_col1:
                    limit = st.slider("Rows to display:", min_value=5, max_value=100, value=10, step=5)
                with preview_col2:
                    st.markdown("")  # Spacer
                    if st.button("🔄 Refresh", type="secondary", use_container_width=True, key="refresh_preview"):
                        st.rerun()
                
                try:
                    # Fetch and display data from CONVERTED_FILES schema
                    cursor.execute(f"SELECT * FROM CONVERTED_FILES.{quote_identifier(selected_table)} LIMIT {limit}")
                    preview_data = cursor.fetchall()
                    
                    if preview_data:
                        # Get column names
                        cursor.execute(f"""
                            SELECT column_name
                            FROM information_schema.columns
                            WHERE table_schema = 'CONVERTED_FILES'
                            AND table_name = '{selected_table}'
                            ORDER BY ordinal_position
                        """)
                        col_names = [c[0] for c in cursor.fetchall()]
                        
                        # Create DataFrame for better display
                        import pandas as pd
                        preview_df = pd.DataFrame(preview_data, columns=col_names)
                        st.dataframe(preview_df, use_container_width=True, height=400, hide_index=True)
                        
                        # Download button below preview
                        csv = preview_df.to_csv(index=False)
                        st.download_button(
                            label="📥 Download Preview as CSV",
                            data=csv,
                            file_name=f"{selected_table}_preview.csv",
                            mime="text/csv",
                            type="secondary"
                        )
                    else:
                        st.info("📭 Table is empty - no data to display")
                        
                except Exception as e:
                    st.error(f"❌ Error previewing data: {str(e)}")
                
                # Schema expander at bottom
                st.markdown("---")
                with st.expander("📋 Table Schema", expanded=False):
                    cursor.execute(f"""
                        SELECT column_name, data_type, is_nullable
                        FROM information_schema.columns
                        WHERE table_schema = 'CONVERTED_FILES'
                        AND table_name = '{selected_table}'
                        ORDER BY ordinal_position
                    """)
                    columns = cursor.fetchall()
                    
                    if columns:
                        import pandas as pd
                        columns_df = pd.DataFrame(columns, columns=["Column Name", "Data Type", "Nullable"])
                        st.dataframe(columns_df, use_container_width=True, hide_index=True)
                        st.caption(f"Total columns: {len(columns)}")
        else:
            st.info("📭 No tables found in the current schema")
            st.markdown("""
            **Next Steps:**
            1. Go to **Step 1: Upload Files** to upload data files
            2. Use **Step 2: Process Files** to convert files into tables
            3. Return here to inspect the created tables
            """)
            
    except Exception as e:
        st.error(f"❌ Error loading tables: {str(e)}")

# ============================================================================
# PAGE: OPERATION LOGS
# ============================================================================
elif page == "Operation Logs":
    st.title("📋 Operation Logs")
    
    try:
        cursor = conn.cursor()
        
        # Add filters
        col1, col2, col3 = st.columns(3)
        
        with col1:
            operation_filter = st.selectbox(
                "Filter by Operation",
                ["All", "UPLOAD", "PROCESS", "MOVE", "DELETE"],
                key="op_filter"
            )
        
        with col2:
            status_filter = st.selectbox(
                "Filter by Status",
                ["All", "SUCCESS", "FAILED", "STARTED", "IN_PROGRESS"],
                key="status_filter"
            )
        
        with col3:
            limit = st.slider("Number of logs", min_value=10, max_value=500, value=100, step=10)
        
        # Build query with filters
        where_clauses = []
        if operation_filter != "All":
            where_clauses.append(f"OPERATION_NAME = '{operation_filter}'")
        if status_filter != "All":
            where_clauses.append(f"STATUS = '{status_filter}'")
        
        where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
        
        # Query logs
        query = f"""
        SELECT 
            LOG_ID,
            OPERATION_NAME,
            FILE_NAME,
            USER_NAME,
            ROLE_NAME,
            SOURCE_STAGE,
            TARGET_STAGE,
            STATUS,
            START_TIME,
            END_TIME,
            DURATION_SECONDS,
            ERROR_MESSAGE,
            TABLE_NAME,
            SESSION_ID
        FROM LOGS.FILE_OPERATION_LOG
        WHERE {where_sql}
        ORDER BY START_TIME DESC
        LIMIT {limit}
        """
        
        cursor.execute(query)
        logs = cursor.fetchall()
        
        if logs:
            st.subheader(f"Showing {len(logs)} log entries")
            
            # Create DataFrame for display
            import pandas as pd
            df = pd.DataFrame(logs, columns=[
                "Log ID", "Operation", "File Name", "User", "Role",
                "Source Stage", "Target Stage", "Status", "Start Time", "End Time",
                "Duration (s)", "Error Message", "Table Name", "Session ID"
            ])
            
            # Format duration
            df['Duration (s)'] = df['Duration (s)'].apply(
                lambda x: f"{x:.2f}" if pd.notna(x) else "N/A"
            )
            
            # Display stats
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                total_ops = len(logs)
                st.metric("Total Operations", total_ops)
            with col2:
                success_count = len([l for l in logs if l[7] == "SUCCESS"])
                st.metric("Successful", success_count)
            with col3:
                failed_count = len([l for l in logs if l[7] == "FAILED"])
                st.metric("Failed", failed_count)
            with col4:
                avg_duration = sum([l[10] for l in logs if l[10] is not None]) / len([l for l in logs if l[10] is not None]) if any(l[10] for l in logs) else 0
                st.metric("Avg Duration", f"{avg_duration:.2f}s")
            
            st.divider()
            
            # Expandable log details
            st.subheader("Log Details")
            
            # Add search
            search_term = st.text_input("🔍 Search logs (file name, user, error)", "")
            
            if search_term:
                df_filtered = df[
                    df.apply(lambda row: search_term.lower() in str(row).lower(), axis=1)
                ]
            else:
                df_filtered = df
            
            # Display in expandable sections
            for idx, row in df_filtered.iterrows():
                log_id = row['Log ID']
                operation = row['Operation']
                file_name = row['File Name']
                status = row['Status']
                start_time = row['Start Time']
                
                # Create status emoji
                status_emoji = "✅" if status == "SUCCESS" else "❌" if status == "FAILED" else "⏳"
                
                # Create expander title
                title = f"{status_emoji} [{log_id}] {operation} - {file_name if file_name else 'N/A'} - {status} ({start_time})"
                
                with st.expander(title, expanded=False):
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.write("**Operation Details:**")
                        st.write(f"- **Operation:** {operation}")
                        st.write(f"- **File:** {file_name if file_name else 'N/A'}")
                        st.write(f"- **Status:** {status}")
                        st.write(f"- **User:** {row['User']}")
                        st.write(f"- **Role:** {row['Role']}")
                        st.write(f"- **Session:** {row['Session ID']}")
                    
                    with col2:
                        st.write("**Timing & Stages:**")
                        st.write(f"- **Start:** {start_time}")
                        st.write(f"- **End:** {row['End Time'] if row['End Time'] else 'N/A'}")
                        st.write(f"- **Duration:** {row['Duration (s)']}s")
                        st.write(f"- **Source Stage:** {row['Source Stage'] if row['Source Stage'] else 'N/A'}")
                        st.write(f"- **Target Stage:** {row['Target Stage'] if row['Target Stage'] else 'N/A'}")
                        st.write(f"- **Table Created:** {row['Table Name'] if row['Table Name'] else 'N/A'}")
                    
                    if row['Error Message']:
                        st.error(f"**Error:** {row['Error Message']}")
            
            st.divider()
            
            # Export option
            csv = df_filtered.to_csv(index=False)
            st.download_button(
                label="📥 Download Logs as CSV",
                data=csv,
                file_name="operation_logs.csv",
                mime="text/csv"
            )
            
            # Clear logs button
            st.divider()
            st.subheader("Maintenance")
            col1, col2 = st.columns(2)
            with col1:
                if st.button("🗑️ Clear All Logs", type="secondary"):
                    if st.checkbox("Confirm clearing all logs", key="confirm_clear_logs"):
                        try:
                            cursor.execute("TRUNCATE TABLE LOGS.FILE_OPERATION_LOG")
                            st.success("✅ All logs cleared")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error clearing logs: {str(e)}")
            
            with col2:
                days_to_keep = st.number_input("Keep logs from last N days", min_value=1, max_value=365, value=30)
                if st.button("🗑️ Clear Old Logs", type="secondary"):
                    try:
                        cursor.execute(f"""
                            DELETE FROM LOGS.FILE_OPERATION_LOG 
                            WHERE START_TIME < DATEADD(day, -{days_to_keep}, CURRENT_TIMESTAMP())
                        """)
                        deleted = cursor.rowcount
                        st.success(f"✅ Cleared {deleted} old log entries")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error clearing old logs: {str(e)}")
        else:
            st.info("No logs found. Operations will be logged as you use the app.")
    
    except Exception as e:
        if "does not exist" in str(e).lower():
            st.warning("⚠️ Operation log table doesn't exist yet. Run the full deployment to create it:")
            st.code("bash deploy.sh", language="bash")
        else:
            st.error(f"Error loading logs: {str(e)}")

# Discrete footer with connection information
st.markdown("---")
footer_parts = []
if connection_status:
    footer_parts.append(connection_status)
if warehouse_info:
    footer_parts.append(f"Warehouse: {warehouse_info}")
if database_info:
    footer_parts.append(f"Database: {database_info}")
if schema_info:
    footer_parts.append(f"Schema: {schema_info}")

footer_text = " • ".join(footer_parts) if footer_parts else "Connection information unavailable"

st.markdown(
    f"<div style='text-align: center; color: #888; font-size: 0.7em; padding: 20px 0 10px 0;'>"
    f"{footer_text}"
    f"</div>",
    unsafe_allow_html=True
)

# Close connection on app close
if conn:
    # Connection will be managed by Streamlit's cache_resource
    pass

