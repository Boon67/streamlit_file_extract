# File Ingest to Snowflake - Streamlit App

A comprehensive Streamlit application that uploads files (CSV, TXT, Excel, PDF) to Snowflake stages and processes them into tables. The application runs entirely on Snowflake using Streamlit in Snowflake with Snowpark integration.

## Table of Contents
- [Features](#features)
- [Quick Start](#quick-start)
- [Architecture](#architecture)
- [Detailed Setup](#detailed-setup)
- [Usage Guide](#usage-guide)
- [File Processing](#file-processing)
- [Deployment](#deployment)
- [Troubleshooting](#troubleshooting)

---

## Features

### Core Functionality
- ✅ **Multi-File Upload**: Upload multiple CSV, TXT, Excel (XLSX, XLS), and PDF files simultaneously
- ✅ **Intelligent Processing**: Automatically convert files to Snowflake tables based on file type
- ✅ **Excel Multi-Sheet**: Each sheet in Excel files creates a separate table (e.g., `FILE_SHEET1`, `FILE_SHEET2`)
- ✅ **Stage Management**: Files move through lifecycle stages (Raw → Processing → Completed/Error)
- ✅ **Source Tracking**: Each table row includes `SOURCE_FILE_NAME` column for data lineage
- ✅ **Original Filenames**: Files preserve their original names (no random temp names)

### Advanced Features
- 🗑️ **File Deletion**: Delete files from any stage individually or in bulk
- ⚙️ **Bulk Processing**: Process all files at once with progress tracking
- 🔄 **Auto-Refresh**: UI updates automatically after operations
- 🔧 **Snowpark Integration**: Native Snowflake operations (no PUT/GET restrictions)
- 📊 **Progress Tracking**: Visual progress bars for uploads and processing
- ⚠️ **Error Handling**: Failed files automatically moved to error stage

---

## Quick Start

### Prerequisites
- Snowflake account with appropriate permissions
- Snow CLI installed (or use Snowsight for deployment)
- Python 3.11+ (for local development)

### 5-Minute Setup

1. **Clone or download this repository**

2. **Deploy to Snowflake** (choose one method):

   **Option A: Automated Script**
   ```bash
   chmod +x deploy.sh
   ./deploy.sh
   ```

   **Option B: Snowsight (Recommended - No CLI needed)**
   1. Go to https://app.snowflake.com
   2. Create `FILE_INGEST_DB` database and `PUBLIC` schema
   3. Run SQL scripts: `setup_stages.sql`, `stored_procedures.sql`
   4. Navigate to **Streamlit** → **Create Streamlit App**
   5. Name it `FILE_INGEST_APP`
   6. Copy/paste contents of `app.py`
   7. Upload `environment.yml`
   8. Click **"Run"**

   **Option C: Python Script**
   ```bash
   pip install snowflake-connector-python
   python3 manual_deploy.py
   ```

3. **Access the app**
   - Find your app in Snowsight under **Streamlit**
   - Or use: `snow streamlit get-url FILE_INGEST_APP`

4. **Start uploading files!**

---

## Architecture

### Stage Lifecycle
```
┌──────────┐     ┌────────────┐     ┌───────────┐
│   RAW    │ --> │ PROCESSING │ --> │ COMPLETED │
│  STAGE   │     │   STAGE    │     │   STAGE   │
└──────────┘     └────────────┘     └───────────┘
     │                  │
     │                  │ (on error)
     └──────────────────┴────────> ┌───────────┐
                                    │   ERROR   │
                                    │   STAGE   │
                                    └───────────┘
```

### Stages
- **RAW_STAGE**: Initial upload location for all files
- **PROCESSING_STAGE**: Temporary storage during file processing (optional, cleaned up)
- **COMPLETED_STAGE**: Successfully processed files (reference only)
- **ERROR_STAGE**: Files that failed processing for debugging

### Data Flow
1. User uploads file(s) → RAW_STAGE
2. User clicks "Process" → File moves to PROCESSING_STAGE
3. File is parsed and table is created with `SOURCE_FILE_NAME` column
4. On success → File removed from stages (table contains the data)
5. On failure → File moved to ERROR_STAGE for review

---

## Detailed Setup

### 1. Install Snow CLI (Optional)

```bash
# Using pip
pip install snowflake-cli-labs

# Or using Homebrew (macOS)
brew install snowflake-cli

# Verify installation
snow --version
```

### 2. Snowflake Permissions

Ensure your Snowflake role has:
- `CREATE DATABASE` permission (or use existing database)
- `CREATE SCHEMA` permission
- `CREATE STAGE` permission
- `CREATE TABLE` permission
- `CREATE PROCEDURE` permission
- `CREATE STREAMLIT` permission
- `USAGE` on warehouse

### 3. Database Setup

Run the SQL scripts in order:

```bash
# Option A: Using Snow CLI
snow sql -f setup_database.sql
snow sql -f setup_stages.sql
snow sql -f stored_procedures.sql
snow sql -f file_management.sql

# Option B: Using deploy script
./deploy.sh

# Option C: Manual execution in Snowsight
# Copy/paste each SQL file content into a worksheet and execute
```

### 4. Deploy the Streamlit App

**Method 1: Deploy Script (if Snow CLI working)**
```bash
./deploy.sh --app-only  # Deploy only app, skip SQL setup
```

**Method 2: Snowsight (Easiest)**
1. Navigate to Snowsight: https://app.snowflake.com
2. Go to **Streamlit** → **+ Streamlit App**
3. Set:
   - **Name**: `FILE_INGEST_APP`
   - **Location**: `FILE_INGEST_DB.PUBLIC.STREAMLIT_STAGE`
   - **Warehouse**: `COMPUTE_WH`
4. Copy entire contents of `app.py` into the editor
5. Upload `environment.yml` when prompted
6. Click **"Run"**

**Method 3: Python Script**
```bash
python3 manual_deploy.py
# Enter your Snowflake credentials when prompted
```

---

## Usage Guide

### Tab 1: Upload Files

1. **Select Files**: Click "Choose files to upload" and select one or multiple files
   - Supported: CSV, TXT, Excel (XLSX, XLS), PDF
   - Multiple files can be selected at once

2. **Review Files**: Check file names and sizes in the preview

3. **Upload**: Click "Upload All to Raw Stage"
   - Progress bar shows upload status
   - Success/failure messages for each file

### Tab 2: Process Files

#### Single File Processing
1. **Select File**: Choose a file from the dropdown (shows files in RAW_STAGE)
2. **Options**:
   - 🗑️ **Delete File**: Remove file from raw stage
   - ⚙️ **Process File**: Convert file to table

#### Bulk Operations
- **🗑️ Clear All Files from Raw Stage**: Delete all files at once
- **⚙️ Process All Files**: Process all files sequentially with progress tracking

**Processing Details**:
- CSV/TXT: Creates table with all columns + `SOURCE_FILE_NAME`
- Excel: Creates separate table for each sheet (e.g., `FILENAME_SHEET1`, `FILENAME_SHEET2`)
- PDF: Creates metadata table (full text extraction requires additional setup)

### Tab 3: View and Manage Files

- **View**: See files across all 4 stages (RAW, PROCESSING, COMPLETED, ERROR)
- **Delete**: Click 🗑️ button next to any file to remove it
- **Refresh**: Page auto-refreshes after deletions

---

## File Processing

### CSV and TXT Files
```
Input:  sales_data.csv
Output: Table SALES_DATA
```
- Automatically detects delimiter (comma, tab, pipe, etc.)
- Infers schema from data
- Handles quoted fields and special characters
- Adds `SOURCE_FILE_NAME` column

### Excel Files (Multi-Sheet Support)
```
Input:  financial_report.xlsx (3 sheets: Q1, Q2, Q3)
Output: 
  - Table FINANCIAL_REPORT_Q1
  - Table FINANCIAL_REPORT_Q2  
  - Table FINANCIAL_REPORT_Q3
```
- Processes each sheet as a separate table
- Skips empty sheets
- Preserves data types from Excel
- Each table includes `SOURCE_FILE_NAME` column

### PDF Files
```
Input:  document.pdf
Output: Table DOCUMENT (metadata only)
```
- Creates metadata table with file info
- Includes: file name, size, processed timestamp
- Note: Full text extraction requires external functions or UDFs

### Table Naming Convention
- Original filename is sanitized for Snowflake
- Special characters replaced with underscores
- Extension removed
- Converted to uppercase

**Examples**:
- `my-data-file.csv` → `MY_DATA_FILE`
- `Sales Report 2024.xlsx` → `SALES_REPORT_2024`
- `751054_Company_Data.txt` → `751054_COMPANY_DATA`

---

## Deployment

### Full Deployment (First Time)
```bash
./deploy.sh
```
This will:
1. Create `FILE_INGEST_DB` database and `PUBLIC` schema
2. Create all 4 stages (RAW, PROCESSING, COMPLETED, ERROR)
3. Create stored procedures (optional - app uses Snowpark)
4. Deploy Streamlit app to Snowflake

### Update App Only
```bash
./deploy.sh --app-only
```
Skips SQL setup, only redeploys the Streamlit app (useful for code updates)

### Manual Deployment via Python
```bash
python3 manual_deploy.py
```
Interactive script that prompts for credentials and deploys the app.

### Verify Deployment
```bash
# List Streamlit apps
snow streamlit list

# Get app URL
snow streamlit get-url FILE_INGEST_APP

# Check stages exist
snow sql -q "SHOW STAGES IN FILE_INGEST_DB.PUBLIC"

# Check tables created
snow sql -q "SHOW TABLES IN FILE_INGEST_DB.PUBLIC"
```

---

## Troubleshooting

### Connection Issues

**Problem**: Cannot connect to Snowflake
**Solutions**:
- Verify account identifier is correct (e.g., `xy12345.us-east-1`)
- Check username and password
- Ensure warehouse exists and is running
- Verify network connectivity (VPN, firewall)
- When running in Snowflake, the app uses native session (no credentials needed)

### Upload Errors

**Problem**: File upload fails
**Solutions**:
- Check file size (Snowflake stages have size limits)
- Verify stage permissions: `GRANT READ, WRITE ON STAGE RAW_STAGE TO ROLE YOUR_ROLE`
- Ensure file format is supported (CSV, TXT, XLSX, XLS, PDF)
- Check for special characters in filename

**Problem**: "Unsupported statement type 'PUT_FILES'" error
**Solution**: This is fixed in the current version. The app now uses Snowpark's `session.file.put()` which is supported in Snowflake's Streamlit environment.

**Problem**: Files show random names like `tmpXYZ123.csv`
**Solution**: This is fixed in the current version. Files now preserve their original names.

### Processing Errors

**Problem**: File processing fails
**Solutions**:
- Check file format matches expected structure
- For CSV: Ensure proper delimiter and encoding (UTF-8)
- For Excel: Verify sheets are not empty or corrupted
- Check error messages in the app UI
- Review files in ERROR_STAGE for problematic files
- View Streamlit logs in Snowsight for detailed errors

**Problem**: Excel file creates empty tables
**Solutions**:
- Verify Excel file has data (not just headers)
- Check if sheets are hidden in Excel
- Try converting to CSV manually to verify data structure

**Problem**: Table already exists error
**Solution**: The app uses `CREATE OR REPLACE TABLE`, so this shouldn't occur. If it does, manually drop the table:
```sql
DROP TABLE IF EXISTS FILE_INGEST_DB.PUBLIC.YOUR_TABLE_NAME;
```

### Deployment Issues

**Problem**: Snow CLI not found or broken
**Solutions**:
- Reinstall Snow CLI: `brew reinstall snowflake-cli` or `pip install --upgrade snowflake-cli-labs`
- Use alternative deployment: Python script (`manual_deploy.py`) or Snowsight UI

**Problem**: Database creation fails
**Solutions**:
- Verify you have CREATE DATABASE permission or ACCOUNTADMIN role
- Manually create database:
  ```sql
  CREATE DATABASE IF NOT EXISTS FILE_INGEST_DB;
  USE DATABASE FILE_INGEST_DB;
  CREATE SCHEMA IF NOT EXISTS PUBLIC;
  ```

**Problem**: Streamlit app won't start
**Solutions**:
- Check warehouse is running: `ALTER WAREHOUSE COMPUTE_WH RESUME;`
- Verify `environment.yml` is properly uploaded
- Check Python package versions in `environment.yml`
- Review app logs in Snowsight under Streamlit → App → Logs

### Performance Issues

**Problem**: Large files take too long to process
**Solutions**:
- Use larger warehouse: Change `COMPUTE_WH` to `LARGE_WH` or bigger
- Process files in smaller batches
- For very large Excel files, convert to CSV first
- Consider breaking large files into smaller chunks

**Problem**: Bulk processing is slow
**Solutions**:
- This is expected as files process sequentially
- Each file waits for the previous to complete
- For faster processing, consider using Snowflake tasks or Snowpipe

---

## Technical Details

### Snowpark Integration

The app automatically detects if it's running in Snowflake and uses Snowpark native methods:

**File Upload**:
```python
session = get_active_session()
session.file.put(tmp_path, f"@{stage_name}", auto_compress=False, overwrite=True)
```

**DataFrame to Table**:
```python
session.write_pandas(
    df, 
    table_name,
    database="FILE_INGEST_DB",
    schema="PUBLIC",
    overwrite=True,
    auto_create_table=True
)
```

**Benefits**:
- No PUT/GET SQL restrictions
- Direct DataFrame-to-table operations (faster)
- Automatic session management
- Better integration with Snowflake environment

### File Structure
```
fileingest/
├── app.py                    # Main Streamlit application (867 lines)
├── environment.yml           # Conda environment for Snowflake
├── requirements.txt          # Python dependencies
├── snowflake.yml            # Snow CLI configuration
├── app.toml                 # Application metadata
├── deploy.sh                # Automated deployment script
├── manual_deploy.py         # Python deployment script
├── setup_database.sql       # Database and schema setup
├── setup_stages.sql         # Stage creation script
├── stored_procedures.sql    # SQL procedures (optional)
├── file_management.sql      # Helper procedures (optional)
├── .streamlit/
│   ├── config.toml         # Streamlit configuration
│   └── secrets.toml        # Local credentials (not used in Snowflake)
├── samples/                 # Sample data files
└── README.md               # This file
```

### Dependencies

**Python Packages** (from `requirements.txt`):
- `streamlit==1.50.0` - Streamlit framework
- `snowflake-connector-python>=3.0.0` - Snowflake connector
- `snowflake-snowpark-python>=1.42.0` - Snowpark API
- `pandas>=2.0.0` - Data manipulation
- `openpyxl>=3.1.0` - Excel file handling
- `PyPDF2>=3.0.0` - PDF file handling

### Key Functions

- `upload_file_to_stage()` - Upload files with original filenames preserved
- `delete_file_from_stage()` - Remove files from any stage
- `process_csv_file()` - Convert CSV/TXT to tables using `write_pandas()`
- `process_excel_file()` - Multi-sheet Excel processing
- `process_pdf_file()` - PDF metadata extraction
- `get_stage_files()` - List files in a stage
- `clean_table_name()` - Sanitize filenames for valid table names

---

## Usage Examples

### Example 1: Simple CSV Upload and Process
```
1. Tab 1: Upload sales_data.csv
2. Tab 2: Select sales_data.csv
3. Tab 2: Click "⚙️ Process File"
4. Result: Table SALES_DATA created

Query the table:
SELECT * FROM FILE_INGEST_DB.PUBLIC.SALES_DATA
WHERE SOURCE_FILE_NAME = 'sales_data.csv'
LIMIT 10;
```

### Example 2: Excel Multi-Sheet Processing
```
1. Tab 1: Upload quarterly_report.xlsx (has sheets: Q1, Q2, Q3, Q4)
2. Tab 2: Click "⚙️ Process File"
3. Result: 4 tables created:
   - QUARTERLY_REPORT_Q1
   - QUARTERLY_REPORT_Q2
   - QUARTERLY_REPORT_Q3
   - QUARTERLY_REPORT_Q4

Query all sheets:
SELECT 'Q1' as Quarter, * FROM QUARTERLY_REPORT_Q1
UNION ALL
SELECT 'Q2' as Quarter, * FROM QUARTERLY_REPORT_Q2
UNION ALL
SELECT 'Q3' as Quarter, * FROM QUARTERLY_REPORT_Q3
UNION ALL
SELECT 'Q4' as Quarter, * FROM QUARTERLY_REPORT_Q4;
```

### Example 3: Bulk Upload and Process
```
1. Tab 1: Select 10 CSV files (sales_jan.csv through sales_oct.csv)
2. Tab 1: Click "Upload All to Raw Stage"
3. Tab 2: Click "⚙️ Process All Files"
4. Result: 10 tables created (SALES_JAN through SALES_OCT)

Query all months:
SELECT * FROM SALES_JAN
UNION ALL
SELECT * FROM SALES_FEB
-- ... etc
```

### Example 4: Error Handling and Cleanup
```
1. Upload a corrupted file
2. Try to process it → Moves to ERROR_STAGE
3. Tab 3: Navigate to Error Stage column
4. Click 🗑️ to delete the bad file
5. Tab 2: Click "🗑️ Clear All Files" to clean up RAW_STAGE
```

---

## Best Practices

### File Preparation
- Use UTF-8 encoding for CSV/TXT files
- Ensure Excel files don't have merged cells or complex formatting
- Keep filenames simple (alphanumeric, underscores, hyphens)
- Test with small files first before bulk uploads

### Processing Strategy
- Process smaller batches (10-20 files) at a time
- Monitor warehouse usage during large operations
- Use appropriate warehouse size for file volume
- Clean up stages regularly to avoid clutter

### Production Recommendations
- Implement file validation before upload
- Add custom error handling and notifications
- Set up monitoring for failed files in ERROR_STAGE
- Consider Snowpipe for continuous file ingestion
- Implement data quality checks after table creation
- Use Snowflake's COPY INTO for very large files
- Set up access controls and row-level security as needed

### Security
- Use OAuth or key-pair authentication instead of passwords
- Implement role-based access control (RBAC)
- Audit file uploads and processing activities
- Encrypt sensitive data at rest and in transit
- Regularly review and rotate credentials

---

## License

This project is provided as-is for demonstration and educational purposes.

---

## Support and Contribution

For issues, questions, or contributions:
- Review this README thoroughly
- Check the Troubleshooting section
- Examine Streamlit logs in Snowsight
- Test with sample files first

---

**Version**: 2.0  
**Last Updated**: November 14, 2025  
**Features**: Multi-file upload, bulk processing, file deletion, Snowpark integration, Excel multi-sheet support
