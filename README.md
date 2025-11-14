# File Ingest to Snowflake - Streamlit App

A modern Streamlit application for uploading and processing files (CSV, TXT, Excel, PDF) into Snowflake tables. Features an intuitive 3-step workflow with automatic navigation, bulk operations, and real-time progress tracking.

![File Extract Demo](File_Extract.gif)

## Table of Contents
- [Features](#features)
- [Quick Start](#quick-start)
- [User Interface](#user-interface)
- [Architecture](#architecture)
- [Setup Guide](#setup-guide)
- [Usage](#usage)
- [File Processing](#file-processing)
- [Troubleshooting](#troubleshooting)
- [Technical Details](#technical-details)

---

## Features

### 🚀 Core Functionality
- **Multi-File Upload**: Upload multiple files simultaneously (CSV, TXT, Excel, PDF)
- **3-Step Workflow**: Upload → Process → View Tables with automatic navigation
- **Bulk Operations**: Process or clear all files at once with progress tracking
- **Excel Multi-Sheet Support**: Each Excel sheet becomes a separate table
- **Stage Management**: Files move through lifecycle stages (Raw → Processing → Completed/Error)
- **Real-Time Progress**: Live progress bars and status updates
- **Source Tracking**: Every table includes `SOURCE_FILE_NAME` column for lineage

### ✨ UI/UX Features
- **Automatic Navigation**: Moves to next step after successful completion
- **Compact Metrics**: Clean, space-efficient summary cards
- **Progress Display**: Single-line status updates for each file being processed
- **Responsive Layout**: Full-width display with optimized spacing
- **Visual Feedback**: Balloons 🎈 and success messages on completion

### 🔧 Technical Features
- **Snowpark Integration**: Native Snowflake operations (no PUT/GET restrictions)
- **Session State Management**: Persistent file tracking across reruns
- **Error Handling**: Failed files automatically move to error stage
- **Original Filenames**: Files preserve their actual names (no temp names)
- **Auto-Refresh**: UI updates automatically after operations

---

## Quick Start

### Prerequisites
- Snowflake account with appropriate permissions
- Snow CLI installed (optional - can deploy via Snowsight)
- Python 3.11+ (for local development)

### 5-Minute Setup

**Option 1: Deploy via Snowsight (Easiest - No CLI needed)**
1. Go to https://app.snowflake.com
2. Run SQL scripts in order:
   - `setup_database.sql` - Creates database and schema
   - `setup_stages.sql` - Creates the 4 stages
3. Navigate to **Streamlit** → **+ Streamlit App**
4. Name it `FILE_INGEST_APP`
5. Copy/paste contents of `app.py`
6. Upload `environment.yml`
7. Click **Run**

**Option 2: Automated Script**
```bash
chmod +x deploy.sh
./deploy.sh
```

**Option 3: Manual Python Script**
```bash
pip install snowflake-connector-python
python3 manual_deploy.py
```

### Access Your App
- Find it in Snowsight under **Streamlit**
- Or use: `snow streamlit get-url FILE_INGEST_APP`

---

## User Interface

### Workflow Overview
```
Step 1: Upload Files → Step 2: Process Files → Step 3: View Tables
        ↓                      ↓                        ↓
    Select & Upload      Convert to Tables      Query Your Data
```

### Step 1: Upload Files
```
┌─────────────────────────────────────────────────────────┐
│ 📤 Step 1: Upload Files                                 │
├─────────────────────────────────────────────────────────┤
│ ┌────────────┐ ┌────────────┐ ┌───────────────────────┐│
│ │📁 Files: 5 │ │💾 Size:... │ │📤 Upload All to Raw...││
│ └────────────┘ └────────────┘ └───────────────────────┘│
├─────────────────────────────────────────────────────────┤
│ [File Selection Widget]                                 │
├─────────────────────────────────────────────────────────┤
│ 📋 Selected Files:                                      │
│ • file1.xlsx                                            │
│ • file2.csv                                             │
└─────────────────────────────────────────────────────────┘
```

### Step 2: Process Files
```
┌─────────────────────────────────────────────────────────┐
│ 🚀 Bulk Operations                                      │
│ [⚙️ Process All Files] [🗑️ Clear All] 💡 Tip...       │
├─────────────────────────────────────────────────────────┤
│ ━━━━━━━━━━━━━━━━━ 60% Complete                         │
├─────────────────────────────────────────────────────────┤
│ ℹ️ Processing 730803_CompanyName3_Tokio... (3/5)       │
├─────────────────────────────────────────────────────────┤
│ 📄 Files (Individual actions)                           │
│ • file1.xlsx  [⚙️] [🗑️]                                 │
│ • file2.csv   [⚙️] [🗑️]                                 │
└─────────────────────────────────────────────────────────┘
```

### Step 3: View Tables
```
┌─────────────────────────────────────────────────────────┐
│ 📊 View Tables                                          │
│                                                         │
│ Select a table: [Dropdown]                             │
│ Preview first 100 rows                                  │
│ [Table Preview]                                         │
└─────────────────────────────────────────────────────────┘
```

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

### Data Flow
1. **Upload**: Files → RAW_STAGE
2. **Process**: RAW_STAGE → PROCESSING_STAGE → Parse → Create Table
3. **Success**: File removed from stages, data in table
4. **Failure**: File → ERROR_STAGE for debugging
5. **Auto-Navigate**: Success → Move to next step

### Stages Explained
- **RAW_STAGE**: Initial upload location
- **PROCESSING_STAGE**: Temporary storage during processing
- **COMPLETED_STAGE**: Archive of successfully processed files
- **ERROR_STAGE**: Failed files for debugging

---

## Setup Guide

### Database Setup

Run these SQL scripts in order:

```sql
-- 1. Create database and schema
-- From: setup_database.sql
CREATE DATABASE IF NOT EXISTS FILE_INGEST_DB;
USE DATABASE FILE_INGEST_DB;
CREATE SCHEMA IF NOT EXISTS PUBLIC;

-- 2. Create stages
-- From: setup_stages.sql
CREATE OR REPLACE STAGE RAW_STAGE;
CREATE OR REPLACE STAGE PROCESSING_STAGE;
CREATE OR REPLACE STAGE COMPLETED_STAGE;
CREATE OR REPLACE STAGE ERROR_STAGE;

-- 3. (Optional) Create helper procedures
-- From: stored_procedures.sql and file_management.sql
```

### Required Permissions

Your Snowflake role needs:
- `CREATE DATABASE` (or use existing database)
- `CREATE SCHEMA`
- `CREATE STAGE`
- `CREATE TABLE`
- `CREATE STREAMLIT`
- `USAGE` on warehouse

### Deploy Streamlit App

**Via Snowsight:**
1. Navigate to **Streamlit** → **+ Streamlit App**
2. Configure:
   - Name: `FILE_INGEST_APP`
   - Database: `FILE_INGEST_DB`
   - Schema: `PUBLIC`
   - Warehouse: `COMPUTE_WH`
3. Copy entire `app.py` content
4. Upload `environment.yml`
5. Click **Run**

**Via Snow CLI:**
```bash
snow streamlit deploy \
  --file app.py \
  --name FILE_INGEST_APP \
  --database FILE_INGEST_DB \
  --schema PUBLIC \
  --warehouse COMPUTE_WH
```

---

## Usage

### Workflow: Upload → Process → View

#### Step 1: Upload Files

1. Click **Step 1: Upload Files** in sidebar
2. Select files using the file picker
3. Review summary (file count, total size)
4. Click **📤 Upload All to Raw Stage**
5. Watch progress bar and status updates
6. ✅ Auto-navigates to **Step 2: Process Files** on success

#### Step 2: Process Files

**Bulk Processing (Recommended):**
1. Click **⚙️ Process All Files** button
2. Watch real-time processing status
3. See progress: "Processing filename... (2/5)"
4. ✅ Auto-navigates to **Step 3: View Tables** when complete

**Individual File Processing:**
1. Find file in the Files table
2. Click **⚙️** button next to specific file
3. File processes and table is created
4. Page refreshes to show updated file list

**Clear Files:**
- Click **🗑️ Clear All Files** to delete all files from RAW_STAGE
- Or click **🗑️** next to individual files

#### Step 3: View Tables

1. Select table from dropdown
2. View first 100 rows
3. Query tables directly in Snowflake:
   ```sql
   SELECT * FROM FILE_INGEST_DB.PUBLIC.YOUR_TABLE_NAME;
   ```

---

## File Processing

### CSV and TXT Files
```
Input:  sales_data.csv
Output: Table SALES_DATA
```
- Auto-detects delimiter (comma, tab, pipe, etc.)
- Infers schema from data
- Handles quoted fields and special characters
- Adds `SOURCE_FILE_NAME` column

### Excel Files (Multi-Sheet)
```
Input:  quarterly_report.xlsx (Sheets: Q1, Q2, Q3)
Output: 
  - QUARTERLY_REPORT_Q1
  - QUARTERLY_REPORT_Q2
  - QUARTERLY_REPORT_Q3
```
- Each sheet becomes a separate table
- Skips empty sheets
- Preserves data types
- All tables include `SOURCE_FILE_NAME`

### PDF Files
```
Input:  document.pdf
Output: DOCUMENT (metadata table)
```
- Creates metadata table
- Includes: filename, size, timestamp
- Note: Full text extraction requires additional setup

### Table Naming
- Filenames sanitized for Snowflake compatibility
- Special characters → underscores
- Extension removed
- Converted to uppercase

**Examples:**
- `my-data-file.csv` → `MY_DATA_FILE`
- `Sales Report 2024.xlsx` → `SALES_REPORT_2024`
- `751054_Company_Data.txt` → `751054_COMPANY_DATA`

---

## Troubleshooting

### Upload Issues

**Problem**: Upload button doesn't appear after selecting files
**Solution**: 
- Fixed in current version - button now appears immediately
- Files are stored in session state and persist across reruns
- Clear browser cache if issues persist

**Problem**: File upload fails
**Solutions**:
- Check file size limits
- Verify stage permissions: `GRANT READ, WRITE ON STAGE RAW_STAGE TO ROLE YOUR_ROLE`
- Ensure supported file type (CSV, TXT, XLSX, XLS, PDF)

### Processing Issues

**Problem**: Processing fails for Excel files
**Solutions**:
- Verify sheets contain data (not just headers)
- Check for merged cells or complex formatting
- Ensure sheets are not password protected

**Problem**: CSV file not parsing correctly
**Solutions**:
- Verify UTF-8 encoding
- Check delimiter (comma, tab, pipe)
- Ensure no extra blank lines at end of file

**Problem**: Table already exists
**Solution**: App uses `CREATE OR REPLACE TABLE` - should not occur. If it does:
```sql
DROP TABLE IF EXISTS FILE_INGEST_DB.PUBLIC.YOUR_TABLE_NAME;
```

### Connection Issues

**Problem**: Cannot connect to Snowflake
**Solutions**:
- When running in Snowflake, app uses native session (no credentials needed)
- For local development, check `.streamlit/secrets.toml`
- Verify warehouse is running: `ALTER WAREHOUSE COMPUTE_WH RESUME;`

### Performance Issues

**Problem**: Large files process slowly
**Solutions**:
- Use larger warehouse (LARGE_WH, X-LARGE_WH)
- Process files in smaller batches
- Convert large Excel files to CSV first
- Consider breaking into smaller files

---

## Technical Details

### Technology Stack
- **Frontend**: Streamlit 1.50.0
- **Backend**: Snowflake Snowpark
- **File Processing**: Pandas, OpenPyXL, PyPDF2
- **Deployment**: Streamlit in Snowflake (native)

### Key Features Implementation

**Session State Management:**
```python
# Files persist across reruns
st.session_state.staged_files = file_info_list
st.session_state.current_page = "Step 2: Process Files"
```

**Automatic Navigation:**
```python
if uploaded_count == len(file_info_list):
    st.balloons()
    time.sleep(1.5)  # Show success message
    st.session_state.current_page = "Step 2: Process Files"
    st.rerun()
```

**Snowpark Integration:**
```python
# Get native Snowflake session
session = get_active_session()

# Upload with original filename
session.file.put(tmp_path, f"@{stage_name}", 
                 auto_compress=False, overwrite=True)

# Write DataFrame to table
session.write_pandas(df, table_name,
                    database="FILE_INGEST_DB",
                    schema="PUBLIC",
                    overwrite=True)
```

### File Structure
```
streamlit_file_extract/
├── app.py                   # Main application (2100+ lines)
├── environment.yml          # Conda dependencies for Snowflake
├── requirements.txt         # Python package dependencies
├── snowflake.yml           # Snow CLI configuration
├── deploy.sh               # Automated deployment script
├── manual_deploy.py        # Python deployment alternative
├── setup_database.sql      # Database creation
├── setup_stages.sql        # Stage setup
├── stored_procedures.sql   # Optional SQL procedures
├── file_management.sql     # Helper procedures
└── README.md              # This file
```

### Dependencies
```yaml
# environment.yml
channels:
  - snowflake
dependencies:
  - python=3.11
  - streamlit=1.50.0
  - snowflake-snowpark-python=1.42.0
  - pandas=2.0.0
  - openpyxl=3.1.0
```

---

## Best Practices

### File Preparation
- Use UTF-8 encoding for text files
- Avoid merged cells in Excel
- Keep filenames simple (alphanumeric, underscores, hyphens)
- Test with small files first

### Processing Strategy
- Use bulk operations for multiple files
- Process 10-20 files at a time for optimal performance
- Monitor warehouse usage during large operations
- Clean up stages regularly

### Production Recommendations
- Implement file validation before upload
- Set up monitoring for ERROR_STAGE
- Use appropriate warehouse size for workload
- Consider Snowpipe for continuous ingestion
- Implement data quality checks post-processing
- Set up access controls (RBAC)

### Security
- Use OAuth or key-pair authentication
- Implement role-based access control
- Audit file operations via operation logs
- Encrypt sensitive data
- Regularly review permissions

---

## Examples

### Example 1: Simple CSV Upload
```
1. Upload: sales_data.csv
2. Process: Click "Process All Files"
3. Query: SELECT * FROM SALES_DATA LIMIT 10;
```

### Example 2: Excel Multi-Sheet
```
1. Upload: quarterly_report.xlsx (4 sheets)
2. Process: Creates 4 tables
   - QUARTERLY_REPORT_Q1
   - QUARTERLY_REPORT_Q2
   - QUARTERLY_REPORT_Q3
   - QUARTERLY_REPORT_Q4
3. Query all: 
   SELECT 'Q1' AS quarter, * FROM QUARTERLY_REPORT_Q1
   UNION ALL
   SELECT 'Q2' AS quarter, * FROM QUARTERLY_REPORT_Q2;
```

### Example 3: Bulk Processing
```
1. Upload: 10 files (sales_jan.csv through sales_oct.csv)
2. Click: "Process All Files"
3. Watch: Progress updates for each file
4. Result: 10 tables created
5. Auto-navigate: Moves to View Tables
```

---

## Version History

**Version 3.0** (November 14, 2025)
- ✅ Automatic navigation between workflow steps
- ✅ Compact metric cards with reduced whitespace
- ✅ Single-line processing status (full width display)
- ✅ Upload button positioned before file selector
- ✅ Session state management for file persistence
- ✅ Improved UI/UX with real-time updates

**Version 2.0**
- Multi-file upload support
- Bulk processing operations
- Excel multi-sheet support
- Snowpark integration

**Version 1.0**
- Initial release
- Basic file upload and processing

---

## Support

For issues or questions:
1. Review this README thoroughly
2. Check the Troubleshooting section
3. Examine Streamlit logs in Snowsight
4. Test with sample files

---

## License

This project is provided as-is for demonstration and educational purposes.

---

**Application**: File Ingest to Snowflake  
**Version**: 3.0  
**Last Updated**: November 14, 2025  
**Key Features**: 3-Step Workflow, Auto-Navigation, Bulk Operations, Multi-Sheet Excel, Real-Time Progress
