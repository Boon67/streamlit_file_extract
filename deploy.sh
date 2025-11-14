#!/bin/bash

# Deployment script for File Extract Streamlit App
# This script sets up Snowflake objects and deploys the Streamlit app
# Usage: ./deploy.sh [--app-only]
#   --app-only: Only deploy the Streamlit app, skip SQL setup

# Don't exit on error for individual steps - we want to continue and report all issues

# Parse command line arguments
APP_ONLY=false
if [[ "$1" == "--app-only" ]] || [[ "$1" == "-a" ]]; then
    APP_ONLY=true
    echo "🚀 Deploying Streamlit app only (skipping SQL setup)..."
else
    echo "🚀 Starting full deployment of File Extract App to Snowflake..."
fi

# Check if Snow CLI is installed
if ! command -v snow &> /dev/null; then
    echo "❌ Snow CLI is not installed. Please install it first:"
    echo "   pip install snowflake-cli-labs"
    exit 1
fi

echo "✅ Snow CLI found"

if [ "$APP_ONLY" = false ]; then
    # Step 1: Setup database and schema
    echo ""
    echo "🗄️  Step 1: Creating database and schema..."
    snow sql -f setup_database.sql || {
        echo "⚠️  Warning: Could not run setup_database.sql automatically"
        echo "   Please run it manually in Snowflake"
        echo "   Note: You may need ACCOUNTADMIN role to create database/warehouse"
    }

    # Step 2: Setup Snowflake stages
    echo ""
    echo "📦 Step 2: Creating Snowflake stages..."
    snow sql -f setup_stages.sql || {
        echo "⚠️  Warning: Could not run setup_stages.sql automatically"
        echo "   Please run it manually in Snowflake"
    }

    # Step 3: Create stored procedures
    echo ""
    echo "⚙️  Step 3: Creating stored procedures..."
    snow sql -f stored_procedures.sql || {
        echo "⚠️  Warning: Could not run stored_procedures.sql automatically"
        echo "   Please run it manually in Snowflake"
    }

    # Step 4: Create file management procedures
    echo ""
    echo "📁 Step 4: Creating file management procedures..."
    snow sql -f file_management.sql || {
        echo "⚠️  Warning: Could not run file_management.sql automatically"
        echo "   Please run it manually in Snowflake"
    }
fi

# Deploy Streamlit app
if [ "$APP_ONLY" = false ]; then
    echo ""
    echo "🎨 Step 5: Deploying Streamlit app..."
else
    echo ""
    echo "🎨 Deploying Streamlit app..."
fi
snow streamlit deploy file_extract_app --database FILE_EXTRACT_DB --schema PUBLIC --replace || {
    echo "❌ Failed to deploy Streamlit app"
    echo "   Please check your Snowflake connection and permissions"
    exit 1
}

echo ""
echo "✅ Deployment complete!"
echo ""
echo "📝 Next steps:"
echo "   1. Open your app: snow object list streamlit"
echo "   2. Get app URL: snow streamlit get-url file_extract_app"
echo "   3. Or access it in Snowflake UI (link shown above)"
echo "   4. Start uploading files!"
echo ""
echo "💡 Note: The app uses your Snowflake session permissions automatically."
echo "   No secrets.toml configuration needed when running in Snowflake."

