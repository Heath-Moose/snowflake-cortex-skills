#!/bin/bash

# Download DCM project files from the latest deployment
# Usage: scripts/download_project.sh <project_name> --connection <connection> --target <target_folder>

set -e

# Parse arguments
PROJECT_NAME=""
CONNECTION=""
TARGET=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --connection)
            CONNECTION="$2"
            shift 2
            ;;
        --target)
            TARGET="$2"
            shift 2
            ;;
        *)
            if [[ -z "$PROJECT_NAME" ]]; then
                PROJECT_NAME="$1"
            fi
            shift
            ;;
    esac
done

# Validate required arguments
if [[ -z "$PROJECT_NAME" ]]; then
    echo "Error: Project name is required"
    echo "Usage: $0 <project_name> --connection <connection> --target <target_folder>"
    exit 1
fi

if [[ -z "$CONNECTION" ]]; then
    echo "Error: --connection is required"
    echo "Usage: $0 <project_name> --connection <connection> --target <target_folder>"
    exit 1
fi

if [[ -z "$TARGET" ]]; then
    echo "Error: --target is required"
    echo "Usage: $0 <project_name> --connection <connection> --target <target_folder>"
    exit 1
fi

echo "=== DCM Project Download ==="
echo "Project: $PROJECT_NAME"
echo "Connection: $CONNECTION"
echo "Target: $TARGET"
echo ""

# Get project description
echo "Fetching project description..."
snow dcm describe "$PROJECT_NAME" -c "$CONNECTION"
echo ""

# Get deployments and extract the latest one
echo "Fetching deployments..."
DEPLOYMENTS_JSON=$(snow dcm list-deployments "$PROJECT_NAME" -c "$CONNECTION" --format json)

# Extract deployment_file_path from the latest deployment (first in list, sorted by created_on desc)
# The JSON format is: [{"created_on": "...", "deployment_file_path": "snow://project/.../deployments/deployment$1/", ...}]
# We extract the deployment path using grep and sed
DEPLOYMENT_PATH=$(echo "$DEPLOYMENTS_JSON" | grep -o '"deployment_file_path"[[:space:]]*:[[:space:]]*"[^"]*"' | head -1 | sed 's/"deployment_file_path"[[:space:]]*:[[:space:]]*"//' | sed 's/"$//')

if [[ -z "$DEPLOYMENT_PATH" ]]; then
    echo "Error: Could not find any deployments for project $PROJECT_NAME"
    exit 1
fi

echo "Latest deployment path: $DEPLOYMENT_PATH"

# Extract the deployment identifier (e.g., "deployment$1") from the path
# Path format: snow://project/DCM_TEMP.BLAH.ANALYTICS_PIPELINE/deployments/deployment$1/
DEPLOYMENT_ID=$(echo "$DEPLOYMENT_PATH" | grep -o 'deployments/[^/]*' | sed 's/deployments\///')

echo "Deployment ID: $DEPLOYMENT_ID"
echo ""

# Construct the sources path for listing files
SOURCES_PATH="snow://project/${PROJECT_NAME}/deployments/${DEPLOYMENT_ID}/sources"

echo "Listing files from: $SOURCES_PATH"

# List all files in the sources folder
# Use single quotes to prevent shell interpretation of $
FILES_JSON=$(snow stage list-files "$SOURCES_PATH" -c "$CONNECTION" --format json)

# Extract file names from JSON
# Format: [{"name": "/deployments/deployment$1/sources/definitions/infrastructure.sql", ...}, ...]
FILE_NAMES=$(echo "$FILES_JSON" | grep -o '"name"[[:space:]]*:[[:space:]]*"[^"]*"' | sed 's/"name"[[:space:]]*:[[:space:]]*"//' | sed 's/"$//')

# Create target directory
mkdir -p "$TARGET"

echo ""
echo "Downloading files..."

# Process each file
echo "$FILE_NAMES" | while IFS= read -r FILE_PATH; do
    # Skip empty lines
    if [[ -z "$FILE_PATH" ]]; then
        continue
    fi
    
    # Skip files in sources/out directory
    if echo "$FILE_PATH" | grep -q '/sources/out/'; then
        echo "Skipping (in out/): $FILE_PATH"
        continue
    fi
    
    # Extract the relative path after sources/
    # File path format: /deployments/deployment$1/sources/definitions/infrastructure.sql
    RELATIVE_PATH=$(echo "$FILE_PATH" | sed 's|.*/sources/||')
    
    # Get the directory part of the relative path
    FILE_DIR=$(dirname "$RELATIVE_PATH")
    
    # Create the target directory structure
    if [[ "$FILE_DIR" != "." ]]; then
        mkdir -p "${TARGET}/${FILE_DIR}"
    fi
    
    # Construct the full snow:// URL for this file
    FILE_URL="snow://project/${PROJECT_NAME}/deployments/${DEPLOYMENT_ID}/sources/${RELATIVE_PATH}"
    
    # Determine the target directory for this file
    if [[ "$FILE_DIR" == "." ]]; then
        COPY_TARGET="$TARGET"
    else
        COPY_TARGET="${TARGET}/${FILE_DIR}"
    fi
    
    echo "Downloading: $RELATIVE_PATH"
    snow stage copy "$FILE_URL" "$COPY_TARGET" -c "$CONNECTION"
done

echo ""
echo "=== Download Complete ==="
echo "Files downloaded to: $TARGET"
