#!/bin/bash

# Cleanup script to remove all .txt and .tmp files from storage directory
# This allows starting fresh for each test run

STORAGE_DIR="storage"

echo "Cleaning up storage directory..."

if [ ! -d "$STORAGE_DIR" ]; then
    echo "Storage directory not found. Nothing to clean."
    exit 0
fi

# Count files before deletion
TXT_COUNT=$(find "$STORAGE_DIR" -name "*.txt" -type f | wc -l | tr -d ' ')
TMP_COUNT=$(find "$STORAGE_DIR" -name "*.tmp" -type f | wc -l | tr -d ' ')

echo "Found $TXT_COUNT .txt files and $TMP_COUNT .tmp files"

# Delete all .txt files
if [ "$TXT_COUNT" -gt 0 ]; then
    echo ""
    echo "Deleting .txt files:"
    find "$STORAGE_DIR" -name "*.txt" -type f | while read -r file; do
        echo "  - $file"
        rm "$file"
    done
    echo "Deleted $TXT_COUNT .txt files"
fi

# Delete all .tmp files
if [ "$TMP_COUNT" -gt 0 ]; then
    echo ""
    echo "Deleting .tmp files:"
    find "$STORAGE_DIR" -name "*.tmp" -type f | while read -r file; do
        echo "  - $file"
        rm "$file"
    done
    echo "Deleted $TMP_COUNT .tmp files"
fi

echo "Cleanup complete!"
echo ""
echo "Note: This only deletes .txt and .tmp files."
echo "      Directory structure (OwnedFiles, ReplicatedFiles, TempFiles) is preserved."

