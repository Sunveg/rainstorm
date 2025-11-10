#!/bin/bash

# Cleanup script to remove all .txt and .tmp files from storage directory
# and .log files from the MP3 directory
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

# Clean up .log files in the current directory (MP3 directory)
echo ""
echo "Cleaning up .log files in current directory..."
LOG_COUNT=$(find . -maxdepth 1 -name "*.log" -type f | wc -l | tr -d ' ')

if [ "$LOG_COUNT" -gt 0 ]; then
    echo "Found $LOG_COUNT .log file(s):"
    find . -maxdepth 1 -name "*.log" -type f | while read -r file; do
        echo "  - $file"
        rm "$file"
    done
    echo "Deleted $LOG_COUNT .log file(s)"
else
    echo "No .log files found in current directory"
fi

echo ""
echo "Cleanup complete!"
echo ""
echo "Note: This deletes .txt and .tmp files from storage directory,"
echo "      and .log files from the MP3 directory."
echo "      Directory structure (OwnedFiles, ReplicatedFiles, TempFiles) is preserved."

