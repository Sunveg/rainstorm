#!/bin/bash

# Cleanup script to remove storage directory and .log files from MP3 directory
# This allows starting fresh for each test run
# Works regardless of where the script is run from (scripts/ or MP3/)

# Find the MP3 root directory
# If we're in scripts/, go up one level; otherwise assume we're in MP3 root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ "$(basename "$SCRIPT_DIR")" = "scripts" ]; then
    MP3_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
else
    MP3_ROOT="$SCRIPT_DIR"
fi

# Change to MP3 root directory
cd "$MP3_ROOT" || exit 1

echo "Cleaning up in MP3 directory: $MP3_ROOT"
echo ""

# Remove entire storage directory
STORAGE_DIR="storage"
if [ -d "$STORAGE_DIR" ]; then
    echo "Removing storage directory..."
    rm -rf "$STORAGE_DIR"
    echo "Deleted storage directory: $STORAGE_DIR"
else
    echo "Storage directory not found: $STORAGE_DIR"
fi

# Remove all .log files in MP3 root directory
echo ""
echo "Cleaning up .log files..."
LOG_COUNT=$(find . -maxdepth 1 -name "*.log" -type f 2>/dev/null | wc -l | tr -d ' ')

if [ "$LOG_COUNT" -gt 0 ]; then
    echo "Found $LOG_COUNT .log file(s):"
    find . -maxdepth 1 -name "*.log" -type f -print | while read -r file; do
        echo "  - $(basename "$file")"
    done
    rm -f ./*.log
    echo "Deleted $LOG_COUNT .log file(s)"
else
    echo "No .log files found"
fi

echo ""
echo "Cleanup complete!"
echo ""
echo "Note: Removed entire storage directory and all .log files from MP3 directory."

