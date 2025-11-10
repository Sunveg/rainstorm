#!/bin/bash

# Script to kill all processes on ports 8001-8010 and 9001-9010

echo "Killing processes on ports 8001-8010 and 9001-9010..."

# Function to kill process on a specific port
kill_port() {
    local port=$1
    # Find PIDs listening on the port
    local pids=$(lsof -ti :$port 2>/dev/null)
    
    if [ -z "$pids" ]; then
        echo "  Port $port: No process found"
    else
        for pid in $pids; do
            echo "  Port $port: Killing process $pid"
            kill -9 $pid 2>/dev/null
        done
    fi
}

# Kill processes on ports 8001-8010
echo ""
echo "Checking ports 8001-8010:"
for port in {8001..8010}; do
    kill_port $port
done

# Kill processes on ports 9001-9010
echo ""
echo "Checking ports 9001-9010:"
for port in {9001..9010}; do
    kill_port $port
done

echo ""
echo "Done! All processes on specified ports have been terminated."

