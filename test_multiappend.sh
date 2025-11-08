#!/bin/bash

# Automated multiappend test script
# This script tests the multiappend command end-to-end

set -e

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Configuration
HYDFS_FILE="/hydfs/test_multiappend.txt"
OUTPUT_FILE="result_multiappend.txt"
VMs=("127.0.0.1:5001" "127.0.0.1:5002" "127.0.0.1:5003" "127.0.0.1:5004")
LOCAL_FILES=("test_files/append1.txt" "test_files/append2.txt" "test_files/append3.txt" "test_files/append4.txt")

echo "=========================================="
echo "Multiappend Test Script"
echo "=========================================="
echo ""

# Check if hydfs binary exists
if [ ! -f "./hydfs" ]; then
    echo -e "${RED}Error: hydfs binary not found${NC}"
    echo "Please build first: go build -o hydfs cmd/hydfs/main.go"
    exit 1
fi

# Step 1: Prepare test files
echo -e "${BLUE}Step 1: Preparing test files...${NC}"
mkdir -p test_files

echo "CREATE: Initial content for multiappend test" > test_files/create.txt
echo "APPEND 1 from VM1 (127.0.0.1:5001)" > test_files/append1.txt
echo "APPEND 2 from VM2 (127.0.0.1:5002)" > test_files/append2.txt
echo "APPEND 3 from VM3 (127.0.0.1:5003)" > test_files/append3.txt
echo "APPEND 4 from VM4 (127.0.0.1:5004)" > test_files/append4.txt

echo -e "${GREEN}✓ Test files created${NC}"
echo ""

# Step 2: Check if VMs are running
echo -e "${BLUE}Step 2: Checking VM status...${NC}"
echo ""
echo -e "${YELLOW}NOTE: This script assumes you have 4 VMs running in separate terminals.${NC}"
echo "If not, start them with:"
echo "  Terminal 1: ./hydfs 127.0.0.1 5001"
echo "  Terminal 2: ./hydfs 127.0.0.1 5002 127.0.0.1:5001"
echo "  Terminal 3: ./hydfs 127.0.0.1 5003 127.0.0.1:5001"
echo "  Terminal 4: ./hydfs 127.0.0.1 5004 127.0.0.1:5001"
echo ""
read -p "Press ENTER when all 4 VMs are running and have joined the cluster..."

# Step 3: Create HyDFS file
echo ""
echo -e "${BLUE}Step 3: Creating HyDFS file...${NC}"
echo ""
echo -e "${CYAN}In one of your VM terminals, run:${NC}"
echo "  create test_files/create.txt $HYDFS_FILE"
echo ""
read -p "Press ENTER after you've created the file in one of the VMs..."

# Step 4: Run multiappend
echo ""
echo -e "${BLUE}Step 4: Running multiappend command...${NC}"
echo ""
echo -e "${CYAN}In one of your VM terminals, run:${NC}"
echo "  multiappend $HYDFS_FILE ${VMs[0]} ${LOCAL_FILES[0]} ${VMs[1]} ${LOCAL_FILES[1]} ${VMs[2]} ${LOCAL_FILES[2]} ${VMs[3]} ${LOCAL_FILES[3]}"
echo ""
read -p "Press ENTER after multiappend completes..."

# Step 5: Wait for converger
echo ""
echo -e "${BLUE}Step 5: Waiting for converger to process appends...${NC}"
echo "Waiting 10 seconds for converger to apply all appends..."
sleep 10

# Step 6: Fetch and verify file
echo ""
echo -e "${BLUE}Step 6: Fetching and verifying file...${NC}"
echo ""
echo -e "${CYAN}In one of your VM terminals, run:${NC}"
echo "  get $HYDFS_FILE $OUTPUT_FILE"
echo ""
read -p "Press ENTER after you've fetched the file..."

if [ ! -f "$OUTPUT_FILE" ]; then
    echo -e "${RED}✗ Output file not found: $OUTPUT_FILE${NC}"
    echo "Please fetch the file first"
    exit 1
fi

echo ""
echo "=========================================="
echo "Verification Results"
echo "=========================================="
echo ""

# Check file content
echo "File content:"
echo "---"
cat "$OUTPUT_FILE"
echo "---"
echo ""

# Count appends
APPEND_COUNT=$(grep -c "APPEND" "$OUTPUT_FILE" 2>/dev/null || echo "0")
APPEND_COUNT=$(echo "$APPEND_COUNT" | tr -d ' \n')
echo "Append count: $APPEND_COUNT / 4"

if [ -n "$APPEND_COUNT" ] && [ "$APPEND_COUNT" -eq 4 ] 2>/dev/null; then
    echo -e "${GREEN}✓ All 4 appends present${NC}"
else
    echo -e "${RED}✗ Missing appends (expected 4, got $APPEND_COUNT)${NC}"
fi

# Check for each append
echo ""
echo "Checking for each append:"
for i in 1 2 3 4; do
    if grep -q "APPEND $i" "$OUTPUT_FILE"; then
        echo -e "${GREEN}  ✓ Append $i found${NC}"
    else
        echo -e "${RED}  ✗ Append $i missing${NC}"
    fi
done

# Check file size
FILE_SIZE=$(wc -c < "$OUTPUT_FILE" | tr -d ' ')
echo ""
echo "File size: $FILE_SIZE bytes"

# Step 7: Check logs
echo ""
echo "=========================================="
echo "Log Analysis"
echo "=========================================="
echo ""

# Find log files
LOG_FILES=($(ls -t hydfs_*.log 2>/dev/null | head -4))

if [ ${#LOG_FILES[@]} -eq 0 ]; then
    echo -e "${YELLOW}⚠ No log files found${NC}"
else
    echo "Found ${#LOG_FILES[@]} log file(s)"
    echo ""
    
    # Check for assigned opIDs
    echo "Checking assigned operation IDs:"
    ALL_OPIDS=""
    for LOG_FILE in "${LOG_FILES[@]}"; do
        OPIDS=$(grep "Assigned next operation ID" "$LOG_FILE" 2>/dev/null | grep "$HYDFS_FILE" | \
            sed -n 's/.*Assigned next operation ID for file.*: \([0-9]*\).*/\1/p' | sort -n)
        if [ -n "$OPIDS" ]; then
            echo "  $LOG_FILE: $(echo "$OPIDS" | tr '\n' ' ')"
            ALL_OPIDS="$ALL_OPIDS$OPIDS"$'\n'
        fi
    done
    
    if [ -n "$ALL_OPIDS" ]; then
        ALL_OPIDS=$(echo "$ALL_OPIDS" | grep -v '^$' | sort -n | uniq)
        UNIQUE_COUNT=$(echo "$ALL_OPIDS" | wc -l | tr -d ' ')
        echo ""
        echo "Unique opIDs found: $UNIQUE_COUNT"
        echo "OpIDs: $(echo "$ALL_OPIDS" | tr '\n' ' ')"
        
        if [ "$UNIQUE_COUNT" -eq 4 ]; then
            echo -e "${GREEN}✓ All 4 appends have unique opIDs${NC}"
        else
            echo -e "${YELLOW}⚠ Expected 4 unique opIDs, found $UNIQUE_COUNT${NC}"
        fi
    fi
    
    # Check for replica received logs
    echo ""
    echo "Checking replica received logs:"
    for LOG_FILE in "${LOG_FILES[@]}"; do
        RECEIVED=$(grep -c "REPLICA RECEIVED APPEND" "$LOG_FILE" 2>/dev/null | tr -d '\n' || echo "0")
        RECEIVED=$(echo "$RECEIVED" | tr -d ' ')
        if [ -n "$RECEIVED" ] && [ "$RECEIVED" -gt 0 ] 2>/dev/null; then
            echo "  $LOG_FILE: $RECEIVED append(s) received"
        fi
    done
    
    # Check for converger completion
    echo ""
    echo "Checking converger completion:"
    for LOG_FILE in "${LOG_FILES[@]}"; do
        COMPLETED=$(grep -c "REPLICA COMPLETED APPEND" "$LOG_FILE" 2>/dev/null | tr -d '\n' || echo "0")
        COMPLETED=$(echo "$COMPLETED" | tr -d ' ')
        if [ -n "$COMPLETED" ] && [ "$COMPLETED" -gt 0 ] 2>/dev/null; then
            echo "  $LOG_FILE: $COMPLETED append(s) completed"
        fi
    done
fi

# Summary
echo ""
echo "=========================================="
echo "Summary"
echo "=========================================="
echo ""

SUCCESS=true

if [ -z "$APPEND_COUNT" ] || [ "$APPEND_COUNT" -ne 4 ] 2>/dev/null; then
    SUCCESS=false
    echo -e "${RED}✗ Not all appends present in file${NC}"
else
    echo -e "${GREEN}✓ All appends present in file${NC}"
fi

if [ "$SUCCESS" = true ]; then
    echo ""
    echo -e "${GREEN}==========================================${NC}"
    echo -e "${GREEN}Multiappend test PASSED!${NC}"
    echo -e "${GREEN}==========================================${NC}"
else
    echo ""
    echo -e "${RED}==========================================${NC}"
    echo -e "${RED}Multiappend test FAILED${NC}"
    echo -e "${RED}==========================================${NC}"
    echo ""
    echo "Troubleshooting:"
    echo "  1. Check that all 4 VMs are running"
    echo "  2. Verify all VMs have joined the cluster (use 'membership' command)"
    echo "  3. Check log files for errors"
    echo "  4. Wait longer for converger to process (run 'get' command again)"
fi

echo ""
echo "Output file: $OUTPUT_FILE"
echo "Log files: ${LOG_FILES[*]}"
echo ""

