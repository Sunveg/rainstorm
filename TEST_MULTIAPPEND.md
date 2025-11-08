# Testing multiappend Command

This guide shows how to test and verify the `multiappend` command works correctly.

---

## Prerequisites

### 1. Start 4 VMs

Open **4 separate terminal windows**:

**Terminal 1 - VM1 (127.0.0.1:5001)**
```bash
cd /Users/sunvegnalwar/Fall\ 25\ UIUC/CS\ 425/MP3/MP3
./hydfs 127.0.0.1 5001
```

**Terminal 2 - VM2 (127.0.0.1:5002)**
```bash
cd /Users/sunvegnalwar/Fall\ 25\ UIUC/CS\ 425/MP3/MP3
./hydfs 127.0.0.1 5002 127.0.0.1:5001
```

**Terminal 3 - VM3 (127.0.0.1:5003)**
```bash
cd /Users/sunvegnalwar/Fall\ 25\ UIUC/CS\ 425/MP3/MP3
./hydfs 127.0.0.1 5003 127.0.0.1:5001
```

**Terminal 4 - VM4 (127.0.0.1:5004)**
```bash
cd /Users/sunvegnalwar/Fall\ 25\ UIUC/CS\ 425/MP3/MP3
./hydfs 127.0.0.1 5004 127.0.0.1:5001
```

**Wait for all nodes to join the cluster** (check `membership` command on each node).

---

## Step 1: Prepare Test Files

### Option A: Use Shared Directory (Recommended)

If all VMs share the same filesystem (e.g., NFS or local testing):

```bash
# Create test files in a shared location
mkdir -p test_files
echo "APPEND 1 from VM1" > test_files/append1.txt
echo "APPEND 2 from VM2" > test_files/append2.txt
echo "APPEND 3 from VM3" > test_files/append3.txt
echo "APPEND 4 from VM4" > test_files/append4.txt
echo "CREATE: Initial content" > test_files/create.txt
```

### Option B: Create Files on Each VM

If VMs have separate filesystems, create files on each VM:

**On VM1:**
```bash
echo "APPEND 1 from VM1" > file1.txt
```

**On VM2:**
```bash
echo "APPEND 2 from VM2" > file2.txt
```

**On VM3:**
```bash
echo "APPEND 3 from VM3" > file3.txt
```

**On VM4:**
```bash
echo "APPEND 4 from VM4" > file4.txt
```

---

## Step 2: Create the HyDFS File

**On any VM (e.g., VM1):**
```bash
create test_files/create.txt /hydfs/test_multiappend.txt
```

Wait for CREATE to complete.

---

## Step 3: Run multiappend

**On any VM (e.g., VM1):**
```bash
multiappend /hydfs/test_multiappend.txt 127.0.0.1:5001 test_files/append1.txt 127.0.0.1:5002 test_files/append2.txt 127.0.0.1:5003 test_files/append3.txt 127.0.0.1:5004 test_files/append4.txt
```

### Expected Output:
```
Launching 4 concurrent appends to /hydfs/test_multiappend.txt...
  Sending append from VM 127.0.0.1:5001 (file: test_files/append1.txt)...
  Sending append from VM 127.0.0.1:5002 (file: test_files/append2.txt)...
  Sending append from VM 127.0.0.1:5003 (file: test_files/append3.txt)...
  Sending append from VM 127.0.0.1:5004 (file: test_files/append4.txt)...
  ✓ Success: VM 127.0.0.1:5001
  ✓ Success: VM 127.0.0.1:5002
  ✓ Success: VM 127.0.0.1:5003
  ✓ Success: VM 127.0.0.1:5004
>>> CLIENT: MULTIAPPEND COMPLETED for /hydfs/test_multiappend.txt (all 4 VMs) <<<
```

---

## Step 4: Verify Results

### 4.1: Check File Content

**On any VM:**
```bash
get /hydfs/test_multiappend.txt result.txt
cat result.txt
```

### Expected Content:
```
CREATE: Initial content
APPEND 1 from VM1
APPEND 2 from VM2
APPEND 3 from VM3
APPEND 4 from VM4
```

**Note:** The order of appends may vary due to concurrency, but all 4 should be present.

---

### 4.2: Check Logs for Concurrent Execution

**On the owner node (check which node owns the file):**
```bash
# Find the owner node
ls /hydfs/test_multiappend.txt
```

**Then check logs on that node:**
```bash
# Look for concurrent append requests
grep "Assigned next operation ID" hydfs_127.0.0.1_*.log | grep test_multiappend

# Should show 4 different opIDs assigned (e.g., 2, 3, 4, 5)
```

**Check logs on each VM to see when they received the append:**
```bash
# On VM1
grep "REPLICA RECEIVED APPEND" hydfs_127.0.0.1_5001.log | grep test_multiappend

# On VM2
grep "REPLICA RECEIVED APPEND" hydfs_127.0.0.1_5002.log | grep test_multiappend

# On VM3
grep "REPLICA RECEIVED APPEND" hydfs_127.0.0.1_5003.log | grep test_multiappend

# On VM4
grep "REPLICA RECEIVED APPEND" hydfs_127.0.0.1_5004.log | grep test_multiappend
```

All 4 VMs should show they received append requests.

---

### 4.3: Verify All Appends Are Present

```bash
# Count APPEND mentions
grep -c "APPEND" result.txt
# Should output: 4

# Check for each append
grep "APPEND 1" result.txt && echo "✓ Append 1 found"
grep "APPEND 2" result.txt && echo "✓ Append 2 found"
grep "APPEND 3" result.txt && echo "✓ Append 3 found"
grep "APPEND 4" result.txt && echo "✓ Append 4 found"
```

---

### 4.4: Check Operation IDs

**On the owner node:**
```bash
# Check assigned opIDs (should be unique and sequential)
grep "Assigned next operation ID" hydfs_127.0.0.1_*.log | grep test_multiappend | \
  sed -n 's/.*Assigned next operation ID for file.*: \([0-9]*\).*/\1/p' | sort -n

# Should show: 2, 3, 4, 5 (or similar sequential IDs)
```

---

## Step 5: Verify Concurrent Execution (Demo Requirement)

The demo requires showing that appends were performed **concurrently** at the replicas.

### Check Replica Logs:

**On each replica node, check the timestamps:**
```bash
# On VM2 (if it's a replica)
grep "REPLICA RECEIVED APPEND" hydfs_127.0.0.1_5002.log | grep test_multiappend

# On VM3 (if it's a replica)
grep "REPLICA RECEIVED APPEND" hydfs_127.0.0.1_5003.log | grep test_multiappend

# On VM4 (if it's a replica)
grep "REPLICA RECEIVED APPEND" hydfs_127.0.0.1_5004.log | grep test_multiappend
```

**Check converger logs on replicas:**
```bash
# Should show appends being processed concurrently
grep "REPLICA COMPLETED APPEND" hydfs_127.0.0.1_*.log | grep test_multiappend
```

The timestamps should show that multiple appends were received and processed around the same time (within seconds of each other).

---

## Success Criteria

✅ **All tests pass if:**
1. All 4 VMs report success in multiappend output
2. File contains all 4 appends
3. All 4 appends have unique, sequential opIDs
4. Replica logs show concurrent append reception
5. Converger logs show appends being processed
6. Final file content matches expected (all appends present)

---

## Troubleshooting

### Issue: "local file does not exist"
**Solution:** Make sure the local file exists on the target VM. Use shared storage or create files on each VM.

### Issue: "VM returned error"
**Solution:** 
- Check if the VM is running
- Check if the VM has the local file
- Check VM logs for detailed error messages

### Issue: Some appends missing
**Solution:**
- Wait longer for converger to process (5-10 seconds)
- Check converger logs on owner node
- Verify all VMs are in the cluster (`membership` command)

### Issue: Appends not concurrent
**Solution:**
- Check that all VMs are running
- Verify network connectivity between VMs
- Check logs to see if requests were sent simultaneously

---

## Quick Verification Script

Save this as `verify_multiappend.sh`:

```bash
#!/bin/bash

HYDFS_FILE="/hydfs/test_multiappend.txt"
OUTPUT_FILE="result.txt"

echo "Verifying multiappend results..."
echo ""

# Fetch file
echo "Fetching file..."
# (Run this manually: get $HYDFS_FILE $OUTPUT_FILE)

if [ ! -f "$OUTPUT_FILE" ]; then
    echo "Error: File not found. Run: get $HYDFS_FILE $OUTPUT_FILE"
    exit 1
fi

# Check content
echo "File content:"
cat "$OUTPUT_FILE"
echo ""

# Count appends
APPEND_COUNT=$(grep -c "APPEND" "$OUTPUT_FILE" 2>/dev/null || echo "0")
echo "Append count: $APPEND_COUNT"

if [ "$APPEND_COUNT" -eq 4 ]; then
    echo "✓ All 4 appends present"
else
    echo "✗ Missing appends (expected 4, got $APPEND_COUNT)"
fi

# Check for each append
for i in 1 2 3 4; do
    if grep -q "APPEND $i" "$OUTPUT_FILE"; then
        echo "✓ Append $i found"
    else
        echo "✗ Append $i missing"
    fi
done
```

---

## Next Steps

After verifying multiappend works:
1. Run `merge` command to ensure consistency
2. Use `getfromreplica` to verify replicas are identical
3. Test with more VMs (up to 10 as per demo requirements)

