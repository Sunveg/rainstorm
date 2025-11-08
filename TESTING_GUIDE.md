# HyDFS Testing Guide - Demo Preparation

This guide walks through testing CREATE and APPEND operations based on the MP3 demo requirements.

---

## Prerequisites

### 1. Build the Binary

```bash
cd /Users/sunvegnalwar/Fall\ 25\ UIUC/CS\ 425/MP3/MP3
go build -o hydfs cmd/hydfs/main.go
```

### 2. Prepare Test Files

```bash
# Create test directory
mkdir -p test_files

# Create sample files for testing
echo "This is business file 1 content for CREATE testing" > test_files/business_1.txt
echo "This is business file 2 content for CREATE testing" > test_files/business_2.txt
echo "This is business file 3 content for CREATE testing" > test_files/business_3.txt
echo "This is business file 4 content for CREATE testing" > test_files/business_4.txt
echo "This is business file 5 content for CREATE testing" > test_files/business_5.txt

# Create files for APPEND testing
echo "APPEND DATA 1: First append content" > test_files/append_1.txt
echo "APPEND DATA 2: Second append content" > test_files/append_2.txt
echo "APPEND DATA 3: Third append content" > test_files/append_3.txt
echo "APPEND DATA 4: Fourth append content" > test_files/append_4.txt

# Verify files exist
ls -lh test_files/
```

---

## Test Setup: Start 4 VMs

Open **4 separate terminal windows**.

### Terminal 1 - Node VM1 (Introducer)
```bash
cd /Users/sunvegnalwar/Fall\ 25\ UIUC/CS\ 425/MP3/MP3
./hydfs 127.0.0.1 5001
```

### Terminal 2 - Node VM2
```bash
cd /Users/sunvegnalwar/Fall\ 25\ UIUC/CS\ 425/MP3/MP3
./hydfs 127.0.0.1 5002 127.0.0.1:5001
```

### Terminal 3 - Node VM3
```bash
cd /Users/sunvegnalwar/Fall\ 25\ UIUC/CS\ 425/MP3/MP3
./hydfs 127.0.0.1 5003 127.0.0.1:5001
```

### Terminal 4 - Node VM4
```bash
cd /Users/sunvegnalwar/Fall\ 25\ UIUC/CS\ 425/MP3/MP3
./hydfs 127.0.0.1 5004 127.0.0.1:5001
```

**Wait 5 seconds** for all nodes to join the cluster.

---

## Verify Cluster Formation

In **any terminal**, run:

```bash
hydfs> membership
```

**Expected output:**
```
Cluster membership (4 nodes):
  127.0.0.1:5001: ALIVE
  127.0.0.1:5002: ALIVE
  127.0.0.1:5003: ALIVE
  127.0.0.1:5004: ALIVE
```

Check hash ring:
```bash
hydfs> ring
```

**Expected output:**
```
Hash ring (4 nodes):
  127.0.0.1:5001 -> xxxxxxxx
  127.0.0.1:5002 -> yyyyyyyy
  127.0.0.1:5003 -> zzzzzzzz
  127.0.0.1:5004 -> aaaaaaaa
```

---

## Test 1: CREATE Operations [5%]

**Requirement:** Create 5 files one after the other from any VM.

### Step 1: Create Files

In **Terminal 1 (VM1)**, run:

```bash
hydfs> create test_files/business_1.txt /hydfs/file1.txt
hydfs> create test_files/business_2.txt /hydfs/file2.txt
hydfs> create test_files/business_3.txt /hydfs/file3.txt
hydfs> create test_files/business_4.txt /hydfs/file4.txt
hydfs> create test_files/business_5.txt /hydfs/file5.txt
```

**Expected output for each:**
```
Creating file /hydfs/fileX.txt in HyDFS from local file test_files/business_X.txt (XX bytes)...
Successfully created file /hydfs/fileX.txt in HyDFS
```

### Step 2: Verify Files Exist

```bash
hydfs> list
```

**Expected output:**
```
Found 5 files:
  /hydfs/file1.txt
  /hydfs/file2.txt
  /hydfs/file3.txt
  /hydfs/file4.txt
  /hydfs/file5.txt
```

### Step 3: Check Replica Logs

**Watch Terminal 2, 3, 4** for these logs:

**Owner Terminal (depends on hash):**
```
[HyDFS] >>> REPLICATING FILE '/hydfs/file1.txt' TO VMs: [127.0.0.1:5002, 127.0.0.1:5003] <<<
[HyDFS] CREATE completed successfully - file: /hydfs/file1.txt
```

**Replica Terminals:**
```
[HyDFS] >>> REPLICA RECEIVED CREATE REQUEST: file=/hydfs/file1.txt, from=127.0.0.1:5001 <<<
[HyDFS] >>> REPLICA COMPLETED CREATE: file=/hydfs/file1.txt <<<
```

✅ **Demo Point:** TAs will see concurrent replication logs!

---

## Test 2: GET + Correct Replicas [10%]

**Requirement:** Get file from different VM, verify content, check replicas on ring.

### Check 1: GET [5%]

Pick a file (e.g., `/hydfs/file2.txt`) and get from **different VM** than creator.

In **Terminal 2 (VM2)**, run:

```bash
hydfs> get /hydfs/file2.txt test_files/retrieved_file2.txt
```

**Expected output:**
```
Retrieving file /hydfs/file2.txt...
Successfully saved XX bytes to test_files/retrieved_file2.txt
```

**Verify content is identical:**

In a **new terminal** (outside hydfs CLI):
```bash
diff test_files/business_2.txt test_files/retrieved_file2.txt
```

**Expected:** No output (files are identical) ✅

### Check 2: Correct Replicas on Ring [5%]

**TA will pick a file and verify it's on correct ring positions.**

For `/hydfs/file2.txt`, run in **any terminal**:

```bash
hydfs> ls /hydfs/file2.txt
```

**Expected output:**
```
File: /hydfs/file2.txt
FileID: abc123def456789...
Owner: 127.0.0.1:5002
Replicas: [127.0.0.1:5003, 127.0.0.1:5004]
```

**Show sorted membership list:**
```bash
hydfs> list_mem_ids
```

**Expected output:**
```
Sorted Membership Ring:
  Position 1: 127.0.0.1:5001 (Ring ID: 0x12345678)
  Position 2: 127.0.0.1:5002 (Ring ID: 0x23456789)
  Position 3: 127.0.0.1:5003 (Ring ID: 0x34567890)
  Position 4: 127.0.0.1:5004 (Ring ID: 0x45678901)
```

**TA verifies:** FileID → Owner is first successor, replicas are next 2 successors ✅

**Show file on specific VM:**

In **Terminal 3 (VM3)**, run:
```bash
hydfs> liststore
```

**Expected output:**
```
Node ID on ring: 127.0.0.1:5003 (Ring ID: 0x34567890)

Files stored on this node (X files):
  FileName: /hydfs/file2.txt, FileID: abc123def456789...
  FileName: /hydfs/file3.txt, FileID: def456789abc123...
```

✅ **Demo Point:** File is stored on correct replicas based on ring position!

---

## Test 4: Read-My-Writes + Ordering [15%]

**Requirement:** Append 2 files sequentially, verify both appends exist in correct order.

### Step 1: Pick a File and VM

In **Terminal 2 (VM2)**, we'll append to `/hydfs/file3.txt`:

```bash
hydfs> append test_files/append_1.txt /hydfs/file3.txt
```

**Expected output:**
```
Appending 34 bytes from test_files/append_1.txt to HyDFS file /hydfs/file3.txt...
Successfully appended to file /hydfs/file3.txt
```

**Immediately after**, append second file:

```bash
hydfs> append test_files/append_2.txt /hydfs/file3.txt
```

**Expected output:**
```
Appending 35 bytes from test_files/append_2.txt to HyDFS file /hydfs/file3.txt...
Successfully appended to file /hydfs/file3.txt
```

### Step 2: Verify Read-My-Writes

**From SAME VM (VM2)** that did the appends:

```bash
hydfs> get /hydfs/file3.txt test_files/after_append.txt
```

**Check file contains both appends:**

In a **new terminal**:
```bash
cat test_files/after_append.txt
```

**Expected output:**
```
This is business file 3 content for CREATE testing
APPEND DATA 1: First append content
APPEND DATA 2: Second append content
```

### Step 3: Verify Ordering

**Check append order using grep:**

```bash
grep -n "business file 3" test_files/after_append.txt
grep -n "APPEND DATA 1" test_files/after_append.txt
grep -n "APPEND DATA 2" test_files/after_append.txt
```

**Expected output:**
```
1:This is business file 3 content for CREATE testing
2:APPEND DATA 1: First append content
3:APPEND DATA 2: Second append content
```

✅ **Demo Points:**
- **Read-my-writes:** Client sees its own appends immediately
- **Ordering:** Appends are in correct order (1 before 2)

### Step 4: Check Replica Logs

**Watch replica terminals** for these logs:

**Owner:**
```
[HyDFS] >>> REPLICATING APPEND for '/hydfs/file3.txt' (opID=2) TO VMs: [127.0.0.1:5003, 127.0.0.1:5004] <<<
[HyDFS] >>> REPLICATING APPEND for '/hydfs/file3.txt' (opID=3) TO VMs: [127.0.0.1:5003, 127.0.0.1:5004] <<<
```

**Replicas:**
```
[HyDFS] >>> REPLICA RECEIVED APPEND REQUEST: file=/hydfs/file3.txt, opID=2, from=127.0.0.1:5002 <<<
[HyDFS] >>> REPLICA QUEUED APPEND: file=/hydfs/file3.txt, opID=2 (will converge) <<<
[HyDFS] >>> REPLICA COMPLETED APPEND: file=/hydfs/file3.txt, opID=2 <<<
```

---

## Test 5: Concurrent Appends + Merge [15%]

**Requirement:** 4 VMs append concurrently, show concurrency, run merge, verify consistency.

### Step 1: Concurrent Appends (Need multiappend command)

**Note:** This requires the `multiappend` command to be implemented.

```bash
hydfs> multiappend /hydfs/file3.txt 127.0.0.1:8002 test_files/append_1.txt 127.0.0.1:8003 test_files/append_2.txt 127.0.0.1:8004 test_files/append_3.txt 127.0.0.1:8005 test_files/append_4.txt
```

### Step 2: Show Concurrent Logs

**Watch all 4 replica terminals** - should see overlapping logs:

```
[VM1] >>> REPLICA RECEIVED APPEND REQUEST: file=/hydfs/file3.txt, opID=4 <<<
[VM2] >>> REPLICA RECEIVED APPEND REQUEST: file=/hydfs/file3.txt, opID=5 <<<
[VM3] >>> REPLICA RECEIVED APPEND REQUEST: file=/hydfs/file3.txt, opID=6 <<<
[VM4] >>> REPLICA RECEIVED APPEND REQUEST: file=/hydfs/file3.txt, opID=7 <<<
```

✅ **Demo Point:** Timestamps show concurrent processing!

### Step 3: Run Merge

```bash
hydfs> merge /hydfs/file3.txt
```

**Expected output:**
```
Merging file /hydfs/file3.txt across replicas...
Successfully merged file /hydfs/file3.txt
```

**Expected logs:**
```
[HyDFS] Querying 3 replicas in parallel...
[HyDFS] Node 127.0.0.1:5002: opID=7, pending=0, hash=xyz789ab
[HyDFS] Node 127.0.0.1:5003: opID=7, pending=0, hash=xyz789ab
[HyDFS] Node 127.0.0.1:5004: opID=7, pending=0, hash=xyz789ab
[HyDFS] All replicas converged to opID=7!
[HyDFS] All replicas verified identical!
```

### Step 4: Verify Replicas Identical

**Get file from 2 different replicas** (TA picks):

```bash
hydfs> getfromreplica 127.0.0.1:5003 /hydfs/file3.txt test_files/from_replica1.txt
hydfs> getfromreplica 127.0.0.1:5004 /hydfs/file3.txt test_files/from_replica2.txt
```

**Compare with diff:**

```bash
diff test_files/from_replica1.txt test_files/from_replica2.txt
```

**Expected:** No output (files identical) ✅

### Step 5: Verify No Appends Lost

**Check file contains all 4 appends:**

```bash
cat test_files/from_replica1.txt
grep "APPEND DATA 1" test_files/from_replica1.txt
grep "APPEND DATA 2" test_files/from_replica1.txt
grep "APPEND DATA 3" test_files/from_replica1.txt
grep "APPEND DATA 4" test_files/from_replica1.txt
```

**All 4 greps should return results** ✅

---

## Quick Test Script

Create `quick_test.sh`:

```bash
#!/bin/bash

echo "=== Quick HyDFS Test ==="
echo ""

echo "Step 1: Creating 3 test files..."
echo "create test_files/business_1.txt /hydfs/test1.txt" | timeout 2 nc 127.0.0.1 5001 || true
sleep 1
echo "create test_files/business_2.txt /hydfs/test2.txt" | timeout 2 nc 127.0.0.1 5001 || true
sleep 1
echo "create test_files/business_3.txt /hydfs/test3.txt" | timeout 2 nc 127.0.0.1 5001 || true
sleep 2

echo "Step 2: Listing files..."
echo "list" | timeout 2 nc 127.0.0.1 5001 || true
sleep 1

echo "Step 3: Getting a file..."
echo "get /hydfs/test1.txt test_files/retrieved.txt" | timeout 2 nc 127.0.0.1 5002 || true
sleep 1

echo "Step 4: Appending to file..."
echo "append test_files/append_1.txt /hydfs/test1.txt" | timeout 2 nc 127.0.0.1 5002 || true
sleep 1

echo "Step 5: Getting file after append..."
echo "get /hydfs/test1.txt test_files/after_append.txt" | timeout 2 nc 127.0.0.1 5002 || true

echo ""
echo "=== Test Complete ==="
echo "Check test_files/ directory for results"
```

Run:
```bash
chmod +x quick_test.sh
./quick_test.sh
```

---

## Troubleshooting

### Files Not Found
- Check file exists in test_files/
- Verify path is correct (use absolute or relative from hydfs binary location)

### Cluster Not Forming
- Ensure all nodes use same introducer address
- Wait 5-10 seconds after starting all nodes
- Check no port conflicts (5001-5004, 7001-7004, 8001-8004)

### Replication Not Working
- Check logs for "REPLICATING FILE" messages
- Verify at least 3 nodes are alive
- Check network connectivity between nodes

### Appends Out of Order
- This should NOT happen - contact TA if it does
- Check converger thread is running
- Verify operation IDs are incrementing

---

## Demo Checklist

Before demo, verify:

- [ ] All 4 VMs start without errors
- [ ] Cluster forms (membership shows 4 nodes)
- [ ] CREATE works from any VM
- [ ] Files appear in `list` command
- [ ] GET retrieves correct content
- [ ] `liststore` shows files on correct replicas
- [ ] APPEND works and is visible immediately
- [ ] Appends are in correct order
- [ ] Terminal logs show replication messages
- [ ] MERGE completes without errors
- [ ] Replicas are identical after merge

---

## Expected Terminal Output Summary

**✅ Good Signs:**
```
Successfully created file...
Successfully appended to file...
>>> REPLICATING FILE TO VMs: [...]
>>> REPLICA RECEIVED CREATE REQUEST
>>> REPLICA COMPLETED CREATE
All replicas converged!
All replicas verified identical!
```

**❌ Bad Signs:**
```
Error creating file
Connection refused
File not found
Replication failed
panic: runtime error
```

---

**Good luck with your demo!** 🚀

