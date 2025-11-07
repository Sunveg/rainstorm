# Consistency Testing Guide

## Test Setup

### 1. Start All Nodes
```bash
# Terminal 1 (Introducer)
./hydfs 127.0.0.1 5001

# Terminal 2
./hydfs 127.0.0.1 5002 127.0.0.1:5001

# Terminal 3
./hydfs 127.0.0.1 5003 127.0.0.1:5001

# Terminal 4
./hydfs 127.0.0.1 5004 127.0.0.1:5001
```

Wait 3-5 seconds for membership to stabilize.

---

## Test 1: Per-Client Append Ordering

**Goal:** Two appends from the same client must appear in order.

### Steps:

1. **Create a test file:**
```bash
# On any node (e.g., 5001)
hydfs> create test_files/business_1.txt /hydfs/ordering_test.txt
```

2. **Wait 2-3 seconds for replication**

3. **First append (from same node):**
```bash
# On node 5001
hydfs> append test_files/append_2.txt /hydfs/ordering_test.txt
```

4. **Wait for completion** (see `>>> CLIENT: APPEND COMPLETED <<<`)

5. **Second append (from same node, immediately after):**
```bash
# On node 5001 (same client)
hydfs> append test_files/business_2.txt /hydfs/ordering_test.txt
```

6. **Wait 5 seconds for converger to process both**

7. **Verify ordering:**
```bash
hydfs> get /hydfs/ordering_test.txt ordering_result.txt
```

8. **Check content (in new terminal):**
```bash
cat ordering_result.txt
```

**Expected Result:**
- Original content
- First append content (from append_2.txt)
- Second append content (from business_2.txt)
- **In that exact order**

**Verification:**
```bash
# Check that first append appears before second
grep -o "first_append_keyword" ordering_result.txt  # Should find it
grep -o "second_append_keyword" ordering_result.txt  # Should find it after first
```

---

## Test 2: Read-My-Writes

**Goal:** Client's get should reflect their own recent appends.

### Steps:

1. **Create a test file:**
```bash
# On node 5001
hydfs> create test_files/business_1.txt /hydfs/readmywrites_test.txt
```

2. **Wait 2-3 seconds**

3. **Append from node 5001:**
```bash
# On node 5001
hydfs> append test_files/append_2.txt /hydfs/readmywrites_test.txt
```

4. **Wait for completion** (see `>>> CLIENT: APPEND COMPLETED <<<`)

5. **Immediately get from same node:**
```bash
# On node 5001 (same client, immediately after append)
hydfs> get /hydfs/readmywrites_test.txt readmywrites_result.txt
```

6. **Check content:**
```bash
# In new terminal
cat readmywrites_result.txt
```

**Expected Result:**
- File contains original + appended content
- **Must reflect the append that just completed**

**Verification:**
```bash
# Search for keywords from append_2.txt
grep "keyword_from_append_2" readmywrites_result.txt
# Should find it - this proves read-my-writes works
```

---

## Test 3: Eventual Consistency

**Goal:** All replicas eventually have identical content.

### Steps:

1. **Create a test file:**
```bash
# On any node
hydfs> create test_files/business_1.txt /hydfs/consistency_test.txt
```

2. **Wait 2-3 seconds**

3. **Append multiple times from different nodes:**
```bash
# On node 5001
hydfs> append test_files/append_2.txt /hydfs/consistency_test.txt

# Wait 3 seconds, then on node 5002
hydfs> append test_files/business_2.txt /hydfs/consistency_test.txt

# Wait 3 seconds, then on node 5003
hydfs> append test_files/business_3.txt /hydfs/consistency_test.txt
```

4. **Wait 10 seconds for all appends to converge**

5. **Run merge to ensure consistency:**
```bash
# On any node
hydfs> merge /hydfs/consistency_test.txt
```

6. **Wait 5 seconds for merge to complete**

7. **Get file from different replicas:**
```bash
# Find owner and replicas
hydfs> ls /hydfs/consistency_test.txt

# Get from owner (e.g., 5002)
# On node 5002
hydfs> get /hydfs/consistency_test.txt replica1.txt

# Get from replica (e.g., 5003)
# On node 5003
hydfs> get /hydfs/consistency_test.txt replica2.txt
```

8. **Compare files:**
```bash
# In new terminal
diff replica1.txt replica2.txt
```

**Expected Result:**
- Files are identical (no diff output)
- Both contain all appends in the same order

**Verification:**
```bash
# Check file sizes are identical
ls -lh replica1.txt replica2.txt

# Check hashes are identical
md5 replica1.txt replica2.txt  # macOS
# or
md5sum replica1.txt replica2.txt  # Linux
```

---

## Test 4: Concurrent Appends from Same Client

**Goal:** Multiple rapid appends from same client maintain order.

### Steps:

1. **Create a test file:**
```bash
hydfs> create test_files/business_1.txt /hydfs/concurrent_test.txt
```

2. **Wait 2-3 seconds**

3. **Rapid sequential appends (same client):**
```bash
# On node 5001
hydfs> append test_files/append_2.txt /hydfs/concurrent_test.txt
# Wait for completion message
hydfs> append test_files/business_2.txt /hydfs/concurrent_test.txt
# Wait for completion message
hydfs> append test_files/business_3.txt /hydfs/concurrent_test.txt
```

4. **Wait 10 seconds for all to converge**

5. **Verify:**
```bash
hydfs> get /hydfs/concurrent_test.txt concurrent_result.txt
cat concurrent_result.txt
```

**Expected:**
- All three appends appear in order
- No missing data

---

## Test 5: Cross-Client Ordering (Eventual Consistency)

**Goal:** Appends from different clients eventually appear in same order on all replicas.

### Steps:

1. **Create a test file:**
```bash
# On node 5001
hydfs> create test_files/business_1.txt /hydfs/crossclient_test.txt
```

2. **Wait 2-3 seconds**

3. **Append from different nodes (simulating different clients):**
```bash
# Terminal 1 (node 5001) - Client A
hydfs> append test_files/append_2.txt /hydfs/crossclient_test.txt

# Terminal 2 (node 5002) - Client B (immediately after, don't wait)
hydfs> append test_files/business_2.txt /hydfs/crossclient_test.txt

# Terminal 3 (node 5003) - Client C (immediately after)
hydfs> append test_files/business_3.txt /hydfs/crossclient_test.txt
```

4. **Wait 15 seconds for convergence**

5. **Run merge:**
```bash
# On any node
hydfs> merge /hydfs/crossclient_test.txt
```

6. **Wait 5 seconds**

7. **Get from all replicas and compare:**
```bash
# On node 5001
hydfs> get /hydfs/crossclient_test.txt from_5001.txt

# On node 5002
hydfs> get /hydfs/crossclient_test.txt from_5002.txt

# On node 5003
hydfs> get /hydfs/crossclient_test.txt from_5003.txt
```

8. **Compare all files:**
```bash
diff from_5001.txt from_5002.txt
diff from_5002.txt from_5003.txt
```

**Expected:**
- All files are identical after merge
- All appends present (may be in different order before merge, but same after)

---

## Verification Commands

### Check Operation IDs:
```bash
# On owner node
hydfs> liststore
# Should show OpID matching number of appends
```

### Check Replica State:
```bash
# Use merge to see replica states
hydfs> merge /hydfs/test_file.txt
# Check logs for replica state information
```

### Check Logs for Convergence:
```bash
# On any node
tail -100 hydfs_127.0.0.1_<port>.log | grep -i "converger\|appended\|opID"
```

---

## Expected Behaviors

### ✅ Per-Client Ordering:
- Appends from same client appear in order
- Client A: append1 → append2 → file shows append1 before append2

### ✅ Read-My-Writes:
- Client's get immediately after append shows that append
- No need to wait for other clients' appends

### ✅ Eventual Consistency:
- After merge and convergence, all replicas identical
- All appends present on all replicas
- Same order on all replicas (after merge)

---

## Troubleshooting

**If appends don't appear in order:**
- Wait longer for converger (check logs)
- Check operation IDs are sequential
- Verify no duplicate operations

**If read-my-writes fails:**
- Check if append completed (see completion message)
- Verify file exists on owner
- Check converger processed the append

**If replicas not consistent:**
- Run merge command
- Wait longer for convergence
- Check all nodes are ALIVE
- Verify replication completed

