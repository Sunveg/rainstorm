package coordinator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"hydfs/pkg/membership"
	"hydfs/pkg/utils"
)

// ActionType represents different types of fault tolerance actions
type ActionType int

const (
	ActionPromoteToOwner ActionType = iota
	ActionTransferOwnership
	ActionCreateReplica
	ActionVerifyReplica
	ActionRemoveFile
)

func (a ActionType) String() string {
	switch a {
	case ActionPromoteToOwner:
		return "PROMOTE_TO_OWNER"
	case ActionTransferOwnership:
		return "TRANSFER_OWNERSHIP"
	case ActionCreateReplica:
		return "CREATE_REPLICA"
	case ActionVerifyReplica:
		return "VERIFY_REPLICA"
	case ActionRemoveFile:
		return "REMOVE_FILE"
	default:
		return "UNKNOWN"
	}
}

// FaultToleranceAction represents a specific action to be taken
type FaultToleranceAction struct {
	Type         ActionType
	FileName     string
	SourceNode   string
	TargetNode   string
	ReplicaIndex int
	Description  string
}

// recalculateOwnership performs periodic ownership recalculation for consistency
//
// SCENARIO: Called periodically (every 5 minutes) to ensure system consistency
// and catch any missed membership changes or inconsistencies.
//
// COMMON CAUSES:
// - Periodic consistency check: Scheduled background verification
// - Missed membership events: Backup mechanism for event-driven system
// - Network partition recovery: Ensuring consistent state after reconnection
// - Administrative verification: Manual consistency checks
//
// ACTIONS PERFORMED:
// 1. Analyze all local files for ownership/replica correctness
// 2. Generate and execute any required fault tolerance actions
// 3. Log consistency status and any corrections made
func (cs *CoordinatorServer) recalculateOwnership(ctx context.Context) {
	cs.logger("Periodic ownership recalculation started")

	// Analyze what actions are required for consistency
	actions := cs.analyzeRequiredActions(ctx)

	if len(actions) == 0 {
		cs.logger("Periodic check complete: System is consistent, no actions required")
		return
	}

	cs.logger("Periodic check found %d consistency issues, correcting...", len(actions))

	// Execute corrective actions
	cs.executeActions(ctx, actions)

	cs.logger("Periodic ownership recalculation completed")
}

// calculateFileHash calculates SHA-256 hash of a file
func (cs *CoordinatorServer) calculateFileHash(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hasher := sha256.New()
	if _, err := io.Copy(hasher, file); err != nil {
		return "", err
	}

	return hex.EncodeToString(hasher.Sum(nil)), nil
}

// analyzeRequiredActions determines what fault tolerance actions are needed
func (cs *CoordinatorServer) analyzeRequiredActions(ctx context.Context) []FaultToleranceAction {
	var actions []FaultToleranceAction
	selfNodeID := membership.StringifyNodeID(cs.nodeID)

	// Get all files stored locally (includes both OwnedFiles and ReplicatedFiles)
	storedFiles := cs.fileServer.ListStoredFiles()

	// Get files that are currently in ReplicatedFiles directory
	replicatedFiles := cs.fileServer.ListReplicatedFiles()

	cs.logger("Analyzing fault tolerance actions for %d local files", len(storedFiles))

	for filename := range storedFiles {
		// Calculate expected ownership and replication based on new ring
		expectedOwner := cs.hashSystem.ComputeLocation(filename)
		expectedReplicas := cs.hashSystem.GetReplicaNodes(filename)

		// Determine ACTUAL current file status (where file actually is)
		_, isCurrentlyReplica := replicatedFiles[filename]
		isCurrentlyOwner := !isCurrentlyReplica // If not in ReplicatedFiles, it's in OwnedFiles

		// Determine EXPECTED status (where file should be according to new ring)
		shouldOwn := (expectedOwner == selfNodeID)
		isExpectedReplica := false
		replicaIndex := -1

		for i, replica := range expectedReplicas {
			if replica == selfNodeID {
				isExpectedReplica = true
				replicaIndex = i
				break
			}
		}

		cs.logger("File %s: currentlyOwner=%v, currentlyReplica=%v, shouldOwn=%v, shouldBeReplica=%v",
			filename, isCurrentlyOwner, isCurrentlyReplica, shouldOwn, isExpectedReplica)

		// Analyze what actions are needed based on current vs expected state
		fileActions := cs.analyzeFileActions(filename, expectedOwner, expectedReplicas,
			isCurrentlyOwner, isCurrentlyReplica, shouldOwn, isExpectedReplica, replicaIndex)

		actions = append(actions, fileActions...)
	}

	cs.logger("Analysis complete: %d fault tolerance actions required", len(actions))
	return actions
}

// analyzeFileActions determines specific actions needed for a single file
// Parameters:
//   - isCurrentlyOwner: true if file is in OwnedFiles directory
//   - isCurrentlyReplica: true if file is in ReplicatedFiles directory
//   - shouldOwn: true if we should own this file according to new ring
//   - isExpectedReplica: true if we should be a replica according to new ring
func (cs *CoordinatorServer) analyzeFileActions(filename, expectedOwner string, expectedReplicas []string,
	isCurrentlyOwner, isCurrentlyReplica, shouldOwn, isExpectedReplica bool, replicaIndex int) []FaultToleranceAction {

	var actions []FaultToleranceAction
	selfNodeID := membership.StringifyNodeID(cs.nodeID)

	// Scenario 1: Currently replica + should own → Promote
	if isCurrentlyReplica && shouldOwn {
		actions = append(actions, FaultToleranceAction{
			Type:        ActionPromoteToOwner,
			FileName:    filename,
			SourceNode:  selfNodeID,
			TargetNode:  selfNodeID,
			Description: fmt.Sprintf("Promote file %s from ReplicatedFiles to OwnedFiles (we should own it)", filename),
		})

		// NOTE: executePromoteToOwner will handle replica creation internally (launches goroutines)
		// So we don't need to add CreateReplica actions here - they would be duplicates
		// The promotion action will complete first, then replicas will be created
		return actions
	}

	// Scenario 2: Currently owner + should not own → Transfer
	if isCurrentlyOwner && !shouldOwn {
		actions = append(actions, FaultToleranceAction{
			Type:        ActionTransferOwnership,
			FileName:    filename,
			SourceNode:  selfNodeID,
			TargetNode:  expectedOwner,
			Description: fmt.Sprintf("Transfer ownership of file %s to %s (we should not own it)", filename, expectedOwner),
		})
		return actions
	}

	// Scenario 3: Currently owner + should own → No action (correct state)
	if isCurrentlyOwner && shouldOwn {
		// File is correctly in OwnedFiles and we should own it
		// Just ensure all replicas exist
		for i, replica := range expectedReplicas {
			if replica != selfNodeID {
				actions = append(actions, FaultToleranceAction{
					Type:         ActionCreateReplica,
					FileName:     filename,
					SourceNode:   selfNodeID,
					TargetNode:   replica,
					ReplicaIndex: i,
					Description:  fmt.Sprintf("Create replica %d of file %s on node %s", i, filename, replica),
				})
			}
		}
		return actions
	}

	// Scenario 4: Currently replica + should be replica → Verify (correct state)
	if isCurrentlyReplica && isExpectedReplica {
		actions = append(actions, FaultToleranceAction{
			Type:         ActionVerifyReplica,
			FileName:     filename,
			SourceNode:   expectedOwner,
			TargetNode:   selfNodeID,
			ReplicaIndex: replicaIndex,
			Description:  fmt.Sprintf("Verify replica %d of file %s is up to date", replicaIndex, filename),
		})
		return actions
	}

	// Scenario 5: Currently replica + should not be replica → Remove
	if isCurrentlyReplica && !isExpectedReplica {
		actions = append(actions, FaultToleranceAction{
			Type:        ActionRemoveFile,
			FileName:    filename,
			SourceNode:  selfNodeID,
			TargetNode:  selfNodeID,
			Description: fmt.Sprintf("Remove file %s from ReplicatedFiles (we should not be a replica)", filename),
		})
		return actions
	}

	// Edge case: File exists but in unexpected state (shouldn't happen, but handle gracefully)
	cs.logger("WARNING: File %s in unexpected state - currentlyOwner=%v, currentlyReplica=%v, shouldOwn=%v, shouldBeReplica=%v",
		filename, isCurrentlyOwner, isCurrentlyReplica, shouldOwn, isExpectedReplica)

	return actions
}

// faultToleranceRoutine handles fault tolerance operations for membership changes
// Flow: Converge Pending Operations → Analyze Actions → Execute Actions
func (cs *CoordinatorServer) faultToleranceRoutine(ctx context.Context) {
	cs.logger("Fault tolerance routine started")

	// STEP 1: Converge all pending operations FIRST before taking any actions
	// This ensures file content is up-to-date before we transfer/promote/remove files
	cs.logger("Step 1: Converging all pending operations before fault tolerance actions...")
	convergenceTimeout := 30 * time.Second // Allow up to 30 seconds for convergence
	if err := cs.fileServer.ConvergeAllPendingOperations(convergenceTimeout); err != nil {
		cs.logger("WARNING: Convergence incomplete or timed out: %v. Proceeding with fault tolerance actions anyway.", err)
		// Continue anyway - some operations might still be pending, but we proceed
	} else {
		cs.logger("Step 1 complete: All pending operations converged successfully")
	}

	// STEP 1.5: Process any pending cleanup from previous partial failures
	cs.logger("Step 1.5: Processing pending cleanup from previous partial transfer failures...")
	cs.processPendingCleanup()

	// STEP 2: Analyze what actions are required based on new hash ring
	cs.logger("Step 2: Analyzing required fault tolerance actions...")
	actions := cs.analyzeRequiredActions(ctx)

	if len(actions) == 0 {
		cs.logger("No fault tolerance actions required")
		cs.logger("Fault tolerance routine completed")
		return
	}

	// STEP 3: Execute actions by priority
	cs.logger("Step 3: Executing %d fault tolerance actions...", len(actions))
	cs.executeActions(ctx, actions)

	cs.logger("Fault tolerance routine completed")
}

// executeActions executes all fault tolerance actions
// Groups actions by file to respect dependencies, then executes different files in parallel
// Continues processing even if individual actions fail (eventual consistency)
func (cs *CoordinatorServer) executeActions(ctx context.Context, actions []FaultToleranceAction) {
	cs.logger("Executing %d fault tolerance actions", len(actions))

	// Group actions by file to respect dependencies (e.g., Promote before CreateReplica for same file)
	actionsByFile := make(map[string][]FaultToleranceAction)
	for _, action := range actions {
		actionsByFile[action.FileName] = append(actionsByFile[action.FileName], action)
	}

	cs.logger("Grouped actions into %d files (some files have multiple dependent actions)", len(actionsByFile))

	// Execute actions for different files in parallel
	// Actions for the same file are executed sequentially to respect dependencies
	var wg sync.WaitGroup
	successCount := int64(0)
	failureCount := int64(0)
	var mu sync.Mutex // Protects counters

	for fileName, fileActions := range actionsByFile {
		wg.Add(1)
		go func(fn string, actions []FaultToleranceAction) {
			defer wg.Done()
			cs.logger("Processing %d actions for file %s", len(actions), fn)

			// Execute actions for this file sequentially (to respect dependencies)
			for _, action := range actions {
				cs.logger("Action: %s - %s", action.Type.String(), action.Description)

				// Execute the specific action
				err := cs.executeAction(ctx, action)
				if err != nil {
					mu.Lock()
					failureCount++
					mu.Unlock()
					cs.logger("Action failed: %s - error: %v (continuing with other actions)", action.Description, err)
					// Continue processing other actions for this file - don't stop
				} else {
					mu.Lock()
					successCount++
					mu.Unlock()
					cs.logger("Action completed: %s", action.Description)
				}
			}
		}(fileName, fileActions)
	}

	// Wait for all files to complete
	wg.Wait()

	cs.logger("Fault tolerance actions completed: %d succeeded, %d failed (will retry failed actions in next cycle)",
		successCount, failureCount)
}

// executeAction executes a specific fault tolerance action
func (cs *CoordinatorServer) executeAction(ctx context.Context, action FaultToleranceAction) error {
	cs.logger("Executing action: %s for file %s", action.Type.String(), action.FileName)

	switch action.Type {
	case ActionPromoteToOwner:
		return cs.executePromoteToOwner(ctx, action)

	case ActionTransferOwnership:
		return cs.executeTransferOwnership(ctx, action)

	case ActionCreateReplica:
		return cs.ExecuteCreateReplica(ctx, action)

	case ActionVerifyReplica:
		return cs.executeVerifyReplica(ctx, action)

	case ActionRemoveFile:
		return cs.executeRemoveFile(ctx, action)

	default:
		return fmt.Errorf("unknown action type: %s", action.Type.String())
	}
}

// executePromoteToOwner promotes a replica file to owner status
//
// SCENARIO: Called when this node should own a file according to the hash ring,
// but currently only has it as a replica (stored in ReplicatedFiles/).
//
// COMMON CAUSES:
// - Node failure: The original owner failed, and we're the next node in the ring
// - Hash ring changes: Membership changes moved ownership responsibility to us
// - Network partition recovery: We had a replica, now we should be the owner
//
// ACTIONS PERFORMED:
// 1. Move file from ReplicatedFiles/ → OwnedFiles/
// 2. Update hash ring state
// 3. Create replicas on other nodes (push-based replication)
func (cs *CoordinatorServer) executePromoteToOwner(ctx context.Context, action FaultToleranceAction) error {
	cs.logger("Promoting file %s to owner status", action.FileName)

	// 1. Move file from ReplicatedFiles/ to OwnedFiles/ using file system operations
	replicatedPath := filepath.Join(cs.storagePath, "ReplicatedFiles", action.FileName)
	ownedPath := filepath.Join(cs.storagePath, "OwnedFiles", action.FileName)

	// Check if file exists in ReplicatedFiles
	if _, err := os.Stat(replicatedPath); os.IsNotExist(err) {
		return fmt.Errorf("file %s not found in ReplicatedFiles", action.FileName)
	}

	// Ensure OwnedFiles directory exists
	ownedDir := filepath.Dir(ownedPath)
	if err := os.MkdirAll(ownedDir, 0755); err != nil {
		return fmt.Errorf("failed to create OwnedFiles directory: %v", err)
	}

	// Move the file (protected by global mutex)
	cs.fileOperationMutex.Lock()
	err := os.Rename(replicatedPath, ownedPath)
	if err != nil {
		cs.fileOperationMutex.Unlock()
		return fmt.Errorf("failed to move file to owned: %v", err)
	}

	// Update metadata location after file move
	if err := cs.fileServer.UpdateMetadataLocation(action.FileName, ownedPath); err != nil {
		cs.logger("Warning: Failed to update metadata location for %s: %v", action.FileName, err)
		// Continue anyway - file was moved successfully
	}
	cs.fileOperationMutex.Unlock()

	// 2. Update hash ring to reflect new ownership
	// cs.updateHashRing()

	// 3. As the new owner, start replication to other nodes
	// This is our responsibility now that we own the file
	expectedReplicas := cs.hashSystem.GetReplicaNodes(action.FileName)
	selfNodeID := membership.StringifyNodeID(cs.nodeID)

	for i, replica := range expectedReplicas {
		if replica != selfNodeID {
			// Create replica asynchronously to avoid blocking
			go func(targetNode string, replicaIndex int) {
				replicaAction := FaultToleranceAction{
					Type:         ActionCreateReplica,
					FileName:     action.FileName,
					SourceNode:   selfNodeID,
					TargetNode:   targetNode,
					ReplicaIndex: replicaIndex,
				}
				cs.ExecuteCreateReplica(ctx, replicaAction)
			}(replica, i)
		}
	}

	cs.logger("Successfully promoted file %s to owner status", action.FileName)
	return nil
}

// executeTransferOwnership transfers file ownership to the correct node
//
// SCENARIO: Called when this node currently owns a file (stored in OwnedFiles/),
// but according to the current hash ring, a different node should own it.
//
// COMMON CAUSES:
// - Node joins: A new node joined and should now own this file
// - Hash ring rebalancing: Membership changes shifted ownership responsibility
// - Recovery from network partition: Hash ring state has been corrected
//
// ACTIONS PERFORMED:
// 1. Send file data to the correct new owner (push-based transfer)
// 2. Check if we should keep a replica copy
// 3. Move to ReplicatedFiles/ OR delete entirely based on replica status
// 4. New owner becomes responsible for creating other replicas
func (cs *CoordinatorServer) executeTransferOwnership(ctx context.Context, action FaultToleranceAction) error {
	cs.logger("Transferring ownership of file %s from %s to %s",
		action.FileName, action.SourceNode, action.TargetNode)

	// 1. Read file data from OwnedFiles
	ownedPath := filepath.Join(cs.storagePath, "OwnedFiles", action.FileName)
	fileData, err := os.ReadFile(ownedPath)
	if err != nil {
		return fmt.Errorf("failed to read file %s: %v", action.FileName, err)
	}

	// 2. Send file to new owner using existing coordinator API
	// The new owner will be responsible for creating replicas once they receive the file
	cs.logger("Sending file %s (%d bytes) to new owner %s", action.FileName, len(fileData), action.TargetNode)

	selfNodeIDForTransfer := membership.StringifyNodeID(cs.nodeID)
	err = cs.performRemoteOperation(utils.Create, action.FileName, fileData, "fault_tolerance", action.TargetNode, selfNodeIDForTransfer, action.TargetNode, true) // true = destIsOwner
	if err != nil {
		return fmt.Errorf("failed to transfer file to new owner: %v", err)
	}

	cs.logger("File %s successfully transferred to new owner %s", action.FileName, action.TargetNode)

	// 3. Check if we should keep a replica copy
	expectedReplicas := cs.hashSystem.GetReplicaNodes(action.FileName)
	selfNodeID := membership.StringifyNodeID(cs.nodeID)
	shouldKeepReplica := false

	for _, replica := range expectedReplicas {
		if replica == selfNodeID {
			shouldKeepReplica = true
			break
		}
	}

	// 4. Handle local file based on replica status
	// CRITICAL: Transfer succeeded, so new owner has the file. We MUST clean up locally.
	// If cleanup fails, we mark it for retry but don't fail the entire operation.
	cleanupErr := cs.handleLocalFileCleanup(action.FileName, ownedPath, shouldKeepReplica)
	if cleanupErr != nil {
		// Transfer succeeded but cleanup failed - mark for retry
		cs.logger("ERROR: Transfer succeeded but cleanup failed for %s: %v", action.FileName, cleanupErr)
		cs.logger("CRITICAL: File %s is now in inconsistent state - new owner has it, but old owner still has it in OwnedFiles", action.FileName)
		cs.logger("Marking file %s for cleanup retry in next convergence cycle", action.FileName)

		// Mark for cleanup retry
		cs.markForCleanup(action.FileName, ownedPath, shouldKeepReplica, action.TargetNode)

		// Don't return error - transfer succeeded, cleanup will be retried
		// This allows fault tolerance routine to continue processing other files
		cs.logger("Continuing fault tolerance routine - cleanup will be retried for %s", action.FileName)
		return nil // Return nil to allow routine to continue
	}

	cs.logger("Successfully transferred ownership of file %s to %s", action.FileName, action.TargetNode)
	return nil
}

// handleLocalFileCleanup handles cleanup of local file after successful transfer
// Returns error if cleanup fails, nil if successful
// Protected by global fileOperationMutex to prevent race conditions
func (cs *CoordinatorServer) handleLocalFileCleanup(fileName, ownedPath string, shouldKeepReplica bool) error {
	cs.fileOperationMutex.Lock()
	defer cs.fileOperationMutex.Unlock()

	if shouldKeepReplica {
		// Move from OwnedFiles to ReplicatedFiles
		replicatedPath := filepath.Join(cs.storagePath, "ReplicatedFiles", fileName)
		replicatedDir := filepath.Dir(replicatedPath)

		// Best-effort: Try multiple times with small delays
		var err error
		for attempt := 0; attempt < 3; attempt++ {
			if attempt > 0 {
				// Release lock temporarily for retry delay (allows other operations)
				cs.fileOperationMutex.Unlock()
				time.Sleep(100 * time.Millisecond) // Small delay between retries
				cs.fileOperationMutex.Lock()
			}

			if err = os.MkdirAll(replicatedDir, 0755); err != nil {
				cs.logger("Attempt %d: Failed to create ReplicatedFiles directory: %v", attempt+1, err)
				continue
			}

			if err = os.Rename(ownedPath, replicatedPath); err != nil {
				cs.logger("Attempt %d: Failed to move file to replicated: %v", attempt+1, err)
				continue
			}

			// Success - update metadata
			if err = cs.fileServer.UpdateMetadataLocation(fileName, replicatedPath); err != nil {
				cs.logger("Warning: Failed to update metadata location for %s: %v", fileName, err)
				// Continue anyway - file was moved successfully
			}

			cs.logger("Successfully moved file %s to ReplicatedFiles (attempt %d)", fileName, attempt+1)
			return nil // Success
		}

		// All attempts failed
		return fmt.Errorf("failed to move file to replicated after 3 attempts: %v", err)
	} else {
		// Delete the file entirely - best effort with retries
		var err error
		for attempt := 0; attempt < 3; attempt++ {
			if attempt > 0 {
				// Release lock temporarily for retry delay (allows other operations)
				cs.fileOperationMutex.Unlock()
				time.Sleep(100 * time.Millisecond) // Small delay between retries
				cs.fileOperationMutex.Lock()
			}

			if err = os.Remove(ownedPath); err != nil {
				cs.logger("Attempt %d: Failed to remove file: %v", attempt+1, err)
				// Check if file still exists
				if _, statErr := os.Stat(ownedPath); os.IsNotExist(statErr) {
					// File was deleted by another process, consider it success
					cs.logger("File %s was deleted by another process", fileName)
					err = nil
					break
				}
				continue
			}

			// Success - delete metadata
			if err = cs.fileServer.DeleteMetadata(fileName); err != nil {
				cs.logger("Warning: Failed to delete metadata for %s: %v", fileName, err)
				// Continue anyway - file was removed successfully
			}

			cs.logger("Successfully deleted file %s (attempt %d)", fileName, attempt+1)
			return nil // Success
		}

		// All attempts failed
		return fmt.Errorf("failed to remove file after 3 attempts: %v", err)
	}
}

// markForCleanup marks a file for cleanup retry in the next convergence cycle
func (cs *CoordinatorServer) markForCleanup(fileName, currentPath string, shouldBeReplica bool, transferredTo string) {
	cs.cleanupMutex.Lock()
	defer cs.cleanupMutex.Unlock()

	targetPath := ""
	if shouldBeReplica {
		targetPath = filepath.Join(cs.storagePath, "ReplicatedFiles", fileName)
	}

	cs.pendingCleanup[fileName] = &CleanupInfo{
		FileName:        fileName,
		OldPath:         currentPath,
		TargetPath:      targetPath,
		ShouldBeReplica: shouldBeReplica,
		TransferredTo:   transferredTo,
		CreatedAt:       time.Now(),
	}

	cs.logger("Marked file %s for cleanup retry (current: %s, target: %s, transferred to: %s)",
		fileName, currentPath, targetPath, transferredTo)
}

// processPendingCleanup attempts to clean up files that had partial transfer failures
func (cs *CoordinatorServer) processPendingCleanup() {
	cs.cleanupMutex.Lock()
	defer cs.cleanupMutex.Unlock()

	if len(cs.pendingCleanup) == 0 {
		return
	}

	cs.logger("Processing %d files marked for cleanup retry", len(cs.pendingCleanup))

	// Process each pending cleanup
	for fileName, info := range cs.pendingCleanup {
		// Check if file still exists at old path
		if _, err := os.Stat(info.OldPath); os.IsNotExist(err) {
			// File already cleaned up, remove from pending
			cs.logger("File %s already cleaned up, removing from pending cleanup", fileName)
			delete(cs.pendingCleanup, fileName)
			continue
		}

		// Verify the file was actually transferred (check if new owner has it)
		// For now, we'll just try cleanup again
		cs.logger("Retrying cleanup for file %s (transferred to %s)", fileName, info.TransferredTo)

		// Try cleanup again
		err := cs.handleLocalFileCleanup(fileName, info.OldPath, info.ShouldBeReplica)
		if err == nil {
			// Success - remove from pending
			cs.logger("Successfully cleaned up file %s on retry", fileName)
			delete(cs.pendingCleanup, fileName)
		} else {
			// Still failed - keep in pending for next cycle
			cs.logger("Cleanup retry failed for %s: %v (will retry in next cycle)", fileName, err)

			// If cleanup has been pending for too long (5 minutes), log a warning
			if time.Since(info.CreatedAt) > 5*time.Minute {
				cs.logger("WARNING: File %s has been pending cleanup for %v - may need manual intervention",
					fileName, time.Since(info.CreatedAt))
			}
		}
	}
}

// executeCreateReplica creates a replica of a file on this node
//
// SCENARIO: Called when this node is selected as a new replica location
// for a file that is owned by another node.
//
// COMMON CAUSES:
// - Replica failure: One of the 3 replicas failed, need replacement
// - New node joins: Hash ring changes require new replica placement
// - Replication factor correction: Ensuring each file has exactly 3 replicas
// - Owner recovery: File owner recovered and is creating proper replicas
//
// ACTIONS PERFORMED:
// 1. Receive file data from the owner (push-based transfer)
// 2. Store file in local ReplicatedFiles/ directory
// 3. Confirm successful replication to the owner
// 4. Begin monitoring for owner health
//
// ExecuteCreateReplica creates a replica of a file on this node.
// This is a public method that can be called from other packages (e.g., network package).
func (cs *CoordinatorServer) ExecuteCreateReplica(ctx context.Context, action FaultToleranceAction) error {
	cs.logger("Creating replica %d of file %s on node %s",
		action.ReplicaIndex, action.FileName, action.TargetNode)

	// 1. Read file data from OwnedFiles
	ownedPath := filepath.Join(cs.storagePath, "OwnedFiles", action.FileName)
	fileData, err := os.ReadFile(ownedPath)
	if err != nil {
		return fmt.Errorf("failed to read file %s: %v", action.FileName, err)
	}

	// 2. Send to target node as replica using existing coordinator API
	cs.logger("Sending replica of file %s (%d bytes) to node %s",
		action.FileName, len(fileData), action.TargetNode)

	// Use propagateFileToReplica which handles both local and remote replicas
	err = cs.propagateFileToReplica(action.TargetNode, action.FileName, fileData, "fault_tolerance", true) // true = isCreate
	if err != nil {
		return fmt.Errorf("failed to create replica on node %s: %v", action.TargetNode, err)
	}

	// 3. Success
	cs.logger("Successfully created replica %d of file %s on node %s",
		action.ReplicaIndex, action.FileName, action.TargetNode)

	return nil
}

// CreateReplicaForFaultTolerance creates a replica of a file for fault tolerance purposes.
// This is a simpler public method that doesn't require FaultToleranceAction, avoiding circular dependencies.
// It can be called from the network package without importing coordinator types.
func (cs *CoordinatorServer) CreateReplicaForFaultTolerance(ctx context.Context, fileName, targetNode string, replicaIndex int) error {
	action := FaultToleranceAction{
		Type:         ActionCreateReplica,
		FileName:     fileName,
		SourceNode:   membership.StringifyNodeID(cs.nodeID),
		TargetNode:   targetNode,
		ReplicaIndex: replicaIndex,
	}
	return cs.ExecuteCreateReplica(ctx, action)
}

// executeVerifyReplica verifies that a replica is still valid and accessible
//
// SCENARIO: Called to confirm that an existing replica is healthy and
// accessible when there are concerns about replica integrity.
//
// COMMON CAUSES:
// - Node recovery: Previously suspected node came back, verify its data
// - Health check: Periodic verification of replica consistency
// - Network partition recovery: Ensure replicas are still synchronized
// - Client read failures: Verify replica when client reports read errors
//
// ACTIONS PERFORMED:
// 1. Contact the replica node to verify file exists and is accessible
// 2. Compare file metadata (size, checksum) for consistency
// 3. Mark replica as valid or trigger replacement if corrupted
// 4. Update internal replica tracking state
func (cs *CoordinatorServer) executeVerifyReplica(ctx context.Context, action FaultToleranceAction) error {
	cs.logger("Verifying replica %d of file %s is up to date",
		action.ReplicaIndex, action.FileName)

	// 1. Calculate local replica file hash
	replicatedPath := filepath.Join(cs.storagePath, "ReplicatedFiles", action.FileName)
	localHash, err := cs.calculateFileHash(replicatedPath)
	if err != nil {
		return fmt.Errorf("failed to calculate local file hash: %v", err)
	}

	// 2. Get file data from owner node and calculate its hash
	cs.logger("Requesting file data from owner node %s for file %s",
		action.SourceNode, action.FileName)

	ownerFileData, err := cs.GetFile(action.FileName, "fault_tolerance")
	if err != nil {
		return fmt.Errorf("failed to get file from owner: %v", err)
	}

	// Calculate hash of owner's file data
	ownerHash := utils.ComputeDataHash(ownerFileData)

	// 3. Compare hashes
	if localHash != ownerHash {
		cs.logger("Hash mismatch detected for file %s - updating replica", action.FileName)

		// 4. Update replica with owner's file data
		cs.logger("Updating replica file %s with owner's version", action.FileName)

		// Write the owner's file data to our replica location
		replicatedPath := filepath.Join(cs.storagePath, "ReplicatedFiles", action.FileName)
		replicatedDir := filepath.Dir(replicatedPath)

		// Ensure directory exists
		if err := os.MkdirAll(replicatedDir, 0755); err != nil {
			return fmt.Errorf("failed to create replica directory: %v", err)
		}

		// Write the updated file data (protected by global mutex)
		cs.fileOperationMutex.Lock()
		err := os.WriteFile(replicatedPath, ownerFileData, 0644)
		cs.fileOperationMutex.Unlock()
		if err != nil {
			return fmt.Errorf("failed to update replica file: %v", err)
		}

		cs.logger("Successfully updated replica %d of file %s", action.ReplicaIndex, action.FileName)
	}

	cs.logger("Replica %d of file %s is up to date", action.ReplicaIndex, action.FileName)
	return nil
}

// executeRemoveFile removes a file from local storage
//
// SCENARIO: Called when this node should no longer store a particular file
// either as owner or replica, typically due to membership changes.
//
// COMMON CAUSES:
// - Hash ring rebalancing: File ownership/replica responsibility shifted away
// - Replica count optimization: Too many replicas exist, removing excess
// - Data cleanup: Removing orphaned files after node failures
// - Administrative deletion: File was deleted by client, removing all copies
//
// ACTIONS PERFORMED:
// 1. Remove file from OwnedFiles/ or ReplicatedFiles/ directory
// 2. Clean up any associated metadata or index entries
// 3. Update internal file tracking structures
// 4. Confirm successful removal to requester if needed
func (cs *CoordinatorServer) executeRemoveFile(ctx context.Context, action FaultToleranceAction) error {
	cs.logger("Removing file %s (not in expected replica set)", action.FileName)

	// 1. Double-check that file is really not needed
	expectedOwner := cs.hashSystem.ComputeLocation(action.FileName)
	expectedReplicas := cs.hashSystem.GetReplicaNodes(action.FileName)
	selfNodeID := membership.StringifyNodeID(cs.nodeID)

	shouldKeep := (expectedOwner == selfNodeID)
	if !shouldKeep {
		for _, replica := range expectedReplicas {
			if replica == selfNodeID {
				shouldKeep = true
				break
			}
		}
	}

	if shouldKeep {
		cs.logger("Warning: File %s should be kept according to current hash ring", action.FileName)
		return fmt.Errorf("refusing to remove file %s - it should be kept", action.FileName)
	}

	// 2. Remove from storage (check both OwnedFiles and ReplicatedFiles)
	// Protected by global mutex to prevent race conditions
	cs.fileOperationMutex.Lock()
	defer cs.fileOperationMutex.Unlock()

	ownedPath := filepath.Join(cs.storagePath, "OwnedFiles", action.FileName)
	replicatedPath := filepath.Join(cs.storagePath, "ReplicatedFiles", action.FileName)

	removed := false

	// Try to remove from OwnedFiles
	if _, err := os.Stat(ownedPath); err == nil {
		if err := os.Remove(ownedPath); err != nil {
			return fmt.Errorf("failed to remove file from OwnedFiles: %v", err)
		}
		cs.logger("Removed file %s from OwnedFiles", action.FileName)
		removed = true
	}

	// Try to remove from ReplicatedFiles
	if _, err := os.Stat(replicatedPath); err == nil {
		if err := os.Remove(replicatedPath); err != nil {
			return fmt.Errorf("failed to remove file from ReplicatedFiles: %v", err)
		}
		cs.logger("Removed file %s from ReplicatedFiles", action.FileName)
		removed = true
	}

	// Delete metadata after file removal (only if file was actually removed)
	if removed {
		if err := cs.fileServer.DeleteMetadata(action.FileName); err != nil {
			cs.logger("Warning: Failed to delete metadata for %s: %v", action.FileName, err)
			// Continue anyway - file was removed successfully
		}
	} else {
		cs.logger("File %s not found in storage (already removed?)", action.FileName)
		// Still try to clean up metadata in case it exists
		_ = cs.fileServer.DeleteMetadata(action.FileName)
	}

	cs.logger("Successfully removed file %s from local storage", action.FileName)
	return nil
}

// startPeriodicConvergence starts the background convergence goroutine
func (cs *CoordinatorServer) startPeriodicConvergence(ctx context.Context) {
	if cs.convergenceRunning {
		cs.logger("Periodic convergence already running")
		return
	}

	cs.stopConvergence = make(chan bool, 1)
	cs.convergenceRunning = true

	go func() {
		cs.logger("Starting periodic convergence routine (5-minute intervals)")
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				// Perform periodic ownership recalculation
				cs.recalculateOwnership(ctx)

			case <-cs.stopConvergence:
				cs.logger("Stopping periodic convergence routine")
				cs.convergenceRunning = false
				return

			case <-ctx.Done():
				cs.logger("Context cancelled, stopping periodic convergence routine")
				cs.convergenceRunning = false
				return
			}
		}
	}()
}

// stopPeriodicConvergence stops the background convergence goroutine
func (cs *CoordinatorServer) stopPeriodicConvergence() {
	if !cs.convergenceRunning {
		return
	}

	cs.logger("Requesting stop of periodic convergence routine")
	select {
	case cs.stopConvergence <- true:
	default:
		// Channel might be full, but that's okay
	}
}
