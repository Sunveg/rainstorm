package fileserver

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"hydfs/pkg/utils"
)

// FileServer handles file operations and storage management
type FileServer struct {
	// Storage paths
	ownedFilesDir      string
	replicatedFilesDir string
	tempFilesDir       string

	// Metadata management
	fileMetadata  map[string]*utils.FileMetaData
	metadataMutex sync.RWMutex

	// FileSystem for coordinator integration
	fileSystem *utils.FileSystem

	// Operation management
	operationCounter int
	operationMutex   sync.Mutex

	// Request handling
	requestQueue chan *utils.FileRequest
	workers      int

	// Logging
	logger func(string, ...interface{})

	// Shutdown handling
	shutdownCh chan struct{}
	isRunning  bool
}

// NewFileServer creates a new file server instance
func NewFileServer(baseStoragePath string, workers int, logger func(string, ...interface{})) (*FileServer, error) {
	// Create storage directories
	ownedDir := filepath.Join(baseStoragePath, "OwnedFiles")
	replicatedDir := filepath.Join(baseStoragePath, "ReplicatedFiles")
	tempDir := filepath.Join(baseStoragePath, "TempFiles")

	for _, dir := range []string{ownedDir, replicatedDir, tempDir} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create directory %s: %v", dir, err)
		}
	}

	fs := &FileServer{
		ownedFilesDir:      ownedDir,
		replicatedFilesDir: replicatedDir,
		tempFilesDir:       tempDir,
		fileMetadata:       make(map[string]*utils.FileMetaData),
		fileSystem:         utils.NewFileSystem(),              // Initialize FileSystem
		requestQueue:       make(chan *utils.FileRequest, 100), // Buffered channel
		workers:            workers,
		logger:             logger,
		shutdownCh:         make(chan struct{}),
	}

	return fs, nil
}

// Start starts the file server workers
func (fs *FileServer) Start() error {
	fs.isRunning = true

	// Start worker goroutines
	for i := 0; i < fs.workers; i++ {
		go fs.worker(i)
	}

	// Start background converger thread
	go fs.convergerThread()

	fs.logger("FileServer started with %d workers", fs.workers)
	return nil
}

// Stop stops the file server
func (fs *FileServer) Stop() {
	if !fs.isRunning {
		return
	}

	fs.isRunning = false
	close(fs.shutdownCh)
	fs.logger("FileServer stopped")
}

// SubmitRequest submits a file operation request to the queue
func (fs *FileServer) SubmitRequest(req *utils.FileRequest) error {
	if !fs.isRunning {
		return fmt.Errorf("file server is not running")
	}

	select {
	case fs.requestQueue <- req:
		return nil
	default:
		return fmt.Errorf("request queue is full")
	}
}

// worker processes file requests from the queue
func (fs *FileServer) worker(workerID int) {
	fs.logger("Worker %d started", workerID)

	for {
		select {
		case <-fs.shutdownCh:
			fs.logger("Worker %d stopping", workerID)
			return
		case req := <-fs.requestQueue:
			fs.processRequest(req, workerID)
		}
	}
}

// processRequest dispatches file operation requests to appropriate handlers
// Called by worker goroutines from the worker pool for ASYNC operations
func (fs *FileServer) processRequest(req *utils.FileRequest, workerID int) {
	fs.logger("Worker %d processing %s request for file %s", workerID, req.OperationType, req.FileName)

	// Route based on operation type
	switch req.OperationType {

	case utils.Append:
		// Owner or replica handling append operation
		// Goes to TempFiles and PendingOperations for converger
		fs.handleAppendFile(req)

	case utils.AppendReplica:
		// Same as Append (replica receives append from owner)
		fs.handleAppendFile(req)

	case utils.Get:
		// Handle file retrieval (if using async pattern)
		fs.handleGetFile(req)

	default:
		fs.logger("Worker %d: Unknown operation type: %v", workerID, req.OperationType)
	}
}

// handleAppendFile handles append operations for both owner and replica nodes
// Saves append data to /TempFiles and queues for converger processing
func (fs *FileServer) handleAppendFile(req *utils.FileRequest) {
	fs.logger("Worker handling APPEND for file: %s (opID: %d)", req.FileName, req.FileOperationID)

	// Validate operation ID
	if req.FileOperationID <= 0 {
		fs.logger("ERROR: Invalid operation ID %d for append", req.FileOperationID)
		return
	}

	// Step 1: Determine data source (buffer or file path)
	var dataToSave []byte
	var err error

	if req.Data != nil && len(req.Data) > 0 {
		// Data received in memory (from gRPC)
		dataToSave = req.Data
		fs.logger("Append data in memory: %d bytes", len(dataToSave))
	} else if req.LocalFilePath != "" {
		// Data in local file (forwarded from coordinator)
		dataToSave, err = ioutil.ReadFile(req.LocalFilePath)
		if err != nil {
			fs.logger("Failed to read file %s for append: %v", req.LocalFilePath, err)
			return
		}
		fs.logger("Append data from file: %s (%d bytes)", req.LocalFilePath, len(dataToSave))
	} else {
		fs.logger("ERROR: No data provided for append operation on file %s", req.FileName)
		return
	}

	// Step 2: Create temp file in /TempFiles directory (sanitize filename)
	sanitizedName := strings.ReplaceAll(req.FileName, "/", "_")
	tempFileName := fmt.Sprintf("%s_op%d.tmp", sanitizedName, req.FileOperationID)
	tempFilePath := filepath.Join(fs.tempFilesDir, tempFileName)

	// Create TempFiles directory if it doesn't exist
	if err := os.MkdirAll(fs.tempFilesDir, 0755); err != nil {
		fs.logger("Failed to create TempFiles directory: %v", err)
		return
	}

	// Write append data to temp file
	if err := ioutil.WriteFile(tempFilePath, dataToSave, 0644); err != nil {
		fs.logger("Failed to write temp file for append: %v", err)
		return
	}

	fs.logger("Saved append data to temp file: %s", tempFilePath)

	// Step 3: Create operation record (NO data in memory, just file path)
	operation := utils.Operation{
		ID:            req.FileOperationID,
		Type:          utils.Append,
		Timestamp:     time.Now(),
		ClientID:      req.ClientID,
		FileName:      req.FileName,
		Data:          nil,          // Do NOT store data in memory
		LocalFilePath: tempFilePath, // Store temp file path instead
	}

	// Step 4: Add to pending operations (will be processed by converger)
	fs.addPendingOperation(req.FileName, operation)

	fs.logger("Queued append operation %d for file %s (temp file: %s)",
		req.FileOperationID, req.FileName, tempFilePath)
}

// SaveFile saves a file from either local path or memory buffer
// Delegates to SaveFromPath or SaveFromBuffer based on request
func (fs *FileServer) SaveFile(req *utils.FileRequest) {
	fs.logger("Handling create file: %s", req.FileName)

	var err error

	// Delegate based on data source
	if req.LocalFilePath != "" {
		// File is on disk - copy from path
		err = fs.SaveFromPath(req)
	} else if req.Data != nil {
		// File is in memory - write from buffer
		err = fs.SaveFromBuffer(req)
	} else {
		// Neither source provided
		fs.logger("ERROR: Neither LocalFilePath nor Data provided for file %s", req.FileName)
		return
	}

	if err != nil {
		fs.logger("Failed to save file %s: %v", req.FileName, err)
		return
	}

	fs.logger("Created file %s successfully", req.FileName)
}

// SaveFromPath saves a file by copying from a local file path
// Used when the file is already on disk (owner creates locally, replication source)
func (fs *FileServer) SaveFromPath(req *utils.FileRequest) error {
	fs.logger("Saving file from path - file: %s, source: %s", req.FileName, req.LocalFilePath)

	if req.LocalFilePath == "" {
		return fmt.Errorf("LocalFilePath is empty")
	}

	// Determine storage path based on ownership
	storagePath := fs.determineStoragePath(req)

	// Ensure parent directory exists
	if err := os.MkdirAll(filepath.Dir(storagePath), 0755); err != nil {
		fs.logger("Failed to create parent directory: %v", err)
		return fmt.Errorf("failed to create parent directory: %v", err)
	}

	// Check if file already exists
	if _, err := os.Stat(storagePath); err == nil {
		fs.logger("File %s already exists at %s", req.FileName, storagePath)
		return fmt.Errorf("file already exists")
	}

	// Copy file from source to destination
	if err := fs.copyFile(req.LocalFilePath, storagePath); err != nil {
		fs.logger("Failed to copy file: %v", err)
		return fmt.Errorf("failed to copy file: %v", err)
	}

	// Create metadata
	opID := fs.getNextOperationID()
	fs.createFileMetadata(req.FileName, storagePath, opID, req.ClientID)

	fs.logger("Saved file from path successfully - %s -> %s", req.LocalFilePath, storagePath)
	return nil
}

// SaveFromBuffer saves a file by writing data from memory buffer
// Used when file data is received over network (forwarded CREATE, replica receives)
func (fs *FileServer) SaveFromBuffer(req *utils.FileRequest) error {
	fs.logger("Saving file from buffer - file: %s, size: %d bytes", req.FileName, len(req.Data))

	if req.Data == nil || len(req.Data) == 0 {
		return fmt.Errorf("Data buffer is empty")
	}

	// Determine storage path based on ownership
	storagePath := fs.determineStoragePath(req)

	// Check if file already exists
	if _, err := os.Stat(storagePath); err == nil {
		fs.logger("File %s already exists at %s", req.FileName, storagePath)
		return fmt.Errorf("file already exists")
	}

	// Create directory if needed
	if err := os.MkdirAll(filepath.Dir(storagePath), 0755); err != nil {
		fs.logger("Failed to create directory: %v", err)
		return fmt.Errorf("failed to create directory: %v", err)
	}

	// Write buffer to file
	if err := ioutil.WriteFile(storagePath, req.Data, 0644); err != nil {
		fs.logger("Failed to write file: %v", err)
		return fmt.Errorf("failed to write file: %v", err)
	}

	// Create metadata
	opID := fs.getNextOperationID()
	fs.createFileMetadata(req.FileName, storagePath, opID, req.ClientID)

	fs.logger("Saved file from buffer successfully - %s (%d bytes)", storagePath, len(req.Data))
	return nil
}

// determineStoragePath determines whether to store in OwnedFiles or ReplicatedFiles
func (fs *FileServer) determineStoragePath(req *utils.FileRequest) string {
	// If this node (DestinationNodeID) is the owner, store in OwnedFiles
	// Otherwise, store in ReplicatedFiles
	if req.OwnerNodeID != "" && req.DestinationNodeID == req.OwnerNodeID {
		// This node is the owner
		return filepath.Join(fs.ownedFilesDir, req.FileName)
	} else if req.OwnerNodeID != "" && req.DestinationNodeID != req.OwnerNodeID {
		// This node is a replica
		return filepath.Join(fs.replicatedFilesDir, req.FileName)
	} else {
		// Fallback: if OwnerNodeID not set, use old logic
		if req.SourceNodeID == req.DestinationNodeID {
			return filepath.Join(fs.ownedFilesDir, req.FileName)
		}
		return filepath.Join(fs.replicatedFilesDir, req.FileName)
	}
}

// handleGetFile handles file retrieval requests
func (fs *FileServer) handleGetFile(req *utils.FileRequest) {
	fs.logger("Handling get file: %s", req.FileName)

	// Find file in storage
	filePath := fs.findFile(req.FileName)
	if filePath == "" {
		fs.logger("File %s not found", req.FileName)
		return
	}

	// Copy to requested location if specified
	if req.LocalFilePath != "" {
		if err := fs.copyFile(filePath, req.LocalFilePath); err != nil {
			fs.logger("Failed to copy file to %s: %v", req.LocalFilePath, err)
			return
		}
	}

	fs.logger("Retrieved file %s successfully", req.FileName)
}

// convergerThread processes pending operations in order
func (fs *FileServer) convergerThread() {
	ticker := time.NewTicker(1000 * time.Millisecond) // Check every 100ms
	defer ticker.Stop()

	for {
		select {
		case <-fs.shutdownCh:
			return
		case <-ticker.C:
			fs.processPendingOperations()
		}
	}
}

// IncrementAndGetOperationID atomically returns the next operation ID for a file
// Increments NextOperationId on assignment (separate from LastOperationId which tracks applied ops)
// Returns (nextOperationID, error)
func (fs *FileServer) IncrementAndGetOperationID(fileName string) (int, error) {
	fs.metadataMutex.Lock()
	defer fs.metadataMutex.Unlock()

	metadata, exists := fs.fileMetadata[fileName]
	if !exists {
		return 0, fmt.Errorf("file %s not found", fileName)
	}

	// If NextOperationId is 0 (uninitialized), initialize it from LastOperationId
	if metadata.NextOperationId == 0 {
		metadata.NextOperationId = metadata.LastOperationId + 1
	}

	// Assign current NextOperationId and increment for next assignment
	assignedOpID := metadata.NextOperationId
	metadata.NextOperationId++

	fs.logger("Assigned next operation ID for file %s: %d (lastApplied=%d, nextToAssign=%d)",
		fileName, assignedOpID, metadata.LastOperationId, metadata.NextOperationId)
	return assignedOpID, nil
}

// processPendingOperations applies pending operations in order
func (fs *FileServer) processPendingOperations() {
	fs.metadataMutex.Lock()
	defer fs.metadataMutex.Unlock()

	for fileName, metadata := range fs.fileMetadata {
		// Check if there are pending operations
		if metadata.PendingOperations == nil || metadata.PendingOperations.IsEmpty() {
			continue
		}

		// Get all pending operations (already sorted in TreeSet)
		pendingOps := metadata.PendingOperations.GetAll()
		fs.logger("Converger: Found %d pending operations for %s (lastOpID=%d)",
			len(pendingOps), fileName, metadata.LastOperationId)

		// Process operations in order
		successCount := 0
		skipCount := 0
		for _, op := range pendingOps {
			// Skip operations that are already applied (duplicates or out-of-order)
			if op.ID <= metadata.LastOperationId {
				fs.logger("Converger: Skipping operation %d for file %s (already applied, lastOpID=%d)", op.ID, fileName, metadata.LastOperationId)
				skipCount++
				continue
			}

			// Check if this is the next expected operation
			expectedID := metadata.LastOperationId + 1
			if op.ID != expectedID {
				// Missing an operation in sequence, stop processing
				fs.logger("Converger: Waiting for operation %d for file %s (got %d)", expectedID, fileName, op.ID)
				break
			}

			// Try to apply this operation
			fs.logger("Converger: Applying operation %d for file %s", op.ID, fileName)
			if fs.applyOperation(fileName, op) {
				successCount++
				metadata.LastOperationId = op.ID
				metadata.Operations = append(metadata.Operations, op)
				fs.logger(">>> REPLICA COMPLETED APPEND <<<: file=%s, opID=%d", fileName, op.ID)
			} else {
				// Failed to apply, stop processing
				fs.logger("Converger: Failed to apply operation %d for file %s", op.ID, fileName)
				break
			}
		}

		// Remove successfully processed operations AND skipped duplicates from TreeSet
		totalRemoved := successCount + skipCount
		if totalRemoved > 0 {
			metadata.PendingOperations.RemoveFirst(totalRemoved)
			if successCount > 0 {
				fs.logger(">>> CONVERGER: Processed %d operations for %s (skipped %d duplicates) <<<", successCount, fileName, skipCount)
			} else if skipCount > 0 {
				fs.logger("Converger: Removed %d duplicate operations from queue for file %s", skipCount, fileName)
			}
		}
	}
}

// applyOperation applies a single operation to a file
func (fs *FileServer) applyOperation(fileName string, op utils.Operation) bool {
	metadata := fs.fileMetadata[fileName]
	if metadata == nil {
		return false
	}

	switch op.Type {
	case utils.Append:
		return fs.applyAppendOperation(fileName, op)
	default:
		fs.logger("Unknown operation type in converger: %v", op.Type)
		return false
	}
}

// applyAppendOperation applies an append operation
func (fs *FileServer) applyAppendOperation(fileName string, op utils.Operation) bool {
	metadata := fs.fileMetadata[fileName]
	if metadata == nil {
		fs.logger("Cannot apply append to %s - metadata not found", fileName)
		return false
	}

	filePath := metadata.Location
	if filePath == "" {
		// Try to find file if location is empty
		filePath = fs.findFile(fileName)
		if filePath == "" {
			fs.logger("Cannot apply append to %s - file not found in storage", fileName)
			return false
		}
		// Update metadata with correct location
		metadata.Location = filePath
	}

	// Read data from temp file (if LocalFilePath is set) or use in-memory data
	var dataToAppend []byte
	var tempFilePath string

	if op.LocalFilePath != "" {
		// Read from temp file in /TempFiles
		tempFilePath = op.LocalFilePath
		fs.logger("Converger: Reading temp file %s for append", tempFilePath)
		data, err := ioutil.ReadFile(tempFilePath)
		if err != nil {
			fs.logger("Converger: Failed to read temp file %s for append: %v", tempFilePath, err)
			return false
		}
		dataToAppend = data
		fs.logger("Converger: Read %d bytes from temp file", len(dataToAppend))
	} else if op.Data != nil {
		// Fallback: use in-memory data (for backwards compatibility)
		dataToAppend = op.Data
		fs.logger("Converger: Using in-memory data (%d bytes)", len(dataToAppend))
	} else {
		fs.logger("Converger: No data available for append operation %d on file %s", op.ID, fileName)
		return false
	}

	// Open file for appending
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		fs.logger("Failed to open file %s for append: %v", fileName, err)
		return false
	}
	defer file.Close()

	// Write data
	if _, err := file.Write(dataToAppend); err != nil {
		fs.logger("Converger: Failed to append to file %s: %v", fileName, err)
		return false
	}

	fs.logger(">>> CONVERGER: Successfully appended %d bytes to %s (opID=%d) <<<", len(dataToAppend), fileName, op.ID)

	// Update metadata
	metadata.LastModified = time.Now()
	metadata.Size += int64(len(dataToAppend))

	// Recompute hash
	if newHash, err := utils.ComputeFileHash(filePath); err == nil {
		metadata.Hash = newHash
	}

	// Clean up temp file after successful append
	if tempFilePath != "" {
		if err := os.Remove(tempFilePath); err != nil {
			fs.logger("Warning: Failed to remove temp file %s: %v", tempFilePath, err)
			// Don't fail the operation just because cleanup failed
		} else {
			fs.logger("Cleaned up temp file %s", tempFilePath)
		}
	}

	fs.logger("Applied append operation %d to file %s", op.ID, fileName)
	return true
}

// Helper methods

func (fs *FileServer) getNextOperationID() int {
	fs.operationMutex.Lock()
	defer fs.operationMutex.Unlock()

	fs.operationCounter++
	return fs.operationCounter
}

func (fs *FileServer) createFileMetadata(fileName, location string, opID int, clientID string) {
	fs.metadataMutex.Lock()
	defer fs.metadataMutex.Unlock()

	// Compute file hash
	hash, err := utils.ComputeFileHash(location)
	if err != nil {
		hash = ""
	}

	// Get file size
	stat, err := os.Stat(location)
	size := int64(0)
	if err == nil {
		size = stat.Size()
	}

	fs.fileMetadata[fileName] = &utils.FileMetaData{
		FileName:          fileName,
		LastModified:      time.Now(),
		Hash:              hash,
		Location:          location,
		Type:              utils.Self,
		LastOperationId:   opID,
		NextOperationId:   opID + 1, // Initialize next ID (for CREATE, opID=1, so NextOperationId=2)
		Operations:        []utils.Operation{},
		PendingOperations: &utils.TreeSet{}, // Initialize as TreeSet
		Size:              size,
	}

	// Also add to FileSystem for coordinator tracking
	fs.fileSystem.AddFile(fileName, fs.fileMetadata[fileName])

	fs.logger("Created metadata for file %s at %s", fileName, location)
}

func (fs *FileServer) addPendingOperation(fileName string, op utils.Operation) {
	fs.metadataMutex.Lock()
	defer fs.metadataMutex.Unlock()

	metadata := fs.fileMetadata[fileName]
	if metadata == nil {
		// For append operations, file must already exist - find it first
		filePath := fs.findFile(fileName)
		if filePath == "" {
			fs.logger("Cannot append to file %s - file not found in storage", fileName)
			return
		}

		fs.metadataMutex.Unlock()
		fs.createFileMetadata(fileName, filePath, op.ID, op.ClientID)
		fs.metadataMutex.Lock()
		metadata = fs.fileMetadata[fileName]
	}

	// Initialize PendingOperations if nil
	if metadata.PendingOperations == nil {
		metadata.PendingOperations = &utils.TreeSet{}
	}

	// Add operation to TreeSet (automatically sorted)
	metadata.PendingOperations.Add(op)
	fs.logger("Added operation %d to pending queue for file %s", op.ID, fileName)
}

func (fs *FileServer) findFile(fileName string) string {
	// Check owned files first
	ownedPath := filepath.Join(fs.ownedFilesDir, fileName)
	if _, err := os.Stat(ownedPath); err == nil {
		return ownedPath
	}

	// Check replicated files
	replicatedPath := filepath.Join(fs.replicatedFilesDir, fileName)
	if _, err := os.Stat(replicatedPath); err == nil {
		return replicatedPath
	}

	return ""
}

func (fs *FileServer) copyFile(src, dst string) error {
	// Create destination directory if needed
	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return err
	}

	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	dstFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	_, err = io.Copy(dstFile, srcFile)
	return err
}

// Public query methods

// GetFileMetadata returns metadata for a specific file
func (fs *FileServer) GetFileMetadata(fileName string) *utils.FileMetaData {
	fs.metadataMutex.RLock()
	defer fs.metadataMutex.RUnlock()

	if metadata, exists := fs.fileMetadata[fileName]; exists {
		// Return a copy to prevent external modification
		copy := *metadata
		return &copy
	}
	return nil
}

// ListStoredFiles returns all files stored on this server
func (fs *FileServer) ListStoredFiles() map[string]*utils.FileMetaData {
	fs.metadataMutex.RLock()
	defer fs.metadataMutex.RUnlock()

	result := make(map[string]*utils.FileMetaData)
	for fileName, metadata := range fs.fileMetadata {
		copy := *metadata
		result[fileName] = &copy
	}
	return result
}

// ListReplicatedFiles returns only files stored in ReplicatedFiles directory
// (files where this node is a replica, not the owner)
func (fs *FileServer) ListReplicatedFiles() map[string]*utils.FileMetaData {
	fs.metadataMutex.RLock()
	defer fs.metadataMutex.RUnlock()

	result := make(map[string]*utils.FileMetaData)
	for fileName, metadata := range fs.fileMetadata {
		// Check if file location is in ReplicatedFiles directory
		if metadata.Location != "" {
			// Check if the location path contains ReplicatedFiles
			if filepath.Dir(metadata.Location) == fs.replicatedFilesDir ||
				filepath.HasPrefix(metadata.Location, fs.replicatedFilesDir+string(filepath.Separator)) {
				copy := *metadata
				result[fileName] = &copy
			}
		} else {
			// If location not set, check directly on disk
			replicatedPath := filepath.Join(fs.replicatedFilesDir, fileName)
			if _, err := os.Stat(replicatedPath); err == nil {
				copy := *metadata
				result[fileName] = &copy
			}
		}
	}
	return result
}

// ReadFile synchronously reads file data from storage
// Returns file data as bytes, or error if file not found
func (fs *FileServer) ReadFile(fileName string) ([]byte, error) {
	// Find file in storage (checks OwnedFiles first, then ReplicatedFiles)
	filePath := fs.findFile(fileName)
	if filePath == "" {
		return nil, fmt.Errorf("file %s not found", fileName)
	}

	// Read file content
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file %s: %v", fileName, err)
	}

	return data, nil
}

// GetFileSystem returns the FileSystem instance for coordinator integration
func (fs *FileServer) GetFileSystem() *utils.FileSystem {
	return fs.fileSystem
}

// GetOwnedFilesDir returns the path to the OwnedFiles directory
func (fs *FileServer) GetOwnedFilesDir() string {
	return fs.ownedFilesDir
}

// GetTempFilesDir returns the path to the TempFiles directory
func (fs *FileServer) GetTempFilesDir() string {
	return fs.tempFilesDir
}

// GetTempFilePath returns the temp file path for an append operation
func (fs *FileServer) GetTempFilePath(fileName string, opID int) string {
	sanitizedName := strings.ReplaceAll(fileName, "/", "_")
	tempFileName := fmt.Sprintf("%s_op%d.tmp", sanitizedName, opID)
	return filepath.Join(fs.tempFilesDir, tempFileName)
}

// CreateMetadataIfNeeded creates file metadata if it doesn't exist (for idempotent CREATE)
func (fs *FileServer) CreateMetadataIfNeeded(fileName, location string, opID int, clientID string) {
	fs.metadataMutex.Lock()
	defer fs.metadataMutex.Unlock()

	// Check if metadata already exists
	if _, exists := fs.fileMetadata[fileName]; exists {
		return // Metadata already exists
	}

	// Create metadata using the same logic as createFileMetadata
	hash, err := utils.ComputeFileHash(location)
	if err != nil {
		hash = ""
	}

	stat, err := os.Stat(location)
	size := int64(0)
	if err == nil {
		size = stat.Size()
	}

	fs.fileMetadata[fileName] = &utils.FileMetaData{
		FileName:          fileName,
		LastModified:      time.Now(),
		Hash:              hash,
		Location:          location,
		Type:              utils.Self,
		LastOperationId:   opID,
		NextOperationId:   opID + 1, // Initialize next ID
		Operations:        []utils.Operation{},
		PendingOperations: &utils.TreeSet{},
		Size:              size,
	}

	// Also add to FileSystem for coordinator tracking
	fs.fileSystem.AddFile(fileName, fs.fileMetadata[fileName])
}
