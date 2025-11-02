package fileserver

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
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

// processRequest handles a single file request
func (fs *FileServer) processRequest(req *utils.FileRequest, workerID int) {
	fs.logger("Worker %d processing %s request for file %s", workerID, req.OperationType, req.FileName)

	switch req.OperationType {
	case utils.Create:
		fs.handleCreateFile(req)
	case utils.Append:
		fs.handleAppendFile(req)
	case utils.Get:
		fs.handleGetFile(req)
	default:
		fs.logger("Unknown operation type: %v", req.OperationType)
	}
}

// handleCreateFile handles file creation requests
func (fs *FileServer) handleCreateFile(req *utils.FileRequest) {
	fs.logger("Handling create file: %s", req.FileName)

	// Generate operation ID
	opID := fs.getNextOperationID()

	// Determine storage path based on ownership
	// If this node (DestinationNodeID) is the owner, store in OwnedFiles
	// Otherwise, store in ReplicatedFiles
	var storagePath string
	if req.OwnerNodeID != "" && req.DestinationNodeID == req.OwnerNodeID {
		// This node is the owner
		storagePath = filepath.Join(fs.ownedFilesDir, req.FileName)
	} else if req.OwnerNodeID != "" && req.DestinationNodeID != req.OwnerNodeID {
		// This node is a replica
		storagePath = filepath.Join(fs.replicatedFilesDir, req.FileName)
	} else {
		// Fallback: if OwnerNodeID not set, use old logic
		if req.SourceNodeID == req.DestinationNodeID {
			storagePath = filepath.Join(fs.ownedFilesDir, req.FileName)
		} else {
			storagePath = filepath.Join(fs.replicatedFilesDir, req.FileName)
		}
	}

	// Check if file already exists
	if _, err := os.Stat(storagePath); err == nil {
		fs.logger("File %s already exists", req.FileName)
		return
	}

	// Copy from local file path if specified
	if req.LocalFilePath != "" {
		if err := fs.copyFile(req.LocalFilePath, storagePath); err != nil {
			fs.logger("Failed to copy file: %v", err)
			return
		}
	} else if req.Data != nil {
		// Write data directly
		if err := ioutil.WriteFile(storagePath, req.Data, 0644); err != nil {
			fs.logger("Failed to write file: %v", err)
			return
		}
	}

	// Create metadata
	fs.createFileMetadata(req.FileName, storagePath, opID, req.ClientID)

	fs.logger("Created file %s successfully", req.FileName)
}

// handleAppendFile handles file append requests
func (fs *FileServer) handleAppendFile(req *utils.FileRequest) {
	fs.logger("Handling append to file: %s", req.FileName)

	// Generate operation ID
	opID := fs.getNextOperationID()

	// Create operation record
	operation := utils.Operation{
		ID:        opID,
		Type:      utils.Append,
		Timestamp: time.Now(),
		ClientID:  req.ClientID,
		FileName:  req.FileName,
		Data:      req.Data,
	}

	// Add to pending operations (will be processed by converger)
	fs.addPendingOperation(req.FileName, operation)

	fs.logger("Queued append operation %d for file %s", opID, req.FileName)
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
	ticker := time.NewTicker(100 * time.Millisecond) // Check every 100ms
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

// processPendingOperations applies pending operations in order
func (fs *FileServer) processPendingOperations() {
	fs.metadataMutex.Lock()
	defer fs.metadataMutex.Unlock()

	for fileName, metadata := range fs.fileMetadata {
		if len(metadata.PendingOperations) == 0 {
			continue
		}

		// Sort pending operations by ID to ensure ordering
		sort.Slice(metadata.PendingOperations, func(i, j int) bool {
			return metadata.PendingOperations[i].ID < metadata.PendingOperations[j].ID
		})

		// Process operations in order
		var processedOps []utils.Operation
		for _, op := range metadata.PendingOperations {
			if fs.applyOperation(fileName, op) {
				processedOps = append(processedOps, op)
			} else {
				break // Stop on first failed operation to maintain order
			}
		}

		// Remove processed operations
		if len(processedOps) > 0 {
			metadata.PendingOperations = metadata.PendingOperations[len(processedOps):]
			metadata.Operations = append(metadata.Operations, processedOps...)
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
	filePath := metadata.Location

	// Open file for appending
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		fs.logger("Failed to open file %s for append: %v", fileName, err)
		return false
	}
	defer file.Close()

	// Write data
	if _, err := file.Write(op.Data); err != nil {
		fs.logger("Failed to append to file %s: %v", fileName, err)
		return false
	}

	// Update metadata
	metadata.LastModified = time.Now()
	metadata.Size += int64(len(op.Data))

	// Recompute hash
	if newHash, err := utils.ComputeFileHash(filePath); err == nil {
		metadata.Hash = newHash
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
	var size int64
	if info, err := os.Stat(location); err == nil {
		size = info.Size()
	}

	metadata := &utils.FileMetaData{
		FileName:          fileName,
		LastModified:      time.Now(),
		Hash:              hash,
		Location:          location,
		Type:              utils.Self, // Will be updated based on ownership
		Operations:        []utils.Operation{},
		PendingOperations: []utils.Operation{},
		Size:              size,
	}

	fs.fileMetadata[fileName] = metadata
}

func (fs *FileServer) addPendingOperation(fileName string, op utils.Operation) {
	fs.metadataMutex.Lock()
	defer fs.metadataMutex.Unlock()

	metadata := fs.fileMetadata[fileName]
	if metadata == nil {
		// Create metadata if file doesn't exist yet
		fs.metadataMutex.Unlock()
		fs.createFileMetadata(fileName, "", op.ID, op.ClientID)
		fs.metadataMutex.Lock()
		metadata = fs.fileMetadata[fileName]
	}

	metadata.PendingOperations = append(metadata.PendingOperations, op)
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
