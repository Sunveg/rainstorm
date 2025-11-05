package utils

import (
	"fmt"
	"sync"
	"time"
)

// FileType represents the type of file storage on a node
type FileType int

const (
	Self FileType = iota
	Replica1
	Replica2
)

// OperationType represents the type of file operation
type OperationType int

const (
	Create OperationType = iota
	Append
	Get
	Merge
	Delete
)

// Operation represents a single file operation with ordering
type Operation struct {
	ID            int           `json:"id"`
	Type          OperationType `json:"type"`
	Timestamp     time.Time     `json:"timestamp"`
	ClientID      string        `json:"client_id"`
	FileName      string        `json:"file_name"`
	Data          []byte        `json:"data,omitempty"`
	LocalFilePath string        `json:"local_file_path,omitempty"`
}

// MembershipTable represents a node in the membership protocol
type MembershipTable struct {
	NodeID      string    `json:"node_id"`     // IP + Port
	State       string    `json:"state"`       // Active, Failed, Left
	Incarnation int       `json:"incarnation"` // Version number for failure detection
	Timestamp   time.Time `json:"timestamp"`   // Last update time
}

// FileMetaData represents metadata for a file stored in HyDFS
type FileMetaData struct {
	FileName          string      `json:"file_name"`
	LastModified      time.Time   `json:"last_modified"`
	Hash              string      `json:"hash"`               // SHA-256 hash of file content
	Location          string      `json:"location"`           // File path on disk
	Type              FileType    `json:"type"`               // Self, Replica1, Replica2
	LastOperationId   int         `json:"last_operation_id"`  // Last operation ID for ordering
	Operations        []Operation `json:"operations"`         // History of operations
	PendingOperations *TreeSet    `json:"pending_operations"` // Operations waiting to be applied, sorted by opID
	Size              int64       `json:"size"`               // File size in bytes
}

// ComputeHash computes the hash of a file
func (fm *FileMetaData) ComputeHash(data []byte) string {
	// TODO: Implement hash computation
	return ""
}

// FileRequest represents a request for file operations between nodes
type FileRequest struct {
	OperationType     OperationType `json:"operation_type"`
	FileName          string        `json:"file_name"`
	FileLoc           string        `json:"file_loc"`
	FileOperationID   int           `json:"file_operation_id"`
	ClientID          string        `json:"client_id"`
	SourceNodeID      string        `json:"source_node_id"`
	DestinationNodeID string        `json:"destination_node_id"`
	OwnerNodeID       string        `json:"owner_node_id,omitempty"` // Primary owner node for this file
	Data              []byte        `json:"data,omitempty"`
	LocalFilePath     string        `json:"local_file_path,omitempty"`
}

// Response represents a response to file operations
type Response struct {
	Success   bool   `json:"success"`
	Message   string `json:"message"`
	ErrorCode int    `json:"error_code,omitempty"`
}

// TreeSet represents a thread-safe sorted set of Operations
type TreeSet struct {
	mu         sync.RWMutex
	operations []Operation
}

// Add adds an operation to the TreeSet in sorted order by operation ID
func (ts *TreeSet) Add(op Operation) {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	// Find insertion point using binary search
	index := 0
	for index < len(ts.operations) && ts.operations[index].ID < op.ID {
		index++
	}

	// Insert the operation at the correct position
	if index == len(ts.operations) {
		ts.operations = append(ts.operations, op)
	} else {
		ts.operations = append(ts.operations[:index+1], ts.operations[index:]...)
		ts.operations[index] = op
	}
}

// GetAll returns all operations in sorted order
func (ts *TreeSet) GetAll() []Operation {
	ts.mu.RLock()
	defer ts.mu.RUnlock()

	result := make([]Operation, len(ts.operations))
	copy(result, ts.operations)
	return result
}

// FileSystem represents the file system state of a node
type FileSystem struct {
	mu    sync.RWMutex
	Files map[string]*FileMetaData // Map of filename to FileMetaData
	// TODO : Remove this channel, we might not require this
	PendingOperations chan FileRequest // Queue for pending operations
}

// NewFileSystem creates a new FileSystem instance
func NewFileSystem() *FileSystem {
	return &FileSystem{
		Files:             make(map[string]*FileMetaData),
		PendingOperations: make(chan FileRequest, 1000), // Buffer size of 1000
	}
}

// GetFiles returns a copy of the files map
func (fs *FileSystem) GetFiles() map[string]*FileMetaData {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	result := make(map[string]*FileMetaData, len(fs.Files))
	for k, v := range fs.Files {
		result[k] = v
	}
	return result
}

// GetFile returns a file's metadata by name
func (fs *FileSystem) GetFile(filename string) *FileMetaData {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	if metadata, exists := fs.Files[filename]; exists {
		copy := *metadata
		return &copy
	}
	return nil
}

// AddFile adds or updates a file's metadata
func (fs *FileSystem) AddFile(filename string, metadata *FileMetaData) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.Files[filename] = metadata
}

// QueueOperation adds an operation to the pending operations queue
func (fs *FileSystem) QueueOperation(request FileRequest) error {
	select {
	case fs.PendingOperations <- request:
		return nil
	default:
		return fmt.Errorf("pending operations queue is full")
	}
}

// FileHandlers represents the file operation handlers
type FileHandlers struct {
	NoOfThreads      int
	RequestQueue     chan FileRequest
	FileSystem       *FileSystem
	ConvergerStopped chan bool
}

// NewFileHandlers creates a new FileHandlers instance
func NewFileHandlers(threads int, fs *FileSystem) *FileHandlers {
	return &FileHandlers{
		NoOfThreads:      threads,
		RequestQueue:     make(chan FileRequest, 1000),
		FileSystem:       fs,
		ConvergerStopped: make(chan bool),
	}
}
