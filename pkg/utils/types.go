package utils

import (
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
	Operations        []Operation `json:"operations"`         // History of operations
	PendingOperations []Operation `json:"pending_operations"` // Operations waiting to be applied
	Size              int64       `json:"size"`               // File size in bytes
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
	Data              []byte        `json:"data,omitempty"`
	LocalFilePath     string        `json:"local_file_path,omitempty"`
}

// Response represents a response to file operations
type Response struct {
	Success   bool   `json:"success"`
	Message   string `json:"message"`
	ErrorCode int    `json:"error_code,omitempty"`
}
