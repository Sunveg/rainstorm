package network

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"hydfs/pkg/fileserver"
	"hydfs/pkg/utils"
)

// Server handles HTTP requests for file operations
type Server struct {
	httpServer *http.Server
	fileServer *fileserver.FileServer
	logger     func(string, ...interface{})
	nodeID     string
}

// NewServer creates a new HTTP server for handling file operations
func NewServer(address string, fileServer *fileserver.FileServer, nodeID string, logger func(string, ...interface{})) *Server {
	s := &Server{
		fileServer: fileServer,
		logger:     logger,
		nodeID:     nodeID,
	}

	mux := http.NewServeMux()

	// File operation endpoints
	mux.HandleFunc("/api/file/create", s.handleCreateFile)
	mux.HandleFunc("/api/file/append", s.handleAppendFile)
	mux.HandleFunc("/api/file/get", s.handleGetFile)
	mux.HandleFunc("/api/file/list", s.handleListFiles)
	mux.HandleFunc("/api/file/merge", s.handleMergeFile)

	// Replication endpoints
	mux.HandleFunc("/api/replicate", s.handleReplication)

	// Health check
	mux.HandleFunc("/api/health", s.handleHealthCheck)

	s.httpServer = &http.Server{
		Addr:    address,
		Handler: mux,
	}

	return s
}

// Start starts the HTTP server
func (s *Server) Start() error {
	s.logger("HTTP server starting on %s", s.httpServer.Addr)
	return s.httpServer.ListenAndServe()
}

// Stop stops the HTTP server
func (s *Server) Stop() error {
	s.logger("HTTP server stopping")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.httpServer.Shutdown(ctx)
}

// handleCreateFile handles file creation requests
func (s *Server) handleCreateFile(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req FileRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.sendErrorResponse(w, "Invalid request format", http.StatusBadRequest)
		return
	}

	// Create file request for the file server
	fileReq := &utils.FileRequest{
		OperationType:     utils.Create,
		FileName:          req.Filename,
		Data:              req.Data,
		ClientID:          strconv.FormatInt(req.ClientID, 10),
		FileOperationID:   int(req.OperationID),
		SourceNodeID:      req.SourceNodeID,
		DestinationNodeID: req.DestinationNodeID,
		OwnerNodeID:       req.OwnerNodeID,
	}

	// If DestinationNodeID is not set, use this node's ID
	if fileReq.DestinationNodeID == "" {
		fileReq.DestinationNodeID = s.nodeID
	}

	// Submit to file server for processing
	err := s.fileServer.SubmitRequest(fileReq)
	if err != nil {
		s.sendErrorResponse(w, fmt.Sprintf("Failed to submit request: %v", err), http.StatusInternalServerError)
		return
	}

	// For now, return success immediately
	// TODO: Add proper response handling with channels or callbacks
	response := FileResponse{
		Success: true,
		Version: 1, // Placeholder
	}
	s.sendJSONResponse(w, response)
}

// handleAppendFile handles file append requests
func (s *Server) handleAppendFile(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req FileRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.sendErrorResponse(w, "Invalid request format", http.StatusBadRequest)
		return
	}

	// Create file request for the file server
	fileReq := &utils.FileRequest{
		OperationType:     utils.Append,
		FileName:          req.Filename,
		Data:              req.Data,
		ClientID:          strconv.FormatInt(req.ClientID, 10),
		FileOperationID:   int(req.OperationID),
		SourceNodeID:      req.SourceNodeID,
		DestinationNodeID: req.DestinationNodeID,
		OwnerNodeID:       req.OwnerNodeID,
	}

	// If DestinationNodeID is not set, use this node's ID
	if fileReq.DestinationNodeID == "" {
		fileReq.DestinationNodeID = s.nodeID
	}

	// Submit to file server for processing
	err := s.fileServer.SubmitRequest(fileReq)
	if err != nil {
		s.sendErrorResponse(w, fmt.Sprintf("Failed to submit request: %v", err), http.StatusInternalServerError)
		return
	}

	// For now, return success immediately
	// TODO: Add proper response handling with channels or callbacks
	response := FileResponse{
		Success: true,
		Version: 1, // Placeholder
	}
	s.sendJSONResponse(w, response)
}

// handleGetFile handles file retrieval requests
func (s *Server) handleGetFile(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req FileRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.sendErrorResponse(w, "Invalid request format", http.StatusBadRequest)
		return
	}

	// Create file request for the file server
	fileReq := &utils.FileRequest{
		OperationType: utils.Get,
		FileName:      req.Filename,
		ClientID:      strconv.FormatInt(req.ClientID, 10),
	}

	// Submit to file server for processing
	err := s.fileServer.SubmitRequest(fileReq)
	if err != nil {
		s.sendErrorResponse(w, fmt.Sprintf("Failed to submit request: %v", err), http.StatusInternalServerError)
		return
	}

	// For now, return success immediately
	// TODO: Add proper response handling for file data retrieval
	response := FileResponse{
		Success: true,
		Data:    []byte("placeholder data"), // Placeholder
		Version: 1,                          // Placeholder
	}
	s.sendJSONResponse(w, response)
}

// handleListFiles handles file listing requests
func (s *Server) handleListFiles(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		ClientID int64 `json:"client_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.sendErrorResponse(w, "Invalid request format", http.StatusBadRequest)
		return
	}

	// Get stored files from file server
	filesMap := s.fileServer.ListStoredFiles()

	// Extract filenames from the metadata map
	var filenames []string
	for filename := range filesMap {
		filenames = append(filenames, filename)
	}

	response := struct {
		Filenames []string `json:"filenames"`
	}{
		Filenames: filenames,
	}

	s.sendJSONResponse(w, response)
}

// handleMergeFile handles file merge requests
func (s *Server) handleMergeFile(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req MergeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.sendErrorResponse(w, "Invalid request format", http.StatusBadRequest)
		return
	}

	// TODO: Implement merge functionality
	response := MergeResponse{
		Success:      true,
		FinalVersion: 1, // Placeholder
	}

	s.logger("Merge operation requested for file: %s by client: %d", req.Filename, req.ClientID)
	s.sendJSONResponse(w, response)
}

// handleReplication handles replication requests from other nodes
func (s *Server) handleReplication(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req ReplicationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.sendErrorResponse(w, "Invalid request format", http.StatusBadRequest)
		return
	}

	s.logger("Received replication request for file: %s from node: %s", req.Filename, req.SenderID)

	// Create file request for the file server
	fileReq := &utils.FileRequest{
		OperationType:   req.Operation.Type,
		FileName:        req.Operation.Filename,
		Data:            req.Operation.Data,
		ClientID:        strconv.FormatInt(req.Operation.ClientID, 10),
		FileOperationID: int(req.Operation.OperationID),
	}

	// Submit to file server for processing
	err := s.fileServer.SubmitRequest(fileReq)
	if err != nil {
		response := ReplicationResponse{
			Success: false,
			Error:   fmt.Sprintf("Failed to submit request: %v", err),
		}
		s.sendJSONResponse(w, response)
		return
	}

	// Return success response for replication
	response := ReplicationResponse{
		Success: true,
	}
	s.sendJSONResponse(w, response)
}

// handleHealthCheck handles health check requests
func (s *Server) handleHealthCheck(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Simple health check - return OK if server is running
	filesMap := s.fileServer.ListStoredFiles()

	response := struct {
		Healthy     bool `json:"healthy"`
		ActiveFiles int  `json:"active_files"`
	}{
		Healthy:     true,
		ActiveFiles: len(filesMap),
	}

	s.sendJSONResponse(w, response)
}

// sendJSONResponse sends a JSON response
func (s *Server) sendJSONResponse(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")

	jsonData, err := json.Marshal(data)
	if err != nil {
		s.logger("Failed to marshal response: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write(jsonData)
}

// sendErrorResponse sends an error response
func (s *Server) sendErrorResponse(w http.ResponseWriter, message string, statusCode int) {
	s.logger("HTTP Error: %s", message)
	response := struct {
		Success bool   `json:"success"`
		Error   string `json:"error"`
	}{
		Success: false,
		Error:   message,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)

	jsonData, _ := json.Marshal(response)
	w.Write(jsonData)
}
