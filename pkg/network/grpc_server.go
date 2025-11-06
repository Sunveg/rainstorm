package network

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"hydfs/pkg/fileserver"
	"hydfs/pkg/utils"
	"hydfs/protoBuilds/coordination"
	"hydfs/protoBuilds/fileservice"

	"google.golang.org/grpc"
)

// fileServiceImpl adapts GRPCServer to implement fileservice.FileServiceServer
type fileServiceImpl struct {
	*GRPCServer
	fileservice.UnimplementedFileServiceServer
}

// HealthCheck for fileservice
func (fs *fileServiceImpl) HealthCheck(ctx context.Context, req *fileservice.HealthCheckRequest) (*fileservice.HealthCheckResponse, error) {
	fs.logger("gRPC FileService HealthCheck request from: %s", req.SenderId)

	// Get actual file count from file server
	activeFiles := int32(0)
	if fsys := fs.fileServer.GetFileSystem(); fsys != nil {
		activeFiles = int32(len(fsys.GetFiles()))
	}

	return &fileservice.HealthCheckResponse{
		Healthy:     true,
		ActiveFiles: activeFiles,
	}, nil
}

// MergeFile for fileservice
func (fs *fileServiceImpl) MergeFile(ctx context.Context, req *fileservice.MergeRequest) (*fileservice.MergeResponse, error) {
	fs.logger("gRPC FileService MergeFile request: filename=%s, client_id=%d", req.Filename, req.ClientId)

	// TODO: Implement actual merge logic
	return &fileservice.MergeResponse{
		Success:      true,
		Error:        "",
		FinalVersion: 1,
	}, nil
}

// ListFiles for fileservice
func (fs *fileServiceImpl) ListFiles(ctx context.Context, req *fileservice.ListFilesRequest) (*fileservice.ListFilesResponse, error) {
	fs.logger("gRPC FileService ListFiles request: client_id=%d", req.ClientId)

	// Get file list from file server's FileSystem
	fsys := fs.fileServer.GetFileSystem()
	if fsys == nil {
		return &fileservice.ListFilesResponse{
			Filenames: []string{},
		}, nil
	}

	// Get all files from FileSystem
	files := fsys.GetFiles()
	filenames := make([]string, 0, len(files))
	for filename := range files {
		filenames = append(filenames, filename)
	}

	return &fileservice.ListFilesResponse{
		Filenames: filenames,
	}, nil
}

// GetFile delegates to GRPCServer.GetFile
func (fs *fileServiceImpl) GetFile(req *fileservice.GetFileRequest, stream grpc.ServerStreamingServer[fileservice.FileChunk]) error {
	return fs.GRPCServer.GetFile(req, stream)
}

// SendFile delegates to GRPCServer.SendFile
func (fs *fileServiceImpl) SendFile(stream fileservice.FileService_SendFileServer) error {
	return fs.GRPCServer.SendFile(stream)
}

// SendReplica delegates to GRPCServer.SendReplica
func (fs *fileServiceImpl) SendReplica(stream fileservice.FileService_SendReplicaServer) error {
	return fs.GRPCServer.SendReplica(stream)
}

// GRPCServer handles gRPC requests for both file transfer and coordination
type GRPCServer struct {
	// Server components
	fileServer        *fileserver.FileServer
	coordinatorServer interface {
		ReplicateFileToReplicas(hydfsFileName string, localFilePath string) error
		// for append replication
		ReplicateAppendToReplicas(hydfsFileName string, localFilePath string, operationID int) error
	}

	nodeID string
	logger func(string, ...interface{})

	// gRPC servers
	fileserviceServer  *grpc.Server
	coordinationServer *grpc.Server

	// Network addresses
	fileserviceAddr  string
	coordinationAddr string

	// Server state
	startTime time.Time

	// Embedded unimplemented service servers for forward compatibility
	fileservice.UnimplementedFileServiceServer
	coordination.UnimplementedCoordinationServiceServer
}

// SetCoordinator sets the coordinator server reference
func (s *GRPCServer) SetCoordinator(coordinator interface {
	ReplicateFileToReplicas(hydfsFileName string, localFilePath string) error
	ReplicateAppendToReplicas(hydfsFileName string, localFilePath string, operationID int) error
}) {
	s.coordinatorServer = coordinator
}

// NewGRPCServer creates a new gRPC server for handling both file transfer and coordination
func NewGRPCServer(fileserviceAddr, coordinationAddr string, fileServer *fileserver.FileServer, nodeID string, logger func(string, ...interface{})) *GRPCServer {
	return &GRPCServer{
		fileServer:       fileServer,
		nodeID:           nodeID,
		logger:           logger,
		fileserviceAddr:  fileserviceAddr,
		coordinationAddr: coordinationAddr,
		startTime:        time.Now(),
	}
}

// Start starts both gRPC servers
func (s *GRPCServer) Start() error {
	// Start file transfer server
	go func() {
		if err := s.startfileserviceServer(); err != nil {
			s.logger("File transfer server error: %v", err)
		}
	}()

	// Start coordination server
	go func() {
		if err := s.startCoordinationServer(); err != nil {
			s.logger("Coordination server error: %v", err)
		}
	}()

	s.logger("gRPC servers started - fileservice: %s, Coordination: %s", s.fileserviceAddr, s.coordinationAddr)
	return nil
}

// Stop stops both gRPC servers
func (s *GRPCServer) Stop() error {
	if s.fileserviceServer != nil {
		s.fileserviceServer.GracefulStop()
	}
	if s.coordinationServer != nil {
		s.coordinationServer.GracefulStop()
	}
	return nil
}

// startfileserviceServer starts the file transfer gRPC server
func (s *GRPCServer) startfileserviceServer() error {
	lis, err := net.Listen("tcp", s.fileserviceAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %v", s.fileserviceAddr, err)
	}

	s.fileserviceServer = grpc.NewServer()
	// Register file service (primary service for file operations)
	fileservice.RegisterFileServiceServer(s.fileserviceServer, &fileServiceImpl{
		GRPCServer: s,
	})
	// Note: fileservice has its own HealthCheck, ListFiles, MergeFile
	// which conflict with coordination service. We use adapter pattern.

	s.logger("fileservice gRPC server listening on %s", s.fileserviceAddr)
	return s.fileserviceServer.Serve(lis)
}

// startCoordinationServer starts the coordination gRPC server
func (s *GRPCServer) startCoordinationServer() error {
	lis, err := net.Listen("tcp", s.coordinationAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %v", s.coordinationAddr, err)
	}

	s.coordinationServer = grpc.NewServer()
	// Register the coordination service implementation
	coordination.RegisterCoordinationServiceServer(s.coordinationServer, s)

	s.logger("Coordination gRPC server listening on %s", s.coordinationAddr)
	return s.coordinationServer.Serve(lis)
}

// GetFile from fileservice proto - streams file chunks back to client
func (s *GRPCServer) GetFile(req *fileservice.GetFileRequest, stream grpc.ServerStreamingServer[fileservice.FileChunk]) error {
	s.logger("gRPC GetFile request: filename=%s, client_id=%d", req.Filename, req.ClientId)

	// Read file directly from file server (synchronous)
	data, err := s.fileServer.ReadFile(req.Filename)
	if err != nil {
		s.logger("File not found: %v", err)
		return fmt.Errorf("file not found: %v", err)
	}

	// Stream the file back in chunks
	const chunkSize = 1024 * 1024 // 1MB chunks
	for i := 0; i < len(data); i += chunkSize {
		end := i + chunkSize
		if end > len(data) {
			end = len(data)
		}

		chunk := &fileservice.FileChunk{
			Content: data[i:end],
			Offset:  int64(i),
		}

		if err := stream.Send(chunk); err != nil {
			return fmt.Errorf("failed to send chunk: %v", err)
		}
	}

	return nil
}

// Getfileservice from fileservice proto - returns file data directly
func (s *GRPCServer) Getfileservice(ctx context.Context, req *fileservice.GetFileRequest) (*fileservice.GetFileResponse, error) {
	s.logger("gRPC Getfileservice request: filename=%s, client_id=%d", req.Filename, req.ClientId)

	// Read file directly from file server (synchronous)
	data, err := s.fileServer.ReadFile(req.Filename)
	if err != nil {
		return &fileservice.GetFileResponse{
			Success: false,
			Error:   fmt.Sprintf("File not found: %v", err),
			Data:    nil,
			Version: 0,
		}, nil
	}

	// Get file metadata for version info
	metadata := s.fileServer.GetFileMetadata(req.Filename)
	version := int64(1)
	if metadata != nil {
		version = int64(metadata.LastOperationId)
	}

	return &fileservice.GetFileResponse{
		Success: true,
		Data:    data,
		Version: version,
	}, nil
}

// CoordinationService implementation
func (s *GRPCServer) HealthCheck(ctx context.Context, req *coordination.HealthCheckRequest) (*coordination.HealthCheckResponse, error) {
	s.logger("gRPC HealthCheck request from: %s", req.SenderId)

	uptime := int64(time.Since(s.startTime).Seconds())

	return &coordination.HealthCheckResponse{
		Healthy:           true,
		ActiveFiles:       0, // TODO: Get actual count from file server
		NodeId:            s.nodeID,
		UptimeSeconds:     uptime,
		ServicesAvailable: []string{"fileserviceService", "CoordinationService"},
	}, nil
}

func (s *GRPCServer) GetNodeStatus(ctx context.Context, req *coordination.NodeStatusRequest) (*coordination.NodeStatusResponse, error) {
	s.logger("gRPC GetNodeStatus request from: %s", req.RequestingNodeId)

	return &coordination.NodeStatusResponse{
		NodeId:               s.nodeID,
		IsHealthy:            true,
		TotalFiles:           0, // TODO: Get actual count
		StorageUsedBytes:     0, // TODO: Get actual usage
		LastUpdatedTimestamp: time.Now().UnixMilli(),
		ReplicaNodes:         []string{}, // TODO: Get actual replica nodes
	}, nil
}

// coordination.ListFiles implementation on GRPCServer
func (s *GRPCServer) ListFiles(ctx context.Context, req *coordination.ListFilesRequest) (*coordination.ListFilesResponse, error) {
	s.logger("gRPC CoordinationListFiles request: client_id=%d, requesting_node=%s", req.ClientId, req.RequestingNodeId)

	// Get file list from file server's FileSystem
	fs := s.fileServer.GetFileSystem()
	if fs == nil {
		return &coordination.ListFilesResponse{
			Filenames:  []string{},
			TotalCount: 0,
		}, nil
	}

	// Get all files from FileSystem
	files := fs.GetFiles()
	filenames := make([]string, 0, len(files))
	for filename := range files {
		filenames = append(filenames, filename)
	}

	return &coordination.ListFilesResponse{
		Filenames:  filenames,
		TotalCount: int32(len(filenames)),
	}, nil
}

func (s *GRPCServer) MergeFile(ctx context.Context, req *coordination.MergeRequest) (*coordination.MergeResponse, error) {
	s.logger("gRPC MergeFile request: filename=%s, client_id=%d", req.Filename, req.ClientId)

	// TODO: Implement actual merge logic
	return &coordination.MergeResponse{
		Success:       true,
		FinalVersion:  1, // Placeholder
		InvolvedNodes: []string{s.nodeID},
	}, nil
}

// SendFile handles incoming file streams from clients or forwarding nodes
// Receive chunks
// TODO get the metadata first. Based on Metadata take decision
// IF op type is CREATE then create new metadata and add to FileSystem
// Save the file to /ownedFiles and then create Request to create Replicas and add them to submitRequest
// IF op type is APPEND then get existing metadata from FileSystem and update operation ID
// Add this operation to pending operations in metadata of that file
// Create Request to append file and add to submitRequest
func (s *GRPCServer) SendFile(stream fileservice.FileService_SendFileServer) error {
	s.logger("=== SendFile: Receiving file stream ===")

	// Step 1: Receive complete file from stream
	metadata, buffer, opID, err := s.receiveFileStream(stream)
	if err != nil {
		return s.sendFileError(stream, "Failed to receive file", err)
	}

	// Step 2: Route based on operation type
	if metadata.OperationType == fileservice.OperationType_CREATE {
		return s.processIncomingCreate(stream, metadata, buffer, opID)
	}

	if metadata.OperationType == fileservice.OperationType_APPEND {
		return s.processIncomingAppend(stream, metadata, buffer, opID)
	}

	return s.sendFileError(stream, "Unknown operation type", nil)
}

// receiveFileStream receives and buffers the entire file from the stream
func (s *GRPCServer) receiveFileStream(stream fileservice.FileService_SendFileServer) (
	*fileservice.SendFileMetadata, []byte, int64, error) {

	var metadata *fileservice.SendFileMetadata
	var buffer []byte
	var opID int64

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, 0, fmt.Errorf("stream error: %v", err)
		}

		// Handle metadata (first message)
		if md := req.GetSendFileMetadata(); md != nil {
			metadata = md
			opID = s.calculateOperationID(md)
			s.logger("Received metadata: file=%s, operation=%s, opID=%d",
				md.HydfsFilename, md.OperationType, opID)
			continue
		}

		// Accumulate data chunks
		if chunk := req.GetChunk(); chunk != nil {
			buffer = append(buffer, chunk.Content...)
		}
	}

	if metadata == nil {
		return nil, nil, 0, fmt.Errorf("no metadata in stream")
	}

	s.logger("Stream complete: %d bytes for %s", len(buffer), metadata.HydfsFilename)
	return metadata, buffer, opID, nil
}

// calculateOperationID determines the operation ID based on operation type
func (s *GRPCServer) calculateOperationID(metadata *fileservice.SendFileMetadata) int64 {
	if metadata.OperationType == fileservice.OperationType_CREATE {
		return 1 // CREATE always starts at 1
	}

	// APPEND: get next sequential ID
	newOpID, err := s.fileServer.IncrementAndGetOperationID(metadata.HydfsFilename)
	if err != nil {
		s.logger("WARNING: Could not get opID for %s: %v", metadata.HydfsFilename, err)
		return 0
	}
	return int64(newOpID)
}

// processIncomingCreate handles a forwarded CREATE request
// Flow: Save → Replicate → Respond
func (s *GRPCServer) processIncomingCreate(
	stream fileservice.FileService_SendFileServer,
	metadata *fileservice.SendFileMetadata,
	buffer []byte,
	opID int64) error {

	s.logger("=== Processing CREATE: %s (opID=%d) ===", metadata.HydfsFilename, opID)

	// Step 2.1: Save file locally to /OwnedFiles
	if err := s.saveCreateToOwned(metadata, buffer, opID); err != nil {
		return s.sendFileError(stream, "Save failed", err)
	}

	// Step 2.2: Replicate to other nodes
	if err := s.replicateCreateToOthers(metadata); err != nil {
		s.logger("WARNING: Replication incomplete: %v", err)
		// Don't fail - file is saved locally
	}

	s.logger("CREATE completed: %s", metadata.HydfsFilename)
	return s.sendFileSuccess(stream)
}

// saveCreateToOwned saves a CREATE operation to /OwnedFiles (synchronous)
func (s *GRPCServer) saveCreateToOwned(
	metadata *fileservice.SendFileMetadata,
	buffer []byte,
	opID int64) error {

	s.logger("Saving CREATE to /OwnedFiles: %s", metadata.HydfsFilename)

	fileReq := &utils.FileRequest{
		OperationType:     utils.Create,
		FileName:          metadata.HydfsFilename,
		Data:              buffer,
		ClientID:          strconv.FormatInt(metadata.ClientId, 10),
		FileOperationID:   int(opID),
		OwnerNodeID:       s.nodeID,
		DestinationNodeID: s.nodeID,
		SourceNodeID:      s.nodeID,
	}

	s.fileServer.SaveFile(fileReq)
	time.Sleep(50 * time.Millisecond) // Ensure write completes

	s.logger("CREATE saved: %s", metadata.HydfsFilename)
	return nil
}

// replicateCreateToOthers sends CREATE to replica nodes
func (s *GRPCServer) replicateCreateToOthers(metadata *fileservice.SendFileMetadata) error {
	if s.coordinatorServer == nil {
		return fmt.Errorf("no coordinator available")
	}

	savedPath := filepath.Join(s.fileServer.GetOwnedFilesDir(), metadata.HydfsFilename)
	return s.coordinatorServer.ReplicateFileToReplicas(metadata.HydfsFilename, savedPath)
}

// processIncomingAppend handles a forwarded APPEND request
// Flow: Save Temp → Queue → Replicate → Cleanup → Respond
func (s *GRPCServer) processIncomingAppend(
	stream fileservice.FileService_SendFileServer,
	metadata *fileservice.SendFileMetadata,
	buffer []byte,
	opID int64) error {

	s.logger("=== Processing APPEND: %s (opID=%d) ===", metadata.HydfsFilename, opID)

	// Step 3.1: Save to temp file (needed for replication)
	tempPath, err := s.saveAppendToTemp(metadata, buffer, opID)
	if err != nil {
		return s.sendFileError(stream, "Temp save failed", err)
	}
	defer os.Remove(tempPath) // Cleanup after replication

	// Step 3.2: Queue locally for ordered processing
	if err := s.queueAppendLocally(metadata, buffer, opID); err != nil {
		return s.sendFileError(stream, "Queue failed", err)
	}

	// Step 3.3: Replicate to other nodes
	if err := s.replicateAppendToOthers(metadata, tempPath, opID); err != nil {
		s.logger("WARNING: Append replication incomplete: %v", err)
		// Don't fail - append is queued locally
	}

	s.logger("APPEND completed: %s (opID=%d)", metadata.HydfsFilename, opID)
	return s.sendFileSuccess(stream)
}

// saveAppendToTemp saves append data to temporary file for replication
func (s *GRPCServer) saveAppendToTemp(
	metadata *fileservice.SendFileMetadata,
	buffer []byte,
	opID int64) (string, error) {

	// Create temp directory
	tempDir := filepath.Join(os.TempDir(), "hydfs_forwarded_appends")
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		return "", fmt.Errorf("temp dir creation failed: %v", err)
	}

	// Generate unique temp file path
	tempFileName := fmt.Sprintf("%s_fwd_op%d.tmp", metadata.HydfsFilename, opID)
	tempPath := filepath.Join(tempDir, tempFileName)

	// Write buffer to temp file
	if err := ioutil.WriteFile(tempPath, buffer, 0644); err != nil {
		return "", fmt.Errorf("temp file write failed: %v", err)
	}

	s.logger("Saved to temp: %s", tempPath)
	return tempPath, nil
}

// queueAppendLocally submits append to local queue for ordered processing
func (s *GRPCServer) queueAppendLocally(
	metadata *fileservice.SendFileMetadata,
	buffer []byte,
	opID int64) error {

	s.logger("Queueing APPEND: %s (opID=%d)", metadata.HydfsFilename, opID)

	fileReq := &utils.FileRequest{
		OperationType:     utils.Append,
		FileName:          metadata.HydfsFilename,
		Data:              buffer,
		ClientID:          strconv.FormatInt(metadata.ClientId, 10),
		FileOperationID:   int(opID),
		DestinationNodeID: s.nodeID,
		SourceNodeID:      s.nodeID,
		OwnerNodeID:       s.nodeID,
	}

	return s.fileServer.SubmitRequest(fileReq)
}

// replicateAppendToOthers sends APPEND to replica nodes
func (s *GRPCServer) replicateAppendToOthers(
	metadata *fileservice.SendFileMetadata,
	tempPath string,
	opID int64) error {

	if s.coordinatorServer == nil {
		return fmt.Errorf("no coordinator available")
	}

	return s.coordinatorServer.ReplicateAppendToReplicas(
		metadata.HydfsFilename,
		tempPath,
		int(opID),
	)
}

// sendFileSuccess sends a success response
func (s *GRPCServer) sendFileSuccess(stream fileservice.FileService_SendFileServer) error {
	return stream.SendAndClose(&fileservice.SendFileResponse{Success: true})
}

// sendFileError sends an error response with logging
func (s *GRPCServer) sendFileError(
	stream fileservice.FileService_SendFileServer,
	message string,
	err error) error {

	fullError := message
	if err != nil {
		fullError = fmt.Sprintf("%s: %v", message, err)
	}

	s.logger("ERROR in SendFile: %s", fullError)
	return stream.SendAndClose(&fileservice.SendFileResponse{
		Success: false,
		Error:   fullError,
	})
}

// SendReplica handles incoming replica data from the owner node
// Flow: Receive → Parse → Route → Process → Respond
func (s *GRPCServer) SendReplica(stream fileservice.FileService_SendReplicaServer) error {
	s.logger("=== SendReplica: Receiving replica stream ===")

	// Step 1: Receive complete replica from stream
	metadata, buffer, err := s.receiveReplicaStream(stream)
	if err != nil {
		return s.sendReplicaError(stream, "Failed to receive replica", err)
	}

	// Step 2: Determine operation type from metadata
	opType, ownerID := s.parseReplicaMetadata(metadata)
	s.logger("Replica operation: %v, file: %s, opID: %d",
		opType, metadata.Filename, metadata.LastOperationId)

	// Step 3: Build file request
	fileReq := s.buildReplicaRequest(metadata, buffer, opType, ownerID)

	// Step 4: Route to appropriate handler
	if s.isCreateOperation(opType) {
		return s.processReplicaCreate(stream, fileReq, metadata)
	}
	return s.processReplicaAppend(stream, fileReq, metadata)
}

// receiveReplicaStream receives and buffers the replica data
func (s *GRPCServer) receiveReplicaStream(stream fileservice.FileService_SendReplicaServer) (
	*fileservice.FileMetadata, []byte, error) {

	var metadata *fileservice.FileMetadata
	var buffer []byte

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, fmt.Errorf("stream error: %v", err)
		}

		// Handle metadata (first message)
		if md := req.GetMetadata(); md != nil {
			metadata = md
			s.logger("Replica metadata: file=%s, type=%s, opID=%d",
				md.Filename, md.Type, md.LastOperationId)
			continue
		}

		// Accumulate data chunks
		if chunk := req.GetChunk(); chunk != nil {
			buffer = append(buffer, chunk.Content...)
		}
	}

	if metadata == nil {
		return nil, nil, fmt.Errorf("no metadata in stream")
	}

	s.logger("Replica stream complete: %d bytes for %s", len(buffer), metadata.Filename)
	return metadata, buffer, nil
}

// parseReplicaMetadata determines operation type from metadata
// Returns: operation type, owner node ID
func (s *GRPCServer) parseReplicaMetadata(metadata *fileservice.FileMetadata) (utils.OperationType, string) {
	// Determine owner vs replica from Type field
	var ownerNodeID string
	if metadata.Type == fileservice.FileMetadata_SELF {
		ownerNodeID = s.nodeID
	} else {
		ownerNodeID = "REMOTE_OWNER"
	}

	// Determine CREATE vs APPEND from LastOperationId
	if metadata.LastOperationId == 1 {
		// First operation = CREATE
		if metadata.Type == fileservice.FileMetadata_SELF {
			return utils.Create, ownerNodeID
		}
		return utils.CreateReplica, ownerNodeID
	}

	// Subsequent operations = APPEND
	if metadata.Type == fileservice.FileMetadata_SELF {
		return utils.Append, ownerNodeID
	}
	return utils.AppendReplica, ownerNodeID
}

// buildReplicaRequest constructs a FileRequest from replica metadata
func (s *GRPCServer) buildReplicaRequest(
	metadata *fileservice.FileMetadata,
	buffer []byte,
	opType utils.OperationType,
	ownerNodeID string) *utils.FileRequest {

	return &utils.FileRequest{
		OperationType:     opType,
		FileName:          metadata.Filename,
		Data:              buffer,
		ClientID:          strconv.FormatInt(metadata.LastOperationId, 10),
		FileOperationID:   int(metadata.LastOperationId),
		DestinationNodeID: s.nodeID,
		SourceNodeID:      s.nodeID,
		OwnerNodeID:       ownerNodeID,
	}
}

// isCreateOperation checks if the operation is a CREATE variant
func (s *GRPCServer) isCreateOperation(opType utils.OperationType) bool {
	return opType == utils.Create || opType == utils.CreateReplica
}

// processReplicaCreate processes a CREATE replica (synchronous)
func (s *GRPCServer) processReplicaCreate(
	stream fileservice.FileService_SendReplicaServer,
	fileReq *utils.FileRequest,
	metadata *fileservice.FileMetadata) error {

	s.logger("Processing CREATE replica: %s", metadata.Filename)

	s.fileServer.SaveFile(fileReq)
	time.Sleep(50 * time.Millisecond) // Ensure write completes

	s.logger("CREATE replica saved: %s", metadata.Filename)
	return s.sendReplicaSuccess(stream)
}

// processReplicaAppend processes an APPEND replica (asynchronous)
func (s *GRPCServer) processReplicaAppend(
	stream fileservice.FileService_SendReplicaServer,
	fileReq *utils.FileRequest,
	metadata *fileservice.FileMetadata) error {

	s.logger("Queueing APPEND replica: %s (opID=%d)",
		metadata.Filename, metadata.LastOperationId)

	if err := s.fileServer.SubmitRequest(fileReq); err != nil {
		return s.sendReplicaError(stream, "Queue failed", err)
	}

	s.logger("APPEND replica queued: %s", metadata.Filename)
	return s.sendReplicaSuccess(stream)
}

// sendReplicaSuccess sends a success response
func (s *GRPCServer) sendReplicaSuccess(stream fileservice.FileService_SendReplicaServer) error {
	return stream.SendAndClose(&fileservice.SendReplicaResponse{Success: true})
}

// sendReplicaError sends an error response with logging
func (s *GRPCServer) sendReplicaError(
	stream fileservice.FileService_SendReplicaServer,
	message string,
	err error) error {

	fullError := message
	if err != nil {
		fullError = fmt.Sprintf("%s: %v", message, err)
	}

	s.logger("ERROR in SendReplica: %s", fullError)
	return stream.SendAndClose(&fileservice.SendReplicaResponse{
		Success: false,
		Error:   fullError,
	})
}
