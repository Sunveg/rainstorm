package network

import (
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"

	"hydfs/pkg/fileserver"
	"hydfs/pkg/utils"
	"hydfs/protoBuilds/coordination"
	"hydfs/protoBuilds/fileservice"
	"hydfs/protoBuilds/filetransfer"

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
	fileServer *fileserver.FileServer
	nodeID     string
	logger     func(string, ...interface{})

	// gRPC servers
	fileTransferServer *grpc.Server
	coordinationServer *grpc.Server

	// Network addresses
	fileTransferAddr string
	coordinationAddr string

	// Server state
	startTime time.Time

	// Embedded unimplemented service servers for forward compatibility
	filetransfer.UnimplementedFileTransferServiceServer
	coordination.UnimplementedCoordinationServiceServer
}

// NewGRPCServer creates a new gRPC server for handling both file transfer and coordination
func NewGRPCServer(fileTransferAddr, coordinationAddr string, fileServer *fileserver.FileServer, nodeID string, logger func(string, ...interface{})) *GRPCServer {
	return &GRPCServer{
		fileServer:       fileServer,
		nodeID:           nodeID,
		logger:           logger,
		fileTransferAddr: fileTransferAddr,
		coordinationAddr: coordinationAddr,
		startTime:        time.Now(),
	}
}

// Start starts both gRPC servers
func (s *GRPCServer) Start() error {
	// Start file transfer server
	go func() {
		if err := s.startFileTransferServer(); err != nil {
			s.logger("File transfer server error: %v", err)
		}
	}()

	// Start coordination server
	go func() {
		if err := s.startCoordinationServer(); err != nil {
			s.logger("Coordination server error: %v", err)
		}
	}()

	s.logger("gRPC servers started - FileTransfer: %s, Coordination: %s", s.fileTransferAddr, s.coordinationAddr)
	return nil
}

// Stop stops both gRPC servers
func (s *GRPCServer) Stop() error {
	if s.fileTransferServer != nil {
		s.fileTransferServer.GracefulStop()
	}
	if s.coordinationServer != nil {
		s.coordinationServer.GracefulStop()
	}
	return nil
}

// startFileTransferServer starts the file transfer gRPC server
func (s *GRPCServer) startFileTransferServer() error {
	lis, err := net.Listen("tcp", s.fileTransferAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %v", s.fileTransferAddr, err)
	}

	s.fileTransferServer = grpc.NewServer()
	// Register file service (primary service for file operations)
	fileservice.RegisterFileServiceServer(s.fileTransferServer, &fileServiceImpl{
		GRPCServer: s,
	})
	// Note: fileservice has its own HealthCheck, ListFiles, MergeFile
	// which conflict with coordination service. We use adapter pattern.

	s.logger("FileTransfer gRPC server listening on %s", s.fileTransferAddr)
	return s.fileTransferServer.Serve(lis)
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

// GetFileTransfer from filetransfer proto - returns file data directly
func (s *GRPCServer) GetFileTransfer(ctx context.Context, req *filetransfer.GetFileRequest) (*filetransfer.GetFileResponse, error) {
	s.logger("gRPC GetFileTransfer request: filename=%s, client_id=%d", req.Filename, req.ClientId)

	// Read file directly from file server (synchronous)
	data, err := s.fileServer.ReadFile(req.Filename)
	if err != nil {
		return &filetransfer.GetFileResponse{
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

	return &filetransfer.GetFileResponse{
		Success: true,
		Data:    data,
		Version: version,
	}, nil
}

func (s *GRPCServer) ReplicateOperation(ctx context.Context, req *filetransfer.ReplicationRequest) (*filetransfer.ReplicationResponse, error) {
	s.logger("gRPC ReplicateOperation request: filename=%s, sender=%s", req.Filename, req.SenderId)

	if req.Operation == nil {
		return &filetransfer.ReplicationResponse{
			Success: false,
			Error:   "Operation is required",
		}, nil
	}

	// Convert to internal operation type
	var opType utils.OperationType
	switch req.Operation.Type {
	case filetransfer.Operation_CREATE:
		opType = utils.Create
	case filetransfer.Operation_APPEND:
		opType = utils.Append
	case filetransfer.Operation_DELETE:
		opType = utils.Delete
	default:
		return &filetransfer.ReplicationResponse{
			Success: false,
			Error:   "Unknown operation type",
		}, nil
	}

	// Create file request for replication
	fileReq := &utils.FileRequest{
		OperationType:   opType,
		FileName:        req.Operation.Filename,
		Data:            req.Operation.Data,
		ClientID:        strconv.FormatInt(req.Operation.ClientId, 10),
		FileOperationID: int(req.Operation.OperationId),
		SourceNodeID:    req.SenderId,
	}

	// Submit to file server for processing
	err := s.fileServer.SubmitRequest(fileReq)
	if err != nil {
		return &filetransfer.ReplicationResponse{
			Success: false,
			Error:   fmt.Sprintf("Failed to replicate operation: %v", err),
		}, nil
	}

	return &filetransfer.ReplicationResponse{
		Success: true,
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
		ServicesAvailable: []string{"FileTransferService", "CoordinationService"},
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

// SendFile handles the streaming of file data from clients
func (s *GRPCServer) SendFile(stream fileservice.FileService_SendFileServer) error {
	var sendFileMetadata *fileservice.SendFileMetadata
	var buffer []byte
	var operationId int64
	var operationType fileservice.OperationType

	// Receive chunks
	// TODO get the metadata first. Based on Metadata take decision
	// IF op type is CREATE then create new metadata and add to FileSystem
	// Save the file to /ownedFiles and then create Request to create Replicas and add them to submitRequest
	// IF op type is APPEND then get existing metadata from FileSystem and update operation ID
	// Add this operation to pending operations in metadata of that file
	// Create Request to append file and add to submitRequest
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error receiving chunk: %v", err)
		}

		// Handle first message which contains metadata
		if md := req.GetSendFileMetadata(); md != nil {
			sendFileMetadata = md
			operationType = md.OperationType

			// Initialize metadata based on operation type
			if md.OperationType == fileservice.OperationType_CREATE {
				// For create operations, initialize with operation ID 1
				operationId = 1
			} else if md.OperationType == fileservice.OperationType_APPEND {
				// For append operations, atomically increment operation ID with proper locking
				newOpID, err := s.fileServer.IncrementAndGetOperationID(md.HydfsFilename)
				if err != nil {
					return fmt.Errorf("file %s not found for append operation: %v", md.HydfsFilename, err)
				}
				operationId = int64(newOpID)
				s.logger("Assigned operation ID %d for append to file %s", operationId, md.HydfsFilename)
			} else {
				return fmt.Errorf("unsupported operation type: %v", md.OperationType)
			}
			continue
		}

		// Handle data chunk
		if chunk := req.GetChunk(); chunk != nil {
			buffer = append(buffer, chunk.Content...)
		}
	}

	if sendFileMetadata == nil {
		return fmt.Errorf("no metadata received")
	}

	// For create operations, create metadata and add to FileSystem
	if operationType == fileservice.OperationType_CREATE {
		fileMd := &utils.FileMetaData{
			FileName:          sendFileMetadata.HydfsFilename,
			LastModified:      time.Now(),
			Type:              utils.Self,
			LastOperationId:   int(operationId),
			Operations:        make([]utils.Operation, 0),
			PendingOperations: &utils.TreeSet{}, // Initialize as empty TreeSet
			Size:              int64(len(buffer)),
		}

		// Add to FileSystem for tracking
		if fs := s.fileServer.GetFileSystem(); fs != nil {
			fs.AddFile(sendFileMetadata.HydfsFilename, fileMd)
		}
		// TODO: Check if the file is stored correctly to owned files.
		// use the code from SaveFile function in fileserver/server.go

	}

	if operationType == fileservice.OperationType_APPEND {
		// TODO: Add this append operation to pending operations in metadata of that file
		//existingMetadata := s.fileServer.GetFileMetadata(md.HydfsFilename)
		//existingMetadata.PendingOperations.Add(op)
		// For append operations, the file request will be processed by FileServer
		// which will save to TempFiles and add to PendingOperations
		// The converger will apply the append in order
	}

	// Create file request for the file server to create Replicas
	fileReq := &utils.FileRequest{
		OperationType:     utils.OperationType(operationType),
		FileName:          sendFileMetadata.HydfsFilename,
		Data:              buffer,
		ClientID:          strconv.FormatInt(sendFileMetadata.ClientId, 10),
		FileOperationID:   int(operationId),
		DestinationNodeID: s.nodeID,
		SourceNodeID:      s.nodeID,
		OwnerNodeID:       s.nodeID, // This node is the owner for files created here
	}

	// Submit to file server for processing
	err := s.fileServer.SubmitRequest(fileReq)
	if err != nil {
		return stream.SendAndClose(&fileservice.SendFileResponse{
			Success: false,
			Error:   fmt.Sprintf("Failed to process file: %v", err),
		})
	}

	return stream.SendAndClose(&fileservice.SendFileResponse{
		Success: true,
	})
}

// SendReplica handles streaming of replica data
func (s *GRPCServer) SendReplica(stream fileservice.FileService_SendReplicaServer) error {
	var metadata *fileservice.FileMetadata
	var buffer []byte

	// Receive chunks
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error receiving replica chunk: %v", err)
		}

		// Handle metadata in first message
		if md := req.GetMetadata(); md != nil {
			metadata = md
			continue
		}

		// Handle data chunk
		if chunk := req.GetChunk(); chunk != nil {
			buffer = append(buffer, chunk.Content...)
		}
	}

	if metadata == nil {
		return fmt.Errorf("no metadata received")
	}

	// Determine operation type based on metadata
	var opType utils.OperationType
	var ownerNodeID string

	if metadata.Type == fileservice.FileMetadata_SELF {
		// This is the owner node - save to OwnedFiles
		opType = utils.Create
		ownerNodeID = s.nodeID
	} else {
		// This is a replica node (REPLICA1 or REPLICA2) - save to ReplicatedFiles
		opType = utils.Create
		ownerNodeID = "REMOTE_OWNER" // Mark as replica, not owner
	}

	// Create file request for the file server
	fileReq := &utils.FileRequest{
		OperationType:     opType,
		FileName:          metadata.Filename,
		Data:              buffer,
		ClientID:          strconv.FormatInt(metadata.LastOperationId, 10),
		FileOperationID:   int(metadata.LastOperationId),
		DestinationNodeID: s.nodeID,
		SourceNodeID:      s.nodeID,
		OwnerNodeID:       ownerNodeID,
	}

	// Submit to file server for processing
	err := s.fileServer.SubmitRequest(fileReq)
	if err != nil {
		return stream.SendAndClose(&fileservice.SendReplicaResponse{
			Success: false,
			Error:   fmt.Sprintf("Failed to process replica: %v", err),
		})
	}

	return stream.SendAndClose(&fileservice.SendReplicaResponse{
		Success: true,
	})
}
