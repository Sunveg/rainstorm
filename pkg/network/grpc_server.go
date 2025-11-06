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

		if md := req.GetSendFileMetadata(); md != nil {
			sendFileMetadata = md
			operationType = md.OperationType

			if md.OperationType == fileservice.OperationType_CREATE {
				operationId = 1
			} else if md.OperationType == fileservice.OperationType_APPEND {
				newOpID, err := s.fileServer.IncrementAndGetOperationID(md.HydfsFilename)
				if err != nil {
					return fmt.Errorf("file %s not found for append: %v", md.HydfsFilename, err)
				}
				operationId = int64(newOpID)
			}
			continue
		}

		if chunk := req.GetChunk(); chunk != nil {
			buffer = append(buffer, chunk.Content...)
		}
	}

	if sendFileMetadata == nil {
		return fmt.Errorf("no metadata received")
	}

	// Use FileRequest and SaveFile for CREATE
	if operationType == fileservice.OperationType_CREATE {
		fileReq := &utils.FileRequest{
			OperationType:     utils.Create,
			FileName:          sendFileMetadata.HydfsFilename,
			Data:              buffer,
			ClientID:          strconv.FormatInt(sendFileMetadata.ClientId, 10),
			FileOperationID:   int(operationId),
			OwnerNodeID:       s.nodeID,
			DestinationNodeID: s.nodeID,
			SourceNodeID:      s.nodeID,
		}

		// Reuse existing SaveFile method!
		s.fileServer.SaveFile(fileReq)

		// Wait briefly for file to be written
		time.Sleep(50 * time.Millisecond)

		// Trigger replication
		if s.coordinatorServer != nil {
			savedFilePath := filepath.Join(s.fileServer.GetOwnedFilesDir(), sendFileMetadata.HydfsFilename)
			err := s.coordinatorServer.ReplicateFileToReplicas(sendFileMetadata.HydfsFilename, savedFilePath)
			if err != nil {
				s.logger("WARNING: Replication incomplete: %v", err)
			}
		}
	} else if operationType == fileservice.OperationType_APPEND {
		// Handle forwarded APPEND with replication

		// Step 1: Save buffer to temp file (so we can replicate it)
		tempDir := filepath.Join(os.TempDir(), "hydfs_forwarded_appends")
		os.MkdirAll(tempDir, 0755)

		tempFileName := fmt.Sprintf("%s_fwd_op%d.tmp", sendFileMetadata.HydfsFilename, operationId)
		tempFilePath := filepath.Join(tempDir, tempFileName)

		if err := ioutil.WriteFile(tempFilePath, buffer, 0644); err != nil {
			return stream.SendAndClose(&fileservice.SendFileResponse{
				Success: false,
				Error:   fmt.Sprintf("Failed to save temp file: %v", err),
			})
		}

		s.logger("Saved forwarded append to temp file: %s", tempFilePath)

		// Step 2: Queue locally for processing
		fileReq := &utils.FileRequest{
			OperationType:     utils.Append,
			FileName:          sendFileMetadata.HydfsFilename,
			Data:              buffer,
			ClientID:          strconv.FormatInt(sendFileMetadata.ClientId, 10),
			FileOperationID:   int(operationId),
			DestinationNodeID: s.nodeID,
			SourceNodeID:      s.nodeID,
			OwnerNodeID:       s.nodeID,
		}

		if err := s.fileServer.SubmitRequest(fileReq); err != nil {
			return stream.SendAndClose(&fileservice.SendFileResponse{
				Success: false,
				Error:   fmt.Sprintf("Failed to queue append: %v", err),
			})
		}

		// Step 3 :  Replicate
		if s.coordinatorServer != nil {
			err := s.coordinatorServer.ReplicateAppendToReplicas(
				sendFileMetadata.HydfsFilename,
				tempFilePath,
				int(operationId),
			)
			if err != nil {
				s.logger("WARNING: Forwarded append replication incomplete: %v", err)
			}

			// Clean up temp file
			os.Remove(tempFilePath)
		}
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
		ownerNodeID = s.nodeID
	} else {
		ownerNodeID = "REMOTE_OWNER" // Mark as replica, not owner
	}

	// Determine CREATE vs APPEND from operation ID
	if metadata.LastOperationId == 1 {
		// First operation = CREATE
		if metadata.Type == fileservice.FileMetadata_SELF {
			opType = utils.Create
		} else {
			opType = utils.CreateReplica
		}
	} else {
		// Subsequent operation = APPEND
		if metadata.Type == fileservice.FileMetadata_SELF {
			opType = utils.Append
		} else {
			opType = utils.AppendReplica
		}
	}

	s.logger("Replica received %v operation for file %s (opID: %d)", opType, metadata.Filename, metadata.LastOperationId)

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

	var err error
	if opType == utils.Create || opType == utils.CreateReplica {
		// CREATE: Use synchronous SaveFile
		s.fileServer.SaveFile(fileReq)
		s.logger("CREATE replica saved synchronously")
	} else {
		// APPEND: Use asynchronous SubmitRequest for ordering
		err = s.fileServer.SubmitRequest(fileReq)
		if err != nil {
			return stream.SendAndClose(&fileservice.SendReplicaResponse{
				Success: false,
				Error:   fmt.Sprintf("Failed to queue append: %v", err),
			})
		}
		s.logger("APPEND replica queued for processing")
	}

	return stream.SendAndClose(&fileservice.SendReplicaResponse{
		Success: true,
	})
}
