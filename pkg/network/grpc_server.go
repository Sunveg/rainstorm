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
	// Register both file transfer services
	filetransfer.RegisterFileTransferServiceServer(s.fileTransferServer, s)
	fileservice.RegisterFileServiceServer(s.fileTransferServer, s)

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

func (s *GRPCServer) GetFile(ctx context.Context, req *filetransfer.GetFileRequest) (*filetransfer.GetFileResponse, error) {
	s.logger("gRPC GetFile request: filename=%s, client_id=%d", req.Filename, req.ClientId)

	// Create file request for the file server
	fileReq := &utils.FileRequest{
		OperationType:     utils.Get,
		FileName:          req.Filename,
		ClientID:          strconv.FormatInt(req.ClientId, 10),
		SourceNodeID:      req.SourceNodeId,
		DestinationNodeID: req.DestinationNodeId,
		OwnerNodeID:       req.OwnerNodeId,
	}

	// If DestinationNodeID is not set, use this node's ID
	if fileReq.DestinationNodeID == "" {
		fileReq.DestinationNodeID = s.nodeID
	}

	// Submit to file server for processing
	err := s.fileServer.SubmitRequest(fileReq)
	if err != nil {
		return &filetransfer.GetFileResponse{
			Success: false,
			Error:   fmt.Sprintf("Failed to submit request: %v", err),
			Data:    nil,
			Version: 0,
		}, nil
	}

	// TODO: Add proper response handling with channels or callbacks
	return &filetransfer.GetFileResponse{
		Success: true,
		Data:    []byte{}, // Placeholder - should get actual data
		Version: 1,        // Placeholder
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

func (s *GRPCServer) ListFiles(ctx context.Context, req *coordination.ListFilesRequest) (*coordination.ListFilesResponse, error) {
	s.logger("gRPC ListFiles request: client_id=%d, requesting_node=%s", req.ClientId, req.RequestingNodeId)

	// TODO: Get actual file list from file server
	filenames := []string{} // Placeholder

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
	var metadata *fileservice.FileMetadata
	var buffer []byte
	var operationId int64
	var operationType fileservice.OperationType

	// Receive chunks
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error receiving chunk: %v", err)
		}

		// Handle first message which contains metadata
		// TODO get the metadata first. Based on Metadata take decision
		// IF op type is CREATE then create new metadata and add to FileSystem
		// Save the file to /ownedFiles and then create Request to create Replicas and add them to submitRequest
		// IF op type is APPEND then get existing metadata from FileSystem and update operation ID
		// Add this operation to pending operations in metadata of that file
		// Create Request to append file and add to submitRequest
		if md := req.GetMetadata(); md != nil {
			metadata = md
			operationType = req.OperationType

			// Initialize metadata based on operation type
			if req.OperationType == fileservice.OperationType_CREATE {
				// For create operations, initialize with operation ID 1
				operationId = 1
			} else if req.OperationType == fileservice.OperationType_APPEND {
				// For append operations, get existing metadata and increment operation ID
				existingMetadata := s.fileServer.GetFileMetadata(md.Filename)
				if existingMetadata == nil {
					return fmt.Errorf("file %s not found for append operation", md.Filename)
				}
				operationId = int64(existingMetadata.LastOperationId + 1)
				existingMetadata.LastOperationId = int(operationId)
			} else {
				return fmt.Errorf("unsupported operation type: %v", req.OperationType)
			}
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

	// TODO save the file to /ownedFiles if Create operation
	// TODO save the file to /tempFiles if Append operation
	// For create operations, create metadata and add to FileSystem
	if operationType == fileservice.OperationType_CREATE {
		fileMd := &utils.FileMetaData{
			FileName:          metadata.Filename,
			LastModified:      time.Now(),
			Type:              utils.Self,
			LastOperationId:   int(operationId),
			Operations:        make([]utils.Operation, 0),
			PendingOperations: []utils.Operation{}, // Initialize as empty slice
			Size:              int64(len(buffer)),
		}

		// Add to FileSystem for tracking
		if fs := s.fileServer.GetFileSystem(); fs != nil {
			fs.AddFile(metadata.Filename, fileMd)
		}
	}

	// Create file request for the file server
	fileReq := &utils.FileRequest{
		OperationType:     utils.OperationType(operationType),
		FileName:          metadata.Filename,
		Data:              buffer,
		ClientID:          strconv.FormatInt(metadata.LastOperationId, 10),
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
		Success:     true,
		OperationId: operationId,
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
	// BAsed on optype save the file to respective folder and
	// if create save it to ownedFiles and if append save it to tempFiles
	// Add the append operation to pending operations in metadata of that file

	return stream.SendAndClose(&fileservice.SendReplicaResponse{
		Success: true,
	})
}
