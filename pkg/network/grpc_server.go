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
	"hydfs/protoBuilds/filetransfer"
	pb "hydfs/protoBuilds/hydfs/pkg/pb"

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
	pb.RegisterFileServiceServer(s.fileTransferServer, s)

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

// FileTransferService implementation
// AppendFileRequest
// AppendFile
func (s *GRPCServer) CreateFile(ctx context.Context, req *filetransfer.CreateFileRequest) (*filetransfer.CreateFileResponse, error) {
	s.logger("gRPC CreateFile request: filename=%s, client_id=%d", req.Filename, req.ClientId)

	// Create file request for the file server
	fileReq := &utils.FileRequest{
		OperationType:     utils.Create,
		FileName:          req.Filename,
		Data:              req.Data,
		ClientID:          strconv.FormatInt(req.ClientId, 10),
		FileOperationID:   int(req.OperationId),
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
		return &filetransfer.CreateFileResponse{
			Success: false,
			Error:   fmt.Sprintf("Failed to submit request: %v", err),
			Version: 0,
		}, nil
	}

	return &filetransfer.CreateFileResponse{
		Success: true,
		Version: 1, // Placeholder
	}, nil
}

func (s *GRPCServer) AppendFile(ctx context.Context, req *filetransfer.AppendFileRequest) (*filetransfer.AppendFileResponse, error) {
	s.logger("gRPC AppendFile request: filename=%s, client_id=%d", req.Filename, req.ClientId)

	// Create file request for the file server
	fileReq := &utils.FileRequest{
		OperationType:     utils.Append,
		FileName:          req.Filename,
		Data:              req.Data,
		ClientID:          strconv.FormatInt(req.ClientId, 10),
		FileOperationID:   int(req.OperationId),
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
		return &filetransfer.AppendFileResponse{
			Success: false,
			Error:   fmt.Sprintf("Failed to submit request: %v", err),
			Version: 0,
		}, nil
	}

	return &filetransfer.AppendFileResponse{
		Success: true,
		Version: 1, // Placeholder
	}, nil
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

// InitSendFile is called by clients to initiate a file send operation (create/append)
func (s *GRPCServer) InitSendFile(ctx context.Context, req *pb.InitSendFileRequest) (*pb.InitSendFileResponse, error) {
	s.logger("InitSendFile request: filename=%s, client_id=%d, type=%v", req.Filename, req.ClientId, req.OperationType)

	var metadata *pb.FileMetadata
	var operationId int64

	if req.OperationType == pb.OperationType_CREATE {
		// For create operations, initialize with operation ID 1
		operationId = 1
		metadata = &pb.FileMetadata{
			Filename:        req.Filename,
			LastModified:    time.Now().UnixNano(),
			Type:            pb.FileMetadata_SELF, // This is the owner node
			LastOperationId: operationId,
		}

		// Create and register the internal metadata
		err := s.fileServer.SubmitRequest(&utils.FileRequest{
			OperationType:     utils.Create,
			FileName:          req.Filename,
			ClientID:          strconv.FormatInt(req.ClientId, 10),
			FileOperationID:   int(operationId),
			SourceNodeID:      s.nodeID,
			DestinationNodeID: s.nodeID,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to initialize file metadata: %v", err)
		}
	} else if req.OperationType == pb.OperationType_APPEND {
		// For append operations, get existing metadata
		existingMetadata := s.fileServer.GetFileMetadata(req.Filename)
		if existingMetadata == nil {
			return nil, fmt.Errorf("file %s not found for append operation", req.Filename)
		}

		// Increment operation ID
		operationId = int64(existingMetadata.LastOperationId + 1)

		metadata = &pb.FileMetadata{
			Filename:        req.Filename,
			LastModified:    time.Now().UnixNano(),
			Type:            pb.FileMetadata_SELF,
			LastOperationId: operationId,
		}

		// Update operation ID in internal metadata
		existingMetadata.LastOperationId = int(operationId)
	} else {
		return nil, fmt.Errorf("unsupported operation type: %v", req.OperationType)
	}

	return &pb.InitSendFileResponse{
		Success:     true,
		OperationId: operationId,
		Metadata:    metadata,
	}, nil
}

// SendFile handles the streaming of file data from clients
func (s *GRPCServer) SendFile(stream pb.FileService_SendFileServer) error {
	var fileDetails *pb.FileDetails
	var buffer []byte
	var operationId int64

	// Receive chunks
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error receiving chunk: %v", err)
		}

		// Handle first message which contains file details
		if details := req.GetFileDetails(); details != nil {
			fileDetails = details

			// Initialize metadata based on operation type
			if details.OperationType == pb.OperationType_CREATE {
				// For create operations, initialize with operation ID 1
				operationId = 1
			} else if details.OperationType == pb.OperationType_APPEND {
				// For append operations, get existing metadata and increment operation ID
				existingMetadata := s.fileServer.GetFileMetadata(details.Filename)
				if existingMetadata == nil {
					return fmt.Errorf("file %s not found for append operation", details.Filename)
				}
				operationId = int64(existingMetadata.LastOperationId + 1)
				existingMetadata.LastOperationId = int(operationId)
			} else {
				return fmt.Errorf("unsupported operation type: %v", details.OperationType)
			}
			continue
		}

		// Handle data chunk
		if chunk := req.GetChunk(); chunk != nil {
			buffer = append(buffer, chunk.Content...)
		}
	}

	if fileDetails == nil {
		return fmt.Errorf("no file details received")
	}

	// For create operations, we let FileServer handle the metadata creation
	if fileDetails.OperationType == pb.OperationType_CREATE {
		// The FileServer's handleCreateFile method will handle metadata creation 
		// through createFileMetadata after writing the file to disk
		s.logger("Creating file %s with operation ID %d", fileDetails.Filename, operationId)
	}

	// Create file request for the file server
	fileReq := &utils.FileRequest{
		OperationType:     utils.OperationType(fileDetails.OperationType),
		FileName:          fileDetails.Filename,
		Data:              buffer,
		ClientID:          strconv.FormatInt(fileDetails.ClientId, 10),
		FileOperationID:   int(operationId),
		DestinationNodeID: s.nodeID,
		SourceNodeID:      s.nodeID,
	}

	// Submit to file server for processing
	err := s.fileServer.SubmitRequest(fileReq)
	if err != nil {
		return stream.SendAndClose(&pb.SendFileResponse{
			Success: false,
			Error:   fmt.Sprintf("Failed to process file: %v", err),
		})
	}

	return stream.SendAndClose(&pb.SendFileResponse{
		Success:     true,
		OperationId: operationId,
	})
}

// InitReplica initializes a replica creation operation
func (s *GRPCServer) InitReplica(ctx context.Context, req *pb.InitReplicaRequest) (*pb.InitReplicaResponse, error) {
	s.logger("InitReplica request: filename=%s, type=%v", req.Filename, req.OperationType)

	// Validate and possibly update the metadata for the replica
	metadata := req.Metadata
	if metadata == nil {
		return &pb.InitReplicaResponse{
			Success: false,
			Error:   "no metadata provided",
		}, nil
	}

	return &pb.InitReplicaResponse{
		Success:  true,
		Metadata: metadata,
	}, nil
}

// SendReplica handles streaming of replica data
func (s *GRPCServer) SendReplica(stream pb.FileService_SendReplicaServer) error {
	var metadata *pb.FileMetadata
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
		if req.Metadata != nil {
			metadata = req.Metadata
			continue
		}

		// Handle data chunk
		if req.Chunk != nil {
			buffer = append(buffer, req.Chunk.Content...)
		}
	}

	if metadata == nil {
		return fmt.Errorf("no metadata received")
	}

	// Create file request for the file server
	fileReq := &utils.FileRequest{
		OperationType:     utils.Create, // Using Create operation type for replica, the fileserver will handle it as a replica based on metadata
		FileName:          metadata.Filename,
		Data:              buffer,
		ClientID:          "replica",
		FileOperationID:   int(metadata.LastOperationId),
		DestinationNodeID: s.nodeID,
	}

	// Submit to file server for processing
	err := s.fileServer.SubmitRequest(fileReq)
	if err != nil {
		return stream.SendAndClose(&pb.SendReplicaResponse{
			Success: false,
			Error:   fmt.Sprintf("Failed to process replica: %v", err),
		})
	}

	return stream.SendAndClose(&pb.SendReplicaResponse{
		Success: true,
	})
}
