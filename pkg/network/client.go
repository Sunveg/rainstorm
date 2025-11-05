package network

import (
	"context"
	"fmt"
	"io"
	"time"

	"hydfs/pkg/utils"
	"hydfs/protoBuilds/coordination"
	pb "hydfs/protoBuilds/hydfs/pkg/pb"

	"strconv"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Helper function to convert string to int64
func stringToInt64(s string) int64 {
	i, _ := strconv.ParseInt(s, 10, 64)
	return i
}

// Type aliases for consistency with existing code
type FileRequest = utils.FileRequest

// FileResponse represents a response to file operations
type FileResponse struct {
	Success   bool   `json:"success"`
	Message   string `json:"message"`
	ErrorCode int    `json:"error_code,omitempty"`
	Data      []byte `json:"data,omitempty"`
}

// ReplicationRequest represents a replication operation request
type ReplicationRequest struct {
	Filename  string          `json:"filename"`
	Operation utils.Operation `json:"operation"`
	SenderID  string          `json:"sender_id"`
}

// ReplicationResponse represents a replication operation response
type ReplicationResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}

// MergeRequest represents a file merge request
type MergeRequest struct {
	Filename string `json:"filename"`
	ClientID string `json:"client_id"`
}

// MergeResponse represents a file merge response
type MergeResponse struct {
	Success      bool   `json:"success"`
	Error        string `json:"error,omitempty"`
	FinalVersion int64  `json:"final_version"`
}

// Client handles gRPC communication with other HyDFS nodes
type Client struct {
	timeout time.Duration

	// Connection pools for different services
	fileTransferConns map[string]*grpc.ClientConn
	coordinationConns map[string]*grpc.ClientConn
}

// NewClient creates a new gRPC client
func NewClient(timeout time.Duration) *Client {
	return &Client{
		timeout:           timeout,
		fileTransferConns: make(map[string]*grpc.ClientConn),
		coordinationConns: make(map[string]*grpc.ClientConn),
	}
}

// getFileTransferConnection gets or creates a connection to a node's file transfer service
func (c *Client) getFileTransferConnection(nodeAddr string) (*grpc.ClientConn, error) {
	if conn, exists := c.fileTransferConns[nodeAddr]; exists {
		return conn, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	conn, err := grpc.DialContext(ctx, nodeAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock())
	if err != nil {
		return nil, fmt.Errorf("failed to connect to file transfer service at %s: %v", nodeAddr, err)
	}

	c.fileTransferConns[nodeAddr] = conn
	return conn, nil
}

// getCoordinationConnection gets or creates a connection to a node's coordination service
func (c *Client) getCoordinationConnection(nodeAddr string) (*grpc.ClientConn, error) {
	if conn, exists := c.coordinationConns[nodeAddr]; exists {
		return conn, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	conn, err := grpc.DialContext(ctx, nodeAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock())
	if err != nil {
		return nil, fmt.Errorf("failed to connect to coordination service at %s: %v", nodeAddr, err)
	}

	c.coordinationConns[nodeAddr] = conn
	return conn, nil
}

// Close closes all connections
func (c *Client) Close() error {
	for addr, conn := range c.fileTransferConns {
		if err := conn.Close(); err != nil {
			fmt.Printf("Error closing file transfer connection to %s: %v\n", addr, err)
		}
	}
	for addr, conn := range c.coordinationConns {
		if err := conn.Close(); err != nil {
			fmt.Printf("Error closing coordination connection to %s: %v\n", addr, err)
		}
	}
	return nil
}

func (c *Client) SendFileStream(ctx context.Context, nodeAddr string, req FileRequest, operationType utils.OperationType) (*FileResponse, error) {
	conn, err := c.getFileTransferConnection(nodeAddr)
	if err != nil {
		return nil, err
	}

	client := pb.NewFileServiceClient(conn)

	// Start streaming with file details
	stream, err := client.SendFile(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create send stream: %v", err)
	}

	// Send file details in the first message
	firstMsg := &pb.SendFileRequest{
		Data: &pb.SendFileRequest_FileDetails{
			FileDetails: &pb.FileDetails{
				Filename:      req.FileName,
				ClientId:      stringToInt64(req.ClientID),
				OperationType: pb.OperationType(operationType),
			},
		},
	}

	if err := stream.Send(firstMsg); err != nil {
		return nil, fmt.Errorf("failed to send file details: %v", err)
	}

	// Send file data in chunks (1MB chunks)
	const chunkSize = 1024 * 1024 // 1MB
	data := req.Data

	for i := 0; i < len(data); i += chunkSize {
		end := i + chunkSize
		if end > len(data) {
			end = len(data)
		}

		chunkMsg := &pb.SendFileRequest{
			Data: &pb.SendFileRequest_Chunk{
				Chunk: &pb.FileChunk{
					Content: data[i:end],
					Offset:  int64(i),
				},
			},
		}

		if err := stream.Send(chunkMsg); err != nil {
			return nil, fmt.Errorf("failed to send chunk: %v", err)
		}
	}

	// Close the stream and get response
	resp, err := stream.CloseAndRecv()
	if err != nil {
		return nil, fmt.Errorf("error receiving response: %v", err)
	}

	return &FileResponse{
		Success: resp.Success,
		Message: resp.Error,
	}, nil
}

func (c *Client) GetFile(ctx context.Context, nodeAddr string, req FileRequest) (*FileResponse, error) {
	conn, err := c.getFileTransferConnection(nodeAddr)
	if err != nil {
		return nil, err
	}

	client := pb.NewFileServiceClient(conn)

	grpcReq := &pb.GetFileRequest{
		Filename: req.FileName,
		ClientId: stringToInt64(req.ClientID),
	}

	stream, err := client.GetFile(ctx, grpcReq)
	if err != nil {
		return nil, fmt.Errorf("failed to get file stream: %v", err)
	}

	// Initialize a buffer to store all chunks
	var fileData []byte

	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			// End of stream
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error receiving file chunk: %v", err)
		}

		fileData = append(fileData, chunk.Content...)
	}

	return &FileResponse{
		Success: true,
		Message: "",
		Data:    fileData,
	}, nil
}

func (c *Client) SendReplica(ctx context.Context, nodeAddr string, req ReplicationRequest) (*ReplicationResponse, error) {
	conn, err := c.getFileTransferConnection(nodeAddr)
	if err != nil {
		return nil, err
	}

	client := pb.NewFileServiceClient(conn)

	// Initialize replica first
	initReq := &pb.InitReplicaRequest{
		Filename: req.Filename,
		Metadata: &pb.FileMetadata{
			Filename:        req.Operation.FileName,
			LastModified:    time.Now().UnixNano(),
			Type:            pb.FileMetadata_REPLICA1,
			LastOperationId: int64(req.Operation.ID),
		},
		OperationType: pb.OperationType(req.Operation.Type),
	}

	initResp, err := client.InitReplica(ctx, initReq)
	if err != nil {
		return nil, fmt.Errorf("failed to init replica: %v", err)
	}

	if !initResp.Success {
		return nil, fmt.Errorf("init replica failed: %s", initResp.Error)
	}

	// Start streaming the file data
	stream, err := client.SendReplica(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create send replica stream: %v", err)
	}

	// Send metadata first
	firstMsg := &pb.SendReplicaRequest{
		Metadata: initResp.Metadata,
		Chunk:    nil,
	}

	if err := stream.Send(firstMsg); err != nil {
		return nil, fmt.Errorf("failed to send replica metadata: %v", err)
	}

	// Send file data in chunks
	const chunkSize = 1024 * 1024 // 1MB
	data := req.Operation.Data

	for i := 0; i < len(data); i += chunkSize {
		end := i + chunkSize
		if end > len(data) {
			end = len(data)
		}

		chunkMsg := &pb.SendReplicaRequest{
			Metadata: nil,
			Chunk: &pb.FileChunk{
				Content: data[i:end],
				Offset:  int64(i),
			},
		}

		if err := stream.Send(chunkMsg); err != nil {
			return nil, fmt.Errorf("failed to send replica chunk: %v", err)
		}
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		return nil, fmt.Errorf("error receiving replica response: %v", err)
	}

	return &ReplicationResponse{
		Success: resp.Success,
		Error:   resp.Error,
	}, nil
}

// Coordination operations
func (c *Client) ListFiles(ctx context.Context, nodeAddr string, clientID int64) ([]string, error) {
	conn, err := c.getCoordinationConnection(nodeAddr)
	if err != nil {
		return nil, err
	}

	client := coordination.NewCoordinationServiceClient(conn)

	grpcReq := &coordination.ListFilesRequest{
		ClientId:         clientID,
		RequestingNodeId: "client", // TODO: Get actual node ID
	}

	resp, err := client.ListFiles(ctx, grpcReq)
	if err != nil {
		return nil, fmt.Errorf("gRPC ListFiles failed: %v", err)
	}

	return resp.Filenames, nil
}

func (c *Client) MergeFile(ctx context.Context, nodeAddr string, req MergeRequest) (*MergeResponse, error) {
	conn, err := c.getCoordinationConnection(nodeAddr)
	if err != nil {
		return nil, err
	}

	client := coordination.NewCoordinationServiceClient(conn)

	grpcReq := &coordination.MergeRequest{
		Filename:         req.Filename,
		ClientId:         stringToInt64(req.ClientID),
		RequestingNodeId: "client", // TODO: Get actual node ID
	}

	resp, err := client.MergeFile(ctx, grpcReq)
	if err != nil {
		return nil, fmt.Errorf("gRPC MergeFile failed: %v", err)
	}

	return &MergeResponse{
		Success:      resp.Success,
		Error:        resp.Error,
		FinalVersion: resp.FinalVersion,
	}, nil
}

func (c *Client) HealthCheck(ctx context.Context, nodeAddr string) (bool, error) {
	conn, err := c.getCoordinationConnection(nodeAddr)
	if err != nil {
		return false, err
	}

	client := coordination.NewCoordinationServiceClient(conn)

	grpcReq := &coordination.HealthCheckRequest{
		SenderId:  "client", // TODO: Get actual node ID
		Timestamp: time.Now().UnixMilli(),
	}

	resp, err := client.HealthCheck(ctx, grpcReq)
	if err != nil {
		return false, fmt.Errorf("gRPC HealthCheck failed: %v", err)
	}

	return resp.Healthy, nil
}
