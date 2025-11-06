package coordinator

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"hydfs/pkg/fileserver"
	"hydfs/pkg/membership"
	"hydfs/pkg/network"
	"hydfs/pkg/protocol"
	"hydfs/pkg/transport"
	"hydfs/pkg/utils"
	mpb "hydfs/protoBuilds/membership"
)

// CoordinatorServer handles client commands and inter-node coordination for HyDFS
type CoordinatorServer struct {
	// Membership and networking from MP2
	membershipTable *membership.Table
	protocol        *protocol.Protocol
	udp             *transport.UDP

	// HyDFS specific components
	hashSystem    *utils.HashSystem
	fileServer    *fileserver.FileServer
	networkServer *network.GRPCServer
	networkClient *network.Client
	fileSystem    *utils.FileSystem // Structured filesystem object

	// Server configuration
	nodeID           *mpb.NodeID
	fileTransferPort int
	coordinationPort int
	storagePath      string

	// Logging
	logger func(string, ...interface{})

	// Operational state
	isRunning  bool
	shutdownCh chan struct{}
}

// NewCoordinatorServer creates a new HyDFS coordinator server
func NewCoordinatorServer(ip string, port int, logger func(string, ...interface{})) (*CoordinatorServer, error) {
	// Create node identity with timestamp as incarnation
	incarnation := uint64(time.Now().UnixMilli())
	nodeID, err := membership.NewNodeID(ip, uint32(port), incarnation)
	if err != nil {
		return nil, fmt.Errorf("failed to create node ID: %v", err)
	}

	// Initialize membership table
	table := membership.NewTable(nodeID, logger)

	// Create UDP transport for membership protocol
	// Handler will be set after protocol is created
	udp, err := transport.NewUDP(fmt.Sprintf("%s:%d", ip, port), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create UDP transport: %v", err)
	}

	// Create membership protocol (fanout of 3 for replication)
	proto := protocol.NewProtocol(table, udp, logger, 12)

	// Connect UDP handler to protocol
	udp.SetHandler(proto.Handle)

	// Create hash system with 3 replicas (can tolerate 2 failures)
	hashSystem := utils.NewHashSystem(3)

	// Add self to hash ring
	hashSystem.AddNode(membership.StringifyNodeID(nodeID))

	// Create storage path for this node
	storagePath := filepath.Join("storage", fmt.Sprintf("node_%s_%d", ip, port))

	// Create file server (4 workers for concurrent operations)
	fileServer, err := fileserver.NewFileServer(storagePath, 4, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create file server: %v", err)
	}

	// Create network client for inter-node communication
	networkClient := network.NewClient(10 * time.Second)

	// Port allocation:
	// UDP port (N) - membership protocol
	// UDP port + 1000 - file transfer gRPC service
	// UDP port + 2000 - coordination gRPC service
	fileTransferPort := port + 1000
	coordinationPort := port + 2000

	// Create gRPC network server for handling both services
	networkServer := network.NewGRPCServer(
		fmt.Sprintf("%s:%d", ip, fileTransferPort), // FileTransfer service
		fmt.Sprintf("%s:%d", ip, coordinationPort), // Coordination service
		fileServer,
		membership.StringifyNodeID(nodeID),
		logger,
	)

	// Create coordinator server
	cs := &CoordinatorServer{
		membershipTable:  table,
		protocol:         proto,
		udp:              udp,
		hashSystem:       hashSystem,
		fileServer:       fileServer,
		networkServer:    networkServer,
		networkClient:    networkClient,
		fileSystem:       utils.NewFileSystem(), // Initialize the FileSystem
		nodeID:           nodeID,
		fileTransferPort: fileTransferPort,
		coordinationPort: coordinationPort,
		storagePath:      storagePath,
		logger:           logger,
		shutdownCh:       make(chan struct{}),
	}

	// Set up membership change callback to update hash ring
	table.SetMembershipChangeCallback(cs.updateHashRing)

	networkServer.SetCoordinator(cs)

	return cs, nil
}

// Start starts the coordinator server
func (cs *CoordinatorServer) Start(ctx context.Context) error {
	cs.isRunning = true

	// Start file server
	if err := cs.fileServer.Start(); err != nil {
		return fmt.Errorf("failed to start file server: %v", err)
	}

	// Start network server for HTTP API
	go func() {
		if err := cs.networkServer.Start(); err != nil {
			cs.logger("Network server error: %v", err)
		}
	}()

	// Start UDP transport for membership protocol
	go func() {
		if err := cs.udp.Serve(ctx); err != nil {
			cs.logger("UDP server error: %v", err)
		}
	}()

	// Start membership protocol (ping mode with suspicion ON by default)
	cs.protocol.SetMode("ping")
	cs.protocol.SetSuspicion(true)

	// Initialize suspicion grace to prevent false SUSPECT storms during initial joining
	// This marks all current peers as "heard from" so they don't immediately get suspected
	cs.protocol.InitSuspicionGrace()

	// Set grace period to prevent suspicion storms when system starts up or nodes join
	// During this grace period, nodes won't be marked SUSPECT due to silence (only from missed ACKs)
	// Use Tfail (4 seconds) as grace period to give nodes time to stabilize
	if cs.protocol.SuspicionOn() {
		cs.protocol.Sus.StartGrace(4 * time.Second)
	}

	// Start ping loop for membership protocol (1 second period, 2 second ACK timeout)
	// Longer ACK timeout helps handle network congestion and prevents false SUSPECTs
	go cs.protocol.StartPingAck(ctx, 1*time.Second, 2*time.Second)

	// Start background tasks
	go cs.backgroundTasks(ctx)

	cs.logger("HyDFS Coordinator started on node %s", membership.StringifyNodeID(cs.nodeID))
	cs.logger("gRPC services available - FileTransfer: %d, Coordination: %d", cs.fileTransferPort, cs.coordinationPort)
	cs.logger("Storage path: %s", cs.storagePath)
	cs.logger("Hash system initialized with 3 replicas")
	cs.logger("Node startup completed successfully - ready for operations")
	return nil
}

// Stop stops the coordinator server
func (cs *CoordinatorServer) Stop() {
	if !cs.isRunning {
		return
	}

	cs.isRunning = false
	close(cs.shutdownCh)

	// Stop network server
	if cs.networkServer != nil {
		cs.networkServer.Stop()
	}

	// Stop file server
	if cs.fileServer != nil {
		cs.fileServer.Stop()
	}

	// Stop UDP transport
	if cs.udp != nil {
		cs.udp.Close()
	}

	cs.logger("HyDFS Coordinator stopped")
}

// CreateFile handles distributed file creation
//func (cs *CoordinatorServer) CreateFile(hydfsFileName string, localFileName string, clientID string) error {
//	cs.logger("CREATE operation initiated - file name on HyDFS: %s, local File Name: %d bytes, client: %s", hydfsFileName, localFileName, clientID)
//
//	// Get the designated coordinator for this file
//	coordinatorNodeID := cs.hashSystem.ComputeLocation(hydfsFileName)
//	if coordinatorNodeID == "" {
//		return fmt.Errorf("no coordinator found - hash ring may be empty")
//	}
//
//	selfNodeID := membership.StringifyNodeID(cs.nodeID)
//
//	if coordinatorNodeID != selfNodeID {
//		// Forward request to the designated coordinator
//		cs.logger("Forwarding CREATE operation for file %s to coordinator %s", hydfsFileName, coordinatorNodeID)
//		err := cs.sendToCoordinator(coordinatorNodeID, utils.Create, hydfsFileName, localFileName)
//		if err == nil {
//			cs.logger("CREATE operation forwarded successfully - file: %s", hydfsFileName)
//		} else {
//			cs.logger("CREATE operation forward failed - file: %s, error: %v", hydfsFileName, err)
//		}
//		return err
//	}
//
//	// This node itself is the coordinator - handle the operation directly
//	cs.logger("Acting as coordinator for CREATE operation - file: %s", hydfsFileName)
//
//	// Step 1: Save file locally to OwnedFiles/
//	_, err := cs.performLocalOperation(utils.Create, hydfsFileName, localFileName, clientID, selfNodeID, selfNodeID, true)
//	if err != nil {
//		cs.logger("Failed to save file locally: %v", err)
//		return fmt.Errorf("failed to save file locally: %v", err)
//	}
//
//	// Step 2: Create replica request
//	fileReq := &utils.FileRequest{
//		OperationType:     utils.OperationType(operationType),
//		FileName:          sendFileMetadata.HydfsFilename,
//		Data:              buffer,
//		ClientID:          strconv.FormatInt(sendFileMetadata.ClientId, 10),
//		FileOperationID:   int(operationId),
//		DestinationNodeID: s.nodeID,
//		SourceNodeID:      s.nodeID,
//		OwnerNodeID:       s.nodeID, // This node is the owner for files created here
//	}
//
//	// Submit to file server for processing
//	err := s.fileServer.SubmitRequest(fileReq)
//	if err != nil {
//		return stream.SendAndClose(&fileservice.SendFileResponse{
//			Success: false,
//			Error:   fmt.Sprintf("Failed to process file: %v", err),
//		})
//	}
//
//	cs.logger("CREATE operation completed - file: %s, replicas: %d/%d", hydfsFileName, successCount, len(replicas))
//	return nil
//}

func (cs *CoordinatorServer) CreateFile(hydfsFileName string, localFileName string, clientID string) error {
	cs.logger("CREATE initiated - HyDFS file: %s, local file: %s, client: %s",
		hydfsFileName, localFileName, clientID)

	// Step 1: Determine ownership
	ownerNodeID, err := cs.determineFileOwner(hydfsFileName)
	if err != nil {
		return err
	}

	// Step 2: Route based on ownership
	if cs.isCurrentNode(ownerNodeID) {
		return cs.handleCreateAsOwner(hydfsFileName, localFileName, clientID)
	}

	return cs.forwardCreateToOwner(ownerNodeID, hydfsFileName, localFileName)
}

// determineFileOwner uses consistent hashing to find the owner node for a file
func (cs *CoordinatorServer) determineFileOwner(fileName string) (string, error) {
	ownerNodeID := cs.hashSystem.ComputeLocation(fileName)
	if ownerNodeID == "" {
		return "", fmt.Errorf("no owner found - hash ring may be empty")
	}
	return ownerNodeID, nil
}

// isCurrentNode checks if the given nodeID is this node
func (cs *CoordinatorServer) isCurrentNode(nodeID string) bool {
	selfNodeID := membership.StringifyNodeID(cs.nodeID)
	return nodeID == selfNodeID
}

func (cs *CoordinatorServer) handleCreateAsOwner(hydfsFileName string, localFileName string, clientID string) error {
	cs.logger("Acting as owner for CREATE - file: %s", hydfsFileName)

	// Step 1: Save file locally (direct, synchronous)
	if err := cs.saveFileLocally(hydfsFileName, localFileName, clientID); err != nil {
		return fmt.Errorf("failed to save file locally: %v", err)
	}

	// Step 2: Replicate to other nodes
	ownedFilePath := filepath.Join(cs.fileServer.GetOwnedFilesDir(), hydfsFileName)
	if err := cs.ReplicateFileToReplicas(hydfsFileName, ownedFilePath); err != nil {
		cs.logger("WARNING: Replication incomplete for %s: %v", hydfsFileName, err)
		// Don't return error - file is saved on owner
	}

	cs.logger("CREATE completed successfully - file: %s", hydfsFileName)
	return nil
}

// saveFileLocally saves a file directly to /OwnedFiles
// Uses FileServer.SaveFile for consistent file handling
func (cs *CoordinatorServer) saveFileLocally(hydfsFileName string, localFileName string, clientID string) error {
	cs.logger("Saving file locally - HyDFS: %s, source: %s", hydfsFileName, localFileName)

	selfNodeID := membership.StringifyNodeID(cs.nodeID)

	// Create file request with all tracking fields
	fileReq := &utils.FileRequest{
		OperationType:     utils.Create,
		FileName:          hydfsFileName,
		LocalFilePath:     localFileName, // SaveFile will read from this path
		ClientID:          clientID,
		FileOperationID:   1, // CREATE always starts at opID 1
		OwnerNodeID:       selfNodeID,
		DestinationNodeID: selfNodeID,
		SourceNodeID:      selfNodeID,
	}

	// Use FileServer's SaveFile method
	cs.fileServer.SaveFile(fileReq)

	// Wait briefly for file to be written to disk
	time.Sleep(50 * time.Millisecond)

	cs.logger("File saved locally to /OwnedFiles: %s", hydfsFileName)
	return nil
}

// replicateFileToReplicas sends a file to all replica nodes
// Uses consistent hashing to determine replica nodes
func (cs *CoordinatorServer) ReplicateFileToReplicas(hydfsFileName string, localFilePath string) error {
	// Get replica nodes from hash ring
	replicas := cs.hashSystem.GetReplicaNodes(hydfsFileName)
	if len(replicas) < 1 {
		return fmt.Errorf("no replica nodes found in hash ring")
	}

	cs.logger("Replicating file %s to %d nodes", hydfsFileName, len(replicas))

	// Send to each replica (skip index 0 which is owner)
	successCount := 1 // Count owner as success
	for i, replicaNodeID := range replicas {
		if i == 0 {
			continue // Skip owner (this node)
		}

		if err := cs.sendFileToReplica(replicaNodeID, hydfsFileName, localFilePath); err != nil {
			cs.logger("Failed to replicate to node %s: %v", replicaNodeID, err)
		} else {
			successCount++
			cs.logger("Successfully replicated to node %s", replicaNodeID)
		}
	}

	cs.logger("Replication summary - file: %s, success: %d/%d",
		hydfsFileName, successCount, len(replicas))

	if successCount < 2 {
		return fmt.Errorf("replication failed - only %d/%d nodes successful", successCount, len(replicas))
	}

	return nil
}

// sendFileToReplica sends a file to a single replica node
func (cs *CoordinatorServer) sendFileToReplica(replicaNodeID string, hydfsFileName string, localFilePath string) error {
	// Get replica node address
	nodeAddr, err := cs.getNodeFileTransferAddr(replicaNodeID)
	if err != nil {
		return fmt.Errorf("failed to get address for replica %s: %v", replicaNodeID, err)
	}

	selfNodeID := membership.StringifyNodeID(cs.nodeID)

	// Create replica request
	req := network.ReplicaRequest{
		HydfsFilename: hydfsFileName,
		LocalFilename: localFilePath,
		OperationType: utils.CreateReplica,
		SenderID:      selfNodeID,
		OperationID:   1, // CREATE always has opID = 1
	}

	// Send via gRPC
	resp, err := cs.networkClient.SendReplica(context.Background(), nodeAddr, req)
	if err != nil {
		return fmt.Errorf("gRPC call failed: %v", err)
	}

	if !resp.Success {
		return fmt.Errorf("replica returned error: %s", resp.Error)
	}

	return nil
}

// forwardCreateToOwner forwards a CREATE request to the designated owner node
// The owner will handle saving and replication
func (cs *CoordinatorServer) forwardCreateToOwner(ownerNodeID string, hydfsFileName string, localFileName string) error {
	cs.logger("Forwarding CREATE to owner %s - file: %s", ownerNodeID, hydfsFileName)

	// Get owner's file transfer address
	nodeAddr, err := cs.getNodeFileTransferAddr(ownerNodeID)
	if err != nil {
		return fmt.Errorf("failed to get owner address: %v", err)
	}

	// Create request
	req := network.FileTransferRequest{
		HydfsFilename: hydfsFileName,
		LocalFilename: localFileName,
		OperationType: utils.Create,
		SenderID:      membership.StringifyNodeID(cs.nodeID),
	}

	// Send file to owner via gRPC
	resp, err := cs.networkClient.SendFile(context.Background(), nodeAddr, req)
	if err != nil {
		return fmt.Errorf("forward to owner failed: %v", err)
	}

	if !resp.Success {
		return fmt.Errorf("owner returned error: %s", resp.Message)
	}

	cs.logger("CREATE forwarded successfully to owner %s", ownerNodeID)
	return nil
}

// AppendFile handles distributed file append with 3-way replication
// This is the main entry point that delegates to appropriate handlers
func (cs *CoordinatorServer) AppendFile(hydfsFileName string, localFileName string, clientID string) error {
	cs.logger("APPEND initiated - HyDFS file: %s, local file: %s, client: %s",
		hydfsFileName, localFileName, clientID)

	// Step 1: Determine ownership
	ownerNodeID, err := cs.determineFileOwner(hydfsFileName)
	if err != nil {
		return err
	}

	// Step 2: Route based on ownership
	if cs.isCurrentNode(ownerNodeID) {
		return cs.handleAppendAsOwner(hydfsFileName, localFileName, clientID)
	}

	return cs.forwardAppendToOwner(ownerNodeID, hydfsFileName, localFileName)
}

// handleAppendAsOwner handles APPEND when this node is the owner
// Appends locally (via queue for ordering) and replicates to other nodes
func (cs *CoordinatorServer) handleAppendAsOwner(hydfsFileName string, localFileName string, clientID string) error {
	cs.logger("Acting as owner for APPEND - file: %s", hydfsFileName)

	selfNodeID := membership.StringifyNodeID(cs.nodeID)

	// Step 1: Append locally (queued for ordering)
	opID, err := cs.appendFileLocally(hydfsFileName, localFileName, clientID, selfNodeID)
	if err != nil {
		return fmt.Errorf("failed to append file locally: %v", err)
	}

	cs.logger("Local append queued with opID: %d", opID)

	// Step 2: Replicate append to other nodes
	if err := cs.ReplicateAppendToReplicas(hydfsFileName, localFileName, opID); err != nil {
		cs.logger("WARNING: Append replication incomplete for %s: %v", hydfsFileName, err)
		// Don't return error - append is queued locally
	}

	cs.logger("APPEND completed successfully - file: %s, opID: %d", hydfsFileName, opID)
	return nil
}

// forwards an APPEND request to the designated owner node
// The owner will handle appending and replication
func (cs *CoordinatorServer) forwardAppendToOwner(ownerNodeID string, hydfsFileName string, localFileName string) error {
	cs.logger("Forwarding APPEND to owner %s - file: %s", ownerNodeID, hydfsFileName)

	// Get owner's file transfer address
	nodeAddr, err := cs.getNodeFileTransferAddr(ownerNodeID)
	if err != nil {
		return fmt.Errorf("failed to get owner address: %v", err)
	}

	// Create request
	req := network.FileTransferRequest{
		HydfsFilename: hydfsFileName,
		LocalFilename: localFileName,
		OperationType: utils.Append,
		SenderID:      membership.StringifyNodeID(cs.nodeID),
	}

	// Send append to owner via gRPC
	resp, err := cs.networkClient.SendFile(context.Background(), nodeAddr, req)
	if err != nil {
		return fmt.Errorf("forward to owner failed: %v", err)
	}

	if !resp.Success {
		return fmt.Errorf("owner returned error: %s", resp.Message)
	}

	cs.logger("APPEND forwarded successfully to owner %s", ownerNodeID)
	return nil
}

// appendFileLocally queues an append operation for local processing
// Returns the assigned operation ID
func (cs *CoordinatorServer) appendFileLocally(hydfsFileName string, localFileName string, clientID string, nodeID string) (int, error) {
	cs.logger("Queueing local append - file: %s, source: %s", hydfsFileName, localFileName)

	// Step 1: Check if file exists and get next operation ID
	metadata := cs.fileServer.GetFileMetadata(hydfsFileName)
	if metadata == nil {
		return 0, fmt.Errorf("cannot append to file %s: file not found", hydfsFileName)
	}

	// Step 2: Atomically increment and get new operation ID
	opID, err := cs.fileServer.IncrementAndGetOperationID(hydfsFileName)
	if err != nil {
		return 0, fmt.Errorf("failed to get operation ID for append: %v", err)
	}

	cs.logger("Assigned operation ID %d for append to file %s", opID, hydfsFileName)

	// Step 3: Create file request for queued processing
	fileReq := &utils.FileRequest{
		OperationType:     utils.Append,
		FileName:          hydfsFileName,
		LocalFilePath:     localFileName, // FileServer will read from this path
		ClientID:          clientID,
		FileOperationID:   opID,
		OwnerNodeID:       nodeID,
		DestinationNodeID: nodeID,
		SourceNodeID:      nodeID,
	}

	// Step 4: Submit to queue (asynchronous processing)
	if err := cs.fileServer.SubmitRequest(fileReq); err != nil {
		return 0, fmt.Errorf("failed to submit append request: %v", err)
	}

	cs.logger("Append operation %d queued for file %s", opID, hydfsFileName)
	return opID, nil
}

// ReplicateAppendToReplicas Sends an append operation to all replica nodes
// Uses the same operation ID to maintain consistency
func (cs *CoordinatorServer) ReplicateAppendToReplicas(hydfsFileName string, localFilePath string, operationID int) error {
	cs.logger("Replicating append for file: %s (opID: %d)", hydfsFileName, operationID)

	// Get replica nodes from hash ring
	replicas := cs.hashSystem.GetReplicaNodes(hydfsFileName)
	if len(replicas) < 1 {
		return fmt.Errorf("no replica nodes found in hash ring")
	}

	// Send append to each replica (skip index 0 which is owner)
	successCount := 1 // Count owner as success
	for i, replicaNodeID := range replicas {
		if i == 0 {
			continue // Skip owner (this node)
		}

		if err := cs.sendAppendToReplica(replicaNodeID, hydfsFileName, localFilePath, operationID); err != nil {
			cs.logger("Failed to replicate append to node %s: %v", replicaNodeID, err)
		} else {
			successCount++
			cs.logger("Successfully replicated append to node %s", replicaNodeID)
		}
	}

	cs.logger("Append replication summary - file: %s, opID: %d, success: %d/%d",
		hydfsFileName, operationID, successCount, len(replicas))

	if successCount < 2 {
		return fmt.Errorf("append replication failed - only %d/%d nodes successful", successCount, len(replicas))
	}

	return nil
}

// sendAppendToReplica sends an append operation to a single replica node
// Includes the operation ID to ensure proper ordering
func (cs *CoordinatorServer) sendAppendToReplica(replicaNodeID string, hydfsFileName string, localFilePath string, operationID int) error {
	cs.logger("Sending append to replica %s: %s (opID: %d)", replicaNodeID, hydfsFileName, operationID)

	// Get replica node address
	nodeAddr, err := cs.getNodeFileTransferAddr(replicaNodeID)
	if err != nil {
		return fmt.Errorf("failed to get address for replica %s: %v", replicaNodeID, err)
	}

	selfNodeID := membership.StringifyNodeID(cs.nodeID)

	// Create replica request with operation ID
	req := network.ReplicaRequest{
		HydfsFilename: hydfsFileName,
		LocalFilename: localFilePath,
		OperationType: utils.AppendReplica,
		SenderID:      selfNodeID,
		OperationID:   operationID, // Pass the assigned opID
	}

	// Send via gRPC
	resp, err := cs.networkClient.SendReplica(context.Background(), nodeAddr, req)
	if err != nil {
		return fmt.Errorf("gRPC call failed: %v", err)
	}

	if !resp.Success {
		return fmt.Errorf("replica returned error: %s", resp.Error)
	}

	cs.logger("Successfully sent append to replica %s", replicaNodeID)
	return nil
}

// GetFile handles distributed file retrieval
func (cs *CoordinatorServer) GetFile(filename string, clientID string) ([]byte, error) {
	cs.logger("GET operation initiated - file: %s, client: %s", filename, clientID)

	// Determine the owner (coordinator) for this file using consistent hashing
	ownerNodeID := cs.hashSystem.ComputeLocation(filename)
	if ownerNodeID == "" {
		return nil, fmt.Errorf("no owner found - hash ring may be empty")
	}

	selfNodeID := membership.StringifyNodeID(cs.nodeID)

	// If this node is the owner, read locally
	if ownerNodeID == selfNodeID {
		cs.logger("This node is the owner for file %s, reading locally", filename)
		data, err := cs.fileServer.ReadFile(filename)
		if err == nil {
			cs.logger("File %s found locally", filename)
			return data, nil
		}
		cs.logger("File %s not found locally: %v", filename, err)
		return nil, fmt.Errorf("file %s not found", filename)
	}

	// This node is NOT the owner - try to get from the owner node first
	cs.logger("Owner of file %s is node %s, fetching from owner", filename, ownerNodeID)
	nodeAddr, err := cs.getNodeFileTransferAddr(ownerNodeID)
	if err != nil {
		cs.logger("Failed to get address for owner node %s: %v", ownerNodeID, err)
	} else {
		req := network.FileRequest{
			OperationType: utils.Get,
			FileName:      filename,
			ClientID:      clientID,
		}

		resp, reqErr := cs.networkClient.GetFile(context.Background(), nodeAddr, req)
		if reqErr == nil && resp.Success && len(resp.Data) > 0 {
			cs.logger("File %s successfully retrieved from owner %s", filename, ownerNodeID)
			return resp.Data, nil
		}
		cs.logger("Failed to get file %s from owner: %v", filename, reqErr)
	}

	// Owner failed - fall back to replicas
	cs.logger("Owner unavailable, trying replicas for file %s", filename)
	replicas := cs.hashSystem.GetReplicaNodes(filename)

	for i, replicaNodeID := range replicas {
		// Skip the owner (index 0) since we already tried it
		if i == 0 {
			continue
		}

		// Skip if this is the current node
		if replicaNodeID == selfNodeID {
			data, err := cs.fileServer.ReadFile(filename)
			if err == nil {
				cs.logger("File %s found locally as replica", filename)
				return data, nil
			}
			continue
		}

		// Try to get from this replica
		nodeAddr, addrErr := cs.getNodeFileTransferAddr(replicaNodeID)
		if addrErr != nil {
			cs.logger("Failed to get address for replica %s: %v", replicaNodeID, addrErr)
			continue
		}

		req := network.FileRequest{
			OperationType: utils.Get,
			FileName:      filename,
			ClientID:      clientID,
		}

		resp, reqErr := cs.networkClient.GetFile(context.Background(), nodeAddr, req)
		if reqErr == nil && resp.Success && len(resp.Data) > 0 {
			cs.logger("File %s retrieved from replica %s", filename, replicaNodeID)
			return resp.Data, nil
		}
		cs.logger("Failed to get file %s from replica %s: %v", filename, replicaNodeID, reqErr)
	}

	return nil, fmt.Errorf("file %s not found on owner or any replica", filename)
}

// performLocalOperation performs file operation on local node
// Returns the operation ID that was assigned
func (cs *CoordinatorServer) performLocalOperation(opType utils.OperationType, hydfsFileName string, localFileName string, clientID string, ownerNodeID string, currentNodeID string, isOwner bool) (int, error) {
	var opID int

	if opType == utils.Create {
		// For CREATE, operation ID is always 1
		opID = 1
	} else if opType == utils.Append {
		// For APPEND, get current operation ID and increment
		metadata := cs.fileServer.GetFileMetadata(hydfsFileName)
		if metadata == nil {
			return 0, fmt.Errorf("cannot append to file %s: file not found", hydfsFileName)
		}

		// Atomically increment and get new operation ID
		newOpID, err := cs.fileServer.IncrementAndGetOperationID(hydfsFileName)
		if err != nil {
			return 0, fmt.Errorf("failed to get operation ID for append: %v", err)
		}
		opID = newOpID
		cs.logger("Assigned operation ID %d for append to file %s", opID, hydfsFileName)
	} else {
		// For other operations, use 1 as default
		opID = 1
	}

	req := &utils.FileRequest{
		OperationType:     opType,
		FileName:          hydfsFileName,
		LocalFilePath:     localFileName, // Path to read the file from
		ClientID:          clientID,
		SourceNodeID:      currentNodeID, // This node is the source
		DestinationNodeID: currentNodeID, // This node is also the destination
		OwnerNodeID:       ownerNodeID,   // Primary owner node for this file
		FileOperationID:   opID,          // Correct operation ID based on type
	}

	// Submit the request to the local FileServer
	err := cs.fileServer.SubmitRequest(req)
	if err != nil {
		return 0, fmt.Errorf("failed to submit local operation: %v", err)
	}

	cs.logger("Submitted local %v operation (ID: %d) for file %s", opType, opID, hydfsFileName)
	return opID, nil // Return the assigned operation ID
}

// getNodeFileTransferAddr converts a node ID to file transfer gRPC address
func (cs *CoordinatorServer) getNodeFileTransferAddr(nodeID string) (string, error) {
	ip, port, err := utils.ParseNodeID(nodeID)
	if err != nil {
		return "", err
	}

	// File transfer gRPC port is UDP port + 2000
	fileTransferPort := port + 2000
	return fmt.Sprintf("%s:%d", ip, fileTransferPort), nil
}

// getNodeCoordinationAddr converts a node ID to coordination gRPC address
func (cs *CoordinatorServer) getNodeCoordinationAddr(nodeID string) (string, error) {
	ip, port, err := utils.ParseNodeID(nodeID)
	if err != nil {
		return "", err
	}

	// Coordination gRPC port is UDP port + 3000
	coordinationPort := port + 3000
	return fmt.Sprintf("%s:%d", ip, coordinationPort), nil
}

// sendToCoordinator forwards file operations to the designated coordinator node
func (cs *CoordinatorServer) sendToCoordinator(coordinatorNodeID string, opType utils.OperationType, hydfsFileName string, localFileName string) error {
	cs.logger("Forwarding %v operation for file %s to coordinator %s", opType, hydfsFileName, coordinatorNodeID)

	if opType == utils.Create || opType == utils.Append {
		// Use network client to send file to coordinator via gRPC streaming
		nodeAddr, err := cs.getNodeFileTransferAddr(coordinatorNodeID)
		if err != nil {
			return fmt.Errorf("failed to get coordinator address: %v", err)
		}

		selfNodeID := membership.StringifyNodeID(cs.nodeID)

		// Call SendFile to send the file
		req := network.FileTransferRequest{
			HydfsFilename: hydfsFileName,
			LocalFilename: localFileName,
			OperationType: opType,
			SenderID:      selfNodeID,
		}

		ctx := context.Background()
		fileSentResponse, err := cs.networkClient.SendFile(ctx, nodeAddr, req)
		if err != nil {
			return fmt.Errorf("coordinator request failed: %v", err)
		}

		if !fileSentResponse.Success {
			return fmt.Errorf("coordinator operation failed: %s", err)
		}

		return nil
	}

	return fmt.Errorf("invalid operation type")
}

// ListFiles returns a list of all files in the distributed system
// OPTIMIZED: Queries all nodes in parallel instead of sequentially
func (cs *CoordinatorServer) ListFiles(clientID string) ([]string, error) {
	allFiles := make(map[string]bool)
	var mu sync.Mutex
	var wg sync.WaitGroup

	// Get files from local node's FileSystem
	localFiles := cs.fileSystem.GetFiles()
	for filename := range localFiles {
		allFiles[filename] = true
	}

	// Get files from all other nodes in parallel
	members := cs.membershipTable.GetMembers()
	for _, member := range members {
		nodeID := membership.StringifyNodeID(member.NodeID)
		if nodeID == membership.StringifyNodeID(cs.nodeID) {
			continue // Skip self
		}

		if member.State != mpb.MemberState_ALIVE {
			continue // Skip non-alive nodes
		}

		wg.Add(1)
		go func(nodeID string) {
			defer wg.Done()

			nodeAddr, err := cs.getNodeCoordinationAddr(nodeID)
			if err != nil {
				cs.logger("Failed to get address for node %s: %v", nodeID, err)
				return
			}

			files, err := cs.networkClient.ListFiles(context.Background(), nodeAddr, 1) // clientID as int64
			if err != nil {
				cs.logger("Failed to list files from node %s: %v", nodeID, err)
				return
			}

			mu.Lock()
			for _, filename := range files {
				allFiles[filename] = true
			}
			mu.Unlock()
		}(nodeID)
	}

	// Wait for all queries to complete
	wg.Wait()

	// Convert to slice
	var result []string
	for filename := range allFiles {
		result = append(result, filename)
	}

	return result, nil
}

// MergeFile synchronizes file versions across replicas
// TODO Ignore this function for now
func (cs *CoordinatorServer) MergeFile(filename string, clientID string) error {
	replicas := cs.hashSystem.GetReplicaNodes(filename)

	// Collect file data from all replicas in parallel
	replicaData := make(map[string][]byte)
	replicaVersions := make(map[string]int64)
	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, nodeID := range replicas {
		wg.Add(1)
		go func(nodeID string) {
			defer wg.Done()

			var data []byte
			var version int64

			if nodeID == membership.StringifyNodeID(cs.nodeID) {
				// Local replica - check both FileSystem and FileServer
				metadata := cs.fileSystem.GetFile(filename)
				if metadata == nil {
					// Try FileServer as fallback
					metadata = cs.fileServer.GetFileMetadata(filename)
				}
				if metadata != nil {
					// Read local file data
					if localData := cs.readLocalFileData(filename); localData != nil {
						data = localData
						version = int64(len(metadata.Operations))
					}
				}
			} else {
				// Remote replica
				nodeAddr, addrErr := cs.getNodeFileTransferAddr(nodeID)
				if addrErr != nil {
					cs.logger("Failed to get address for node %s: %v", nodeID, addrErr)
					return
				}

				req := network.FileRequest{
					OperationType: utils.Get,
					FileName:      filename,
					ClientID:      "1", // Convert later
				}

				resp, reqErr := cs.networkClient.GetFile(context.Background(), nodeAddr, req)
				if reqErr == nil && resp.Success {
					// Note: gRPC GetFile may not return file data directly
					version = 1 // Default version
				} else {
					cs.logger("Failed to get file from node %s: %v", nodeID, reqErr)
					return
				}
			}

			if data != nil {
				mu.Lock()
				replicaData[nodeID] = data
				replicaVersions[nodeID] = version
				mu.Unlock()
			}
		}(nodeID)
	}

	// Wait for all replicas to respond
	wg.Wait()

	if len(replicaData) == 0 {
		return fmt.Errorf("file %s not found on any replica", filename)
	}

	// Find the replica with the highest version (most recent)
	var newestNodeID string
	var newestVersion int64 = -1
	var newestData []byte

	for nodeID, version := range replicaVersions {
		if version > newestVersion {
			newestVersion = version
			newestNodeID = nodeID
			newestData = replicaData[nodeID]
		}
	}

	cs.logger("Merging file %s: newest version %d from node %s", filename, newestVersion, newestNodeID)

	// Propagate the newest version to all replicas in parallel
	var successCount int64
	// Reuse the existing wg from above (already waited on line 540)

	for _, nodeID := range replicas {
		wg.Add(1)
		go func(nodeID string) {
			defer wg.Done()

			currentData, exists := replicaData[nodeID]
			var err error

			if !exists {
				// Replica doesn't have the file, create it
				// TODO: Implement propagateFileToReplica
				cs.logger("TODO: Need to create file on replica %s", nodeID)
				err = fmt.Errorf("propagateFileToReplica not implemented")
			} else if string(currentData) != string(newestData) {
				// Replica has different content, update it
				// TODO: Implement propagateFileToReplica
				cs.logger("TODO: Need to update file on replica %s", nodeID)
				err = fmt.Errorf("propagateFileToReplica not implemented")
			}
			// else: replica already has correct content, count as success

			if err == nil {
				atomic.AddInt64(&successCount, 1)
			}
		}(nodeID)
	}

	// Wait for all propagation operations to complete
	wg.Wait()

	count := int(atomic.LoadInt64(&successCount))

	if count >= 2 {
		cs.logger("Merge completed successfully for file %s (%d/%d replicas synced)", filename, count, len(replicas))
		return nil
	}

	return fmt.Errorf("merge failed: only %d/%d replicas synced", count, len(replicas))
}

// readLocalFileData reads file data from local storage
func (cs *CoordinatorServer) readLocalFileData(filename string) []byte {
	data, err := cs.fileServer.ReadFile(filename)
	if err != nil {
		cs.logger("Failed to read local file %s: %v", filename, err)
		return nil
	}
	return data
}

// backgroundTasks runs background maintenance tasks
func (cs *CoordinatorServer) backgroundTasks(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-cs.shutdownCh:
			return
		case <-ticker.C:
			// Perform membership cleanup (hash ring updates automatically via callback)
			cs.membershipTable.GCStates(30*time.Second, true)
		}
	}
}

// updateHashRing updates the consistent hash ring with current membership
func (cs *CoordinatorServer) updateHashRing() {
	members := cs.membershipTable.GetMembers()

	// Get current nodes in hash ring
	currentNodes := cs.hashSystem.GetAllNodes()

	initialRingSize := len(currentNodes)
	aliveMembers := 0

	// Add new alive members to hash ring
	for _, member := range members {
		if member.State == mpb.MemberState_ALIVE {
			aliveMembers++
			nodeIDStr := membership.StringifyNodeID(member.NodeID)
			if _, exists := currentNodes[nodeIDStr]; !exists {
				cs.hashSystem.AddNode(nodeIDStr)
				cs.logger("Added node %s to hash ring", nodeIDStr)
			}
		}
	}

	// Remove failed/left members from hash ring
	aliveNodes := make(map[string]bool)
	for _, member := range members {
		if member.State == mpb.MemberState_ALIVE {
			aliveNodes[membership.StringifyNodeID(member.NodeID)] = true
		}
	}

	removedNodes := 0
	for nodeIDStr := range currentNodes {
		if !aliveNodes[nodeIDStr] {
			cs.hashSystem.RemoveNode(nodeIDStr)
			cs.logger("Removed node %s from hash ring", nodeIDStr)
			removedNodes++
		}
	}

	// Log ring state changes
	finalRingSize := len(cs.hashSystem.GetAllNodes())
	if finalRingSize != initialRingSize || removedNodes > 0 {
		cs.logger("Hash ring updated - members: %d, ring size: %d->%d", aliveMembers, initialRingSize, finalRingSize)
	}

	// Warn if membership and ring size don't match (indicates convergence issue)
	if aliveMembers > 0 && finalRingSize != aliveMembers {
		cs.logger("WARNING: Membership has %d ALIVE members but hash ring has %d nodes - convergence may be incomplete", aliveMembers, finalRingSize)
	}
}

// GetMembershipTable returns the current membership table
func (cs *CoordinatorServer) GetMembershipTable() *membership.Table {
	return cs.membershipTable
}

// GetHashSystem returns the hash system
func (cs *CoordinatorServer) GetHashSystem() *utils.HashSystem {
	return cs.hashSystem
}

// GetNodeID returns the node ID
func (cs *CoordinatorServer) GetNodeID() *mpb.NodeID {
	return cs.nodeID
}

// ListStoredFiles returns all files stored locally on this node
func (cs *CoordinatorServer) ListStoredFiles() map[string]*utils.FileMetaData {
	return cs.fileServer.ListStoredFiles()
}

// ListReplicatedFiles returns only files stored in ReplicatedFiles directory
// (files where this node is a replica, not the owner)
func (cs *CoordinatorServer) ListReplicatedFiles() map[string]*utils.FileMetaData {
	return cs.fileServer.ListReplicatedFiles()
}
