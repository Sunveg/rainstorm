package coordinator

import (
	"context"
	"fmt"
	"hydfs/protoBuilds/coordination"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"hydfs/pkg/fileserver"
	"hydfs/pkg/membership"
	"hydfs/pkg/network"
	"hydfs/pkg/protocol"
	"hydfs/pkg/transport"
	"hydfs/pkg/utils"
	mpb "hydfs/protoBuilds/membership"

	"google.golang.org/grpc"
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

// Join joins the cluster via an introducer node
func (cs *CoordinatorServer) Join(introducerAddr string) error {
	// Parse introducer address (format: "ip:port")
	udpAddr, err := net.ResolveUDPAddr("udp", introducerAddr)
	if err != nil {
		return fmt.Errorf("failed to resolve introducer address: %v", err)
	}

	// Send join request via membership protocol
	ctx := context.Background()
	if err := cs.protocol.SendJoin(ctx, udpAddr); err != nil {
		return fmt.Errorf("failed to send join: %v", err)
	}

	cs.logger("JOIN request sent to introducer %s", introducerAddr)
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

// CreateFile handles distributed file creation with 3-way replication
// Flow: Determine Owner → Route (Owner: Save + Replicate | Non-Owner: Forward)
func (cs *CoordinatorServer) CreateFile(hydfsFileName string, localFileName string, clientID string) error {
	cs.logger("=== CREATE INITIATED: %s (from %s) ===", hydfsFileName, clientID)

	// Step 1: Determine ownership using consistent hashing
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

// isCurrentNode checks if the given nodeID matches this node
func (cs *CoordinatorServer) isCurrentNode(nodeID string) bool {
	selfNodeID := membership.StringifyNodeID(cs.nodeID)
	return nodeID == selfNodeID
}

// handleCreateAsOwner handles CREATE when this node is the owner
// Flow: Save Locally → Replicate to Others
func (cs *CoordinatorServer) handleCreateAsOwner(hydfsFileName string, localFileName string, clientID string) error {
	cs.logger("Acting as OWNER for CREATE: %s", hydfsFileName)

	// Step 1: Save file locally to /OwnedFiles (synchronous)
	if err := cs.saveFileLocally(hydfsFileName, localFileName, clientID); err != nil {
		return fmt.Errorf("local save failed: %v", err)
	}

	// Step 2: Replicate to replica nodes
	ownedFilePath := filepath.Join(cs.fileServer.GetOwnedFilesDir(), hydfsFileName)
	if err := cs.ReplicateFileToReplicas(hydfsFileName, ownedFilePath); err != nil {
		cs.logger("WARNING: Replication incomplete: %v", err)
		// Don't fail - file is saved on owner
	}

	cs.logger("CREATE completed successfully: %s", hydfsFileName)
	return nil
}

// saveFileLocally saves a file directly to /OwnedFiles (synchronous)
func (cs *CoordinatorServer) saveFileLocally(hydfsFileName string, localFileName string, clientID string) error {
	cs.logger("Saving locally: %s → %s", localFileName, hydfsFileName)

	selfNodeID := membership.StringifyNodeID(cs.nodeID)

	fileReq := &utils.FileRequest{
		OperationType:     utils.Create,
		FileName:          hydfsFileName,
		LocalFilePath:     localFileName,
		ClientID:          clientID,
		FileOperationID:   1, // CREATE always starts at opID 1
		OwnerNodeID:       selfNodeID,
		DestinationNodeID: selfNodeID,
		SourceNodeID:      selfNodeID,
	}

	cs.fileServer.SaveFile(fileReq)
	time.Sleep(50 * time.Millisecond) // Ensure write completes

	cs.logger("Saved to /OwnedFiles: %s", hydfsFileName)
	return nil
}

// ReplicateFileToReplicas sends a CREATE file to all replica nodes
func (cs *CoordinatorServer) ReplicateFileToReplicas(hydfsFileName string, localFilePath string) error {
	replicas := cs.hashSystem.GetReplicaNodes(hydfsFileName)
	if len(replicas) < 1 {
		return fmt.Errorf("no replica nodes in hash ring")
	}

	replicaNodeIDs := []string{}
	for i, replicaNodeID := range replicas {
		if i == 0 {
			continue // Skip owner
		}
		replicaNodeIDs = append(replicaNodeIDs, replicaNodeID)
	}

	cs.logger(">>> REPLICATING FILE '%s' TO VMs: %v <<<", hydfsFileName, replicaNodeIDs)

	cs.logger("Starting replication: %s → %d nodes", hydfsFileName, len(replicas))

	// Dont wait
	for i, replicaNodeID := range replicas {
		if i == 0 {
			continue // Skip owner
		}

		go func(nodeID string) {
			err := cs.sendFileToReplica(nodeID, hydfsFileName, localFilePath)
			if err != nil {
				cs.logger("Replication failed to %s: %v", nodeID, err)
				// TODO: Add retry logic when we do fault tolerance
			} else {
				cs.logger("Replicated successfully to %s", nodeID)
			}
		}(replicaNodeID)
	}

	// Return immediately without waiting
	return nil
}

// sendFileToReplica sends a file to a single replica node via gRPC
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
func (cs *CoordinatorServer) forwardCreateToOwner(ownerNodeID string, hydfsFileName string, localFileName string) error {
	cs.logger("Forwarding CREATE to owner %s: %s", ownerNodeID, hydfsFileName)

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
// Flow: Determine Owner → Route (Owner: Queue + Replicate | Non-Owner: Forward)
func (cs *CoordinatorServer) AppendFile(hydfsFileName string, localFileName string, clientID string) error {
	cs.logger("=== APPEND INITIATED: %s (from %s) ===", hydfsFileName, clientID)

	// Step 1: Determine ownership using consistent hashing
	ownerNodeID, err := cs.determineFileOwner(hydfsFileName)
	if err != nil {
		return err
	}

	// Step 2: Route based on ownership
	if cs.isCurrentNode(ownerNodeID) {
		cs.logger("This node IS owner → Handle locally")
		return cs.handleAppendAsOwner(hydfsFileName, localFileName, clientID)
	}

	cs.logger("This node is NOT owner → Forward to %s", ownerNodeID)
	return cs.forwardAppendToOwner(ownerNodeID, hydfsFileName, localFileName)
}

// handleAppendAsOwner handles APPEND when this node is the owner
// Flow: Queue Locally → Replicate to Others
func (cs *CoordinatorServer) handleAppendAsOwner(hydfsFileName string, localFileName string, clientID string) error {
	cs.logger("Acting as OWNER for APPEND: %s", hydfsFileName)

	selfNodeID := membership.StringifyNodeID(cs.nodeID)

	// Step 1: Queue append locally (asynchronous for ordering)
	opID, err := cs.appendFileLocally(hydfsFileName, localFileName, clientID, selfNodeID)
	if err != nil {
		return fmt.Errorf("local append failed: %v", err)
	}

	cs.logger("Append queued locally with opID: %d", opID)

	// Step 2: Replicate append to replica nodes
	// Use localFileName directly - it contains the append data (either from local client or forwarded temp file)
	// The worker will create a separate temp file for the converger, but for replication we use the original
	if err := cs.ReplicateAppendToReplicas(hydfsFileName, localFileName, opID); err != nil {
		cs.logger("WARNING: Append replication incomplete: %v", err)
		// Don't fail - append is queued locally
	}

	cs.logger("APPEND completed successfully: %s (opID=%d)", hydfsFileName, opID)
	return nil
}

// forwardAppendToOwner forwards an APPEND request to the designated owner node
func (cs *CoordinatorServer) forwardAppendToOwner(ownerNodeID string, hydfsFileName string, localFileName string) error {
	cs.logger("Forwarding APPEND to owner %s: %s", ownerNodeID, hydfsFileName)

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

// SendAppendToVM sends an append command to a specific VM address
// VM address can be in format "ip:port" or full nodeID "ip:port#timestamp"
func (cs *CoordinatorServer) SendAppendToVM(vmAddress string, hydfsFileName string, localFileName string) error {
	// Parse VM address - could be "ip:port" or "ip:port#timestamp"
	var fileTransferAddr string

	// Check if it's a full nodeID (contains #)
	if strings.Contains(vmAddress, "#") {
		// Use existing method to convert nodeID to file transfer address
		var err error
		fileTransferAddr, err = cs.getNodeFileTransferAddr(vmAddress)
		if err != nil {
			return fmt.Errorf("invalid nodeID format: %v", err)
		}
	} else {
		// Parse as "ip:port" and convert to file transfer address (port + 1000)
		parts := strings.Split(vmAddress, ":")
		if len(parts) != 2 {
			return fmt.Errorf("invalid VM address format: %s (expected ip:port)", vmAddress)
		}

		ip := parts[0]
		port, err := strconv.Atoi(parts[1])
		if err != nil {
			return fmt.Errorf("invalid port in VM address: %v", err)
		}

		// File transfer port is UDP port + 1000
		fileTransferPort := port + 1000
		fileTransferAddr = fmt.Sprintf("%s:%d", ip, fileTransferPort)
	}

	cs.logger("Sending append to VM %s (file transfer: %s): %s", vmAddress, fileTransferAddr, hydfsFileName)

	// Note: localFileName should exist on the remote VM, not this node
	// The remote VM will check for file existence

	// Create request
	selfNodeID := membership.StringifyNodeID(cs.nodeID)
	req := network.FileTransferRequest{
		HydfsFilename: hydfsFileName,
		LocalFilename: localFileName,
		OperationType: utils.Append,
		SenderID:      selfNodeID,
	}

	// Send append to VM via gRPC
	ctx := context.Background()
	resp, err := cs.networkClient.SendFile(ctx, fileTransferAddr, req)
	if err != nil {
		return fmt.Errorf("failed to send append to VM %s: %v", vmAddress, err)
	}

	if !resp.Success {
		return fmt.Errorf("VM %s returned error: %s", vmAddress, resp.Message)
	}

	cs.logger("Append sent successfully to VM %s", vmAddress)
	return nil
}

// appendFileLocally queues an append operation for local processing
// Returns the assigned operation ID
func (cs *CoordinatorServer) appendFileLocally(hydfsFileName string, localFileName string, clientID string, nodeID string) (int, error) {
	cs.logger("Queueing append: %s", hydfsFileName)

	// Step 1: Verify file exists
	metadata := cs.fileServer.GetFileMetadata(hydfsFileName)
	if metadata == nil {
		return 0, fmt.Errorf("file not found: %s", hydfsFileName)
	}

	// Step 2: Get next sequential operation ID (atomically)
	opID, err := cs.fileServer.IncrementAndGetOperationID(hydfsFileName)
	if err != nil {
		return 0, fmt.Errorf("failed to get operation ID: %v", err)
	}

	cs.logger("Assigned opID %d for append: %s", opID, hydfsFileName)

	// Step 3: Create file request for queue
	fileReq := &utils.FileRequest{
		OperationType:     utils.Append,
		FileName:          hydfsFileName,
		LocalFilePath:     localFileName,
		ClientID:          clientID,
		FileOperationID:   opID,
		OwnerNodeID:       nodeID,
		DestinationNodeID: nodeID,
		SourceNodeID:      nodeID,
	}

	// Step 4: Submit to queue (processed by converger)
	if err := cs.fileServer.SubmitRequest(fileReq); err != nil {
		return 0, fmt.Errorf("queue submission failed: %v", err)
	}

	cs.logger("Append queued: %s (opID=%d)", hydfsFileName, opID)
	return opID, nil
}

// ReplicateAppendToReplicas sends an APPEND operation to all replica nodes
func (cs *CoordinatorServer) ReplicateAppendToReplicas(hydfsFileName string, localFilePath string, operationID int) error {
	replicas := cs.hashSystem.GetReplicaNodes(hydfsFileName)
	if len(replicas) < 1 {
		return fmt.Errorf("no replica nodes in hash ring")
	}

	replicaNodeIDs := []string{}
	for i, replicaNodeID := range replicas {
		if i == 0 {
			continue // Skip owner
		}
		replicaNodeIDs = append(replicaNodeIDs, replicaNodeID)
	}

	cs.logger(">>> REPLICATING APPEND '%s' (opID=%d) TO VMs: %v <<<",
		hydfsFileName, operationID, replicaNodeIDs)

	// Dont wait
	for i, replicaNodeID := range replicas {
		if i == 0 {
			continue // Skip owner
		}

		go func(nodeID string, opID int) {
			err := cs.sendAppendToReplica(nodeID, hydfsFileName, localFilePath, opID)
			if err != nil {
				cs.logger("Append replication failed to %s: %v", nodeID, err)
				// TODO: Add retry logic when we do fault tolerance
			} else {
				cs.logger("Append replicated successfully to %s", nodeID)
			}
		}(replicaNodeID, operationID)
	}

	// Return immediately without waiting
	return nil
}

// sendAppendToReplica sends an append operation to a single replica node via gRPC
func (cs *CoordinatorServer) sendAppendToReplica(replicaNodeID string, hydfsFileName string, localFilePath string, operationID int) error {
	cs.logger("Sending append to %s: %s (opID=%d)", replicaNodeID, hydfsFileName, operationID)

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

// GetFile retrieves a file from HyDFS (parallelized for speed)
func (cs *CoordinatorServer) GetFile(filename string, clientID string) ([]byte, error) {
	cs.logger("GET operation initiated - file: %s, client: %s", filename, clientID)

	// Get all replica nodes (owner + replicas)
	allNodes := cs.hashSystem.GetReplicaNodes(filename)
	if len(allNodes) == 0 {
		return nil, fmt.Errorf("no replicas found - hash ring may be empty")
	}

	selfNodeID := membership.StringifyNodeID(cs.nodeID)

	// Try all nodes in parallel (owner + replicas)
	type getResult struct {
		data   []byte
		nodeID string
		err    error
	}

	resultChan := make(chan getResult, len(allNodes))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var wg sync.WaitGroup

	// Launch parallel GET requests to all replicas
	for _, nodeID := range allNodes {
		wg.Add(1)
		go func(nid string) {
			defer wg.Done()

			// If this is the current node, read locally
			if nid == selfNodeID {
				data, err := cs.fileServer.ReadFile(filename)
				resultChan <- getResult{data: data, nodeID: nid, err: err}
				return
			}

			// Remote node - fetch via gRPC
			addr, err := cs.getNodeFileTransferAddr(nid)
			if err != nil {
				resultChan <- getResult{data: nil, nodeID: nid, err: err}
				return
			}

			req := network.FileRequest{
				OperationType: utils.Get,
				FileName:      filename,
				ClientID:      clientID,
			}

			resp, err := cs.networkClient.GetFile(ctx, addr, req)
			if err != nil || !resp.Success || len(resp.Data) == 0 {
				resultChan <- getResult{data: nil, nodeID: nid, err: fmt.Errorf("fetch failed")}
				return
			}

			resultChan <- getResult{data: resp.Data, nodeID: nid, err: nil}
		}(nodeID)
	}

	// Close channel when all requests complete
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Return first successful result
	var lastErr error
	for result := range resultChan {
		if result.err == nil && len(result.data) > 0 {
			cs.logger("File %s retrieved from node %s", filename, result.nodeID)
			cancel() // Cancel remaining requests
			return result.data, nil
		}
		cs.logger("Failed to get from %s: %v", result.nodeID, result.err)
		lastErr = result.err
	}

	return nil, fmt.Errorf("file %s not found on any replica: %v", filename, lastErr)
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

	// File transfer gRPC port is UDP port + 1000
	fileTransferPort := port + 1000
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

	// Get files from local node's FileServer (owned + replicated)
	localFiles := cs.fileServer.ListStoredFiles()
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

// MergeFile ensures all replicas have converged and are identical
func (cs *CoordinatorServer) MergeFile(filename string, clientID string) error {
	cs.logger("=== MERGE STARTED: %s ===", filename)

	// Step 1: Get all replica nodes (owner + 2 replicas)
	allNodes := cs.hashSystem.GetReplicaNodes(filename)
	if len(allNodes) == 0 {
		return fmt.Errorf("no replicas found for file: %s", filename)
	}
	cs.logger("Checking %d replicas: %v", len(allNodes), allNodes)

	// Step 2: Query all replicas in parallel for state
	replicaStates, maxOpID := cs.queryAllReplicasParallel(filename, allNodes)
	if len(replicaStates) == 0 {
		return fmt.Errorf("merge failed: could not query any replica")
	}
	cs.logger("Target convergence: opID=%d", maxOpID)

	// Step 3: Wait for all replicas to converge
	converged := cs.waitForConvergenceParallel(filename, allNodes, maxOpID, 15*time.Second)
	if !converged {
		return fmt.Errorf("merge timeout: replicas did not converge")
	}

	// Step 4: Verify all replicas are identical
	if err := cs.verifyReplicaConsistencyParallel(filename, allNodes); err != nil {
		return fmt.Errorf("merge failed: %v", err)
	}

	cs.logger("=== MERGE COMPLETED: %s (all replicas consistent at opID=%d) ===", filename, maxOpID)
	return nil
}

// ReplicaState represents the state of a replica
type ReplicaState struct {
	LastOperationId int
	PendingOps      int
	FileHash        string
	FileSize        int64
}

// queryAllReplicasParallel queries all replicas concurrently
func (cs *CoordinatorServer) queryAllReplicasParallel(fileName string, nodes []string) (map[string]*ReplicaState, int) {
	cs.logger("Querying %d replicas in parallel...", len(nodes))

	type queryResult struct {
		nodeID string
		state  *ReplicaState
		err    error
	}

	resultChan := make(chan queryResult, len(nodes))
	var wg sync.WaitGroup

	// Launch parallel queries
	for _, nodeID := range nodes {
		wg.Add(1)
		go func(nid string) {
			defer wg.Done()
			state, err := cs.queryReplicaState(nid, fileName)
			resultChan <- queryResult{nodeID: nid, state: state, err: err}
		}(nodeID)
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Collect results
	replicaStates := make(map[string]*ReplicaState)
	maxOpID := 0

	for result := range resultChan {
		if result.err != nil {
			cs.logger("WARNING: Failed to query %s: %v", result.nodeID, result.err)
			continue
		}

		replicaStates[result.nodeID] = result.state
		if result.state.LastOperationId > maxOpID {
			maxOpID = result.state.LastOperationId
		}

		cs.logger("Node %s: opID=%d, pending=%d, hash=%s",
			result.nodeID, result.state.LastOperationId,
			result.state.PendingOps, result.state.FileHash[:8])
	}

	return replicaStates, maxOpID
}

// queryReplicaState queries a single replica for its current state
func (cs *CoordinatorServer) queryReplicaState(nodeID string, fileName string) (*ReplicaState, error) {
	if nodeID == membership.StringifyNodeID(cs.nodeID) {
		return cs.getLocalReplicaState(fileName)
	}

	addr, err := cs.getNodeCoordinationAddr(nodeID)
	if err != nil {
		return nil, fmt.Errorf("failed to get address for %s: %v", nodeID, err)
	}

	return cs.queryRemoteReplicaState(addr, fileName)
}

// getLocalReplicaState gets state from local fileServer
func (cs *CoordinatorServer) getLocalReplicaState(fileName string) (*ReplicaState, error) {
	metadata := cs.fileServer.GetFileMetadata(fileName)
	if metadata == nil {
		return nil, fmt.Errorf("file not found locally: %s", fileName)
	}

	fileData, err := cs.fileServer.ReadFile(fileName)
	if err != nil {
		return nil, fmt.Errorf("cannot read local file: %v", err)
	}

	fileHash := utils.ComputeDataHash(fileData)

	return &ReplicaState{
		LastOperationId: metadata.LastOperationId,
		PendingOps:      metadata.PendingOperations.Len(),
		FileHash:        fileHash,
		FileSize:        int64(len(fileData)),
	}, nil
}

// queryRemoteReplicaState queries remote replica via gRPC
func (cs *CoordinatorServer) queryRemoteReplicaState(address string, fileName string) (*ReplicaState, error) {
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithTimeout(5*time.Second))
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %v", err)
	}
	defer conn.Close()

	client := coordination.NewCoordinationServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.GetReplicaState(ctx, &coordination.GetReplicaStateRequest{
		Filename:         fileName,
		RequestingNodeId: membership.StringifyNodeID(cs.nodeID),
	})
	if err != nil {
		return nil, fmt.Errorf("gRPC call failed: %v", err)
	}

	return &ReplicaState{
		LastOperationId: int(resp.LastOperationId),
		PendingOps:      int(resp.PendingOps),
		FileHash:        resp.FileHash,
		FileSize:        resp.FileSize,
	}, nil
}

// waitForConvergenceParallel waits for all replicas to reach target opID
func (cs *CoordinatorServer) waitForConvergenceParallel(fileName string, nodes []string, targetOpID int, timeout time.Duration) bool {
	cs.logger("Waiting for convergence to opID=%d...", targetOpID)

	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for time.Now().Before(deadline) {
		<-ticker.C

		if cs.checkAllReplicasConvergedParallel(fileName, nodes, targetOpID) {
			cs.logger("All replicas converged to opID=%d!", targetOpID)
			return true
		}
	}

	cs.logger("Convergence timeout after %v!", timeout)
	return false
}

// checkAllReplicasConvergedParallel checks if all replicas have converged
func (cs *CoordinatorServer) checkAllReplicasConvergedParallel(fileName string, nodes []string, targetOpID int) bool {
	type convergenceResult struct {
		nodeID    string
		converged bool
		state     *ReplicaState
		err       error
	}

	resultChan := make(chan convergenceResult, len(nodes))
	var wg sync.WaitGroup

	for _, nodeID := range nodes {
		wg.Add(1)
		go func(nid string) {
			defer wg.Done()

			state, err := cs.queryReplicaState(nid, fileName)
			if err != nil {
				resultChan <- convergenceResult{nodeID: nid, converged: false, err: err}
				return
			}

			converged := state.LastOperationId >= targetOpID && state.PendingOps == 0
			resultChan <- convergenceResult{nodeID: nid, converged: converged, state: state}
		}(nodeID)
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	allConverged := true
	for result := range resultChan {
		if result.err != nil {
			cs.logger("Check failed for %s: %v", result.nodeID, result.err)
			allConverged = false
			continue
		}

		if !result.converged {
			cs.logger("Waiting for %s: opID=%d/%d, pending=%d",
				result.nodeID, result.state.LastOperationId,
				targetOpID, result.state.PendingOps)
			allConverged = false
		}
	}

	return allConverged
}

// verifyReplicaConsistencyParallel verifies all replicas are identical
func (cs *CoordinatorServer) verifyReplicaConsistencyParallel(fileName string, nodes []string) error {
	cs.logger("Verifying replica consistency in parallel...")

	states, _ := cs.queryAllReplicasParallel(fileName, nodes)

	if len(states) != len(nodes) {
		return fmt.Errorf("could not verify all replicas: got %d/%d", len(states), len(nodes))
	}

	// Pick first node as reference
	var referenceHash string
	var referenceOpID int
	var referenceSize int64
	var referenceNode string

	for nodeID, state := range states {
		referenceNode = nodeID
		referenceHash = state.FileHash
		referenceOpID = state.LastOperationId
		referenceSize = state.FileSize
		break
	}

	cs.logger("Reference (%s): hash=%s, opID=%d, size=%d",
		referenceNode, referenceHash[:8], referenceOpID, referenceSize)

	// Compare all other nodes to reference
	for nodeID, state := range states {
		if nodeID == referenceNode {
			continue
		}

		if state.FileHash != referenceHash {
			return fmt.Errorf("HASH MISMATCH at %s: expected=%s, got=%s",
				nodeID, referenceHash[:8], state.FileHash[:8])
		}
		if state.LastOperationId != referenceOpID {
			return fmt.Errorf("OPID MISMATCH at %s: expected=%d, got=%d",
				nodeID, referenceOpID, state.LastOperationId)
		}
		if state.FileSize != referenceSize {
			return fmt.Errorf("SIZE MISMATCH at %s: expected=%d, got=%d",
				nodeID, referenceSize, state.FileSize)
		}

		cs.logger("Node %s: CONSISTENT ✓", nodeID)
	}

	cs.logger("All replicas verified identical!")
	return nil
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
	// Small delay to ensure this runs after the membership update completes
	time.Sleep(10 * time.Millisecond)

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
