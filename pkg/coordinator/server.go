package coordinator

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"hydfs/pkg/fileserver"
	"hydfs/pkg/membership"
	"hydfs/pkg/network"
	"hydfs/pkg/protocol"
	"hydfs/pkg/transport"
	"hydfs/pkg/utils"
	"hydfs/protoBuilds/fileservice"
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
func (cs *CoordinatorServer) CreateFile(hydfsFileName string, localFileName string, clientID string) error {
	cs.logger("CREATE operation initiated - file name on HyDFS: %s, local File Name: %d bytes, client: %s", hydfsFileName, localFileName, clientID)

	// Get the designated coordinator for this file
	coordinatorNodeID := cs.hashSystem.ComputeLocation(hydfsFileName)
	if coordinatorNodeID == "" {
		return fmt.Errorf("no coordinator found - hash ring may be empty")
	}

	selfNodeID := membership.StringifyNodeID(cs.nodeID)

	if coordinatorNodeID != selfNodeID {
		// Forward request to the designated coordinator
		cs.logger("Forwarding CREATE operation for file %s to coordinator %s", hydfsFileName, coordinatorNodeID)
		err := cs.sendToCoordinator(coordinatorNodeID, utils.Create, hydfsFileName, localFileName)
		if err == nil {
			cs.logger("CREATE operation forwarded successfully - file: %s", hydfsFileName)
		} else {
			cs.logger("CREATE operation forward failed - file: %s, error: %v", hydfsFileName, err)
		}
		return err
	}

	// This node itself is the coordinator - handle the operation directly
	cs.logger("Acting as coordinator for CREATE operation - file: %s", hydfsFileName)

	// Step 1: Save file locally to OwnedFiles/
	_, err := cs.performLocalOperation(utils.Create, hydfsFileName, localFileName, clientID, selfNodeID, selfNodeID, true)
	if err != nil {
		cs.logger("Failed to save file locally: %v", err)
		return fmt.Errorf("failed to save file locally: %v", err)
	}

	// Step 2: Create replica request
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

	cs.logger("CREATE operation completed - file: %s, replicas: %d/%d", hydfsFileName, successCount, len(replicas))
	return nil
}

// AppendFile handles distributed file append
// Requires that the file already exists in HyDFS
func (cs *CoordinatorServer) AppendFile(hydfsFileName string, localFileName string, clientID string) error {
	cs.logger("APPEND operation initiated - file: %s, local file: %s, client: %s", hydfsFileName, localFileName, clientID)

	// Get the designated coordinator for this file (append should go to same coordinator as create)
	coordinatorNodeID := cs.hashSystem.ComputeLocation(hydfsFileName)
	if coordinatorNodeID == "" {
		return fmt.Errorf("no coordinator found - hash ring may be empty")
	}

	selfNodeID := membership.StringifyNodeID(cs.nodeID)

	if coordinatorNodeID != selfNodeID {
		// Forward request to the designated coordinator
		cs.logger("Forwarding APPEND operation for file %s to coordinator %s", hydfsFileName, coordinatorNodeID)
		err := cs.sendToCoordinator(coordinatorNodeID, utils.Append, hydfsFileName, localFileName)
		if err == nil {
			cs.logger("APPEND operation forwarded successfully - file: %s", hydfsFileName)
		} else {
			cs.logger("APPEND operation forward failed - file: %s, error: %v", hydfsFileName, err)
		}
		return err
	}

	// This node is the coordinator - handle the operation
	cs.logger("Acting as coordinator for APPEND operation - file: %s", hydfsFileName)

	// Step 1: Append to local file (adds to PendingOperations)
	opID, err := cs.performLocalOperation(utils.Append, hydfsFileName, localFileName, clientID, selfNodeID, selfNodeID, true)
	if err != nil {
		cs.logger("Failed to append file locally: %v", err)
		return fmt.Errorf("failed to append file locally: %v", err)
	}

	// Step 2: Get replica nodes
	replicas := cs.hashSystem.GetReplicaNodes(hydfsFileName)
	if len(replicas) < 1 {
		cs.logger("WARNING: No replica nodes found in hash ring")
		return nil // File is appended locally, but no replicas
	}

	// Step 3: Send append to replicas (skip the first one, which is this node)
	successCount := 1 // Count this node as success
	for i, replicaNodeID := range replicas {
		if i == 0 {
			// First replica is the owner (this node), skip it
			continue
		}

		// Pass the correct operation ID
		err := cs.sendToReplicaWithOpID(replicaNodeID, utils.AppendReplica, hydfsFileName, localFileName, opID)
		if err != nil {
			cs.logger("Failed to replicate append to node %s: %v", replicaNodeID, err)
		} else {
			successCount++
			cs.logger("Successfully replicated append to node %s", replicaNodeID)
		}
	}

	cs.logger("APPEND operation completed - file: %s, replicas: %d/%d", hydfsFileName, successCount, len(replicas))
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

// sendToReplica sends a file to a replica node (for CREATE or APPEND operations)
func (cs *CoordinatorServer) sendToReplica(replicaNodeID string, opType utils.OperationType, hydfsFileName string, localFileName string) error {
	if opType == utils.CreateReplica {
		// For CREATE, operation ID is always 1
		return cs.sendToReplicaWithOpID(replicaNodeID, opType, hydfsFileName, localFileName, 1)
	}
	// For APPEND, should not use this method - use sendToReplicaWithOpID directly
	return fmt.Errorf("sendToReplica should not be used for APPEND operations")
}

// sendToReplicaWithOpID sends a file to a replica node with explicit operation ID
func (cs *CoordinatorServer) sendToReplicaWithOpID(replicaNodeID string, opType utils.OperationType, hydfsFileName string, localFileName string, operationID int) error {
	nodeAddr, err := cs.getNodeFileTransferAddr(replicaNodeID)
	if err != nil {
		return fmt.Errorf("failed to get address for replica %s: %v", replicaNodeID, err)
	}

	selfNodeID := membership.StringifyNodeID(cs.nodeID)

	// Determine replica index
	replicas := cs.hashSystem.GetReplicaNodes(hydfsFileName)
	replicaIndex := 1
	for i, nodeID := range replicas {
		if nodeID == replicaNodeID {
			replicaIndex = i
			break
		}
	}

	// Use the provided operation ID (not reading from metadata)
	req := network.ReplicaRequest{
		HydfsFilename: hydfsFileName,
		LocalFilename: localFileName,
		OperationType: opType,
		SenderID:      selfNodeID,
		OperationID:   operationID, // Use provided opID
		ReplicaIndex:  replicaIndex,
	}

	resp, err := cs.networkClient.SendReplica(context.Background(), nodeAddr, req)
	if err != nil {
		return fmt.Errorf("failed to send replica: %v", err)
	}

	if !resp.Success {
		return fmt.Errorf("replica returned error: %s", resp.Error)
	}

	return nil
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
