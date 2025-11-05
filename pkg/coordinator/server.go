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
func (cs *CoordinatorServer) CreateFile(filename string, data []byte, clientID string) error {
	cs.logger("CREATE operation initiated - file: %s, size: %d bytes, client: %s", filename, len(data), clientID)

	// Get the designated coordinator for this file
	coordinatorNodeID := cs.hashSystem.ComputeLocation(filename)
	if coordinatorNodeID == "" {
		return fmt.Errorf("no coordinator found - hash ring may be empty")
	}

	selfNodeID := membership.StringifyNodeID(cs.nodeID)

	if coordinatorNodeID != selfNodeID {
		// Forward request to the designated coordinator
		cs.logger("Forwarding CREATE operation for file %s to coordinator %s", filename, coordinatorNodeID)
		err := cs.forwardToCoordinator(coordinatorNodeID, utils.Create, filename, data, clientID)
		if err == nil {
			cs.logger("CREATE operation forwarded successfully - file: %s", filename)
		} else {
			cs.logger("CREATE operation forward failed - file: %s, error: %v", filename, err)
		}
		return err
	}

	// This node itself is the coordinator - handle the operation directly
	cs.logger("Acting as coordinator for CREATE operation - file: %s", filename)

	// TODO : transfer the file to owned files and then do same action as coordinator
	// returning temp error
	return nil
}

// TODO check if this is really required
func (cs *CoordinatorServer) ReplicateFile(filename string, data []byte, clientID string) error {
	// Get the designated replica nodes for this file based on consistent hashing
	// This returns the coordinator (owner) + 2 successor nodes, regardless of where the request came from
	replicas := cs.hashSystem.GetReplicaNodes(filename)
	if len(replicas) == 0 {
		return fmt.Errorf("no replica nodes found - hash ring may be empty")
	}

	//err := cs.coordinateFileOperationWithReplicas(utils.Create, filename, data, clientID, replicas)
	//if err == nil {
	//	cs.logger("CREATE operation completed successfully - file: %s", filename)
	//} else {
	//	cs.logger("CREATE operation failed - file: %s, error: %v", filename, err)
	//}
	return nil
}

// AppendFile handles distributed file append
// Requires that the file already exists in HyDFS
func (cs *CoordinatorServer) AppendFile(filename string, data []byte, clientID string) error {
	cs.logger("APPEND operation initiated - file: %s, size: %d bytes, client: %s", filename, len(data), clientID)

	// Get the designated coordinator for this file (append should go to same coordinator as create)
	coordinatorNodeID := cs.hashSystem.ComputeLocation(filename)
	if coordinatorNodeID == "" {
		return fmt.Errorf("no coordinator found - hash ring may be empty")
	}

	selfNodeID := membership.StringifyNodeID(cs.nodeID)

	if coordinatorNodeID != selfNodeID {
		// Forward request to the designated coordinator
		cs.logger("Forwarding APPEND operation for file %s to coordinator %s", filename, coordinatorNodeID)
		err := cs.forwardToCoordinator(coordinatorNodeID, utils.Append, filename, data, clientID)
		if err == nil {
			cs.logger("APPEND operation forwarded successfully - file: %s", filename)
		} else {
			cs.logger("APPEND operation forward failed - file: %s, error: %v", filename, err)
		}
		return err
	}

	// TODO : This node is the coordinator - handle the operation to append to owned files
	cs.logger("Acting as coordinator for APPEND operation - file: %s", filename)

	return nil
}

// GetFile handles distributed file retrieval
// Tries hash ring replicas first, then falls back to querying all nodes if not found
func (cs *CoordinatorServer) GetFile(filename string, clientID string) ([]byte, error) {
	// First, try hash ring replicas (fast path)
	replicas := cs.hashSystem.GetReplicaNodes(filename)
	data, err := cs.queryNodesForFile(filename, replicas, clientID)
	if err == nil && data != nil {
		return data, nil
	}

	// If not found on hash ring replicas, query ALL nodes in cluster
	// This handles cases where hash ring changed after file was created
	cs.logger("File %s not found on hash ring replicas, querying all nodes", filename)
	members := cs.membershipTable.GetMembers()
	allNodeIDs := make([]string, 0, len(members))
	for _, member := range members {
		if member.State == mpb.MemberState_ALIVE {
			allNodeIDs = append(allNodeIDs, membership.StringifyNodeID(member.NodeID))
		}
	}

	return cs.queryNodesForFile(filename, allNodeIDs, clientID)
}

// performLocalOperation performs file operation on local node
func (cs *CoordinatorServer) performLocalOperation(opType utils.OperationType, filename string, data []byte, clientID string, ownerNodeID string, currentNodeID string, isOwner bool) error {
	// TODO : This function should transfer the file from local folder to owned files folder and then do the operation
	//req := &utils.FileRequest{
	//	OperationType:     opType,
	//	FileName:          filename,
	//	Data:              data,
	//	ClientID:          clientID,
	//	SourceNodeID:      currentNodeID, // This node is the source
	//	DestinationNodeID: currentNodeID, // This node is also the destination
	//	OwnerNodeID:       ownerNodeID,   // Primary owner node for this file
	//}
	//
	//return cs.fileServer.SubmitRequest(req)
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

// forwardToCoordinator forwards file operations to the designated coordinator node
func (cs *CoordinatorServer) forwardToCoordinator(coordinatorNodeID string, opType utils.OperationType, filename string, data []byte, clientID string) error {
	cs.logger("Forwarding %v operation for file %s to coordinator %s", opType, filename, coordinatorNodeID)

	nodeAddr, err := cs.getNodeFileTransferAddr(coordinatorNodeID)
	if err != nil {
		return fmt.Errorf("failed to get coordinator address: %v", err)
	}

	// Convert clientID string to int64 for network request
	clientIDInt := int64(cs.hashSystem.GetNodeID(clientID))

	req := network.FileRequest{
		OperationType: opType,
		FileName:      filename,
		Data:          data,
		ClientID:      fmt.Sprintf("%d", clientIDInt),
	}

	var resp *network.FileResponse
	ctx := context.Background()

	if opType == utils.Create || opType == utils.Append {
		resp, err = cs.networkClient.SendFileStream(ctx, nodeAddr, req, opType)
	} else {
		return fmt.Errorf("Invalid Operation Type")
	}

	if err != nil {
		return fmt.Errorf("coordinator request failed: %v", err)
	}

	if !resp.Success {
		return fmt.Errorf("coordinator operation failed: %s", resp.Message)
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
				err = cs.propagateFileToReplica(nodeID, filename, newestData, clientID, true)
				if err != nil {
					cs.logger("Failed to create file on replica %s: %v", nodeID, err)
				}
			} else if string(currentData) != string(newestData) {
				// Replica has different content, update it
				err = cs.propagateFileToReplica(nodeID, filename, newestData, clientID, false)
				if err != nil {
					cs.logger("Failed to update file on replica %s: %v", nodeID, err)
				}
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
