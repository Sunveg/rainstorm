package coordinator

import (
	"context"
	"fmt"
	"net"
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
	networkServer *network.Server
	networkClient *network.Client
	fileMetadata  map[string]*utils.FileMetaData

	// Server configuration
	nodeID      *mpb.NodeID
	httpPort    int
	storagePath string

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
	proto := protocol.NewProtocol(table, udp, logger, 3)

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

	// HTTP port is UDP port + 1000 for clear separation
	httpPort := port + 1000

	// Create network server for handling HTTP API
	networkServer := network.NewServer(
		fmt.Sprintf("%s:%d", ip, httpPort),
		fileServer,
		membership.StringifyNodeID(nodeID),
		logger,
	)

	// Create coordinator server
	cs := &CoordinatorServer{
		membershipTable: table,
		protocol:        proto,
		udp:             udp,
		hashSystem:      hashSystem,
		fileServer:      fileServer,
		networkServer:   networkServer,
		networkClient:   networkClient,
		fileMetadata:    make(map[string]*utils.FileMetaData),
		nodeID:          nodeID,
		httpPort:        httpPort,
		storagePath:     storagePath,
		logger:          logger,
		shutdownCh:      make(chan struct{}),
	}

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
	cs.logger("File operations available on HTTP port %d", cs.httpPort)
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

// Join joins the HyDFS cluster via an introducer
func (cs *CoordinatorServer) Join(introducerAddr string) error {
	// Parse introducer address
	host, portStr, err := net.SplitHostPort(introducerAddr)
	if err != nil {
		return fmt.Errorf("invalid introducer address: %v", err)
	}

	// Convert port to UDP address
	udpAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%s", host, portStr))
	if err != nil {
		return fmt.Errorf("failed to resolve introducer address: %v", err)
	}

	// Send join request via membership protocol
	ctx := context.Background()
	if err := cs.protocol.SendJoin(ctx, udpAddr); err != nil {
		return fmt.Errorf("failed to send join request: %v", err)
	}

	cs.logger("Sent join request to introducer %s", introducerAddr)
	return nil
}

// CreateFile handles distributed file creation
func (cs *CoordinatorServer) CreateFile(filename string, data []byte, clientID string) error {
	cs.logger("CREATE operation initiated - file: %s, size: %d bytes, client: %s", filename, len(data), clientID)
	err := cs.coordinateFileOperation(utils.Create, filename, data, clientID)
	if err == nil {
		cs.logger("CREATE operation completed successfully - file: %s", filename)
	} else {
		cs.logger("CREATE operation failed - file: %s, error: %v", filename, err)
	}
	return err
}

// AppendFile handles distributed file append
func (cs *CoordinatorServer) AppendFile(filename string, data []byte, clientID string) error {
	cs.logger("APPEND operation initiated - file: %s, size: %d bytes, client: %s", filename, len(data), clientID)
	err := cs.coordinateFileOperation(utils.Append, filename, data, clientID)
	if err == nil {
		cs.logger("APPEND operation completed successfully - file: %s", filename)
	} else {
		cs.logger("APPEND operation failed - file: %s, error: %v", filename, err)
	}
	return err
}

// GetFile handles distributed file retrieval
// OPTIMIZED: Tries all replicas in parallel, returns first successful response
func (cs *CoordinatorServer) GetFile(filename string, clientID string) ([]byte, error) {
	// Find replica nodes for this file
	replicas := cs.hashSystem.GetReplicaNodes(filename)

	var wg sync.WaitGroup
	resultChan := make(chan []byte, 1) // Buffered to allow first success
	errChan := make(chan error, len(replicas))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Launch parallel requests to all replicas
	for _, nodeID := range replicas {
		wg.Add(1)
		go func(nodeID string) {
			defer wg.Done()

			var data []byte
			var err error

			if nodeID == membership.StringifyNodeID(cs.nodeID) {
				// Local file - handle directly
				req := &utils.FileRequest{
					OperationType: utils.Get,
					FileName:      filename,
					ClientID:      clientID,
				}

				if err := cs.fileServer.SubmitRequest(req); err == nil {
					// For now, we need a better way to get response data
					// This is a limitation we'll address in the file operation handlers
					data = []byte("placeholder - need proper response handling")
				} else {
					err = fmt.Errorf("local file server error: %v", err)
				}
			} else {
				// Remote file - use network client
				nodeAddr, addrErr := cs.getNodeHTTPAddr(nodeID)
				if addrErr != nil {
					err = fmt.Errorf("failed to get address: %v", addrErr)
				} else {
					req := network.FileRequest{
						Type:     utils.Get,
						Filename: filename,
						ClientID: 1, // Convert string to int64 later
					}

					resp, reqErr := cs.networkClient.GetFile(ctx, nodeAddr, req)
					if reqErr == nil && resp.Success {
						data = resp.Data
					} else {
						err = fmt.Errorf("remote get failed: %v", reqErr)
					}
				}
			}

			if err == nil && data != nil {
				// First success wins
				select {
				case resultChan <- data:
					cancel() // Cancel other requests
				default:
					// Someone else already succeeded
				}
			} else {
				errChan <- err
			}
		}(nodeID)
	}

	// Wait for first successful result or all to fail
	go func() {
		wg.Wait()
		close(resultChan)
		close(errChan)
	}()

	// Return first successful result
	select {
	case data := <-resultChan:
		return data, nil
	case <-time.After(30 * time.Second):
		return nil, fmt.Errorf("timeout waiting for file %s", filename)
	}
}

// coordinateFileOperation coordinates create/append operations across replicas
// OPTIMIZED: Now performs replica operations in parallel using goroutines
func (cs *CoordinatorServer) coordinateFileOperation(opType utils.OperationType, filename string, data []byte, clientID string) error {
	// Find replica nodes for this file
	replicas := cs.hashSystem.GetReplicaNodes(filename)

	// First replica is the owner, rest are replicas
	var ownerNodeID string
	if len(replicas) > 0 {
		ownerNodeID = replicas[0]
	}
	selfNodeID := membership.StringifyNodeID(cs.nodeID)

	var successCount int64
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstError error

	// Launch all replica operations in parallel
	for _, nodeID := range replicas {
		wg.Add(1)
		go func(nodeID string) {
			defer wg.Done()

			var err error
			if nodeID == membership.StringifyNodeID(cs.nodeID) {
				// Local operation - determine if owner or replica
				isOwner := (nodeID == ownerNodeID)
				err = cs.performLocalOperation(opType, filename, data, clientID, ownerNodeID, nodeID, isOwner)
			} else {
				// Remote operation - determine if target is owner or replica
				isOwner := (nodeID == ownerNodeID)
				err = cs.performRemoteOperation(opType, filename, data, clientID, ownerNodeID, selfNodeID, nodeID, isOwner)
			}

			if err == nil {
				atomic.AddInt64(&successCount, 1)
			} else {
				mu.Lock()
				if firstError == nil {
					firstError = err
				}
				mu.Unlock()
				cs.logger("Operation failed on node %s: %v", nodeID, err)
			}
		}(nodeID)
	}

	// Wait for all operations to complete
	wg.Wait()

	count := int(atomic.LoadInt64(&successCount))
	// Require majority success (at least 2 out of 3 replicas)
	if count >= 2 {
		cs.logger("Operation %v on file %s succeeded on %d/%d replicas", opType, filename, count, len(replicas))
		return nil
	}

	return fmt.Errorf("operation failed: only %d/%d replicas succeeded", count, len(replicas))
}

// performLocalOperation performs file operation on local node
func (cs *CoordinatorServer) performLocalOperation(opType utils.OperationType, filename string, data []byte, clientID string, ownerNodeID string, currentNodeID string, isOwner bool) error {
	req := &utils.FileRequest{
		OperationType:     opType,
		FileName:          filename,
		Data:              data,
		ClientID:          clientID,
		SourceNodeID:      currentNodeID, // This node is the source
		DestinationNodeID: currentNodeID, // This node is also the destination
		OwnerNodeID:       ownerNodeID,   // Primary owner node for this file
	}

	return cs.fileServer.SubmitRequest(req)
}

// performRemoteOperation performs file operation on remote node
func (cs *CoordinatorServer) performRemoteOperation(opType utils.OperationType, filename string, data []byte, clientID string, ownerNodeID string, sourceNodeID string, destNodeID string, destIsOwner bool) error {
	nodeAddr, err := cs.getNodeHTTPAddr(destNodeID)
	if err != nil {
		return fmt.Errorf("failed to get address for node %s: %v", destNodeID, err)
	}

	req := network.FileRequest{
		Type:              opType,
		Filename:          filename,
		Data:              data,
		ClientID:          1,            // Convert string to int64 later
		SourceNodeID:      sourceNodeID, // Node sending the request
		DestinationNodeID: destNodeID,   // Node receiving the request (owner or replica)
		OwnerNodeID:       ownerNodeID,  // Primary owner node for this file
	}

	var resp *network.FileResponse
	ctx := context.Background()

	switch opType {
	case utils.Create:
		resp, err = cs.networkClient.CreateFile(ctx, nodeAddr, req)
	case utils.Append:
		resp, err = cs.networkClient.AppendFile(ctx, nodeAddr, req)
	default:
		return fmt.Errorf("unsupported operation type: %v", opType)
	}

	if err != nil {
		return err
	}

	if !resp.Success {
		return fmt.Errorf("remote operation failed: %s", resp.Error)
	}

	return nil
}

// getNodeHTTPAddr converts a node ID to HTTP address
func (cs *CoordinatorServer) getNodeHTTPAddr(nodeID string) (string, error) {
	ip, port, err := utils.ParseNodeID(nodeID)
	if err != nil {
		return "", err
	}

	// HTTP port is UDP port + 1000
	httpPort := port + 1000
	return fmt.Sprintf("%s:%d", ip, httpPort), nil
}

// ListFiles returns a list of all files in the distributed system
// OPTIMIZED: Queries all nodes in parallel instead of sequentially
func (cs *CoordinatorServer) ListFiles(clientID string) ([]string, error) {
	allFiles := make(map[string]bool)
	var mu sync.Mutex
	var wg sync.WaitGroup

	// Get files from local node
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

			nodeAddr, err := cs.getNodeHTTPAddr(nodeID)
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
				// Local replica
				metadata := cs.fileServer.GetFileMetadata(filename)
				if metadata != nil {
					// Read local file data
					if localData := cs.readLocalFileData(filename); localData != nil {
						data = localData
						version = int64(len(metadata.Operations))
					}
				}
			} else {
				// Remote replica
				nodeAddr, addrErr := cs.getNodeHTTPAddr(nodeID)
				if addrErr != nil {
					cs.logger("Failed to get address for node %s: %v", nodeID, addrErr)
					return
				}

				req := network.FileRequest{
					Type:     utils.Get,
					Filename: filename,
					ClientID: 1, // Convert later
				}

				resp, reqErr := cs.networkClient.GetFile(context.Background(), nodeAddr, req)
				if reqErr == nil && resp.Success {
					data = resp.Data
					version = resp.Version
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

// propagateFileToReplica sends file data to a specific replica
func (cs *CoordinatorServer) propagateFileToReplica(nodeID, filename string, data []byte, clientID string, isCreate bool) error {
	// Determine owner node for this file
	ownerNodeID := cs.hashSystem.ComputeLocation(filename)
	selfNodeID := membership.StringifyNodeID(cs.nodeID)
	isOwner := (nodeID == ownerNodeID)

	if nodeID == membership.StringifyNodeID(cs.nodeID) {
		// Local replica
		if isCreate {
			return cs.performLocalOperation(utils.Create, filename, data, clientID, ownerNodeID, nodeID, isOwner)
		} else {
			// For updates, we need to recreate the file with new content
			// This is a simplified approach - in a real system, you'd want more sophisticated versioning
			return cs.performLocalOperation(utils.Create, filename, data, clientID, ownerNodeID, nodeID, isOwner)
		}
	} else {
		// Remote replica
		nodeAddr, err := cs.getNodeHTTPAddr(nodeID)
		if err != nil {
			return err
		}

		// Use merge endpoint if available, otherwise use create
		req := network.MergeRequest{
			Filename: filename,
			ClientID: 1, // Convert later
		}

		resp, err := cs.networkClient.MergeFile(context.Background(), nodeAddr, req)
		if err != nil {
			// Fallback to create if merge not available
			if isCreate {
				return cs.performRemoteOperation(utils.Create, filename, data, clientID, ownerNodeID, selfNodeID, nodeID, isOwner)
			} else {
				return cs.performRemoteOperation(utils.Create, filename, data, clientID, ownerNodeID, selfNodeID, nodeID, isOwner)
			}
		}

		if !resp.Success {
			return fmt.Errorf("merge failed on remote node: %s", resp.Error)
		}

		return nil
	}
}

// readLocalFileData reads file data from local storage
func (cs *CoordinatorServer) readLocalFileData(filename string) []byte {
	// This is a simplified implementation
	// In practice, we'd need to coordinate with the file server to read actual file data
	metadata := cs.fileServer.GetFileMetadata(filename)
	if metadata == nil {
		return nil
	}

	// For now, return placeholder data
	// In a complete implementation, this would read from the actual file
	return []byte("placeholder file data - implement actual file reading")
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
			// Update hash ring with current membership
			cs.updateHashRing()

			// Perform membership cleanup
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
