package coordinator

import (
	"context"
	"fmt"
	"testing"
	"time"

	"hydfs/pkg/membership"
	mpb "hydfs/protoBuilds/membership"
)

// TestCoordinatorServerCreation tests creating a new coordinator server
func TestCoordinatorServerCreation(t *testing.T) {
	logger := func(format string, args ...interface{}) {
		t.Logf(format, args...)
	}

	ip := "127.0.0.1"
	port := 9001

	cs, err := NewCoordinatorServer(ip, port, logger)
	if err != nil {
		t.Fatalf("Failed to create coordinator server: %v", err)
	}

	if cs.nodeID == nil {
		t.Error("Node ID should not be nil")
	}

	if cs.membershipTable == nil {
		t.Error("Membership table should not be nil")
	}

	if cs.hashSystem == nil {
		t.Error("Hash system should not be nil")
	}

	if cs.protocol == nil {
		t.Error("Protocol should not be nil")
	}

	if cs.udp == nil {
		t.Error("UDP transport should not be nil")
	}

	// Verify node ID format
	nodeIDStr := membership.StringifyNodeID(cs.nodeID)
	if nodeIDStr == "" {
		t.Error("Node ID string should not be empty")
	}

	t.Logf("Created coordinator with node ID: %s", nodeIDStr)
}

// TestCoordinatorServerStartStop tests starting and stopping the coordinator
func TestCoordinatorServerStartStop(t *testing.T) {
	logger := func(format string, args ...interface{}) {
		t.Logf(format, args...)
	}

	cs, err := NewCoordinatorServer("127.0.0.1", 9002, logger)
	if err != nil {
		t.Fatalf("Failed to create coordinator server: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Test start
	if err := cs.Start(ctx); err != nil {
		t.Fatalf("Failed to start coordinator server: %v", err)
	}

	if !cs.isRunning {
		t.Error("Coordinator should be running after start")
	}

	// Give it a moment to start up
	time.Sleep(100 * time.Millisecond)

	// Test stop
	cs.Stop()
	if cs.isRunning {
		t.Error("Coordinator should not be running after stop")
	}
}

// TestMembershipTableIntegration tests membership table integration
func TestMembershipTableIntegration(t *testing.T) {
	logger := func(format string, args ...interface{}) {
		t.Logf(format, args...)
	}

	cs, err := NewCoordinatorServer("127.0.0.1", 9003, logger)
	if err != nil {
		t.Fatalf("Failed to create coordinator server: %v", err)
	}

	// Test getting membership table
	table := cs.GetMembershipTable()
	if table == nil {
		t.Error("Membership table should not be nil")
	}

	// Test getting self node
	selfNode := table.GetSelf()
	if selfNode == nil {
		t.Error("Self node should not be nil")
	}

	// Test getting members (should include self)
	members := table.GetMembers()
	if len(members) == 0 {
		t.Error("Should have at least self as member")
	}

	// Verify self is in membership
	found := false
	selfStr := membership.StringifyNodeID(selfNode)
	for _, member := range members {
		memberStr := membership.StringifyNodeID(member.NodeID)
		if memberStr == selfStr {
			found = true
			break
		}
	}

	if !found {
		t.Error("Self node should be in membership list")
	}
}

// TestHashSystemIntegration tests hash system integration
func TestHashSystemIntegration(t *testing.T) {
	logger := func(format string, args ...interface{}) {
		t.Logf(format, args...)
	}

	cs, err := NewCoordinatorServer("127.0.0.1", 9004, logger)
	if err != nil {
		t.Fatalf("Failed to create coordinator server: %v", err)
	}

	hashSystem := cs.GetHashSystem()
	if hashSystem == nil {
		t.Error("Hash system should not be nil")
	}

	// Verify self was added to hash ring
	allNodes := hashSystem.GetAllNodes()
	if len(allNodes) == 0 {
		t.Error("Hash ring should contain at least self")
	}

	selfStr := membership.StringifyNodeID(cs.nodeID)
	if _, exists := allNodes[selfStr]; !exists {
		t.Error("Self node should be in hash ring")
	}

	// Test file location computation
	filename := "test.txt"
	location := hashSystem.ComputeLocation(filename)
	if location == "" {
		t.Error("File location should not be empty")
	}

	if location != selfStr {
		t.Errorf("With only self in ring, file should map to self. Got: %s, Expected: %s", location, selfStr)
	}
}

// TestHashRingUpdates tests that hash ring updates with membership changes
func TestHashRingUpdates(t *testing.T) {
	logger := func(format string, args ...interface{}) {
		t.Logf(format, args...)
	}

	cs, err := NewCoordinatorServer("127.0.0.1", 9005, logger)
	if err != nil {
		t.Fatalf("Failed to create coordinator server: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := cs.Start(ctx); err != nil {
		t.Fatalf("Failed to start coordinator: %v", err)
	}
	defer cs.Stop()

	// Initially should have only self
	initialNodes := cs.hashSystem.GetAllNodes()
	if len(initialNodes) != 1 {
		t.Errorf("Expected 1 node initially, got %d", len(initialNodes))
	}

	// Simulate adding a new member to membership table
	newNodeID, err := membership.NewNodeID("127.0.0.1", 9006, uint64(time.Now().UnixMilli()))
	if err != nil {
		t.Fatalf("Failed to create new node ID: %v", err)
	}

	// Simulate membership update (in real scenario this would come via network)
	entry := &mpb.MembershipEntry{
		Node:         newNodeID,
		State:        mpb.MemberState_ALIVE,
		Incarnation:  newNodeID.GetIncarnation(),
		LastUpdateMs: uint64(time.Now().UnixMilli()),
	}

	// Add to membership table
	cs.membershipTable.ApplyUpdate(entry)

	// Manually trigger hash ring update (in real scenario this happens in background)
	cs.updateHashRing()

	// Verify hash ring was updated
	updatedNodes := cs.hashSystem.GetAllNodes()
	if len(updatedNodes) != 2 {
		t.Errorf("Expected 2 nodes after update, got %d", len(updatedNodes))
	}

	// Verify both nodes are in the ring
	selfStr := membership.StringifyNodeID(cs.nodeID)
	newNodeStr := membership.StringifyNodeID(newNodeID)

	if _, exists := updatedNodes[selfStr]; !exists {
		t.Error("Self node should still be in hash ring")
	}

	if _, exists := updatedNodes[newNodeStr]; !exists {
		t.Error("New node should be added to hash ring")
	}
}

// TestReplicaCalculation tests replica node calculation
func TestReplicaCalculation(t *testing.T) {
	logger := func(format string, args ...interface{}) {
		t.Logf(format, args...)
	}

	cs, err := NewCoordinatorServer("127.0.0.1", 9006, logger)
	if err != nil {
		t.Fatalf("Failed to create coordinator server: %v", err)
	}

	// Add more nodes to test replication
	nodeIDs := []*mpb.NodeID{}
	for i := 1; i <= 5; i++ {
		nodeID, err := membership.NewNodeID("127.0.0.1", uint32(9000+i), uint64(time.Now().UnixMilli()+int64(i)))
		if err != nil {
			t.Fatalf("Failed to create node ID %d: %v", i, err)
		}
		nodeIDs = append(nodeIDs, nodeID)

		// Add to hash system
		cs.hashSystem.AddNode(membership.StringifyNodeID(nodeID))
	}

	// Test replica selection
	filename := "test.txt"
	replicas := cs.hashSystem.GetReplicaNodes(filename)

	// Should get 3 replicas (our configured replication factor)
	if len(replicas) != 3 {
		t.Errorf("Expected 3 replicas, got %d", len(replicas))
	}

	// Verify all replicas are different
	seen := make(map[string]bool)
	for _, replica := range replicas {
		if seen[replica] {
			t.Errorf("Duplicate replica: %s", replica)
		}
		seen[replica] = true
	}

	// Test that same file always gets same replicas
	replicas2 := cs.hashSystem.GetReplicaNodes(filename)
	if len(replicas2) != len(replicas) {
		t.Error("Replica count not consistent")
	}

	for i, replica := range replicas {
		if replicas2[i] != replica {
			t.Errorf("Replica %d not consistent: %s vs %s", i, replica, replicas2[i])
		}
	}
}

// TestProtocolConfiguration tests protocol configuration
func TestProtocolConfiguration(t *testing.T) {
	logger := func(format string, args ...interface{}) {
		t.Logf(format, args...)
	}

	cs, err := NewCoordinatorServer("127.0.0.1", 9007, logger)
	if err != nil {
		t.Fatalf("Failed to create coordinator server: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := cs.Start(ctx); err != nil {
		t.Fatalf("Failed to start coordinator: %v", err)
	}
	defer cs.Stop()

	// Test default protocol settings
	if cs.protocol.Mode() != "ping" {
		t.Errorf("Expected ping mode, got %s", cs.protocol.Mode())
	}

	if cs.protocol.SuspicionOn() {
		t.Error("Suspicion should be off by default")
	}

	// Test changing protocol settings
	cs.protocol.SetMode("gossip")
	if cs.protocol.Mode() != "gossip" {
		t.Errorf("Expected gossip mode after change, got %s", cs.protocol.Mode())
	}

	cs.protocol.SetSuspicion(true)
	if !cs.protocol.SuspicionOn() {
		t.Error("Suspicion should be on after enabling")
	}
}

// TestConcurrentAccess tests concurrent access to coordinator components
func TestConcurrentAccess(t *testing.T) {
	logger := func(format string, args ...interface{}) {
		t.Logf(format, args...)
	}

	cs, err := NewCoordinatorServer("127.0.0.1", 9008, logger)
	if err != nil {
		t.Fatalf("Failed to create coordinator server: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := cs.Start(ctx); err != nil {
		t.Fatalf("Failed to start coordinator: %v", err)
	}
	defer cs.Stop()

	// Test concurrent access to membership table
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func(id int) {
			defer func() { done <- true }()

			// Access membership table
			members := cs.GetMembershipTable().GetMembers()
			if len(members) == 0 {
				t.Errorf("Goroutine %d: No members found", id)
				return
			}

			// Access hash system
			nodes := cs.GetHashSystem().GetAllNodes()
			if len(nodes) == 0 {
				t.Errorf("Goroutine %d: No nodes in hash ring", id)
				return
			}

			// Compute file locations
			for j := 0; j < 10; j++ {
				filename := fmt.Sprintf("file_%d_%d.txt", id, j)
				location := cs.GetHashSystem().ComputeLocation(filename)
				if location == "" {
					t.Errorf("Goroutine %d: Empty location for file %s", id, filename)
					return
				}
			}
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		<-done
	}
}

// TestBackgroundTasks tests that background tasks run properly
func TestBackgroundTasks(t *testing.T) {
	logger := func(format string, args ...interface{}) {
		t.Logf(format, args...)
	}

	cs, err := NewCoordinatorServer("127.0.0.1", 9009, logger)
	if err != nil {
		t.Fatalf("Failed to create coordinator server: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := cs.Start(ctx); err != nil {
		t.Fatalf("Failed to start coordinator: %v", err)
	}

	// Let background tasks run for a bit
	time.Sleep(1 * time.Second)

	// Background tasks should maintain the system state
	members := cs.GetMembershipTable().GetMembers()
	if len(members) == 0 {
		t.Error("Background tasks should maintain membership")
	}

	nodes := cs.GetHashSystem().GetAllNodes()
	if len(nodes) == 0 {
		t.Error("Background tasks should maintain hash ring")
	}

	cs.Stop()

	// Verify shutdown works properly
	if cs.isRunning {
		t.Error("Coordinator should not be running after stop")
	}
}
