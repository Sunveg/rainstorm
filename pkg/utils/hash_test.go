package utils

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
)

// TestHashSystemCreation tests creating a new hash system
func TestHashSystemCreation(t *testing.T) {
	replicas := 3
	hs := NewHashSystem(replicas)

	if hs.replicas != replicas {
		t.Errorf("Expected %d replicas, got %d", replicas, hs.replicas)
	}

	if len(hs.ring) != 0 {
		t.Errorf("Expected empty ring, got %d entries", len(hs.ring))
	}

	if len(hs.sortedHashes) != 0 {
		t.Errorf("Expected empty sorted hashes, got %d entries", len(hs.sortedHashes))
	}
}

// TestAddNode tests adding nodes to the hash ring
func TestAddNode(t *testing.T) {
	hs := NewHashSystem(3)

	// Add first node
	node1 := "192.168.1.1:8001"
	hs.AddNode(node1)

	if len(hs.ring) != 1 {
		t.Errorf("Expected 1 node in ring, got %d", len(hs.ring))
	}

	if len(hs.sortedHashes) != 1 {
		t.Errorf("Expected 1 sorted hash, got %d", len(hs.sortedHashes))
	}

	// Add second node
	node2 := "192.168.1.2:8001"
	hs.AddNode(node2)

	if len(hs.ring) != 2 {
		t.Errorf("Expected 2 nodes in ring, got %d", len(hs.ring))
	}

	if len(hs.sortedHashes) != 2 {
		t.Errorf("Expected 2 sorted hashes, got %d", len(hs.sortedHashes))
	}

	// Verify nodes are properly mapped
	hash1 := hs.hash(node1)
	hash2 := hs.hash(node2)

	if hs.ring[hash1] != node1 {
		t.Errorf("Expected node1 at hash %x, got %s", hash1, hs.ring[hash1])
	}

	if hs.ring[hash2] != node2 {
		t.Errorf("Expected node2 at hash %x, got %s", hash2, hs.ring[hash2])
	}
}

// TestRemoveNode tests removing nodes from the hash ring
func TestRemoveNode(t *testing.T) {
	hs := NewHashSystem(3)

	// Add nodes
	nodes := []string{"192.168.1.1:8001", "192.168.1.2:8001", "192.168.1.3:8001"}
	for _, node := range nodes {
		hs.AddNode(node)
	}

	if len(hs.ring) != 3 {
		t.Errorf("Expected 3 nodes, got %d", len(hs.ring))
	}

	// Remove middle node
	hs.RemoveNode(nodes[1])

	if len(hs.ring) != 2 {
		t.Errorf("Expected 2 nodes after removal, got %d", len(hs.ring))
	}

	if len(hs.sortedHashes) != 2 {
		t.Errorf("Expected 2 sorted hashes after removal, got %d", len(hs.sortedHashes))
	}

	// Verify the right node was removed
	hash1 := hs.hash(nodes[0])
	hash2 := hs.hash(nodes[1])
	hash3 := hs.hash(nodes[2])

	if hs.ring[hash1] != nodes[0] {
		t.Error("Node 0 should still be in ring")
	}

	if _, exists := hs.ring[hash2]; exists {
		t.Error("Node 1 should be removed from ring")
	}

	if hs.ring[hash3] != nodes[2] {
		t.Error("Node 2 should still be in ring")
	}
}

// TestComputeLocation tests finding the owner node for a file
func TestComputeLocation(t *testing.T) {
	hs := NewHashSystem(3)

	// Test empty ring
	location := hs.ComputeLocation("test.txt")
	if location != "" {
		t.Errorf("Expected empty location for empty ring, got %s", location)
	}

	// Add nodes in specific order to test ring positioning
	nodes := []string{
		"192.168.1.1:8001", // These will have specific hash values
		"192.168.1.2:8001",
		"192.168.1.3:8001",
	}

	for _, node := range nodes {
		hs.AddNode(node)
	}

	// Test file location
	filename := "test.txt"
	location = hs.ComputeLocation(filename)

	if location == "" {
		t.Error("Expected non-empty location")
	}

	// Verify the location is one of our nodes
	found := false
	for _, node := range nodes {
		if location == node {
			found = true
			break
		}
	}

	if !found {
		t.Errorf("Location %s is not one of the expected nodes", location)
	}

	// Test that same filename always maps to same node
	location2 := hs.ComputeLocation(filename)
	if location != location2 {
		t.Errorf("File location not consistent: %s vs %s", location, location2)
	}
}

// TestGetReplicaNodes tests getting replica nodes for a file
func TestGetReplicaNodes(t *testing.T) {
	hs := NewHashSystem(3)

	// Test empty ring
	replicas := hs.GetReplicaNodes("test.txt")
	if len(replicas) != 0 {
		t.Errorf("Expected 0 replicas for empty ring, got %d", len(replicas))
	}

	// Add nodes
	nodes := []string{
		"192.168.1.1:8001",
		"192.168.1.2:8001",
		"192.168.1.3:8001",
		"192.168.1.4:8001",
		"192.168.1.5:8001",
	}

	for _, node := range nodes {
		hs.AddNode(node)
	}

	// Test with enough nodes
	filename := "test.txt"
	replicas = hs.GetReplicaNodes(filename)

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

	// Verify all replicas are valid nodes
	nodeSet := make(map[string]bool)
	for _, node := range nodes {
		nodeSet[node] = true
	}

	for _, replica := range replicas {
		if !nodeSet[replica] {
			t.Errorf("Invalid replica node: %s", replica)
		}
	}
}

// TestGetReplicaNodesWithFewerNodes tests replica selection when fewer nodes than replicas
func TestGetReplicaNodesWithFewerNodes(t *testing.T) {
	hs := NewHashSystem(5) // Want 5 replicas

	// Add only 3 nodes
	nodes := []string{
		"192.168.1.1:8001",
		"192.168.1.2:8001",
		"192.168.1.3:8001",
	}

	for _, node := range nodes {
		hs.AddNode(node)
	}

	replicas := hs.GetReplicaNodes("test.txt")

	// Should get all 3 nodes (limited by available nodes)
	if len(replicas) != 3 {
		t.Errorf("Expected 3 replicas (all available nodes), got %d", len(replicas))
	}

	// Verify no duplicates
	seen := make(map[string]bool)
	for _, replica := range replicas {
		if seen[replica] {
			t.Errorf("Duplicate replica: %s", replica)
		}
		seen[replica] = true
	}
}

// TestGetAllNodes tests getting all nodes with their IDs
func TestGetAllNodes(t *testing.T) {
	hs := NewHashSystem(3)

	// Test empty ring
	allNodes := hs.GetAllNodes()
	if len(allNodes) != 0 {
		t.Errorf("Expected 0 nodes for empty ring, got %d", len(allNodes))
	}

	// Add nodes
	nodes := []string{
		"192.168.1.1:8001",
		"192.168.1.2:8001",
		"192.168.1.3:8001",
	}

	for _, node := range nodes {
		hs.AddNode(node)
	}

	allNodes = hs.GetAllNodes()

	if len(allNodes) != 3 {
		t.Errorf("Expected 3 nodes, got %d", len(allNodes))
	}

	// Verify all nodes are present with correct IDs
	for _, node := range nodes {
		expectedID := hs.hash(node)
		if id, exists := allNodes[node]; !exists {
			t.Errorf("Node %s not found in all nodes", node)
		} else if id != expectedID {
			t.Errorf("Node %s has wrong ID: expected %x, got %x", node, expectedID, id)
		}
	}
}

// TestComputeFileHash tests file hash computation
func TestComputeFileHash(t *testing.T) {
	// Create temporary file
	tempFile, err := ioutil.TempFile("", "test_hash")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tempFile.Name())

	// Write test content
	testContent := []byte("Hello, World! This is a test file.")
	if _, err := tempFile.Write(testContent); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	tempFile.Close()

	// Test hash computation
	hash1, err := ComputeFileHash(tempFile.Name())
	if err != nil {
		t.Fatalf("Failed to compute file hash: %v", err)
	}

	if hash1 == "" {
		t.Error("Hash should not be empty")
	}

	// Test that same file produces same hash
	hash2, err := ComputeFileHash(tempFile.Name())
	if err != nil {
		t.Fatalf("Failed to compute file hash second time: %v", err)
	}

	if hash1 != hash2 {
		t.Errorf("Hash not consistent: %s vs %s", hash1, hash2)
	}

	// Test non-existent file
	_, err = ComputeFileHash("/non/existent/file.txt")
	if err == nil {
		t.Error("Expected error for non-existent file")
	}
}

// TestComputeDataHash tests data hash computation
func TestComputeDataHash(t *testing.T) {
	testData := []byte("Hello, World! This is test data.")

	hash1 := ComputeDataHash(testData)
	if hash1 == "" {
		t.Error("Hash should not be empty")
	}

	// Test consistency
	hash2 := ComputeDataHash(testData)
	if hash1 != hash2 {
		t.Errorf("Data hash not consistent: %s vs %s", hash1, hash2)
	}

	// Test different data produces different hash
	differentData := []byte("Different test data.")
	hash3 := ComputeDataHash(differentData)
	if hash1 == hash3 {
		t.Error("Different data should produce different hash")
	}

	// Test empty data
	emptyHash := ComputeDataHash([]byte{})
	if emptyHash == "" {
		t.Error("Empty data hash should not be empty string")
	}
}

// TestParseNodeID tests node ID parsing
func TestParseNodeID(t *testing.T) {
	// Test valid node ID
	nodeID := "192.168.1.1:8001"
	ip, port, err := ParseNodeID(nodeID)
	if err != nil {
		t.Fatalf("Failed to parse valid node ID: %v", err)
	}

	expectedIP := "192.168.1.1"
	expectedPort := 8001

	if ip != expectedIP {
		t.Errorf("Expected IP %s, got %s", expectedIP, ip)
	}

	if port != expectedPort {
		t.Errorf("Expected port %d, got %d", expectedPort, port)
	}

	// Test invalid formats
	invalidIDs := []string{
		"192.168.1.1",            // No port
		"192.168.1.1:abc",        // Invalid port
		":8001",                  // No IP
		"",                       // Empty
		"192.168.1.1:8001:extra", // Too many parts
	}

	for _, invalidID := range invalidIDs {
		_, _, err := ParseNodeID(invalidID)
		if err == nil {
			t.Errorf("Expected error for invalid node ID: %s", invalidID)
		}
	}
}

// TestCreateNodeID tests node ID creation
func TestCreateNodeID(t *testing.T) {
	ip := "192.168.1.1"
	port := 8001
	timestamp := int64(1234567890)

	nodeID := CreateNodeID(ip, port, timestamp)
	expected := "192.168.1.1:8001:1234567890"

	if nodeID != expected {
		t.Errorf("Expected node ID %s, got %s", expected, nodeID)
	}

	// Test that different timestamps produce different IDs
	timestamp2 := timestamp + 1
	nodeID2 := CreateNodeID(ip, port, timestamp2)

	if nodeID == nodeID2 {
		t.Error("Different timestamps should produce different node IDs")
	}
}

// TestHashRingDistribution tests that files are distributed across nodes
func TestHashRingDistribution(t *testing.T) {
	hs := NewHashSystem(1) // Use 1 replica for simpler testing

	// Add nodes
	nodes := []string{
		"192.168.1.1:8001",
		"192.168.1.2:8001",
		"192.168.1.3:8001",
		"192.168.1.4:8001",
		"192.168.1.5:8001",
	}

	for _, node := range nodes {
		hs.AddNode(node)
	}

	// Test many files to see distribution
	fileLocations := make(map[string]int)
	numFiles := 1000

	for i := 0; i < numFiles; i++ {
		filename := fmt.Sprintf("file_%d.txt", i)
		location := hs.ComputeLocation(filename)
		fileLocations[location]++
	}

	// Verify all nodes got some files (probabilistically should happen with 1000 files)
	for _, node := range nodes {
		if count, exists := fileLocations[node]; !exists || count == 0 {
			t.Errorf("Node %s got no files, which is unlikely with good distribution", node)
		}
	}

	// Verify total count
	totalFiles := 0
	for _, count := range fileLocations {
		totalFiles += count
	}

	if totalFiles != numFiles {
		t.Errorf("Expected %d total files, got %d", numFiles, totalFiles)
	}
}

// TestHashRingConsistency tests that ring operations maintain consistency
func TestHashRingConsistency(t *testing.T) {
	hs := NewHashSystem(3)

	// Add nodes
	nodes := []string{
		"192.168.1.1:8001",
		"192.168.1.2:8001",
		"192.168.1.3:8001",
		"192.168.1.4:8001",
	}

	for _, node := range nodes {
		hs.AddNode(node)
	}

	// Test file locations
	testFiles := []string{"file1.txt", "file2.txt", "file3.txt", "file4.txt", "file5.txt"}

	// Record initial locations
	initialLocations := make(map[string]string)
	for _, file := range testFiles {
		initialLocations[file] = hs.ComputeLocation(file)
	}

	// Remove a node
	hs.RemoveNode(nodes[1])

	// Check that remaining files still map consistently
	for _, file := range testFiles {
		newLocation := hs.ComputeLocation(file)
		oldLocation := initialLocations[file]

		// If the old location was the removed node, location should change
		// If the old location was not the removed node, location might stay the same
		if oldLocation == nodes[1] && newLocation == nodes[1] {
			t.Errorf("File %s still maps to removed node %s", file, nodes[1])
		}

		// Verify new location is valid
		validNode := false
		for _, node := range nodes {
			if node == nodes[1] {
				continue // Skip removed node
			}
			if newLocation == node {
				validNode = true
				break
			}
		}

		if !validNode {
			t.Errorf("File %s maps to invalid node %s", file, newLocation)
		}
	}

	// Re-add the node and verify consistency
	hs.AddNode(nodes[1])

	for _, file := range testFiles {
		location1 := hs.ComputeLocation(file)
		location2 := hs.ComputeLocation(file)

		if location1 != location2 {
			t.Errorf("File %s location not consistent: %s vs %s", file, location1, location2)
		}
	}
}
