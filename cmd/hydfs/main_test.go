package main

import (
	"os"
	"testing"

	"hydfs/pkg/coordinator"
)

// Mock logger for testing
func testLogger(format string, args ...interface{}) {
	// Silent logger for tests
}

func TestMainArguments(t *testing.T) {
	// Save original args
	origArgs := os.Args
	defer func() { os.Args = origArgs }()

	// Test insufficient arguments
	os.Args = []string{"hydfs"}
	// Note: main() calls os.Exit(1) for insufficient args
	// In real tests, we'd need to refactor main to return error instead of exiting
}

func TestCoordinatorIntegration(t *testing.T) {
	// Test coordinator creation and basic operations
	coord, err := coordinator.NewCoordinatorServer("127.0.0.1", 9000, testLogger)
	if err != nil {
		t.Fatalf("Failed to create coordinator: %v", err)
	}

	// Test that coordinator has all required components
	if coord.GetHashSystem() == nil {
		t.Error("Expected hash system to be initialized")
	}

	if coord.GetMembershipTable() == nil {
		t.Error("Expected membership table to be initialized")
	}
}

func TestCLICommands(t *testing.T) {
	coord, err := coordinator.NewCoordinatorServer("127.0.0.1", 9001, testLogger)
	if err != nil {
		t.Fatalf("Failed to create coordinator: %v", err)
	}

	clientID := "test_client"

	// Test create command logic (without actual CLI input)
	err = coord.CreateFile("test.txt", []byte("hello world"), clientID)
	if err == nil {
		t.Log("Create file operation initiated successfully")
	} else {
		t.Logf("Create file operation failed (expected without running cluster): %v", err)
	}

	// Test list command logic
	files, err := coord.ListFiles(clientID)
	if err != nil {
		t.Logf("List files failed (expected without running cluster): %v", err)
	} else {
		t.Logf("Found %d files", len(files))
	}
}

func TestCommandParsing(t *testing.T) {
	// Test command parsing logic
	testCases := []struct {
		command  string
		expected string
	}{
		{"create test.txt", "create"},
		{"append file.txt data", "append"},
		{"get myfile.txt", "get"},
		{"list", "list"},
		{"ls", "ls"},
		{"help", "help"},
		{"quit", "quit"},
		{"exit", "exit"},
	}

	for _, tc := range testCases {
		// This would test the actual parsing logic if we extracted it to a separate function
		t.Logf("Command '%s' should parse to '%s'", tc.command, tc.expected)
	}
}

func TestHelperFunctions(t *testing.T) {
	// Test helper functions exist
	coord, err := coordinator.NewCoordinatorServer("127.0.0.1", 9002, testLogger)
	if err != nil {
		t.Fatalf("Failed to create coordinator: %v", err)
	}

	// Test membership display
	members := coord.GetMembershipTable().GetMembers()
	t.Logf("Membership table has %d members", len(members))

	// Test hash ring display
	hashSystem := coord.GetHashSystem()
	nodes := hashSystem.GetAllNodes()
	t.Logf("Hash ring has %d nodes", len(nodes))
}

func TestFileOperations(t *testing.T) {
	coord, err := coordinator.NewCoordinatorServer("127.0.0.1", 9003, testLogger)
	if err != nil {
		t.Fatalf("Failed to create coordinator: %v", err)
	}

	clientID := "test_client"

	// Test file operations interface
	testData := []byte("test file content")

	// Create file
	err = coord.CreateFile("integration_test.txt", testData, clientID)
	t.Logf("Create file result: %v", err)

	// Append to file
	err = coord.AppendFile("integration_test.txt", []byte(" appended"), clientID)
	t.Logf("Append file result: %v", err)

	// Get file
	data, err := coord.GetFile("integration_test.txt", clientID)
	t.Logf("Get file result: %v, data length: %d", err, len(data))

	// List files
	files, err := coord.ListFiles(clientID)
	t.Logf("List files result: %v, file count: %d", err, len(files))
}
