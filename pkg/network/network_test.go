package network

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"hydfs/pkg/fileserver"
	"hydfs/pkg/utils"
)

// Mock logger for testing
func testLogger(format string, args ...interface{}) {
	// Silent logger for tests
}

func TestNetworkClientCreation(t *testing.T) {
	client := NewClient(5 * time.Second)
	if client == nil {
		t.Fatal("Expected client to be created")
	}

	if client.timeout != 5*time.Second {
		t.Errorf("Expected timeout to be 5s, got %v", client.timeout)
	}
}

func TestNetworkServerCreation(t *testing.T) {
	// Create a temporary file server for testing
	fileServer, err := fileserver.NewFileServer("./test_storage", 1, testLogger)
	if err != nil {
		t.Fatalf("Failed to create file server: %v", err)
	}

	server := NewServer("127.0.0.1:8080", fileServer, "test-node-1", testLogger)
	if server == nil {
		t.Fatal("Expected server to be created")
	}

	if server.nodeID != "test-node-1" {
		t.Errorf("Expected nodeID to be 'test-node-1', got %s", server.nodeID)
	}
}

func TestHealthCheckEndpoint(t *testing.T) {
	// Create a temporary file server for testing
	fileServer, err := fileserver.NewFileServer("./test_storage", 1, testLogger)
	if err != nil {
		t.Fatalf("Failed to create file server: %v", err)
	}

	server := NewServer("127.0.0.1:8080", fileServer, "test-node-1", testLogger)

	// Create a test request
	req := httptest.NewRequest(http.MethodGet, "/api/health", nil)
	w := httptest.NewRecorder()

	// Call the handler
	server.handleHealthCheck(w, req)

	// Check the response
	if w.Code != http.StatusOK {
		t.Errorf("Expected status code %d, got %d", http.StatusOK, w.Code)
	}

	contentType := w.Header().Get("Content-Type")
	if contentType != "application/json" {
		t.Errorf("Expected content type 'application/json', got %s", contentType)
	}
}

func TestFileRequestValidation(t *testing.T) {
	req := FileRequest{
		Type:        utils.Create,
		Filename:    "test.txt",
		Data:        []byte("test data"),
		ClientID:    123,
		OperationID: 1,
	}

	if req.Filename != "test.txt" {
		t.Errorf("Expected filename 'test.txt', got %s", req.Filename)
	}

	if string(req.Data) != "test data" {
		t.Errorf("Expected data 'test data', got %s", string(req.Data))
	}
}

func TestReplicationRequestStructure(t *testing.T) {
	operation := Operation{
		Type:        utils.Create,
		Filename:    "test.txt",
		Data:        []byte("test data"),
		ClientID:    123,
		OperationID: 1,
	}

	req := ReplicationRequest{
		Filename:  "test.txt",
		Operation: operation,
		SenderID:  "node-1",
	}

	if req.Filename != "test.txt" {
		t.Errorf("Expected filename 'test.txt', got %s", req.Filename)
	}

	if req.SenderID != "node-1" {
		t.Errorf("Expected sender ID 'node-1', got %s", req.SenderID)
	}
}

func TestClientHealthCheck(t *testing.T) {
	// Create a test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"healthy": true, "active_files": 0}`))
	}))
	defer server.Close()

	client := NewClient(5 * time.Second)

	// Remove the http:// prefix from the test server URL
	addr := server.URL[7:] // Remove "http://"

	healthy, err := client.HealthCheck(context.Background(), addr)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if !healthy {
		t.Error("Expected health check to return true")
	}
}

func TestMergeRequestResponse(t *testing.T) {
	req := MergeRequest{
		Filename: "test.txt",
		ClientID: 123,
	}

	resp := MergeResponse{
		Success:      true,
		FinalVersion: 5,
	}

	if req.Filename != "test.txt" {
		t.Errorf("Expected filename 'test.txt', got %s", req.Filename)
	}

	if resp.FinalVersion != 5 {
		t.Errorf("Expected final version 5, got %d", resp.FinalVersion)
	}
}
