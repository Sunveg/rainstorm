package network

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"hydfs/pkg/utils"
	"io"
	"net/http"
	"time"
)

// Client handles network communication with other HyDFS nodes
type Client struct {
	httpClient *http.Client
	timeout    time.Duration
}

// NewClient creates a new network client
func NewClient(timeout time.Duration) *Client {
	return &Client{
		httpClient: &http.Client{
			Timeout: timeout,
		},
		timeout: timeout,
	}
}

// FileRequest represents a file operation request
type FileRequest struct {
	Type              utils.OperationType `json:"type"`
	Filename          string              `json:"filename"`
	Data              []byte              `json:"data,omitempty"`
	ClientID          int64               `json:"client_id"`
	OperationID       int64               `json:"operation_id"`
	SourceNodeID      string              `json:"source_node_id,omitempty"`
	DestinationNodeID string              `json:"destination_node_id,omitempty"`
	OwnerNodeID       string              `json:"owner_node_id,omitempty"` // Primary owner node for this file
}

// FileResponse represents a file operation response
type FileResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
	Data    []byte `json:"data,omitempty"`
	Version int64  `json:"version"`
}

// Operation represents a replicated file operation (network layer)
type Operation struct {
	Type        utils.OperationType `json:"type"`
	Filename    string              `json:"filename"`
	Data        []byte              `json:"data,omitempty"`
	ClientID    int64               `json:"client_id"`
	OperationID int64               `json:"operation_id"`
}

// ReplicationRequest represents a replication request between nodes
type ReplicationRequest struct {
	Filename  string    `json:"filename"`
	Operation Operation `json:"operation"`
	SenderID  string    `json:"sender_id"`
}

// ReplicationResponse represents a replication response
type ReplicationResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}

// MergeRequest represents a merge operation request
type MergeRequest struct {
	Filename string `json:"filename"`
	ClientID int64  `json:"client_id"`
}

// MergeResponse represents a merge operation response
type MergeResponse struct {
	Success      bool   `json:"success"`
	Error        string `json:"error,omitempty"`
	FinalVersion int64  `json:"final_version"`
}

// CreateFile sends a create file request to a node
func (c *Client) CreateFile(ctx context.Context, nodeAddr string, req FileRequest) (*FileResponse, error) {
	url := fmt.Sprintf("http://%s/api/file/create", nodeAddr)
	return c.sendFileRequest(ctx, url, req)
}

// AppendFile sends an append file request to a node
func (c *Client) AppendFile(ctx context.Context, nodeAddr string, req FileRequest) (*FileResponse, error) {
	url := fmt.Sprintf("http://%s/api/file/append", nodeAddr)
	return c.sendFileRequest(ctx, url, req)
}

// GetFile sends a get file request to a node
func (c *Client) GetFile(ctx context.Context, nodeAddr string, req FileRequest) (*FileResponse, error) {
	url := fmt.Sprintf("http://%s/api/file/get", nodeAddr)
	return c.sendFileRequest(ctx, url, req)
}

// ListFiles sends a list files request to a node
func (c *Client) ListFiles(ctx context.Context, nodeAddr string, clientID int64) ([]string, error) {
	url := fmt.Sprintf("http://%s/api/file/list", nodeAddr)

	req := struct {
		ClientID int64 `json:"client_id"`
	}{ClientID: clientID}

	jsonData, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %v", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %v", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("request failed with status: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %v", err)
	}

	var result struct {
		Filenames []string `json:"filenames"`
	}

	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %v", err)
	}

	return result.Filenames, nil
}

// ReplicateOperation sends a replication request to a node
func (c *Client) ReplicateOperation(ctx context.Context, nodeAddr string, req ReplicationRequest) (*ReplicationResponse, error) {
	url := fmt.Sprintf("http://%s/api/replicate", nodeAddr)

	jsonData, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %v", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %v", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %v", err)
	}

	var result ReplicationResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %v", err)
	}

	return &result, nil
}

// MergeFile sends a merge request to a node
func (c *Client) MergeFile(ctx context.Context, nodeAddr string, req MergeRequest) (*MergeResponse, error) {
	url := fmt.Sprintf("http://%s/api/file/merge", nodeAddr)

	jsonData, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %v", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %v", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %v", err)
	}

	var result MergeResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %v", err)
	}

	return &result, nil
}

// HealthCheck sends a health check request to a node
func (c *Client) HealthCheck(ctx context.Context, nodeAddr string) (bool, error) {
	url := fmt.Sprintf("http://%s/api/health", nodeAddr)

	httpReq, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return false, fmt.Errorf("failed to create request: %v", err)
	}

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return false, fmt.Errorf("failed to send request: %v", err)
	}
	defer resp.Body.Close()

	return resp.StatusCode == http.StatusOK, nil
}

// sendFileRequest is a helper to send file operation requests
func (c *Client) sendFileRequest(ctx context.Context, url string, req FileRequest) (*FileResponse, error) {
	jsonData, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %v", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %v", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %v", err)
	}

	var result FileResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %v", err)
	}

	return &result, nil
}
