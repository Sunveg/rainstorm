package fileserver

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"hydfs/pkg/utils"
)

// TestFileServerCreation tests the creation of a new FileServer
func TestFileServerCreation(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "hydfs_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	logger := func(format string, args ...interface{}) {
		t.Logf(format, args...)
	}

	fs, err := NewFileServer(tempDir, 2, logger)
	if err != nil {
		t.Fatalf("Failed to create FileServer: %v", err)
	}

	// Verify directories were created
	expectedDirs := []string{
		filepath.Join(tempDir, "OwnedFiles"),
		filepath.Join(tempDir, "ReplicatedFiles"),
		filepath.Join(tempDir, "TempFiles"),
	}

	for _, dir := range expectedDirs {
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			t.Errorf("Expected directory %s was not created", dir)
		}
	}

	// Verify initial state
	if fs.workers != 2 {
		t.Errorf("Expected 2 workers, got %d", fs.workers)
	}

	if len(fs.fileMetadata) != 0 {
		t.Errorf("Expected empty file metadata, got %d entries", len(fs.fileMetadata))
	}
}

// TestFileServerStartStop tests starting and stopping the FileServer
func TestFileServerStartStop(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "hydfs_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	logger := func(format string, args ...interface{}) {
		t.Logf(format, args...)
	}

	fs, err := NewFileServer(tempDir, 1, logger)
	if err != nil {
		t.Fatalf("Failed to create FileServer: %v", err)
	}

	// Test start
	if err := fs.Start(); err != nil {
		t.Fatalf("Failed to start FileServer: %v", err)
	}

	if !fs.isRunning {
		t.Error("FileServer should be running after start")
	}

	// Test stop
	fs.Stop()
	if fs.isRunning {
		t.Error("FileServer should not be running after stop")
	}
}

// TestCreateFile tests file creation functionality
func TestCreateFile(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "hydfs_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	logger := func(format string, args ...interface{}) {
		t.Logf(format, args...)
	}

	fs, err := NewFileServer(tempDir, 1, logger)
	if err != nil {
		t.Fatalf("Failed to create FileServer: %v", err)
	}

	if err := fs.Start(); err != nil {
		t.Fatalf("Failed to start FileServer: %v", err)
	}
	defer fs.Stop()

	// Create a test file to copy
	testData := []byte("Hello, HyDFS!")
	testFile := filepath.Join(tempDir, "test_input.txt")
	if err := ioutil.WriteFile(testFile, testData, 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Submit create request
	req := &utils.FileRequest{
		OperationType:     utils.Create,
		FileName:          "test.txt",
		LocalFilePath:     testFile,
		ClientID:          "client1",
		SourceNodeID:      "node1",
		DestinationNodeID: "node1", // Same node = owned file
	}

	if err := fs.SubmitRequest(req); err != nil {
		t.Fatalf("Failed to submit create request: %v", err)
	}

	// Wait for processing
	time.Sleep(100 * time.Millisecond)

	// Verify file was created
	expectedPath := filepath.Join(fs.ownedFilesDir, "test.txt")
	if _, err := os.Stat(expectedPath); os.IsNotExist(err) {
		t.Error("Expected file was not created")
	}

	// Verify file content
	content, err := ioutil.ReadFile(expectedPath)
	if err != nil {
		t.Fatalf("Failed to read created file: %v", err)
	}

	if string(content) != string(testData) {
		t.Errorf("Expected file content %q, got %q", testData, content)
	}

	// Verify metadata was created
	metadata := fs.GetFileMetadata("test.txt")
	if metadata == nil {
		t.Error("Expected file metadata to be created")
	} else {
		if metadata.FileName != "test.txt" {
			t.Errorf("Expected filename test.txt, got %s", metadata.FileName)
		}
		if metadata.Size != int64(len(testData)) {
			t.Errorf("Expected size %d, got %d", len(testData), metadata.Size)
		}
	}
}

// TestCreateFileWithData tests file creation with direct data
func TestCreateFileWithData(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "hydfs_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	logger := func(format string, args ...interface{}) {
		t.Logf(format, args...)
	}

	fs, err := NewFileServer(tempDir, 1, logger)
	if err != nil {
		t.Fatalf("Failed to create FileServer: %v", err)
	}

	if err := fs.Start(); err != nil {
		t.Fatalf("Failed to start FileServer: %v", err)
	}
	defer fs.Stop()

	// Submit create request with direct data
	testData := []byte("Direct data test")
	req := &utils.FileRequest{
		OperationType:     utils.Create,
		FileName:          "direct.txt",
		Data:              testData,
		ClientID:          "client1",
		SourceNodeID:      "node1",
		DestinationNodeID: "node2", // Different node = replicated file
	}

	if err := fs.SubmitRequest(req); err != nil {
		t.Fatalf("Failed to submit create request: %v", err)
	}

	// Wait for processing
	time.Sleep(100 * time.Millisecond)

	// Verify file was created in replicated directory
	expectedPath := filepath.Join(fs.replicatedFilesDir, "direct.txt")
	if _, err := os.Stat(expectedPath); os.IsNotExist(err) {
		t.Error("Expected replicated file was not created")
	}

	// Verify file content
	content, err := ioutil.ReadFile(expectedPath)
	if err != nil {
		t.Fatalf("Failed to read created file: %v", err)
	}

	if string(content) != string(testData) {
		t.Errorf("Expected file content %q, got %q", testData, content)
	}
}

// TestAppendFile tests file append functionality
func TestAppendFile(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "hydfs_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	logger := func(format string, args ...interface{}) {
		t.Logf(format, args...)
	}

	fs, err := NewFileServer(tempDir, 1, logger)
	if err != nil {
		t.Fatalf("Failed to create FileServer: %v", err)
	}

	if err := fs.Start(); err != nil {
		t.Fatalf("Failed to start FileServer: %v", err)
	}
	defer fs.Stop()

	// First create a file
	initialData := []byte("Initial content\n")
	createReq := &utils.FileRequest{
		OperationType:     utils.Create,
		FileName:          "append_test.txt",
		Data:              initialData,
		ClientID:          "client1",
		SourceNodeID:      "node1",
		DestinationNodeID: "node1",
	}

	if err := fs.SubmitRequest(createReq); err != nil {
		t.Fatalf("Failed to submit create request: %v", err)
	}

	// Wait for create to complete
	time.Sleep(100 * time.Millisecond)

	// Now append to the file
	appendData := []byte("Appended content\n")
	appendReq := &utils.FileRequest{
		OperationType: utils.Append,
		FileName:      "append_test.txt",
		Data:          appendData,
		ClientID:      "client1",
		SourceNodeID:  "node1",
	}

	if err := fs.SubmitRequest(appendReq); err != nil {
		t.Fatalf("Failed to submit append request: %v", err)
	}

	// Wait for append to be processed by converger
	time.Sleep(200 * time.Millisecond)

	// Verify file content
	expectedPath := filepath.Join(fs.ownedFilesDir, "append_test.txt")
	content, err := ioutil.ReadFile(expectedPath)
	if err != nil {
		t.Fatalf("Failed to read file after append: %v", err)
	}

	expectedContent := string(initialData) + string(appendData)
	if string(content) != expectedContent {
		t.Errorf("Expected content %q, got %q", expectedContent, content)
	}

	// Verify metadata was updated
	metadata := fs.GetFileMetadata("append_test.txt")
	if metadata == nil {
		t.Error("Expected file metadata to exist")
	} else {
		expectedSize := int64(len(initialData) + len(appendData))
		if metadata.Size != expectedSize {
			t.Errorf("Expected size %d, got %d", expectedSize, metadata.Size)
		}
	}
}

// TestMultipleAppends tests multiple appends to ensure ordering
func TestMultipleAppends(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "hydfs_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	logger := func(format string, args ...interface{}) {
		t.Logf(format, args...)
	}

	fs, err := NewFileServer(tempDir, 1, logger)
	if err != nil {
		t.Fatalf("Failed to create FileServer: %v", err)
	}

	if err := fs.Start(); err != nil {
		t.Fatalf("Failed to start FileServer: %v", err)
	}
	defer fs.Stop()

	// Create initial file
	createReq := &utils.FileRequest{
		OperationType:     utils.Create,
		FileName:          "multi_append.txt",
		Data:              []byte("Start\n"),
		ClientID:          "client1",
		SourceNodeID:      "node1",
		DestinationNodeID: "node1",
	}

	if err := fs.SubmitRequest(createReq); err != nil {
		t.Fatalf("Failed to submit create request: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Submit multiple appends
	appendTexts := []string{"First\n", "Second\n", "Third\n"}
	for _, text := range appendTexts {
		appendReq := &utils.FileRequest{
			OperationType: utils.Append,
			FileName:      "multi_append.txt",
			Data:          []byte(text),
			ClientID:      "client1",
		}

		if err := fs.SubmitRequest(appendReq); err != nil {
			t.Fatalf("Failed to submit append request: %v", err)
		}
	}

	// Wait for all appends to be processed
	time.Sleep(300 * time.Millisecond)

	// Verify final content
	expectedPath := filepath.Join(fs.ownedFilesDir, "multi_append.txt")
	content, err := ioutil.ReadFile(expectedPath)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}

	expectedContent := "Start\nFirst\nSecond\nThird\n"
	if string(content) != expectedContent {
		t.Errorf("Expected content %q, got %q", expectedContent, content)
	}
}

// TestGetFile tests file retrieval functionality
func TestGetFile(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "hydfs_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	logger := func(format string, args ...interface{}) {
		t.Logf(format, args...)
	}

	fs, err := NewFileServer(tempDir, 1, logger)
	if err != nil {
		t.Fatalf("Failed to create FileServer: %v", err)
	}

	if err := fs.Start(); err != nil {
		t.Fatalf("Failed to start FileServer: %v", err)
	}
	defer fs.Stop()

	// Create a file first
	testData := []byte("Test file for retrieval")
	createReq := &utils.FileRequest{
		OperationType:     utils.Create,
		FileName:          "get_test.txt",
		Data:              testData,
		ClientID:          "client1",
		SourceNodeID:      "node1",
		DestinationNodeID: "node1",
	}

	if err := fs.SubmitRequest(createReq); err != nil {
		t.Fatalf("Failed to submit create request: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Now get the file
	outputPath := filepath.Join(tempDir, "retrieved.txt")
	getReq := &utils.FileRequest{
		OperationType: utils.Get,
		FileName:      "get_test.txt",
		LocalFilePath: outputPath,
		ClientID:      "client1",
	}

	if err := fs.SubmitRequest(getReq); err != nil {
		t.Fatalf("Failed to submit get request: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Verify file was retrieved
	if _, err := os.Stat(outputPath); os.IsNotExist(err) {
		t.Error("Retrieved file was not created")
	}

	// Verify content
	content, err := ioutil.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("Failed to read retrieved file: %v", err)
	}

	if string(content) != string(testData) {
		t.Errorf("Expected retrieved content %q, got %q", testData, content)
	}
}

// TestListStoredFiles tests the file listing functionality
func TestListStoredFiles(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "hydfs_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	logger := func(format string, args ...interface{}) {
		t.Logf(format, args...)
	}

	fs, err := NewFileServer(tempDir, 1, logger)
	if err != nil {
		t.Fatalf("Failed to create FileServer: %v", err)
	}

	if err := fs.Start(); err != nil {
		t.Fatalf("Failed to start FileServer: %v", err)
	}
	defer fs.Stop()

	// Create multiple files
	fileNames := []string{"file1.txt", "file2.txt", "file3.txt"}
	for _, fileName := range fileNames {
		req := &utils.FileRequest{
			OperationType:     utils.Create,
			FileName:          fileName,
			Data:              []byte("Test content for " + fileName),
			ClientID:          "client1",
			SourceNodeID:      "node1",
			DestinationNodeID: "node1",
		}

		if err := fs.SubmitRequest(req); err != nil {
			t.Fatalf("Failed to create file %s: %v", fileName, err)
		}
	}

	time.Sleep(200 * time.Millisecond)

	// List stored files
	storedFiles := fs.ListStoredFiles()

	// Verify all files are listed
	if len(storedFiles) != len(fileNames) {
		t.Errorf("Expected %d files, got %d", len(fileNames), len(storedFiles))
	}

	for _, fileName := range fileNames {
		if _, exists := storedFiles[fileName]; !exists {
			t.Errorf("Expected file %s to be listed", fileName)
		}
	}
}

// TestConcurrentOperations tests concurrent file operations
func TestConcurrentOperations(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "hydfs_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	logger := func(format string, args ...interface{}) {
		t.Logf(format, args...)
	}

	fs, err := NewFileServer(tempDir, 4, logger) // More workers for concurrency
	if err != nil {
		t.Fatalf("Failed to create FileServer: %v", err)
	}

	if err := fs.Start(); err != nil {
		t.Fatalf("Failed to start FileServer: %v", err)
	}
	defer fs.Stop()

	// Create multiple files concurrently
	numFiles := 10
	done := make(chan bool, numFiles)

	for i := 0; i < numFiles; i++ {
		go func(fileNum int) {
			fileName := fmt.Sprintf("concurrent_%d.txt", fileNum)
			req := &utils.FileRequest{
				OperationType:     utils.Create,
				FileName:          fileName,
				Data:              []byte(fmt.Sprintf("Content for file %d", fileNum)),
				ClientID:          "client1",
				SourceNodeID:      "node1",
				DestinationNodeID: "node1",
			}

			if err := fs.SubmitRequest(req); err != nil {
				t.Errorf("Failed to create file %s: %v", fileName, err)
			}
			done <- true
		}(i)
	}

	// Wait for all operations to complete
	for i := 0; i < numFiles; i++ {
		<-done
	}

	time.Sleep(300 * time.Millisecond)

	// Verify all files were created
	storedFiles := fs.ListStoredFiles()
	if len(storedFiles) != numFiles {
		t.Errorf("Expected %d files, got %d", numFiles, len(storedFiles))
	}

	for i := 0; i < numFiles; i++ {
		fileName := fmt.Sprintf("concurrent_%d.txt", i)
		if _, exists := storedFiles[fileName]; !exists {
			t.Errorf("Expected file %s to exist", fileName)
		}
	}
}
