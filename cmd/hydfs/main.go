package main

import (
	"bufio"
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"hydfs/pkg/coordinator"
	"hydfs/pkg/membership"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: ./hydfs <ip> <port> [introducer_ip:port]")
		os.Exit(1)
	}

	ip := os.Args[1]
	portStr := os.Args[2]
	port, err := strconv.Atoi(portStr)
	if err != nil {
		log.Fatalf("Invalid port: %v", err)
	}

	var introducerAddr string
	if len(os.Args) > 3 {
		introducerAddr = os.Args[3]
	}

	// Setup logging: redirect ALL standard log output to file
	logFileName := fmt.Sprintf("hydfs_%s_%d.log", ip, port)
	logFile, err := os.OpenFile(logFileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("Failed to open log file: %v", err)
	}
	defer logFile.Close()

	// Redirect ALL log.Printf calls to file (this catches background logs too)
	log.SetOutput(logFile)

	// Print startup to console using fmt (not log)
	fmt.Printf("Node %s:%d started | Logs: %s\n", ip, port, logFileName)

	// Create smart logger: writes to file, shows important messages on console
	logger := func(format string, args ...interface{}) {
		msg := fmt.Sprintf(format, args...)

		// Always write to file via standard log
		log.Printf("[HyDFS] %s", msg)

		// Also print to console if message is marked as important (contains >>>)
		showOnConsole := strings.Contains(msg, ">>>")

		if showOnConsole {
			// Print with newline before to avoid interfering with CLI prompt
			fmt.Printf("\n[%s:%d] %s\n", ip, port, msg)
		}
	}

	// Create and start coordinator server
	coordinator, err := coordinator.NewCoordinatorServer(ip, port, logger)
	if err != nil {
		log.Fatalf("Failed to create coordinator: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start the coordinator
	if err := coordinator.Start(ctx); err != nil {
		log.Fatalf("Failed to start coordinator: %v", err)
	}

	// Join cluster if introducer provided
	if introducerAddr != "" {
		logger("Joining cluster via introducer %s", introducerAddr)
		if err := coordinator.Join(introducerAddr); err != nil {
			logger("Warning: Failed to join cluster: %v", err)
		}
	}

	// Log startup information (goes to file only)
	nodeID := coordinator.GetNodeID()
	logger("Node started successfully")

	// Print node ID to console for user
	fmt.Printf("Node ID: %s\n", membership.StringifyNodeID(nodeID))
	if introducerAddr != "" {
		fmt.Printf("Joined cluster via: %s\n", introducerAddr)
	}
	fmt.Println()

	// Setup shutdown signal handler in background
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		fmt.Println("\nShutting down...")
		coordinator.Stop()
		cancel()
		os.Exit(0)
	}()

	// Run interactive CLI in main thread (needed for proper stdin handling)
	interactiveCLI(coordinator, logger)
}

// interactiveCLI provides a comprehensive interactive command interface
func interactiveCLI(coord *coordinator.CoordinatorServer, logger func(string, ...interface{})) {
	// Force stdin to be line-buffered and ensure it's ready
	reader := bufio.NewReader(os.Stdin)
	clientID := fmt.Sprintf("client_%d", time.Now().UnixNano())

	fmt.Println("=== HyDFS Interactive CLI ===")
	fmt.Println("Commands:")
	fmt.Println("  create <local_file> <hydfs_file>   - Create file in HyDFS")
	fmt.Println("  append <local_file> <hydfs_file>   - Append to HyDFS file")
	fmt.Println("  multiappend <hydfs_file> <VM1> <local1> [VM2] [local2] ... - Concurrent appends from multiple VMs")
	fmt.Println("  get <hydfs_file> [local_file]      - Get file from HyDFS")
	fmt.Println("  getfromreplica <VM> <hydfs_file> <local_file> - Get file from specific replica")
	fmt.Println("  list                               - List all files in system")
	fmt.Println("  ls <hydfs_file>                    - Show file details & replicas")
	fmt.Println("  liststore                          - List files on this node")
	fmt.Println("  list_mem_ids                       - Show sorted ring membership")
	fmt.Println("  merge <filename>                   - Merge file replicas")
	fmt.Println("  membership                         - Show cluster members")
	fmt.Println("  ring                               - Show hash ring")
	fmt.Println("  help                               - Show this message")
	fmt.Println("  quit, exit                         - Exit")
	fmt.Println()
	fmt.Println("NOTE: Async logs may appear between commands.")
	fmt.Println("      Press ENTER after logs to get a fresh prompt.")
	fmt.Println("      Full logs in .log file.")
	fmt.Println("=================================================")
	fmt.Println()

	for {
		fmt.Print("hydfs> ")

		line, err := reader.ReadString('\n')
		if err != nil {
			if err.Error() != "EOF" {
				fmt.Printf("Error reading input: %v\n", err)
			}
			break
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}

		command := strings.ToLower(parts[0])

		switch command {
		case "create":
			handleCreateCommand(coord, parts, clientID, logger)

		case "append":
			handleAppendCommand(coord, parts, clientID, logger)

		case "multiappend":
			handleMultiappendCommand(coord, parts, clientID, logger)

		case "get":
			handleGetCommand(coord, parts, clientID, logger)

		case "getfromreplica":
			handleGetFromReplicaCommand(coord, parts, clientID, logger)

		case "list":
			handleListCommand(coord, clientID, logger)

		case "ls":
			if len(parts) < 2 {
				fmt.Println("Usage: ls <HyDFSfilename>")
			} else {
				handleLsCommand(coord, parts[1], logger)
			}

		case "liststore":
			handleListStoreCommand(coord, logger)

		case "list_mem_ids":
			handleListMemIdsCommand(coord, logger)

		case "merge":
			handleMergeCommand(coord, parts, clientID, logger)

		case "membership":
			handleMembershipCommand(coord, logger)

		case "ring":
			handleRingCommand(coord, logger)

		case "help":
			showHelp()

		case "quit", "exit":
			fmt.Println("Goodbye!")
			os.Exit(0)

		default:
			fmt.Printf("Unknown command: %s. Type 'help' for available commands.\n", command)
		}
	}
}

// handleCreateCommand handles file creation
func handleCreateCommand(coord *coordinator.CoordinatorServer, parts []string, clientID string, logger func(string, ...interface{})) {
	if len(parts) < 3 {
		fmt.Println("Usage: create <local_filename> <hydfs_filename>")
		fmt.Println("  local_filename:  Path to local file to upload")
		fmt.Println("  hydfs_filename:  Name to store the file as in HyDFS")
		return
	}

	localFileName := parts[1] // Local file path
	hydfsFileName := parts[2] // HyDFS file name

	// Check if local file exists
	if _, err := os.Stat(localFileName); os.IsNotExist(err) {
		fmt.Printf("Error: Local file %s does not exist\n", localFileName)
		return
	}

	// Get file info for display
	fileInfo, err := os.Stat(localFileName)
	if err != nil {
		fmt.Printf("Error reading file info: %v\n", err)
		return
	}

	fmt.Printf("Creating file %s in HyDFS from local file %s (%d bytes)...\n",
		hydfsFileName, localFileName, fileInfo.Size())

	// Call coordinator with file path (not data!)
	if err := coord.CreateFile(hydfsFileName, localFileName, clientID); err != nil {
		fmt.Printf("Error creating file: %v\n", err)
	} else {
		fmt.Printf(">>> CLIENT: CREATE COMPLETED for %s <<<\n", hydfsFileName)
	}
}

// handleAppendCommand handles file append
func handleAppendCommand(coord *coordinator.CoordinatorServer, parts []string, clientID string, logger func(string, ...interface{})) {
	if len(parts) < 3 {
		fmt.Println("Usage: append <local_file_path> <hydfs_filename>")
		fmt.Println("  local_file_path: Path to local file containing data to append")
		fmt.Println("  hydfs_filename:  HyDFS file to append to")
		return
	}

	localFilePath := parts[1] // Local file with append data
	hydfsFileName := parts[2] // HyDFS file to append to

	// Check if local file exists
	fileInfo, err := os.Stat(localFilePath)
	if os.IsNotExist(err) {
		fmt.Printf("Error: Local file %s does not exist\n", localFilePath)
		return
	}
	if err != nil {
		fmt.Printf("Error reading file info: %v\n", err)
		return
	}

	fmt.Printf("Appending %d bytes from %s to HyDFS file %s...\n",
		fileInfo.Size(), localFilePath, hydfsFileName)

	if err := coord.AppendFile(hydfsFileName, localFilePath, clientID); err != nil {
		fmt.Printf("Error appending to file: %v\n", err)
	} else {
		fmt.Printf(">>> CLIENT: APPEND COMPLETED for %s <<<\n", hydfsFileName)
	}
}

// handleMultiappendCommand handles concurrent appends from multiple VMs
// Usage: multiappend <hydfs_filename> <VM1> <local1> [VM2] [local2] ...
func handleMultiappendCommand(coord *coordinator.CoordinatorServer, parts []string, clientID string, logger func(string, ...interface{})) {
	if len(parts) < 4 || (len(parts)-2)%2 != 0 {
		fmt.Println("Usage: multiappend <hydfs_filename> <VM1> <local1> [VM2] [local2] ...")
		fmt.Println("  hydfs_filename: HyDFS file to append to")
		fmt.Println("  VM1, VM2, ...:  VM addresses (ip:port) or node IDs")
		fmt.Println("  local1, local2, ...: Local files on respective VMs to append")
		fmt.Println("Example: multiappend /hydfs/test.txt 127.0.0.1:5001 file1.txt 127.0.0.1:5002 file2.txt")
		return
	}

	hydfsFileName := parts[1]

	// Parse VM and local file pairs
	type vmAppend struct {
		vmAddress string
		localFile string
	}

	vmAppends := []vmAppend{}
	for i := 2; i < len(parts); i += 2 {
		if i+1 >= len(parts) {
			fmt.Printf("Error: Missing local file for VM %s\n", parts[i])
			return
		}
		vmAppends = append(vmAppends, vmAppend{
			vmAddress: parts[i],
			localFile: parts[i+1],
		})
	}

	fmt.Printf("Launching %d concurrent appends to %s...\n", len(vmAppends), hydfsFileName)

	// Launch all appends concurrently using goroutines
	var wg sync.WaitGroup
	errors := make([]error, len(vmAppends))

	for i, va := range vmAppends {
		wg.Add(1)
		go func(index int, vmAddr, localFile string) {
			defer wg.Done()
			fmt.Printf("  Sending append from VM %s (file: %s)...\n", vmAddr, localFile)
			err := coord.SendAppendToVM(vmAddr, hydfsFileName, localFile)
			if err != nil {
				errors[index] = fmt.Errorf("VM %s: %v", vmAddr, err)
				fmt.Printf("  ✗ Failed: VM %s - %v\n", vmAddr, err)
			} else {
				fmt.Printf("  ✓ Success: VM %s\n", vmAddr)
			}
		}(i, va.vmAddress, va.localFile)
	}

	// Wait for all goroutines to complete
	wg.Wait()

	// Check for errors
	hasErrors := false
	for i, err := range errors {
		if err != nil {
			hasErrors = true
			fmt.Printf("Error from %s: %v\n", vmAppends[i].vmAddress, err)
		}
	}

	if !hasErrors {
		fmt.Printf(">>> CLIENT: MULTIAPPEND COMPLETED for %s (all %d VMs) <<<\n", hydfsFileName, len(vmAppends))
	} else {
		fmt.Printf(">>> CLIENT: MULTIAPPEND PARTIALLY COMPLETED for %s (some VMs failed) <<<\n", hydfsFileName)
	}
}

// handleGetCommand handles file retrieval
func handleGetCommand(coord *coordinator.CoordinatorServer, parts []string, clientID string, logger func(string, ...interface{})) {
	if len(parts) < 2 {
		fmt.Println("Usage: get <filename> [local_file_path]")
		return
	}

	filename := parts[1]

	fmt.Printf("Retrieving file %s...\n", filename)
	data, err := coord.GetFile(filename, clientID)
	if err != nil {
		fmt.Printf("Error retrieving file: %v\n", err)
		return
	}

	if len(parts) >= 3 {
		// Save to local file
		localPath := parts[2]
		if err := ioutil.WriteFile(localPath, data, 0644); err != nil {
			fmt.Printf("Error writing to local file %s: %v\n", localPath, err)
			return
		}
		fmt.Printf(">>> CLIENT: GET COMPLETED - saved %d bytes to %s <<<\n", len(data), localPath)
	} else {
		// Print to console
		fmt.Printf("File content (%d bytes):\n", len(data))
		fmt.Println(string(data))
		fmt.Printf(">>> CLIENT: GET COMPLETED - retrieved %d bytes <<<\n", len(data))
	}
}

// handleGetFromReplicaCommand handles fetching a file from a specific replica
func handleGetFromReplicaCommand(coord *coordinator.CoordinatorServer, parts []string, clientID string, logger func(string, ...interface{})) {
	if len(parts) < 4 {
		fmt.Println("Usage: getfromreplica <VMaddress> <HyDFSfilename> <localfilename>")
		fmt.Println("  VMaddress:      VM address (ip:port) or node ID to fetch from")
		fmt.Println("  HyDFSfilename:  Name of the file in HyDFS")
		fmt.Println("  localfilename:  Local file path to save the fetched file")
		fmt.Println("Example: getfromreplica 127.0.0.1:5001 /hydfs/file1.txt replica1.txt")
		return
	}

	nodeAddress := parts[1]
	hydfsFileName := parts[2]
	localFileName := parts[3]

	fmt.Printf("Fetching file %s from replica %s...\n", hydfsFileName, nodeAddress)

	data, err := coord.GetFileFromReplica(nodeAddress, hydfsFileName, clientID)
	if err != nil {
		fmt.Printf("Error fetching file from replica: %v\n", err)
		return
	}

	// Save to local file
	if err := ioutil.WriteFile(localFileName, data, 0644); err != nil {
		fmt.Printf("Error writing to local file %s: %v\n", localFileName, err)
		return
	}

	fmt.Printf(">>> CLIENT: GETFROMREPLICA COMPLETED - saved %d bytes from %s to %s <<<\n", len(data), nodeAddress, localFileName)
}

// handleListCommand handles file listing
func handleListCommand(coord *coordinator.CoordinatorServer, clientID string, logger func(string, ...interface{})) {
	fmt.Println("Listing files...")
	files, err := coord.ListFiles(clientID)
	if err != nil {
		fmt.Printf("Error listing files: %v\n", err)
		return
	}

	if len(files) == 0 {
		fmt.Println("No files found in the system")
	} else {
		fmt.Printf("Found %d files:\n", len(files))
		for _, filename := range files {
			fmt.Printf("  %s\n", filename)
		}
	}
}

// handleListStoreCommand handles local file listing with fileIDs
// Shows all files (owned + replicated) with their storage location
func handleListStoreCommand(coord *coordinator.CoordinatorServer, logger func(string, ...interface{})) {
	fmt.Println("Listing files stored on this node...")

	// Get node ID and hash ring ID
	nodeID := coord.GetNodeID()
	nodeIDStr := membership.StringifyNodeID(nodeID)
	hashSystem := coord.GetHashSystem()
	ringID := hashSystem.GetNodeID(nodeIDStr)

	fmt.Printf("Node: %s\n", nodeIDStr)
	fmt.Printf("Ring ID: %08x\n", ringID)
	fmt.Println()

	// Get all stored files (owned + replicated)
	files := coord.ListStoredFiles()

	if len(files) == 0 {
		fmt.Println("No files stored on this node")
		return
	}

	// Get directory paths to determine file location
	ownedFilesDir := coord.GetOwnedFilesDir()
	replicatedFilesDir := coord.GetReplicatedFilesDir()

	// Separate files by type
	var ownedFiles []string
	var replicatedFiles []string

	for filename, metadata := range files {
		// Determine if file is owned or replicated by checking Location path
		location := metadata.Location
		isOwned := false

		if location != "" {
			// Check if location path contains OwnedFiles or ReplicatedFiles
			// Use both the full directory path and simple string matching for robustness
			if strings.Contains(location, ownedFilesDir) || strings.Contains(location, "OwnedFiles") {
				isOwned = true
			} else if strings.Contains(location, replicatedFilesDir) || strings.Contains(location, "ReplicatedFiles") {
				isOwned = false
			} else {
				// Fallback: check directory name in path
				dir := filepath.Dir(location)
				if strings.Contains(dir, "OwnedFiles") {
					isOwned = true
				}
			}
		}

		if isOwned {
			ownedFiles = append(ownedFiles, filename)
		} else {
			replicatedFiles = append(replicatedFiles, filename)
		}
	}

	fmt.Printf("Files stored on this node (%d total):\n", len(files))

	// Display owned files first
	for _, filename := range ownedFiles {
		metadata := files[filename]
		fileHash := hashSystem.GetNodeID(filename)
		fmt.Printf("  [OWNED] %s (FileID: %08x, OpID: %d)\n", filename, fileHash, metadata.LastOperationId)
	}

	// Display replicated files
	for _, filename := range replicatedFiles {
		metadata := files[filename]
		fileHash := hashSystem.GetNodeID(filename)
		fmt.Printf("  [REPLICATED] %s (FileID: %08x, OpID: %d)\n", filename, fileHash, metadata.LastOperationId)
	}
}

// handleLsCommand shows file details including fileID and replicas
func handleLsCommand(coord *coordinator.CoordinatorServer, filename string, logger func(string, ...interface{})) {
	hashSystem := coord.GetHashSystem()

	// Compute fileID (hash of filename)
	fileID := hashSystem.GetNodeID(filename)

	// Get owner
	owner := hashSystem.ComputeLocation(filename)
	if owner == "" {
		fmt.Printf("File: %s\n", filename)
		fmt.Println("Error: No owner found (hash ring may be empty)")
		return
	}

	// Get replica nodes (excludes owner)
	replicas := hashSystem.GetReplicaNodes(filename)

	fmt.Printf("File: %s\n", filename)
	fmt.Printf("FileID (ring ID): %08x\n", fileID)
	fmt.Printf("Replication factor: %d replicas\n", len(replicas))
	fmt.Printf("Owner: %s (ring ID: %08x)\n", owner, hashSystem.GetNodeID(owner))
	fmt.Println()
	fmt.Printf("All replicas (%d nodes):\n", len(replicas))
	for i, replica := range replicas {
		replicaRingID := hashSystem.GetNodeID(replica)
		fmt.Printf("  %d. [REPLICA %d] %s -> %08x\n", i+1, i+1, replica, replicaRingID)
	}
}

// handleListMemIdsCommand shows sorted membership list with ring IDs
func handleListMemIdsCommand(coord *coordinator.CoordinatorServer, logger func(string, ...interface{})) {
	members := coord.GetMembershipTable().GetMembers()
	hashSystem := coord.GetHashSystem()

	// Create a list of members with their ring IDs
	type memberInfo struct {
		nodeID string
		ringID uint32
	}

	var memberList []memberInfo
	for _, member := range members {
		nodeIDStr := membership.StringifyNodeID(member.NodeID)
		ringID := hashSystem.GetNodeID(nodeIDStr)
		memberList = append(memberList, memberInfo{
			nodeID: nodeIDStr,
			ringID: ringID,
		})
	}

	// Sort by ring ID
	sort.Slice(memberList, func(i, j int) bool {
		return memberList[i].ringID < memberList[j].ringID
	})

	fmt.Printf("Membership List (sorted by ring position, %d nodes):\n", len(memberList))
	fmt.Println("Position | Node ID                           | Ring ID (hex)")
	fmt.Println("---------|-----------------------------------|-------------")
	for i, m := range memberList {
		fmt.Printf("   %2d    | %-33s | %08x\n", i+1, m.nodeID, m.ringID)
	}
}

// handleMergeCommand handles file merge operations
func handleMergeCommand(coord *coordinator.CoordinatorServer, parts []string, clientID string, logger func(string, ...interface{})) {
	if len(parts) < 2 {
		fmt.Println("Usage: merge <filename>")
		return
	}

	filename := parts[1]

	fmt.Printf("Merging file %s across replicas...\n", filename)
	if err := coord.MergeFile(filename, clientID); err != nil {
		fmt.Printf("Error merging file: %v\n", err)
	} else {
		fmt.Printf("Successfully merged file %s\n", filename)
	}
}

// handleMembershipCommand shows cluster membership
func handleMembershipCommand(coord *coordinator.CoordinatorServer, logger func(string, ...interface{})) {
	members := coord.GetMembershipTable().GetMembers()
	fmt.Printf("Cluster membership (%d nodes):\n", len(members))
	for _, member := range members {
		fmt.Printf("  %s: %s\n", membership.StringifyNodeID(member.NodeID), member.State.String())
	}
}

// handleRingCommand shows hash ring status
func handleRingCommand(coord *coordinator.CoordinatorServer, logger func(string, ...interface{})) {
	hashSystem := coord.GetHashSystem()
	nodes := hashSystem.GetAllNodes()
	fmt.Printf("Hash ring (%d nodes):\n", len(nodes))
	for nodeID, ringID := range nodes {
		fmt.Printf("  %s -> %08x\n", nodeID, ringID)
	}
}

// showHelp displays help information
func showHelp() {
	fmt.Println("\nHyDFS Commands:")
	fmt.Println("  create <filename> [local_file_path] - Create a file (optionally from local file)")
	fmt.Println("  append <local_file_path> <HyDFSfilename> - Append local file contents to HyDFS file")
	fmt.Println("  get <filename> [local_file_path]   - Get a file (optionally save to local file)")
	fmt.Println("  list                               - List all files in the system")
	fmt.Println("  ls                                 - List all files in the system (alias)")
	fmt.Println("  liststore                          - List files stored on this node with fileIDs")
	fmt.Println("  merge <filename>                   - Synchronize file versions across replicas")
	fmt.Println("  membership                         - Show cluster membership")
	fmt.Println("  ring                               - Show hash ring status")
	fmt.Println("  help                               - Show this help message")
	fmt.Println("  quit, exit                         - Exit the program")
	fmt.Println()
}
