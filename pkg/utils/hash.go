package utils

import (
	"crypto/sha256"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
)

// HashSystem implements consistent hashing for HyDFS
type HashSystem struct {
	ring         map[uint32]string // hash -> nodeID mapping
	sortedHashes []uint32          // sorted list of hash values
	replicas     int               // number of replicas per file
}

// NewHashSystem creates a new hash system with specified number of replicas
func NewHashSystem(replicas int) *HashSystem {
	return &HashSystem{
		ring:     make(map[uint32]string),
		replicas: replicas,
	}
}

// AddNode adds a node to the hash ring
func (hs *HashSystem) AddNode(nodeID string) {
	hash := hs.hash(nodeID)
	hs.ring[hash] = nodeID
	hs.sortedHashes = append(hs.sortedHashes, hash)
	sort.Slice(hs.sortedHashes, func(i, j int) bool {
		return hs.sortedHashes[i] < hs.sortedHashes[j]
	})
}

// RemoveNode removes a node from the hash ring
func (hs *HashSystem) RemoveNode(nodeID string) {
	hash := hs.hash(nodeID)
	delete(hs.ring, hash)

	for i, h := range hs.sortedHashes {
		if h == hash {
			hs.sortedHashes = append(hs.sortedHashes[:i], hs.sortedHashes[i+1:]...)
			break
		}
	}
}

// ComputeLocation returns the primary owner node for a given filename
func (hs *HashSystem) ComputeLocation(filename string) string {
	if len(hs.sortedHashes) == 0 {
		return ""
	}

	hash := hs.hash(filename)
	idx := hs.search(hash)
	return hs.ring[hs.sortedHashes[idx]]
}

// GetReplicaNodes returns all replica nodes for a given filename
func (hs *HashSystem) GetReplicaNodes(filename string) []string {
	if len(hs.sortedHashes) == 0 {
		return []string{}
	}

	hash := hs.hash(filename)
	idx := hs.search(hash)

	replicas := make([]string, 0, hs.replicas)
	visited := make(map[string]bool)

	for i := 0; i < len(hs.sortedHashes) && len(replicas) < hs.replicas; i++ {
		currentIdx := (idx + i) % len(hs.sortedHashes)
		nodeID := hs.ring[hs.sortedHashes[currentIdx]]

		if !visited[nodeID] {
			replicas = append(replicas, nodeID)
			visited[nodeID] = true
		}
	}

	return replicas
}

// GetNodeID returns the hash ring ID for a given node
func (hs *HashSystem) GetNodeID(nodeID string) uint32 {
	return hs.hash(nodeID)
}

// GetAllNodes returns all nodes in the hash ring with their IDs
func (hs *HashSystem) GetAllNodes() map[string]uint32 {
	result := make(map[string]uint32)
	for hash, nodeID := range hs.ring {
		result[nodeID] = hash
	}
	return result
}

// hash computes hash value for a string using CRC32
func (hs *HashSystem) hash(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

// search finds the index of the first hash >= target hash
func (hs *HashSystem) search(target uint32) int {
	idx := sort.Search(len(hs.sortedHashes), func(i int) bool {
		return hs.sortedHashes[i] >= target
	})

	if idx == len(hs.sortedHashes) {
		return 0 // wrap around to the first node
	}

	return idx
}

// ComputeFileHash computes SHA-256 hash of a file
func ComputeFileHash(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hasher := sha256.New()
	if _, err := io.Copy(hasher, file); err != nil {
		return "", err
	}

	return fmt.Sprintf("%x", hasher.Sum(nil)), nil
}

// ComputeDataHash computes SHA-256 hash of byte data
func ComputeDataHash(data []byte) string {
	hasher := sha256.New()
	hasher.Write(data)
	return fmt.Sprintf("%x", hasher.Sum(nil))
}

// ParseNodeID extracts IP and port from a nodeID
// Handles both formats: "IP:PORT" and "IP:PORT#INCARNATION"
func ParseNodeID(nodeID string) (string, int, error) {
	parts := strings.Split(nodeID, ":")
	if len(parts) != 2 {
		return "", 0, fmt.Errorf("invalid nodeID format: %s", nodeID)
	}

	// Validate IP part is not empty
	if parts[0] == "" {
		return "", 0, fmt.Errorf("invalid IP in nodeID: %s", nodeID)
	}

	// Strip the incarnation suffix (format: PORT#INCARNATION)
	portStr := parts[1]
	if hashIdx := strings.Index(portStr, "#"); hashIdx != -1 {
		portStr = portStr[:hashIdx]
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		return "", 0, fmt.Errorf("invalid port in nodeID: %s", nodeID)
	}

	return parts[0], port, nil
} // CreateNodeID creates a nodeID from IP, port, and timestamp
func CreateNodeID(ip string, port int, timestamp int64) string {
	return fmt.Sprintf("%s:%d:%d", ip, port, timestamp)
}
