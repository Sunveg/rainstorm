package membership

import (
	"fmt"
	mpb "hydfs/protoBuilds/membership"
	"net"
)

// StringifyNodeID returns a stable unique string for logging/keys.
func StringifyNodeID(n *mpb.NodeID) string {
	if n == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%s:%d#%d", n.GetIp(), n.GetPort(), n.GetIncarnation())
}

// NewNodeID constructs a NodeID with validated ip and given port/incarnation.
func NewNodeID(ip string, port uint32, incarnation uint64) (*mpb.NodeID, error) {
	if parsed := net.ParseIP(ip); parsed == nil {
		return nil, fmt.Errorf("invalid ip: %s", ip)
	}
	if port == 0 {
		return nil, fmt.Errorf("port must be > 0")
	}
	return &mpb.NodeID{Ip: ip, Port: port, Incarnation: incarnation}, nil
}
