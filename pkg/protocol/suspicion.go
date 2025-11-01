package protocol

import (
	"hydfs/pkg/membership"
	mpb "hydfs/protoBuilds/membership"
	"sync"
	"time"
)

type SuspicionManager struct {
	mu        sync.Mutex
	lastHeard map[string]time.Time
	suspectAt map[string]time.Time
	failAt    map[string]time.Time
	Tfail     time.Duration
	Tcleanup  time.Duration
	Logf      func(string, ...interface{})
	Self      *mpb.NodeID
	// GraceUntil: before this time, Tick will not create new SUSPECTs from silence.
	// It still promotes already-suspected nodes to DEAD.
	GraceUntil time.Time
}

func NewSuspicion(tfail, tcleanup time.Duration, logf func(string, ...interface{}), self *mpb.NodeID) *SuspicionManager {
	return &SuspicionManager{
		lastHeard: make(map[string]time.Time),
		suspectAt: make(map[string]time.Time),
		failAt:    make(map[string]time.Time),
		Tfail:     tfail,
		Tcleanup:  tcleanup,
		Logf:      logf,
		Self:      self,
	}
}

// Call whenever we receive any message from nodeKey.
func (s *SuspicionManager) OnHearFrom(nodeKey string, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// If this node was suspected or under no-suspect fail timer, log refutation
	_, wasSus := s.suspectAt[nodeKey]
	_, hadFail := s.failAt[nodeKey]
	if wasSus || hadFail {
		s.Logf("Was sus %s (heard again)", nodeKey)
	}
	s.lastHeard[nodeKey] = now
	delete(s.suspectAt, nodeKey)
	delete(s.failAt, nodeKey)
}

// StartGrace sets a temporary window during which Tick will not create
// new SUSPECT entries from silence. Use when enabling suspicion to avoid
// cold-start mass suspicion. Promotions still occur.
func (s *SuspicionManager) StartGrace(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.GraceUntil = time.Now().Add(d)
}

func (s *SuspicionManager) HeardSince(key string, since time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastHeard[key].After(since)
}

// Call when a probe to nodeKey fails (PingAck path)
// Returns a SUSPECT entry to disseminate (or nil if already suspected)
func (s *SuspicionManager) OnNoAck(nodeKey string, node *mpb.NodeID, now time.Time) *mpb.MembershipEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.suspectAt[nodeKey]; ok {
		return nil
	}
	s.suspectAt[nodeKey] = now.Add(s.Tfail)
	s.Logf("SUSPECT %s", nodeKey)
	return &mpb.MembershipEntry{
		Node:         node,
		State:        mpb.MemberState_SUSPECTED,
		Incarnation:  node.GetIncarnation(),
		LastUpdateMs: uint64(now.UnixMilli()),
	}
}

// OnNoAckNoSuspect is used when suspicion is OFF. It starts a fail timer for the
// probed node. If the node remains silent until the deadline, TickNoSuspect will
// emit a DEAD update. Any message from the node cancels this via OnHearFrom.
func (s *SuspicionManager) OnNoAckNoSuspect(nodeKey string, node *mpb.NodeID, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.failAt[nodeKey]; ok {
		return
	}
	s.failAt[nodeKey] = now.Add(s.Tfail)
	s.Logf("NOACK start %s (nosuspect)", nodeKey)
}

// TickNoSuspect promotes nodes with expired fail timers directly to DEAD.
// Returns entries to disseminate.
func (s *SuspicionManager) TickNoSuspect(now time.Time, members []*mpb.MembershipEntry) []*mpb.MembershipEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	var output []*mpb.MembershipEntry
	for key, deadline := range s.failAt {
		if now.After(deadline) {
			delete(s.failAt, key)
			for _, e := range members {
				if membership.StringifyNodeID(e.Node) == key {
					output = append(output, &mpb.MembershipEntry{
						Node:         e.Node,
						State:        mpb.MemberState_DEAD,
						Incarnation:  e.Incarnation,
						LastUpdateMs: uint64(now.UnixMilli()),
					})
					s.Logf("DEAD %s (nosuspect)", key)
					break
				}
			}
		}
	}
	return output
}

// Tick drives promotions: gossip-silence -> SUSPECT; SUSPECT -> FAULTY after Tfail and Tcleanup.
// Returns entries to disseminate (0, 1, or many).
func (s *SuspicionManager) Tick(now time.Time, members []*mpb.MembershipEntry) []*mpb.MembershipEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	var output []*mpb.MembershipEntry

	// Mark suspect -> failed if more than Tfail has passed
	for _, e := range members {
		key := membership.StringifyNodeID(e.Node)
		// only consider ALIVE peers for new suspicion
		if e.Node.GetIp() == s.Self.GetIp() && e.Node.GetPort() == s.Self.GetPort() {
			continue
		}
		if e.State != mpb.MemberState_ALIVE {
			continue
		}
		// If we are within grace, do not create new SUSPECTs from silence
		if now.Before(s.GraceUntil) {
			continue
		}
		last := s.lastHeard[key]
		// First-seen grace: initialize lastHeard for newly learned peers and skip
		if last.IsZero() {
			s.lastHeard[key] = now
			continue
		}
		// silence beyond Tfail => enter SUSPECT
		if now.Sub(last) > s.Tfail {
			if _, already := s.suspectAt[key]; !already {
				s.suspectAt[key] = now.Add(s.Tcleanup) // promote after Tcleanup
				s.Logf("SUSPECT %s (silence)", key)
				output = append(output, &mpb.MembershipEntry{
					Node:         e.Node,
					State:        mpb.MemberState_SUSPECTED,
					Incarnation:  e.Incarnation,
					LastUpdateMs: uint64(now.UnixMilli()),
				})
			}
		}
	}

	// Promote expired SUSPECT to DEAD
	for key, deadline := range s.suspectAt {
		if now.After(deadline) {
			delete(s.suspectAt, key)
			// We must find the nodeId from members param
			for _, e := range members {
				if membership.StringifyNodeID(e.Node) == key {
					output = append(output, &mpb.MembershipEntry{
						Node:         e.Node,
						State:        mpb.MemberState_DEAD,
						Incarnation:  e.Incarnation,
						LastUpdateMs: uint64(now.UnixMilli()),
					})
					s.Logf("DEAD %s", key)
					break
				}
			}
		}
	}
	return output
}

// TickPromote only promotes already-suspected nodes to DEAD when their
// suspicion deadline expires. It does NOT create new SUSPECT entries from silence.
// Used by ping+suspect where suspicion is initiated only by missed ACKs.
func (s *SuspicionManager) TickPromote(now time.Time, members []*mpb.MembershipEntry) []*mpb.MembershipEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	var output []*mpb.MembershipEntry
	for key, deadline := range s.suspectAt {
		if now.After(deadline) {
			delete(s.suspectAt, key)
			for _, e := range members {
				if membership.StringifyNodeID(e.Node) == key {
					output = append(output, &mpb.MembershipEntry{
						Node:         e.Node,
						State:        mpb.MemberState_DEAD,
						Incarnation:  e.Incarnation,
						LastUpdateMs: uint64(now.UnixMilli()),
					})
					s.Logf("DEAD %s", key)
					break
				}
			}
		}
	}
	return output
}
