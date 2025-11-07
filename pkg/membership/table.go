package membership

import (
	"fmt"
	"sync"
	"time"

	mpb "hydfs/protoBuilds/membership"
)

type Member struct {
	NodeID      *mpb.NodeID
	State       mpb.MemberState
	Incarnation uint64
	LastUpdate  time.Time
}

type Table struct {
	mu                 sync.RWMutex //thread safety - making sure multiple goroutinescan safely access the same data without causing problems.
	self               *mpb.NodeID
	members            map[string]*Member // key: StringifyNodeID(node)
	logger             func(string, ...interface{})
	onMembershipChange func() // callback for when membership changes occur
}

func Precedence(s mpb.MemberState) int {
	switch s {
	case mpb.MemberState_DEAD:
		return 4
	case mpb.MemberState_LEFT:
		return 3
	case mpb.MemberState_SUSPECTED:
		return 2
	case mpb.MemberState_ALIVE:
		return 1
	default:
		return 0
	}
}

func isNewer(newInc, oldInc uint64, newState, oldState mpb.MemberState) bool {
	if newInc > oldInc {
		return true
	}
	if newInc < oldInc {
		return false
	}
	return Precedence(newState) > Precedence(oldState)
}

// Find existing key for a node by ip:port (handles older map keys that include incarnation)
func (t *Table) findKeyByIPPort(ip string, port uint32) (string, *Member, bool) {
	for k, m := range t.members {
		if m.NodeID.GetIp() == ip && m.NodeID.GetPort() == port {
			return k, m, true
		}
	}
	return "", nil, false
}

func (t *Table) Snapshot() []*mpb.MembershipEntry {
	t.mu.RLock()
	defer t.mu.RUnlock()
	entries := make([]*mpb.MembershipEntry, 0, len(t.members))
	nowMs := uint64(time.Now().UnixMilli())
	for _, m := range t.members {
		entries = append(entries, &mpb.MembershipEntry{
			Node:         m.NodeID,
			State:        m.State,
			Incarnation:  m.Incarnation,
			LastUpdateMs: nowMs,
		})
	}
	return entries
}

func NewTable(self *mpb.NodeID, logger func(string, ...interface{})) *Table {
	// Create a new table
	t := &Table{
		self:               self,
		members:            make(map[string]*Member),
		logger:             logger,
		onMembershipChange: nil, // Will be set later via SetMembershipChangeCallback
	}
	// Add self as ALIVE
	t.addSelf()
	return t
}

// function to add self to the table
func (t *Table) addSelf() {
	key := StringifyNodeID(t.self)
	t.members[key] = &Member{
		NodeID:      t.self,
		State:       mpb.MemberState_ALIVE,
		Incarnation: t.self.GetIncarnation(),
		LastUpdate:  time.Now(),
	}
	t.logger("Added self: %s", key)
}

// ApplyUpdate processes a membership entry
func (t *Table) ApplyUpdate(entry *mpb.MembershipEntry) bool {
	if entry == nil || entry.Node == nil {
		return false
	}
	ip := entry.Node.GetIp()
	port := entry.Node.GetPort()
	now := time.Now()

	t.mu.Lock()
	defer t.mu.Unlock()

	existingKey, existing, exists := t.findKeyByIPPort(ip, port)
	if !exists {
		// First time we see this ip:port
		key := StringifyNodeID(entry.Node) // may include incarnation; ok for new insert
		t.members[key] = &Member{
			NodeID:      entry.Node,
			State:       entry.State,
			Incarnation: entry.Incarnation,
			LastUpdate:  now,
		}
		t.logger("Added new member: %s state=%v", key, entry.State)
		if t.onMembershipChange != nil {
			go t.onMembershipChange() // Run callback in goroutine to avoid deadlock
		}
		return true
	}

	// Decide if improvement
	if !isNewer(entry.Incarnation, existing.Incarnation, entry.State, existing.State) {
		return false
	}

	// If incarnation changed, move map key to the new StringifyNodeID
	newKey := StringifyNodeID(entry.Node)
	if newKey != existingKey {
		delete(t.members, existingKey)
	}
	t.members[newKey] = &Member{
		NodeID:      entry.Node,
		State:       entry.State,
		Incarnation: entry.Incarnation,
		LastUpdate:  now,
	}
	t.logger("Updated member: %s state=%v", newKey, entry.State)
	if t.onMembershipChange != nil {
		go t.onMembershipChange() // Run callback in goroutine to avoid deadlock
	}
	return true
}

// Merge a received snapshot; returns how many entries changed
func (t *Table) MergeSnapshot(entries []*mpb.MembershipEntry) int {
	changed := 0
	for _, e := range entries {
		if t.ApplyUpdate(e) {
			changed++
		}
	}
	return changed
}

// Remove entries in terminal states after a TTL.
// Returns number of removed entries.
func (t *Table) GCStates(ttl time.Duration, removeLeft bool) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now()
	removed := 0
	for key, m := range t.members {
		if m.State == mpb.MemberState_DEAD && now.Sub(m.LastUpdate) >= ttl {
			delete(t.members, key)
			removed++
			continue
		}
		if removeLeft && m.State == mpb.MemberState_LEFT && now.Sub(m.LastUpdate) >= ttl {
			delete(t.members, key)
			removed++
		}
	}
	if removed > 0 {
		if t.logger != nil {
			t.logger("GC removed %d entries", removed)
		}
		if t.onMembershipChange != nil {
			go t.onMembershipChange() // Run callback in goroutine to avoid deadlock
		}
	}
	return removed
}

func (t *Table) GetMembers() []*Member {
	t.mu.RLock()         // Lock for reading
	defer t.mu.RUnlock() // Unlock when function exits

	result := make([]*Member, 0, len(t.members))
	for _, member := range t.members {
		result = append(result, &Member{
			NodeID:      member.NodeID,
			State:       member.State,
			Incarnation: member.Incarnation,
			LastUpdate:  member.LastUpdate,
		})
	}
	return result
}

func (t *Table) GetSelf() *mpb.NodeID {
	return t.self
}

func (t *Table) String() string {
	t.mu.RLock()         // Lock for reading
	defer t.mu.RUnlock() // Unlock when function exits

	result := fmt.Sprintf("Membership Table (%d members):\n", len(t.members))
	for key, member := range t.members {
		result += fmt.Sprintf("  %s: state=%v incarnation=%d last_update=%v\n",
			key, member.State, member.Incarnation, member.LastUpdate)
	}
	return result
}

// SetMembershipChangeCallback sets the callback function to be called when membership changes
func (t *Table) SetMembershipChangeCallback(callback func()) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.onMembershipChange = callback
}
