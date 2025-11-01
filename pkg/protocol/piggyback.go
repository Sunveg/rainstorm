package protocol

import (
	"fmt"
	s "hydfs/pkg/membership"
	mpb "hydfs/protoBuilds/membership"
	"sync"
)

type PiggybackQueue struct {
	mu     sync.Mutex
	latest map[string]*mpb.MembershipEntry // nodeKey (ip:port) -> newest entry
	order  []string
}

func NewPiggybackQueue() *PiggybackQueue {
	return &PiggybackQueue{latest: make(map[string]*mpb.MembershipEntry)}
}

func nodeKey(n *mpb.NodeID) string {
	return n.GetIp() + ":" + fmt.Sprint(n.GetPort())
}

func (q *PiggybackQueue) Enqueue(e *mpb.MembershipEntry) {
	if e == nil || e.Node == nil {
		return
	}
	key := nodeKey(e.Node)
	q.mu.Lock()
	defer q.mu.Unlock()
	if cur, ok := q.latest[key]; ok {
		if e.Incarnation < cur.Incarnation {
			return
		}
		if e.Incarnation == cur.Incarnation && s.Precedence(e.State) <= s.Precedence(cur.State) {
			return
		}
	}
	if _, existed := q.latest[key]; !existed {
		q.order = append(q.order, key)
	}
	q.latest[key] = e
}

// Drain using a marshal-prober to respect the 1200B budget
func (q *PiggybackQueue) DrainUpToBytes(budget int, marshal func([]*mpb.MembershipEntry) (int, error)) []*mpb.MembershipEntry {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.order) == 0 {
		return nil
	}
	var picked []*mpb.MembershipEntry
	var pickedKeys []string

	for _, k := range q.order {
		e := q.latest[k]
		try := append(picked, e)
		size, err := marshal(try)
		if err != nil {
			continue
		}
		if size > budget {
			if len(picked) == 0 {
				// single entry too large; skip it
				continue
			}
			break
		}
		picked = try
		pickedKeys = append(pickedKeys, k)
	}
	if len(picked) == 0 {
		return nil
	}
	// Remove picked
	rm := map[string]struct{}{}
	for _, k := range pickedKeys {
		delete(q.latest, k)
		rm[k] = struct{}{}
	}
	rest := make([]string, 0, len(q.order)-len(pickedKeys))
	for _, k := range q.order {
		if _, gone := rm[k]; !gone {
			rest = append(rest, k)
		}
	}
	q.order = rest
	return picked
}
