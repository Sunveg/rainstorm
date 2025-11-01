package protocol

import (
	"context"
	"fmt"
	"hydfs/pkg/membership"
	"hydfs/pkg/transport"
	mpb "hydfs/protoBuilds/membership"
	"math/rand"
	"net"
	"time"

	"google.golang.org/protobuf/proto"
)

const budget = 1200

type Protocol struct {
	Table   *membership.Table
	UDP     *transport.UDP
	PQ      *PiggybackQueue
	Logf    func(string, ...interface{})
	FanoutK int
	Sus     *SuspicionManager
	mode    string
	suspOn  bool
}

func NewProtocol(t *membership.Table, udp *transport.UDP, logf func(string, ...interface{}), fanout int) *Protocol {
	p := &Protocol{Table: t, UDP: udp, PQ: NewPiggybackQueue(), Logf: logf, FanoutK: fanout}
	p.mode = "ping"
	p.suspOn = false
	p.Sus = NewSuspicion(4*time.Second, 4*time.Second, logf, t.GetSelf())
	return p
}

func (p *Protocol) Mode() string         { return p.mode }
func (p *Protocol) SuspicionOn() bool    { return p.suspOn }
func (p *Protocol) SetMode(m string)     { p.mode = m }
func (p *Protocol) SetSuspicion(on bool) { p.suspOn = on }

// InitSuspicionGrace initializes last-heard timestamps for ALIVE peers we have
// never heard from before, to avoid immediate SUSPECT storms when suspicion is
// enabled. Peers we have heard from already keep their original last-heard so
// real silence is still detected promptly.
func (p *Protocol) InitSuspicionGrace() {
	now := time.Now()
	self := p.Table.GetSelf()
	for _, e := range p.Table.Snapshot() {
		if e.State != mpb.MemberState_ALIVE {
			continue
		}
		if e.Node.GetIp() == self.GetIp() && e.Node.GetPort() == self.GetPort() {
			continue
		}
		key := membership.StringifyNodeID(e.Node)
		if !p.Sus.HeardSince(key, time.Time{}) {
			p.Sus.OnHearFrom(key, now)
		}
	}
}

func (p *Protocol) Handle(ctx context.Context, env *mpb.Envelope, addr *net.UDPAddr) {
	if env.GetSender() != nil {
		p.Sus.OnHearFrom(membership.StringifyNodeID(env.GetSender()), time.Now())
	}

	switch env.GetType() {
	case mpb.Envelope_JOIN:
		p.onJoin(ctx, env, addr)
	case mpb.Envelope_JOIN_ACK:
		p.onJoinAck(ctx, env, addr)
	case mpb.Envelope_UPDATE_BATCH:
		p.onUpdateBatch(ctx, env, addr)
	case mpb.Envelope_PING:
		p.onPing(p.Table.GetSelf(), env.Sender)
	case mpb.Envelope_ACK:
		p.onACK(env.GetSender())
	default:
		// ignore
	}
}

func (p *Protocol) onJoin(ctx context.Context, env *mpb.Envelope, addr *net.UDPAddr) {
	j := env.GetJoin()
	if j == nil || j.Node == nil {
		return
	}
	p.Logf("JOIN recv from=%s node=%s",
		addr.String(), membership.StringifyNodeID(j.Node))

	// Apply update
	changed := p.Table.ApplyUpdate(&mpb.MembershipEntry{
		Node:         j.Node,
		State:        mpb.MemberState_ALIVE,
		Incarnation:  j.Node.GetIncarnation(),
		LastUpdateMs: uint64(time.Now().UnixMilli()),
	})

	// Enqueue for piggyback if there was a change
	if changed && p.PQ != nil {
		p.PQ.Enqueue(&mpb.MembershipEntry{
			Node:         j.Node,
			State:        mpb.MemberState_ALIVE,
			Incarnation:  j.Node.GetIncarnation(),
			LastUpdateMs: uint64(time.Now().UnixMilli()),
		})
		p.Logf("JOIN fanout: enqueued node=%s; sending immediate gossip",
			membership.StringifyNodeID(j.Node))
		p.fanoutOnce()
	}

	// Send JoinAck with current membership snapshot
	ack := &mpb.JoinAck{
		Node:               j.Node,
		MembershipSnapshot: p.Table.Snapshot(),
		SentMs:             uint64(time.Now().UnixMilli()),
	}

	// Create response envelope
	resp := &mpb.Envelope{
		Version: 1,
		Sender:  p.Table.GetSelf(),
		Type:    mpb.Envelope_JOIN_ACK,
		Payload: &mpb.Envelope_JoinAck{JoinAck: ack},
	}

	// Send if within budget
	if b, err := proto.Marshal(resp); err == nil && len(b) <= budget {
		_ = p.UDP.Send(addr, resp)
	}

	// Additionally, proactively send a full UPDATE_BATCH of current known members
	// to the joining node so it quickly converges even in ping mode.
	{
		entries := p.Table.Snapshot()
		if len(entries) > 0 {
			env := &mpb.Envelope{
				Version:   1,
				Sender:    p.Table.GetSelf(),
				Type:      mpb.Envelope_UPDATE_BATCH,
				RequestId: "joinpush",
				Payload:   &mpb.Envelope_UpdateBatch{UpdateBatch: &mpb.UpdateBatch{Entries: entries}},
			}
			if wire, err := proto.Marshal(env); err == nil && len(wire) <= budget {
				_ = p.UDP.Send(addr, env)
			}
		}
	}
}

func (p *Protocol) onJoinAck(ctx context.Context, env *mpb.Envelope, addr *net.UDPAddr) {
	ack := env.GetJoinAck()
	if ack == nil {
		return
	}

	p.Logf("JOIN_ACK recv from=%s sender=%s snapshot=%d",
		addr.String(), membership.StringifyNodeID(env.GetSender()), len(ack.GetMembershipSnapshot()))

	if n := p.Table.MergeSnapshot(ack.GetMembershipSnapshot()); n > 0 && p.PQ != nil {
		// Enqueue self ALIVE and immediately fan out once so peers hear directly from us
		self := p.Table.GetSelf()
		p.PQ.Enqueue(&mpb.MembershipEntry{
			Node:         self,
			State:        mpb.MemberState_ALIVE,
			Incarnation:  self.GetIncarnation(),
			LastUpdateMs: uint64(time.Now().UnixMilli()),
		})
		p.Logf("JOIN_ACK merge applied=%d", n)
		// Immediate fanout helps prevent premature SUSPECT due to initial silence
		p.fanoutOnce()
	}

}

func (p *Protocol) onUpdateBatch(ctx context.Context, env *mpb.Envelope, addr *net.UDPAddr) {
	b := env.GetUpdateBatch()
	if b == nil {
		return
	}
	if len(b.GetEntries()) == 0 {
		return
	} // ignore heartbeat batches
	source := env.GetRequestId()
	if source == "" {
		source = "unknown"
	}
	p.Logf("UPDATE_BATCH recv from=%s entries=%d source=%s", addr.String(), len(b.GetEntries()), source)
	for _, e := range b.GetEntries() {
		// Ignore remote attempts to change our own state unless it's ALIVE with
		// an equal or higher incarnation. Prevents peers from marking us SUSPECT/DEAD.
		self := p.Table.GetSelf()
		if e.GetNode().GetIp() == self.GetIp() && e.GetNode().GetPort() == self.GetPort() {
			if e.GetState() != mpb.MemberState_ALIVE || e.GetIncarnation() < self.GetIncarnation() {
				continue
			}
		}

		changed := p.Table.ApplyUpdate(e)
		p.Logf("APPLY mode=%s from=%s node=%s state=%v inc=%d changed=%v source=%s",
			p.modeStr(), addr.String(),
			membership.StringifyNodeID(e.Node), e.State, e.Incarnation, changed, source)
		if changed && e.State == mpb.MemberState_ALIVE {
			// Initialize liveness for newly learned peers
			p.Sus.OnHearFrom(membership.StringifyNodeID(e.Node), time.Now())
		}
		if changed && p.PQ != nil {
			p.PQ.Enqueue(e)
		}
	}
}

// Helper: choose up to k alive peers (excluding self)
func (p *Protocol) chooseTargets(k int) []*mpb.NodeID {
	selfKey := membership.StringifyNodeID(p.Table.GetSelf())
	var peers []*mpb.NodeID
	for _, m := range p.Table.GetMembers() {
		if m.State != mpb.MemberState_ALIVE {
			continue
		}
		if membership.StringifyNodeID(m.NodeID) == selfKey {
			continue
		}
		peers = append(peers, m.NodeID)
	}
	rand.Shuffle(len(peers), func(i, j int) { peers[i], peers[j] = peers[j], peers[i] })
	if k > len(peers) {
		k = len(peers)
	}
	return peers[:k]
}

func nodeAddr(n *mpb.NodeID) *net.UDPAddr {
	return &net.UDPAddr{IP: net.ParseIP(n.GetIp()), Port: int(n.GetPort())}
}

// Build and send one UPDATE_BATCH within budget to selected targets
func (p *Protocol) fanoutOnce() {
	if p.PQ == nil || !p.selfAlive() {
		return
	}
	entries := p.PQ.DrainUpToBytes(budget, func(es []*mpb.MembershipEntry) (int, error) {
		env := &mpb.Envelope{
			Version: 1,
			Sender:  p.Table.GetSelf(),
			Type:    mpb.Envelope_UPDATE_BATCH,
			Payload: &mpb.Envelope_UpdateBatch{UpdateBatch: &mpb.UpdateBatch{Entries: es}},
		}
		b, err := proto.Marshal(env)
		if err != nil {
			return 0, err
		}
		return len(b), nil
	})
	if len(entries) == 0 {
		// send empty UPDATE_BATCH as heartbeat
		hb := &mpb.Envelope{
			Version: 1,
			Sender:  p.Table.GetSelf(),
			Type:    mpb.Envelope_UPDATE_BATCH,
			Payload: &mpb.Envelope_UpdateBatch{UpdateBatch: &mpb.UpdateBatch{}},
		}
		targets := p.chooseTargets(p.FanoutK)
		for _, n := range targets {
			_ = p.UDP.Send(nodeAddr(n), hb)
		}
		return
	}
	env := &mpb.Envelope{
		Version:   1,
		Sender:    p.Table.GetSelf(),
		Type:      mpb.Envelope_UPDATE_BATCH,
		RequestId: "gossip",
		Payload:   &mpb.Envelope_UpdateBatch{UpdateBatch: &mpb.UpdateBatch{Entries: entries}},
	}
	wire, err := proto.Marshal(env)
	if err != nil || len(wire) > budget {
		return
	}

	p.Logf("GOSSIP fanout entries=%d", len(entries))
	targets := p.chooseTargets(p.FanoutK)
	p.Logf("GOSSIP targets=%d", len(targets))
	for _, n := range targets {
		p.Logf("GOSSIP send to=%s", membership.StringifyNodeID(n))
		_ = p.UDP.Send(nodeAddr(n), env)
	}
}

func (p *Protocol) selfAlive() bool {
	self := p.Table.GetSelf()
	for _, e := range p.Table.Snapshot() {
		if e.GetNode().GetIp() == self.GetIp() && e.GetNode().GetPort() == self.GetPort() {
			return e.GetState() == mpb.MemberState_ALIVE
		}
	}
	return true
}

func (p *Protocol) modeStr() string {
	sus := "nosuspect"
	if p.SuspicionOn() {
		sus = "suspect"
	}
	return p.Mode() + " " + sus
}

// SendJoin sends a join request to an introducer
func (p *Protocol) SendJoin(ctx context.Context, introducerAddr *net.UDPAddr) error {
	join := &mpb.Join{
		Node:   p.Table.GetSelf(),
		SentMs: uint64(time.Now().UnixMilli()),
	}

	env := &mpb.Envelope{
		Version:   1,
		Sender:    p.Table.GetSelf(),
		Type:      mpb.Envelope_JOIN,
		RequestId: fmt.Sprintf("join-%d", time.Now().UnixNano()),
		Payload:   &mpb.Envelope_Join{Join: join},
	}

	if err := p.UDP.Send(introducerAddr, env); err != nil {
		return fmt.Errorf("failed to send join request: %v", err)
	}

	p.Logf("JOIN sent to %s", introducerAddr.String())
	return nil
}
