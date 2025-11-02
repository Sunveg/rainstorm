package protocol

import (
	"context"
	"hydfs/pkg/membership"
	mpb "hydfs/protoBuilds/membership"
	"time"

	"google.golang.org/protobuf/proto"
)

func (p *Protocol) StartPingAck(ctx context.Context, period time.Duration, ackMs time.Duration) {
	t := time.NewTimer(period)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if p.Mode() != "ping" || !p.selfAlive() {
				t.Reset(period)
				continue
			}
			// pick one target
			targets := p.chooseTargets(1)
			if len(targets) == 0 {
				t.Reset(period)
				continue
			}
			dst := targets[0]
			// drain piggyback under budget
			env := p.buildUpdateBatchEnvelope() // may be nil if no entries
			// build PING with piggyback optionally attached (we attach neither in proto; just send UPDATE first if present)
			if env != nil {
				_ = p.UDP.Send(nodeAddr(dst), env)
			}
			p.sendPING(dst)

			// wait for ACK
			key := membership.StringifyNodeID(dst)

			sentAt := time.Now()
			timer := time.NewTimer(ackMs)
			select {
			case <-timer.C:
				if p.Mode() != "ping" || !p.selfAlive() {
					t.Reset(period)
					continue
				}
				if p.SuspicionOn() && !p.Sus.HeardSince(key, sentAt) {
					p.Logf("DETECT mode=%s reason=no-ack node=%s action=SUSPECT",
						p.modeStr(), membership.StringifyNodeID(dst))
					if e := p.Sus.OnNoAck(key, dst, time.Now()); e != nil {
						p.PQ.Enqueue(e)
					}
				} else if !p.SuspicionOn() && !p.Sus.HeardSince(key, sentAt) {
					// With nosuspect, start a fail timer only for the probed node
					p.Sus.OnNoAckNoSuspect(key, dst, time.Now())
				}
			case <-ctx.Done():
				timer.Stop()
				return
			}
			timer.Stop()
			if p.SuspicionOn() {
				// In ping+suspect, only promote existing suspects (from missed ACKs)
				entries := p.Sus.TickPromote(time.Now(), p.Table.Snapshot())
				for _, e := range entries {
					if p.Table.ApplyUpdate(e) {
						p.PQ.Enqueue(e)
					}
				}
			} else {
				entries := p.Sus.TickNoSuspect(time.Now(), p.Table.Snapshot())
				for _, e := range entries {
					if p.Table.ApplyUpdate(e) {
						p.PQ.Enqueue(e)
					}
				}
			}
			// GC DEAD entries periodically (same as gossip loop)
			_ = p.Table.GCStates(5*time.Second, true)

			// Periodically fanout updates to help converge membership faster in ping mode
			// This ensures updates don't just sit in the piggyback queue
			p.fanoutOnce()

			t.Reset(period)
		}
	}
}

func (p *Protocol) sendPING(dst *mpb.NodeID) {
	env := &mpb.Envelope{
		Version: 1,
		Sender:  p.Table.GetSelf(),
		Type:    mpb.Envelope_PING,
	}
	//p.Logf("PING send mode=%s dst=%s",
	//	p.modeStr(), membership.StringifyNodeID(dst))
	_ = p.UDP.Send(nodeAddr(dst), env)
}

func (p *Protocol) onPing(dst *mpb.NodeID, from *mpb.NodeID) {
	// reply Ack, include piggyback if any
	//p.Logf("PING recv mode=%s from=%s",
	//	p.modeStr(), membership.StringifyNodeID(from))
	env := p.buildUpdateBatchEnvelope()
	if env != nil {
		_ = p.UDP.Send(nodeAddr(from), env)
	}
	ack := &mpb.Envelope{Version: 1, Sender: p.Table.GetSelf(), Type: mpb.Envelope_ACK}
	_ = p.UDP.Send(nodeAddr(from), ack)
	//p.Logf("ACK send mode=%s to=%s",
	//	p.modeStr(), membership.StringifyNodeID(from))
}

func (p *Protocol) onACK(from *mpb.NodeID) {
	//p.Logf("ACK recv mode=%s from=%s",
	//	p.modeStr(), membership.StringifyNodeID(from))
	// mark heard; any waiting goroutine will see this through Suspicion path
	if p.SuspicionOn() {
		p.Sus.OnHearFrom(membership.StringifyNodeID(from), time.Now())
	}
}

func (p *Protocol) buildUpdateBatchEnvelope() *mpb.Envelope {
	if !p.selfAlive() {
		return nil
	}
	entries := p.PQ.DrainUpToBytes(budget, func(es []*mpb.MembershipEntry) (int, error) {
		e := &mpb.Envelope{Version: 1, Sender: p.Table.GetSelf(), Type: mpb.Envelope_UPDATE_BATCH,
			RequestId: "piggyback",
			Payload:   &mpb.Envelope_UpdateBatch{UpdateBatch: &mpb.UpdateBatch{Entries: es}}}
		b, err := proto.Marshal(e)
		if err != nil {
			return 0, err
		}
		return len(b), nil
	})
	if len(entries) == 0 {
		return nil
	}
	return &mpb.Envelope{
		Version:   1,
		Sender:    p.Table.GetSelf(),
		Type:      mpb.Envelope_UPDATE_BATCH,
		RequestId: "piggyback",
		Payload:   &mpb.Envelope_UpdateBatch{UpdateBatch: &mpb.UpdateBatch{Entries: entries}},
	}
}
