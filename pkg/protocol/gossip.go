package protocol

import (
	"context"
	"hydfs/pkg/membership"
	mpb "hydfs/protoBuilds/membership"
	"time"
)

func StartGossip(ctx context.Context, p *Protocol, period time.Duration, jitterFrac float64) (stop func()) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		randJitter := func() time.Duration {
			j := 1 + (jitterFrac*(0.5-float64(time.Now().UnixNano()%1000)/1000.0))*2
			if j < 0.5 {
				j = 0.5
			}
			if j > 1.5 {
				j = 1.5
			}
			return time.Duration(j * float64(period))
		}
		timer := time.NewTimer(randJitter())
		defer timer.Stop()
		for {
			select {
			// stop when the parent context is canceled
			case <-ctx.Done():
				return
			// on each tick, call p.fanoutOnce() to send one UPDATE_BATCH to k random peers, then reset timer with a new jittered period.
			case <-timer.C:
				// Always run GC periodically regardless of mode to avoid DEAD entries lingering
				_ = p.Table.GCStates(5*time.Second, true)
				if p.Mode() != "gossip" || !p.selfAlive() {
					timer.Reset(randJitter())
					continue
				}
				if p.SuspicionOn() {
					entries := p.Sus.Tick(time.Now(), p.Table.Snapshot())
					for _, e := range entries {
						if p.Table.ApplyUpdate(e) {
							p.Logf("DETECT mode=%s node=%s new_state=%v",
								p.modeStr(), membership.StringifyNodeID(e.Node), e.State)
							p.PQ.Enqueue(e)
						}
					}
				} else {
					now := time.Now()
					snap := p.Table.Snapshot()
					self := p.Table.GetSelf()
					for _, e := range snap {
						if e.State != mpb.MemberState_ALIVE {
							continue
						}
						if e.Node.GetIp() == self.GetIp() && e.Node.GetPort() == self.GetPort() {
							continue
						}

						key := membership.StringifyNodeID(e.Node)
						// First-seen grace: init lastHeard, skip this round
						if !p.Sus.HeardSince(key, time.Time{}) {
							p.Sus.OnHearFrom(key, now)
							continue
						}
						// Mark DEAD only if truly silent for > Tfail
						if !p.Sus.HeardSince(key, now.Add(-p.Sus.Tfail)) {
							dead := &mpb.MembershipEntry{
								Node: e.Node, State: mpb.MemberState_DEAD,
								Incarnation: e.Incarnation, LastUpdateMs: uint64(now.UnixMilli()),
							}
							if p.Table.ApplyUpdate(dead) {
								p.Logf("DETECT mode=%s reason=silence>tfail node=%s new_state=DEAD",
									p.modeStr(), membership.StringifyNodeID(e.Node))
								p.PQ.Enqueue(dead)
							}
						}
					}
				}
				// GC already executed at the top of the tick

				p.fanoutOnce()
				timer.Reset(randJitter())
			}
		}
	}()
	return func() { /* cancel via ctx */ }
}
