package transport

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"

	mpb "hydfs/protoBuilds/membership"

	"google.golang.org/protobuf/proto"
)

type Handler func(ctx context.Context, env *mpb.Envelope, addr *net.UDPAddr)

type UDPStats struct {
	ReceivedPackets  uint64
	DroppedPackets   uint64
	DeliveredPackets uint64
	SentBytes        uint64
	RecvBytes        uint64
	DropRate         float64
	Since            time.Time
}

type UDP struct {
	conn       *net.UDPConn
	handler    Handler
	handlerMu  sync.Mutex   // Protect handler updates
	dropRate   atomic.Value // float64
	recvCount  uint64
	dropCount  uint64
	delivCount uint64
	sentBytes  uint64
	recvBytes  uint64
	statsSince atomic.Value // time.Time
}

func NewUDP(bindAddr string, handler Handler) (*UDP, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", bindAddr)
	if err != nil {
		return nil, err
	}
	c, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, err
	}
	u := &UDP{conn: c, handler: handler}
	u.dropRate.Store(float64(0))
	u.statsSince.Store(time.Now())
	return u, nil
}

func (u *UDP) SetDropRate(rate float64) {
	if rate < 0 {
		rate = 0
	}
	if rate > 1 {
		rate = 1
	}
	u.dropRate.Store(rate)
}

func (u *UDP) SetHandler(handler Handler) {
	u.handlerMu.Lock()
	defer u.handlerMu.Unlock()
	u.handler = handler
}

func (u *UDP) Close() error { return u.conn.Close() }

func (u *UDP) Addr() net.Addr { return u.conn.LocalAddr() }

func (u *UDP) Send(to *net.UDPAddr, env *mpb.Envelope) error {
	b, err := proto.Marshal(env)
	if err != nil {
		return err
	}
	if len(b) > 1200 {
		return fmt.Errorf("oversize datagram %dB > 1200B", len(b))
	}
	atomic.AddUint64(&u.sentBytes, uint64(len(b)))
	_, err = u.conn.WriteToUDP(b, to)
	return err
}

func (u *UDP) Serve(ctx context.Context) error {
	buf := make([]byte, 64*1024)
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	for {
		u.conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		n, addr, err := u.conn.ReadFromUDP(buf)
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			continue
		}
		if err != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			continue
		}
		// ... receive packet ...
		atomic.AddUint64(&u.recvCount, 1)
		atomic.AddUint64(&u.recvBytes, uint64(n))

		// ARTIFICIAL DROP - this is drop simulation!
		if rnd.Float64() < u.dropRate.Load().(float64) {
			atomic.AddUint64(&u.dropCount, 1)
			continue // <-- DROP the message, don't process it
		}
		var env mpb.Envelope
		if err := proto.Unmarshal(buf[:n], &env); err != nil {
			// drop malformed
			atomic.AddUint64(&u.dropCount, 1)
			continue
		}
		atomic.AddUint64(&u.delivCount, 1)
		u.handlerMu.Lock()
		handler := u.handler
		u.handlerMu.Unlock()
		if handler != nil {
			handler(ctx, &env, addr)
		}
	}
}

func (u *UDP) Stats() UDPStats {
	since, _ := u.statsSince.Load().(time.Time)
	return UDPStats{
		ReceivedPackets:  atomic.LoadUint64(&u.recvCount),
		DroppedPackets:   atomic.LoadUint64(&u.dropCount),
		DeliveredPackets: atomic.LoadUint64(&u.delivCount),
		SentBytes:        atomic.LoadUint64(&u.sentBytes),
		RecvBytes:        atomic.LoadUint64(&u.recvBytes),
		DropRate:         u.dropRate.Load().(float64),
		Since:            since,
	}
}

func (u *UDP) ResetStats() {
	atomic.StoreUint64(&u.recvCount, 0)
	atomic.StoreUint64(&u.dropCount, 0)
	atomic.StoreUint64(&u.delivCount, 0)
	atomic.StoreUint64(&u.sentBytes, 0)
	atomic.StoreUint64(&u.recvBytes, 0)
	u.statsSince.Store(time.Now())
}
