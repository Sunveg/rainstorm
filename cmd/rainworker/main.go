package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"hydfs/pkg/membership"
	"hydfs/pkg/protocol"
	"hydfs/pkg/rainstorm"
	"hydfs/pkg/transport"
	"log"
	"net"
	"net/http"
	"os"
	"time"
)

// WorkerConfig holds configuration for a worker VM process.
type WorkerConfig struct {
	IP         string
	UDPPort    int
	Introducer string
}

// workerMembership bundles membership runtime for the worker.
type workerMembership struct {
	Table *membership.Table
	Proto *protocol.Protocol
	Ctx   context.Context
}

func main() {
	cfg := parseWorkerConfig()
	logger := newWorkerLogger()

	mr := startWorkerMembership(cfg, logger)
	joinClusterIfNeeded(cfg, mr, logger)

	workerManager := rainstorm.NewWorkerManager(logger)

	controlPort := cfg.UDPPort + 3000
	startWorkerHTTPServer(cfg.IP, controlPort, workerManager, logger)
}

// parseWorkerConfig parses CLI flags into WorkerConfig.
func parseWorkerConfig() WorkerConfig {
	ipFlag := flag.String("ip", "127.0.0.1", "local IP for membership")
	udpPortFlag := flag.Int("port", 8001, "UDP port for membership SWIM")
	introducerFlag := flag.String("introducer", "", "introducer ip:port (empty means this node is introducer)")
	flag.Parse()

	return WorkerConfig{
		IP:         *ipFlag,
		UDPPort:    *udpPortFlag,
		Introducer: *introducerFlag,
	}
}

// newWorkerLogger logs to stdout with a clear prefix.
func newWorkerLogger() *log.Logger {
	return log.New(os.Stdout, "[rainworker] ", log.LstdFlags|log.Lmicroseconds)
}

// startWorkerMembership starts membership + gossip for this worker VM.
func startWorkerMembership(cfg WorkerConfig, logger *log.Logger) *workerMembership {
	incarnation := uint64(time.Now().UnixMilli())
	selfNodeID, err := membership.NewNodeID(cfg.IP, uint32(cfg.UDPPort), incarnation)
	if err != nil {
		logger.Fatalf("failed to create NodeID: %v", err)
	}

	table := membership.NewTable(selfNodeID, func(format string, args ...interface{}) {
		logger.Printf("[MEM] "+format, args...)
	})

	udpAddr := fmt.Sprintf("%s:%d", cfg.IP, cfg.UDPPort)
	udp, err := transport.NewUDP(udpAddr, nil)
	if err != nil {
		logger.Fatalf("failed to create UDP transport on %s: %v", udpAddr, err)
	}

	proto := protocol.NewProtocol(table, udp, func(format string, args ...interface{}) {
		logger.Printf("[SWIM] "+format, args...)
	}, 3)
	udp.SetHandler(proto.Handle)

	ctx := context.Background()
	go func() {
		if err := udp.Serve(ctx); err != nil {
			logger.Printf("UDP server error: %v", err)
		}
	}()

	// Gossip-only mode (no suspicion / ping-based failure detection).
	proto.SetMode("gossip")
	proto.SetSuspicion(false)
	_ = protocol.StartGossip(ctx, proto, 1*time.Second, 0.2)

	return &workerMembership{
		Table: table,
		Proto: proto,
		Ctx:   ctx,
	}
}

// joinClusterIfNeeded is shared idea with leader: join via introducer if set.
func joinClusterIfNeeded(cfg WorkerConfig, mr *workerMembership, logger *log.Logger) {
	if cfg.Introducer == "" {
		return
	}

	introAddr, err := net.ResolveUDPAddr("udp", cfg.Introducer)
	if err != nil {
		logger.Fatalf("invalid introducer address %q: %v", cfg.Introducer, err)
	}
	if err := mr.Proto.SendJoin(mr.Ctx, introAddr); err != nil {
		logger.Fatalf("failed to send JOIN: %v", err)
	}
	logger.Printf("JOIN sent to introducer %s", cfg.Introducer)
}

// startWorkerHTTPServer exposes WorkerManager over a simple HTTP+JSON API.
func startWorkerHTTPServer(ip string, controlPort int, wm *rainstorm.WorkerManager, logger *log.Logger) {
	mux := http.NewServeMux()

	mux.HandleFunc("/create_worker", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		defer r.Body.Close()

		var spec rainstorm.TaskSpec
		if err := json.NewDecoder(r.Body).Decode(&spec); err != nil {
			http.Error(w, "invalid JSON body", http.StatusBadRequest)
			return
		}

		ti, err := wm.CreateWorker(spec)
		if err != nil {
			logger.Printf("CreateWorker error: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(ti)
	})

	mux.HandleFunc("/kill_worker", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		defer r.Body.Close()

		var id rainstorm.TaskID
		if err := json.NewDecoder(r.Body).Decode(&id); err != nil {
			http.Error(w, "invalid JSON body", http.StatusBadRequest)
			return
		}

		if err := wm.KillWorker(id); err != nil {
			logger.Printf("KillWorker error: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
	})

	addr := fmt.Sprintf("%s:%d", ip, controlPort)
	logger.Printf("WorkerManager HTTP listening on %s", addr)

	server := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Fatalf("HTTP server error: %v", err)
	}
}
