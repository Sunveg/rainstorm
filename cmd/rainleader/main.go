package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"hydfs/pkg/membership"
	"hydfs/pkg/protocol"
	"hydfs/pkg/rainstorm"
	"hydfs/pkg/transport"
	mpb "hydfs/protoBuilds/membership"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"time"
)

type WorkerInfo struct {
	ID      string
	Host    string
	Port    int
	Status  string // working or idle or whatevs
	TaskIDs []rainstorm.TaskID
}

type Leader struct {
	workerList      []WorkerInfo
	taskInfos       map[rainstorm.TaskID]*rainstorm.TaskInfo
	logger          *log.Logger
	membershipTable *membership.Table
}

// LeaderConfig holds all configuration parsed from CLI flags.
type LeaderConfig struct {
	JobID      string
	NumTasks   int
	IP         string
	UDPPort    int
	Introducer string
}

// membershipRuntime bundles objects related to the membership subsystem.
type membershipRuntime struct {
	Table *membership.Table
	Proto *protocol.Protocol
	Ctx   context.Context
}

func main() {
	cfg := parseLeaderConfig()

	logger := newLeaderLogger(cfg.JobID)
	memRuntime := startMembershipRuntime(cfg, logger)
	joinClusterIfNeeded(cfg, memRuntime, logger)

	// Give membership a brief moment to converge before we snapshot it.
	time.Sleep(2 * time.Second)

	leader := newLeader(cfg, memRuntime.Table, logger)
	leader.run(cfg)
}

// parseLeaderConfig parses CLI flags into a simple config struct.
func parseLeaderConfig() LeaderConfig {
	jobIDFlag := flag.String("job", "job1", "Rainstorm job ID")
	numTasksFlag := flag.Int("tasks", 3, "number of tasks in the (single) stage")
	ipFlag := flag.String("ip", "127.0.0.1", "local IP for membership")
	udpPortFlag := flag.Int("port", 8001, "UDP port for membership SWIM")
	introducerFlag := flag.String("introducer", "", "introducer ip:port (empty means this node is introducer)")
	flag.Parse()

	return LeaderConfig{
		JobID:      *jobIDFlag,
		NumTasks:   *numTasksFlag,
		IP:         *ipFlag,
		UDPPort:    *udpPortFlag,
		Introducer: *introducerFlag,
	}
}

// newLeaderLogger creates a logger that writes to a job-specific file.
func newLeaderLogger(jobID string) *log.Logger {
	logFileName := fmt.Sprintf("leader-%s.log", jobID)
	logFile, err := os.Create(logFileName)
	if err != nil {
		log.Fatalf("failed to create leader log file %s: %v", logFileName, err)
	}
	return log.New(logFile, "", log.LstdFlags|log.Lmicroseconds)
}

// startMembershipRuntime sets up membership table, UDP transport, and protocol.
func startMembershipRuntime(cfg LeaderConfig, logger *log.Logger) *membershipRuntime {
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

	return &membershipRuntime{
		Table: table,
		Proto: proto,
		Ctx:   ctx,
	}
}

// joinClusterIfNeeded sends a JOIN message to the introducer, if one is configured.
func joinClusterIfNeeded(cfg LeaderConfig, mr *membershipRuntime, logger *log.Logger) {
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

// newLeader constructs a Leader instance with membership and worker manager wired in.
func newLeader(cfg LeaderConfig, table *membership.Table, logger *log.Logger) *Leader {
	return &Leader{
		membershipTable: table,
		workerList:      discoverVMs(table),
		taskInfos:       make(map[rainstorm.TaskID]*rainstorm.TaskInfo),
		logger:          logger,
	}
}

// run executes the full lifecycle of a Rainstorm job under this leader.
func (l *Leader) run(cfg LeaderConfig) {
	l.logger.Printf("Job %s: starting", cfg.JobID)

	taskSpecs := l.buildTaskSpecs(cfg)
	if err := l.startTasks(taskSpecs); err != nil {
		l.logger.Fatalf("failed to start tasks: %v", err)
	}

	go l.rateMonitorLoop()
	go l.controlServerLoop() // for list_tasks / kill_task

	l.startSource() // send "start" to source task

	l.waitForCompletion() // block until sink reports done

	l.shutdownAllTasks()
	l.logger.Printf("Job %s: completed", cfg.JobID)
}

// Sketches:

func discoverVMs(table *membership.Table) []WorkerInfo {
	members := table.GetMembers()
	self := table.GetSelf()

	var workers []WorkerInfo
	for _, m := range members {
		if m.State != mpb.MemberState_ALIVE {
			continue
		}
		// Skip the leader's own membership node; only treat actual worker VMs as workers.
		if m.NodeID.GetIp() == self.GetIp() && m.NodeID.GetPort() == self.GetPort() {
			continue
		}

		id := membership.StringifyNodeID(m.NodeID)
		workers = append(workers, WorkerInfo{
			ID:     id,
			Host:   m.NodeID.GetIp(),
			Port:   int(m.NodeID.GetPort()), // base UDP port; you can map to a Rainstorm port if needed
			Status: "Ready",
		})
	}
	return workers
}

// buildTaskSpecs creates a simple, single-stage job plan for now.
// Later, you can extend this to parse a full Rainstorm DAG description.
func (l *Leader) buildTaskSpecs(cfg LeaderConfig) []rainstorm.TaskSpec {
	var specs []rainstorm.TaskSpec
	for i := 0; i < cfg.NumTasks; i++ {
		specs = append(specs, rainstorm.TaskSpec{
			ID: rainstorm.TaskID{
				JobID:    cfg.JobID,
				StageID:  1,
				Sequence: i,
			},
			OpExe:  "./dummyop", // built from cmd/dummyop
			OpArgs: []string{},
		})
	}
	return specs
}

func (l *Leader) startTasks(specs []rainstorm.TaskSpec) error {
	// TODO: simple round-robin over VMs, call WorkerManager.createWorker
	for i, spec := range specs {
		worker := &l.workerList[i%len(l.workerList)]
		ti, err := l.startTaskOnWorker(worker, spec)
		if err != nil {
			return err
		}
		l.taskInfos[spec.ID] = ti
		worker.TaskIDs = append(worker.TaskIDs, spec.ID)
		l.logger.Printf("Task %v started on %s pid=%d log=%s",
			spec.ID, worker.ID, ti.PID, ti.LocalLog)
	}
	return nil
}

func (l *Leader) startTaskOnWorker(worker *WorkerInfo, spec rainstorm.TaskSpec) (*rainstorm.TaskInfo, error) {
	// Delegate process creation to the remote WorkerManager running on this worker VM.
	ti, err := l.createWorkerRemote(worker, spec)
	if err != nil {
		return nil, err
	}

	// Annotate with VM / endpoint info for Rainstorm control plane.
	ti.VM = worker.ID
	ti.Endpoint = rainstorm.Endpoint{
		Host: worker.Host,
		Port: worker.Port, // placeholder: later map to worker's Rainstorm port
	}

	l.logger.Printf("started worker for %v on %s (pid=%d, log=%s)",
		spec.ID, worker.ID, ti.PID, ti.LocalLog)

	return ti, nil
}

// workerControlAddress builds the HTTP base URL for a worker's WorkerManager.
func (l *Leader) workerControlAddress(worker *WorkerInfo) string {
	// By convention, WorkerManager control port = UDP membership port + 3000.
	controlPort := worker.Port + 3000
	return fmt.Sprintf("http://%s:%d", worker.Host, controlPort)
}

// createWorkerRemote sends a CreateWorker request to the remote WorkerManager.
func (l *Leader) createWorkerRemote(worker *WorkerInfo, spec rainstorm.TaskSpec) (*rainstorm.TaskInfo, error) {
	url := l.workerControlAddress(worker) + "/create_worker"

	body, err := json.Marshal(spec)
	if err != nil {
		return nil, fmt.Errorf("marshal TaskSpec: %w", err)
	}

	resp, err := http.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("POST %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("CreateWorker failed (%d): %s", resp.StatusCode, string(respBody))
	}

	var ti rainstorm.TaskInfo
	if err := json.NewDecoder(resp.Body).Decode(&ti); err != nil {
		return nil, fmt.Errorf("decode TaskInfo: %w", err)
	}
	return &ti, nil
}

// killWorkerRemote sends a KillWorker request to the remote WorkerManager.
func (l *Leader) killWorkerRemote(worker *WorkerInfo, id rainstorm.TaskID) error {
	url := l.workerControlAddress(worker) + "/kill_worker"

	body, err := json.Marshal(id)
	if err != nil {
		return fmt.Errorf("marshal TaskID: %w", err)
	}

	resp, err := http.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("POST %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("KillWorker failed (%d): %s", resp.StatusCode, string(respBody))
	}
	return nil
}

func (l *Leader) waitForCompletion() {
	// TODO: block until sink notifies completion
	time.Sleep(5 * time.Second)
}

func (l *Leader) shutdownAllTasks() {
	for i := range l.workerList {
		worker := &l.workerList[i]
		for _, id := range worker.TaskIDs {
			if err := l.killWorkerRemote(worker, id); err != nil {
				l.logger.Printf("error killing task %v on %s: %v", id, worker.ID, err)
			}
		}
	}
}

func (l *Leader) rateMonitorLoop() {
	// TODO: listen for RateReport messages from tasks and log tuples/sec
}

func (l *Leader) controlServerLoop() {
	// TODO: expose simple API/CLI for list_tasks, kill_task
}

func (l *Leader) startSource() {
	// TODO: send control message to source task to begin reading
}
