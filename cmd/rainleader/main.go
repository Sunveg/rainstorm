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
	"strconv"
	"strings"
	"sync"
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
	config          LeaderConfig

	rateMu    sync.Mutex
	taskRates map[rainstorm.TaskID]float64

	autoscaleMu           sync.Mutex
	stage1TaskCount       int
	currentTransformTasks int
	maxTransformTasks     int
}

// LeaderConfig holds all configuration parsed from CLI flags.
type LeaderConfig struct {
	JobID    string
	NumTasks int // total tasks in first stage (for legacy/simple mode)

	IP         string
	UDPPort    int
	Introducer string

	// RainStorm application-level config (for Application 0 and beyond)
	NStages        int
	NTasksPerStage int
	OpExeStage1    string
	OpArgsStage1   []string
	HydfsSrcPath   string
	HydfsDestPath  string
	ExactlyOnce    bool
	Autoscale      bool
	InputRate      int
	LowWatermark   int
	HighWatermark  int

	// Optional stage 2 configuration:
	// - If AggColumnIndex > 0: run aggregation stage (Application 1).
	// - If UseTransformStage is true: run transform stage (Application 2).
	AggColumnIndex    int
	AggOutputPath     string
	UseTransformStage bool

	// Optional transform tuning (Application 2 autoscaling demos).
	TransformSleepPerTupleMs int
	TransformInitialTasks    int
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
	aggColumnFlag := flag.Int("agg_column", 0, "optional: 1-based column index for aggregation stage (Application 1)")
	aggOutputFlag := flag.String("agg_output", "", "optional: output path for aggregation stage (Application 1)")
	transformStageFlag := flag.Bool("transform_stage", false, "if true, use transform stage (Application 2) instead of aggregation")
	transformSleepMsFlag := flag.Int("transform_sleep_ms", 0, "Application 2: sleep this many ms per transform output tuple (for autoscale demo)")
	transformInitialTasksFlag := flag.Int("transform_initial_tasks", 0, "Application 2: initial number of transform tasks (defaults to Ntasks_per_stage)")
	flag.Parse()

	cfg := LeaderConfig{
		JobID:                    *jobIDFlag,
		NumTasks:                 *numTasksFlag,
		IP:                       *ipFlag,
		UDPPort:                  *udpPortFlag,
		Introducer:               *introducerFlag,
		AggColumnIndex:           *aggColumnFlag,
		AggOutputPath:            *aggOutputFlag,
		UseTransformStage:        *transformStageFlag,
		TransformSleepPerTupleMs: *transformSleepMsFlag,
		TransformInitialTasks:    *transformInitialTasksFlag,
	}

	// Parse RainStorm-style positional args AFTER flags, if provided.
	// Format (for now, only NStages=1 supported):
	// RainStorm <Nstages> <Ntasks_per_stage> <op1_exe> <op1_args...> <hydfs_src> <hydfs_dest> <exactly_once> <autoscale_enabled> <INPUT_RATE> <LW> <HW>
	appArgs := flag.Args()
	if len(appArgs) == 0 {
		// Legacy/simple mode: single stage, NumTasks workers, dummy operator.
		cfg.NStages = 1
		cfg.NTasksPerStage = cfg.NumTasks
		cfg.OpExeStage1 = "./dummyop"
		cfg.OpArgsStage1 = []string{}
		return cfg
	}

	// Need at least: 2 (Nstages,Ntasks) + 1 (op exe) + 2 (src,dest) + 5 (exactly_once, autoscale, rate, LW, HW)
	if len(appArgs) < 2+1+2+5 {
		log.Fatalf("invalid RainStorm invocation: expected at least 10 args after flags, got %d (%v)", len(appArgs), appArgs)
	}

	// Parse Nstages and Ntasks_per_stage
	nStages, err := strconv.Atoi(appArgs[0])
	if err != nil || nStages <= 0 {
		log.Fatalf("invalid Nstages %q: %v", appArgs[0], err)
	}
	nTasks, err := strconv.Atoi(appArgs[1])
	if err != nil || nTasks <= 0 {
		log.Fatalf("invalid Ntasks_per_stage %q: %v", appArgs[1], err)
	}

	if nStages != 1 {
		log.Fatalf("only Nstages=1 is supported for Application 0 at the moment, got %d", nStages)
	}

	// Last 7 args are: hydfs_src, hydfs_dest, exactly_once, autoscale_enabled, INPUT_RATE, LW, HW
	trailing := 7
	if len(appArgs) < 2+1+trailing { // ensure at least one token for op1_exe
		log.Fatalf("invalid RainStorm invocation: not enough args for stage 1 operator")
	}

	trailStart := len(appArgs) - trailing
	stageTokens := appArgs[2:trailStart]
	if len(stageTokens) < 1 {
		log.Fatalf("invalid RainStorm invocation: missing stage 1 operator exe")
	}

	opExe := stageTokens[0]
	opArgs := stageTokens[1:]

	hydfsSrc := appArgs[trailStart]
	hydfsDest := appArgs[trailStart+1]

	exactlyOnceStr := appArgs[trailStart+2]
	autoscaleStr := appArgs[trailStart+3]
	inputRateStr := appArgs[trailStart+4]
	lwStr := appArgs[trailStart+5]
	hwStr := appArgs[trailStart+6]

	exactlyOnce := exactlyOnceStr == "1" || strings.ToLower(exactlyOnceStr) == "true"
	autoscale := autoscaleStr == "1" || strings.ToLower(autoscaleStr) == "true"

	inputRate, err := strconv.Atoi(inputRateStr)
	if err != nil {
		log.Fatalf("invalid INPUT_RATE %q: %v", inputRateStr, err)
	}
	lw, err := strconv.Atoi(lwStr)
	if err != nil {
		log.Fatalf("invalid LW %q: %v", lwStr, err)
	}
	hw, err := strconv.Atoi(hwStr)
	if err != nil {
		log.Fatalf("invalid HW %q: %v", hwStr, err)
	}

	cfg.NStages = nStages
	cfg.NTasksPerStage = nTasks
	cfg.OpExeStage1 = opExe
	cfg.OpArgsStage1 = opArgs
	cfg.HydfsSrcPath = hydfsSrc
	cfg.HydfsDestPath = hydfsDest
	cfg.ExactlyOnce = exactlyOnce
	cfg.Autoscale = autoscale
	cfg.InputRate = inputRate
	cfg.LowWatermark = lw
	cfg.HighWatermark = hw

	// For compatibility, also keep NumTasks in sync for stage 1.
	cfg.NumTasks = nTasks

	return cfg
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
		config:          cfg,
		taskRates:       make(map[rainstorm.TaskID]float64),
	}
}

// run executes the full lifecycle of a Rainstorm job under this leader.
func (l *Leader) run(cfg LeaderConfig) {
	l.logger.Printf("Job %s: starting", cfg.JobID)

	// Stage 1: build and run tasks (identity/filter depending on CLI).
	stage1Specs := l.buildTaskSpecs(cfg)
	l.stage1TaskCount = len(stage1Specs)
	if err := l.startTasks(stage1Specs); err != nil {
		l.logger.Fatalf("failed to start tasks: %v", err)
	}

	go l.rateMonitorLoop()
	go l.controlServerLoop() // for list_tasks / kill_task

	l.startSource() // send "start" to source task

	l.waitForCompletion() // block until stage 1 tasks report done (stubbed sleep for now)

	// Optional Stage 2:
	// - Aggregation for Application 1 (AggColumnIndex > 0).
	// - Transform for Application 2 (UseTransformStage == true).
	if cfg.AggOutputPath != "" {
		if cfg.UseTransformStage {
			l.logger.Printf("Job %s: starting transform stage (output=%s)",
				cfg.JobID, cfg.AggOutputPath)
			// For Application 2 autoscaling demos, allow an initial transform
			// parallelism different from the maximum (NTasksPerStage).
			initialCfg := cfg
			if cfg.TransformInitialTasks > 0 {
				initialCfg.NTasksPerStage = cfg.TransformInitialTasks
			}
			xformSpecs := l.buildTransformTaskSpecs(initialCfg, len(stage1Specs))
			if err := l.startTasks(xformSpecs); err != nil {
				l.logger.Fatalf("failed to start transform tasks: %v", err)
			}
			l.autoscaleMu.Lock()
			l.currentTransformTasks = len(xformSpecs)
			// Use NTasksPerStage as the maximum parallelism for autoscaling, or
			// fall back to stage1 parallelism.
			max := cfg.NTasksPerStage
			if max <= 0 {
				max = len(stage1Specs)
			}
			if max <= 0 {
				max = 1
			}
			l.maxTransformTasks = max
			l.autoscaleMu.Unlock()
			l.waitForCompletion()
		} else if cfg.AggColumnIndex > 0 {
			l.logger.Printf("Job %s: starting aggregation stage (column=%d, output=%s)",
				cfg.JobID, cfg.AggColumnIndex, cfg.AggOutputPath)

			aggSpecs := l.buildAggTaskSpecs(cfg, len(stage1Specs))
			if err := l.startTasks(aggSpecs); err != nil {
				l.logger.Fatalf("failed to start aggregation tasks: %v", err)
			}
			l.waitForCompletion()
		}
	}

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

func (l *Leader) buildTaskSpecs(cfg LeaderConfig) []rainstorm.TaskSpec {
	// For Application 0, we support a single identity stage only.
	// If the full RainStorm CLI was provided, prefer that; otherwise fall back to legacy flags.
	numTasks := cfg.NumTasks
	if cfg.NTasksPerStage > 0 {
		numTasks = cfg.NTasksPerStage
	}

	opExe := cfg.OpExeStage1
	if opExe == "" {
		opExe = "./dummyop" // legacy default
	}

	var specs []rainstorm.TaskSpec
	for i := 0; i < numTasks; i++ {
		// Base op args from CLI, plus per-task sharding and rate information if we are using op_identity.
		opArgs := make([]string, 0, len(cfg.OpArgsStage1)+8)
		opArgs = append(opArgs, cfg.OpArgsStage1...)

		// If HydfsSrcPath/HydfsDestPath/InputRate are set, pass them to the operator.
		if cfg.HydfsSrcPath != "" {
			opArgs = append(opArgs, "-input", cfg.HydfsSrcPath)
		}
		if cfg.HydfsDestPath != "" {
			opArgs = append(opArgs, "-output", cfg.HydfsDestPath)
		}
		if cfg.InputRate > 0 {
			opArgs = append(opArgs, "-input_rate", fmt.Sprintf("%d", cfg.InputRate))
		}

		// Per-task index and total count so the operator can partition input.
		opArgs = append(opArgs,
			"-task_index", fmt.Sprintf("%d", i),
			"-task_count", fmt.Sprintf("%d", numTasks),
		)

		specs = append(specs, rainstorm.TaskSpec{
			ID: rainstorm.TaskID{
				JobID:    cfg.JobID,
				StageID:  1,
				Sequence: i,
			},
			OpExe:  opExe,
			OpArgs: opArgs,
		})
	}
	return specs
}

// buildAggTaskSpecs creates TaskSpecs for the aggregation stage (Application 1 stage 2).
// It assumes stage 1 wrote its outputs to cfg.HydfsDestPath with ".task<idx>" suffix
// and creates one aggregator per task in the stage (partitioned by key hash).
func (l *Leader) buildAggTaskSpecs(cfg LeaderConfig, stage1TaskCount int) []rainstorm.TaskSpec {
	opExe := "./op_agg_count"

	// Use the same parallelism as stage 1 for aggregators.
	numAgg := cfg.NTasksPerStage
	if numAgg <= 0 {
		numAgg = stage1TaskCount
	}
	if numAgg <= 0 {
		numAgg = 1
	}

	var specs []rainstorm.TaskSpec
	for i := 0; i < numAgg; i++ {
		var opArgs []string
		if cfg.HydfsDestPath != "" {
			opArgs = append(opArgs, "-input_prefix", cfg.HydfsDestPath)
		}
		if stage1TaskCount > 0 {
			opArgs = append(opArgs, "-input_task_count", fmt.Sprintf("%d", stage1TaskCount))
		}
		if cfg.AggColumnIndex > 0 {
			opArgs = append(opArgs, "-column_index", fmt.Sprintf("%d", cfg.AggColumnIndex))
		}

		// Each aggregator writes its own part file.
		outputPath := cfg.AggOutputPath
		if outputPath != "" {
			outputPath = fmt.Sprintf("%s.part%d", cfg.AggOutputPath, i)
			opArgs = append(opArgs, "-output", outputPath)
		}

		// Partitioning configuration.
		opArgs = append(opArgs,
			"-partition_index", fmt.Sprintf("%d", i),
			"-partition_count", fmt.Sprintf("%d", numAgg),
		)

		specs = append(specs, rainstorm.TaskSpec{
			ID: rainstorm.TaskID{
				JobID:    cfg.JobID,
				StageID:  2,
				Sequence: i,
			},
			OpExe:  opExe,
			OpArgs: opArgs,
		})
	}
	return specs
}

// buildTransformTaskSpecs creates TaskSpecs for the transform stage (Application 2 stage 2).
// It assumes stage 1 wrote its outputs to cfg.HydfsDestPath with ".task<idx>" suffix
// and creates one transform task per desired parallelism.
func (l *Leader) buildTransformTaskSpecs(cfg LeaderConfig, stage1TaskCount int) []rainstorm.TaskSpec {
	opExe := "./op_transform3"

	// Use the same parallelism as stage 1 by default.
	numTasks := cfg.NTasksPerStage
	if numTasks <= 0 {
		numTasks = stage1TaskCount
	}
	if numTasks <= 0 {
		numTasks = 1
	}

	// Leader's control-plane URL for rate reports from transform tasks.
	var rateURL string
	if cfg.Autoscale {
		controlPort := cfg.UDPPort + 5000
		rateURL = fmt.Sprintf("http://%s:%d/rate_report", cfg.IP, controlPort)
	}

	var specs []rainstorm.TaskSpec
	for i := 0; i < numTasks; i++ {
		var opArgs []string
		if cfg.HydfsDestPath != "" {
			opArgs = append(opArgs, "-input_prefix", cfg.HydfsDestPath)
		}
		if stage1TaskCount > 0 {
			opArgs = append(opArgs, "-input_task_count", fmt.Sprintf("%d", stage1TaskCount))
		}
		outputPath := cfg.AggOutputPath
		if outputPath != "" {
			outputPath = fmt.Sprintf("%s.part%d", cfg.AggOutputPath, i)
			opArgs = append(opArgs, "-output", outputPath)
		}
		opArgs = append(opArgs,
			"-task_index", fmt.Sprintf("%d", i),
			"-task_count", fmt.Sprintf("%d", numTasks),
		)

		// Optional slowdown so autoscale tests can keep transform tasks alive longer.
		if cfg.TransformSleepPerTupleMs > 0 {
			opArgs = append(opArgs,
				"-sleep_per_tuple_ms", fmt.Sprintf("%d", cfg.TransformSleepPerTupleMs),
			)
		}

		// When autoscaling is enabled, configure transform tasks to report
		// their per-second rates back to the leader.
		if rateURL != "" {
			opArgs = append(opArgs,
				"-job_id", cfg.JobID,
				"-stage_id", "2",
				"-task_seq", fmt.Sprintf("%d", i),
				"-rate_url", rateURL,
			)
		}

		specs = append(specs, rainstorm.TaskSpec{
			ID: rainstorm.TaskID{
				JobID:    cfg.JobID,
				StageID:  2,
				Sequence: i,
			},
			OpExe:  opExe,
			OpArgs: opArgs,
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
	// TODO: block until sink notifies completion.
	// For now, use a fixed sleep so that, even at low INPUT_RATE,
	// stage 1 has enough time to finish before we launch stage 2.
	// 2 minutes works well for current tests (INPUT_RATE=10 or 100).
	time.Sleep(2 * time.Minute)
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
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		// Autoscaling is currently only implemented for Application 2
		// transform stage (stage 2) when explicitly enabled.
		if !l.config.Autoscale || !l.config.UseTransformStage {
			continue
		}

		l.rateMu.Lock()
		var total float64
		for id, r := range l.taskRates {
			if id.JobID != l.config.JobID || id.StageID != 2 {
				continue
			}
			total += r
		}
		l.rateMu.Unlock()

		l.autoscaleMu.Lock()
		current := l.currentTransformTasks
		max := l.maxTransformTasks
		l.autoscaleMu.Unlock()

		if current == 0 || max == 0 {
			// Transform stage not running yet.
			continue
		}

		lw := l.config.LowWatermark
		hw := l.config.HighWatermark

		if total > float64(hw) && current < max {
			// High load: scale UP by one.
			newCount := current + 1
			if newCount > max {
				newCount = max
			}
			l.logger.Printf("AUTOSCALE: total_rate=%.2f > HW=%d; scaling UP stage2_tasks %d -> %d",
				total, hw, current, newCount)
			l.scaleTransformTasks(newCount)
		} else if total < float64(lw) && current > 1 {
			// Low load: scale DOWN by one (but keep at least one task).
			newCount := current - 1
			if newCount < 1 {
				newCount = 1
			}
			l.logger.Printf("AUTOSCALE: total_rate=%.2f < LW=%d; scaling DOWN stage2_tasks %d -> %d",
				total, lw, current, newCount)
			l.scaleTransformTasks(newCount)
		} else {
			l.logger.Printf("AUTOSCALE: total_rate=%.2f within [%d,%d]; no change (stage2_tasks=%d)",
				total, lw, hw, current)
		}
	}
}

func (l *Leader) controlServerLoop() {
	// Expose simple HTTP endpoints for list_tasks and kill_task.
	controlPort := l.config.UDPPort + 5000
	addr := fmt.Sprintf("0.0.0.0:%d", controlPort)

	mux := http.NewServeMux()

	// list_tasks: returns JSON list of tasks with VM, PID, OpExe, and LocalLog.
	mux.HandleFunc("/list_tasks", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		type taskView struct {
			JobID    string `json:"job_id"`
			StageID  int    `json:"stage_id"`
			Seq      int    `json:"sequence"`
			VM       string `json:"vm"`
			PID      int    `json:"pid"`
			OpExe    string `json:"op_exe"`
			LocalLog string `json:"local_log"`
		}

		var result []taskView
		for _, worker := range l.workerList {
			for _, id := range worker.TaskIDs {
				ti, ok := l.taskInfos[id]
				if !ok {
					continue
				}
				result = append(result, taskView{
					JobID:    id.JobID,
					StageID:  id.StageID,
					Seq:      id.Sequence,
					VM:       worker.ID,
					PID:      ti.PID,
					OpExe:    ti.Spec.OpExe,
					LocalLog: ti.LocalLog,
				})
			}
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(result); err != nil {
			http.Error(w, "failed to encode JSON", http.StatusInternalServerError)
			return
		}
	})

	// kill_task: expects JSON { "vm": "<vm_id>", "pid": <pid> }.
	mux.HandleFunc("/kill_task", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		defer r.Body.Close()

		type killRequest struct {
			VM  string `json:"vm"`
			PID int    `json:"pid"`
		}

		var req killRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid JSON body", http.StatusBadRequest)
			return
		}

		// Find the worker entry matching this VM.
		var worker *WorkerInfo
		for i := range l.workerList {
			if l.workerList[i].ID == req.VM {
				worker = &l.workerList[i]
				break
			}
		}
		if worker == nil {
			http.Error(w, "unknown VM "+req.VM, http.StatusBadRequest)
			return
		}

		// Look up the task ID by PID.
		var taskID *rainstorm.TaskID
		for _, id := range worker.TaskIDs {
			ti, ok := l.taskInfos[id]
			if !ok {
				continue
			}
			if ti.PID == req.PID {
				// create a copy so we can take its address
				idCopy := id
				taskID = &idCopy
				break
			}
		}
		if taskID == nil {
			http.Error(w, fmt.Sprintf("no task with PID %d on VM %s", req.PID, req.VM), http.StatusBadRequest)
			return
		}

		if err := l.killWorkerRemote(worker, *taskID); err != nil {
			http.Error(w, fmt.Sprintf("failed to kill task %v: %v", *taskID, err), http.StatusInternalServerError)
			return
		}

		l.logger.Printf("kill_task: VM=%s PID=%d Task=%v", req.VM, req.PID, *taskID)
		w.WriteHeader(http.StatusOK)
	})

	// restart_task: expects JSON { "vm": "<vm_id>", "pid": <old_pid> }.
	// Kills the old process (if still running) and starts a new one for the same TaskID.
	mux.HandleFunc("/restart_task", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		defer r.Body.Close()

		type restartRequest struct {
			VM  string `json:"vm"`
			PID int    `json:"pid"`
		}

		var req restartRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid JSON body", http.StatusBadRequest)
			return
		}

		// Find the worker entry matching this VM.
		var worker *WorkerInfo
		for i := range l.workerList {
			if l.workerList[i].ID == req.VM {
				worker = &l.workerList[i]
				break
			}
		}
		if worker == nil {
			http.Error(w, "unknown VM "+req.VM, http.StatusBadRequest)
			return
		}

		// Look up the task ID by PID.
		var taskID *rainstorm.TaskID
		for _, id := range worker.TaskIDs {
			ti, ok := l.taskInfos[id]
			if !ok {
				continue
			}
			if ti.PID == req.PID {
				idCopy := id
				taskID = &idCopy
				break
			}
		}
		if taskID == nil {
			http.Error(w, fmt.Sprintf("no task with PID %d on VM %s", req.PID, req.VM), http.StatusBadRequest)
			return
		}

		// Kill old process (best-effort).
		if err := l.killWorkerRemote(worker, *taskID); err != nil {
			// Log but don't fail restart just because kill failed; the process may already be gone.
			l.logger.Printf("restart_task: warning: failed to kill old task %v on %s: %v", *taskID, worker.ID, err)
		}

		// Restart the task with its original TaskSpec (same args, same output/state paths).
		oldInfo, ok := l.taskInfos[*taskID]
		if !ok {
			http.Error(w, fmt.Sprintf("no TaskInfo found for task %v", *taskID), http.StatusInternalServerError)
			return
		}

		newInfo, err := l.startTaskOnWorker(worker, oldInfo.Spec)
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to restart task %v: %v", *taskID, err), http.StatusInternalServerError)
			return
		}

		// Update stored TaskInfo with new PID/log/etc.
		l.taskInfos[*taskID] = newInfo

		l.logger.Printf("restart_task: VM=%s oldPID=%d newPID=%d Task=%v log=%s",
			req.VM, req.PID, newInfo.PID, *taskID, newInfo.LocalLog)
		w.WriteHeader(http.StatusOK)
	})

	// rate_report: expects JSON { "job_id": "...", "stage_id": 2, "sequence": i, "rate": <float> }.
	// Transform tasks (Application 2 stage 2) call this periodically so the leader
	// can make autoscaling decisions based on observed throughput.
	mux.HandleFunc("/rate_report", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		defer r.Body.Close()

		type ratePayload struct {
			JobID    string  `json:"job_id"`
			StageID  int     `json:"stage_id"`
			Sequence int     `json:"sequence"`
			Rate     float64 `json:"rate"`
		}

		var p ratePayload
		if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
			http.Error(w, "invalid JSON body", http.StatusBadRequest)
			return
		}
		if p.JobID == "" || p.StageID == 0 {
			http.Error(w, "missing job_id or stage_id", http.StatusBadRequest)
			return
		}

		id := rainstorm.TaskID{
			JobID:    p.JobID,
			StageID:  p.StageID,
			Sequence: p.Sequence,
		}

		l.rateMu.Lock()
		l.taskRates[id] = p.Rate
		l.rateMu.Unlock()

		l.logger.Printf("rate_report: task=%v rate=%.2f", id, p.Rate)
		w.WriteHeader(http.StatusOK)
	})

	l.logger.Printf("Leader control server listening on %s (list_tasks, kill_task, restart_task)", addr)
	if err := http.ListenAndServe(addr, mux); err != nil && err != http.ErrServerClosed {
		l.logger.Printf("control server error: %v", err)
	}
}

// scaleTransformTasks tears down the current Application 2 transform tasks
// (stage 2) and starts a new set with the desired parallelism. This keeps
// the partitioning configuration consistent within each scaling epoch.
func (l *Leader) scaleTransformTasks(newCount int) {
	l.autoscaleMu.Lock()
	defer l.autoscaleMu.Unlock()

	if newCount <= 0 || l.stage1TaskCount <= 0 {
		return
	}

	current := l.currentTransformTasks
	if newCount == current {
		return
	}

	l.logger.Printf("AUTOSCALE: resizing transform stage to %d tasks (was %d)", newCount, current)

	// Kill existing stage-2 tasks for this job and clean up bookkeeping.
	for wi := range l.workerList {
		worker := &l.workerList[wi]
		var remaining []rainstorm.TaskID
		for _, id := range worker.TaskIDs {
			if id.JobID == l.config.JobID && id.StageID == 2 {
				if err := l.killWorkerRemote(worker, id); err != nil {
					l.logger.Printf("AUTOSCALE: error killing old transform task %v on %s: %v", id, worker.ID, err)
				}
				delete(l.taskInfos, id)
				continue
			}
			remaining = append(remaining, id)
		}
		worker.TaskIDs = remaining
	}

	// Clear any cached rate reports for old transform tasks.
	l.rateMu.Lock()
	for id := range l.taskRates {
		if id.JobID == l.config.JobID && id.StageID == 2 {
			delete(l.taskRates, id)
		}
	}
	l.rateMu.Unlock()

	// Start new transform tasks with updated parallelism.
	specs := l.buildTransformTaskSpecs(l.config, l.stage1TaskCount)
	// Override transform parallelism for this epoch by temporarily adjusting
	// NTasksPerStage used in buildTransformTaskSpecs.
	oldN := l.config.NTasksPerStage
	l.config.NTasksPerStage = newCount
	specs = l.buildTransformTaskSpecs(l.config, l.stage1TaskCount)
	l.config.NTasksPerStage = oldN

	if err := l.startTasks(specs); err != nil {
		l.logger.Printf("AUTOSCALE: failed to start new transform tasks: %v", err)
		return
	}

	l.currentTransformTasks = newCount
}

func (l *Leader) startSource() {
	// TODO: send control message to source task to begin reading
}
