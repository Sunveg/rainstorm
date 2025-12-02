package rainstorm

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"sync"
)

// WorkerManager is a local daemon-like controller that starts and stops
// worker processes for Rainstorm tasks. Later you can expose these methods
// over RPC so the Leader can call them on each VM.
type WorkerManager struct {
	mu      sync.Mutex
	workers map[TaskID]*managedWorker
	logger  *log.Logger
}

type managedWorker struct {
	info *TaskInfo
	cmd  *exec.Cmd
}

// NewWorkerManager constructs a new manager. If logger is nil, logs go to stdout.
func NewWorkerManager(logger *log.Logger) *WorkerManager {
	if logger == nil {
		logger = log.New(os.Stdout, "[WorkerManager] ", log.LstdFlags|log.Lmicroseconds)
	}
	return &WorkerManager{
		workers: make(map[TaskID]*managedWorker),
		logger:  logger,
	}
}

// CreateWorker starts a new OS process for the given TaskSpec and tracks it.
// This is what the Leader will ultimately call (directly or via RPC).
func (wm *WorkerManager) CreateWorker(spec TaskSpec) (*TaskInfo, error) {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	logFileName := fmt.Sprintf("worker-job-%s-stage-%d-task-%d.log",
		spec.ID.JobID, spec.ID.StageID, spec.ID.Sequence)
	f, err := os.Create(logFileName)
	if err != nil {
		return nil, fmt.Errorf("create log file: %w", err)
	}

	cmd := exec.Command(spec.OpExe, spec.OpArgs...)
	cmd.Stdout = f
	cmd.Stderr = f

	if err := cmd.Start(); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("start worker process: %w", err)
	}

	ti := &TaskInfo{
		Spec: spec,
		PID:  cmd.Process.Pid,
		// VM and Endpoint will be filled in by the caller (Leader) if needed.
		LocalLog: logFileName,
	}

	mw := &managedWorker{
		info: ti,
		cmd:  cmd,
	}
	wm.workers[spec.ID] = mw
	wm.logger.Printf("CreateWorker: started %v pid=%d log=%s",
		spec.ID, ti.PID, ti.LocalLog)

	// Background goroutine waits for process exit and cleans up.
	go func(id TaskID, cmd *exec.Cmd, file *os.File) {
		_ = cmd.Wait()
		_ = file.Close()

		wm.mu.Lock()
		defer wm.mu.Unlock()
		if _, ok := wm.workers[id]; ok {
			delete(wm.workers, id)
		}
		wm.logger.Printf("worker %v exited", id)
	}(spec.ID, cmd, f)

	return ti, nil
}

// KillWorker terminates a running worker process for the given TaskID.
func (wm *WorkerManager) KillWorker(id TaskID) error {
	wm.mu.Lock()
	mw, ok := wm.workers[id]
	wm.mu.Unlock()

	if !ok {
		return fmt.Errorf("KillWorker: unknown task %v", id)
	}

	if mw.cmd.Process == nil {
		return fmt.Errorf("KillWorker: task %v has no running process", id)
	}

	if err := mw.cmd.Process.Kill(); err != nil {
		return fmt.Errorf("KillWorker: failed to kill %v: %w", id, err)
	}

	wm.logger.Printf("KillWorker: killed %v pid=%d", id, mw.info.PID)
	return nil
}
