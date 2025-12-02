package main

import (
	"fmt"
	"log"
	"os"
	"time"
)

// dummyop is a very small stand‑in "worker" process.
// It just prints a few fake tuple processing log lines to stdout,
// which the leader will capture into a per‑task log file.
func main() {
	logger := log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds)

	jobID := os.Getenv("RAIN_JOB_ID")
	stageID := os.Getenv("RAIN_STAGE_ID")
	taskSeq := os.Getenv("RAIN_TASK_SEQ")

	logger.Printf("dummyop starting job=%s stage=%s task=%s", jobID, stageID, taskSeq)

	for i := 0; i < 10; i++ {
		logger.Printf("TUPLE job=%s stage=%s task=%s seq=%d key=k%d value=v%d",
			jobID, stageID, taskSeq, i, i, i)
		time.Sleep(300 * time.Millisecond)
	}

	logger.Printf("dummyop done job=%s stage=%s task=%s", jobID, stageID, taskSeq)

	fmt.Fprintln(os.Stdout, "DONE")
}
