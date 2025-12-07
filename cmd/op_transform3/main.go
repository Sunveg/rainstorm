package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// op_transform3 is Stage 2 of Application 2: it reads filtered CSV shards
// from stage 1 and outputs only the first three fields (columns 1-3) for
// each line. Like other operators, it partitions work across tasks by
// line-number mod task_count and logs basic rate information.
func main() {
	inputPrefix := flag.String("input_prefix", "", "prefix of stage1 output files (e.g., ./output/app2_filter.csv)")
	inputTaskCount := flag.Int("input_task_count", 1, "number of stage1 tasks (number of shard files)")
	outputPath := flag.String("output", "", "base path for output file")
	taskIndex := flag.Int("task_index", 0, "index of this task (0-based)")
	taskCount := flag.Int("task_count", 1, "number of tasks in this stage")
	sleepPerTupleMs := flag.Int("sleep_per_tuple_ms", 0, "optional: sleep this many milliseconds after each emitted tuple (for autoscale demo)")
	jobID := flag.String("job_id", "", "RainStorm job ID (for autoscale reporting; optional)")
	stageID := flag.Int("stage_id", 0, "stage ID (for autoscale reporting; optional)")
	taskSeq := flag.Int("task_seq", 0, "task sequence (for autoscale reporting; optional)")
	rateURL := flag.String("rate_url", "", "leader rate-report URL (for autoscale reporting; optional)")
	flag.Parse()

	logger := log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds)

	if *inputPrefix == "" || *outputPath == "" {
		logger.Fatalf("op_transform3: -input_prefix and -output are required")
	}
	if *inputTaskCount <= 0 {
		logger.Fatalf("op_transform3: input_task_count must be > 0")
	}
	if *taskIndex < 0 || *taskCount <= 0 || *taskIndex >= *taskCount {
		logger.Fatalf("op_transform3: invalid task_index/task_count: %d/%d", *taskIndex, *taskCount)
	}

	perTaskOutput := fmt.Sprintf("%s.task%d", *outputPath, *taskIndex)

	// Ensure the output directory exists.
	outputDir := filepath.Dir(perTaskOutput)
	if outputDir != "" && outputDir != "." {
		if err := os.MkdirAll(outputDir, 0o755); err != nil {
			logger.Fatalf("op_transform3: failed to create output directory %s: %v", outputDir, err)
		}
	}

	out, err := os.Create(perTaskOutput)
	if err != nil {
		logger.Fatalf("op_transform3: failed to create output %s: %v", perTaskOutput, err)
	}
	defer out.Close()

	writer := bufio.NewWriter(out)
	defer writer.Flush()

	logger.Printf("op_transform3 START input_prefix=%s shards=%d output=%s index=%d count=%d",
		*inputPrefix, *inputTaskCount, perTaskOutput, *taskIndex, *taskCount)

	var (
		globalLineNum int
		emitted       int
		totalEmitted  int
		windowStart   = time.Now()
		startTime     = time.Now()
	)

	type ratePayload struct {
		JobID    string  `json:"job_id"`
		StageID  int     `json:"stage_id"`
		Sequence int     `json:"sequence"`
		Rate     float64 `json:"rate"`
	}

	// Read each shard from stage 1.
	for shard := 0; shard < *inputTaskCount; shard++ {
		shardPath := fmt.Sprintf("%s.task%d", *inputPrefix, shard)

		f, err := os.Open(shardPath)
		if err != nil {
			if os.IsNotExist(err) {
				logger.Printf("op_transform3: shard %s does not exist, skipping", shardPath)
				continue
			}
			logger.Fatalf("op_transform3: failed to open shard %s: %v", shardPath, err)
		}

		logger.Printf("op_transform3: reading shard %s", shardPath)
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			line := scanner.Text()

			// Partition across transform tasks by a global line counter.
			if globalLineNum%*taskCount == *taskIndex {
				fields := strings.Split(line, ",")
				f1, f2, f3 := "", "", ""
				if len(fields) > 0 {
					f1 = fields[0]
				}
				if len(fields) > 1 {
					f2 = fields[1]
				}
				if len(fields) > 2 {
					f3 = fields[2]
				}
				outLine := fmt.Sprintf("%s,%s,%s\n", f1, f2, f3)
				if _, err := writer.WriteString(outLine); err != nil {
					_ = f.Close()
					logger.Fatalf("op_transform3: failed to write output: %v", err)
				}

				// Log each output tuple for debugging.
				logger.Printf("OUTPUT_TUPLE index=%d shard=%d raw=%q out=%q",
					*taskIndex, shard, line, strings.TrimSpace(outLine))

				emitted++
				totalEmitted++

				// Optional slowdown so that autoscaling demos can observe rates over time.
				if *sleepPerTupleMs > 0 {
					time.Sleep(time.Duration(*sleepPerTupleMs) * time.Millisecond)
				}
				elapsed := time.Since(windowStart)
				if elapsed >= time.Second {
					rate := float64(emitted) / elapsed.Seconds()
					logger.Printf("RATE tuples/sec=%s emitted=%d",
						strconv.FormatFloat(rate, 'f', 2, 64), emitted)

					// Optionally report rate back to the leader for autoscaling.
					if *rateURL != "" && *jobID != "" && *stageID != 0 {
						payload := ratePayload{
							JobID:    *jobID,
							StageID:  *stageID,
							Sequence: *taskSeq,
							Rate:     rate,
						}
						if body, err := json.Marshal(payload); err == nil {
							req, err := http.NewRequest(http.MethodPost, *rateURL, bytes.NewReader(body))
							if err == nil {
								req.Header.Set("Content-Type", "application/json")
								// Best-effort; ignore errors for now.
								_, _ = http.DefaultClient.Do(req)
							} else {
								logger.Printf("op_transform3: failed to build rate report request: %v", err)
							}
						} else {
							logger.Printf("op_transform3: failed to marshal rate payload: %v", err)
						}
					}

					windowStart = time.Now()
					emitted = 0
				}
			}

			globalLineNum++
		}
		if err := scanner.Err(); err != nil {
			_ = f.Close()
			logger.Fatalf("op_transform3: error reading shard %s: %v", shardPath, err)
		}
		_ = f.Close()
	}

	// Final overall rate report based on total run time, so that even short-lived
	// transform tasks provide at least one meaningful rate sample to the leader.
	if totalEmitted > 0 && *rateURL != "" && *jobID != "" && *stageID != 0 {
		totalElapsed := time.Since(startTime)
		if totalElapsed > 0 {
			finalRate := float64(totalEmitted) / totalElapsed.Seconds()
			logger.Printf("FINAL_RATE tuples/sec=%s total_emitted=%d",
				strconv.FormatFloat(finalRate, 'f', 2, 64), totalEmitted)

			payload := ratePayload{
				JobID:    *jobID,
				StageID:  *stageID,
				Sequence: *taskSeq,
				Rate:     finalRate,
			}
			if body, err := json.Marshal(payload); err == nil {
				req, err := http.NewRequest(http.MethodPost, *rateURL, bytes.NewReader(body))
				if err == nil {
					req.Header.Set("Content-Type", "application/json")
					_, _ = http.DefaultClient.Do(req)
				} else {
					logger.Printf("op_transform3: failed to build final rate report request: %v", err)
				}
			} else {
				logger.Printf("op_transform3: failed to marshal final rate payload: %v", err)
			}
		}
	}

	logger.Printf("op_transform3 DONE input_prefix=%s shards=%d output=%s index=%d count=%d",
		*inputPrefix, *inputTaskCount, perTaskOutput, *taskIndex, *taskCount)
}
