package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

// op_identity is Application 0: it reads an input file and writes
// the same lines to an output file, partitioned across tasks by line-number mod task_count.
// It also logs tuples and basic rate information to stdout.
func main() {
	inputPath := flag.String("input", "", "path to input CSV file")
	outputPath := flag.String("output", "", "path to output file (base)")
	taskIndex := flag.Int("task_index", 0, "index of this task (0-based)")
	taskCount := flag.Int("task_count", 1, "number of tasks in this stage")
	inputRate := flag.Int("input_rate", 0, "target input rate (tuples/sec), 0 = no limit")
	flag.Parse()

	logger := log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds)

	if *inputPath == "" || *outputPath == "" {
		logger.Fatalf("op_identity: -input and -output are required")
	}
	if *taskIndex < 0 || *taskCount <= 0 || *taskIndex >= *taskCount {
		logger.Fatalf("op_identity: invalid task_index/task_count: %d/%d", *taskIndex, *taskCount)
	}

	// Derive a per-task output file to avoid concurrent writes to a single file.
	perTaskOutput := fmt.Sprintf("%s.task%d", *outputPath, *taskIndex)

	in, err := os.Open(*inputPath)
	if err != nil {
		logger.Fatalf("op_identity: failed to open input %s: %v", *inputPath, err)
	}
	defer in.Close()

	out, err := os.Create(perTaskOutput)
	if err != nil {
		logger.Fatalf("op_identity: failed to create output %s: %v", perTaskOutput, err)
	}
	defer out.Close()

	logger.Printf("op_identity START input=%s output=%s index=%d count=%d rate=%d",
		*inputPath, perTaskOutput, *taskIndex, *taskCount, *inputRate)

	scanner := bufio.NewScanner(in)
	writer := bufio.NewWriter(out)
	defer writer.Flush()

	var (
		lineNum     int
		emitted     int
		windowStart = time.Now()
	)

	for scanner.Scan() {
		line := scanner.Text()
		// Partition by line number across tasks.
		if lineNum%*taskCount == *taskIndex {
			// Identity: write line as-is.
			if _, err := writer.WriteString(line + "\n"); err != nil {
				logger.Fatalf("op_identity: failed to write output: %v", err)
			}
			emitted++

			// Simple rate logging every second.
			elapsed := time.Since(windowStart)
			if elapsed >= time.Second {
				rate := float64(emitted) / elapsed.Seconds()
				logger.Printf("RATE tuples/sec=%s emitted=%d", strconv.FormatFloat(rate, 'f', 2, 64), emitted)
				windowStart = time.Now()
				emitted = 0
			}

			// Optional rate limiting if input_rate is set.
			if *inputRate > 0 {
				// Very simple: sleep to approximate the target rate.
				sleepPerTuple := time.Second / time.Duration(*inputRate)
				time.Sleep(sleepPerTuple)
			}
		}
		lineNum++
	}

	if err := scanner.Err(); err != nil {
		logger.Fatalf("op_identity: error reading input: %v", err)
	}

	logger.Printf("op_identity DONE input=%s output=%s index=%d count=%d",
		*inputPath, perTaskOutput, *taskIndex, *taskCount)
}
