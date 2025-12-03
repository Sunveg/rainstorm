package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// op_filter is Stage 1 of Application 1: it reads an input CSV file
// and outputs only the lines that contain a given pattern (case-sensitive).
// Lines are partitioned across tasks by line-number mod task_count.
func main() {
	inputPath := flag.String("input", "", "path to input CSV file")
	outputPath := flag.String("output", "", "base path for output file")
	taskIndex := flag.Int("task_index", 0, "index of this task (0-based)")
	taskCount := flag.Int("task_count", 1, "number of tasks in this stage")
	pattern := flag.String("pattern", "", "case-sensitive substring to filter on")
	inputRate := flag.Int("input_rate", 0, "target input rate (tuples/sec), 0 = no limit")
	flag.Parse()

	logger := log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds)

	if *inputPath == "" || *outputPath == "" {
		logger.Fatalf("op_filter: -input and -output are required")
	}
	if *pattern == "" {
		logger.Fatalf("op_filter: -pattern is required")
	}
	if *taskIndex < 0 || *taskCount <= 0 || *taskIndex >= *taskCount {
		logger.Fatalf("op_filter: invalid task_index/task_count: %d/%d", *taskIndex, *taskCount)
	}

	perTaskOutput := fmt.Sprintf("%s.task%d", *outputPath, *taskIndex)
	statePath := perTaskOutput + ".state"

	in, err := os.Open(*inputPath)
	if err != nil {
		logger.Fatalf("op_filter: failed to open input %s: %v", *inputPath, err)
	}
	defer in.Close()

	// Ensure the output directory exists.
	outputDir := filepath.Dir(perTaskOutput)
	if outputDir != "" && outputDir != "." {
		if err := os.MkdirAll(outputDir, 0o755); err != nil {
			logger.Fatalf("op_filter: failed to create output directory %s: %v", outputDir, err)
		}
	}

	out, err := os.Create(perTaskOutput)
	if err != nil {
		logger.Fatalf("op_filter: failed to create output %s: %v", perTaskOutput, err)
	}
	defer out.Close()

	logger.Printf("op_filter START input=%s output=%s index=%d count=%d pattern=%q rate=%d",
		*inputPath, perTaskOutput, *taskIndex, *taskCount, *pattern, *inputRate)

	scanner := bufio.NewScanner(in)
	writer := bufio.NewWriter(out)
	defer writer.Flush()

	var (
		lineNum          int
		emitted          int
		windowStart      = time.Now()
		lastProcessedIdx = -1
	)

	// If a state file exists, resume from the last processed line index.
	if data, err := os.ReadFile(statePath); err == nil {
		if v, err := strconv.Atoi(strings.TrimSpace(string(data))); err == nil {
			lastProcessedIdx = v
			logger.Printf("op_filter RESUME from input line index %d", lastProcessedIdx)
		}
	}

	for scanner.Scan() {
		line := scanner.Text()

		// Skip lines we've already processed in a previous run.
		if lineNum <= lastProcessedIdx {
			lineNum++
			continue
		}

		// Partition across tasks by line number.
		if lineNum%*taskCount == *taskIndex {
			if strings.Contains(line, *pattern) {
				if _, err := writer.WriteString(line + "\n"); err != nil {
					logger.Fatalf("op_filter: failed to write output: %v", err)
				}

				emitted++
				// Log simple per-second rate.
				elapsed := time.Since(windowStart)
				if elapsed >= time.Second {
					rate := float64(emitted) / elapsed.Seconds()
					logger.Printf("RATE tuples/sec=%s emitted=%d",
						strconv.FormatFloat(rate, 'f', 2, 64), emitted)
					windowStart = time.Now()
					emitted = 0
				}

				// Optional input rate limiting.
				if *inputRate > 0 {
					sleepPerTuple := time.Second / time.Duration(*inputRate)
					time.Sleep(sleepPerTuple)
				}
			}
		}

		// Update state after processing this input line index.
		lastProcessedIdx = lineNum
		if err := os.WriteFile(statePath, []byte(fmt.Sprintf("%d\n", lastProcessedIdx)), 0o644); err != nil {
			logger.Printf("op_filter: failed to write state file %s: %v", statePath, err)
		}
		lineNum++
	}

	if err := scanner.Err(); err != nil {
		logger.Fatalf("op_filter: error reading input: %v", err)
	}

	logger.Printf("op_filter DONE input=%s output=%s index=%d count=%d pattern=%q lastProcessedIdx=%d",
		*inputPath, perTaskOutput, *taskIndex, *taskCount, *pattern, lastProcessedIdx)
}
