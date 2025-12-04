package main

import (
	"bufio"
	"flag"
	"fmt"
	"hash/crc32"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// op_agg_count implements Stage 2 of Application 1 (offline version):
// it reads all stage-1 shard files, groups by the Nth CSV column, and
// writes "key,count" lines to an output file.
func main() {
	inputPrefix := flag.String("input_prefix", "", "prefix of stage1 output files (e.g., ./output/app1_filter.csv)")
	inputTaskCount := flag.Int("input_task_count", 1, "number of stage1 tasks (number of shard files)")
	columnIndex := flag.Int("column_index", 1, "1-based index of CSV column to group by")
	outputPath := flag.String("output", "", "path to aggregate output file")
	partitionIndex := flag.Int("partition_index", 0, "index of this aggregator (0-based)")
	partitionCount := flag.Int("partition_count", 1, "number of parallel aggregators for this stage")
	xoLogPath := flag.String("xo_log", "", "optional: path to exactly-once log file (can later be uploaded to HyDFS)")
	flag.Parse()

	logger := log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds)

	if *inputPrefix == "" || *outputPath == "" {
		logger.Fatalf("op_agg_count: -input_prefix and -output are required")
	}
	if *inputTaskCount <= 0 {
		logger.Fatalf("op_agg_count: input_task_count must be > 0")
	}
	if *columnIndex <= 0 {
		logger.Fatalf("op_agg_count: column_index must be >= 1")
	}
	if *partitionIndex < 0 || *partitionCount <= 0 || *partitionIndex >= *partitionCount {
		logger.Fatalf("op_agg_count: invalid partition_index/partition_count: %d/%d", *partitionIndex, *partitionCount)
	}

	logger.Printf("op_agg_count START input_prefix=%s shards=%d column_index=%d output=%s partition_index=%d partition_count=%d xo_log=%s",
		*inputPrefix, *inputTaskCount, *columnIndex, *outputPath, *partitionIndex, *partitionCount, *xoLogPath)

	counts := make(map[string]int64)
	// Track processed tuples within this run to avoid double-counting
	// if the same line appears multiple times (e.g., due to upstream replay).
	seen := make(map[string]struct{})
	startTime := time.Now()

	// Set up exactly-once log writer (local file that can be uploaded to HyDFS).
	effectiveXOPath := *xoLogPath
	if effectiveXOPath == "" {
		// Default to outputPath + ".xo" so it's colocated with the aggregate output.
		effectiveXOPath = *outputPath + ".xo"
	}

	var xoWriter *bufio.Writer
	if effectiveXOPath != "" {
		xoDir := filepath.Dir(effectiveXOPath)
		if xoDir != "" && xoDir != "." {
			if err := os.MkdirAll(xoDir, 0o755); err != nil {
				logger.Fatalf("op_agg_count: failed to create xo_log directory %s: %v", xoDir, err)
			}
		}

		xoFile, err := os.Create(effectiveXOPath)
		if err != nil {
			logger.Fatalf("op_agg_count: failed to create xo_log file %s: %v", effectiveXOPath, err)
		}
		defer xoFile.Close()

		xoWriter = bufio.NewWriter(xoFile)
		defer xoWriter.Flush()

		logger.Printf("op_agg_count: exactly-once log file=%s", effectiveXOPath)
	}

	// Read each shard file produced by stage 1.
	for i := 0; i < *inputTaskCount; i++ {
		shardPath := fmt.Sprintf("%s.task%d", *inputPrefix, i)

		f, err := os.Open(shardPath)
		if err != nil {
			// It's okay if some shard files are missing (e.g., empty tasks).
			if os.IsNotExist(err) {
				logger.Printf("op_agg_count: shard %s does not exist, skipping", shardPath)
				continue
			}
			logger.Fatalf("op_agg_count: failed to open shard %s: %v", shardPath, err)
		}

		logger.Printf("op_agg_count: reading shard %s", shardPath)
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			line := scanner.Text()
			// Deduplicate on full line content for now.
			if _, exists := seen[line]; exists {
				logger.Printf("DUPLICATE_REJECTED line=%q", line)
				if xoWriter != nil {
					h := crc32.ChecksumIEEE([]byte(line))
					if _, err := fmt.Fprintf(xoWriter, "DUPLICATE_REJECTED line_hash=%08x raw=%q\n", h, line); err != nil {
						logger.Printf("op_agg_count: failed to write to xo_log: %v", err)
					}
				}
				continue
			}
			seen[line] = struct{}{}

			fields := strings.Split(line, ",")
			// 1-based column index; treat missing as empty string.
			var key string
			if len(fields) >= *columnIndex {
				key = fields[*columnIndex-1]
			} else {
				key = ""
			}

			// Partition by key hash across parallel aggregators.
			if *partitionCount > 1 {
				h := crc32.ChecksumIEEE([]byte(key))
				p := int(h % uint32(*partitionCount))
				if p != *partitionIndex {
					continue
				}
			}

			counts[key]++

			// Record exactly-once processing information.
			if xoWriter != nil {
				h := crc32.ChecksumIEEE([]byte(line))
				if _, err := fmt.Fprintf(xoWriter, "PROCESSED line_hash=%08x key=%q partition=%d\n",
					h, key, *partitionIndex); err != nil {
					logger.Printf("op_agg_count: failed to write to xo_log: %v", err)
				}
			}
		}
		if err := scanner.Err(); err != nil {
			_ = f.Close()
			logger.Fatalf("op_agg_count: error reading shard %s: %v", shardPath, err)
		}
		_ = f.Close()
	}

	logger.Printf("op_agg_count: completed aggregation, unique_keys=%d, elapsed=%s",
		len(counts), time.Since(startTime))

	// Ensure output directory exists.
	outputDir := filepath.Dir(*outputPath)
	if outputDir != "" && outputDir != "." {
		if err := os.MkdirAll(outputDir, 0o755); err != nil {
			logger.Fatalf("op_agg_count: failed to create output directory %s: %v", outputDir, err)
		}
	}

	out, err := os.Create(*outputPath)
	if err != nil {
		logger.Fatalf("op_agg_count: failed to create output file %s: %v", *outputPath, err)
	}
	defer out.Close()

	writer := bufio.NewWriter(out)
	defer writer.Flush()

	// Write keys in sorted order for deterministic output.
	keys := make([]string, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	emitted := 0
	for _, k := range keys {
		line := fmt.Sprintf("%s,%d\n", k, counts[k])
		if _, err := writer.WriteString(line); err != nil {
			logger.Fatalf("op_agg_count: failed to write output: %v", err)
		}

		// Log each output tuple for exactly-once debugging.
		logger.Printf("OUTPUT_TUPLE key=%q count=%d", k, counts[k])

		emitted++
	}

	logger.Printf("op_agg_count DONE output=%s keys=%d emitted_lines=%d",
		*outputPath, len(keys), emitted)
}
