#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${ROOT_DIR}"

JOB_ID="app1_clean"
LEADER_PORT=8102

echo "[test_app1_clean] Cleaning previous clean-run outputs..."
rm -f output/app1_clean_stage1.csv.task* \
      output/app1_clean_stage1.csv.task*.state \
      output/app1_clean_counts.csv.part* \
      output/app1_clean_counts.csv.part*.xo \
      worker-job-"${JOB_ID}"-stage-2-task-*.log || true

echo "[test_app1_clean] Running Application 1 (Filter & Count) on dataset1.csv (no failures)..."

./RainStorm \
  -job "${JOB_ID}" \
  -ip 127.0.0.1 \
  -port "${LEADER_PORT}" \
  -introducer 127.0.0.1:8001 \
  -agg_column 2 \
  -agg_output ./output/app1_clean_counts.csv \
  -- \
  1 3 \
  ./op_filter -pattern "SIGN" \
  ./dataset1.csv \
  ./output/app1_clean_stage1.csv \
  1 0 100 80 120

echo "[test_app1_clean] Sorting combined counts (excluding .xo logs)..."

# Only include real aggregate outputs like *.part0, *.part1, *.part2 (not *.xo).
cat output/app1_clean_counts.csv.part? | sort > /tmp/app1_clean_counts.sorted

echo "[test_app1_clean] Done. Use this file for later diff against failure run:"
echo "  /tmp/app1_clean_counts.sorted"



