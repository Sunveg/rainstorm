#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${ROOT_DIR}"

JOB_ID="app0"
LEADER_PORT=8101

echo "[test_app0] Running Application 0 (identity) on dataset1.csv..."

./RainStorm \
  -job "${JOB_ID}" \
  -ip 127.0.0.1 \
  -port "${LEADER_PORT}" \
  -introducer 127.0.0.1:8001 \
  -- \
  1 3 \
  ./op_identity \
  ./dataset1.csv \
  ./output/app0_output.csv \
  1 0 100 80 120

echo "[test_app0] Comparing input vs combined output (order-insensitive)..."

cat output/app0_output.csv.task* | sort > /tmp/app0_out.sorted
sort dataset1.csv                 > /tmp/app0_in.sorted

if diff /tmp/app0_in.sorted /tmp/app0_out.sorted >/dev/null; then
  echo "[test_app0] SUCCESS: identity operator preserved all lines."
else
  echo "[test_app0] ERROR: diff between input and output."
  diff /tmp/app0_in.sorted /tmp/app0_out.sorted || true
fi



