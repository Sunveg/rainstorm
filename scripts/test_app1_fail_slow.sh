#!/usr/bin/env bash

set -euo pipefail

# This script ONLY starts the slow-failure leader for Application 1.
# You still need to:
#   1) In another terminal, call list_tasks on the control port.
#   2) Kill a stage-1 task via kill_task.
#   3) Call restart_task for that task.
#   4) Ctrl+C this leader when you're done.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${ROOT_DIR}"

JOB_ID="app1_fail_slow"
LEADER_PORT=8103

echo "[test_app1_fail_slow] Starting Application 1 (Filter & Count) with slow rate and long wait..."
echo "[test_app1_fail_slow] Leader port: ${LEADER_PORT}, control port: $((LEADER_PORT + 5000))"
echo "[test_app1_fail_slow] In another terminal, you can run:"
echo "  curl http://127.0.0.1:$((LEADER_PORT + 5000))/list_tasks | jq"
echo "and then kill/restart a stage-1 task via kill_task/restart_task."
echo

./rainleader \
  -job "${JOB_ID}" \
  -ip 127.0.0.1 \
  -port "${LEADER_PORT}" \
  -introducer 127.0.0.1:8001 \
  -agg_column 2 \
  -agg_output ./output/app1_fail_slow_counts.csv \
  -- \
  1 3 \
  ./op_filter -pattern "SIGN" \
  ./dataset1.csv \
  ./output/app1_fail_slow_stage1.csv \
  1 0 10 8 12

echo "[test_app1_fail_slow] Leader exited."



