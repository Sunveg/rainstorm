#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${ROOT_DIR}"

JOB_ID="app1_fail_slow"
LEADER_PORT=8103
CONTROL_PORT=$((LEADER_PORT + 5000))

echo "[test_app1_fail_slow_auto] Root: ${ROOT_DIR}"

# Ensure clean baseline exists; if not, run clean test first.
if [[ ! -f /tmp/app1_clean_counts.sorted ]]; then
  echo "[test_app1_fail_slow_auto] Baseline /tmp/app1_clean_counts.sorted not found; running test_app1_clean.sh..."
  "${SCRIPT_DIR}/test_app1_clean.sh"
fi

echo "[test_app1_fail_slow_auto] Cleaning previous failure outputs..."
rm -f output/app1_fail_slow_stage1.csv.task* || true
rm -f output/app1_fail_slow_counts.csv.part* || true
rm -f worker-job-"${JOB_ID}"-stage-2-task-*.log || true

echo "[test_app1_fail_slow_auto] Starting leader ${JOB_ID} on port ${LEADER_PORT} (control ${CONTROL_PORT})..."

./RainStorm \
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
  1 0 10 8 12 &

LEADER_PID=$!
echo "[test_app1_fail_slow_auto] Leader PID: ${LEADER_PID}"

echo "[test_app1_fail_slow_auto] Waiting 10s for tasks to start..."
sleep 10

echo "[test_app1_fail_slow_auto] Fetching task list from control port ${CONTROL_PORT}..."

TASKS_JSON=$(curl -s "http://127.0.0.1:${CONTROL_PORT}/list_tasks" || echo "[]")

if [[ "${TASKS_JSON}" == "[]" ]]; then
  echo "[test_app1_fail_slow_auto] ERROR: no tasks returned from list_tasks."
  kill "${LEADER_PID}" 2>/dev/null || true
  exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "[test_app1_fail_slow_auto] ERROR: jq is required for automatic failure test."
  kill "${LEADER_PID}" 2>/dev/null || true
  exit 1
fi

VM=$(echo "${TASKS_JSON}" | jq -r '.[] | select(.job_id=="'"${JOB_ID}"'" and .stage_id==1) | .vm' | head -n1)
PID=$(echo "${TASKS_JSON}" | jq -r '.[] | select(.job_id=="'"${JOB_ID}"'" and .stage_id==1) | .pid' | head -n1)

if [[ -z "${VM}" || -z "${PID}" || "${VM}" == "null" || "${PID}" == "null" ]]; then
  echo "[test_app1_fail_slow_auto] ERROR: could not find a stage-1 task to kill."
  kill "${LEADER_PID}" 2>/dev/null || true
  exit 1
fi

echo "[test_app1_fail_slow_auto] Chosen task to kill/restart: VM=${VM}, PID=${PID}"

echo "[test_app1_fail_slow_auto] Killing task..."
curl -s -X POST "http://127.0.0.1:${CONTROL_PORT}/kill_task" \
  -H 'Content-Type: application/json' \
  -d "{\"vm\":\"${VM}\",\"pid\":${PID}}" >/dev/null || true

echo "[test_app1_fail_slow_auto] Waiting 5s before restart..."
sleep 5

echo "[test_app1_fail_slow_auto] Restarting task..."
curl -s -X POST "http://127.0.0.1:${CONTROL_PORT}/restart_task" \
  -H 'Content-Type: application/json' \
  -d "{\"vm\":\"${VM}\",\"pid\":${PID}}" >/dev/null || true

echo "[test_app1_fail_slow_auto] Waiting for aggregation (stage 2) to complete..."

MAX_WAIT=300  # seconds
INTERVAL=5
ELAPSED=0

while (( ELAPSED < MAX_WAIT )); do
  # NOTE: grep returns exit code 1 if there are no matches; with `set -e` that
  # would normally abort the script. We guard it with `|| true` so that the
  # wait loop keeps running until the DONE lines actually appear.
  DONE_COUNT=$(
    grep -h "op_agg_count DONE" worker-job-"${JOB_ID}"-stage-2-task-*.log 2>/dev/null \
      || true \
  )
  DONE_COUNT=$(printf "%s\n" "${DONE_COUNT}" | wc -l | awk '{print $1}')
  if (( DONE_COUNT >= 3 )); then
    echo "[test_app1_fail_slow_auto] All aggregation tasks report DONE."
    break
  fi
  sleep "${INTERVAL}"
  ELAPSED=$((ELAPSED + INTERVAL))
  echo "[test_app1_fail_slow_auto] ... still waiting (${ELAPSED}s, DONE_COUNT=${DONE_COUNT})"
done

echo "[test_app1_fail_slow_auto] Stopping leader PID ${LEADER_PID}..."
kill "${LEADER_PID}" 2>/dev/null || true

echo "[test_app1_fail_slow_auto] Building sorted failure output..."

if ls output/app1_fail_slow_counts.csv.part? >/dev/null 2>&1; then
  # Only include the aggregate output parts like *.part0, *.part1 (not *.xo logs).
  cat output/app1_fail_slow_counts.csv.part? | sort > /tmp/app1_fail_slow_counts.sorted
else
  echo "[test_app1_fail_slow_auto] ERROR: no stage-2 output files found (output/app1_fail_slow_counts.csv.part*)."
  echo "[test_app1_fail_slow_auto] Check leader and worker logs for this job."
  exit 1
fi

echo "[test_app1_fail_slow_auto] Comparing clean vs failure outputs..."

wc -l /tmp/app1_clean_counts.sorted /tmp/app1_fail_slow_counts.sorted || true

if diff /tmp/app1_clean_counts.sorted /tmp/app1_fail_slow_counts.sorted >/dev/null; then
  echo "[test_app1_fail_slow_auto] SUCCESS: outputs match (exactly-once at sink under failure)."
else
  echo "[test_app1_fail_slow_auto] ERROR: diff between clean and failure outputs."
  diff /tmp/app1_clean_counts.sorted /tmp/app1_fail_slow_counts.sorted | head -40 || true
fi

echo "[test_app1_fail_slow_auto] Checking for duplicate output lines in failure run..."
cat output/app1_fail_slow_counts.csv.part? | sort | uniq -c | awk '$1 > 1' || true

echo "[test_app1_fail_slow_auto] Done."


