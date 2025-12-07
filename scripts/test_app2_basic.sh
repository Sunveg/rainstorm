#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${ROOT_DIR}"

JOB_ID="app2_basic"
LEADER_PORT=8120

echo "[test_app2_basic] Cleaning previous App 2 outputs..."
rm -f output/app2_filter_stage1.csv.task*
rm -f output/app2_transform.csv.part*

echo "[test_app2_basic] Running Application 2 (Filter & Transform) on dataset2.csv..."

./RainStorm \
  -job "${JOB_ID}" \
  -ip 127.0.0.1 \
  -port "${LEADER_PORT}" \
  -introducer 127.0.0.1:8001 \
  -transform_stage \
  -agg_output ./output/app2_transform.csv \
  -- \
  1 3 \
  ./op_filter -pattern "SIGN" \
  ./dataset2.csv \
  ./output/app2_filter_stage1.csv \
  1 0 100 80 120

echo "[test_app2_basic] Leader exited. Summarizing outputs..."

echo "[test_app2_basic] Filter stage line counts:"
wc -l output/app2_filter_stage1.csv.task* || true

echo "[test_app2_basic] Transform stage line counts:"
wc -l output/app2_transform.csv.part* || true

TOTAL_FILTER=$(cat output/app2_filter_stage1.csv.task* 2>/dev/null | wc -l | awk '{print $1}')
TOTAL_TRANSFORM=$(cat output/app2_transform.csv.part* 2>/dev/null | wc -l | awk '{print $1}')

echo "[test_app2_basic] Total filtered lines:   ${TOTAL_FILTER}"
echo "[test_app2_basic] Total transformed lines: ${TOTAL_TRANSFORM}"

if [[ "${TOTAL_FILTER}" == "${TOTAL_TRANSFORM}" ]]; then
  echo "[test_app2_basic] SUCCESS: transform produced one line per filtered line."
else
  echo "[test_app2_basic] WARNING: counts differ (this may happen if you Ctrl+C early)."
fi



