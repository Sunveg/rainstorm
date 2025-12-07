#!/usr/bin/env bash

set -euo pipefail

# Resolve project root (directory containing this script is inside the repo)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${ROOT_DIR}"

echo "[build_all] Building HyDFS and RainStorm binaries..."

go build -o hydfs        ./cmd/hydfs
go build -o rainworker   ./cmd/rainworker
go build -o RainStorm    ./cmd/rainleader
go build -o op_identity  ./cmd/op_identity
go build -o op_filter    ./cmd/op_filter
go build -o op_agg_count ./cmd/op_agg_count
go build -o op_transform3 ./cmd/op_transform3

echo "[build_all] Done."



