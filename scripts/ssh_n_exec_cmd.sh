#!/usr/bin/env bash
set -euo pipefail

USER="root"
BASE="fa25-cs425-10"

# Everything inside single quotes is sent literally to the remote.
# Use \$ to ensure $... expands on the remote, not locally.
CMD='
  set -e
  cd /root/MP/MP3/

  # Install the protobuf Go plugin
  go install google.golang.org/protobuf/cmd/protoc-gen-go@latest

  # Install the gRPC Go plugin
  go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest


  go env GOPATH
  export PATH=$PATH:$(go env GOPATH)/bin

  export GOTOOLCHAIN=auto

  which protoc-gen-go
  which protoc-gen-go-grpc

  go mod tidy
  make proto
  make build
 
'

for i in $(seq -w 01 10); do
  HOST="${BASE}${i}.cs.illinois.edu"
  echo "=== $HOST ==="
  ssh -o BatchMode=yes "$USER@$HOST" "$CMD"
done
