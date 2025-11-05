#!/bin/bash

# Generate Go code from protobuf definitions
echo "Generating protobuf code..."

# Generate for membership protocol (existing)
protoc --go_out=. --go-grpc_out=. proto/membership.proto

# Generate for file transfer service
protoc --go_out=. --go-grpc_out=. proto/filetransfer.proto

# Generate for coordination service
protoc --go_out=. --go-grpc_out=. proto/coordination.proto

# Generate for file service
protoc --go_out=. --go-grpc_out=./protoBuilds/fileservice proto/fileservice.proto

# Generate for existing hydfs proto if it exists
if [ -f "proto/hydfs.proto" ]; then
    protoc --go_out=. --go-grpc_out=. proto/hydfs.proto
fi

echo "Proto generation complete"

# Install required Go modules
go mod tidy