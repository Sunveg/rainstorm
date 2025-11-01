#!/bin/bash

# Generate Go code from protobuf definitions
protoc --go_out=. --go-grpc_out=. proto/hydfs.proto

# Install required Go modules
go mod tidy