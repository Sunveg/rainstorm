PROTOC ?= protoc

PROTO_DIR := proto
GEN_DIR := protoBuilds

.PHONY: proto tidy build clean test

# Generate protobuf files
proto:
	@echo "Generating protobuf files..."
	@mkdir -p $(GEN_DIR)/membership
	@mkdir -p $(GEN_DIR)
	$(PROTOC) \
		--go_out=$(GEN_DIR)/membership --go_opt=paths=source_relative \
		--go-grpc_out=$(GEN_DIR)/membership --go-grpc_opt=paths=source_relative \
		-I $(PROTO_DIR) \
		$(PROTO_DIR)/membership.proto
	$(PROTOC) \
		--go_out=. --go-grpc_out=. \
		-I $(PROTO_DIR) \
		$(PROTO_DIR)/hydfs.proto
	@echo "✓ Protobuf files generated"

# Tidy Go modules
tidy:
	@echo "Tidying Go modules..."
	go mod tidy
	@echo "✓ Go modules tidied"

# Build the hydfs binary
build:
	@echo "Building hydfs..."
	go build -o hydfs ./cmd/hydfs
	@echo "✓ hydfs built successfully"

# Build with proto generation
build-all: proto tidy build

# Run tests
test:
	@echo "Running tests..."
	go test ./pkg/...

# Run tests with verbose output
test-verbose:
	@echo "Running tests (verbose)..."
	go test -v ./pkg/...

# Run integration tests
test-integration:
	@echo "Running integration tests..."
	go test -v -run TestFullSystemIntegration

# Clean build artifacts
clean:
	@echo "Cleaning..."
	rm -f hydfs hydfs.exe
	@echo "✓ Cleaned"

# Clean everything including storage and generated files
clean-all: clean
	rm -rf storage/
	rm -rf protoBuilds/*/
	@echo "✓ Cleaned all"

