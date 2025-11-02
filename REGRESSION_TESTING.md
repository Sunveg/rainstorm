# HyDFS Regression Testing

A comprehensive regression testing suite for the HyDFS (Hybrid Distributed File System) that validates distributed system functionality across multiple nodes.

## Overview

This regression testing framework:
- **Creates multiple HyDFS instances** to simulate a distributed cluster
- **Monitors system logs** in real-time to validate functionality
- **Cross-platform compatible** (Windows, Linux, macOS)
- **Provides multiple test scenarios** covering different aspects of the system
- **Offers flexible execution options** for different development workflows

## Quick Start

### Prerequisites

- **Go 1.20+** (for building HyDFS binary)
- **Python 3.6+** (for test runner)
- **Git** (for repository management)

### Installation

1. Clone the repository and navigate to the project:
```bash
cd "MP3 Go"
```

2. Build the HyDFS binary:
```bash
go build -o hydfs ./cmd/hydfs  # Linux/macOS
go build -o hydfs.exe ./cmd/hydfs  # Windows
```

3. You're ready to run tests!

## Usage

### Quick Validation (Recommended for Development)
```bash
# Fast smoke test - validates basic functionality in ~30 seconds
python run-tests.py quick
```

### Individual Test Scenarios
```bash
# Run specific test scenarios (40-90 seconds each)
python run-tests.py regression -t BasicClusterFormation
python run-tests.py regression -t FileOperationsAcrossNodes
python run-tests.py regression -t ReplicationConsistency
python run-tests.py regression -t MembershipConsistency
python run-tests.py regression -t HashRingValidation
python run-tests.py regression -t ConcurrentOperations
python run-tests.py regression -t ClusterOperationalLoad
```

### Full Test Suite
```bash
# Complete validation - runs all test scenarios (3-5 minutes)
python run-tests.py full
```

### Advanced Options
```bash
# Custom configuration
python run-tests.py regression -t BasicClusterFormation --nodes 2 --timeout 3m

# Verbose output for debugging
python run-tests.py quick --verbose

# Custom timeout for slower systems
python run-tests.py full --timeout 10m
```

## Test Scenarios

| Scenario | Duration | Nodes | Description |
|----------|----------|-------|-------------|
| **BasicClusterFormation** | ~40s | 4 | Validates node startup, cluster formation, and basic connectivity |
| **FileOperationsAcrossNodes** | ~60s | 4 | Tests distributed file operations (create, read, write, delete) |
| **ReplicationConsistency** | ~90s | 5 | Validates file replication and consistency across replicas |
| **MembershipConsistency** | ~45s | 4 | Tests cluster membership management and failure detection |
| **HashRingValidation** | ~40s | 4 | Verifies consistent hashing for data distribution |
| **ConcurrentOperations** | ~80s | 6 | Tests system behavior under concurrent file operations |
| **ClusterOperationalLoad** | ~120s | 6 | Performance testing under sustained operational load |

## Development Workflow

### During Active Development
```bash
# Quick validation after code changes
python run-tests.py quick

# Test specific functionality you're working on
python run-tests.py regression -t FileOperationsAcrossNodes
```

### Before Committing Changes
```bash
# Run a few key scenarios to ensure stability
python run-tests.py regression -t BasicClusterFormation
python run-tests.py regression -t FileOperationsAcrossNodes
python run-tests.py regression -t ReplicationConsistency
```

### Before Releases
```bash
# Full comprehensive testing
python run-tests.py full
```

### Debugging Issues
```bash
# Verbose output shows detailed logs
python run-tests.py regression -t FailingTest --verbose

# Use fewer nodes for faster iteration
python run-tests.py regression -t FailingTest --nodes 2
```

## Understanding Test Output

### Success Example
```
==================================================
HyDFS Regression Test Runner - Windows
==================================================
ℹ Running quick validation test...
ℹ Executing: go test -run TestHyDFSRegressionSuite/BasicClusterFormation -timeout 2m
✓ Regression tests completed successfully (36.2s)

📊 Test Report Summary:
      Tests Run: 1
      Passed: 1
      Failed: 0
      Success Rate: 100.0%
      Total Duration: 10.0279509s
✓ No errors found in node logs
```

### What Gets Tested
- **Cluster Formation**: Node startup, membership sync, leader election
- **File Operations**: Create, read, write, delete, list operations across nodes
- **Data Consistency**: Replication synchronization, conflict resolution
- **Failure Handling**: Network partitions, node failures, recovery
- **Performance**: Concurrent operations, load handling, resource usage

### Log Analysis
The test framework automatically:
- **Monitors node logs** in real-time during test execution
- **Detects error patterns** and failure conditions
- **Generates comprehensive reports** with timing and operation counts
- **Provides detailed failure analysis** when issues occur

## Troubleshooting

### Common Issues

#### "Binary not found" Error
```bash
# Make sure HyDFS binary is built
go build -o hydfs ./cmd/hydfs        # Linux/macOS
go build -o hydfs.exe ./cmd/hydfs    # Windows
```

#### Port Conflicts
```bash
# The framework automatically uses ports 9001-9010
# Make sure these ports are available
netstat -an | findstr 900  # Windows
netstat -an | grep 900     # Linux/macOS
```

#### Test Timeouts
```bash
# Increase timeout for slower systems
python run-tests.py regression --timeout 5m

# Or use fewer nodes for faster execution
python run-tests.py regression --nodes 2
```

#### Python Import Errors
```bash
# Make sure you're using Python 3.6+
python --version

# The script uses only standard library modules
# No additional pip installations required
```

### Debug Mode

For detailed debugging information:
```bash
# Verbose mode shows all command output
python run-tests.py regression -t BasicClusterFormation --verbose

# Check the generated test logs
ls test_logs/
cat test_logs/regression_report.txt
cat test_logs/node_*.log
```

## Architecture

### Test Framework Components

1. **Test Runner (`run-tests.py`)**: 
   - Cross-platform Python script
   - Handles process management and coordination
   - Real-time progress monitoring

2. **Go Test Suite (`regression_test.go`)**:
   - Core test logic and scenarios
   - Multi-node cluster management
   - Log monitoring and validation

3. **Storage Management**:
   - Automatic cleanup of test data
   - Isolated storage directories per test run
   - Comprehensive log collection

### Cross-Platform Support

The framework automatically detects and adapts to:
- **Windows**: Uses `hydfs.exe`, `taskkill` for process management
- **Linux/macOS**: Uses `hydfs`, `pkill` for process management
- **Process Management**: Platform-appropriate cleanup and monitoring

## Performance Characteristics

### Execution Times (Typical)
- **Quick Test**: ~30 seconds (1 scenario, 2 nodes)
- **Individual Scenarios**: 40-90 seconds (scenario-dependent)
- **Full Test Suite**: 3-5 minutes (all 7 scenarios)

### Resource Usage
- **Memory**: ~50-100MB per node (depends on test scenario)
- **Disk**: ~10-50MB for logs and temporary files
- **Network**: Uses localhost ports 9001-9010

### Scalability
- **Node Count**: Configurable from 2-10 nodes per test
- **Parallel Execution**: Tests run sequentially for isolation
- **Cleanup**: Automatic process termination and resource cleanup

## Integration with CI/CD

### GitHub Actions Example
```yaml
name: HyDFS Regression Tests
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v2
    - uses: actions/setup-go@v2
      with:
        go-version: 1.20
    - name: Build HyDFS
      run: go build -o hydfs ./cmd/hydfs
    - name: Quick Test
      run: python run-tests.py quick
    - name: Full Test Suite
      run: python run-tests.py full
```

### Jenkins Pipeline Example
```groovy
pipeline {
    agent any
    stages {
        stage('Build') {
            steps {
                sh 'go build -o hydfs ./cmd/hydfs'
            }
        }
        stage('Quick Test') {
            steps {
                sh 'python run-tests.py quick'
            }
        }
        stage('Full Regression') {
            steps {
                sh 'python run-tests.py full'
            }
        }
    }
}
```

## Contributing

### Adding New Test Scenarios

1. **Add to Go test suite** (`regression_test.go`):
```go
func (suite *HyDFSRegressionSuite) TestNewScenario() {
    // Your test logic here
}
```

2. **Update Python runner** if needed for special configuration

3. **Update this documentation** with the new scenario details

### Best Practices

- **Keep tests isolated**: Each test should clean up after itself
- **Use descriptive names**: Test names should clearly indicate what's being tested  
- **Add appropriate timeouts**: Consider the expected duration of your test
- **Include error conditions**: Test both success and failure scenarios
- **Document new scenarios**: Update this README with new test descriptions