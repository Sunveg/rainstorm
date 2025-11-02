#!/usr/bin/env pwsh

# HyDFS Regression Test Runner for Windows PowerShell
# This script builds the HyDFS binary and runs comprehensive regression tests

param(
    [switch]$Clean = $false,
    [switch]$Verbose = $false,
    [int]$NodeCount = 4,
    [string]$LogDir = "test_logs"
)

Write-Host "=================================" -ForegroundColor Cyan
Write-Host "HyDFS Regression Test Runner" -ForegroundColor Cyan
Write-Host "=================================" -ForegroundColor Cyan

# Function to cleanup previous test artifacts
function Cleanup-TestEnvironment {
    Write-Host "Cleaning up test environment..." -ForegroundColor Yellow
    
    if (Test-Path "test_logs") {
        Remove-Item "test_logs" -Recurse -Force -ErrorAction SilentlyContinue
        Write-Host "  ✓ Removed old test logs" -ForegroundColor Green
    }
    
    if (Test-Path "test_storage") {
        Remove-Item "test_storage" -Recurse -Force -ErrorAction SilentlyContinue
        Write-Host "  ✓ Removed old test storage" -ForegroundColor Green
    }
    
    if (Test-Path "hydfs.exe") {
        Remove-Item "hydfs.exe" -Force -ErrorAction SilentlyContinue
        Write-Host "  ✓ Removed old binary" -ForegroundColor Green
    }
    
    # Kill any remaining HyDFS processes
    Get-Process -Name "hydfs" -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
    Write-Host "  ✓ Killed any remaining HyDFS processes" -ForegroundColor Green
}

# Function to build the HyDFS binary
function Build-HyDFS {
    Write-Host "Building HyDFS binary..." -ForegroundColor Yellow
    
    $buildResult = & go build -o hydfs.exe ./cmd/hydfs 2>&1
    
    if ($LASTEXITCODE -ne 0) {
        Write-Host "  ✗ Build failed:" -ForegroundColor Red
        Write-Host $buildResult -ForegroundColor Red
        return $false
    }
    
    if (-not (Test-Path "hydfs.exe")) {
        Write-Host "  ✗ Binary not found after build" -ForegroundColor Red
        return $false
    }
    
    Write-Host "  ✓ HyDFS binary built successfully" -ForegroundColor Green
    return $true
}

# Function to run regression tests
function Run-RegressionTests {
    Write-Host "Running comprehensive regression tests..." -ForegroundColor Yellow
    
    # Set test parameters via environment variables if needed
    $env:HYDFS_TEST_NODE_COUNT = $NodeCount
    $env:HYDFS_TEST_LOG_DIR = $LogDir
    
    if ($Verbose) {
        Write-Host "  Test Configuration:" -ForegroundColor Cyan
        Write-Host "    Node Count: $NodeCount" -ForegroundColor Cyan
        Write-Host "    Log Directory: $LogDir" -ForegroundColor Cyan
        Write-Host "    Verbose Output: Enabled" -ForegroundColor Cyan
    }
    
    # Run the regression tests
    if ($Verbose) {
        $testResult = & go test -v -run TestHyDFSRegressionSuite -timeout 10m 2>&1
    } else {
        $testResult = & go test -run TestHyDFSRegressionSuite -timeout 10m 2>&1
    }
    
    $exitCode = $LASTEXITCODE
    
    # Display test output
    Write-Host $testResult
    
    return $exitCode -eq 0
}

# Function to analyze test results
function Analyze-Results {
    param([string]$LogDirectory)
    
    Write-Host "Analyzing test results..." -ForegroundColor Yellow
    
    if (-not (Test-Path $LogDirectory)) {
        Write-Host "  ⚠ Log directory not found: $LogDirectory" -ForegroundColor Yellow
        return
    }
    
    # Count log files (nodes that started)
    $nodeLogFiles = Get-ChildItem -Path $LogDirectory -Filter "node_*.log" -ErrorAction SilentlyContinue
    if ($nodeLogFiles) {
        Write-Host "  ✓ Found $($nodeLogFiles.Count) node log files" -ForegroundColor Green
    }
    
    # Check for test report
    $reportFile = Join-Path $LogDirectory "regression_report.txt"
    if (Test-Path $reportFile) {
        Write-Host "  ✓ Test report generated: $reportFile" -ForegroundColor Green
        
        # Display report summary
        $reportContent = Get-Content $reportFile -ErrorAction SilentlyContinue
        $summarySection = $reportContent | Select-String -Pattern "SUMMARY:" -Context 0,10
        if ($summarySection) {
            Write-Host "  📊 Test Report Summary:" -ForegroundColor Cyan
            $summarySection.Context.PostContext | ForEach-Object {
                if ($_ -match "^\s*(.*)$") {
                    Write-Host "    $($matches[1])" -ForegroundColor White
                }
            }
        }
    }
    
    # Check for errors in node logs
    $errorCount = 0
    foreach ($logFile in $nodeLogFiles) {
        $errors = Select-String -Path $logFile.FullName -Pattern "Error:|Failed|panic|fatal" -ErrorAction SilentlyContinue
        if ($errors) {
            $errorCount += $errors.Count
        }
    }
    
    if ($errorCount -gt 0) {
        Write-Host "  ⚠ Found $errorCount error(s) in node logs" -ForegroundColor Yellow
    } else {
        Write-Host "  ✓ No errors found in node logs" -ForegroundColor Green
    }
}

# Function to display usage help
function Show-Help {
    Write-Host "Usage: .\run-regression-tests.ps1 [OPTIONS]" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "Options:" -ForegroundColor Yellow
    Write-Host "  -Clean        Clean up previous test artifacts before running"
    Write-Host "  -Verbose      Enable verbose test output"
    Write-Host "  -NodeCount    Number of nodes to test with (default: 4)"
    Write-Host "  -LogDir       Directory for test logs (default: test_logs)"
    Write-Host "  -Help         Show this help message"
    Write-Host ""
    Write-Host "Examples:" -ForegroundColor Yellow
    Write-Host "  .\run-regression-tests.ps1                    # Run with defaults"
    Write-Host "  .\run-regression-tests.ps1 -Clean -Verbose    # Clean and verbose"
    Write-Host "  .\run-regression-tests.ps1 -NodeCount 6       # Test with 6 nodes"
}

# Main execution
try {
    # Check if help was requested
    if ($args -contains "-Help" -or $args -contains "--help" -or $args -contains "/?" -or $args -contains "help") {
        Show-Help
        exit 0
    }
    
    # Check if Go is available
    $goVersion = & go version 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Host "✗ Go is not installed or not in PATH" -ForegroundColor Red
        exit 1
    }
    
    Write-Host "✓ Go available: $($goVersion)" -ForegroundColor Green
    
    # Clean up if requested
    if ($Clean) {
        Cleanup-TestEnvironment
    }
    
    # Build the binary
    if (-not (Build-HyDFS)) {
        Write-Host "✗ Failed to build HyDFS binary" -ForegroundColor Red
        exit 1
    }
    
    # Run regression tests
    Write-Host ""
    $testSuccess = Run-RegressionTests
    
    # Analyze results
    Write-Host ""
    Analyze-Results -LogDirectory $LogDir
    
    # Final status
    Write-Host ""
    Write-Host "=================================" -ForegroundColor Cyan
    if ($testSuccess) {
        Write-Host "✓ REGRESSION TESTS PASSED" -ForegroundColor Green
        Write-Host "All tests completed successfully!" -ForegroundColor Green
    } else {
        Write-Host "✗ REGRESSION TESTS FAILED" -ForegroundColor Red
        Write-Host "Some tests failed. Check logs for details." -ForegroundColor Red
    }
    Write-Host "=================================" -ForegroundColor Cyan
    
    # Cleanup processes
    Get-Process -Name "hydfs" -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
    
    exit $(if ($testSuccess) { 0 } else { 1 })
}
catch {
    Write-Host "✗ Unexpected error: $($_.Exception.Message)" -ForegroundColor Red
    
    # Cleanup processes on error
    Get-Process -Name "hydfs" -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
    
    exit 1
}