#!/usr/bin/env python3
"""
HyDFS Regression Test Runner - Cross Platform Python Version
Works on any system with Python 3.6+
"""

import os
import sys
import subprocess
import platform
import argparse
import shutil
import time
from pathlib import Path

class Colors:
    """ANSI color codes for colored output"""
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    END = '\033[0m'
    
    @classmethod
    def disable(cls):
        """Disable colors for non-terminal output"""
        cls.RED = cls.GREEN = cls.YELLOW = cls.BLUE = cls.CYAN = cls.END = ''

class TestRunner:
    def __init__(self):
        self.os_name = platform.system()
        self.is_windows = self.os_name == 'Windows'
        self.binary_name = 'hydfs.exe' if self.is_windows else 'hydfs'
        self.verbose = False
        
        # Disable colors if not in terminal
        if not sys.stdout.isatty():
            Colors.disable()
    
    def print_status(self, message):
        print(f"{Colors.GREEN}✓{Colors.END} {message}")
    
    def print_error(self, message):
        print(f"{Colors.RED}✗{Colors.END} {message}")
    
    def print_warning(self, message):
        print(f"{Colors.YELLOW}⚠{Colors.END} {message}")
    
    def print_info(self, message):
        print(f"{Colors.CYAN}ℹ{Colors.END} {message}")
    
    def print_header(self, message):
        print(f"{Colors.CYAN}{'='*50}{Colors.END}")
        print(f"{Colors.CYAN}{message}{Colors.END}")
        print(f"{Colors.CYAN}{'='*50}{Colors.END}")
    
    def run_command(self, cmd, check=True, capture=False):
        """Run a shell command"""
        try:
            if capture:
                result = subprocess.run(cmd, shell=True, capture_output=True, text=True, check=check)
                return result.returncode == 0, result.stdout, result.stderr
            else:
                result = subprocess.run(cmd, shell=True, check=check)
                return result.returncode == 0, "", ""
        except subprocess.CalledProcessError as e:
            if capture:
                return False, "", str(e)
            return False, "", ""
    
    def check_requirements(self):
        """Check system requirements"""
        self.print_info("Checking system requirements...")
        
        # Check Go
        success, stdout, _ = self.run_command("go version", check=False, capture=True)
        if not success:
            self.print_error("Go is not installed or not in PATH")
            return False
        
        self.print_status(f"Go available: {stdout.strip()}")
        
        # Check OS-specific tools
        if self.is_windows:
            success, _, _ = self.run_command("taskkill /?", check=False, capture=True)
            if success:
                self.print_status("taskkill available for process management")
            else:
                self.print_warning("taskkill not found - process cleanup may not work")
        else:
            success, _, _ = self.run_command("which pkill", check=False, capture=True)
            if success:
                self.print_status("pkill available for process management")
            else:
                self.print_warning("pkill not found - process cleanup may not work")
        
        self.print_info(f"OS: {self.os_name}, Binary: {self.binary_name}")
        return True
    
    def cleanup_environment(self):
        """Clean up test environment"""
        self.print_info("Cleaning up test environment...")
        
        # Kill existing processes
        if self.is_windows:
            self.run_command(f"taskkill /f /im {self.binary_name}", check=False)
        else:
            self.run_command(f"pkill -f {self.binary_name}", check=False)
        
        # Remove old test artifacts
        for path in ['test_logs', 'test_storage', self.binary_name]:
            if os.path.exists(path):
                if os.path.isdir(path):
                    shutil.rmtree(path)
                else:
                    os.remove(path)
        
        self.print_status("Cleanup completed")
    
    def build_binary(self):
        """Build HyDFS binary"""
        self.print_info(f"Building HyDFS binary for {self.os_name}...")
        
        success, _, stderr = self.run_command(f"go build -o {self.binary_name} ./cmd/hydfs", capture=True)
        
        if success and os.path.exists(self.binary_name):
            self.print_status(f"Build completed successfully: {self.binary_name}")
            return True
        else:
            self.print_error(f"Build failed: {stderr}")
            return False
    
    def run_basic_tests(self):
        """Run basic unit tests"""
        self.print_info("Running basic unit tests...")
        
        success, _, stderr = self.run_command("go test ./...", capture=True)
        
        if success:
            self.print_status("Basic tests passed")
            return True
        else:
            self.print_error(f"Basic tests failed: {stderr}")
            return False
    
    def run_regression_tests(self, node_count=4, log_dir="test_logs", test_name=None, timeout="5m"):
        """Run regression tests"""
        if test_name:
            self.print_info(f"Running specific test: {test_name}...")
        else:
            self.print_info("Running comprehensive regression tests...")
            self.print_warning("This may take 3-5 minutes for all 7 test scenarios...")
        
        # Set environment variables
        os.environ['HYDFS_TEST_NODE_COUNT'] = str(node_count)
        os.environ['HYDFS_TEST_LOG_DIR'] = log_dir
        
        if self.verbose:
            self.print_info("Test Configuration:")
            self.print_info(f"  Node Count: {node_count}")
            self.print_info(f"  Log Directory: {log_dir}")
            self.print_info(f"  Timeout: {timeout}")
            if test_name:
                self.print_info(f"  Specific Test: {test_name}")
            self.print_info("  Verbose Output: Enabled")
            
            if test_name:
                cmd = f"go test -v -run TestHyDFSRegressionSuite/{test_name} -timeout {timeout}"
            else:
                cmd = f"go test -v -run TestHyDFSRegressionSuite -timeout {timeout}"
        else:
            if test_name:
                cmd = f"go test -run TestHyDFSRegressionSuite/{test_name} -timeout {timeout}"
            else:
                cmd = f"go test -run TestHyDFSRegressionSuite -timeout {timeout}"
        
        self.print_info(f"Executing: {cmd}")
        start_time = time.time()
        
        # Run with real-time output for better feedback
        try:
            process = subprocess.Popen(
                cmd, 
                shell=True, 
                stdout=subprocess.PIPE, 
                stderr=subprocess.STDOUT, 
                text=True,
                bufsize=1,
                universal_newlines=True
            )
            
            # Show progress dots every 5 seconds
            last_output_time = time.time()
            
            while True:
                output = process.stdout.readline()
                if output == '' and process.poll() is not None:
                    break
                if output:
                    if self.verbose:
                        print(output.strip())
                    last_output_time = time.time()
                else:
                    # Show progress if no output for a while
                    if time.time() - last_output_time > 5:
                        elapsed = time.time() - start_time
                        print(f"⏳ Test running... ({elapsed:.0f}s elapsed)")
                        last_output_time = time.time()
                    time.sleep(0.1)
            
            return_code = process.poll()
            elapsed = time.time() - start_time
            
            if return_code == 0:
                self.print_status(f"Regression tests completed successfully ({elapsed:.1f}s)")
                return True
            else:
                self.print_error(f"Regression tests failed ({elapsed:.1f}s)")
                return False
                
        except KeyboardInterrupt:
            self.print_warning("Test interrupted by user")
            if process:
                process.terminate()
            return False
    
    def analyze_results(self, log_dir="test_logs"):
        """Analyze test results"""
        self.print_info("Analyzing test results...")
        
        log_path = Path(log_dir)
        if not log_path.exists():
            self.print_warning(f"Log directory not found: {log_dir}")
            return
        
        # Count log files
        node_logs = list(log_path.glob("node_*.log"))
        if node_logs:
            self.print_status(f"Found {len(node_logs)} node log files")
        
        # Check for test report
        report_file = log_path / "regression_report.txt"
        if report_file.exists():
            self.print_status(f"Test report generated: {report_file}")
            
            # Display summary
            try:
                with open(report_file, 'r') as f:
                    content = f.read()
                    if 'SUMMARY:' in content:
                        print(f"{Colors.CYAN}📊 Test Report Summary:{Colors.END}")
                        lines = content.split('\n')
                        in_summary = False
                        for line in lines:
                            if 'SUMMARY:' in line:
                                in_summary = True
                                continue
                            elif in_summary and line.strip():
                                if line.startswith('DETAILED'):
                                    break
                                print(f"    {line}")
            except Exception as e:
                self.print_warning(f"Could not read report file: {e}")
        
        # Check for errors
        error_count = 0
        for log_file in node_logs:
            try:
                with open(log_file, 'r') as f:
                    content = f.read()
                    if any(keyword in content for keyword in ['Error:', 'Failed', 'panic', 'fatal']):
                        error_count += 1
            except Exception:
                pass
        
        if error_count > 0:
            self.print_warning(f"Found errors in {error_count} node log files")
        else:
            self.print_status("No errors found in node logs")

def main():
    parser = argparse.ArgumentParser(description='HyDFS Regression Test Runner - Cross Platform')
    parser.add_argument('command', nargs='?', default='help',
                       choices=['clean', 'build', 'test', 'regression', 'full', 'check', 'quick', 'help'],
                       help='Command to execute')
    parser.add_argument('-v', '--verbose', action='store_true',
                       help='Enable verbose output')
    parser.add_argument('-n', '--nodes', type=int, default=4,
                       help='Number of nodes to test (default: 4)')
    parser.add_argument('-l', '--logdir', default='test_logs',
                       help='Log directory (default: test_logs)')
    parser.add_argument('-c', '--clean', action='store_true',
                       help='Clean before running')
    parser.add_argument('-t', '--test-name', 
                       choices=['BasicClusterFormation', 'FileOperationsAcrossNodes', 
                               'ReplicationConsistency', 'MembershipConsistency',
                               'HashRingValidation', 'ConcurrentOperations', 
                               'ClusterOperationalLoad'],
                       help='Run specific test scenario only')
    parser.add_argument('--timeout', default='5m',
                       help='Test timeout (default: 5m)')
    
    args = parser.parse_args()
    
    runner = TestRunner()
    runner.verbose = args.verbose
    
    # Show header
    runner.print_header(f"HyDFS Regression Test Runner - {runner.os_name}")
    
    # Clean first if requested
    if args.clean:
        runner.cleanup_environment()
        print()
    
    # Execute command
    try:
        if args.command == 'clean':
            runner.cleanup_environment()
            
        elif args.command == 'build':
            if not runner.check_requirements():
                sys.exit(1)
            print()
            if not runner.build_binary():
                sys.exit(1)
                
        elif args.command == 'test':
            if not runner.check_requirements():
                sys.exit(1)
            print()
            if not runner.run_basic_tests():
                sys.exit(1)
                
        elif args.command == 'regression':
            if not runner.check_requirements():
                sys.exit(1)
            print()
            if not os.path.exists(runner.binary_name):
                runner.print_info("Binary not found, building first...")
                if not runner.build_binary():
                    sys.exit(1)
                print()
            
            success = runner.run_regression_tests(args.nodes, args.logdir, args.test_name, args.timeout)
            print()
            runner.analyze_results(args.logdir)
            if not success:
                sys.exit(1)
                
        elif args.command == 'quick':
            if not runner.check_requirements():
                sys.exit(1)
            print()
            if not os.path.exists(runner.binary_name):
                runner.print_info("Binary not found, building first...")
                if not runner.build_binary():
                    sys.exit(1)
                print()
            
            # Run just BasicClusterFormation for quick validation
            runner.print_info("Running quick validation test (BasicClusterFormation only)...")
            success = runner.run_regression_tests(2, args.logdir, "BasicClusterFormation", "2m")
            print()
            runner.analyze_results(args.logdir)
            if not success:
                sys.exit(1)
                
        elif args.command == 'full':
            if not runner.check_requirements():
                sys.exit(1)
            print()
            
            runner.cleanup_environment()
            print()
            
            if not runner.build_binary():
                sys.exit(1)
            print()
            
            if not runner.run_basic_tests():
                sys.exit(1)
            print()
            
            success = runner.run_regression_tests(args.nodes, args.logdir, args.test_name, args.timeout)
            print()
            runner.analyze_results(args.logdir)
            print()
            
            if success:
                runner.print_header("✓ FULL TEST CYCLE COMPLETED SUCCESSFULLY")
            else:
                runner.print_header("✗ FULL TEST CYCLE FAILED")
                sys.exit(1)
                
        elif args.command == 'check':
            runner.check_requirements()
            
        elif args.command == 'help':
            parser.print_help()
            print()
            print("Commands:")
            print("  quick       - Quick validation test (~30 seconds)")
            print("  regression  - Full regression suite (~3-5 minutes)")
            print("  full        - Complete cycle: clean + build + test + regression")
            print()
            print("Examples:")
            print("  python run-tests.py quick                                    # Quick test")
            print("  python run-tests.py regression -t BasicClusterFormation     # Single test")
            print("  python run-tests.py regression --verbose --nodes 2          # Fast verbose")
            print("  python run-tests.py regression --timeout 3m                 # Custom timeout")
            print("  python run-tests.py full --nodes 6                         # Complete with 6 nodes")
            print("  python run-tests.py clean                                   # Clean up artifacts")
            print()
            print("Available test scenarios:")
            print("  BasicClusterFormation     - Node startup and cluster formation")
            print("  FileOperationsAcrossNodes - Distributed file operations")
            print("  ReplicationConsistency    - File replication validation")
            print("  MembershipConsistency     - Cluster membership sync")
            print("  HashRingValidation        - Consistent hashing verification")
            print("  ConcurrentOperations      - Multi-threaded operations")
            print("  ClusterOperationalLoad    - Performance under load")
            
    except KeyboardInterrupt:
        runner.print_error("Interrupted by user")
        sys.exit(130)
    except Exception as e:
        runner.print_error(f"Unexpected error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()