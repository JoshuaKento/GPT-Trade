#!/usr/bin/env python3
"""Test runner script for EDGAR toolkit with comprehensive reporting."""

import argparse
import subprocess
import sys
from pathlib import Path


def run_command(cmd, description):
    """Run a shell command and handle output."""
    print(f"\n{'='*60}")
    print(f"Running: {description}")
    print(f"Command: {' '.join(cmd)}")
    print("=" * 60)

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        print(result.stdout)
        if result.stderr:
            print("STDERR:", result.stderr)
        return True
    except subprocess.CalledProcessError as e:
        print(f"ERROR: Command failed with exit code {e.returncode}")
        print("STDOUT:", e.stdout)
        print("STDERR:", e.stderr)
        return False


def main():
    """Main test runner function."""
    parser = argparse.ArgumentParser(description="Run EDGAR toolkit tests")
    parser.add_argument(
        "--type",
        choices=["unit", "integration", "all", "coverage", "fast"],
        default="all",
        help="Type of tests to run",
    )
    parser.add_argument(
        "--file", help="Specific test file to run (e.g., test_companies.py)"
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    parser.add_argument(
        "--no-coverage", action="store_true", help="Skip coverage reporting"
    )
    parser.add_argument(
        "--parallel", "-p", action="store_true", help="Run tests in parallel"
    )

    args = parser.parse_args()

    # Base pytest command
    base_cmd = ["python", "-m", "pytest"]

    # Add verbosity
    if args.verbose:
        base_cmd.extend(["-v", "-s"])

    # Add parallel execution
    if args.parallel:
        base_cmd.extend(["-n", "auto"])

    # Configure coverage
    if not args.no_coverage and args.type in ["all", "coverage"]:
        base_cmd.extend(
            [
                "--cov=edgar",
                "--cov-report=term-missing",
                "--cov-report=html:htmlcov",
                "--cov-report=xml:coverage.xml",
                "--cov-branch",
            ]
        )

    # Configure test selection
    if args.file:
        test_path = (
            f"tests/{args.file}" if not args.file.startswith("tests/") else args.file
        )
        base_cmd.append(test_path)
    elif args.type == "unit":
        base_cmd.extend(["-m", "unit", "tests/"])
    elif args.type == "integration":
        base_cmd.extend(["-m", "integration", "tests/"])
    elif args.type == "fast":
        base_cmd.extend(["-m", "not slow", "tests/"])
    else:
        base_cmd.append("tests/")

    # Run the tests
    success = run_command(base_cmd, f"Running {args.type} tests")

    if success:
        print(f"\n✅ All {args.type} tests passed!")

        # Generate coverage report if applicable
        if not args.no_coverage and args.type in ["all", "coverage"]:
            print(f"\n📊 Coverage report generated:")
            print(f"  - Terminal: See output above")
            print(f"  - HTML: htmlcov/index.html")
            print(f"  - XML: coverage.xml")
    else:
        print(f"\n❌ Some {args.type} tests failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
