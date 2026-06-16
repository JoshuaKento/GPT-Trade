"""Regression tests for orchestration package import entry points."""

import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def run_python(*args: str) -> subprocess.CompletedProcess[str]:
    """Run Python from the repository root."""
    return subprocess.run(
        [sys.executable, *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=10,
    )


def test_orchestration_package_imports() -> None:
    result = run_python("-c", "import orchestration")

    assert result.returncode == 0, result.stderr
    assert result.stderr == ""


def test_orchestration_cli_help_reaches_parser() -> None:
    result = run_python("-m", "orchestration.cli", "--help")

    assert result.returncode == 0, result.stderr
    assert result.stderr == ""
    assert "usage:" in result.stdout.lower()
    assert "ETL Pipeline Orchestration CLI" in result.stdout
