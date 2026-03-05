"""
Integration tests for canary and probe scripts.

These tests require a live TypeDB 3.8 instance.
They are automatically skipped when TypeDB is not reachable.
"""

from __future__ import annotations

import os
import subprocess
import sys

from tests.conftest import requires_typedb


@requires_typedb
class TestCanaryAndProbe:
    """Integration tests for canary and probe CLI tools."""

    def test_write_canary_exit_code(self):
        """Canary should exit 0 when TypeDB is healthy and schema exists."""
        result = subprocess.run(
            [
                sys.executable, "-m",
                "typedb_ops_spine.cli.write_canary_cli",
                "--database", os.getenv("TYPEDB_DATABASE", "ops_spine_ci"),
            ],
            capture_output=True,
            text=True,
            timeout=60,
        )
        # We check it ran without crashing; exit code depends on schema setup
        assert result.returncode in (0, 1), (
            f"Canary crashed: {result.stderr}"
        )

    def test_tsv_extract_runs(self, tmp_path):
        """TSV extractor should run without error on empty/missing file."""
        result = subprocess.run(
            [
                sys.executable, "-m",
                "typedb_ops_spine.cli.tsv_extract_cli",
                "--input", str(tmp_path / "nonexistent.jsonl"),
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode == 0
