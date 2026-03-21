"""
Integration tests for canary and probe scripts.

These tests require a live TypeDB 3.8 instance.
They are automatically skipped when TypeDB is not reachable.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from tests.conftest import requires_typedb


@requires_typedb
class TestCanaryAndProbe:
    """Integration tests for canary and probe CLI tools."""

    def test_write_canary_exit_code(self):
        """Canary should exit 0 when bootstrapped with the packaged example schema."""
        root = Path(__file__).resolve().parents[2]
        schema_path = root / "examples" / "minimal_project" / "schema.tql"
        migrations_dir = root / "examples" / "minimal_project" / "migrations"
        db_name = f"ops_spine_canary_{os.urandom(4).hex()}"

        result = subprocess.run(
            [
                sys.executable, "-m",
                "typedb_ops_spine.cli.write_canary_cli",
                "--database", db_name,
                "--schema", str(schema_path),
                "--migrations-dir", str(migrations_dir),
                "--stamp-schema-version-head",
            ],
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert result.returncode == 0, (
            f"Canary failed:\nSTDOUT: {result.stdout}\nSTDERR: {result.stderr}"
        )

    def test_write_canary_tenant_scoped(self, tmp_path):
        """Tenant-scoped canary should succeed if schema + tenant exists."""
        db_name = f"ops_spine_scoped_{os.urandom(4).hex()}"

        # 1. Setup minimal schema + seed data using migrations
        mig_dir = tmp_path / "migrations"
        mig_dir.mkdir()

        # 001: Schema (must include schema_version bookkeeping)
        (mig_dir / "001_schema.tql").write_text(
            "define \n"
            "schema_version sub entity, owns ordinal, owns git-commit, owns applied-at;\n"
            "ordinal sub attribute, value long;\n"
            "git-commit sub attribute, value string;\n"
            "applied-at sub attribute, value datetime;\n"
            "tenant sub entity, owns tenant-id, plays tenant-ownership:owner;\n"
            "tenant-id sub attribute, value string;\n"
            "run-capsule sub entity, owns capsule-id, owns tenant-id, \n"
            "owns session-id, owns created-at, owns query-hash, \n"
            "owns scope-lock-id, owns intent-id, owns proposal-id, \n"
            "plays tenant-ownership:owned;\n"
            "capsule-id sub attribute, value string;\n"
            "session-id sub attribute, value string;\n"
            "created-at sub attribute, value datetime;\n"
            "query-hash sub attribute, value string;\n"
            "scope-lock-id sub attribute, value string;\n"
            "intent-id sub attribute, value string;\n"
            "proposal-id sub attribute, value string;\n"
            "tenant-ownership sub relation, relates owner, relates owned;\n"
        )

        # 002: Seed Data
        (mig_dir / "002_seed.tql").write_text(
            'insert $t isa tenant, has tenant-id "test-tenant-1";'
        )

        # Run migrations to setup DB
        res_setup = subprocess.run(
            [
                sys.executable, "-m",
                "typedb_ops_spine.cli.migrate_cli",
                "--database", db_name,
                "--migrations-dir", str(mig_dir),
                "--recreate"
            ],
            capture_output=True, text=True, timeout=60
        )
        assert res_setup.returncode == 0, f"Migration setup failed:\nSTDOUT: {res_setup.stdout}\nSTDERR: {res_setup.stderr}"

        # 2. Run the canary with the valid created tenant
        result = subprocess.run(
            [
                sys.executable, "-m",
                "typedb_ops_spine.cli.write_canary_cli",
                "--database", db_name,
                "--tenant-id", "test-tenant-1",
                "--ownership-rel", "tenant-ownership"
            ],
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert result.returncode == 0, f"Canary failed (scoped valid tenant):\nSTDOUT: {result.stdout}\nSTDERR: {result.stderr}"

        # 3. Run the canary with an invalid tenant (fail closed)
        result_fail = subprocess.run(
            [
                sys.executable, "-m",
                "typedb_ops_spine.cli.write_canary_cli",
                "--database", db_name,
                "--tenant-id", "missing-tenant"
            ],
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert result_fail.returncode == 1, "Canary should have failed on missing tenant"
        assert "Target tenant 'missing-tenant' does not exist" in result_fail.stdout or "Target tenant 'missing-tenant' does not exist" in result_fail.stderr

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
