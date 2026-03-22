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


def _bootstrap_scoped_canary_db(db_name: str, tenant_id: str) -> None:
    from typedb.driver import TransactionType

    from typedb_ops_spine.readiness import connect_with_retries, ensure_database
    from typedb_ops_spine.schema_apply import apply_schema

    root = Path(__file__).resolve().parents[2]
    schema_path = root / "examples" / "minimal_project" / "schema.tql"

    address = os.getenv("TYPEDB_ADDRESS", "localhost:1729")
    username = os.getenv("TYPEDB_USERNAME", "admin")
    password = os.getenv("TYPEDB_PASSWORD", "password")

    driver = connect_with_retries(
        address, username, password,
        retries=10, sleep_s=1.0,
    )
    try:
        ensure_database(driver, db_name)
        apply_schema(driver, db_name, [schema_path])
        with driver.transaction(db_name, TransactionType.WRITE) as tx:
            tx.query(
                f'insert $t isa tenant, has tenant-id "{tenant_id}";'
            ).resolve()
            tx.commit()
    finally:
        driver.close()


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

    def test_write_canary_tenant_scoped(self):
        """Tenant-scoped canary should succeed if schema + tenant exists."""
        db_name = f"ops_spine_scoped_{os.urandom(4).hex()}"
        tenant_id = "test-tenant-1"

        _bootstrap_scoped_canary_db(db_name, tenant_id)

        # 2. Run the canary with the valid created tenant
        result = subprocess.run(
            [
                sys.executable, "-m",
                "typedb_ops_spine.cli.write_canary_cli",
                "--database", db_name,
                "--tenant-id", tenant_id,
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
