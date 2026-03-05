"""
Integration tests for schema apply and migrations.

These tests require a live TypeDB 3.8 instance.
They are automatically skipped when TypeDB is not reachable.
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path

import pytest

# Import the skip marker from conftest
from tests.conftest import requires_typedb


@requires_typedb
class TestApplySchemaAndMigrate:
    """Integration tests for schema application and migration pipeline."""

    @pytest.fixture
    def isolated_db(self):
        """Create an isolated database name for test isolation."""
        return f"ops_spine_test_{uuid.uuid4().hex[:8]}"

    @pytest.fixture
    def driver(self):
        """Connect to TypeDB."""
        from typedb_ops_spine.readiness import connect_with_retries

        address = os.getenv("TYPEDB_ADDRESS", "localhost:1729")
        username = os.getenv("TYPEDB_USERNAME", "admin")
        password = os.getenv("TYPEDB_PASSWORD", "password")

        d = connect_with_retries(
            address, username, password,
            retries=10, sleep_s=1.0,
        )
        yield d
        d.close()

    @pytest.fixture
    def example_schema(self, tmp_path: Path) -> Path:
        """Create a minimal schema file for testing."""
        schema = tmp_path / "test_schema.tql"
        schema.write_text(
            "define\n"
            "  entity tenant,\n"
            '    owns tenant-id;\n'
            "  attribute tenant-id,\n"
            "    value string;\n"
            "  entity schema_version,\n"
            "    owns ordinal,\n"
            "    owns git-commit,\n"
            "    owns applied-at;\n"
            "  attribute ordinal,\n"
            "    value long;\n"
            "  attribute git-commit,\n"
            "    value string;\n"
            "  attribute applied-at,\n"
            "    value datetime;\n",
            encoding="utf-8",
        )
        return schema

    @pytest.fixture
    def example_migrations(self, tmp_path: Path) -> Path:
        """Create minimal migration files."""
        mig_dir = tmp_path / "migrations"
        mig_dir.mkdir()
        (mig_dir / "001_bootstrap.tql").write_text(
            "define\n"
            "  entity tenant,\n"
            "    owns tenant-id;\n"
            "  attribute tenant-id,\n"
            "    value string;\n"
            "  entity schema_version,\n"
            "    owns ordinal,\n"
            "    owns git-commit,\n"
            "    owns applied-at;\n"
            "  attribute ordinal,\n"
            "    value long;\n"
            "  attribute git-commit,\n"
            "    value string;\n"
            "  attribute applied-at,\n"
            "    value datetime;\n",
            encoding="utf-8",
        )
        return mig_dir

    def test_apply_schema(self, driver, isolated_db, example_schema):
        """Test that schema can be applied to a fresh database."""
        from typedb_ops_spine.readiness import ensure_database
        from typedb_ops_spine.schema_apply import apply_schema

        ensure_database(driver, isolated_db)
        apply_schema(driver, isolated_db, [example_schema])
        # If no exception, schema was applied successfully

    def test_run_migrations(
        self, driver, isolated_db, example_schema, example_migrations,
    ):
        """Test applying schema + migrations in sequence."""
        from typedb_ops_spine.migrate import run_migrations
        from typedb_ops_spine.readiness import ensure_database
        from typedb_ops_spine.schema_apply import apply_schema

        ensure_database(driver, isolated_db)
        apply_schema(driver, isolated_db, [example_schema])
        applied = run_migrations(driver, isolated_db, example_migrations)
        assert applied >= 0

    def test_schema_health_parity(
        self, driver, isolated_db, example_schema, example_migrations,
    ):
        """Test schema health check after apply + migrate."""
        from typedb_ops_spine.migrate import run_migrations
        from typedb_ops_spine.readiness import ensure_database
        from typedb_ops_spine.schema_apply import apply_schema
        from typedb_ops_spine.schema_health import check_health

        ensure_database(driver, isolated_db)
        apply_schema(driver, isolated_db, [example_schema])
        run_migrations(driver, isolated_db, example_migrations)

        healthy, repo_ord, db_ord = check_health(
            driver, isolated_db, str(example_migrations),
        )
        assert healthy, f"Expected parity, got repo={repo_ord} db={db_ord}"
