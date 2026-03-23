from __future__ import annotations

from pathlib import Path

import pytest

from typedb_ops_spine.cli import apply_schema_cli
from typedb_ops_spine.schema_version import SchemaVersionReconcileRequired


class _ShouldNotBeCalledError(Exception):
    pass


class _FakeDriver:
    def __init__(self):
        self.closed = False
        self.databases = type("_Databases", (), {"contains": lambda *_args: False})()

    def close(self) -> None:
        self.closed = True


def test_apply_schema_cli_dry_run_with_auto_scrub_does_not_connect(monkeypatch, tmp_path: Path):
    schema_path = tmp_path / "schema.tql"
    schema_path.write_text(
        "define\n"
        "  entity evidence,\n"
        "    owns template-id;\n"
        "  entity validation-evidence sub evidence,\n"
        "    owns template-id;\n"
        "  attribute template-id,\n"
        "    value string;\n",
        encoding="utf-8",
    )

    def _boom(*_args, **_kwargs):
        raise _ShouldNotBeCalledError

    monkeypatch.setattr("typedb_ops_spine.readiness.connect_with_retries", _boom)
    monkeypatch.setattr("typedb_ops_spine.readiness.ensure_database", _boom)
    monkeypatch.setattr(
        "sys.argv",
        [
            "ops-apply-schema",
            "--dry-run",
            "--schema",
            str(schema_path),
            "--auto-migrate-redeclarations",
        ],
    )

    rc = apply_schema_cli.main()
    assert rc == 0


def test_apply_schema_cli_fails_fast_on_invalid_tls_localhost(
    monkeypatch, tmp_path: Path, capsys: pytest.CaptureFixture[str],
):
    schema_path = tmp_path / "schema.tql"
    schema_path.write_text("define entity tenant;", encoding="utf-8")

    def _boom(*_args, **_kwargs):
        raise _ShouldNotBeCalledError

    monkeypatch.setattr("typedb_ops_spine.readiness.connect_with_retries", _boom)
    monkeypatch.setenv("TYPEDB_TLS", "true")
    monkeypatch.delenv("TYPEDB_ADDRESS", raising=False)
    monkeypatch.setattr(
        "sys.argv",
        [
            "ops-apply-schema",
            "--schema",
            str(schema_path),
        ],
    )

    rc = apply_schema_cli.main()

    captured = capsys.readouterr()
    assert rc == 1
    assert "No explicit --address/TYPEDB_ADDRESS was provided" in captured.err


def test_apply_schema_cli_reconcile_head_mode_runs_without_schema(
    monkeypatch, tmp_path: Path, capsys: pytest.CaptureFixture[str],
):
    driver = _FakeDriver()
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()

    def _boom(*_args, **_kwargs):
        raise _ShouldNotBeCalledError

    monkeypatch.setattr("typedb_ops_spine.schema_apply.resolve_schema_files", _boom)
    monkeypatch.setattr(
        "typedb_ops_spine.readiness.resolve_connection_config",
        lambda *_args, **_kwargs: ("localhost:1729", False, None),
    )
    monkeypatch.setattr(
        "typedb_ops_spine.readiness.connect_with_retries",
        lambda *_args, **_kwargs: driver,
    )
    monkeypatch.setattr("typedb_ops_spine.readiness.ensure_database", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        "typedb_ops_spine.schema_apply.reconcile_schema_version_head",
        lambda *_args, **_kwargs: 3,
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "ops-apply-schema",
            "--database",
            "ops_db",
            "--migrations-dir",
            str(migrations_dir),
            "--reconcile-schema-version-head",
        ],
    )

    rc = apply_schema_cli.main()

    captured = capsys.readouterr()
    assert rc == 0
    assert driver.closed is True
    assert "Reconciled schema_version head ordinal to 3." in captured.out


def test_apply_schema_cli_reconcile_head_rejects_conflicting_schema_flag(
    monkeypatch, tmp_path: Path, capsys: pytest.CaptureFixture[str],
):
    schema_path = tmp_path / "schema.tql"
    schema_path.write_text("define entity tenant;", encoding="utf-8")
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()

    monkeypatch.setattr(
        "sys.argv",
        [
            "ops-apply-schema",
            "--schema",
            str(schema_path),
            "--migrations-dir",
            str(migrations_dir),
            "--reconcile-schema-version-head",
        ],
    )

    rc = apply_schema_cli.main()

    captured = capsys.readouterr()
    assert rc == 1
    assert "--reconcile-schema-version-head cannot be combined with --schema" in captured.err


def test_apply_schema_cli_prints_recovery_guidance_on_stamp_failure(
    monkeypatch, tmp_path: Path, capsys: pytest.CaptureFixture[str],
):
    schema_path = tmp_path / "schema.tql"
    schema_path.write_text("define entity tenant;", encoding="utf-8")
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()

    driver = _FakeDriver()

    monkeypatch.setattr(
        "typedb_ops_spine.readiness.resolve_connection_config",
        lambda *_args, **_kwargs: ("localhost:1729", False, None),
    )
    monkeypatch.setattr(
        "typedb_ops_spine.readiness.connect_with_retries",
        lambda *_args, **_kwargs: driver,
    )
    monkeypatch.setattr("typedb_ops_spine.readiness.ensure_database", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("typedb_ops_spine.schema_apply.apply_schema", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("typedb_ops_spine.schema_apply.head_migration_ordinal", lambda *_args, **_kwargs: 7)
    monkeypatch.setattr(
        "typedb_ops_spine.schema_apply.stamp_schema_version_head",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            SchemaVersionReconcileRequired(
                db="ops_db",
                target_ordinal=7,
                source_kind="authoritative_apply",
                source_name="migrations",
                recovery_command=(
                    'ops-apply-schema --database "ops_db" '
                    '--migrations-dir "migrations" '
                    "--reconcile-schema-version-head"
                ),
                original_error=RuntimeError("write failed"),
            )
        ),
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "ops-apply-schema",
            "--schema",
            str(schema_path),
            "--database",
            "ops_db",
            "--migrations-dir",
            str(migrations_dir),
            "--stamp-schema-version-head",
        ],
    )

    rc = apply_schema_cli.main()

    captured = capsys.readouterr()
    assert rc == 1
    assert "Plain rerun is unsafe" in captured.err
    assert "Recovery:" in captured.err
