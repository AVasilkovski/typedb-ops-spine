from __future__ import annotations

from pathlib import Path

import pytest

from typedb_ops_spine.cli import migrate_cli
from typedb_ops_spine.schema_version import SchemaVersionReconcileRequired


class _ShouldNotBeCalledError(Exception):
    pass


class _FakeDriver:
    def __init__(self):
        self.closed = False
        self.databases = type("_Databases", (), {"contains": lambda *_args: False})()

    def close(self) -> None:
        self.closed = True


def test_migrate_cli_dry_run_does_not_connect(monkeypatch):
    monkeypatch.setattr(
        "typedb_ops_spine.migrate.get_migrations",
        lambda *_args, **_kwargs: [(1, __import__("pathlib").Path("001_bootstrap.tql"))],
    )

    def _boom(*_args, **_kwargs):
        raise _ShouldNotBeCalledError

    monkeypatch.setattr("typedb_ops_spine.readiness.connect_with_retries", _boom)
    monkeypatch.setattr("typedb_ops_spine.readiness.ensure_database", _boom)

    monkeypatch.setattr(
        "sys.argv",
        ["ops-migrate", "--dry-run", "--migrations-dir", "."],
    )

    rc = migrate_cli.main()
    assert rc == 0


def test_migrate_cli_reconcile_ordinal_runs_without_schema_apply(
    monkeypatch, tmp_path: Path, capsys: pytest.CaptureFixture[str],
):
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    (migrations_dir / "001_bootstrap.tql").write_text(
        "define schema_version sub entity, owns ordinal, owns git-commit, owns applied-at;",
        encoding="utf-8",
    )
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
    monkeypatch.setattr(
        "typedb_ops_spine.migrate.reconcile_migration_ordinal",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "ops-migrate",
            "--migrations-dir",
            str(migrations_dir),
            "--database",
            "ops_db",
            "--reconcile-ordinal",
            "1",
        ],
    )

    rc = migrate_cli.main()

    captured = capsys.readouterr()
    assert rc == 0
    assert driver.closed is True
    assert "Reconciled schema_version ordinal 1." in captured.out


def test_migrate_cli_reconcile_ordinal_rejects_conflicting_target_flag(
    monkeypatch, tmp_path: Path, capsys: pytest.CaptureFixture[str],
):
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()

    monkeypatch.setattr(
        "sys.argv",
        [
            "ops-migrate",
            "--migrations-dir",
            str(migrations_dir),
            "--reconcile-ordinal",
            "1",
            "--target",
            "1",
        ],
    )

    rc = migrate_cli.main()

    captured = capsys.readouterr()
    assert rc == 1
    assert "--reconcile-ordinal cannot be combined with --target" in captured.err


def test_migrate_cli_prints_recovery_guidance_on_stamp_failure(
    monkeypatch, tmp_path: Path, capsys: pytest.CaptureFixture[str],
):
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    (migrations_dir / "001_bootstrap.tql").write_text(
        "define schema_version sub entity, owns ordinal, owns git-commit, owns applied-at;",
        encoding="utf-8",
    )
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
    monkeypatch.setattr(
        "typedb_ops_spine.migrate.run_migrations",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            SchemaVersionReconcileRequired(
                db="ops_db",
                target_ordinal=1,
                source_kind="migration",
                source_name="001_bootstrap.tql",
                recovery_command=(
                    'ops-migrate --database "ops_db" '
                    '--migrations-dir "migrations" '
                    "--reconcile-ordinal 1"
                ),
                original_error=RuntimeError("write failed"),
            )
        ),
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "ops-migrate",
            "--migrations-dir",
            str(migrations_dir),
            "--database",
            "ops_db",
        ],
    )

    rc = migrate_cli.main()

    captured = capsys.readouterr()
    assert rc == 1
    assert "Plain rerun is unsafe" in captured.err
    assert "Recovery:" in captured.err
