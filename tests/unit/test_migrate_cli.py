from __future__ import annotations

from typedb_ops_spine.cli import migrate_cli


class _ShouldNotBeCalledError(Exception):
    pass


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
