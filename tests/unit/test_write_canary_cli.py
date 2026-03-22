from __future__ import annotations

import sys
import types

from typedb_ops_spine.cli import write_canary_cli
from typedb_ops_spine.exec import QueryMode


class _FakeTransaction:
    def __init__(self, tx_type: str):
        self.tx_type = tx_type
        self.committed = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def commit(self) -> None:
        self.committed = True


class _FakeDriver:
    def __init__(self):
        self.closed = False

    def transaction(self, _db_name: str, tx_type: str) -> _FakeTransaction:
        return _FakeTransaction(tx_type)

    def close(self) -> None:
        self.closed = True


def _install_fake_typedb(monkeypatch) -> None:
    typedb_mod = types.ModuleType("typedb")
    typedb_mod.__version__ = "test-driver"

    typedb_driver_mod = types.ModuleType("typedb.driver")

    class _TransactionType:
        READ = "READ"
        WRITE = "WRITE"

    typedb_driver_mod.TransactionType = _TransactionType

    monkeypatch.setitem(sys.modules, "typedb", typedb_mod)
    monkeypatch.setitem(sys.modules, "typedb.driver", typedb_driver_mod)


def test_write_canary_cli_default_path_uses_execute_signature_without_extra_kwargs(
    monkeypatch, tmp_path
):
    _install_fake_typedb(monkeypatch)
    driver = _FakeDriver()
    calls: list[tuple[QueryMode, str]] = []

    def _strict_execute(
        tx,
        query: str,
        mode: QueryMode,
        *,
        component: str = "unknown",
        db_name: str = "unknown",
        address: str = "unknown",
        stage: str = "",
    ):
        assert component == "ops_write_canary"
        assert db_name == "canary-db"
        assert address == "localhost:1729"
        assert stage
        if stage == "verify_tenant_exists":
            assert "select $t;" in query
            assert " get $t;" not in query
        calls.append((mode, stage))
        if mode == QueryMode.READ_ROWS:
            return [object()]
        return None

    monkeypatch.setattr("typedb_ops_spine.cli.write_canary_cli.execute", _strict_execute)
    monkeypatch.setattr("typedb_ops_spine.readiness.connect_with_retries", lambda *args, **kwargs: driver)
    monkeypatch.setattr("typedb_ops_spine.readiness.ensure_database", lambda *args, **kwargs: None)
    monkeypatch.setenv("OPS_DIAG_PATH", str(tmp_path / "typedb_diag.jsonl"))
    monkeypatch.setattr(
        "sys.argv",
        ["ops-write-canary", "--database", "canary-db", "--address", "localhost:1729"],
    )

    rc = write_canary_cli.main()

    assert rc == 0
    assert driver.closed is True
    assert calls == [
        (QueryMode.WRITE, "canary_write"),
        (QueryMode.READ_ROWS, "canary_verify_attempt_1"),
    ]


def test_write_canary_cli_scoped_path_uses_execute_signature_without_extra_kwargs(
    monkeypatch, tmp_path
):
    _install_fake_typedb(monkeypatch)
    driver = _FakeDriver()
    calls: list[tuple[QueryMode, str]] = []

    def _strict_execute(
        tx,
        query: str,
        mode: QueryMode,
        *,
        component: str = "unknown",
        db_name: str = "unknown",
        address: str = "unknown",
        stage: str = "",
    ):
        assert component == "ops_write_canary"
        assert db_name == "canary-db"
        assert address == "localhost:1729"
        assert stage
        calls.append((mode, stage))
        if mode == QueryMode.READ_ROWS:
            return [object()]
        return None

    monkeypatch.setattr("typedb_ops_spine.cli.write_canary_cli.execute", _strict_execute)
    monkeypatch.setattr("typedb_ops_spine.readiness.connect_with_retries", lambda *args, **kwargs: driver)
    monkeypatch.setattr("typedb_ops_spine.readiness.ensure_database", lambda *args, **kwargs: None)
    monkeypatch.setattr("time.sleep", lambda *_args, **_kwargs: None)
    monkeypatch.setenv("OPS_DIAG_PATH", str(tmp_path / "typedb_diag.jsonl"))
    monkeypatch.setattr(
        "sys.argv",
        [
            "ops-write-canary",
            "--database",
            "canary-db",
            "--address",
            "localhost:1729",
            "--tenant-id",
            "tenant-1",
            "--ownership-rel",
            "tenant-ownership",
        ],
    )

    rc = write_canary_cli.main()

    assert rc == 0
    assert driver.closed is True
    assert calls == [
        (QueryMode.READ_ROWS, "verify_tenant_exists"),
        (QueryMode.WRITE, "canary_write_tenant-ownership"),
        (QueryMode.READ_ROWS, "canary_verify_attempt_1"),
    ]
