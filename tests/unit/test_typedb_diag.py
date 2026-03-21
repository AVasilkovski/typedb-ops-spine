from __future__ import annotations

import json
import sys
import types
from pathlib import Path

from typedb_ops_spine.typedb_diag import run_smoke_diagnostics


class _FakeAnswer:
    def as_concept_rows(self):
        return [{"row": "value"}]


class _FakeTransaction:
    def __init__(self):
        self.queries: list[str] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def query(self, query: str):
        self.queries.append(query)
        return types.SimpleNamespace(resolve=lambda: _FakeAnswer())


class _FakeDriver:
    def __init__(self, db_names: list[str]):
        self.closed = False
        self.tx = _FakeTransaction()
        self.databases = types.SimpleNamespace(
            all=lambda: [types.SimpleNamespace(name=name) for name in db_names],
        )

    def transaction(self, *_args, **_kwargs):
        return self.tx

    def close(self):
        self.closed = True


def test_run_smoke_diagnostics_succeeds_and_infers_tls(
    monkeypatch,
    tmp_path: Path,
):
    diag_path = tmp_path / "diag.jsonl"
    monkeypatch.setenv("OPS_DIAG_PATH", str(diag_path))

    captured: dict[str, object] = {}
    driver = _FakeDriver(["ops_db"])

    class _Credentials:
        def __init__(self, *_args, **_kwargs):
            pass

    class _DriverOptions:
        def __init__(self, **kwargs):
            captured.update(kwargs)

    class _TransactionType:
        READ = "READ"

    class _TypeDB:
        @staticmethod
        def driver(address, *_args, **_kwargs):
            captured["address"] = address
            return driver

    fake_mod = types.SimpleNamespace(
        Credentials=_Credentials,
        DriverOptions=_DriverOptions,
        TransactionType=_TransactionType,
        TypeDB=_TypeDB,
    )
    monkeypatch.setitem(sys.modules, "typedb.driver", fake_mod)

    rc = run_smoke_diagnostics(
        "https://cloud.typedb.com:443",
        "ops_db",
        "admin",
        "password",
        tls=None,
        smoke_query="match $v isa schema_version; select $v;",
        retries=1,
        sleep_s=0,
    )

    assert rc == 0
    assert captured["address"] == "https://cloud.typedb.com:443"
    assert captured["is_tls_enabled"] is True
    assert driver.closed is True
    records = [json.loads(line) for line in diag_path.read_text(encoding="utf-8").splitlines()]
    assert any(record["action"] == "smoke_read" for record in records)


def test_run_smoke_diagnostics_fails_when_required_db_missing(monkeypatch, tmp_path: Path):
    diag_path = tmp_path / "diag.jsonl"
    monkeypatch.setenv("OPS_DIAG_PATH", str(diag_path))

    driver = _FakeDriver(["other_db"])

    class _Credentials:
        def __init__(self, *_args, **_kwargs):
            pass

    class _DriverOptions:
        def __init__(self, **_kwargs):
            pass

    class _TransactionType:
        READ = "READ"

    class _TypeDB:
        @staticmethod
        def driver(*_args, **_kwargs):
            return driver

    fake_mod = types.SimpleNamespace(
        Credentials=_Credentials,
        DriverOptions=_DriverOptions,
        TransactionType=_TransactionType,
        TypeDB=_TypeDB,
    )
    monkeypatch.setitem(sys.modules, "typedb.driver", fake_mod)

    rc = run_smoke_diagnostics(
        "localhost:1729",
        "ops_db",
        "admin",
        "password",
        require_db=True,
        retries=1,
        sleep_s=0,
    )

    assert rc == 1
    assert driver.closed is True
