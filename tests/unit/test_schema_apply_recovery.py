from __future__ import annotations

import json
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from typedb_ops_spine import schema_apply
from typedb_ops_spine.schema_version import SchemaVersionReconcileRequired


class _TxContext:
    def __init__(self, tx: MagicMock):
        self._tx = tx

    def __enter__(self) -> MagicMock:
        return self._tx

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


class _Promise:
    def __init__(self, *, answer=None, error: Exception | None = None):
        self._answer = answer
        self._error = error

    def resolve(self):
        if self._error is not None:
            raise self._error
        return self._answer


class _OkAnswer:
    def __init__(self):
        self.as_ok_called = False

    def is_concept_rows(self) -> bool:
        return False

    def is_concept_documents(self) -> bool:
        return False

    def is_ok(self) -> bool:
        return True

    def as_ok(self) -> None:
        self.as_ok_called = True


@pytest.fixture
def fake_transaction_type(monkeypatch: pytest.MonkeyPatch):
    class _TransactionType:
        READ = "READ"
        WRITE = "WRITE"
        SCHEMA = "SCHEMA"

    fake_mod = types.SimpleNamespace(TransactionType=_TransactionType)
    monkeypatch.setitem(sys.modules, "typedb.driver", fake_mod)
    return _TransactionType


def test_apply_schema_uses_one_schema_transaction_for_multiple_files(
    tmp_path: Path,
    fake_transaction_type,
):
    schema_a = tmp_path / "001_a.tql"
    schema_b = tmp_path / "002_b.tql"
    schema_a.write_text("define entity tenant;", encoding="utf-8")
    schema_b.write_text("define attribute tenant-id, value string;", encoding="utf-8")

    tx = MagicMock()
    tx.query.return_value = _Promise(answer=object())
    driver = MagicMock()
    driver.transaction.return_value = _TxContext(tx)

    schema_apply.apply_schema(driver, "ops_db", [schema_a, schema_b])

    driver.transaction.assert_called_once_with("ops_db", fake_transaction_type.SCHEMA)
    assert tx.query.call_count == 2
    tx.commit.assert_called_once()


def test_apply_schema_failure_is_atomic_for_multi_file_bundle(
    tmp_path: Path,
    fake_transaction_type,
):
    schema_a = tmp_path / "001_a.tql"
    schema_b = tmp_path / "002_b.tql"
    schema_a.write_text("define entity tenant;", encoding="utf-8")
    schema_b.write_text("define attribute tenant-id, value string;", encoding="utf-8")

    tx = MagicMock()
    tx.query.side_effect = [
        _Promise(answer=object()),
        _Promise(error=RuntimeError("second file failed")),
    ]
    driver = MagicMock()
    driver.transaction.return_value = _TxContext(tx)

    with pytest.raises(
        RuntimeError,
        match="partial multi-file apply is not expected",
    ):
        schema_apply.apply_schema(driver, "ops_db", [schema_a, schema_b])

    tx.commit.assert_not_called()


def test_stamp_schema_version_head_raises_reconcile_required_and_emits_diag(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_transaction_type,
):
    monkeypatch.setenv("CI_ARTIFACTS_DIR", str(tmp_path))
    monkeypatch.setattr(schema_apply, "get_current_schema_version", lambda *_args: 0)

    write_tx = MagicMock()
    write_tx.query.return_value = _Promise(error=RuntimeError("write failed"))
    driver = MagicMock()
    driver.transaction.return_value = _TxContext(write_tx)

    with pytest.raises(SchemaVersionReconcileRequired) as exc_info:
        schema_apply.stamp_schema_version_head(
            driver,
            "ops_db",
            3,
            migrations_dir=tmp_path,
        )

    exc = exc_info.value
    assert exc.target_ordinal == 3
    assert "--reconcile-schema-version-head" in exc.recovery_command

    diag_path = tmp_path / "apply_schema_diagnostics.jsonl"
    records = [json.loads(line) for line in diag_path.read_text(encoding="utf-8").splitlines()]
    reconcile_records = [
        record for record in records if record["stage"] == "reconcile_required"
    ]
    assert len(reconcile_records) == 1
    assert reconcile_records[0]["target_ordinal"] == 3
    assert reconcile_records[0]["recovery_command"] == exc.recovery_command


def test_reconcile_schema_version_head_writes_only_version_record(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_transaction_type,
):
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    (migrations_dir / "001_bootstrap.tql").write_text("define entity tenant;", encoding="utf-8")
    (migrations_dir / "002_more.tql").write_text("define attribute tenant-id, value string;", encoding="utf-8")

    monkeypatch.setattr(schema_apply, "get_current_schema_version", lambda *_args: 0)

    write_tx = MagicMock()
    ok_answer = _OkAnswer()
    write_tx.query.return_value = _Promise(answer=ok_answer)
    driver = MagicMock()
    driver.transaction.return_value = _TxContext(write_tx)

    reconciled = schema_apply.reconcile_schema_version_head(
        driver,
        "ops_db",
        migrations_dir,
    )

    assert reconciled == 2
    driver.transaction.assert_called_once_with("ops_db", fake_transaction_type.WRITE)
    write_tx.commit.assert_called_once()
    assert ok_answer.as_ok_called is True
