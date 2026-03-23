from __future__ import annotations

import json
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from typedb_ops_spine import migrate
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


class _RowsAnswer:
    def __init__(self):
        self.rows_materialized = False
        self.as_ok_called = False

    def is_concept_rows(self) -> bool:
        return True

    def is_concept_documents(self) -> bool:
        return False

    def is_ok(self) -> bool:
        return True

    def as_concept_rows(self):
        self.rows_materialized = True
        return [{"row": "value"}]

    def as_ok(self):
        self.as_ok_called = True
        raise AssertionError("as_ok should not be called when rows are present")


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


def test_get_migrations_parses_valid_ordinals(tmp_path: Path):
    (tmp_path / "001_bootstrap.tql").write_text(
        "define schema_version sub entity, owns ordinal, owns git-commit, owns applied-at;",
        encoding="utf-8",
    )
    (tmp_path / "002_next.tql").write_text("define entity tenant;", encoding="utf-8")

    migrations = migrate.get_migrations(tmp_path)

    assert len(migrations) == 2
    assert migrations[0][0] == 1
    assert migrations[1][0] == 2


def test_get_migrations_rejects_duplicate_ordinals(tmp_path: Path):
    (tmp_path / "001_bootstrap.tql").write_text(
        "define schema_version sub entity, owns ordinal, owns git-commit, owns applied-at;",
        encoding="utf-8",
    )
    (tmp_path / "001_other.tql").write_text("define entity tenant;", encoding="utf-8")

    with pytest.raises(ValueError, match="Duplicate migration ordinal detected: 1"):
        migrate.get_migrations(tmp_path)


def test_get_migrations_enforces_filename_format(tmp_path: Path):
    (tmp_path / "001_bootstrap.tql").write_text(
        "define schema_version sub entity, owns ordinal, owns git-commit, owns applied-at;",
        encoding="utf-8",
    )
    (tmp_path / "badname.tql").write_text("define entity tenant;", encoding="utf-8")

    with pytest.raises(
        ValueError,
        match="Invalid migration filename format: badname.tql. Must be NNN_name.tql",
    ):
        migrate.get_migrations(tmp_path)


def test_get_migrations_detects_gap_by_default(tmp_path: Path):
    (tmp_path / "001_bootstrap.tql").write_text(
        "define schema_version sub entity, owns ordinal, owns git-commit, owns applied-at;",
        encoding="utf-8",
    )
    (tmp_path / "003_skip.tql").write_text("define entity tenant;", encoding="utf-8")

    with pytest.raises(ValueError, match="Migration gap detected: expected 2, got 3"):
        migrate.get_migrations(tmp_path)


def test_get_migrations_allows_gap_with_explicit_flag(tmp_path: Path):
    (tmp_path / "001_bootstrap.tql").write_text(
        "define schema_version sub entity, owns ordinal, owns git-commit, owns applied-at;",
        encoding="utf-8",
    )
    (tmp_path / "003_skip.tql").write_text("define entity tenant;", encoding="utf-8")

    migrations = migrate.get_migrations(tmp_path, allow_gaps=True)

    assert len(migrations) == 2
    assert migrations[1][0] == 3


def test_get_migrations_requires_schema_version_keywords_in_001(tmp_path: Path):
    (tmp_path / "001_bootstrap.tql").write_text("define entity tenant;", encoding="utf-8")

    with pytest.raises(ValueError, match="Migration 001 must contain schema_version"):
        migrate.get_migrations(tmp_path)


def test_apply_migration_enforces_hygiene_before_driver_use(
    tmp_path: Path,
    fake_transaction_type,
):
    migration_file = tmp_path / "004_bad.tql"
    migration_file.write_text("insert $x isa tenant;", encoding="utf-8")

    driver = MagicMock()

    with pytest.raises(ValueError, match="Migration hygiene violation"):
        migrate.apply_migration(driver, "ops_db", migration_file, 4, dry_run=True)

    driver.transaction.assert_not_called()


def test_apply_migration_materializes_rows_before_write_commit(
    tmp_path: Path,
    fake_transaction_type,
):
    migration_file = tmp_path / "002_add_tenant.tql"
    migration_file.write_text("define entity tenant;", encoding="utf-8")

    schema_tx = MagicMock()
    schema_tx.query.return_value = _Promise(answer=object())

    write_tx = MagicMock()
    rows_answer = _RowsAnswer()
    write_tx.query.return_value = _Promise(answer=rows_answer)

    driver = MagicMock()
    driver.transaction.side_effect = [_TxContext(schema_tx), _TxContext(write_tx)]

    migrate.apply_migration(driver, "ops_db", migration_file, 2)

    assert rows_answer.rows_materialized is True
    assert rows_answer.as_ok_called is False
    schema_tx.commit.assert_called_once()
    write_tx.commit.assert_called_once()


def test_apply_migration_exposes_non_atomic_schema_then_stamp_split(
    tmp_path: Path,
    fake_transaction_type,
):
    migration_file = tmp_path / "002_add_tenant.tql"
    migration_file.write_text("define entity tenant;", encoding="utf-8")

    schema_tx = MagicMock()
    schema_tx.query.return_value = _Promise(answer=object())

    write_tx = MagicMock()
    write_tx.query.return_value = _Promise(error=RuntimeError("write failed"))

    driver = MagicMock()
    driver.transaction.side_effect = [_TxContext(schema_tx), _TxContext(write_tx)]

    with pytest.raises(SchemaVersionReconcileRequired) as exc_info:
        migrate.apply_migration(driver, "ops_db", migration_file, 2)

    schema_tx.commit.assert_called_once()
    write_tx.commit.assert_not_called()
    assert exc_info.value.target_ordinal == 2
    assert "--reconcile-ordinal 2" in exc_info.value.recovery_command


def test_apply_migration_emits_reconcile_required_diagnostic(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_transaction_type,
):
    monkeypatch.setenv("CI_ARTIFACTS_DIR", str(tmp_path))
    migration_file = tmp_path / "002_add_tenant.tql"
    migration_file.write_text("define entity tenant;", encoding="utf-8")

    schema_tx = MagicMock()
    schema_tx.query.return_value = _Promise(answer=object())

    write_tx = MagicMock()
    write_tx.query.return_value = _Promise(error=RuntimeError("write failed"))

    driver = MagicMock()
    driver.transaction.side_effect = [_TxContext(schema_tx), _TxContext(write_tx)]

    with pytest.raises(SchemaVersionReconcileRequired):
        migrate.apply_migration(driver, "ops_db", migration_file, 2)

    diag_path = tmp_path / "migrate_diagnostics.jsonl"
    records = [json.loads(line) for line in diag_path.read_text(encoding="utf-8").splitlines()]
    reconcile_records = [
        record for record in records if record["stage"] == "reconcile_required"
    ]
    assert len(reconcile_records) == 1
    assert reconcile_records[0]["target_ordinal"] == 2
    assert "--reconcile-ordinal 2" in reconcile_records[0]["recovery_command"]


def test_reconcile_migration_ordinal_writes_only_version_record(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_transaction_type,
):
    (tmp_path / "001_bootstrap.tql").write_text(
        "define schema_version sub entity, owns ordinal, owns git-commit, owns applied-at;",
        encoding="utf-8",
    )
    (tmp_path / "002_next.tql").write_text("define entity tenant;", encoding="utf-8")

    monkeypatch.setattr(migrate, "get_current_schema_version", lambda *_args: 0)

    write_tx = MagicMock()
    ok_answer = _OkAnswer()
    write_tx.query.return_value = _Promise(answer=ok_answer)
    driver = MagicMock()
    driver.transaction.return_value = _TxContext(write_tx)

    migrate.reconcile_migration_ordinal(driver, "ops_db", tmp_path, 2)

    driver.transaction.assert_called_once_with("ops_db", fake_transaction_type.WRITE)
    write_tx.commit.assert_called_once()
    assert ok_answer.as_ok_called is True


def test_rerun_after_reconcile_has_no_pending_migrations(monkeypatch, tmp_path: Path):
    (tmp_path / "001_bootstrap.tql").write_text(
        "define schema_version sub entity, owns ordinal, owns git-commit, owns applied-at;",
        encoding="utf-8",
    )
    state = {"ordinal": 0}

    monkeypatch.setattr(migrate, "get_current_schema_version", lambda *_args: state["ordinal"])

    def _fake_write(*_args, **_kwargs):
        state["ordinal"] = 1

    monkeypatch.setattr(migrate, "_write_schema_version_record", _fake_write)
    driver = MagicMock()

    migrate.reconcile_migration_ordinal(driver, "ops_db", tmp_path, 1)

    apply_calls: list[str] = []
    monkeypatch.setattr(
        migrate,
        "apply_migration",
        lambda *_args, **_kwargs: apply_calls.append("called"),
    )

    assert migrate.run_migrations(driver, "ops_db", tmp_path) == 0
    assert apply_calls == []
