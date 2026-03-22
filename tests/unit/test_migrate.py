from __future__ import annotations

import sys
import types
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from typedb_ops_spine import migrate


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

    with pytest.raises(RuntimeError, match="Failed WRITE transaction"):
        migrate.apply_migration(driver, "ops_db", migration_file, 2)

    schema_tx.commit.assert_called_once()
    write_tx.commit.assert_not_called()
