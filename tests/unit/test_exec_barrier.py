"""
Unit tests for the execution barrier layer.

Tests the locked Rows → Docs → OK materialization invariant,
including the _ConceptRowIterator → OkQueryAnswer invalid cast protection,
barrier ordering, diagnostics emission on every failure path,
and safe error code extraction.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from typedb_ops_spine.exec import (
    QueryMode,
    TypeDBAnswerKindError,
    _detect_kind,
    _get_error_code,
    execute,
)

# ---------------------------------------------------------------------------
# Mock answer objects that simulate TypeDB driver behavior
# ---------------------------------------------------------------------------

class MockRowsAnswer:
    """Simulates an answer that has concept rows (is_concept_rows=True)."""

    def is_concept_rows(self) -> bool:
        return True

    def is_concept_documents(self) -> bool:
        return False

    def is_ok(self) -> bool:
        # TypeDB 3.x quirk: is_ok can be truthy even with rows present
        return True

    def as_concept_rows(self):
        return [{"col": "val1"}, {"col": "val2"}]

    def as_ok(self):
        # This would throw the invalid cast if called on a rows answer
        raise TypeError(
            "Invalid query answer conversion from '_ConceptRowIterator' "
            "to 'OkQueryAnswer'"
        )


class MockOkAnswer:
    """Simulates a pure OK answer (no rows, no docs)."""

    def is_concept_rows(self) -> bool:
        return False

    def is_concept_documents(self) -> bool:
        return False

    def is_ok(self) -> bool:
        return True

    def as_ok(self):
        return None  # success

    def as_concept_rows(self):
        raise TypeError("Not concept rows")


class MockDocsAnswer:
    """Simulates an answer with concept documents."""

    def is_concept_rows(self) -> bool:
        return False

    def is_concept_documents(self) -> bool:
        return True

    def is_ok(self) -> bool:
        return False

    def as_concept_documents(self):
        return [{"doc": "value"}]


class MockUnknownAnswer:
    """Simulates an unknown answer type."""
    pass


def _make_tx(ans: Any) -> MagicMock:
    """Create a mock transaction that returns the given answer."""
    tx = MagicMock()
    promise = MagicMock()
    promise.resolve.return_value = ans
    tx.query.return_value = promise
    return tx


# ---------------------------------------------------------------------------
# Tests: _detect_kind
# ---------------------------------------------------------------------------

class TestDetectKind:
    def test_rows_wins_over_ok(self):
        """rows must be detected even when is_ok is also True."""
        ans = MockRowsAnswer()
        assert _detect_kind(ans) == "rows"

    def test_docs_detected(self):
        assert _detect_kind(MockDocsAnswer()) == "docs"

    def test_ok_detected(self):
        assert _detect_kind(MockOkAnswer()) == "ok"

    def test_unknown(self):
        assert _detect_kind(MockUnknownAnswer()) == "unknown"


# ---------------------------------------------------------------------------
# Tests: execute barrier — the critical safety tests
# ---------------------------------------------------------------------------

class TestExecuteBarrier:
    """Tests the locked materialization barrier invariant."""

    def test_rows_answer_never_calls_as_ok(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ):
        """CRITICAL: When rows are present, as_ok() must NOT be called.

        This is the exact bug that caused TypeDB 3.8 ghost writes:
        calling as_ok() on a _ConceptRowIterator throws an invalid cast.
        """
        monkeypatch.setenv("OPS_DIAG_PATH", str(tmp_path / "diag.jsonl"))

        ans = MockRowsAnswer()
        tx = _make_tx(ans)

        # Spy on as_ok to verify it's never called
        original_as_ok = ans.as_ok
        as_ok_called = False

        def tracking_as_ok():
            nonlocal as_ok_called
            as_ok_called = True
            return original_as_ok()

        ans.as_ok = tracking_as_ok

        # execute in WRITE mode — should materialize rows, skip as_ok
        result = execute(
            tx, "insert $t isa tenant;", QueryMode.WRITE,
            component="test", db_name="test_db",
        )

        # Rows should be returned
        assert result is not None
        assert len(result) == 2

        # as_ok must NEVER have been called
        assert not as_ok_called, (
            "BARRIER VIOLATION: as_ok() was called when rows were present. "
            "This would cause the _ConceptRowIterator → OkQueryAnswer invalid cast."
        )

    def test_ok_answer_calls_as_ok_in_write_mode(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ):
        """When only OK is present (no rows/docs), as_ok() should be called."""
        monkeypatch.setenv("OPS_DIAG_PATH", str(tmp_path / "diag.jsonl"))

        ans = MockOkAnswer()
        tx = _make_tx(ans)

        result = execute(
            tx, "insert $t isa tenant;", QueryMode.WRITE_OK,
            component="test", db_name="test_db",
        )

        assert result is None  # OK mode returns None

    def test_read_rows_returns_rows(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ):
        monkeypatch.setenv("OPS_DIAG_PATH", str(tmp_path / "diag.jsonl"))

        ans = MockRowsAnswer()
        tx = _make_tx(ans)

        result = execute(
            tx, "match $t isa tenant;", QueryMode.READ_ROWS,
            component="test", db_name="test_db",
        )

        assert result is not None
        assert len(result) == 2

    def test_read_rows_raises_on_wrong_kind(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ):
        monkeypatch.setenv("OPS_DIAG_PATH", str(tmp_path / "diag.jsonl"))

        ans = MockOkAnswer()
        tx = _make_tx(ans)

        with pytest.raises(TypeDBAnswerKindError) as exc_info:
            execute(
                tx, "match $t isa tenant;", QueryMode.READ_ROWS,
                component="test", db_name="test_db",
            )

        assert exc_info.value.expected == QueryMode.READ_ROWS
        assert exc_info.value.actual == "ok"

    def test_read_docs_returns_docs(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ):
        monkeypatch.setenv("OPS_DIAG_PATH", str(tmp_path / "diag.jsonl"))

        ans = MockDocsAnswer()
        tx = _make_tx(ans)

        result = execute(
            tx, "match $t isa tenant; fetch {};", QueryMode.READ_DOCS,
            component="test", db_name="test_db",
        )

        assert result is not None
        assert len(result) == 1

    def test_empty_query_raises_valueerror(self):
        tx = MagicMock()
        with pytest.raises(ValueError, match="empty query"):
            execute(tx, "", QueryMode.READ_ROWS)

        with pytest.raises(ValueError, match="empty query"):
            execute(tx, "   ", QueryMode.READ_ROWS)

    def test_diagnostics_emitted_on_execute(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ):
        """Every successful execute must emit a diagnostic record."""
        diag_path = tmp_path / "diag.jsonl"
        monkeypatch.setenv("OPS_DIAG_PATH", str(diag_path))

        ans = MockRowsAnswer()
        tx = _make_tx(ans)

        execute(
            tx, "match $t isa tenant;", QueryMode.READ_ROWS,
            component="test_comp", db_name="test_db", stage="test_stage",
        )

        assert diag_path.exists()
        records = [
            json.loads(line)
            for line in diag_path.read_text().strip().split("\n")
        ]
        # Should have at least one "execute" action record
        execute_records = [r for r in records if r.get("action") == "execute"]
        assert len(execute_records) >= 1

        rec = execute_records[0]
        assert rec["component"] == "test_comp"
        assert rec["db_name"] == "test_db"
        assert rec["answer_kind"] == "rows"
        assert rec["row_count"] == 2

    def test_driver_exception_wrapped_in_assertion_error(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ):
        """Low-level driver exceptions should be wrapped in AssertionError."""
        monkeypatch.setenv("OPS_DIAG_PATH", str(tmp_path / "diag.jsonl"))

        tx = MagicMock()
        tx.query.side_effect = RuntimeError("Connection lost")

        with pytest.raises(AssertionError, match="TypeDB Execution Failure"):
            execute(
                tx, "match $t isa tenant;", QueryMode.READ_ROWS,
                component="test", db_name="test_db",
            )

    def test_schema_ok_mode(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ):
        monkeypatch.setenv("OPS_DIAG_PATH", str(tmp_path / "diag.jsonl"))

        ans = MockOkAnswer()
        tx = _make_tx(ans)

        result = execute(
            tx, "define entity tenant;", QueryMode.SCHEMA_OK,
            component="test", db_name="test_db",
        )

        assert result is None


# ---------------------------------------------------------------------------
# Tests: _get_error_code
# ---------------------------------------------------------------------------

class TestGetErrorCode:
    def test_standard_exception(self):
        assert _get_error_code(ValueError("test")) == "ValueError"

    def test_runtime_error(self):
        assert _get_error_code(RuntimeError("oops")) == "RuntimeError"

    def test_type_error(self):
        assert _get_error_code(TypeError("bad")) == "TypeError"
