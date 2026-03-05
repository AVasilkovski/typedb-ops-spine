"""
Unit tests for the diagnostics emitter.

Tests JSONL output format, keyword-only enforcement, field truncation,
timestamp UTC format, query_hash stability, and extra field forwarding.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from typedb_ops_spine.diagnostics import emit_typedb_diag, query_hash, query_head


class TestQueryHash:
    def test_stable_hash(self):
        """Same query should always produce the same hash."""
        q = "insert $t isa tenant, has tenant-id \"test\";"
        assert query_hash(q) == query_hash(q)

    def test_different_queries_different_hash(self):
        assert query_hash("query1") != query_hash("query2")

    def test_empty_query(self):
        h = query_hash("")
        assert len(h) == 64  # SHA-256 hex length

    def test_none_query(self):
        h = query_hash(None)
        assert len(h) == 64


class TestQueryHead:
    def test_truncates(self):
        long_q = "a" * 200
        assert len(query_head(long_q)) == 120

    def test_strips_newlines(self):
        q = "match\n$t isa tenant;\nselect $t;"
        head = query_head(q)
        assert "\n" not in head

    def test_strips_leading_whitespace(self):
        q = "  match $t isa tenant;  "
        head = query_head(q)
        assert head == "match $t isa tenant;"


class TestEmitTypedbDiag:
    def test_creates_jsonl_file(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        """emit_typedb_diag should create a JSONL file."""
        diag_path = tmp_path / "diag.jsonl"
        monkeypatch.setenv("OPS_DIAG_PATH", str(diag_path))

        emit_typedb_diag(
            component="test",
            db_name="test_db",
            tx_type="READ",
            action="test_action",
            query="match $t isa tenant;",
            answer_kind="rows",
        )

        assert diag_path.exists()
        lines = diag_path.read_text(encoding="utf-8").strip().split("\n")
        assert len(lines) == 1

        record = json.loads(lines[0])
        assert record["component"] == "test"
        assert record["db_name"] == "test_db"
        assert record["tx_type"] == "READ"
        assert record["action"] == "test_action"
        assert record["answer_kind"] == "rows"
        assert record["row_count"] == 0
        assert record["doc_count"] == 0

    def test_timestamp_utc_format(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        diag_path = tmp_path / "diag.jsonl"
        monkeypatch.setenv("OPS_DIAG_PATH", str(diag_path))

        emit_typedb_diag(
            component="test", db_name="db", tx_type="READ",
            action="a", query="q", answer_kind="ok",
        )

        record = json.loads(diag_path.read_text().strip())
        assert record["timestamp_utc"].endswith("Z")

    def test_query_hash_present(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        diag_path = tmp_path / "diag.jsonl"
        monkeypatch.setenv("OPS_DIAG_PATH", str(diag_path))

        q = "match $t isa tenant;"
        emit_typedb_diag(
            component="test", db_name="db", tx_type="READ",
            action="a", query=q, answer_kind="ok",
        )

        record = json.loads(diag_path.read_text().strip())
        assert record["query_hash"] == query_hash(q)

    def test_error_message_truncated(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        diag_path = tmp_path / "diag.jsonl"
        monkeypatch.setenv("OPS_DIAG_PATH", str(diag_path))

        long_err = "x" * 300
        emit_typedb_diag(
            component="test", db_name="db", tx_type="READ",
            action="a", query="q", answer_kind="ok",
            error_message=long_err,
        )

        record = json.loads(diag_path.read_text().strip())
        assert len(record["error_message_trunc"]) == 180

    def test_error_code_truncated(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        diag_path = tmp_path / "diag.jsonl"
        monkeypatch.setenv("OPS_DIAG_PATH", str(diag_path))

        long_code = "C" * 100
        emit_typedb_diag(
            component="test", db_name="db", tx_type="READ",
            action="a", query="q", answer_kind="ok",
            error_code=long_code,
        )

        record = json.loads(diag_path.read_text().strip())
        assert len(record["error_code"]) == 64

    def test_extra_fields_forwarded(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        diag_path = tmp_path / "diag.jsonl"
        monkeypatch.setenv("OPS_DIAG_PATH", str(diag_path))

        emit_typedb_diag(
            component="test", db_name="db", tx_type="READ",
            action="a", query="q", answer_kind="ok",
            stage="test_stage", is_ok=True, custom_field="hello",
        )

        record = json.loads(diag_path.read_text().strip())
        assert record["stage"] == "test_stage"
        assert record["is_ok"] == "True"
        assert record["custom_field"] == "hello"

    def test_keyword_only_enforced(self):
        """Positional arguments should raise TypeError."""
        with pytest.raises(TypeError):
            emit_typedb_diag("test", "db", "READ", "a", "q", "ok")  # type: ignore[misc]

    def test_multiple_records_appended(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        diag_path = tmp_path / "diag.jsonl"
        monkeypatch.setenv("OPS_DIAG_PATH", str(diag_path))

        for i in range(3):
            emit_typedb_diag(
                component=f"test_{i}", db_name="db", tx_type="READ",
                action="a", query="q", answer_kind="ok",
            )

        lines = diag_path.read_text().strip().split("\n")
        assert len(lines) == 3

    def test_ci_artifacts_dir_override(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        """CI_ARTIFACTS_DIR env should change default output path."""
        monkeypatch.setenv("CI_ARTIFACTS_DIR", str(tmp_path / "custom"))
        monkeypatch.delenv("OPS_DIAG_PATH", raising=False)

        emit_typedb_diag(
            component="test", db_name="db", tx_type="READ",
            action="a", query="q", answer_kind="ok",
        )

        expected = tmp_path / "custom" / "typedb_diag.jsonl"
        assert expected.exists()
