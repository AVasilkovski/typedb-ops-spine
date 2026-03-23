"""
Shared schema_version helpers for TypeDB schema operations.

This module centralizes schema_version reads, WRITE answer materialization,
and the explicit reconcile-required error raised when SCHEMA work succeeded
but schema_version recording did not.
"""

from __future__ import annotations

import hashlib
import os
from datetime import datetime, timezone
from typing import Any, Callable

DiagEmitter = Callable[[dict[str, Any]], None]
MetaFactory = Callable[[str], dict[str, str]]


class SchemaVersionReconcileRequired(RuntimeError):  # noqa: N818
    """Raised when schema_version stamping failed after SCHEMA work succeeded."""

    def __init__(
        self,
        *,
        db: str,
        target_ordinal: int,
        source_kind: str,
        source_name: str,
        recovery_command: str,
        original_error: Exception,
    ) -> None:
        self.db = db
        self.target_ordinal = target_ordinal
        self.source_kind = source_kind
        self.source_name = source_name
        self.recovery_command = recovery_command
        self.original_error = original_error
        super().__init__(
            "Schema change may have succeeded, but recording schema_version "
            f"{target_ordinal} failed for {source_kind} '{source_name}' in "
            f"database '{db}'. Plain rerun is unsafe. Reconcile with: "
            f"{recovery_command}"
        )


def _default_query_meta(query: str) -> dict[str, str]:
    compact = " ".join(query.split())
    return {
        "query_sha256": hashlib.sha256(query.encode("utf-8")).hexdigest(),
        "query_preview": compact[:200],
    }


def _emit(
    emit_event: DiagEmitter | None,
    event: dict[str, Any],
) -> None:
    if emit_event is not None:
        emit_event(event)


def _materialize_write_answer(ans: Any) -> None:
    """Force TypeDB write answers through the Rows -> Docs -> OK barrier."""
    if hasattr(ans, "is_concept_rows") and ans.is_concept_rows():
        list(ans.as_concept_rows())
        return

    if hasattr(ans, "is_concept_documents") and ans.is_concept_documents():
        list(ans.as_concept_documents())
        return

    if hasattr(ans, "is_ok") and ans.is_ok() and hasattr(ans, "as_ok"):
        ans.as_ok()


def get_current_schema_version(
    driver: Any,
    db: str,
    *,
    emit_event: DiagEmitter | None = None,
    meta_factory: MetaFactory | None = None,
) -> int:
    """Read the highest stamped schema_version ordinal from the database."""
    from typedb.driver import TransactionType

    query = "match $v isa schema_version, has ordinal $o; select $o;"
    meta = (meta_factory or _default_query_meta)(query)

    try:
        with driver.transaction(db, TransactionType.READ) as tx:
            ans = tx.query(query).resolve()
            rows = list(ans.as_concept_rows())

            ordinals = []
            for row in rows:
                o_attr = row.get("o")
                if o_attr and o_attr.is_attribute():
                    ordinals.append(int(o_attr.as_attribute().get_value()))

            ordinal = max(ordinals) if ordinals else 0
            _emit(
                emit_event,
                {
                    "db": db,
                    "tx_type": "READ",
                    **meta,
                    "stage": "schema_version_read",
                    "status": "success",
                    "answer_kind": "concept_rows",
                    "row_count": len(rows),
                    "doc_count": 0,
                    "error_class": None,
                    "error_message": None,
                    "ordinal": ordinal,
                },
            )
            return ordinal
    except Exception as exc:
        _emit(
            emit_event,
            {
                "db": db,
                "tx_type": "READ",
                **meta,
                "stage": "schema_version_read",
                "status": "fail",
                "answer_kind": None,
                "row_count": 0,
                "doc_count": 0,
                "error_class": exc.__class__.__name__,
                "error_message": str(exc),
            },
        )
        return 0


def _write_schema_version_record(
    driver: Any,
    db: str,
    ordinal: int,
    *,
    source_kind: str,
    source_name: str,
    recovery_command: str = "",
    emit_event: DiagEmitter | None = None,
    meta_factory: MetaFactory | None = None,
) -> None:
    """Insert a schema_version record and emit record_schema_version diagnostics."""
    from typedb.driver import TransactionType

    git_commit = os.getenv("GITHUB_SHA", "unknown")
    applied_at = (
        datetime.now(timezone.utc)
        .replace(tzinfo=None)
        .isoformat(timespec="microseconds")
    )
    query = (
        "insert $v isa schema_version, "
        f'has ordinal {ordinal}, '
        f'has git-commit "{git_commit}", '
        f"has applied-at {applied_at};"
    )
    meta = (meta_factory or _default_query_meta)(query)

    try:
        with driver.transaction(db, TransactionType.WRITE) as tx:
            ans = tx.query(query).resolve()
            _materialize_write_answer(ans)
            tx.commit()

        _emit(
            emit_event,
            {
                "db": db,
                "tx_type": "WRITE",
                **meta,
                "stage": "record_schema_version",
                "status": "success",
                "answer_kind": "ok",
                "row_count": 0,
                "doc_count": 0,
                "error_class": None,
                "error_message": None,
                "target_ordinal": ordinal,
                "source_kind": source_kind,
                "source_name": source_name,
                "recovery_command": recovery_command or None,
            },
        )
    except Exception as exc:
        _emit(
            emit_event,
            {
                "db": db,
                "tx_type": "WRITE",
                **meta,
                "stage": "record_schema_version",
                "status": "fail",
                "answer_kind": None,
                "row_count": 0,
                "doc_count": 0,
                "error_class": exc.__class__.__name__,
                "error_message": str(exc),
                "target_ordinal": ordinal,
                "source_kind": source_kind,
                "source_name": source_name,
                "recovery_command": recovery_command or None,
            },
        )
        raise


def record_schema_version(
    driver: Any,
    db: str,
    ordinal: int,
    *,
    source_kind: str,
    source_name: str,
    recovery_command: str,
    emit_event: DiagEmitter | None = None,
    meta_factory: MetaFactory | None = None,
) -> None:
    """Record schema_version and raise an explicit reconcile-required error on failure."""
    try:
        _write_schema_version_record(
            driver,
            db,
            ordinal,
            source_kind=source_kind,
            source_name=source_name,
            recovery_command=recovery_command,
            emit_event=emit_event,
            meta_factory=meta_factory,
        )
    except Exception as exc:
        raise SchemaVersionReconcileRequired(
            db=db,
            target_ordinal=ordinal,
            source_kind=source_kind,
            source_name=source_name,
            recovery_command=recovery_command,
            original_error=exc,
        ) from exc
