"""
Centralized TypeDB execution layer — single source of truth for answer-kind handling.

LOCKED INVARIANT (EPI-16.9):
    All TypeDB queries MUST declare expected answer-kind via QueryMode and be
    validated through execute(). String heuristic detection is prohibited.

    Materialization barrier ordering: Rows → Docs → OK.
    Never call as_ok() when rows/docs exist or can be exhausted.
    Fallback must be guarded and must emit diagnostics on every failure.
    No silent swallowing (no ``except: pass``).
"""

from __future__ import annotations

import enum
import logging
from typing import Any, Optional

from typedb_ops_spine.diagnostics import emit_typedb_diag

logger = logging.getLogger(__name__)


class QueryMode(enum.Enum):
    """Declares the expected answer kind for a TypeDB query."""

    READ_ROWS = "read_rows"       # match/select → concept rows
    READ_DOCS = "read_docs"       # fetch → concept documents
    WRITE = "write"               # insert/match-insert → ok OR rows OR docs (permissive)
    WRITE_ROWS = "write_rows"     # insert with select → strictly concept rows
    WRITE_DOCS = "write_docs"     # insert with fetch → strictly concept documents
    WRITE_OK = "write_ok"         # prefer OK, accept rows/docs, barrier correctly
    SCHEMA_OK = "schema_ok"       # define/undefine/redefine → OK


class TypeDBAnswerKindError(Exception):
    """Raised when the answer kind from TypeDB does not match the expected QueryMode."""

    def __init__(self, expected: QueryMode, actual: str, query_head: str):
        self.expected = expected
        self.actual = actual
        self.query_head = query_head
        super().__init__(
            f"Answer-kind mismatch: expected {expected.value}, got {actual}. "
            f"Query: {query_head}"
        )


def _detect_kind(ans: Any) -> str:
    """Determine answer kind using driver introspection only. Never string heuristics.

    IMPORTANT: rows/docs MUST be checked before ok. TypeDB 3.x can return
    answers where is_ok() is truthy even when concept rows are present.
    """
    # Prefer explicit introspection — rows/docs win over ok
    if hasattr(ans, "is_concept_rows") and callable(ans.is_concept_rows) and ans.is_concept_rows():
        return "rows"
    if (
        hasattr(ans, "is_concept_documents")
        and callable(ans.is_concept_documents)
        and ans.is_concept_documents()
    ):
        return "docs"
    if hasattr(ans, "is_ok") and callable(ans.is_ok) and ans.is_ok():
        return "ok"

    # Fallback: capability presence (rows/docs must still win over ok)
    if hasattr(ans, "as_concept_rows"):
        return "rows"
    if hasattr(ans, "as_concept_documents"):
        return "docs"
    if hasattr(ans, "as_ok"):
        return "ok"

    return "unknown"


# Strict modes: exactly one expected kind
_MODE_EXPECTED_KIND = {
    QueryMode.READ_ROWS: "rows",
    QueryMode.READ_DOCS: "docs",
    QueryMode.WRITE_ROWS: "rows",
    QueryMode.WRITE_DOCS: "docs",
    QueryMode.WRITE_OK: "ok",
    QueryMode.SCHEMA_OK: "ok",
}

# Permissive modes: accept any answer kind
_PERMISSIVE_MODES = {
    QueryMode.WRITE,   # insert without projection: ok, rows, or docs all valid
}

# All write-ish modes that require barrier materialization
_WRITE_MODES = {
    QueryMode.WRITE,
    QueryMode.WRITE_ROWS,
    QueryMode.WRITE_DOCS,
    QueryMode.WRITE_OK,
    QueryMode.SCHEMA_OK,
}


def execute(
    tx: Any,
    query: str,
    mode: QueryMode,
    *,
    component: str = "unknown",
    db_name: str = "unknown",
    address: str = "unknown",
    stage: str = "",
) -> Optional[list[Any]]:
    """Execute a TypeDB query with deterministic answer-kind validation.

    Returns:
        - For READ_ROWS/WRITE_ROWS modes: list of ConceptRow
        - For READ_DOCS/WRITE_DOCS modes: list of ConceptDocument
        - For WRITE/WRITE_OK/SCHEMA_OK modes: None (or rows/docs if returned)

    Raises:
        TypeDBAnswerKindError: if the answer kind does not match the expected mode.
        ValueError: if the query string is empty.
        AssertionError: wraps low-level driver exceptions for clean propagation.
    """
    # 1. Prepare diagnostics data
    qs = query.strip()
    if not qs:
        raise ValueError("empty query")

    # Determine tx_type early — avoid tx.type() in error handlers
    is_write_mode = mode in _WRITE_MODES
    tx_type = "WRITE" if is_write_mode else "READ"
    if mode == QueryMode.SCHEMA_OK:
        tx_type = "SCHEMA"

    # 2. Execute and resolve promise
    try:
        ans = tx.query(qs).resolve()

        # 3) Materialize deterministically (LOCKED: execution barrier)
        rows: list[Any] = []
        docs: list[Any] = []
        as_ok_attempted = False
        as_ok_succeeded = False

        # Determine answer flags robustly
        is_rows = (
            bool(ans.is_concept_rows()) if hasattr(ans, "is_concept_rows") else False
        )
        is_docs = (
            bool(ans.is_concept_documents())
            if hasattr(ans, "is_concept_documents")
            else False
        )
        is_ok = bool(ans.is_ok()) if hasattr(ans, "is_ok") else False

        # CRITICAL (EPI-16.9): Materialize in strict order: rows → docs → ok.
        # Never call as_ok() if the answer actually contains rows/docs — that
        # triggers the "_ConceptRowIterator → OkQueryAnswer" invalid cast.

        # Step 1: Try rows first (highest priority for write modes)
        if is_rows and hasattr(ans, "as_concept_rows"):
            try:
                rows = list(ans.as_concept_rows())
            except Exception as e:
                emit_typedb_diag(
                    component=component, db_name=db_name, address=address,
                    tx_type=tx_type, action="barrier_rows_failure", query=qs,
                    answer_kind="rows_error", error_code=_get_error_code(e),
                    error_message=str(e), stage=stage, query_mode=mode.value,
                )
                raise

        # Step 2: Try docs
        if is_docs and hasattr(ans, "as_concept_documents"):
            try:
                docs = list(ans.as_concept_documents())
            except Exception as e:
                emit_typedb_diag(
                    component=component, db_name=db_name, address=address,
                    tx_type=tx_type, action="barrier_docs_failure", query=qs,
                    answer_kind="docs_error", error_code=_get_error_code(e),
                    error_message=str(e), stage=stage, query_mode=mode.value,
                )
                raise

        # Step 3: Only call as_ok() if is_ok AND we didn't already materialize rows/docs
        if is_write_mode and not rows and not docs:
            if is_ok and hasattr(ans, "as_ok"):
                as_ok_attempted = True
                try:
                    ans.as_ok()
                    as_ok_succeeded = True
                except Exception as e:
                    emit_typedb_diag(
                        component=component, db_name=db_name, address=address,
                        tx_type=tx_type, action="barrier_failure", query=qs,
                        answer_kind="exception", error_code=_get_error_code(e),
                        error_message=str(e), stage=stage, query_mode=mode.value,
                        as_ok_attempted=True, as_ok_succeeded=False,
                    )
                    raise

        # Step 4: Fallback — if write mode and nothing materialized, attempt
        # best-effort exhaustion but never silently swallow errors
        if is_write_mode and not as_ok_attempted and not rows and not docs:
            # Try row exhaustion as last resort
            if hasattr(ans, "as_concept_rows"):
                try:
                    rows = list(ans.as_concept_rows())
                    emit_typedb_diag(
                        component=component, db_name=db_name, address=address,
                        tx_type=tx_type, action="barrier_fallback_rows", query=qs,
                        answer_kind="fallback_rows", row_count=len(rows),
                        stage=stage, query_mode=mode.value,
                    )
                except Exception as e_rows:
                    emit_typedb_diag(
                        component=component, db_name=db_name, address=address,
                        tx_type=tx_type, action="barrier_fallback_rows_failure",
                        query=qs, answer_kind="fallback_rows_error",
                        error_code=_get_error_code(e_rows),
                        error_message=str(e_rows),
                        stage=stage, query_mode=mode.value,
                        is_ok=is_ok, is_rows=is_rows, is_docs=is_docs,
                    )
                    # Fall through to docs/ok attempt

            if not rows:
                # Try docs exhaustion as second resort
                if hasattr(ans, "as_concept_documents"):
                    try:
                        docs = list(ans.as_concept_documents())
                        emit_typedb_diag(
                            component=component, db_name=db_name, address=address,
                            tx_type=tx_type, action="barrier_fallback_docs", query=qs,
                            answer_kind="fallback_docs", doc_count=len(docs),
                            stage=stage, query_mode=mode.value,
                        )
                    except Exception as e_docs:
                        emit_typedb_diag(
                            component=component, db_name=db_name, address=address,
                            tx_type=tx_type, action="barrier_fallback_docs_failure",
                            query=qs, answer_kind="fallback_docs_error",
                            error_code=_get_error_code(e_docs),
                            error_message=str(e_docs),
                            stage=stage, query_mode=mode.value,
                            is_ok=is_ok, is_rows=is_rows, is_docs=is_docs,
                        )
                        # Fall through to ok attempt

            if not rows and not docs:
                if hasattr(ans, "is_ok") and callable(ans.is_ok) and ans.is_ok():
                    as_ok_attempted = True
                    try:
                        ans.as_ok()
                        as_ok_succeeded = True
                    except Exception as e_ok:
                        emit_typedb_diag(
                            component=component, db_name=db_name, address=address,
                            tx_type=tx_type, action="barrier_fallback_ok_failure",
                            query=qs, answer_kind="fallback_ok_error",
                            error_code=_get_error_code(e_ok),
                            error_message=str(e_ok), stage=stage,
                            query_mode=mode.value, as_ok_attempted=True,
                            as_ok_succeeded=False,
                        )
                        raise

            # If still nothing materialized, emit diagnostic (fail-closed for forensics)
            if not rows and not docs and not as_ok_succeeded:
                emit_typedb_diag(
                    component=component, db_name=db_name, address=address,
                    tx_type=tx_type, action="barrier_unknown_kind", query=qs,
                    answer_kind="unknown", stage=stage, query_mode=mode.value,
                    is_ok=is_ok, is_rows=is_rows, is_docs=is_docs,
                )

        # 4) Emit diagnostics with forensic flags
        emit_typedb_diag(
            component=component,
            db_name=db_name,
            address=address,
            tx_type=tx_type,
            action="execute",
            query=qs,
            answer_kind=(
                "rows" if is_rows
                else "docs" if is_docs
                else "ok" if (is_ok or as_ok_succeeded)
                else "unknown"
            ),
            row_count=len(rows),
            doc_count=len(docs),
            stage=stage,
            query_mode=mode.value,
            is_ok=is_ok,
            is_rows=is_rows,
            is_docs=is_docs,
            as_ok_attempted=as_ok_attempted,
            as_ok_succeeded=as_ok_succeeded,
        )

        # 5) Return by mode
        if mode == QueryMode.READ_ROWS:
            if not is_rows:
                raise TypeDBAnswerKindError(
                    mode, "ok" if is_ok else "docs" if is_docs else "unknown", qs
                )
            return rows

        if mode == QueryMode.READ_DOCS:
            if not is_docs:
                raise TypeDBAnswerKindError(
                    mode, "ok" if is_ok else "rows" if is_rows else "unknown", qs
                )
            return docs

        if mode == QueryMode.WRITE_ROWS:
            if not is_rows:
                raise TypeDBAnswerKindError(
                    mode, "ok" if is_ok else "docs" if is_docs else "unknown", qs
                )
            return rows

        if mode == QueryMode.WRITE_DOCS:
            if not is_docs:
                raise TypeDBAnswerKindError(
                    mode, "ok" if is_ok else "rows" if is_rows else "unknown", qs
                )
            return docs

        # Permissive write modes: return any materialized rows/docs, else None
        if mode in (QueryMode.WRITE, QueryMode.WRITE_OK, QueryMode.SCHEMA_OK):
            if rows:
                return rows
            if docs:
                return docs
            return None

    except Exception as e:
        if isinstance(e, (ValueError, TypeDBAnswerKindError, AssertionError)):
            raise

        # Avoid any tx.xxx() calls here that might fail if tx is closed/broken
        err_code = _get_error_code(e)
        emit_typedb_diag(
            component=component,
            db_name=db_name,
            address=address,
            tx_type=tx_type,
            action="query_exception",
            query=qs,
            answer_kind="exception",
            error_code=err_code,
            error_message=str(e),
            stage=stage,
            query_mode=mode.value,
        )
        raise AssertionError(f"TypeDB Execution Failure: {e}") from e

    return None  # unreachable but satisfies type checker


def _get_error_code(e: Exception) -> str:
    """Extract a stable error code from a TypeDB driver exception."""
    try:
        code = getattr(getattr(e, "class", None), "name", None)
        if code:
            return str(code)
        return type(e).__name__
    except Exception:
        return type(e).__name__
