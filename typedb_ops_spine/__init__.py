"""
typedb-ops-spine — Deterministic TypeDB 3.8 schema operations, diagnostics, and CI forensics.

LOCKED INVARIANT (EPI-16.9):
    Materialization barrier ordering: Rows → Docs → OK.
    Never call as_ok() when rows/docs exist or can be exhausted.
    Fallback must be guarded. No silent swallowing.
"""

__version__ = "0.1.0"

from typedb_ops_spine.diagnostics import emit_typedb_diag, query_hash
from typedb_ops_spine.exec import QueryMode, TypeDBAnswerKindError, execute
from typedb_ops_spine.readiness import connect_with_retries, ensure_database

__all__ = [
    "emit_typedb_diag",
    "query_hash",
    "QueryMode",
    "TypeDBAnswerKindError",
    "execute",
    "connect_with_retries",
    "ensure_database",
]
