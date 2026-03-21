"""
typedb-ops-spine — Deterministic TypeDB 3.8 schema operations, diagnostics, and CI forensics.

LOCKED INVARIANT (EPI-16.9):
    Materialization barrier ordering: Rows → Docs → OK.
    Never call as_ok() when rows/docs exist or can be exhausted.
    Fallback must be guarded. No silent swallowing.
"""

from typedb_ops_spine.diagnostics import emit_typedb_diag, query_hash
from typedb_ops_spine.exec import QueryMode, TypeDBAnswerKindError, execute
from typedb_ops_spine.readiness import (
    connect_with_retries,
    ensure_database,
    infer_tls_enabled,
    normalize_typedb_address,
    resolve_connection_address,
)
from typedb_ops_spine.typedb_diag import run_smoke_diagnostics

__version__ = "0.2.0"

__all__ = [
    "emit_typedb_diag",
    "query_hash",
    "QueryMode",
    "TypeDBAnswerKindError",
    "execute",
    "connect_with_retries",
    "ensure_database",
    "normalize_typedb_address",
    "resolve_connection_address",
    "infer_tls_enabled",
    "run_smoke_diagnostics",
]
