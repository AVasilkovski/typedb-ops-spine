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
    TypeDBConfigError,
    connect_with_retries,
    ensure_database,
    infer_tls_enabled,
    normalize_typedb_address,
    resolve_connection_address,
    resolve_connection_config,
    validate_connection_config,
)
from typedb_ops_spine.schema_health import (
    SchemaHealthExtraResult,
    SchemaHealthReport,
    run_health_checks,
)
from typedb_ops_spine.schema_version import SchemaVersionReconcileRequired
from typedb_ops_spine.typedb_diag import run_smoke_diagnostics

__version__ = "0.2.0"

__all__ = [
    "emit_typedb_diag",
    "query_hash",
    "QueryMode",
    "TypeDBAnswerKindError",
    "execute",
    "TypeDBConfigError",
    "connect_with_retries",
    "ensure_database",
    "normalize_typedb_address",
    "resolve_connection_address",
    "resolve_connection_config",
    "validate_connection_config",
    "infer_tls_enabled",
    "SchemaHealthExtraResult",
    "SchemaHealthReport",
    "run_health_checks",
    "SchemaVersionReconcileRequired",
    "run_smoke_diagnostics",
]
