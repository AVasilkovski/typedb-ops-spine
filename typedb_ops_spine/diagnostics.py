"""
Structured diagnostics sink for TypeDB operations.

Emits JSONL records to ci_artifacts/typedb_diag.jsonl (configurable).
All emission is keyword-only to enforce field discipline at call sites.
"""

from __future__ import annotations

import datetime as _dt
import hashlib
import json
import os
from pathlib import Path
from typing import Any


def _diag_path() -> Path:
    """Resolve diagnostics output path from environment or default."""
    env_path = os.getenv("OPS_DIAG_PATH")
    if env_path:
        return Path(env_path)
    artifacts_dir = os.getenv("CI_ARTIFACTS_DIR", "ci_artifacts")
    return Path(artifacts_dir) / "typedb_diag.jsonl"


def _utc_now() -> str:
    return (
        _dt.datetime.now(_dt.timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def query_hash(query: str) -> str:
    """Stable SHA-256 hash of a query string."""
    return hashlib.sha256((query or "").encode("utf-8")).hexdigest()


def query_head(query: str, max_len: int = 120) -> str:
    """Truncated, single-line query preview."""
    return (query or "").strip().replace("\n", " ")[:max_len]


def emit_typedb_diag(
    *,
    component: str,
    db_name: str,
    tx_type: str,
    action: str,
    query: str,
    answer_kind: str,
    row_count: int = 0,
    doc_count: int = 0,
    error_code: str = "",
    error_message: str = "",
    **extra: Any,
) -> None:
    """Emit a single JSONL diagnostic record.

    All parameters are keyword-only to enforce call-site discipline.
    Extra kwargs are forwarded for forward-compatible field extension
    (e.g., mode, stage, expected_kind, barrier flags).
    """
    out = _diag_path()
    out.parent.mkdir(parents=True, exist_ok=True)

    payload: dict[str, Any] = {
        "timestamp_utc": _utc_now(),
        "component": component,
        "db_name": db_name,
        "tx_type": tx_type,
        "action": action,
        "query_hash": query_hash(query),
        "query_head": query_head(query),
        "answer_kind": answer_kind,
        "row_count": int(row_count or 0),
        "doc_count": int(doc_count or 0),
        "error_code": (error_code or "")[:64],
        "error_message": (error_message or "")[:180],
        # Backward-compatible key kept for older readers.
        "error_message_trunc": (error_message or "")[:180],
    }
    # Forward-compatible: merge any extra fields
    for k, v in extra.items():
        payload[k] = str(v) if v is not None else None

    with out.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, sort_keys=True) + "\n")
