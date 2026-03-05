"""
Deterministic TypeDB schema application.

Provides resolve_schema_files() (glob-killer path resolution) and apply_schema()
for SCHEMA transaction application with diagnostics emission.

v0.1.0: auto-migrate redeclarations is deferred to v0.2+.
"""

from __future__ import annotations

import glob as _glob
import hashlib
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def _diag_path() -> Path:
    """Resolve schema apply diagnostics output path."""
    artifacts_dir = os.getenv("CI_ARTIFACTS_DIR", "ci_artifacts")
    return Path(artifacts_dir) / "apply_schema_diagnostics.jsonl"


def _emit_diag(event: dict[str, Any]) -> None:
    out = _diag_path()
    out.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "component": "apply_schema",
        **event,
    }
    with out.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, sort_keys=True) + "\n")


def _query_meta(query: str) -> dict[str, str]:
    compact = " ".join(query.split())
    return {
        "query_sha256": hashlib.sha256(query.encode("utf-8")).hexdigest(),
        "query_preview": compact[:200],
    }


def resolve_schema_files(schema_args: list[str]) -> list[Path]:
    """Resolve schema file arguments (paths and/or globs) deterministically.

    Validates that all resolved paths are concrete files — no unresolved globs.

    Args:
        schema_args: List of file paths or glob patterns.

    Returns:
        Deduplicated, sorted list of resolved Path objects.

    Raises:
        ValueError: if any argument is empty or contains unresolved globs.
        FileNotFoundError: if no files match or a path doesn't exist.
    """
    if not schema_args:
        raise ValueError("No schema paths provided")

    cleaned: list[str] = []
    for raw in schema_args:
        s = (raw or "").strip()
        if not s:
            raise ValueError("Empty schema argument is invalid")
        if "***" in s:
            raise FileNotFoundError(f"Invalid schema glob pattern (triple-star): {s}")
        cleaned.append(s)

    resolved: list[Path] = []
    for item in cleaned:
        has_glob_chars = any(char in item for char in "*?[")
        matches = sorted(Path(p) for p in _glob.glob(item, recursive=True))
        file_matches = [m for m in matches if m.is_file()]
        if file_matches:
            resolved.extend(file_matches)
            continue

        if has_glob_chars:
            raise FileNotFoundError(
                f"Schema glob matched no files: {item}. "
                "Pass explicit schema path(s)."
            )

        path = Path(item)
        if path.is_file():
            resolved.append(path)
            continue

        raise FileNotFoundError(f"Schema file not found: {item}")

    # Deduplicate preserving sort order
    deduped: list[Path] = []
    seen: set[Path] = set()
    for path in sorted(resolved):
        if path not in seen:
            deduped.append(path)
            seen.add(path)

    if not deduped:
        raise FileNotFoundError("No schema files resolved from provided values.")

    # Invariant: resolved must be concrete file paths, not unresolved patterns
    for path in deduped:
        path_str = str(path)
        if any(ch in path_str for ch in ["*", "?", "[", "]"]):
            raise ValueError(
                f"BUG: unresolved glob in resolved schema files: {path_str}"
            )
        if not path.is_file():
            raise FileNotFoundError(f"Schema file not found: {path_str}")

    return deduped


def apply_schema(
    driver: Any,
    db: str,
    schema_paths: list[Path],
) -> None:
    """Apply schema file(s) to a TypeDB database using SCHEMA transactions.

    Each schema file is applied in a separate SCHEMA transaction.
    Diagnostics are emitted for both success and failure.

    Args:
        driver: Connected TypeDB driver.
        db: Database name.
        schema_paths: Resolved schema file paths.
    """
    from typedb.driver import TransactionType

    for schema_path in schema_paths:
        schema = schema_path.read_text(encoding="utf-8")
        try:
            with driver.transaction(db, TransactionType.SCHEMA) as tx:
                tx.query(schema).resolve()
                tx.commit()
            _emit_diag({
                "db": db,
                "tx_type": "SCHEMA",
                **_query_meta(schema),
                "stage": "apply_schema",
                "status": "success",
                "error_class": None,
                "error_message": None,
                "answer_kind": "ok",
                "row_count": 0,
                "doc_count": 0,
            })
            logger.info("Schema applied: %s", schema_path)
        except Exception as exc:
            _emit_diag({
                "db": db,
                "tx_type": "SCHEMA",
                **_query_meta(schema),
                "stage": "apply_schema",
                "status": "fail",
                "error_class": exc.__class__.__name__,
                "error_message": str(exc),
                "answer_kind": None,
                "row_count": 0,
                "doc_count": 0,
            })
            raise
