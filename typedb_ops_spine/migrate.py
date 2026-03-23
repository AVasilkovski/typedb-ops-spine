"""
Deterministic linear schema migrations for TypeDB.

Ordinal-based migrations with gap detection, hygiene checks, and
schema_version entity tracking. Emits diagnostics for every operation.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from typedb_ops_spine.schema_version import (
    SchemaVersionReconcileRequired,
    _write_schema_version_record,
    record_schema_version,
)
from typedb_ops_spine.schema_version import (
    get_current_schema_version as _get_current_schema_version,
)

logger = logging.getLogger(__name__)


def _diag_path() -> Path:
    artifacts_dir = os.getenv("CI_ARTIFACTS_DIR", "ci_artifacts")
    return Path(artifacts_dir) / "migrate_diagnostics.jsonl"


def _emit_diag(event: dict[str, Any]) -> None:
    out = _diag_path()
    out.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "component": "migrate",
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


def get_current_schema_version(driver: Any, db: str) -> int:
    """Read the current schema version ordinal from the database."""
    ordinal = _get_current_schema_version(
        driver,
        db,
        emit_event=_emit_diag,
        meta_factory=_query_meta,
    )
    if ordinal == 0:
        logger.info("Current schema version ordinal is 0 (fresh DB or unreadable state)")
    return ordinal


def get_migrations(
    migrations_dir: Path,
    *,
    allow_gaps: bool = False,
) -> list[tuple[int, Path]]:
    """Discover and validate migration files in ordinal order.

    Migration files must be named ``NNN_description.tql`` where NNN is a
    positive integer ordinal. Gap detection is enforced by default.

    Raises:
        ValueError: on invalid filenames, duplicate ordinals, or gaps.
    """
    if not migrations_dir.is_dir():
        return []

    files = list(migrations_dir.glob("*.tql"))
    valid_files: list[tuple[int, Path]] = []
    seen_ordinals: set[int] = set()

    for f in files:
        name = f.name
        parts = name.split("_")
        if not parts or not parts[0].isdigit():
            raise ValueError(
                f"Invalid migration filename format: {name}. Must be NNN_name.tql"
            )

        ordinal = int(parts[0])
        if ordinal < 1:
            raise ValueError(
                f"Invalid migration ordinal: {ordinal} in {name}. Must be >= 1"
            )

        if ordinal in seen_ordinals:
            raise ValueError(f"Duplicate migration ordinal detected: {ordinal}")

        seen_ordinals.add(ordinal)
        valid_files.append((ordinal, f))

    valid_files.sort(key=lambda x: x[0])

    # Gap detection
    if valid_files and not allow_gaps:
        expected = 1
        for ord_val, _ in valid_files:
            if ord_val != expected:
                raise ValueError(
                    f"Migration gap detected: expected {expected}, got {ord_val}"
                )
            expected += 1

    # Check 001_* contains schema_version definitions
    if valid_files:
        first_ord, first_path = valid_files[0]
        if first_ord == 1:
            content = first_path.read_text(encoding="utf-8")
            required_kw = ["schema_version", "ordinal", "git-commit", "applied-at"]
            missing = [kw for kw in required_kw if kw not in content]
            if missing:
                raise ValueError(
                    f"Migration 001 must contain schema_version definitions with "
                    f"{', '.join(required_kw)}. Missing: {', '.join(missing)}"
                )

    return valid_files


def _reconcile_ordinal_command(db: str, migrations_dir: Path, ordinal: int) -> str:
    return (
        "ops-migrate "
        f'--database "{db}" '
        f'--migrations-dir "{migrations_dir}" '
        f"--reconcile-ordinal {ordinal}"
    )


def _emit_reconcile_required(
    *,
    db: str,
    exc: SchemaVersionReconcileRequired,
) -> None:
    original_error = exc.original_error
    _emit_diag({
        "db": db,
        "tx_type": "WRITE",
        "stage": "reconcile_required",
        "status": "fail",
        "answer_kind": None,
        "row_count": 0,
        "doc_count": 0,
        "target_ordinal": exc.target_ordinal,
        "source_kind": exc.source_kind,
        "source_name": exc.source_name,
        "recovery_command": exc.recovery_command,
        "error_class": original_error.__class__.__name__,
        "error_message": str(original_error),
    })


def apply_migration(
    driver: Any,
    db: str,
    migration_file: Path,
    next_ordinal: int,
    *,
    dry_run: bool = False,
) -> None:
    """Apply a single migration file: SCHEMA transaction + version record.

    Migration files must start with define/undefine/redefine.
    The schema change and schema_version insert are intentionally separate
    transactions, so crash recovery still requires reconciliation if the
    process dies between them.

    Args:
        driver: Connected TypeDB driver.
        db: Database name.
        migration_file: Path to the .tql migration file.
        next_ordinal: The ordinal number for this migration.
        dry_run: If True, log the planned action without executing.
    """
    from typedb.driver import TransactionType

    schema = migration_file.read_text(encoding="utf-8").strip()

    # Migration hygiene: must start with define/undefine/redefine
    lines = [line.strip() for line in schema.splitlines() if line.strip() and not line.strip().startswith("#")]
    if not lines or not any(lines[0].lower().startswith(kw) for kw in ["define", "undefine", "redefine"]):
        preview = schema[:50].replace("\n", " ")
        raise ValueError(
            f"Migration hygiene violation: {migration_file.name} must start with "
            f"define/undefine/redefine. Found: {preview}..."
        )

    file_hash = hashlib.sha256(schema.encode("utf-8")).hexdigest()[:12]
    logger.info("Applying %s (sha256: %s)", migration_file.name, file_hash)

    if dry_run:
        logger.info("Dry-run: skipping %s", migration_file.name)
        return

    # Step 1: Apply schema change
    try:
        with driver.transaction(db, TransactionType.SCHEMA) as tx:
            tx.query(schema).resolve()
            tx.commit()
            _emit_diag({
                "db": db,
                "tx_type": "SCHEMA",
                **_query_meta(schema),
                "stage": "apply_migration_schema",
                "status": "success",
                "error_class": None,
                "error_message": None,
                "answer_kind": "ok",
                "row_count": 0,
                "doc_count": 0,
                "migration": migration_file.name,
            })
    except Exception as e:
        _emit_diag({
            "db": db,
            "tx_type": "SCHEMA",
            **_query_meta(schema),
            "stage": "apply_migration_schema",
            "status": "fail",
            "error_class": e.__class__.__name__,
            "error_message": str(e),
            "answer_kind": None,
            "row_count": 0,
            "doc_count": 0,
            "migration": migration_file.name,
        })
        raise RuntimeError(
            f"Failed SCHEMA transaction for {migration_file.name} "
            f"(Ordinal: {next_ordinal}): {e}"
        ) from e

    # Step 2: Record schema version
    try:
        record_schema_version(
            driver,
            db,
            next_ordinal,
            source_kind="migration",
            source_name=migration_file.name,
            recovery_command=_reconcile_ordinal_command(
                db,
                migration_file.parent,
                next_ordinal,
            ),
            emit_event=_emit_diag,
            meta_factory=_query_meta,
        )
    except SchemaVersionReconcileRequired as exc:
        _emit_reconcile_required(db=db, exc=exc)
        raise


def reconcile_migration_ordinal(
    driver: Any,
    db: str,
    migrations_dir: Path,
    ordinal: int,
    *,
    allow_gaps: bool = False,
) -> None:
    """Record a missing schema_version ordinal without rerunning SCHEMA work."""
    all_migrations = get_migrations(migrations_dir, allow_gaps=allow_gaps)
    known_ordinals = {value for value, _path in all_migrations}
    if ordinal not in known_ordinals:
        raise ValueError(
            f"Cannot reconcile ordinal {ordinal}: migration not found in {migrations_dir}"
        )

    current_ordinal = get_current_schema_version(driver, db)
    if ordinal <= current_ordinal:
        raise ValueError(
            f"Cannot reconcile ordinal {ordinal}: current schema_version is {current_ordinal}"
        )

    migration_name = next(
        path.name for value, path in all_migrations if value == ordinal
    )
    try:
        _write_schema_version_record(
            driver,
            db,
            ordinal,
            source_kind="migration_reconcile",
            source_name=migration_name,
            recovery_command=_reconcile_ordinal_command(db, migrations_dir, ordinal),
            emit_event=_emit_diag,
            meta_factory=_query_meta,
        )
    except Exception as exc:
        raise RuntimeError(
            f"Failed reconcile WRITE for migration ordinal {ordinal}: {exc}"
        ) from exc


def run_migrations(
    driver: Any,
    db: str,
    migrations_dir: Path,
    *,
    target: int | None = None,
    dry_run: bool = False,
    allow_gaps: bool = False,
) -> int:
    """Run all pending migrations in order.

    Returns the number of migrations applied.
    """
    all_migrations = get_migrations(migrations_dir, allow_gaps=allow_gaps)
    if not all_migrations:
        logger.info("No migrations found in %s", migrations_dir)
        return 0

    current_ordinal = get_current_schema_version(driver, db)
    logger.info("Current schema version ordinal: %d", current_ordinal)

    pending = [
        (ordinal, mig)
        for ordinal, mig in all_migrations
        if ordinal > current_ordinal and (target is None or ordinal <= target)
    ]

    if not pending:
        logger.info("No pending migrations to apply.")
        return 0

    logger.info("Planning to apply %d migrations.", len(pending))
    for ordinal, path in pending:
        apply_migration(driver, db, path, ordinal, dry_run=dry_run)

    logger.info("Migration completed cleanly. Applied %d migrations.", len(pending))
    return len(pending)
