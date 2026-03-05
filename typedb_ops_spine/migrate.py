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
    """Read the current schema version ordinal from the database.

    Queries the ``schema_version`` entity for ``ordinal`` attributes.
    Returns 0 if no version is found (e.g. fresh database).
    """
    from typedb.driver import TransactionType

    query = "match $v isa schema_version, has ordinal $o; select $o;"
    try:
        with driver.transaction(db, TransactionType.READ) as tx:
            ans = tx.query(query).resolve()
            rows = list(ans.as_concept_rows())

            ordinals = []
            for r in rows:
                o_attr = r.get("o")
                if o_attr and o_attr.is_attribute():
                    ordinals.append(int(o_attr.as_attribute().get_value()))
            ordinal = max(ordinals) if ordinals else 0
            _emit_diag({
                "db": db,
                "tx_type": "READ",
                **_query_meta(query),
                "stage": "schema_version_read",
                "status": "success",
                "answer_kind": "concept_rows",
                "row_count": len(rows),
                "doc_count": 0,
                "error_class": None,
                "error_message": None,
                "ordinal": ordinal,
            })
            return ordinal
    except Exception as e:
        _emit_diag({
            "db": db,
            "tx_type": "READ",
            **_query_meta(query),
            "stage": "schema_version_read",
            "status": "fail",
            "error_class": e.__class__.__name__,
            "error_message": str(e),
            "answer_kind": None,
            "row_count": 0,
            "doc_count": 0,
        })
        logger.warning("schema_version query failed (assuming 0): %s", e)
        return 0


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
    if not any(schema.lower().startswith(kw) for kw in ["define", "undefine", "redefine"]):
        raise ValueError(
            f"Migration hygiene violation: {migration_file.name} must start with "
            f"define/undefine/redefine. Found: {schema[:20]}..."
        )

    file_hash = hashlib.sha256(schema.encode("utf-8")).hexdigest()[:12]
    logger.info("Applying %s (sha256: %s)", migration_file.name, file_hash)

    if dry_run:
        logger.info("Dry-run: skipping %s", migration_file.name)
        return

    git_commit = os.getenv("GITHUB_SHA", "unknown")
    applied_at = (
        datetime.now(timezone.utc)
        .replace(tzinfo=None)
        .isoformat(timespec="microseconds")
    )

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
    version_query = (
        f"insert $v isa schema_version, "
        f'has ordinal {next_ordinal}, '
        f'has git-commit "{git_commit}", '
        f"has applied-at {applied_at};"
    )

    try:
        with driver.transaction(db, TransactionType.WRITE) as tx:
            tx.query(version_query).resolve()
            tx.commit()
            _emit_diag({
                "db": db,
                "tx_type": "WRITE",
                **_query_meta(version_query),
                "stage": "record_schema_version",
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
            "tx_type": "WRITE",
            **_query_meta(version_query),
            "stage": "record_schema_version",
            "status": "fail",
            "error_class": e.__class__.__name__,
            "error_message": str(e),
            "answer_kind": None,
            "row_count": 0,
            "doc_count": 0,
            "migration": migration_file.name,
        })
        raise RuntimeError(
            f"Failed WRITE transaction for {migration_file.name} "
            f"(Ordinal: {next_ordinal}): {e}"
        ) from e


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
