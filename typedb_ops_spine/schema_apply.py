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
import re
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


def parse_canonical_caps(
    schema_text: str,
) -> tuple[dict[str, str], dict[str, set[str]], dict[str, set[str]]]:
    """Parse canonical schema text into parent/owns/plays capability maps."""
    parent_of: dict[str, str] = {}
    owns_of: dict[str, set[str]] = {}
    plays_of: dict[str, set[str]] = {}

    scrubbed = re.sub(r"#.*", "", schema_text, flags=re.MULTILINE)
    block_re = re.compile(
        r"\b(entity|relation)\b\s+([a-zA-Z0-9_-]+)"
        r"(?:\s+\bsub\b\s+([a-zA-Z0-9_-]+))?\s*(.*?);",
        re.S,
    )
    owns_re = re.compile(r"\bowns\b\s+([a-zA-Z0-9_-]+)")
    plays_re = re.compile(r"\bplays\b\s+([a-zA-Z0-9_-]+:[a-zA-Z0-9_-]+)")

    for _block_type, type_label, supertype, body in block_re.findall(scrubbed):
        if supertype:
            supertype = supertype.strip()
            if supertype not in ("entity", "relation"):
                parent_of[type_label] = supertype

        owns_of.setdefault(type_label, set())
        owns_of[type_label].update(owns_re.findall(body))

        plays_of.setdefault(type_label, set())
        plays_of[type_label].update(plays_re.findall(body))

    return parent_of, owns_of, plays_of


def compute_transitive_subtypes(parent_of: dict[str, str]) -> dict[str, set[str]]:
    """Compute the full subtype closure for each supertype."""
    children_of: dict[str, set[str]] = {}
    for child, parent in parent_of.items():
        children_of.setdefault(parent, set()).add(child)

    subtypes: dict[str, set[str]] = {}

    def _all_children(type_label: str) -> set[str]:
        if type_label in subtypes:
            return subtypes[type_label]

        children = set(children_of.get(type_label, set()))
        for child in list(children):
            children.update(_all_children(child))
        subtypes[type_label] = children
        return children

    for type_label in list(children_of):
        _all_children(type_label)

    return subtypes


def plan_auto_migrations(
    parent_of: dict[str, str],
    owns_of: dict[str, set[str]],
    plays_of: dict[str, set[str]],
) -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
    """Plan guarded undefines for inherited capability redeclarations."""
    undefine_owns_specs: list[tuple[str, str]] = []
    undefine_plays_specs: list[tuple[str, str]] = []
    subtypes = compute_transitive_subtypes(parent_of)

    for supertype, attrs in owns_of.items():
        if supertype not in subtypes:
            continue
        for child in subtypes[supertype]:
            child_attrs = owns_of.get(child, set())
            for attr in attrs:
                if attr in child_attrs:
                    undefine_owns_specs.append((child, attr))

    for supertype, roles in plays_of.items():
        if supertype not in subtypes:
            continue
        for child in subtypes[supertype]:
            child_roles = plays_of.get(child, set())
            for role in roles:
                if role in child_roles:
                    undefine_plays_specs.append((child, role))

    return undefine_owns_specs, undefine_plays_specs


_SKIP_CODES = ("SVL35", "SVL36", "UEX20", "TSV10")


def _is_skip(exc: Exception) -> bool:
    msg = str(exc)
    return any(code in msg for code in _SKIP_CODES)


def parse_undefine_owns_spec(spec: str) -> tuple[str, str]:
    """Parse `<entity>:<attribute>` CLI input for guarded owns undefines."""
    parts = spec.split(":", maxsplit=1)
    if len(parts) != 2 or not parts[0] or not parts[1]:
        raise ValueError(
            "Invalid --undefine-owns spec. Expected format "
            f"'<entity>:<attribute>', got: {spec}"
        )
    return parts[0].strip(), parts[1].strip()


def migrate_undefine_owns(driver: Any, db: str, specs: list[str]) -> None:
    """Run guarded `undefine owns ... from ...;` schema migrations."""
    from typedb.driver import TransactionType

    for spec in specs:
        entity, attribute = parse_undefine_owns_spec(spec)
        query = f"undefine owns {attribute} from {entity};"
        try:
            with driver.transaction(db, TransactionType.SCHEMA) as tx:
                tx.query(query).resolve()
                tx.commit()
            _emit_diag({
                "db": db,
                "tx_type": "SCHEMA",
                **_query_meta(query),
                "stage": "undefine_owns",
                "status": "success",
                "error_class": None,
                "error_message": None,
                "answer_kind": "ok",
                "row_count": 0,
                "doc_count": 0,
            })
            logger.info("Guarded schema scrub applied: %s", query)
        except Exception as exc:
            if _is_skip(exc):
                _emit_diag({
                    "db": db,
                    "tx_type": "SCHEMA",
                    **_query_meta(query),
                    "stage": "undefine_owns",
                    "status": "skip",
                    "error_class": exc.__class__.__name__,
                    "error_message": str(exc),
                    "answer_kind": "skip",
                    "row_count": 0,
                    "doc_count": 0,
                })
                logger.info("Guarded schema scrub skipped for %s: %s", query, exc)
                continue

            _emit_diag({
                "db": db,
                "tx_type": "SCHEMA",
                **_query_meta(query),
                "stage": "undefine_owns",
                "status": "fail",
                "error_class": exc.__class__.__name__,
                "error_message": str(exc),
                "answer_kind": None,
                "row_count": 0,
                "doc_count": 0,
            })
            raise


def parse_undefine_plays_spec(spec: str) -> tuple[str, str]:
    """Parse `<type>:<relation:role>` CLI input for guarded plays undefines."""
    parts = spec.split(":", maxsplit=1)
    if len(parts) != 2 or not parts[0] or not parts[1]:
        raise ValueError(
            "Invalid --undefine-plays spec. Expected format "
            f"'<type>:<relation:role>', got: {spec}"
        )
    return parts[0].strip(), parts[1].strip()


def migrate_undefine_plays(driver: Any, db: str, specs: list[str]) -> None:
    """Run guarded `undefine plays ... from ...;` schema migrations."""
    from typedb.driver import TransactionType

    for spec in specs:
        type_label, scoped_role = parse_undefine_plays_spec(spec)
        query = f"undefine plays {scoped_role} from {type_label};"
        try:
            with driver.transaction(db, TransactionType.SCHEMA) as tx:
                tx.query(query).resolve()
                tx.commit()
            _emit_diag({
                "db": db,
                "tx_type": "SCHEMA",
                **_query_meta(query),
                "stage": "undefine_plays",
                "status": "success",
                "error_class": None,
                "error_message": None,
                "answer_kind": "ok",
                "row_count": 0,
                "doc_count": 0,
            })
            logger.info("Guarded schema scrub applied: %s", query)
        except Exception as exc:
            if _is_skip(exc):
                _emit_diag({
                    "db": db,
                    "tx_type": "SCHEMA",
                    **_query_meta(query),
                    "stage": "undefine_plays",
                    "status": "skip",
                    "error_class": exc.__class__.__name__,
                    "error_message": str(exc),
                    "answer_kind": "skip",
                    "row_count": 0,
                    "doc_count": 0,
                })
                logger.info("Guarded schema scrub skipped for %s: %s", query, exc)
                continue

            _emit_diag({
                "db": db,
                "tx_type": "SCHEMA",
                **_query_meta(query),
                "stage": "undefine_plays",
                "status": "fail",
                "error_class": exc.__class__.__name__,
                "error_message": str(exc),
                "answer_kind": None,
                "row_count": 0,
                "doc_count": 0,
            })
            raise


def apply_schema(
    driver: Any,
    db: str,
    schema_paths: list[Path],
) -> None:
    """Apply schema file(s) to a TypeDB database using SCHEMA transactions.

    All schema files are applied within one SCHEMA transaction so authoritative
    multi-file apply is atomic across files. Diagnostics are emitted for both
    success and failure.

    Args:
        driver: Connected TypeDB driver.
        db: Database name.
        schema_paths: Resolved schema file paths.
    """
    from typedb.driver import TransactionType

    schema_texts = [
        (schema_path, schema_path.read_text(encoding="utf-8"))
        for schema_path in schema_paths
    ]
    bundle_query = "\n\n".join(schema for _, schema in schema_texts)
    schema_files = ",".join(path.name for path, _ in schema_texts)

    try:
        with driver.transaction(db, TransactionType.SCHEMA) as tx:
            for _, schema in schema_texts:
                tx.query(schema).resolve()
            tx.commit()

        _emit_diag({
            "db": db,
            "tx_type": "SCHEMA",
            **_query_meta(bundle_query),
            "stage": "apply_schema",
            "status": "success",
            "error_class": None,
            "error_message": None,
            "answer_kind": "ok",
            "row_count": 0,
            "doc_count": 0,
            "schema_count": len(schema_texts),
            "schema_files": schema_files,
        })
        logger.info("Schema bundle applied atomically: %s", schema_files)
    except Exception as exc:
        failure_note = (
            "No authoritative multi-file bundle commit completed; partial "
            "multi-file apply is not expected."
        )
        _emit_diag({
            "db": db,
            "tx_type": "SCHEMA",
            **_query_meta(bundle_query),
            "stage": "apply_schema",
            "status": "fail",
            "error_class": exc.__class__.__name__,
            "error_message": f"{exc} {failure_note}",
            "answer_kind": None,
            "row_count": 0,
            "doc_count": 0,
            "schema_count": len(schema_texts),
            "schema_files": schema_files,
        })
        raise RuntimeError(
            f"Failed authoritative schema bundle apply for {schema_files}: {exc}. "
            f"{failure_note}"
        ) from exc


def get_current_schema_version(driver: Any, db: str) -> int:
    """Read current schema version ordinal."""
    return _get_current_schema_version(driver, db)


def head_migration_ordinal(migrations_dir: Path) -> int:
    """Parse NNN_*.tql ordinals and return max."""
    if not migrations_dir.is_dir():
        return 0
    files = list(migrations_dir.glob("*.tql"))
    ordinals = []
    for f in files:
        name = f.name
        parts = name.split("_")
        if parts and parts[0].isdigit():
            ordinals.append(int(parts[0]))
    return max(ordinals) if ordinals else 0


def _reconcile_head_command(db: str, migrations_dir: Path | None) -> str:
    migrations_value = str(migrations_dir) if migrations_dir is not None else "<migrations-dir>"
    return (
        "ops-apply-schema "
        f'--database "{db}" '
        f'--migrations-dir "{migrations_value}" '
        "--reconcile-schema-version-head"
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


def stamp_schema_version_head(
    driver: Any,
    db: str,
    head_ordinal: int,
    *,
    migrations_dir: Path | None = None,
) -> None:
    """
    INSERT a new schema_version row to fast-forward an authoritative schema install.
    Guards: No-op if head <= 0 or current >= head.
    """
    if head_ordinal <= 0:
        return

    current = get_current_schema_version(driver, db)
    if current >= head_ordinal:
        logger.info("[ops-apply-schema] skip stamping: current_ordinal=%d head_ordinal=%d", current, head_ordinal)
        return

    logger.info(
        "[ops-apply-schema] stamping schema_version: %d -> %d "
        "(authoritative schema applied)",
        current,
        head_ordinal,
    )

    try:
        record_schema_version(
            driver,
            db,
            head_ordinal,
            source_kind="authoritative_apply",
            source_name=str(migrations_dir) if migrations_dir is not None else "head_migration_ordinal",
            recovery_command=_reconcile_head_command(db, migrations_dir),
            emit_event=_emit_diag,
            meta_factory=_query_meta,
        )
    except SchemaVersionReconcileRequired as exc:
        _emit_reconcile_required(db=db, exc=exc)
        raise


def reconcile_schema_version_head(
    driver: Any,
    db: str,
    migrations_dir: Path,
) -> int:
    """Stamp only the repo head ordinal without reapplying authoritative schema."""
    if not migrations_dir.is_dir():
        raise FileNotFoundError(f"migrations_dir not found: {migrations_dir}")

    head_ordinal = head_migration_ordinal(migrations_dir)
    if head_ordinal <= 0:
        logger.info("[ops-apply-schema] no head ordinal found to reconcile")
        return 0

    current = get_current_schema_version(driver, db)
    if current >= head_ordinal:
        logger.info(
            "[ops-apply-schema] reconcile skipped: current_ordinal=%d head_ordinal=%d",
            current,
            head_ordinal,
        )
        return current

    try:
        _write_schema_version_record(
            driver,
            db,
            head_ordinal,
            source_kind="authoritative_reconcile",
            source_name=str(migrations_dir),
            recovery_command=_reconcile_head_command(db, migrations_dir),
            emit_event=_emit_diag,
            meta_factory=_query_meta,
        )
    except Exception as exc:
        raise RuntimeError(
            f"Failed reconcile WRITE for schema_version head {head_ordinal}: {exc}"
        ) from exc

    return head_ordinal
