"""
Schema health / drift detection.

Compares the repo head ordinal (from migration files) against the
database's current schema version ordinal. Any mismatch = drift failure.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SchemaHealthExtraResult:
    name: str = "extra_invariant"
    ok: bool = True
    message: str = ""
    skipped: bool = False
    details: dict[str, Any] | None = None


@dataclass(frozen=True)
class SchemaHealthReport:
    healthy: bool
    repo_ordinal: int
    db_ordinal: int
    extra_result: SchemaHealthExtraResult | None = None


ExtraInvariant = Callable[[Any, str], bool | SchemaHealthExtraResult]


def _diag_path() -> Path:
    artifacts_dir = os.getenv("CI_ARTIFACTS_DIR", "ci_artifacts")
    return Path(artifacts_dir) / "schema_health_diagnostics.jsonl"


def _emit_diag(event: dict[str, Any]) -> None:
    out = _diag_path()
    out.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "component": "schema_health",
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


def _emit_extra_invariant_diag(
    db: str,
    result: SchemaHealthExtraResult,
) -> None:
    event: dict[str, Any] = {
        "db": db,
        "tx_type": "READ",
        **_query_meta(f"extra_invariant:{result.name}"),
        "stage": "extra_invariant",
        "status": "skip" if result.skipped else ("success" if result.ok else "fail"),
        "answer_kind": "skip" if result.skipped else ("ok" if result.ok else "exception"),
        "row_count": 0,
        "doc_count": 0,
        "error_class": None,
        "error_message": result.message,
        "extra_name": result.name,
        "skipped": result.skipped,
    }
    if result.details:
        event.update(result.details)
    _emit_diag(event)


def _normalize_extra_result(
    raw: bool | SchemaHealthExtraResult,
    *,
    extra_name: str,
) -> SchemaHealthExtraResult:
    if isinstance(raw, bool):
        return SchemaHealthExtraResult(name=extra_name, ok=raw)
    if isinstance(raw, SchemaHealthExtraResult):
        if raw.name:
            return raw
        return SchemaHealthExtraResult(
            name=extra_name,
            ok=raw.ok,
            message=raw.message,
            skipped=raw.skipped,
            details=raw.details,
        )
    raise TypeError(
        f"Extra invariant '{extra_name}' must return bool or "
        f"SchemaHealthExtraResult, got {type(raw).__name__}"
    )


def repo_head_ordinal(migrations_dir: str | Path) -> int:
    """Scan migration directory for the highest ordinal.

    Args:
        migrations_dir: Path to the migrations directory.

    Returns:
        The highest ordinal found, or 0 if none exist.

    Raises:
        FileNotFoundError: if the directory doesn't exist.
    """
    p = Path(migrations_dir)
    if not p.exists():
        raise FileNotFoundError(f"migrations_dir not found: {migrations_dir}")
    ords: list[int] = []
    for f in p.glob("*.tql"):
        m = re.match(r"^(\d+)_", f.name)
        if m:
            ords.append(int(m.group(1)))
    return max(ords) if ords else 0


def db_current_ordinal(driver: Any, db: str) -> int:
    """Query the database for the current schema version ordinal.

    Returns 0 if the schema_version entity doesn't exist yet.
    """
    from typedb.driver import TransactionType

    q = "match $v isa schema_version, has ordinal $o; select $o;"
    try:
        with driver.transaction(db, TransactionType.READ) as tx:
            ans = tx.query(q).resolve()
            rows = list(ans.as_concept_rows())

            ords: list[int] = []
            for row in rows:
                o_attr = row.get("o")
                if o_attr and o_attr.is_attribute():
                    ords.append(int(o_attr.as_attribute().get_value()))
            ordinal = max(ords) if ords else 0
            _emit_diag({
                "db": db,
                "tx_type": "READ",
                **_query_meta(q),
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
            **_query_meta(q),
            "stage": "schema_version_read",
            "status": "fail",
            "answer_kind": None,
            "row_count": 0,
            "doc_count": 0,
            "error_class": e.__class__.__name__,
            "error_message": str(e),
        })
        logger.warning("schema_version query failed (assuming 0): %s", e)
        return 0


def run_health_checks(
    driver: Any,
    db: str,
    migrations_dir: str | Path,
    *,
    extra_invariant: ExtraInvariant | None = None,
    extra_name: str = "extra_invariant",
) -> SchemaHealthReport:
    """Check schema health by comparing repo and DB ordinals and optional hook.

    The extra invariant only runs when ordinal parity already holds.

    Returns:
        SchemaHealthReport with built-in parity status and optional extra result.
    """
    repo_ord = repo_head_ordinal(migrations_dir)
    db_ord = db_current_ordinal(driver, db)
    parity_ok = repo_ord == db_ord

    if not parity_ok:
        logger.warning(
            "Schema health FAIL: drift detected repo=%d db=%d",
            repo_ord, db_ord,
        )
        extra_result: SchemaHealthExtraResult | None = None
        if extra_invariant is not None:
            extra_result = SchemaHealthExtraResult(
                name=extra_name,
                ok=True,
                skipped=True,
                message="Skipped due to ordinal drift",
            )
            _emit_extra_invariant_diag(db, extra_result)
        return SchemaHealthReport(
            healthy=False,
            repo_ordinal=repo_ord,
            db_ordinal=db_ord,
            extra_result=extra_result,
        )

    logger.info("Schema health PASS: parity OK (ordinal=%d)", repo_ord)

    if extra_invariant is None:
        return SchemaHealthReport(
            healthy=True,
            repo_ordinal=repo_ord,
            db_ordinal=db_ord,
            extra_result=None,
        )

    try:
        raw_result = extra_invariant(driver, db)
        extra_result = _normalize_extra_result(raw_result, extra_name=extra_name)
    except Exception as exc:
        extra_result = SchemaHealthExtraResult(
            name=extra_name,
            ok=False,
            message=str(exc),
            details={"error_class": exc.__class__.__name__},
        )

    _emit_extra_invariant_diag(db, extra_result)

    if extra_result.ok:
        logger.info("Schema health PASS: parity OK (ordinal=%d)", repo_ord)
    else:
        logger.warning(
            "Schema health FAIL: extra invariant '%s' failed",
            extra_result.name,
        )
    return SchemaHealthReport(
        healthy=extra_result.ok,
        repo_ordinal=repo_ord,
        db_ordinal=db_ord,
        extra_result=extra_result,
    )


def check_health(
    driver: Any,
    db: str,
    migrations_dir: str | Path,
) -> tuple[bool, int, int]:
    """Backward-compatible schema health wrapper returning the original tuple."""
    report = run_health_checks(driver, db, migrations_dir)
    return report.healthy, report.repo_ordinal, report.db_ordinal
