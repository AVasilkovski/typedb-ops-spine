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
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


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


def check_health(
    driver: Any,
    db: str,
    migrations_dir: str | Path,
) -> tuple[bool, int, int]:
    """Check schema health by comparing repo and DB ordinals.

    Returns:
        Tuple of (healthy: bool, repo_ordinal: int, db_ordinal: int).
    """
    repo_ord = repo_head_ordinal(migrations_dir)
    db_ord = db_current_ordinal(driver, db)
    healthy = repo_ord == db_ord
    if healthy:
        logger.info("Schema health PASS: parity OK (ordinal=%d)", repo_ord)
    else:
        logger.warning(
            "Schema health FAIL: drift detected repo=%d db=%d",
            repo_ord, db_ord,
        )
    return healthy, repo_ord, db_ord
