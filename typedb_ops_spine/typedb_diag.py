"""
Smoke diagnostics for TypeDB connectivity and post-migration verification.

This is intentionally lighter-weight than the write canary and probe tools:
it proves the server is reachable, the target database exists when required,
and optionally executes a caller-provided READ_ROWS smoke query.
"""

from __future__ import annotations

import logging
import time

from typedb_ops_spine.diagnostics import emit_typedb_diag
from typedb_ops_spine.readiness import validate_connection_config

logger = logging.getLogger(__name__)


def _diag(
    *,
    db: str,
    tx_type: str,
    action: str,
    query: str,
    kind: str,
    address: str,
    **extra: object,
) -> None:
    emit_typedb_diag(
        component="ops_typedb_diag",
        db_name=db,
        tx_type=tx_type,
        action=action,
        query=query,
        answer_kind=kind,
        address=address,
        **extra,
    )


def run_smoke_diagnostics(
    address: str,
    db: str,
    username: str,
    password: str,
    *,
    tls: bool | None = None,
    ca_path: str | None = None,
    require_db: bool = False,
    smoke_query: str = "",
    retries: int = 30,
    sleep_s: float = 2.0,
) -> int:
    """Run connectivity/database/smoke-query diagnostics against TypeDB."""
    resolved_address, resolved_tls, resolved_ca_path = validate_connection_config(
        address,
        tls=tls,
        ca_path=ca_path,
    )
    query = (smoke_query or "").strip()

    from typedb.driver import Credentials, DriverOptions, TransactionType, TypeDB

    creds = Credentials(username, password)
    opts = DriverOptions(
        is_tls_enabled=resolved_tls,
        tls_root_ca_path=resolved_ca_path,
    )

    last_err: Exception | None = None

    for attempt in range(1, retries + 1):
        driver = None
        try:
            driver = TypeDB.driver(resolved_address, creds, opts)
            databases = [database.name for database in driver.databases.all()]
            _diag(
                db=db,
                tx_type="META",
                action="list_databases",
                query="databases.all()",
                kind="ok",
                address=resolved_address,
                status="success",
                attempt=attempt,
                tls=resolved_tls,
                databases=",".join(databases),
            )

            db_present = db in databases
            _diag(
                db=db,
                tx_type="META",
                action="database_presence",
                query=db,
                kind="ok" if db_present else "missing_db",
                address=resolved_address,
                status="success" if db_present or not require_db else "fail",
                attempt=attempt,
                tls=resolved_tls,
                require_db=require_db,
                db_present=db_present,
            )

            if not db_present:
                logger.warning("Database %s not present on %s", db, resolved_address)
                return 1 if (require_db or query) else 0

            if not query:
                return 0

            with driver.transaction(db, TransactionType.READ) as tx:
                ans = tx.query(query).resolve()
                rows = list(ans.as_concept_rows())

            _diag(
                db=db,
                tx_type="READ",
                action="smoke_read",
                query=query,
                kind="rows",
                address=resolved_address,
                status="success",
                attempt=attempt,
                tls=resolved_tls,
                row_count=len(rows),
            )
            return 0

        except Exception as exc:
            last_err = exc
            _diag(
                db=db,
                tx_type="READ",
                action="connect_or_smoke_read",
                query=query or "databases.all()",
                kind="exception",
                address=resolved_address,
                status="fail",
                attempt=attempt,
                tls=resolved_tls,
                error_code=type(exc).__name__,
                error_message=str(exc),
            )
            if attempt < retries:
                time.sleep(sleep_s)
        finally:
            if driver is not None:
                try:
                    driver.close()
                except Exception as close_err:
                    logger.debug("Failed to close TypeDB driver after smoke attempt: %s", close_err)

    logger.error(
        "Smoke diagnostics failed for %s after %d attempts: %s",
        resolved_address,
        retries,
        last_err,
    )
    return 1
