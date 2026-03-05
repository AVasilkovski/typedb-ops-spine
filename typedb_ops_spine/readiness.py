"""
TypeDB readiness checks and database management.

Provides connect_with_retries() that forces a real network round-trip
(driver.databases.all()) before returning the driver, plus ensure_database().
"""

from __future__ import annotations

import logging
import time
from typing import Any

logger = logging.getLogger(__name__)


def connect_with_retries(
    address: str,
    username: str,
    password: str,
    tls: bool = False,
    ca_path: str | None = None,
    retries: int = 30,
    sleep_s: float = 2.0,
) -> Any:
    """Connect to TypeDB with retry loop and real round-trip verification.

    Forces a ``driver.databases.all()`` call to prove the connection is live.
    Supports TLS for Cloud deployments with optional CA path.

    Returns:
        A connected TypeDB driver instance.

    Raises:
        RuntimeError: if TypeDB is not reachable after ``retries`` attempts.
    """
    from typedb.driver import Credentials, DriverOptions, TypeDB

    creds = Credentials(username, password)
    opts = DriverOptions(is_tls_enabled=tls, tls_root_ca_path=ca_path)

    last_err: Exception | None = None
    for i in range(1, retries + 1):
        try:
            driver = TypeDB.driver(address, creds, opts)
            # Real round-trip: force the driver to list databases
            _ = [d.name for d in driver.databases.all()]
            logger.info("TypeDB connected on attempt %d/%d", i, retries)
            return driver
        except Exception as e:
            last_err = e
            logger.warning(
                "Waiting for TypeDB (%d/%d): %s", i, retries, e
            )
            time.sleep(sleep_s)

    raise RuntimeError(
        f"TypeDB not ready after {retries} attempts. Last error: {last_err}"
    )


def ensure_database(driver: Any, db: str) -> None:
    """Create the database if it does not already exist."""
    existing = {d.name for d in driver.databases.all()}
    if db not in existing:
        driver.databases.create(db)
        logger.info("Created database: %s", db)
    else:
        logger.info("Database exists: %s", db)
