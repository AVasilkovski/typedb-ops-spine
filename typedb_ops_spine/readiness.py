"""
TypeDB readiness checks and database management.

Provides address normalization helpers and connect_with_retries() that forces
a real network round-trip (driver.databases.all()) before returning the
driver, plus ensure_database().
"""

from __future__ import annotations

import logging
import time
from typing import Any
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


def normalize_typedb_address(raw: str, default_port: int = 1729) -> str:
    """Normalize a TypeDB address for local Core or Cloud deployments.

    Examples:
        "localhost" -> "localhost:1729"
        "localhost:1730" -> "localhost:1730"
        "https://cloud.typedb.com" -> "https://cloud.typedb.com:1729"
        "https://cloud.typedb.com:443/" -> "https://cloud.typedb.com:443"
    """
    value = (raw or "").strip().rstrip("/")
    if not value:
        return f"localhost:{default_port}"

    if "://" in value:
        parsed = urlparse(value)
        scheme = parsed.scheme or "https"
        host = parsed.hostname or value
        port = parsed.port or default_port
        return f"{scheme}://{host}:{port}"

    host_part, sep, port_part = value.rpartition(":")
    if sep and host_part and port_part.isdigit():
        return value

    return f"{value}:{default_port}"


def resolve_connection_address(
    address: str | None,
    host: str,
    port: str | int,
) -> str:
    """Resolve CLI address/host/port inputs into a normalized address string."""
    raw = address if address and str(address).strip() else host
    return normalize_typedb_address(str(raw), int(port))


def infer_tls_enabled(address: str, explicit_tls: bool | None = None) -> bool:
    """Infer TLS from the resolved address unless explicitly overridden."""
    if explicit_tls is not None:
        return explicit_tls
    return address.strip().lower().startswith("https://")


def connect_with_retries(
    address: str,
    username: str,
    password: str,
    tls: bool | None = None,
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

    resolved_address = normalize_typedb_address(address)
    resolved_tls = infer_tls_enabled(resolved_address, tls)
    creds = Credentials(username, password)
    opts = DriverOptions(
        is_tls_enabled=resolved_tls,
        tls_root_ca_path=ca_path,
    )

    last_err: Exception | None = None
    for i in range(1, retries + 1):
        driver = None
        try:
            driver = TypeDB.driver(resolved_address, creds, opts)
            # Real round-trip: force the driver to list databases
            _ = [d.name for d in driver.databases.all()]
            logger.info(
                "TypeDB connected to %s (tls=%s) on attempt %d/%d",
                resolved_address,
                resolved_tls,
                i,
                retries,
            )
            return driver
        except Exception as e:
            last_err = e
            if driver is not None:
                try:
                    driver.close()
                except Exception as close_err:
                    logger.debug("Failed to close TypeDB driver after retry failure: %s", close_err)
            logger.warning(
                "Waiting for TypeDB at %s (tls=%s) (%d/%d): %s",
                resolved_address,
                resolved_tls,
                i,
                retries,
                e,
            )
            time.sleep(sleep_s)

    raise RuntimeError(
        f"TypeDB not ready at {resolved_address} after {retries} attempts. "
        f"Last error: {last_err}"
    )


def ensure_database(driver: Any, db: str) -> None:
    """Create the database if it does not already exist."""
    existing = {d.name for d in driver.databases.all()}
    if db not in existing:
        driver.databases.create(db)
        logger.info("Created database: %s", db)
    else:
        logger.info("Database exists: %s", db)
