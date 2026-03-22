"""
TypeDB readiness checks and database management.

Provides address normalization helpers, connection config validation, and
connect_with_retries() that forces a real network round-trip
(driver.databases.all()) before returning the driver.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


class TypeDBConfigError(ValueError):
    """Raised when TypeDB connection settings are deterministically invalid."""


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


def _invalid_address_help() -> str:
    return "Use host[:port] for local Core or https://host[:port] for Cloud TLS."


def validate_connection_config(
    address: str,
    tls: bool | None = None,
    ca_path: str | None = None,
    *,
    default_port: int = 1729,
    used_host_port_fallback: bool = False,
) -> tuple[str, bool, str | None]:
    """Validate and normalize TypeDB connection settings.

    Returns:
        Tuple of (normalized_address, resolved_tls, normalized_ca_path).

    Raises:
        TypeDBConfigError: if the address/TLS/CA combination is invalid.
    """
    raw = (address or "").strip()
    if not raw:
        raise TypeDBConfigError(f"TypeDB address is empty. {_invalid_address_help()}")

    try:
        normalized = normalize_typedb_address(raw, default_port=default_port)
    except ValueError as exc:
        raise TypeDBConfigError(
            f"Invalid TypeDB address '{raw}': {exc}. {_invalid_address_help()}"
        ) from exc
    cleaned_ca = (ca_path or "").strip() or None

    if "://" in raw:
        parsed = urlparse(raw.rstrip("/"))
        scheme = (parsed.scheme or "").lower()
        if scheme != "https":
            raise TypeDBConfigError(
                f"Unsupported TypeDB address scheme '{parsed.scheme}' in '{raw}'. "
                f"{_invalid_address_help()}"
            )
        if parsed.path not in ("", "/") or parsed.params or parsed.query or parsed.fragment:
            raise TypeDBConfigError(
                f"TypeDB HTTPS addresses must not include a path, query, or fragment: '{raw}'. "
                f"{_invalid_address_help()}"
            )
        if not parsed.hostname:
            raise TypeDBConfigError(
                f"TypeDB HTTPS address is missing a hostname: '{raw}'. {_invalid_address_help()}"
            )
    else:
        if any(ch in raw for ch in "/?#"):
            raise TypeDBConfigError(
                f"Unsupported TypeDB address '{raw}'. {_invalid_address_help()}"
            )

    resolved_tls = infer_tls_enabled(normalized, tls)
    is_https = normalized.lower().startswith("https://")

    if resolved_tls and not is_https:
        fallback_hint = ""
        if used_host_port_fallback:
            fallback_hint = (
                " No explicit --address/TYPEDB_ADDRESS was provided, so host/port "
                f"resolved to '{normalized}'."
            )
        raise TypeDBConfigError(
            f"TLS is enabled but the resolved TypeDB address is not HTTPS: '{normalized}'. "
            "Use an https://host:port address for Cloud TLS or disable TLS for local Core."
            f"{fallback_hint}"
        )

    if not resolved_tls and is_https:
        raise TypeDBConfigError(
            f"TLS is disabled but the resolved TypeDB address is HTTPS: '{normalized}'. "
            "Enable TLS or remove the explicit HTTPS address override."
        )

    if cleaned_ca and not resolved_tls:
        raise TypeDBConfigError(
            "A TLS root CA path was provided but TLS is disabled. "
            "TYPEDB_ROOT_CA_PATH/ca_path is only valid with HTTPS/TLS connections."
        )

    if cleaned_ca:
        ca_file = Path(cleaned_ca).expanduser()
        if not ca_file.is_file():
            raise TypeDBConfigError(
                f"Configured TLS root CA path does not exist or is not a file: '{cleaned_ca}'."
            )
        cleaned_ca = str(ca_file)

    return normalized, resolved_tls, cleaned_ca


def resolve_connection_config(
    address: str | None,
    host: str,
    port: str | int,
    *,
    tls: bool | None = None,
    ca_path: str | None = None,
) -> tuple[str, bool, str | None]:
    """Resolve CLI address/host/port inputs into validated connection settings."""
    try:
        resolved_port = int(port)
    except (TypeError, ValueError) as exc:
        raise TypeDBConfigError(
            f"Invalid TypeDB port '{port}'. Port must be an integer."
        ) from exc

    if address and str(address).strip():
        raw = str(address).strip()
    else:
        raw = normalize_typedb_address(str(host), resolved_port)
    return validate_connection_config(
        raw,
        tls=tls,
        ca_path=ca_path,
        default_port=resolved_port,
        used_host_port_fallback=not (address and str(address).strip()),
    )


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
        TypeDBConfigError: if the address/TLS/CA configuration is invalid.
        RuntimeError: if TypeDB is not reachable after ``retries`` attempts.
    """
    resolved_address, resolved_tls, resolved_ca_path = validate_connection_config(
        address,
        tls=tls,
        ca_path=ca_path,
        default_port=1729,
    )

    from typedb.driver import Credentials, DriverOptions, TypeDB

    creds = Credentials(username, password)
    opts = DriverOptions(
        is_tls_enabled=resolved_tls,
        tls_root_ca_path=resolved_ca_path,
    )

    last_err: Exception | None = None
    for i in range(1, retries + 1):
        driver = None
        try:
            driver = TypeDB.driver(resolved_address, creds, opts)
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
