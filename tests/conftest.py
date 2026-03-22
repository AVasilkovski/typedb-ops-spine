"""
Shared test configuration for typedb-ops-spine.

Provides a TypeDB availability guard that skips integration tests
when no TypeDB server is query-ready.
"""

from __future__ import annotations

import os

import pytest

from typedb_ops_spine.readiness import TypeDBConfigError, connect_with_retries

TYPEDB_ADDRESS = os.getenv("TYPEDB_ADDRESS", "localhost:1729")
TYPEDB_USERNAME = os.getenv("TYPEDB_USERNAME", "admin")
TYPEDB_PASSWORD = os.getenv("TYPEDB_PASSWORD", "password")
TYPEDB_ROOT_CA_PATH = os.getenv("TYPEDB_ROOT_CA_PATH") or None


def _env_tls_override() -> bool | None:
    raw = os.getenv("TYPEDB_TLS")
    if raw is None:
        return None
    return raw.lower() == "true"


def _typedb_reachable() -> bool:
    """Check if TypeDB is query-ready via protocol round-trip."""
    try:
        driver = connect_with_retries(
            TYPEDB_ADDRESS,
            TYPEDB_USERNAME,
            TYPEDB_PASSWORD,
            tls=_env_tls_override(),
            ca_path=TYPEDB_ROOT_CA_PATH,
            retries=1,
            sleep_s=0,
        )
    except (RuntimeError, TypeDBConfigError, ImportError):
        return False
    driver.close()
    return True


# Marker for tests that require a live TypeDB instance
requires_typedb = pytest.mark.skipif(
    not _typedb_reachable(),
    reason=f"TypeDB not reachable at {TYPEDB_ADDRESS}",
)
