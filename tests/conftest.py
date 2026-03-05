"""
Shared test configuration for typedb-ops-spine.

Provides a TypeDB availability guard that skips integration tests
when no TypeDB server is reachable.
"""

from __future__ import annotations

import os
import socket

import pytest

TYPEDB_ADDRESS = os.getenv("TYPEDB_ADDRESS", "localhost:1729")


def _typedb_reachable() -> bool:
    """Check if TypeDB is reachable via TCP."""
    try:
        host, port_s = TYPEDB_ADDRESS.rsplit(":", 1)
        port = int(port_s)
        with socket.create_connection((host, port), timeout=2):
            return True
    except (OSError, ValueError):
        return False


# Marker for tests that require a live TypeDB instance
requires_typedb = pytest.mark.skipif(
    not _typedb_reachable(),
    reason=f"TypeDB not reachable at {TYPEDB_ADDRESS}",
)
