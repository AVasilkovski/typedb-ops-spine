"""
CLI entry point: ops-typedb-diag

Lightweight connectivity/database/smoke-query diagnostics for TypeDB Core or
Cloud deployments.
"""

from __future__ import annotations

import argparse
import os
import sys


def _env_tls_override() -> bool | None:
    raw = os.getenv("TYPEDB_TLS")
    if raw is None:
        return None
    return raw.lower() == "true"


def main() -> int:
    p = argparse.ArgumentParser(
        prog="ops-typedb-diag",
        description="TypeDB connectivity and smoke diagnostics.",
    )
    p.add_argument(
        "--database",
        default=os.getenv("TYPEDB_DATABASE", "default_db"),
    )
    p.add_argument("--address", default=os.getenv("TYPEDB_ADDRESS"))
    p.add_argument("--host", default=os.getenv("TYPEDB_HOST", "localhost"))
    p.add_argument("--port", default=os.getenv("TYPEDB_PORT", "1729"))
    p.add_argument(
        "--username", default=os.getenv("TYPEDB_USERNAME", "admin"),
    )
    p.add_argument(
        "--password", default=os.getenv("TYPEDB_PASSWORD", "password"),
    )
    p.add_argument(
        "--require-db",
        action="store_true",
        help="Fail if the configured database does not exist.",
    )
    p.add_argument(
        "--smoke-query",
        default=os.getenv("TYPEDB_SMOKE_QUERY", ""),
        help="Optional READ_ROWS smoke query to execute after database presence is verified.",
    )
    p.add_argument(
        "--retries",
        type=int,
        default=30,
        help="Connection retry attempts.",
    )
    p.add_argument(
        "--sleep-s",
        type=float,
        default=2.0,
        help="Retry sleep interval in seconds.",
    )
    args = p.parse_args()

    from typedb_ops_spine.readiness import (
        TypeDBConfigError,
        resolve_connection_config,
    )
    from typedb_ops_spine.typedb_diag import run_smoke_diagnostics

    tls = _env_tls_override()
    ca_path = os.getenv("TYPEDB_ROOT_CA_PATH") or None
    try:
        address, resolved_tls, ca_path = resolve_connection_config(
            args.address,
            args.host,
            args.port,
            tls=tls,
            ca_path=ca_path,
        )
    except TypeDBConfigError as e:
        print(f"[ops-typedb-diag] ERROR: {e}", file=sys.stderr)
        return 1

    print(f"[ops-typedb-diag] Connecting to {address} tls={resolved_tls}")
    if args.smoke_query:
        print("[ops-typedb-diag] Smoke query configured.")

    return run_smoke_diagnostics(
        address,
        args.database,
        args.username,
        args.password,
        tls=resolved_tls,
        ca_path=ca_path,
        require_db=args.require_db,
        smoke_query=args.smoke_query,
        retries=args.retries,
        sleep_s=args.sleep_s,
    )


if __name__ == "__main__":
    sys.exit(main())
