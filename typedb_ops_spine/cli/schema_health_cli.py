"""
CLI entry point: ops-schema-health

Checks schema version drift between repo migrations and database state.
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
        prog="ops-schema-health",
        description="Check schema version parity between repo and database.",
    )
    p.add_argument(
        "--migrations-dir",
        default="migrations",
        help="Directory containing NNN_*.tql migration files",
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
    args = p.parse_args()

    from typedb_ops_spine.readiness import (
        connect_with_retries,
        infer_tls_enabled,
        resolve_connection_address,
    )
    from typedb_ops_spine.schema_health import check_health

    tls = _env_tls_override()
    ca_path = os.getenv("TYPEDB_ROOT_CA_PATH") or None

    address = resolve_connection_address(args.address, args.host, args.port)
    resolved_tls = infer_tls_enabled(address, tls)

    if not address or address == ":":
        print("[ops-schema-health] SKIP: missing address")
        return 0

    print(f"[ops-schema-health] Connecting to {address} tls={resolved_tls}")
    driver = connect_with_retries(
        address, args.username, args.password, resolved_tls, ca_path,
    )
    try:
        healthy, repo_ord, db_ord = check_health(
            driver, args.database, args.migrations_dir,
        )
        print(f"[ops-schema-health] Repo ordinal: {repo_ord}")
        print(f"[ops-schema-health] DB ordinal:   {db_ord}")
        if healthy:
            print("[ops-schema-health] PASS: parity OK")
            return 0
        else:
            print("[ops-schema-health] FAIL: drift detected")
            return 1
    except Exception as e:
        print(f"[ops-schema-health] ERROR: {e}", file=sys.stderr)
        return 1
    finally:
        driver.close()


if __name__ == "__main__":
    sys.exit(main())
