"""
CLI entry point: ops-apply-schema

Thin argparse wrapper over typedb_ops_spine.schema_apply and readiness.
"""

from __future__ import annotations

import argparse
import os
import sys


def main() -> int:
    p = argparse.ArgumentParser(
        prog="ops-apply-schema",
        description="Apply TypeDB schema (local Core or Cloud TLS).",
    )
    p.add_argument(
        "--schema",
        action="append",
        default=None,
        help="Schema file path or glob. May be passed multiple times.",
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
        "--recreate",
        action="store_true",
        help="Delete and recreate the database before applying.",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned actions without executing.",
    )
    args = p.parse_args()

    from typedb_ops_spine.readiness import connect_with_retries, ensure_database
    from typedb_ops_spine.schema_apply import apply_schema, resolve_schema_files

    tls = os.getenv("TYPEDB_TLS", "false").lower() == "true"
    ca_path = os.getenv("TYPEDB_ROOT_CA_PATH") or None

    raw_schema_args = args.schema or [
        os.getenv("TYPEDB_SCHEMA", "schema.tql"),
    ]

    try:
        schema_paths = resolve_schema_files(raw_schema_args)
    except (ValueError, FileNotFoundError) as e:
        print(f"[ops-apply-schema] ERROR: {e}", file=sys.stderr)
        return 1

    print("[ops-apply-schema] Resolved schema files:")
    for path in schema_paths:
        print(f"  - {path}")

    if args.dry_run:
        print("[ops-apply-schema] dry-run: no changes will be applied")
        return 0

    address = args.address if args.address else f"{args.host}:{args.port}"
    print(f"[ops-apply-schema] Connecting to {address} tls={tls}")

    driver = connect_with_retries(
        address, args.username, args.password, tls, ca_path,
    )
    try:
        if args.recreate:
            if driver.databases.contains(args.database):
                driver.databases.get(args.database).delete()
                print(f"[ops-apply-schema] Database deleted: {args.database}")

        ensure_database(driver, args.database)
        apply_schema(driver, args.database, schema_paths)
        print("[ops-apply-schema] Done.")
    finally:
        driver.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
