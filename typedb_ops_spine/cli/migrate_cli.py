"""
CLI entry point: ops-migrate

Thin argparse wrapper over typedb_ops_spine.migrate and readiness.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


def main() -> int:
    p = argparse.ArgumentParser(
        prog="ops-migrate",
        description="Deterministic linear schema migrations for TypeDB.",
    )
    p.add_argument(
        "--migrations-dir",
        default="migrations",
        help="Directory containing NNN_*.tql files",
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
        help="Print planned migration actions without executing.",
    )
    p.add_argument(
        "--target",
        type=int,
        default=None,
        help="Apply migrations up to this ordinal.",
    )
    p.add_argument(
        "--allow-gaps",
        action="store_true",
        help="Allow gaps in migration ordinals (not recommended).",
    )
    args = p.parse_args()

    from typedb_ops_spine.migrate import get_migrations, run_migrations
    from typedb_ops_spine.readiness import connect_with_retries, ensure_database

    tls = os.getenv("TYPEDB_TLS", "false").lower() == "true"
    ca_path = os.getenv("TYPEDB_ROOT_CA_PATH") or None

    mig_dir = Path(args.migrations_dir)
    if not mig_dir.is_dir():
        print(
            f"[ops-migrate] WARNING: Migrations directory not found: {mig_dir}",
            file=sys.stderr,
        )
        return 0

    try:
        all_migrations = get_migrations(mig_dir, allow_gaps=args.allow_gaps)
    except ValueError as e:
        print(f"[ops-migrate] ERROR: {e}", file=sys.stderr)
        return 1

    print(f"[ops-migrate] Found {len(all_migrations)} migrations in {mig_dir}")

    address = args.address if args.address else f"{args.host}:{args.port}"
    print(f"[ops-migrate] Connecting to {address}")

    driver = connect_with_retries(
        address, args.username, args.password, tls, ca_path,
    )
    try:
        if args.recreate:
            if driver.databases.contains(args.database):
                driver.databases.get(args.database).delete()
                print(f"[ops-migrate] Database deleted: {args.database}")

        ensure_database(driver, args.database)

        applied = run_migrations(
            driver,
            args.database,
            mig_dir,
            target=args.target,
            dry_run=args.dry_run,
            allow_gaps=args.allow_gaps,
        )
        print(f"[ops-migrate] Done. Applied {applied} migrations.")
    finally:
        driver.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
