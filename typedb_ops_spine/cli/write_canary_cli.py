"""
CLI entry point: ops-write-canary

Write→commit→read durability check with identity logging and
decision-grade diagnostics emission. Fail-slow.
"""

from __future__ import annotations

import argparse
import os
import sys
import uuid

from typedb_ops_spine.diagnostics import emit_typedb_diag
from typedb_ops_spine.exec import QueryMode, execute


def _resolve_address(args: argparse.Namespace) -> str:
    if args.address:
        return args.address
    return f"{args.host}:{args.port}"


def _diag(
    *, stage: str, action: str, tx_type: str, db: str, query: str, kind: str, **extra
) -> None:
    emit_typedb_diag(
        component="ops_write_canary",
        db_name=db,
        tx_type=tx_type,
        action=action,
        query=query,
        answer_kind=kind,
        stage=stage,
        **extra,
    )


def main() -> int:
    p = argparse.ArgumentParser(
        prog="ops-write-canary",
        description="TypeDB write/read commit durability canary.",
    )
    p.add_argument(
        "--database",
        default=os.getenv("TYPEDB_DATABASE", "default_db"),
    )
    p.add_argument(
        "--schema",
        help="Optional schema file for bootstrap.",
    )
    p.add_argument(
        "--migrations-dir",
        help="Optional migrations directory for bootstrap.",
    )
    args = p.parse_args()

    os.makedirs("ci_artifacts", exist_ok=True)

    from typedb_ops_spine.readiness import connect_with_retries, ensure_database
    from typedb_ops_spine.schema_apply import apply_schema, resolve_schema_files
    from typedb_ops_spine.migrate import run_migrations
    import importlib.metadata as md

    import typedb
    from typedb.driver import Credentials, DriverOptions, TransactionType, TypeDB

    db_name = args.database
    address = _resolve_address(args)
    tls = os.getenv("TYPEDB_TLS", "false").lower() == "true"
    ca_path = os.getenv("TYPEDB_ROOT_CA_PATH") or None

    try:
        driver_version = md.version("typedb-driver")
    except md.PackageNotFoundError:
        driver_version = getattr(typedb, "__version__", "unknown")

    print("--- TypeDB Durability Canary ---")
    print(f"  Database: {db_name}")
    print(f"  Address:  {address}")

    _diag(
        stage="init_canary_identity",
        action="identity",
        tx_type="INIT",
        db=db_name,
        query=f"addr={address}",
        kind="ok",
        driver_version=driver_version,
        address=address,
    )

    failures = 0
    try:
        # Step 0: Connect and Ensure DB
        driver = connect_with_retries(
            address, args.username, args.password,
            tls=tls, ca_path=ca_path, retries=10,
        )
        try:
            ensure_database(driver, db_name)

            # Step 0.1: Optional Bootstrap
            if args.schema:
                schema_paths = resolve_schema_files([args.schema])
                print(f"Bootstrapping schema: {schema_paths}")
                apply_schema(driver, db_name, schema_paths)

            if args.migrations_dir:
                from pathlib import Path
                print(f"Bootstrapping migrations: {args.migrations_dir}")
                run_migrations(driver, db_name, Path(args.migrations_dir))

            # Step 1: Write
            canary_tid = f"canary-{uuid.uuid4().hex[:6]}"
            print(f"Writing canary {canary_tid}...")
            with driver.transaction(db_name, TransactionType.WRITE) as tx:
                q = f'insert $t isa tenant, has tenant-id "{canary_tid}";'
                execute(
                    tx, q, QueryMode.WRITE,
                    component="ops_write_canary",
                    db_name=db_name,
                    address=address,
                    stage="canary_write",
                )
                tx.commit()
                _diag(
                    stage="canary_commit", action="commit",
                    tx_type="WRITE", db=db_name, query="commit", kind="ok",
                )

            # Step 2: Read (post-commit verification)
            print(f"Verifying canary {canary_tid}...")
            with driver.transaction(db_name, TransactionType.READ) as rtx:
                v_q = (
                    f'match $t isa tenant, has tenant-id "{canary_tid}"; '
                    f"select $t;"
                )
                res = list(rtx.query(v_q).resolve().as_concept_rows())
                _diag(
                    stage="canary_verify", action="read",
                    tx_type="READ", db=db_name, query=v_q,
                    kind="rows", row_count=len(res),
                )

                if not res:
                    print(f"  [FAIL] Canary {canary_tid} not persisted.")
                    failures += 1
                else:
                    print("  [PASS] Canary ok.")
        finally:
            driver.close()

    except Exception as e:
        import traceback
        print(f"  [ERROR] Canary failed: {e}")
        traceback.print_exc()
        _diag(
            stage="canary_error", action="exception",
            tx_type="WRITE", db=db_name, query="canary_flow",
            kind="exception",
            error_code=type(e).__name__, error_message=str(e),
        )
        failures += 1

    print(f"--- Canary Finished. Failures: {failures} ---")
    return 1 if failures > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
