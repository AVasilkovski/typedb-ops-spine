"""
CLI entry point: ops-write-canary

Write→commit→read durability check with identity logging and
decision-grade diagnostics emission. Fail-slow.

Default queries target the packaged tenant-based example profile. The scoped
mode extends that to the tenant/run-capsule relation shapes used by
SuperHyperion-compatible schemas.
"""

from __future__ import annotations

import argparse
import os
import sys
import uuid

from typedb_ops_spine.diagnostics import emit_typedb_diag
from typedb_ops_spine.exec import QueryMode, execute


def _env_tls_override() -> bool | None:
    raw = os.getenv("TYPEDB_TLS")
    if raw is None:
        return None
    return raw.lower() == "true"


def _diag(
    *, stage: str, action: str, tx_type: str, db: str, query: str, kind: str,
    tenant_scoped: bool = False, target_tenant_id: str = "",
    ownership_rel: str = "", canary_target: str = "tenant", **extra
) -> None:
    emit_typedb_diag(
        component="ops_write_canary",
        db_name=db,
        tx_type=tx_type,
        action=action,
        query=query,
        answer_kind=kind,
        stage=stage,
        tenant_scoped=tenant_scoped,
        target_tenant_id=target_tenant_id,
        ownership_rel=ownership_rel,
        canary_target=canary_target,
        **extra,
    )


def main() -> int:
    p = argparse.ArgumentParser(
        prog="ops-write-canary",
        description="TypeDB write/read commit durability canary for tenant-based schemas.",
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
    p.add_argument("--address", default=os.getenv("TYPEDB_ADDRESS"))
    p.add_argument("--host", default=os.getenv("TYPEDB_HOST", "localhost"))
    p.add_argument("--port", default=os.getenv("TYPEDB_PORT", "1729"))
    p.add_argument(
        "--username", default=os.getenv("TYPEDB_USERNAME", "admin"),
    )
    p.add_argument(
        "--tenant-id",
        help="Optional tenant ID to write a scoped canary (SuperHyperion-profile).",
    )
    p.add_argument(
        "--ownership-rel",
        choices=["auto", "tenant-owns-capsule", "tenant-ownership"],
        default="auto",
        help="Which relation to use for scoped canary (default: auto fallback).",
    )
    p.add_argument(
        "--password", default=os.getenv("TYPEDB_PASSWORD", "password"),
    )
    p.add_argument(
        "--stamp-schema-version-head",
        action="store_true",
        help="Fast-forward schema_version to head migration ordinal after authoritative apply."
    )
    args = p.parse_args()

    os.makedirs("ci_artifacts", exist_ok=True)

    import importlib.metadata as md

    import typedb
    from typedb.driver import TransactionType

    from typedb_ops_spine.migrate import run_migrations
    from typedb_ops_spine.readiness import (
        connect_with_retries,
        ensure_database,
        infer_tls_enabled,
        resolve_connection_address,
    )
    from typedb_ops_spine.schema_apply import (
        apply_schema,
        get_current_schema_version,
        head_migration_ordinal,
        resolve_schema_files,
        stamp_schema_version_head,
    )

    db_name = args.database
    address = resolve_connection_address(args.address, args.host, args.port)
    tls = _env_tls_override()
    resolved_tls = infer_tls_enabled(address, tls)
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
            tls=resolved_tls, ca_path=ca_path, retries=10,
        )
        try:
            ensure_database(driver, db_name)

            # Step 0.1: Optional Bootstrap
            if args.schema:
                schema_paths = resolve_schema_files([args.schema])
                print(f"Bootstrapping schema: {schema_paths}")
                apply_schema(driver, db_name, schema_paths)
                from pathlib import Path
                if args.stamp_schema_version_head and args.migrations_dir:
                    h_ord = head_migration_ordinal(Path(args.migrations_dir))
                    if h_ord > 0:
                        stamp_schema_version_head(driver, db_name, h_ord)
                        v_ord = get_current_schema_version(driver, db_name)
                        print(f"Verified schema_version ordinal: {v_ord}")

            if args.migrations_dir:
                from pathlib import Path
                print(f"Bootstrapping migrations: {args.migrations_dir}")
                run_migrations(driver, db_name, Path(args.migrations_dir))

            # Step 1: Write
            is_scoped = bool(args.tenant_id)
            tenant_val = args.tenant_id if is_scoped else ""
            used_rel = ""
            target_kind = "capsule" if is_scoped else "tenant"

            canary_id = f"canary-{uuid.uuid4().hex[:6]}"
            print(f"Writing canary {canary_id}...")

            if is_scoped:
                # 1. Verify exact tenant exists
                tenant_q = f'match $t isa tenant, has tenant-id "{tenant_val}"; select $t;'
                with driver.transaction(db_name, TransactionType.READ) as rtx:
                    print(f"Verifying target tenant {tenant_val} exists...")
                    t_ans = execute(
                        rtx, tenant_q, QueryMode.READ_ROWS,
                        component="ops_write_canary", db_name=db_name, address=address, stage="verify_tenant_exists",
                    )
                    t_rows = list(t_ans) if t_ans else []
                    if not t_rows:
                        raise ValueError(f"Strict canary failed: Target tenant '{tenant_val}' does not exist.")

            if not is_scoped:
                with driver.transaction(db_name, TransactionType.WRITE) as tx:
                    q = f'insert $t isa tenant, has tenant-id "{canary_id}";'
                    execute(
                        tx, q, QueryMode.WRITE,
                        component="ops_write_canary", db_name=db_name, address=address, stage="canary_write",
                    )
                    tx.commit()
                    _diag(
                        stage="canary_commit", action="commit",
                        tx_type="WRITE", db=db_name, query="commit", kind="ok",
                        tenant_scoped=is_scoped, target_tenant_id=tenant_val, ownership_rel=used_rel, canary_target=target_kind,
                    )
            else:
                from datetime import datetime, timezone
                t_lit = datetime.now(timezone.utc).replace(tzinfo=None).isoformat(timespec="microseconds")

                rels_to_try = (
                    ["tenant-owns-capsule", "tenant-ownership"]
                    if args.ownership_rel == "auto"
                    else [args.ownership_rel]
                )

                success_write = False
                last_err = None
                for rel in rels_to_try:
                    print(f"Trying scoped write with relation: {rel}")
                    if rel == "tenant-owns-capsule":
                        rel_clause = "(tenant: $t, capsule: $c) isa tenant-owns-capsule;"
                    else:
                        rel_clause = "(owner: $t, owned: $c) isa tenant-ownership;"

                    q = (
                        f'match $t isa tenant, has tenant-id "{tenant_val}"; '
                        f'insert $c isa run-capsule, '
                        f'has capsule-id "{canary_id}", '
                        f'has tenant-id "{tenant_val}", '
                        f'has session-id "canary-sess", '
                        f'has created-at {t_lit}, '
                        f'has query-hash "canary-qh", '
                        f'has scope-lock-id "canary-sl", '
                        f'has intent-id "canary-intent", '
                        f'has proposal-id "canary-prop"; '
                        f'{rel_clause}'
                    )
                    try:
                        with driver.transaction(db_name, TransactionType.WRITE) as tx:
                            execute(
                                tx, q, QueryMode.WRITE,
                                component="ops_write_canary", db_name=db_name, address=address, stage=f"canary_write_{rel}",
                            )
                            tx.commit()
                            _diag(
                                stage="canary_commit", action="commit",
                                tx_type="WRITE", db=db_name, query="commit", kind="ok",
                                tenant_scoped=is_scoped, target_tenant_id=tenant_val, ownership_rel=rel, canary_target=target_kind,
                            )
                        success_write = True
                        used_rel = rel
                        break
                    except Exception as e:
                        last_err = e
                        print(f"  [WARN] Fallback: {rel} failed. {getattr(e, 'message', str(e))}")
                        _diag(
                            stage="canary_rel_fallback", action="exception",
                            tx_type="WRITE", db=db_name, query=q, kind="exception",
                            tenant_scoped=is_scoped, target_tenant_id=tenant_val, ownership_rel=rel, canary_target=target_kind,
                            error_code=type(e).__name__, error_message=str(e),
                        )

                if not success_write:
                    raise RuntimeError(f"Strict canary failed: All relation variants failed. Last error: {last_err}")

            # Step 2: Read (post-commit verification)
            import time
            print(f"Verifying canary {canary_id} (with 1s pause)...")
            time.sleep(1.0)

            if not is_scoped:
                v_q = (
                    f'match $t isa tenant, has tenant-id "{canary_id}"; '
                    f"select $t;"
                )
            else:
                if used_rel == "tenant-owns-capsule":
                    v_rel_clause = "(tenant: $t, capsule: $c) isa tenant-owns-capsule;"
                else:
                    v_rel_clause = "(owner: $t, owned: $c) isa tenant-ownership;"

                v_q = (
                    f'match '
                    f'$t isa tenant, has tenant-id "{tenant_val}"; '
                    f'$c isa run-capsule, has capsule-id "{canary_id}"; '
                    f'{v_rel_clause} '
                    f'select $c;'
                )

            success = False
            for attempt in range(1, 4):
                with driver.transaction(db_name, TransactionType.READ) as rtx:
                    res = execute(
                        rtx, v_q, QueryMode.READ_ROWS,
                        component="ops_write_canary",
                        db_name=db_name,
                        address=address,
                        stage=f"canary_verify_attempt_{attempt}",
                    )

                    if res:
                        print(f"  [PASS] Canary found on attempt {attempt}.")
                        success = True
                        break
                    else:
                        print(f"  [WAIT] Canary not found on attempt {attempt}. Retrying...")
                        time.sleep(1.0)

            if not success:
                print(f"  [FAIL] Canary {canary_id} not persisted after 3 attempts.")
                failures += 1
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
            tenant_scoped=locals().get("is_scoped", False),
            target_tenant_id=locals().get("tenant_val", ""),
            ownership_rel=locals().get("used_rel", ""),
            canary_target=locals().get("target_kind", "tenant"),
            error_code=type(e).__name__, error_message=str(e),
        )
        failures += 1

    print(f"--- Canary Finished. Failures: {failures} ---")
    return 1 if failures > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
