"""
CLI entry point: ops-min-write-probe — The Arbiter

PROVES: Which write shapes and materialization patterns persist in the canonical schema.
Operates as a fail-slow diagnostic tool using isolated databases.

5 Variants:
  1. tenant_only_insert_ok       — simple insert
  2. tenant_only_insert_select   — insert with select projection
  3. seed_like_multistatement    — entity + capsule + relation in one insert
  4. split_step1_tenants         — insert tenant (anchor for step 2)
  5. split_step2_capsule_relation — match-anchored insert

Fail-slow: run all variants; return non-zero if any variant fails.
"""

from __future__ import annotations

import argparse
import os
import sys
import traceback
import uuid
from datetime import datetime, timezone

from typedb_ops_spine.diagnostics import emit_typedb_diag
from typedb_ops_spine.exec import _get_error_code


def _diag(
    *, stage: str, action: str, tx_type: str, db: str,
    query: str, kind: str, **extra,
) -> None:
    emit_typedb_diag(
        component="ops_min_write_probe",
        db_name=db,
        tx_type=tx_type,
        action=action,
        query=query,
        answer_kind=kind,
        stage=stage,
        **extra,
    )


def _typedb_datetime_now_utc_literal(timespec: str = "microseconds") -> str:
    return (
        datetime.now(timezone.utc)
        .replace(tzinfo=None)
        .isoformat(timespec=timespec)
    )


def _run_bootstrap(
    db_name: str,
    address: str,
    username: str,
    password: str,
    schema_file: str,
    migrations_dir: str,
) -> bool:
    """Bootstrap an isolated database using library functions (no subprocess)."""
    from pathlib import Path

    from typedb_ops_spine.migrate import run_migrations
    from typedb_ops_spine.readiness import connect_with_retries, ensure_database
    from typedb_ops_spine.schema_apply import apply_schema, resolve_schema_files

    print(f"--- Bootstrapping isolated DB: {db_name} ---")
    tls = os.getenv("TYPEDB_TLS", "false").lower() == "true"
    ca_path = os.getenv("TYPEDB_ROOT_CA_PATH") or None

    try:
        driver = connect_with_retries(
            address, username, password, tls, ca_path, retries=10, sleep_s=1.0,
        )
        try:
            ensure_database(driver, db_name)

            # Apply schema
            schema_paths = resolve_schema_files([schema_file])
            print(f"Applying schema: {schema_paths}")
            apply_schema(driver, db_name, schema_paths)

            # Apply migrations
            mig_dir = Path(migrations_dir)
            if mig_dir.is_dir():
                print(f"Applying migrations from: {mig_dir}")
                run_migrations(driver, db_name, mig_dir)
            else:
                print(f"No migrations directory: {mig_dir}")
        finally:
            driver.close()
        return True
    except Exception as e:
        print(f"Bootstrap failed: {e}")
        traceback.print_exc()
        return False


def main() -> int:
    p = argparse.ArgumentParser(
        prog="ops-min-write-probe",
        description="TypeDB Arbiter — fail-slow write shape probe.",
    )
    p.add_argument(
        "--database-prefix",
        default="probe_db",
        help="Prefix for the isolated probe database name.",
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
        "--schema",
        default=os.getenv("TYPEDB_SCHEMA", "schema.tql"),
        help="Schema file for bootstrap.",
    )
    p.add_argument(
        "--migrations-dir",
        default=os.getenv("TYPEDB_MIGRATIONS_DIR", "migrations"),
        help="Migrations directory for bootstrap.",
    )
    args = p.parse_args()

    os.makedirs("ci_artifacts", exist_ok=True)

    import typedb
    from typedb.driver import Credentials, DriverOptions, TransactionType, TypeDB

    address = args.address if args.address else f"{args.host}:{args.port}"
    use_db = f"{args.database_prefix}_{uuid.uuid4().hex[:6]}"
    driver_version = getattr(typedb, "__version__", "unknown")

    print("--- TypeDB Arbiter Probe Start ---")
    print(f"  Isolated DB: {use_db}")
    print(f"  Address:     {address}")

    _diag(
        stage="init_probe_identity", action="identity",
        tx_type="INIT", db=use_db, query=f"addr={address}",
        kind="ok", driver_version=driver_version,
        python_version=sys.version.split()[0], address=address,
    )

    if not _run_bootstrap(
        use_db, address, args.username, args.password,
        args.schema, args.migrations_dir,
    ):
        print("CRITICAL: Bootstrap failed. Aborting probe.")
        return 1

    tls = os.getenv("TYPEDB_TLS", "false").lower() == "true"
    ca_path = os.getenv("TYPEDB_ROOT_CA_PATH") or None
    creds = Credentials(args.username, args.password)
    opts = DriverOptions(is_tls_enabled=tls, tls_root_ca_path=ca_path)

    failures = 0
    t_lit = _typedb_datetime_now_utc_literal("microseconds")

    with TypeDB.driver(address, creds, opts) as driver:

        def run_variant(label: str, write_q: str, verify_tid: str) -> None:
            nonlocal failures
            print(f"Testing {label}...")
            v_fail = False
            try:
                with driver.transaction(use_db, TransactionType.WRITE) as tx:
                    ans = tx.query(write_q).resolve()

                    is_ok = (
                        bool(ans.is_ok())
                        if hasattr(ans, "is_ok") else False
                    )
                    is_rows = (
                        bool(ans.is_concept_rows())
                        if hasattr(ans, "is_concept_rows") else False
                    )

                    as_ok_attempted = False
                    as_ok_succeeded = False

                    row_count = 0
                    if is_rows:
                        row_count = len(list(ans.as_concept_rows()))

                    # Barrier: only attempt as_ok if no rows were materialized
                    if not is_rows and hasattr(ans, "as_ok"):
                        as_ok_attempted = True
                        try:
                            ans.as_ok()
                            as_ok_succeeded = True
                        except Exception as be:
                            print(f"  [BARRIER_FAIL] {label}: {be}")

                    _diag(
                        stage=f"{label}_write", action="execute",
                        tx_type="WRITE", db=use_db, query=write_q,
                        kind="rows" if is_rows else "ok",
                        is_ok=is_ok, is_rows=is_rows, row_count=row_count,
                        as_ok_attempted=as_ok_attempted,
                        as_ok_succeeded=as_ok_succeeded,
                    )

                    _diag(
                        stage=f"{label}_commit_start", action="commit",
                        tx_type="WRITE", db=use_db, query="commit", kind="ok",
                    )
                    tx.commit()
                    _diag(
                        stage=f"{label}_commit_ok", action="commit",
                        tx_type="WRITE", db=use_db, query="commit", kind="ok",
                    )

                # Verify Q0 and Q2
                with driver.transaction(use_db, TransactionType.READ) as rtx:
                    q0 = "match $t isa tenant; select $t;"
                    res0 = list(rtx.query(q0).resolve().as_concept_rows())
                    _diag(
                        stage=f"{label}_verify_q0", action="read",
                        tx_type="READ", db=use_db, query=q0,
                        kind="rows", row_count=len(res0),
                    )

                    q2 = (
                        f'match $t isa tenant, has tenant-id "{verify_tid}"; '
                        f"select $t;"
                    )
                    res2 = list(rtx.query(q2).resolve().as_concept_rows())
                    _diag(
                        stage=f"{label}_verify_q2", action="read",
                        tx_type="READ", db=use_db, query=q2,
                        kind="rows", row_count=len(res2),
                    )

                    if not res2:
                        print(f"  [FAIL] {label} not persisted.")
                        v_fail = True
                    else:
                        print(f"  [PASS] {label} ok.")
            except Exception as e:
                print(f"  [ERROR] {label} failed: {e}")
                _diag(
                    stage=f"{label}_error", action="exception",
                    tx_type="WRITE", db=use_db, query=write_q,
                    kind="exception",
                    error_code=_get_error_code(e), error_message=str(e),
                )
                v_fail = True
            if v_fail:
                failures += 1

        # 1) probe_tenant_only_insert_ok
        v1_tid = f"v1-ok-{uuid.uuid4().hex[:6]}"
        run_variant(
            "probe_tenant_only_insert_ok",
            f'insert $t isa tenant, has tenant-id "{v1_tid}";',
            v1_tid,
        )

        # 2) probe_tenant_only_insert_select
        v2_tid = f"v2-sel-{uuid.uuid4().hex[:6]}"
        run_variant(
            "probe_tenant_only_insert_select",
            f'insert $t isa tenant, has tenant-id "{v2_tid}"; select $t;',
            v2_tid,
        )

        # 3) probe_seed_like_multistatement
        v3_tid = f"v3-multi-{uuid.uuid4().hex[:6]}"
        v3_cid = f"v3-cap-{uuid.uuid4().hex[:6]}"
        v3_q = (
            f'insert '
            f'$t isa tenant, has tenant-id "{v3_tid}"; '
            f'$c isa run-capsule, '
            f'has capsule-id "{v3_cid}", '
            f'has tenant-id "{v3_tid}", '
            f'has session-id "sess-v3", '
            f'has created-at {t_lit}, '
            f'has query-hash "qh-v3", '
            f'has scope-lock-id "slid-v3", '
            f'has intent-id "iid-v3", '
            f'has proposal-id "pid-v3"; '
            f'(owner: $t, owned: $c) isa tenant-ownership;'
        )
        run_variant("probe_seed_like_multistatement", v3_q, v3_tid)

        # 4) probe_split_seed_step1_tenants
        v4_tid = f"v4-split-{uuid.uuid4().hex[:6]}"
        run_variant(
            "probe_split_seed_step1_tenants",
            f'insert $t isa tenant, has tenant-id "{v4_tid}";',
            v4_tid,
        )

        # 5) probe_split_seed_step2_capsule_relation
        v5_cid = f"v5-cap-{uuid.uuid4().hex[:6]}"
        v5_q = (
            f'match $t isa tenant, has tenant-id "{v4_tid}"; '
            f'insert $c isa run-capsule, '
            f'has capsule-id "{v5_cid}", '
            f'has tenant-id "{v4_tid}", '
            f'has session-id "sess-v5", '
            f'has created-at {t_lit}, '
            f'has query-hash "qh-v5", '
            f'has scope-lock-id "slid-v5", '
            f'has intent-id "iid-v5", '
            f'has proposal-id "pid-v5"; '
            f'(owner: $t, owned: $c) isa tenant-ownership;'
        )
        run_variant(
            "probe_split_seed_step2_capsule_relation", v5_q, v4_tid,
        )

    print(f"--- Arbiter Probe Finished. Total Failures: {failures} ---")
    return 1 if failures > 0 else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print(f"Probe FATAL: {e}")
        traceback.print_exc()
        sys.exit(1)
