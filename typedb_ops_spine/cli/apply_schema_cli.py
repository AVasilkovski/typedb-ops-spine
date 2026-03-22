"""
CLI entry point: ops-apply-schema

Thin argparse wrapper over typedb_ops_spine.schema_apply and readiness.
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
        "--undefine-owns",
        action="append",
        default=[],
        help=(
            "Run guarded migration before schema apply. Repeatable format: "
            "<entity>:<attribute>."
        ),
    )
    p.add_argument(
        "--undefine-plays",
        action="append",
        default=[],
        help=(
            "Run guarded role-play migration before schema apply. Repeatable format: "
            "<type>:<relation:role>."
        ),
    )
    p.add_argument(
        "--auto-migrate-redeclarations",
        action="store_true",
        help="Proactively scrub inherited owns/plays redeclarations from the canonical schema.",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned actions without executing.",
    )
    p.add_argument(
        "--scrub-only",
        action="store_true",
        help="Only run guarded schema scrub operations; do not apply canonical schema files.",
    )
    p.add_argument(
        "--migrations-dir",
        default=None,
        help="Directory containing NNN_*.tql migration files (used for stamping).",
    )
    p.add_argument(
        "--stamp-schema-version-head",
        action="store_true",
        help="Fast-forward schema_version to head migration ordinal after authoritative apply.",
    )
    args = p.parse_args()

    from typedb_ops_spine.readiness import (
        connect_with_retries,
        ensure_database,
        infer_tls_enabled,
        resolve_connection_address,
    )
    from typedb_ops_spine.schema_apply import (
        apply_schema,
        head_migration_ordinal,
        migrate_undefine_owns,
        migrate_undefine_plays,
        parse_canonical_caps,
        plan_auto_migrations,
        resolve_schema_files,
        stamp_schema_version_head,
    )

    tls = _env_tls_override()
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

    auto_owns_specs: list[tuple[str, str]] = []
    auto_plays_specs: list[tuple[str, str]] = []
    if args.auto_migrate_redeclarations:
        schema_text = "\n\n".join(
            path.read_text(encoding="utf-8") for path in schema_paths
        )
        parent_of, owns_of, plays_of = parse_canonical_caps(schema_text)
        auto_owns_specs, auto_plays_specs = plan_auto_migrations(
            parent_of,
            owns_of,
            plays_of,
        )
        print(
            "[ops-apply-schema] Auto-scrub planned "
            f"owns={len(auto_owns_specs)} plays={len(auto_plays_specs)}"
        )

    if args.dry_run:
        if auto_owns_specs or auto_plays_specs or args.undefine_owns or args.undefine_plays:
            print("[ops-apply-schema] Planned guarded schema scrubs:")
            for type_label, attribute in auto_owns_specs:
                print(f"  - auto undefine owns {attribute} from {type_label}")
            for type_label, scoped_role in auto_plays_specs:
                print(f"  - auto undefine plays {scoped_role} from {type_label}")
            for spec in args.undefine_owns:
                print(f"  - manual undefine owns {spec}")
            for spec in args.undefine_plays:
                print(f"  - manual undefine plays {spec}")
        if args.scrub_only:
            print("[ops-apply-schema] dry-run: scrub-only mode; canonical schema apply would be skipped")
        else:
            print("[ops-apply-schema] dry-run: canonical schema apply would run")
        return 0

    address = resolve_connection_address(args.address, args.host, args.port)
    resolved_tls = infer_tls_enabled(address, tls)
    print(f"[ops-apply-schema] Connecting to {address} tls={resolved_tls}")

    driver = connect_with_retries(
        address,
        args.username,
        args.password,
        resolved_tls,
        ca_path,
    )
    try:
        if args.recreate:
            if driver.databases.contains(args.database):
                driver.databases.get(args.database).delete()
                print(f"[ops-apply-schema] Database deleted: {args.database}")

        ensure_database(driver, args.database)

        if auto_owns_specs:
            migrate_undefine_owns(
                driver,
                args.database,
                [f"{type_label}:{attribute}" for type_label, attribute in auto_owns_specs],
            )
        if auto_plays_specs:
            migrate_undefine_plays(
                driver,
                args.database,
                [f"{type_label}:{scoped_role}" for type_label, scoped_role in auto_plays_specs],
            )

        if args.undefine_plays:
            migrate_undefine_plays(driver, args.database, args.undefine_plays)
        if args.undefine_owns:
            migrate_undefine_owns(driver, args.database, args.undefine_owns)

        if args.scrub_only:
            print("[ops-apply-schema] scrub-only: skipping canonical schema apply")
        else:
            apply_schema(driver, args.database, schema_paths)

            if args.stamp_schema_version_head and args.migrations_dir:
                from pathlib import Path

                mig_dir = Path(args.migrations_dir)
                head_ord = head_migration_ordinal(mig_dir)
                if head_ord > 0:
                    stamp_schema_version_head(driver, args.database, head_ord)
        print("[ops-apply-schema] Done.")
    finally:
        driver.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
