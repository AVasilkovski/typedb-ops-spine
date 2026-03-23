"""
CLI entry point: ops-schema-health

Checks schema version drift between repo migrations and database state.
"""

from __future__ import annotations

import argparse
import importlib
import os
import sys
from typing import Any, Callable


def _env_tls_override() -> bool | None:
    raw = os.getenv("TYPEDB_TLS")
    if raw is None:
        return None
    return raw.lower() == "true"


def _load_extra_invariant(spec: str) -> Callable[[Any, str], object]:
    if spec.count(":") != 1:
        raise ValueError(
            "Invalid --extra-invariant value. Expected format 'module:function'."
        )

    module_name, func_name = spec.split(":", maxsplit=1)
    if not module_name or not func_name:
        raise ValueError(
            "Invalid --extra-invariant value. Expected format 'module:function'."
        )

    try:
        module = importlib.import_module(module_name)
    except ImportError as exc:
        raise ValueError(
            f"Failed to import extra invariant module '{module_name}': {exc}"
        ) from exc

    try:
        target = getattr(module, func_name)
    except AttributeError as exc:
        raise ValueError(
            f"Extra invariant target not found: '{spec}'."
        ) from exc

    if not callable(target):
        raise ValueError(f"Extra invariant target '{spec}' is not callable.")

    return target


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
    p.add_argument(
        "--extra-invariant",
        default=None,
        help="Optional extra invariant hook to run after ordinal parity, format: module:function",
    )
    args = p.parse_args()

    from typedb_ops_spine.readiness import (
        TypeDBConfigError,
        connect_with_retries,
        resolve_connection_config,
    )
    from typedb_ops_spine.schema_health import run_health_checks

    extra_invariant = None
    extra_name = "extra_invariant"
    if args.extra_invariant:
        try:
            extra_invariant = _load_extra_invariant(args.extra_invariant)
            extra_name = args.extra_invariant
        except ValueError as e:
            print(f"[ops-schema-health] ERROR: {e}", file=sys.stderr)
            return 1

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
        print(f"[ops-schema-health] ERROR: {e}", file=sys.stderr)
        return 1

    print(f"[ops-schema-health] Connecting to {address} tls={resolved_tls}")
    driver = connect_with_retries(
        address,
        args.username,
        args.password,
        tls=resolved_tls,
        ca_path=ca_path,
    )
    try:
        report = run_health_checks(
            driver,
            args.database,
            args.migrations_dir,
            extra_invariant=extra_invariant,
            extra_name=extra_name,
        )
        print(f"[ops-schema-health] Repo ordinal: {report.repo_ordinal}")
        print(f"[ops-schema-health] DB ordinal:   {report.db_ordinal}")

        if report.extra_result is not None:
            result = report.extra_result
            if result.skipped:
                print(f"[ops-schema-health] Extra invariant '{result.name}': SKIP: ordinal drift")
            elif result.ok:
                print(f"[ops-schema-health] Extra invariant '{result.name}': PASS")
            elif result.message:
                print(f"[ops-schema-health] Extra invariant '{result.name}': FAIL: {result.message}")
            else:
                print(f"[ops-schema-health] Extra invariant '{result.name}': FAIL")

        if report.healthy:
            print("[ops-schema-health] PASS: parity OK")
            return 0
        if report.repo_ordinal != report.db_ordinal:
            print("[ops-schema-health] FAIL: drift detected")
        else:
            print("[ops-schema-health] FAIL: extra invariant failed")
        return 1
    except Exception as e:
        print(f"[ops-schema-health] ERROR: {e}", file=sys.stderr)
        return 1
    finally:
        driver.close()


if __name__ == "__main__":
    sys.exit(main())
