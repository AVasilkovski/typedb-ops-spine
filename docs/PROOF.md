# Proof

## What this repo proves today

`typedb-ops-spine` currently proves a standalone TypeDB 3.8 schema deployment
and migration path built around:

- authoritative schema apply from checked-in `.tql` files
- `schema_version` head stamping and ordinal-based migration state
- repo/database parity checks
- protocol-level readiness and smoke diagnostics
- explicit reconcile commands when schema-version stamping fails after SCHEMA work

Repo evidence:

- [README.md](../README.md)
- [examples/minimal_project/README.md](../examples/minimal_project/README.md)
- [examples/minimal_project/schema.tql](../examples/minimal_project/schema.tql)
- [examples/minimal_project/migrations/001_bootstrap.tql](../examples/minimal_project/migrations/001_bootstrap.tql)
- [tests/unit/test_schema_apply_recovery.py](../tests/unit/test_schema_apply_recovery.py)
- [tests/unit/test_migrate.py](../tests/unit/test_migrate.py)
- [tests/unit/test_schema_health.py](../tests/unit/test_schema_health.py)
- [tests/unit/test_typedb_diag.py](../tests/unit/test_typedb_diag.py)
- [tests/unit/test_readiness.py](../tests/unit/test_readiness.py)
- [tests/integration/test_apply_schema_and_migrate.py](../tests/integration/test_apply_schema_and_migrate.py)

## Verified local demo

Verified locally on Windows PowerShell with Docker, `typedb/typedb:3.8.0`,
and the repo-local Python 3.11 virtualenv using the bundled example workflow
from [LOCAL_DEMO.md](./LOCAL_DEMO.md):

- local TypeDB container on `localhost:11729` was reachable
- authentication succeeded with `admin` / `password`
- authoritative schema apply succeeded
- schema-version head stamping succeeded
- schema health parity succeeded
- smoke diagnostics query against `schema_version` succeeded
- drift detection fail-path succeeded with a non-zero exit and `FAIL: drift detected`

This local proof uses the same shipped example assets and CLI forms documented
in the repo. It does not add any extra features or hidden bootstrap layer.

## Current test signal

Implementation-time reruns:

- `python -m pytest tests/unit/ -q`
- `python -m pytest tests/integration/ -q`

Current status:

- `python -m pytest tests/unit/ -q` → `111 passed`
- `python -m pytest tests/integration/ -q` → `9 skipped`

Integration remains environment-dependent in CI/local runs and skips when no
live, query-ready TypeDB instance is configured.

The test suite covers the core execution model directly:

- answer-kind/materialization barrier handling
- fail-fast readiness validation
- schema apply and migration recovery behavior
- schema health parity and extra invariant hook
- smoke diagnostics and docs contract checks

## Diagnostics artifacts

Successful and failed operations emit machine-readable artifacts under
`ci_artifacts/`:

- `apply_schema_diagnostics.jsonl`
- `migrate_diagnostics.jsonl`
- `schema_health_diagnostics.jsonl`
- `typedb_diag.jsonl`

These files are part of the product proof because they show the toolkit is not
just executing commands; it also leaves an operator-readable audit trail for
apply, migrate, parity, and smoke-diagnostic runs.
