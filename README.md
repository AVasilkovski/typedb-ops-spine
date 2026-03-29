# typedb-ops-spine

[![CI](https://github.com/AVasilkovski/typedb-ops-spine/actions/workflows/ci.yml/badge.svg)](https://github.com/AVasilkovski/typedb-ops-spine/actions)

TypeDB 3.8 schema deployment and migration toolkit.

`typedb-ops-spine` is a standalone TypeDB 3.8 schema deployment and migration toolkit with operator guardrails. It is built for engineers who keep schema in files and need a repeatable path for local runs or CI/CD, while staying intentionally narrow: a thin but tested ops layer around the official TypeDB driver.

Manual TypeDB schema rollout tends to fail in the same places: file ordering, migration sequencing, version stamping, parity checks, connection and TLS mistakes, smoke verification, and partial-failure recovery. This repo standardizes deterministic schema apply from files, ordered `NNN_*.tql` migrations, `schema_version` tracking, repo-vs-database parity checks, fail-fast config validation, smoke diagnostics, structured JSONL diagnostics, and explicit reconcile commands when SCHEMA work succeeds but schema-version stamping does not.

## Release status

There is **no PyPI release yet**.

The supported install paths today are:

- local checkout install for standalone/operator use
- pinned VCS install for consuming repos and CI

Do not rely on `pip install typedb-ops-spine` until a real package release exists.

Private beta notes live in [PRIVATE_BETA.md](PRIVATE_BETA.md). Release history lives in [CHANGELOG.md](CHANGELOG.md).

## What typedb-ops-spine is

`typedb-ops-spine` is a small standalone operations toolkit for managing TypeDB 3.8 schema rollout in a more controlled and repeatable way. The core surface is `ops-apply-schema`, `ops-migrate`, `ops-schema-health`, `ops-typedb-diag`, and `ops-tsv-extract`.

The point is not to wrap TypeDB in more abstraction. The point is to turn a set of manual schema-ops steps into a consistent workflow: deterministic schema apply, ordered `NNN_*.tql` migrations, `schema_version` tracking, fail-fast config validation, parity checks, smoke diagnostics, and explicit reconcile paths when schema work succeeds but version stamping does not.

Technically, the repo is strongest in its operator guardrails: deterministic file resolution, migration hygiene checks, explicit ordinal tracking, fail-fast address/TLS/config validation, query-answer materialization barriers, structured JSONL diagnostics, and recovery commands for split SCHEMA/stamp states. It is a thin but tested layer around the official TypeDB driver, not a new database product or runtime platform.

## What it is / what it is not

It is:

- a TypeDB 3.8 schema deployment and migration toolkit
- an operator-focused and CI/CD-friendly layer around the official TypeDB driver
- a way to make schema rollout more repeatable, diagnosable, and auditable

It is not:

- a TypeDB 2.x -> 3.x migration tool
- an ORM or query builder
- a GUI schema tool
- a runtime platform
- a full observability product
- a rollback system
- a full automatic schema-diff planner

## Who it is for

`typedb-ops-spine` is for:

- engineers and operators managing TypeDB schema through scripts or CI/CD
- teams keeping TypeDB schema in version-controlled `.tql` files
- workflows where deterministic rollout, parity checks, and diagnostics matter

It is not aimed at:

- local-only prototyping with no deploy process
- application developers looking for a runtime abstraction over TypeDB
- GUI-first schema editing workflows

## Why not just do this manually?

You can do these steps manually, but then correctness depends on operator discipline around:

- stable schema file ordering
- migration ordering and gap hygiene
- `schema_version` stamping
- repo-vs-database parity checks
- connection and TLS sanity
- post-failure recovery

`typedb-ops-spine` packages those concerns into a small, tested toolkit so the deploy path is easier to reproduce in local runs and CI/CD.

## Runbooks and proof

- [PRIVATE_BETA.md](PRIVATE_BETA.md): scope, supported install paths, tested versions, and current limitations
- [CHANGELOG.md](CHANGELOG.md): release history
- [docs/LOCAL_DEMO.md](docs/LOCAL_DEMO.md): verified local demo path with the bundled example
- [docs/FAILURE_RECOVERY.md](docs/FAILURE_RECOVERY.md): recovery paths for common operator failure modes
- [docs/PROOF.md](docs/PROOF.md): what the repo, tests, and local demo prove today

## Recent operator-facing changes

- `ops-schema-health` now exits non-zero on missing or invalid connection config instead of treating invalid config like a skip.

## Standalone quickstart

This is the supported copy-paste path from a fresh checkout. It assumes you already have a query-ready TypeDB 3.8 instance. For the verified Windows PowerShell local demo, see [docs/LOCAL_DEMO.md](docs/LOCAL_DEMO.md).

### 1. Clone and install

```bash
git clone https://github.com/AVasilkovski/typedb-ops-spine.git
cd typedb-ops-spine
python -m pip install .
```

### 2. Set connection variables

If you do not set these explicitly, the local defaults are `localhost:1729` with `admin` / `password`.

```bash
export TYPEDB_ADDRESS=localhost:1729
export TYPEDB_USERNAME=admin
export TYPEDB_PASSWORD=password
export TYPEDB_DATABASE=ops_spine_demo
export TYPEDB_TLS=false
```

### 3. Apply the bundled example schema

```bash
ops-apply-schema \
  --schema examples/minimal_project/schema.tql \
  --database "$TYPEDB_DATABASE" \
  --auto-migrate-redeclarations
```

### 4. Stamp to the bundled migration head

```bash
ops-apply-schema \
  --schema examples/minimal_project/schema.tql \
  --database "$TYPEDB_DATABASE" \
  --migrations-dir examples/minimal_project/migrations \
  --stamp-schema-version-head
```

### 5. Verify parity and run a smoke query

```bash
ops-schema-health \
  --migrations-dir examples/minimal_project/migrations \
  --database "$TYPEDB_DATABASE"

ops-typedb-diag \
  --database "$TYPEDB_DATABASE" \
  --require-db \
  --smoke-query 'match $v isa schema_version, has ordinal $o; select $o; limit 1;'
```

### 6. Run ordered migrations when you have a migrations directory

```bash
ops-migrate \
  --migrations-dir examples/minimal_project/migrations \
  --database "$TYPEDB_DATABASE"
```

## Install from another repo or CI

For external projects, pin an immutable commit or tag:

```bash
python -m pip install \
  "typedb-ops-spine @ git+https://github.com/AVasilkovski/typedb-ops-spine.git@<pinned-commit-or-tag>"
```

One minimal GitHub Actions install step looks like this:

```yaml
- name: Install typedb-ops-spine
  run: |
    python -m pip install --upgrade pip
    python -m pip install "typedb-ops-spine @ git+https://github.com/AVasilkovski/typedb-ops-spine.git@${TYPEDB_OPS_SPINE_REF}"
```

The full example workflow is in [examples/minimal_project/ci_example.yml](examples/minimal_project/ci_example.yml).

## Core tools

| Command | Purpose |
| --- | --- |
| `ops-apply-schema` | Deterministic schema apply, optional guarded scrub, optional head stamping |
| `ops-migrate` | Ordinal-based schema migrations with gap detection |
| `ops-schema-health` | Repo ordinal vs database ordinal parity check |
| `ops-typedb-diag` | Connectivity, database presence, and optional smoke query verification |
| `ops-tsv-extract` | TSV extraction for emitted diagnostics |

## Optional extra invariant hook

`ops-schema-health` supports an optional caller-supplied invariant hook:

```bash
ops-schema-health \
  --migrations-dir migrations \
  --database my_db \
  --extra-invariant package.module:function
```

Use this when a consuming repo wants to add one extra health invariant after ordinal parity without forking the generic package.

## Secondary / example-profile tools

The package also ships `ops-write-canary` and `ops-min-write-probe`. These are bundled example-profile diagnostics for the tenant/run-capsule schema under [examples/minimal_project](examples/minimal_project); they are not the primary onboarding path for generic users.

## Recovery after stamp failure

If authoritative schema apply or a migration reports that `schema_version` recording failed after SCHEMA work succeeded, do not rerun the full command blindly. Use the explicit stamp-only reconcile path instead.

Authoritative apply reconcile:

```bash
ops-apply-schema \
  --database my_db \
  --migrations-dir migrations \
  --reconcile-schema-version-head
```

Migration ordinal reconcile:

```bash
ops-migrate \
  --database my_db \
  --migrations-dir migrations \
  --reconcile-ordinal 7
```

See [docs/FAILURE_RECOVERY.md](docs/FAILURE_RECOVERY.md) for worked examples and failure modes.

## Connection settings

Supported address forms:

- `localhost:1729`
- `localhost`
- `https://cloud.example.com:443`
- `https://cloud.example.com` together with `TYPEDB_PORT=443`

Validation is fail-fast:

- `TYPEDB_TLS=true` with a non-HTTPS address is rejected immediately
- `TYPEDB_TLS=false` with an HTTPS address is rejected immediately
- unsupported schemes such as `http://...` are rejected immediately
- `TYPEDB_ROOT_CA_PATH` is only valid with HTTPS/TLS connections

This avoids wasting the full retry budget on deterministic configuration errors.

`ops-typedb-diag` and `run_smoke_diagnostics()` validate address, TLS, and CA settings before any retry loop. Deterministic config errors raise `TypeDBConfigError` immediately; only network or service-readiness failures consume retries.

## Library usage

```python
from typedb.driver import TransactionType
from typedb_ops_spine import QueryMode, connect_with_retries, execute

driver = connect_with_retries("localhost:1729", "admin", "password")
try:
    with driver.transaction("ops_spine_demo", TransactionType.READ) as tx:
        rows = execute(
            tx,
            "match $v isa schema_version, has ordinal $o; select $o;",
            QueryMode.READ_ROWS,
            component="demo",
            db_name="ops_spine_demo",
        )
        print(rows)
finally:
    driver.close()
```

## Development

```bash
git clone https://github.com/AVasilkovski/typedb-ops-spine.git
cd typedb-ops-spine
python -m pip install -e ".[dev]"

python -m pytest tests/unit/ -q
python -m pytest tests/integration/ -q
```

Integration tests use the same protocol-level readiness path as operators. If TypeDB is not query-ready with the configured address, auth, and TLS settings, they skip.

## License

MIT. See [LICENSE](LICENSE).
