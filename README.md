# typedb-ops-spine

TypeDB 3.8 schema deployment and migration toolkit

Packaged TypeDB 3.8 schema deployment and migration toolkit with deterministic apply, ordered migrations, parity checks, readiness validation, diagnostics, and explicit reconcile guidance.

## Release status

There is no PyPI release yet.

The supported install paths today are:

- local checkout install for standalone/operator use
- pinned VCS install for consuming repos and CI

Do not rely on pip install typedb-ops-spine until a real package release exists.

Private beta notes live in [PRIVATE_BETA.md](PRIVATE_BETA.md).  
Release history lives in [CHANGELOG.md](CHANGELOG.md).

## What typedb-ops-spine is

typedb-ops-spine is a small standalone operations toolkit for managing TypeDB 3.8.x schema rollout in a more controlled and repeatable way. It is aimed at engineers who keep schema in files and want a safer path for applying schema, running ordered migrations, checking repo-vs-database parity, validating readiness, and collecting operator-facing diagnostics.

The point is not to wrap TypeDB in more abstraction. The point is to turn a set of manual schema-ops steps into a consistent workflow: deterministic schema apply, ordered NNN_*.tql migrations, schema_version tracking, fail-fast config validation, parity checks, smoke diagnostics, and explicit reconcile paths when schema work succeeds but version stamping does not.

Technically, the repo is strongest in its operator guardrails: deterministic file resolution, migration hygiene checks, explicit ordinal tracking, fail-fast address/TLS/config validation, query-answer materialization barriers, structured JSONL diagnostics, and recovery commands for split SCHEMA/stamp states. It is a thin but tested layer around the official TypeDB driver, not a new database product or runtime platform.

## What it is / what it is not

It is:

- a TypeDB 3.8 schema deployment and migration toolkit
- an operator/CI-focused layer around the official TypeDB driver
- a way to make schema rollout more repeatable, diagnosable, and auditable

It is not:

- a TypeDB 2.x → 3.x migration tool
- an ORM or query builder
- a GUI schema tool
- a runtime platform
- a full observability product
- a rollback system
- a fully automatic schema-diff planner

## Who it is for

typedb-ops-spine is for:

- engineers and operators managing TypeDB schema through scripts or CI/CD
- teams running TypeDB 3.8.x in staging or production-like environments
- workflows where deterministic rollout, parity checks, and diagnostics matter

It is not aimed at:

- local-only prototyping with no deploy process
- application developers looking for a runtime abstraction over TypeDB
- GUI-first schema editing workflows

## Why not just do this manually?

You can do these steps manually, but then correctness depends on operator discipline:

- stable file ordering
- migration ordering
- version stamping
- parity checks
- TLS/config sanity
- post-failure recovery

typedb-ops-spine packages those concerns into a small, tested toolkit so the deploy path is easier to reproduce in local runs and CI/CD.

## Runbooks and proof

- [PRIVATE_BETA.md](PRIVATE_BETA.md)
- [CHANGELOG.md](CHANGELOG.md)
- [docs/LOCAL_DEMO.md](docs/LOCAL_DEMO.md)
- [docs/FAILURE_RECOVERY.md](docs/FAILURE_RECOVERY.md)
- [docs/PROOF.md](docs/PROOF.md)

## Recent operator-facing changes

- ops-schema-health now exits non-zero on missing or invalid connection config instead of treating invalid config like a skip.

## Standalone quickstart

This is the copy-paste path that works today from a fresh checkout:

`bash
git clone https://github.com/AVasilkovski/typedb-ops-spine.git
cd typedb-ops-spine
python -m pip install .

# Apply the bundled example schema
ops-apply-schema \
  --schema examples/minimal_project/schema.tql \
  --database ops_spine_demo \
  --auto-migrate-redeclarations
# Stamp to the bundled migration head
ops-apply-schema \
  --schema examples/minimal_project/schema.tql \
  --database ops_spine_demo \
  --migrations-dir examples/minimal_project/migrations \
  --stamp-schema-version-head

# Verify parity
ops-schema-health \
  --migrations-dir examples/minimal_project/migrations \
  --database ops_spine_demo

# Smoke-read schema_version
ops-typedb-diag \


Defaults assume local TypeDB Core on localhost:1729 with admin/password.
Install from another repo or CI
For external projects, pin an immutable commit or tag:
Bash
python -m pip install \
  "typedb-ops-spine @ git+https://github.com/AVasilkovski/typedb-ops-spine.git@<pinned-commit-or-tag>"
  
The bundled example CI snippet in examples/minimal_project/ci_example.yml uses that install shape and the library's protocol-level readiness check.
Core tools
ops-apply-schema
Deterministic schema apply, optional guarded scrub, optional head stamping
ops-migrate
Ordinal-based schema migrations with gap detection
ops-schema-health
Repo ordinal vs database ordinal parity check
ops-typedb-diag
Connectivity, database presence, and optional smoke query verification
ops-tsv-extract
TSV extraction for emitted diagnostics
Optional extra invariant hook
ops-schema-health also supports an optional caller-supplied invariant hook:
Bash
ops-schema-health \
  --migrations-dir migrations \
  --database my_db \
  --extra-invariant package.module:function
  
Use this when a consuming repo wants to add one extra health invariant after ordinal parity without forking the generic package.
Secondary / example-profile tools
The package also ships ops-write-canary and ops-min-write-probe.
These are bundled example-profile diagnostics for the tenant/run-capsule schema under examples/minimal_project; they are not the primary onboarding path for generic users.
Recovery after stamp failure
If authoritative schema apply or a migration reports that schema_version recording failed after SCHEMA work succeeded, do not rerun the full command blindly.
Use the explicit stamp-only reconcile path instead.
Authoritative apply reconcile
Bash
ops-apply-schema \
  --database my_db \
  --migrations-dir migrations \
  --reconcile-schema-version-head
  
Migration ordinal reconcile
Bash
ops-migrate \
  --database my_db \
  --migrations-dir migrations \
  --reconcile-ordinal 7
  
Connection settings
Supported address forms:
localhost:1729
localhost
https://cloud.example.com:443
https://cloud.example.com together with TYPEDB_PORT=443
Validation is fail-fast:
TYPEDB_TLS=true with a non-HTTPS address is rejected immediately
TYPEDB_TLS=false with an HTTPS address is rejected immediately
unsupported schemes such as http://... are rejected immediately
TYPEDB_ROOT_CA_PATH is only valid with HTTPS/TLS connections
This avoids wasting the full retry budget on deterministic configuration errors.
ops-typedb-diag and run_smoke_diagnostics() validate address/TLS/CA settings before any retry loop. Deterministic config errors raise TypeDBConfigError immediately; only network/service-readiness failures consume retries.
Library usage
Python
from typedb.driver import TransactionType
from typedb_ops_spine import connect_with_retries, execute, QueryMode

driver = connect_with_retries("localhost:1729", "admin", "password")

with driver.transaction("ops_spine_demo", TransactionType.READ) as tx:
    rows = execute(
        tx,
        "match $v isa schema_version, has ordinal $o; select $o;",
        QueryMode.READ_ROWS,
        component="demo",
        db_name="ops_spine_demo",
    )
Development
Bash
git clone https://github.com/AVasilkovski/typedb-ops-spine.git
cd typedb-ops-spine
python -m pip install -e ".[dev]"
pytest tests/unit/ -q
pytest tests/integration/ -q
Integration tests use the same protocol-level readiness path as operators. If TypeDB is not query-ready with the configured address, auth, and TLS settings, they skip.

Small recommendation after you paste it:
- keep this as the README
- move any longer product/market wording out of README and into docs/beta-outreach-pack.md or PRIVATE_BETA.md
  --database ops_spine_demo \
  --require-db \
  --smoke-query 'match $v isa schema_version, has ordinal $o; select $o; limit 1;'
