# typedb-ops-spine

Deterministic TypeDB 3.8 schema operations, smoke diagnostics, and CI forensics.

[![CI](https://github.com/AVasilkovski/typedb-ops-spine/actions/workflows/ci.yml/badge.svg)](https://github.com/AVasilkovski/typedb-ops-spine/actions)

## Release status

There is **no PyPI release yet**.

The supported install paths today are:

- local checkout install for standalone/operator use
- pinned VCS install for consuming repos and CI

Do not rely on `pip install typedb-ops-spine` until a real package release exists.

## Recent operator-facing changes

- `ops-schema-health` now exits non-zero on missing or invalid connection config instead of treating invalid config like a skip.

## Standalone quickstart

This is the copy-paste path that works today from a fresh checkout:

```bash
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

# Verify parity and smoke-read schema_version
ops-schema-health \
  --migrations-dir examples/minimal_project/migrations \
  --database ops_spine_demo

ops-typedb-diag \
  --database ops_spine_demo \
  --require-db \
  --smoke-query 'match $v isa schema_version, has ordinal $o; select $o; limit 1;'
```

Defaults assume local TypeDB Core on `localhost:1729` with admin/password.

## Install from another repo or CI

For external projects, pin an immutable commit or tag:

```bash
python -m pip install \
  "typedb-ops-spine @ git+https://github.com/AVasilkovski/typedb-ops-spine.git@<pinned-commit-or-tag>"
```

The bundled example CI snippet in
[examples/minimal_project/ci_example.yml](examples/minimal_project/ci_example.yml)
uses that install shape and the library's protocol-level readiness check.

## Core tools

- `ops-apply-schema`: deterministic schema apply, optional guarded scrub, optional head stamping
- `ops-migrate`: ordinal-based schema migrations with gap detection
- `ops-schema-health`: repo ordinal vs database ordinal parity check
- `ops-typedb-diag`: connectivity, database presence, and optional smoke query verification
- `ops-tsv-extract`: TSV extraction for emitted diagnostics

`ops-schema-health` also supports an optional caller-supplied invariant hook:

```bash
ops-schema-health \
  --migrations-dir migrations \
  --database my_db \
  --extra-invariant package.module:function
```

Use this when a consuming repo wants to add one extra health invariant after ordinal parity without forking the generic package.

The package also ships `ops-write-canary` and `ops-min-write-probe`. Those are bundled example-profile diagnostics for the tenant/run-capsule schema under `examples/minimal_project`; they are not the primary onboarding path for generic users.

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

`ops-typedb-diag` and `run_smoke_diagnostics()` validate address/TLS/CA settings before any retry loop. Deterministic config errors raise `TypeDBConfigError` immediately; only network/service-readiness failures consume retries.

## Library usage

```python
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
```

## Development

```bash
git clone https://github.com/AVasilkovski/typedb-ops-spine.git
cd typedb-ops-spine
python -m pip install -e ".[dev]"

pytest tests/unit/ -q
pytest tests/integration/ -q
```

Integration tests use the same protocol-level readiness path as operators. If TypeDB is not query-ready with the configured address, auth, and TLS settings, they skip.

## SuperHyperion adoption note

If another repo wants to consume `typedb-ops-spine` before a package release exists, use a pinned VCS dependency instead of `typedb-ops-spine>=...`.
