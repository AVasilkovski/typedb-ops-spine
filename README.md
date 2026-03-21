# typedb-ops-spine

**Deterministic TypeDB 3.8 schema operations, smoke diagnostics, and CI forensics toolkit.**

[![CI](https://github.com/AVasilkovski/typedb-ops-spine/actions/workflows/ci.yml/badge.svg)](https://github.com/AVasilkovski/typedb-ops-spine/actions)
[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

---

## What is this?

`typedb-ops-spine` packages the operational tooling needed to run TypeDB 3.8 deterministically in CI and production:

| Capability | Tool |
|---|---|
| **Schema apply** | `ops-apply-schema` — deterministic schema application with glob-killer path resolution |
| **Schema scrub** | `ops-apply-schema --auto-migrate-redeclarations` — guarded `undefine owns/plays` planning for inherited redeclarations |
| **Migrations** | `ops-migrate` — ordinal-based, gap-detecting, hash-logged migrations |
| **Health check** | `ops-schema-health` — drift detection (repo ordinals vs DB state) |
| **Smoke diagnostics** | `ops-typedb-diag` — connectivity, DB-presence, and optional smoke-query verification |
| **Readiness** | `connect_with_retries()` — real round-trip verification (`databases.all()`) |
| **Safe execution** | `execute()` — answer-kind barrier with Rows → Docs → OK ordering |
| **Diagnostics** | `emit_typedb_diag()` — keyword-only JSONL emission + TSV extractor |
| **Canary** | `ops-write-canary` — write→commit→read durability check for the packaged tenant-based profile |
| **Probe** | `ops-min-write-probe` — 5-variant write-shape arbiter for the packaged tenant/run-capsule profile |

---

## Quickstart

```bash
pip install typedb-ops-spine

# Apply schema
ops-apply-schema --schema schema.tql --database my_db

# Existing databases with inherited redeclarations:
ops-apply-schema \
  --schema schema.tql \
  --database my_db \
  --auto-migrate-redeclarations

# Run migrations
ops-migrate --migrations-dir migrations --database my_db

# Check for schema drift
ops-schema-health --migrations-dir migrations --database my_db

# Smoke diagnostics (DB presence only)
ops-typedb-diag --database my_db --require-db

# Write/read durability canary
# Targets the packaged tenant profile by default
ops-write-canary --database my_db

# Tenant-scoped canary (SuperHyperion-profile)
# Requires schema with 'tenant' and 'run-capsule' types
ops-write-canary --database my_db --tenant-id "tenant-1" --ownership-rel auto

# Smoke diagnostics with an explicit rows query
ops-typedb-diag \
  --database my_db \
  --require-db \
  --smoke-query 'match $v isa schema_version, has ordinal $o; select $o; limit 1;'

# Extract CI diagnostics as TSV
ops-tsv-extract
```

### Cloud / TLS

`typedb-ops-spine` accepts local Core addresses (`localhost:1729`) and normalizes
Cloud-style addresses (`https://cloud.typedb.com` → `https://cloud.typedb.com:443`
when paired with `TYPEDB_PORT=443`). TLS is inferred from `https://...` addresses
unless `TYPEDB_TLS` explicitly overrides it.

```bash
export TYPEDB_ADDRESS="https://cloud.typedb.com"
export TYPEDB_PORT="443"
export TYPEDB_USERNAME="admin"
export TYPEDB_PASSWORD="password"
export TYPEDB_ROOT_CA_PATH="/path/to/ca.pem"

ops-typedb-diag --database my_db --require-db
```

### As a library

```python
from typedb.driver import TransactionType
from typedb_ops_spine import connect_with_retries, execute, QueryMode

driver = connect_with_retries("localhost:1729", "admin", "password")
with driver.transaction("my_db", TransactionType.READ) as tx:
    rows = execute(tx, "match $t isa tenant; select $t;", QueryMode.READ_ROWS,
                   component="my_app", db_name="my_db")
```

---

## Locked Invariants

### EPI-16.9: Materialization Barrier (Rows → Docs → OK)

In TypeDB 3.8, calling `as_ok()` on an answer that actually contains concept rows
throws `Invalid query answer conversion from '_ConceptRowIterator' to 'OkQueryAnswer'`.

**The `execute()` function enforces this barrier:**

1. **Check `is_concept_rows()` first** — if truthy, exhaust rows via `as_concept_rows()`.
2. **Then check `is_concept_documents()`** — if truthy, exhaust docs.
3. **Only then check `is_ok()`** — and only if no rows/docs were materialized.
4. **Fallback** tries rows → docs → `is_ok()` guard → `as_ok()`.
5. **No silent swallowing** — every failure path emits diagnostics.

```
LOCKED: Never call as_ok() when rows/docs exist or can be exhausted.
```

### Diagnostics Contract

All diagnostic emission uses **keyword-only** parameters:

```python
emit_typedb_diag(
    *,
    component: str,    # e.g. "my_app", "ops_write_canary"
    db_name: str,      # database name
    tx_type: str,      # "READ", "WRITE", "SCHEMA"
    action: str,       # "execute", "barrier_failure", "commit", etc.
    query: str,        # the TypeQL query
    answer_kind: str,  # "rows", "docs", "ok", "exception"
    row_count: int,    # materialized row count
    doc_count: int,    # materialized doc count
    error_code: str,
    error_message: str,
    **extra,           # forward-compatible extension fields
)
```

Output: `ci_artifacts/typedb_diag.jsonl` (configurable via `OPS_DIAG_PATH` or `CI_ARTIFACTS_DIR`).

---

## Failure Mode Taxonomy

| Failure Mode | Symptom | Root Cause | Fix |
|---|---|---|---|
| **Ghost writes** | Insert appears to succeed but data not queryable | `as_ok()` called on rows iterator | Enforce Rows→Docs→OK barrier |
| **Seed invisibility** | CI reads return 0 rows after seed | No materialization before commit | Exhaust iterator before `tx.commit()` |
| **Invalid cast exception** | `_ConceptRowIterator → OkQueryAnswer` | Calling `as_ok()` on a rows answer | Check `is_concept_rows()` first |
| **Schema drift** | Tests pass locally, fail in CI | Migration ordinals out of sync | Run `ops-schema-health` in CI |
| **Flaky readiness** | Connection refused in CI | TypeDB not ready when tests start | Use `connect_with_retries()` |

---

## Answer-Kind Safe Execution Guidance

| QueryMode | Expected Answer | Barrier Behavior |
|---|---|---|
| `READ_ROWS` | Concept rows | Materializes rows, raises if not rows |
| `READ_DOCS` | Concept docs | Materializes docs, raises if not docs |
| `WRITE` | Any (permissive) | Tries rows→docs→ok, returns whatever materializes |
| `WRITE_ROWS` | Concept rows | Strict: raises if not rows |
| `WRITE_DOCS` | Concept docs | Strict: raises if not docs |
| `WRITE_OK` | OK preferred | Accepts rows/docs, barriers correctly |
| `SCHEMA_OK` | OK | Schema tx, barriers correctly |

---

## CI Integration

See [`examples/minimal_project/ci_example.yml`](examples/minimal_project/ci_example.yml) for a
complete GitHub Actions snippet. Key pattern:

```yaml
services:
  typedb:
    image: typedb/typedb:3.8.0
    ports: ["1729:1729"]

steps:
  - run: ops-apply-schema --schema schema.tql --auto-migrate-redeclarations --scrub-only
  - run: ops-apply-schema --schema schema.tql --migrations-dir migrations --stamp-schema-version-head
  - run: ops-migrate --migrations-dir migrations
  - run: ops-schema-health --migrations-dir migrations
  - run: >
      ops-typedb-diag --require-db
      --smoke-query 'match $v isa schema_version, has ordinal $o; select $o; limit 1;'
  - if: always()
    run: |
      set +e
      ops-write-canary; echo "CANARY=$?" >> $GITHUB_ENV
      ops-min-write-probe --schema schema.tql --migrations-dir migrations
      echo "PROBE=$?" >> $GITHUB_ENV
  - if: always()
    run: ops-tsv-extract
```

---

## Release Plan

### v0.2.0 (current)

- Core ops: schema apply, migrate, health check, readiness
- Guarded schema scrubber for inherited owns/plays redeclarations
- Smoke diagnostics CLI for connectivity, DB presence, and optional smoke-query checks
- Cloud/TLS address normalization and HTTPS-based TLS inference
- Execution barrier with Rows→Docs→OK invariant
- Diagnostics JSONL + TSV extractor
- Canary + Arbiter probe (5 variants)
- CI workflow pinned to TypeDB 3.8.0 plus 3.8.1 compat

### v0.3.0 (planned)

- Stronger content-level drift / schema fingerprint parity
- Better crash-recovery semantics around authoritative apply + migration bookkeeping
- Broader cloud/TLS integration coverage

### How SuperHyperion can later adopt ops-spine

1. Add `typedb-ops-spine>=0.2.0` to SuperHyperion's `requirements.txt`
2. Replace `from src.db.typedb_exec import execute` with `from typedb_ops_spine import execute`
3. Replace `from src.db.typedb_diagnostics import emit_typedb_diag` with `from typedb_ops_spine import emit_typedb_diag`
4. Remove duplicated `scripts/apply_schema.py`, `scripts/migrate.py`, etc.
5. Update CI to use `ops-apply-schema`, `ops-migrate`, `ops-schema-health`, and `ops-typedb-diag`

This is a **non-breaking, additive** migration — SuperHyperion can adopt incrementally.

---

## Development

```bash
git clone https://github.com/AVasilkovski/typedb-ops-spine.git
cd typedb-ops-spine
pip install -e ".[dev]"

# Unit tests (no TypeDB needed)
pytest tests/unit/ -v

# Lint
ruff check typedb_ops_spine/ tests/

# Integration tests (need TypeDB running on localhost:1729)
TYPEDB_ADDRESS=localhost:1729 pytest tests/integration/ -v
```

## License

MIT — see [LICENSE](LICENSE).
