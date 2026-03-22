# Minimal Project Example

This example shows the supported standalone path for `typedb-ops-spine` today.

There is no PyPI release yet. Install from the repository checkout:

```bash
git clone https://github.com/AVasilkovski/typedb-ops-spine.git
cd typedb-ops-spine
python -m pip install .
```

## Files

- `schema.tql`: authoritative schema definition
- `migrations/001_bootstrap.tql`: bootstrap migration with `schema_version`
- `ci_example.yml`: GitHub Actions snippet using pinned VCS install and protocol readiness

## Example flow

```bash
# Apply the authoritative schema
ops-apply-schema \
  --schema examples/minimal_project/schema.tql \
  --database my_db

# Optional guarded scrub for inherited redeclarations
ops-apply-schema \
  --schema examples/minimal_project/schema.tql \
  --database my_db \
  --auto-migrate-redeclarations \
  --scrub-only

# Fast-forward schema_version to the bundled migration head
ops-apply-schema \
  --schema examples/minimal_project/schema.tql \
  --database my_db \
  --migrations-dir examples/minimal_project/migrations \
  --stamp-schema-version-head

# Parity check and smoke read
ops-schema-health \
  --migrations-dir examples/minimal_project/migrations \
  --database my_db

ops-typedb-diag \
  --database my_db \
  --require-db \
  --smoke-query 'match $v isa schema_version, has ordinal $o; select $o; limit 1;'
```

`ops-write-canary` and `ops-min-write-probe` are bundled example-profile diagnostics for this tenant/run-capsule schema. They are optional and not required for the basic schema apply/migrate/health flow.
