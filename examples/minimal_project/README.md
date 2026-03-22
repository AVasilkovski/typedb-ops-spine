# Minimal Project — typedb-ops-spine Example

This example demonstrates how to use `typedb-ops-spine` for deterministic
TypeDB schema management in a project.

## Files

| File | Purpose |
|------|---------|
| `schema.tql` | Authoritative schema definition |
| `migrations/001_bootstrap.tql` | Bootstrap migration (creates types + schema_version tracking) |
| `ci_example.yml` | GitHub Actions snippet using ops-spine CLI tools |

## Usage

```bash
# Install typedb-ops-spine
pip install typedb-ops-spine

# Apply schema
ops-apply-schema --schema schema.tql --database my_db

# Optional scrub pass for existing databases with inherited redeclarations
ops-apply-schema \
  --schema schema.tql \
  --database my_db \
  --auto-migrate-redeclarations \
  --scrub-only

# Run migrations
ops-migrate --migrations-dir migrations --database my_db

# Check schema health
ops-schema-health --migrations-dir migrations --database my_db

# Smoke diagnostics against the bootstrapped DB
ops-typedb-diag \
  --database my_db \
  --require-db \
  --smoke-query 'match $v isa schema_version, has ordinal $o; select $o; limit 1;'

# Write/read durability canary
# Targets the packaged tenant profile
ops-write-canary --database my_db

# Run probe (creates an isolated database for the tenant/run-capsule example profile)
ops-min-write-probe \
  --schema schema.tql \
  --migrations-dir migrations

# Extract diagnostics TSV
ops-tsv-extract
```
