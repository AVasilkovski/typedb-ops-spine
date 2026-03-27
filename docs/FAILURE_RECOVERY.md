# Failure Recovery

This runbook documents the current failure and recovery paths already present
in `typedb-ops-spine`.

## Invalid credentials

Example:

```powershell
$env:TYPEDB_PASSWORD = "wrong"

ops-typedb-diag `
  --database $env:TYPEDB_DATABASE `
  --retries 3 `
  --sleep-s 1
```

What failure looks like:
- `ops-typedb-diag` exits non-zero
- detailed failure context is emitted to `ci_artifacts/typedb_diag.jsonl`

Why it happens:
- TypeDB rejects the configured username/password pair

How to recover:
- restore the correct credentials
- rerun `ops-typedb-diag` before applying schema or migrations

## Malformed smoke query from PowerShell interpolation

Broken example:

```powershell
ops-typedb-diag `
  --database $env:TYPEDB_DATABASE `
  --require-db `
  --smoke-query "match $v isa schema_version, has ordinal $o; select $o; limit 1;"
```

What failure looks like:
- the command exits non-zero
- `ci_artifacts/typedb_diag.jsonl` records a failed smoke-read attempt

Why it happens:
- PowerShell expands `$v` and `$o` inside double quotes
- the query sent to TypeDB is no longer the intended TypeQL

How to recover:
- rerun with single quotes around the TypeQL:

```powershell
ops-typedb-diag `
  --database $env:TYPEDB_DATABASE `
  --require-db `
  --smoke-query 'match $v isa schema_version, has ordinal $o; select $o; limit 1;'
```

- backtick escaping is possible, but single quotes are the safer default in PowerShell

## Drift detection mismatch

Example:

```powershell
ops-schema-health `
  --migrations-dir $driftDir `
  --database $env:TYPEDB_DATABASE
```

What failure looks like:
- the CLI prints both repo and DB ordinals
- it prints `FAIL: drift detected`
- it exits non-zero

Why it happens:
- the repo migration head does not match the database `schema_version` ordinal
- this can be a wrong migrations directory, a database pointed at the wrong repo state, or a known stamp mismatch

How to recover:
- point the command at the correct migrations directory for that database
- if the mismatch is from a known schema-version stamp failure, use the reconcile flow instead of rerunning blindly

## Schema-version stamp failure

What failure looks like:
- the CLI prints an error containing `Plain rerun is unsafe`
- the CLI prints a `Recovery:` line with an exact reconcile command

Why it happens:
- SCHEMA work succeeded, but the follow-up `schema_version` WRITE did not
- `typedb-ops-spine` fails closed here and does not assume a full rerun is safe

How to recover after authoritative apply:

```powershell
ops-apply-schema `
  --database $env:TYPEDB_DATABASE `
  --migrations-dir .\examples\minimal_project\migrations `
  --reconcile-schema-version-head
```

How to recover after a migration stamp failure:

```powershell
ops-migrate `
  --database my_db `
  --migrations-dir .\migrations `
  --reconcile-ordinal 7
```

Use the exact `Recovery:` command printed by the CLI if it differs from the
examples above.

## Diagnostics artifacts to inspect

- `ci_artifacts/apply_schema_diagnostics.jsonl`
- `ci_artifacts/migrate_diagnostics.jsonl`
- `ci_artifacts/schema_health_diagnostics.jsonl`
- `ci_artifacts/typedb_diag.jsonl`
