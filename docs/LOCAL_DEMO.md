# Local Demo

This runbook demonstrates the current standalone local path for
`typedb-ops-spine` on Windows PowerShell against a local Docker container.

It uses the bundled example only:

- `examples/minimal_project/schema.tql`
- `examples/minimal_project/migrations/001_bootstrap.tql`

## Prerequisites

- Docker running locally
- `typedb-ops-spine` installed from this checkout:
  - `python -m pip install .`
- an active Python environment that exposes the installed `ops-*` console
  scripts on `PATH`
  - verified locally from the repo's `.venv` PowerShell environment
- PowerShell in the repo root

If you want to mirror the verified local setup exactly:

```powershell
.\.venv\Scripts\Activate.ps1
python -m pip install .
```

## 1. Start TypeDB on an alternate local port

```powershell
docker rm -f typedb-ops-spine-demo 2>$null | Out-Null
docker run -d --name typedb-ops-spine-demo -p 11729:1729 typedb/typedb:3.8.0
```

Expected outcome:
- container starts and `11729` is mapped to TypeDB `1729`

## 2. Set local demo environment variables

```powershell
$env:TYPEDB_ADDRESS = "localhost:11729"
$env:TYPEDB_USERNAME = "admin"
$env:TYPEDB_PASSWORD = "password"
$env:TYPEDB_DATABASE = "ops_spine_demo"
$env:TYPEDB_TLS = "false"
Remove-Item Env:TYPEDB_ROOT_CA_PATH -ErrorAction SilentlyContinue
```

Expected outcome:
- subsequent `ops-*` commands target the local demo container

## 3. Prove the server is reachable and auth works

```powershell
ops-typedb-diag `
  --database $env:TYPEDB_DATABASE `
  --retries 30 `
  --sleep-s 2
```

Expected outcome:
- exits `0`
- confirms the container is reachable with the configured credentials

## 4. Apply the bundled example schema

```powershell
ops-apply-schema `
  --schema .\examples\minimal_project\schema.tql `
  --database $env:TYPEDB_DATABASE
```

Expected outcome:
- exits `0`
- authoritative schema apply succeeds

## 5. Stamp schema_version to the bundled migration head

```powershell
ops-apply-schema `
  --schema .\examples\minimal_project\schema.tql `
  --database $env:TYPEDB_DATABASE `
  --migrations-dir .\examples\minimal_project\migrations `
  --stamp-schema-version-head
```

Expected outcome:
- exits `0`
- `schema_version` is stamped to the bundled head ordinal

## 6. Verify repo/database parity

```powershell
ops-schema-health `
  --migrations-dir .\examples\minimal_project\migrations `
  --database $env:TYPEDB_DATABASE
```

Expected outcome:
- prints matching repo and DB ordinals
- prints `PASS: parity OK`
- exits `0`

## 7. Run a smoke query against schema_version

```powershell
ops-typedb-diag `
  --database $env:TYPEDB_DATABASE `
  --require-db `
  --smoke-query 'match $v isa schema_version, has ordinal $o; select $o; limit 1;'
```

Expected outcome:
- exits `0`
- confirms the database exists
- confirms the `schema_version` smoke query succeeds

## 8. Simulate drift detection

```powershell
$driftDir = Join-Path $PWD "tmp-empty-migrations"
New-Item -ItemType Directory -Force $driftDir | Out-Null

ops-schema-health `
  --migrations-dir $driftDir `
  --database $env:TYPEDB_DATABASE
```

Expected outcome:
- prints different repo and DB ordinals
- prints `FAIL: drift detected`
- exits non-zero

## 9. Optional cleanup

```powershell
docker rm -f typedb-ops-spine-demo
Remove-Item -Recurse -Force $driftDir -ErrorAction SilentlyContinue
```

## Pitfalls

- PowerShell backticks must be the final character on the line. A trailing space breaks continuation.
- Use single quotes around TypeQL for `--smoke-query`. PowerShell expands `$v` and `$o` inside double quotes.
- The default local demo credentials are `admin` / `password`.
- If `ops-typedb-diag`, `ops-apply-schema`, or `ops-schema-health` are not found, activate the Python environment that installed `typedb-ops-spine` before running the demo.
- This demo uses `11729` specifically to avoid stale local `1729` conflicts from older environments.
