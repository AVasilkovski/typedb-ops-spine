# typedb-ops-spine v0.2.0-beta — Private-Beta Outreach Pack

## 1. Positioning Statement

**typedb-ops-spine** is a schema evolution and deployment toolkit for teams running TypeDB 3.8.x. It provides deterministic schema application, ordered migration execution with version tracking, fail-fast configuration validation, protocol-level readiness probes, and structured operator-facing diagnostics — so that every schema deploy is repeatable, auditable, and safe to run in CI/CD. It is an operational safety layer for teams that already use TypeDB 3.8.x and want confidence that schema changes follow fail-closed semantics — they either apply cleanly or exit non-zero with structured diagnostics and explicit reconcile paths for partial failures like schema-version stamp mismatches.

## 2. GitHub Release Description

### v0.2.0-beta

Schema evolution and operational safety toolkit for TypeDB 3.8.x.

**Primary commands:**

- `ops-apply-schema` — Deterministic schema loading with glob resolution, deduplication, and stable file ordering. Optional schema-version stamping. Fail-closed on invalid patterns or empty inputs.
- `ops-migrate` — Ordered `NNN_*.tql` migration execution with `schema_version` entity tracking (ordinal, git-commit, applied-at). Two-phase SCHEMA + WRITE execution. Supports dry-run.
- `ops-schema-health` — Validates that repo migration HEAD ordinal matches database current ordinal. Reports drift clearly and exits non-zero on mismatch.
- `ops-typedb-diag` — Protocol-level TypeDB connectivity and readiness probe with configurable retries and backoff. Optional database existence check.
- `ops-tsv-extract` — Extract diagnostic and canary data to TSV for operator review.

**Secondary / example-profile tools:**

- `ops-write-canary` — Write-commit-read durability verification across transaction boundaries.
- `ops-min-write-probe` — Write-shape validation using ephemeral databases.

**Key properties:**

- Fail-fast config validation (address, TLS, CA path)
- Additive-only migration discipline (modifications and deletions of existing migration files are blocked)
- Structured JSONL diagnostics emitted to `ci_artifacts/` with query SHA256 hashes
- Explicit reconcile path for schema-version stamp failures
- Install via local checkout or pinned VCS: `pip install git+https://github.com/AVasilkovski/typedb-ops-spine.git@<COMMIT-OR-TAG>` (pin to a specific commit or tag once the release is cut)

## 3. TypeDB Outreach Message

**Subject:** typedb-ops-spine — schema deployment toolkit for TypeDB 3.8.x (seeking beta feedback)

Hi —

I've been building a schema evolution and deployment toolkit for TypeDB 3.8.x called [typedb-ops-spine](https://github.com/AVasilkovski/typedb-ops-spine). It's at v0.2.0-beta and I'm looking for feedback from teams operating TypeDB in non-trivial environments.

The problem it addresses: for teams doing non-trivial TypeDB schema changes, rollout often requires manual coordination — ordering migrations, verifying what's applied, knowing whether a deploy succeeded or partially failed. typedb-ops-spine makes schema rollout deterministic and adds fail-fast validation, version tracking, readiness probes, and structured diagnostics.

**What it does:**
- Deterministic schema apply with stable file ordering and deduplication
- Ordered `NNN_*.tql` migrations with `schema_version` tracking (ordinal, git SHA, timestamp)
- Repo-vs-DB schema parity checks
- Protocol-level readiness probes with retry/backoff
- Structured JSONL diagnostics for every operation
- Additive-only migration policy enforcement
- Explicit reconcile commands for schema-version stamp failures

I'd welcome feedback on the approach, the command surface, or gaps relative to your deployment workflows. Happy to do a short call or take async feedback.

Repo: https://github.com/AVasilkovski/typedb-ops-spine

## 4. Beta User / Design Partner Outreach Message

Hi —

I'm looking for a few teams running TypeDB 3.8.x who'd be willing to try a schema deployment toolkit and give honest feedback on whether it solves real problems or misses the mark.

The tool is called **typedb-ops-spine**. It handles deterministic schema rollout, ordered migrations with version tracking, readiness probes, and structured diagnostics. The goal is schema deploys that are repeatable and auditable — the kind of thing you want before putting TypeDB schema changes in a CI/CD pipeline.

What I'm asking for:

- Try it against a dev or staging TypeDB instance
- Run through the primary commands (`ops-apply-schema`, `ops-migrate`, `ops-schema-health`, `ops-typedb-diag`, `ops-tsv-extract`)
- Tell me what worked, what didn't, and what's missing

Time commitment: ~30 minutes to install and run through the demo, plus a 15-minute feedback call if you're willing.

Repo: https://github.com/AVasilkovski/typedb-ops-spine

## 5. 5-Minute Demo Script

**Setup (before demo):**

- TypeDB 3.8.x running locally or in a container
- typedb-ops-spine installed from local checkout
- A sample schema directory with 2–3 `.tql` files
- A `migrations/` directory with `001_init.tql` and `002_add_feature.tql`

**Demo sequence:**

| Step | Command | What to show | Operator value |
|------|---------|--------------|----------------|
| 1. Readiness check | `ops-typedb-diag --require-db` | TypeDB is reachable, database exists, retry behavior visible | Confirm the target is alive and the database exists before touching schema. |
| 2. Apply schema | `ops-apply-schema --schema src/schema/*.tql --database mydb` | Glob resolution, stable file ordering, deduplication | Same input always produces the same apply order. No surprises from filesystem ordering. |
| 3. Run migrations | `ops-migrate --migrations-dir migrations/ --database mydb` | Ordered execution of 001 then 002; `schema_version` entity created with ordinal, git SHA, timestamp | Every migration is stamped. Query `schema_version` to see exactly what's deployed. |
| 4. Verify parity | `ops-schema-health --migrations-dir migrations/ --database mydb` | Repo HEAD ordinal matches DB current ordinal | Reports drift clearly and exits non-zero on mismatch. |
| 5. Review diagnostics | Open `ci_artifacts/migrate_diagnostics.jsonl` | Structured JSONL: per-query SHA256, success/fail, timestamps | Machine-readable audit trail. Actionable context for failures. |

**Optional (if time permits):**

| Step | Command | What to show | Operator value |
|------|---------|--------------|----------------|
| 6. Dry-run | `ops-migrate --dry-run` | Shows pending migrations without executing | Safe to run in CI as a validation gate. |
| 7. Write canary | `ops-write-canary` | Write-commit-read round-trip succeeds | Proves writes persist across transaction boundaries. |

**Closing line:**

> Five commands. Deterministic apply, ordered migrations with version tracking, parity checks, readiness probes, and structured diagnostics. Schema deploys that are boring and predictable.

## 6. Who This Is For / Not For

**This is for you if:**
- You run TypeDB 3.8.x in staging or production
- You apply schema changes through CI/CD or scripted deploys
- You want deterministic migration ordering and version tracking
- You need to know whether a schema deploy succeeded, failed, or partially applied
- You want structured, machine-readable diagnostics for every schema operation
- You enforce additive-only schema evolution

**This is not for you if:**

- You use TypeDB only for local prototyping and don't need deploy safety
- You need a GUI or visual schema editor
- You are looking for a general-purpose TypeDB ORM or query builder

## 7. Beta Feedback Questions

1. **Migration workflow fit:** Does the `NNN_*.tql` ordered-migration model match how you currently manage schema changes, or does your team use a different approach? What would need to change for typedb-ops-spine to fit your workflow?

2. **Diagnostics usefulness:** After running `ops-migrate` or `ops-apply-schema`, did the JSONL diagnostics in `ci_artifacts/` give you enough information to understand what happened? What fields or context were missing?

3. **Failure clarity:** Did you encounter any failure during schema apply, migration, or readiness checks? If so, was the error output actionable — could you tell what went wrong and what to do next without reading source code?

4. **CI/CD integration:** If you were to add typedb-ops-spine to your CI/CD pipeline, what's the first friction point you'd hit? (e.g., auth configuration, environment detection, output format, exit codes)

5. **Missing operations:** Looking at the current command set (`ops-apply-schema`, `ops-migrate`, `ops-schema-health`, `ops-typedb-diag`, `ops-tsv-extract`), is there an operational task you perform regularly against TypeDB that isn't covered?
