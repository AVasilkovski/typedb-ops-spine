# Private Beta

`typedb-ops-spine` is in private beta as a standalone TypeDB 3.8 schema
deployment and migration toolkit.

## Primary supported commands

- `ops-apply-schema`
- `ops-migrate`
- `ops-schema-health`
- `ops-typedb-diag`
- `ops-tsv-extract`

## Secondary / example-profile tools

- `ops-write-canary`
- `ops-min-write-probe`

These tools target the bundled tenant/run-capsule example profile and are not
the primary onboarding path for generic users.

## Install path

- local checkout install: `python -m pip install .`
- pinned VCS install for consuming repos and CI
- no PyPI release yet

## Tested versions

- tested with `typedb-driver 3.8.0`
- tested against TypeDB server/runtime `3.8.0`
- tested against TypeDB server/runtime `3.8.1` where applicable in CI compatibility checks

## Known limitations

- schema-version stamp failures require explicit reconcile commands; do not rerun blindly
- schema drift detection does not yet include automatic schema-fingerprint comparison
- example-profile tools are not a generic contract for arbitrary schemas
- integration behavior depends on a live, query-ready TypeDB environment
