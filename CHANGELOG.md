# Changelog

## v0.2.0

- Extracted the generic TypeDB 3.8 schema/migration operations core from SuperHyperion into `typedb-ops-spine`.
- Added release-readiness hardening for install guidance, fail-fast config validation, and protocol-level readiness checks.
- Added the optional schema-health extension seam for one caller-supplied extra invariant.
- Tightened operator-facing config/error behavior and standardized remaining CLI error messaging.
- Added explicit reconcile-only recovery paths for schema-version stamp failures after successful SCHEMA work.
