from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
README = ROOT / "README.md"
CHANGELOG = ROOT / "CHANGELOG.md"
PRIVATE_BETA = ROOT / "PRIVATE_BETA.md"
LOCAL_DEMO = ROOT / "docs" / "LOCAL_DEMO.md"
FAILURE_RECOVERY = ROOT / "docs" / "FAILURE_RECOVERY.md"
PROOF = ROOT / "docs" / "PROOF.md"
EXAMPLE_README = ROOT / "examples" / "minimal_project" / "README.md"
CI_EXAMPLE = ROOT / "examples" / "minimal_project" / "ci_example.yml"


def test_docs_do_not_claim_bare_pypi_install():
    readme = README.read_text(encoding="utf-8")
    example_readme = EXAMPLE_README.read_text(encoding="utf-8")

    assert "pip install typedb-ops-spine\n" not in readme
    assert "pip install typedb-ops-spine\n" not in example_readme
    assert "no PyPI release yet" in readme
    assert "python -m pip install ." in readme


def test_private_beta_and_changelog_docs_exist_and_are_linked():
    readme = README.read_text(encoding="utf-8")

    assert CHANGELOG.is_file()
    assert PRIVATE_BETA.is_file()
    assert LOCAL_DEMO.is_file()
    assert FAILURE_RECOVERY.is_file()
    assert PROOF.is_file()
    assert "CHANGELOG.md" in readme
    assert "PRIVATE_BETA.md" in readme
    assert "docs/LOCAL_DEMO.md" in readme
    assert "docs/FAILURE_RECOVERY.md" in readme
    assert "docs/PROOF.md" in readme


def test_local_demo_and_failure_recovery_docs_cover_expected_contract():
    local_demo = LOCAL_DEMO.read_text(encoding="utf-8")
    failure_recovery = FAILURE_RECOVERY.read_text(encoding="utf-8")

    assert "11729" in local_demo
    assert "ops-typedb-diag" in local_demo
    assert "ops-apply-schema" in local_demo
    assert "ops-schema-health" in local_demo
    assert "schema_version" in local_demo

    assert "--reconcile-schema-version-head" in failure_recovery
    assert "--reconcile-ordinal" in failure_recovery
    assert "typedb_diag.jsonl" in failure_recovery


def test_example_ci_uses_protocol_readiness_and_pinned_vcs_install():
    ci_example = CI_EXAMPLE.read_text(encoding="utf-8")

    assert "connect_with_retries" in ci_example
    assert "socket.create_connection" not in ci_example
    assert "git+https://github.com/AVasilkovski/typedb-ops-spine.git@" in ci_example
