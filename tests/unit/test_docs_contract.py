from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
README = ROOT / "README.md"
CHANGELOG = ROOT / "CHANGELOG.md"
PRIVATE_BETA = ROOT / "PRIVATE_BETA.md"
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
    assert "CHANGELOG.md" in readme
    assert "PRIVATE_BETA.md" in readme


def test_example_ci_uses_protocol_readiness_and_pinned_vcs_install():
    ci_example = CI_EXAMPLE.read_text(encoding="utf-8")

    assert "connect_with_retries" in ci_example
    assert "socket.create_connection" not in ci_example
    assert "git+https://github.com/AVasilkovski/typedb-ops-spine.git@" in ci_example
