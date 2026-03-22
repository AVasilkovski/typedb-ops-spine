from __future__ import annotations

from pathlib import Path

import pytest

from typedb_ops_spine.cli import apply_schema_cli


class _ShouldNotBeCalledError(Exception):
    pass


def test_apply_schema_cli_dry_run_with_auto_scrub_does_not_connect(monkeypatch, tmp_path: Path):
    schema_path = tmp_path / "schema.tql"
    schema_path.write_text(
        "define\n"
        "  entity evidence,\n"
        "    owns template-id;\n"
        "  entity validation-evidence sub evidence,\n"
        "    owns template-id;\n"
        "  attribute template-id,\n"
        "    value string;\n",
        encoding="utf-8",
    )

    def _boom(*_args, **_kwargs):
        raise _ShouldNotBeCalledError

    monkeypatch.setattr("typedb_ops_spine.readiness.connect_with_retries", _boom)
    monkeypatch.setattr("typedb_ops_spine.readiness.ensure_database", _boom)
    monkeypatch.setattr(
        "sys.argv",
        [
            "ops-apply-schema",
            "--dry-run",
            "--schema",
            str(schema_path),
            "--auto-migrate-redeclarations",
        ],
    )

    rc = apply_schema_cli.main()
    assert rc == 0


def test_apply_schema_cli_fails_fast_on_invalid_tls_localhost(
    monkeypatch, tmp_path: Path, capsys: pytest.CaptureFixture[str],
):
    schema_path = tmp_path / "schema.tql"
    schema_path.write_text("define entity tenant;", encoding="utf-8")

    def _boom(*_args, **_kwargs):
        raise _ShouldNotBeCalledError

    monkeypatch.setattr("typedb_ops_spine.readiness.connect_with_retries", _boom)
    monkeypatch.setenv("TYPEDB_TLS", "true")
    monkeypatch.delenv("TYPEDB_ADDRESS", raising=False)
    monkeypatch.setattr(
        "sys.argv",
        [
            "ops-apply-schema",
            "--schema",
            str(schema_path),
        ],
    )

    rc = apply_schema_cli.main()

    captured = capsys.readouterr()
    assert rc == 1
    assert "No explicit --address/TYPEDB_ADDRESS was provided" in captured.err
