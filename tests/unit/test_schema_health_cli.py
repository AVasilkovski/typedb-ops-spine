from __future__ import annotations

import sys
import types

import pytest

from typedb_ops_spine.cli import schema_health_cli
from typedb_ops_spine.schema_health import SchemaHealthExtraResult, SchemaHealthReport


class _FakeDriver:
    def __init__(self):
        self.closed = False

    def close(self) -> None:
        self.closed = True


def test_schema_health_cli_runs_extra_invariant_and_prints_pass(monkeypatch, capsys: pytest.CaptureFixture[str]):
    driver = _FakeDriver()
    captured: dict[str, object] = {}

    module = types.ModuleType("fake_health_hooks")

    def health_hook(*_args, **_kwargs):
        return True

    module.health_hook = health_hook
    monkeypatch.setitem(sys.modules, "fake_health_hooks", module)
    monkeypatch.setattr(
        "typedb_ops_spine.readiness.resolve_connection_config",
        lambda *_args, **_kwargs: ("localhost:1729", False, None),
    )
    monkeypatch.setattr(
        "typedb_ops_spine.readiness.connect_with_retries",
        lambda *_args, **_kwargs: driver,
    )

    def _fake_run_health_checks(*_args, **kwargs):
        captured["extra_invariant"] = kwargs["extra_invariant"]
        captured["extra_name"] = kwargs["extra_name"]
        return SchemaHealthReport(
            healthy=True,
            repo_ordinal=1,
            db_ordinal=1,
            extra_result=SchemaHealthExtraResult(name="fake_health_hooks:health_hook", ok=True),
        )

    monkeypatch.setattr("typedb_ops_spine.schema_health.run_health_checks", _fake_run_health_checks)
    monkeypatch.setattr(
        "sys.argv",
        [
            "ops-schema-health",
            "--migrations-dir",
            "migrations",
            "--database",
            "ops_db",
            "--extra-invariant",
            "fake_health_hooks:health_hook",
        ],
    )

    rc = schema_health_cli.main()

    captured_io = capsys.readouterr()
    assert rc == 0
    assert driver.closed is True
    assert captured["extra_invariant"] is health_hook
    assert captured["extra_name"] == "fake_health_hooks:health_hook"
    assert "Extra invariant 'fake_health_hooks:health_hook': PASS" in captured_io.out


def test_schema_health_cli_prints_fail_message(monkeypatch, capsys: pytest.CaptureFixture[str]):
    driver = _FakeDriver()
    module = types.ModuleType("fake_health_hooks_fail")
    module.health_hook = lambda *_args, **_kwargs: False
    monkeypatch.setitem(sys.modules, "fake_health_hooks_fail", module)
    monkeypatch.setattr(
        "typedb_ops_spine.readiness.resolve_connection_config",
        lambda *_args, **_kwargs: ("localhost:1729", False, None),
    )
    monkeypatch.setattr(
        "typedb_ops_spine.readiness.connect_with_retries",
        lambda *_args, **_kwargs: driver,
    )
    monkeypatch.setattr(
        "typedb_ops_spine.schema_health.run_health_checks",
        lambda *_args, **_kwargs: SchemaHealthReport(
            healthy=False,
            repo_ordinal=1,
            db_ordinal=1,
            extra_result=SchemaHealthExtraResult(
                name="fake_health_hooks_fail:health_hook",
                ok=False,
                message="bad check",
            ),
        ),
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "ops-schema-health",
            "--migrations-dir",
            "migrations",
            "--database",
            "ops_db",
            "--extra-invariant",
            "fake_health_hooks_fail:health_hook",
        ],
    )

    rc = schema_health_cli.main()

    captured = capsys.readouterr()
    assert rc == 1
    assert driver.closed is True
    assert "Extra invariant 'fake_health_hooks_fail:health_hook': FAIL: bad check" in captured.out
    assert "[ops-schema-health] FAIL: extra invariant failed" in captured.out


def test_schema_health_cli_prints_skip_on_drift(monkeypatch, capsys: pytest.CaptureFixture[str]):
    driver = _FakeDriver()
    module = types.ModuleType("fake_health_hooks_skip")
    module.health_hook = lambda *_args, **_kwargs: True
    monkeypatch.setitem(sys.modules, "fake_health_hooks_skip", module)
    monkeypatch.setattr(
        "typedb_ops_spine.readiness.resolve_connection_config",
        lambda *_args, **_kwargs: ("localhost:1729", False, None),
    )
    monkeypatch.setattr(
        "typedb_ops_spine.readiness.connect_with_retries",
        lambda *_args, **_kwargs: driver,
    )
    monkeypatch.setattr(
        "typedb_ops_spine.schema_health.run_health_checks",
        lambda *_args, **_kwargs: SchemaHealthReport(
            healthy=False,
            repo_ordinal=2,
            db_ordinal=1,
            extra_result=SchemaHealthExtraResult(
                name="fake_health_hooks_skip:health_hook",
                ok=True,
                skipped=True,
                message="Skipped due to ordinal drift",
            ),
        ),
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "ops-schema-health",
            "--migrations-dir",
            "migrations",
            "--database",
            "ops_db",
            "--extra-invariant",
            "fake_health_hooks_skip:health_hook",
        ],
    )

    rc = schema_health_cli.main()

    captured = capsys.readouterr()
    assert rc == 1
    assert driver.closed is True
    assert "Extra invariant 'fake_health_hooks_skip:health_hook': SKIP: ordinal drift" in captured.out
    assert "[ops-schema-health] FAIL: drift detected" in captured.out


def test_schema_health_cli_rejects_invalid_extra_invariant_shape(
    monkeypatch, capsys: pytest.CaptureFixture[str],
):
    def _boom(*_args, **_kwargs):
        raise AssertionError("connect should not be called for invalid hook spec")

    monkeypatch.setattr("typedb_ops_spine.readiness.resolve_connection_config", _boom)
    monkeypatch.setattr("typedb_ops_spine.readiness.connect_with_retries", _boom)
    monkeypatch.setattr(
        "sys.argv",
        [
            "ops-schema-health",
            "--extra-invariant",
            "bad-spec",
        ],
    )

    rc = schema_health_cli.main()

    captured = capsys.readouterr()
    assert rc == 1
    assert "Invalid --extra-invariant value" in captured.err


def test_schema_health_cli_rejects_missing_module(monkeypatch, capsys: pytest.CaptureFixture[str]):
    monkeypatch.setattr(
        "sys.argv",
        [
            "ops-schema-health",
            "--extra-invariant",
            "missing_module:health_hook",
        ],
    )

    rc = schema_health_cli.main()

    captured = capsys.readouterr()
    assert rc == 1
    assert "Failed to import extra invariant module 'missing_module'" in captured.err


def test_schema_health_cli_rejects_missing_function(monkeypatch, capsys: pytest.CaptureFixture[str]):
    module = types.ModuleType("fake_health_hooks_missing")
    monkeypatch.setitem(sys.modules, "fake_health_hooks_missing", module)
    monkeypatch.setattr(
        "sys.argv",
        [
            "ops-schema-health",
            "--extra-invariant",
            "fake_health_hooks_missing:health_hook",
        ],
    )

    rc = schema_health_cli.main()

    captured = capsys.readouterr()
    assert rc == 1
    assert "Extra invariant target not found: 'fake_health_hooks_missing:health_hook'." in captured.err


def test_schema_health_cli_rejects_non_callable_target(monkeypatch, capsys: pytest.CaptureFixture[str]):
    module = types.ModuleType("fake_health_hooks_value")
    module.health_hook = 7
    monkeypatch.setitem(sys.modules, "fake_health_hooks_value", module)
    monkeypatch.setattr(
        "sys.argv",
        [
            "ops-schema-health",
            "--extra-invariant",
            "fake_health_hooks_value:health_hook",
        ],
    )

    rc = schema_health_cli.main()

    captured = capsys.readouterr()
    assert rc == 1
    assert "Extra invariant target 'fake_health_hooks_value:health_hook' is not callable." in captured.err
