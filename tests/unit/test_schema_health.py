from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

from typedb_ops_spine import schema_health
from typedb_ops_spine.schema_health import (
    SchemaHealthExtraResult,
    SchemaHealthReport,
    check_health,
    run_health_checks,
)


def test_check_health_preserves_legacy_tuple(monkeypatch):
    monkeypatch.setattr(schema_health, "repo_head_ordinal", lambda *_args, **_kwargs: 3)
    monkeypatch.setattr(schema_health, "db_current_ordinal", lambda *_args, **_kwargs: 3)

    result = check_health(MagicMock(), "ops_db", "migrations")

    assert result == (True, 3, 3)


def test_run_health_checks_returns_healthy_report_without_hook(monkeypatch):
    monkeypatch.setattr(schema_health, "repo_head_ordinal", lambda *_args, **_kwargs: 5)
    monkeypatch.setattr(schema_health, "db_current_ordinal", lambda *_args, **_kwargs: 5)

    report = run_health_checks(MagicMock(), "ops_db", "migrations")

    assert report == SchemaHealthReport(
        healthy=True,
        repo_ordinal=5,
        db_ordinal=5,
        extra_result=None,
    )


def test_run_health_checks_extra_invariant_true(monkeypatch):
    monkeypatch.setattr(schema_health, "repo_head_ordinal", lambda *_args, **_kwargs: 2)
    monkeypatch.setattr(schema_health, "db_current_ordinal", lambda *_args, **_kwargs: 2)

    report = run_health_checks(
        MagicMock(),
        "ops_db",
        "migrations",
        extra_invariant=lambda *_args, **_kwargs: True,
        extra_name="pkg.hooks:ok",
    )

    assert report.healthy is True
    assert report.extra_result == SchemaHealthExtraResult(name="pkg.hooks:ok", ok=True)


def test_run_health_checks_extra_invariant_false(monkeypatch):
    monkeypatch.setattr(schema_health, "repo_head_ordinal", lambda *_args, **_kwargs: 2)
    monkeypatch.setattr(schema_health, "db_current_ordinal", lambda *_args, **_kwargs: 2)

    report = run_health_checks(
        MagicMock(),
        "ops_db",
        "migrations",
        extra_invariant=lambda *_args, **_kwargs: False,
        extra_name="pkg.hooks:fail",
    )

    assert report.healthy is False
    assert report.extra_result == SchemaHealthExtraResult(name="pkg.hooks:fail", ok=False)


def test_run_health_checks_uses_named_extra_result(monkeypatch):
    monkeypatch.setattr(schema_health, "repo_head_ordinal", lambda *_args, **_kwargs: 2)
    monkeypatch.setattr(schema_health, "db_current_ordinal", lambda *_args, **_kwargs: 2)

    report = run_health_checks(
        MagicMock(),
        "ops_db",
        "migrations",
        extra_invariant=lambda *_args, **_kwargs: SchemaHealthExtraResult(
            name="custom_check",
            ok=False,
            message="bad invariant",
        ),
        extra_name="pkg.hooks:custom",
    )

    assert report.healthy is False
    assert report.extra_result == SchemaHealthExtraResult(
        name="custom_check",
        ok=False,
        message="bad invariant",
    )


def test_run_health_checks_fills_empty_result_name_from_extra_name(monkeypatch):
    monkeypatch.setattr(schema_health, "repo_head_ordinal", lambda *_args, **_kwargs: 2)
    monkeypatch.setattr(schema_health, "db_current_ordinal", lambda *_args, **_kwargs: 2)

    report = run_health_checks(
        MagicMock(),
        "ops_db",
        "migrations",
        extra_invariant=lambda *_args, **_kwargs: SchemaHealthExtraResult(
            name="",
            ok=False,
            message="bad invariant",
        ),
        extra_name="pkg.hooks:custom",
    )

    assert report.extra_result == SchemaHealthExtraResult(
        name="pkg.hooks:custom",
        ok=False,
        message="bad invariant",
    )


def test_run_health_checks_fails_closed_on_exception(monkeypatch):
    monkeypatch.setattr(schema_health, "repo_head_ordinal", lambda *_args, **_kwargs: 4)
    monkeypatch.setattr(schema_health, "db_current_ordinal", lambda *_args, **_kwargs: 4)

    def _boom(*_args, **_kwargs):
        raise RuntimeError("hook exploded")

    report = run_health_checks(
        MagicMock(),
        "ops_db",
        "migrations",
        extra_invariant=_boom,
        extra_name="pkg.hooks:boom",
    )

    assert report.healthy is False
    assert report.extra_result == SchemaHealthExtraResult(
        name="pkg.hooks:boom",
        ok=False,
        message="hook exploded",
        details={"error_class": "RuntimeError"},
    )


def test_run_health_checks_skips_extra_invariant_on_drift(monkeypatch):
    calls: list[str] = []
    monkeypatch.setattr(schema_health, "repo_head_ordinal", lambda *_args, **_kwargs: 7)
    monkeypatch.setattr(schema_health, "db_current_ordinal", lambda *_args, **_kwargs: 6)

    def _hook(*_args, **_kwargs):
        calls.append("called")
        return True

    report = run_health_checks(
        MagicMock(),
        "ops_db",
        "migrations",
        extra_invariant=_hook,
        extra_name="pkg.hooks:skip",
    )

    assert calls == []
    assert report == SchemaHealthReport(
        healthy=False,
        repo_ordinal=7,
        db_ordinal=6,
        extra_result=SchemaHealthExtraResult(
            name="pkg.hooks:skip",
            ok=True,
            skipped=True,
            message="Skipped due to ordinal drift",
        ),
    )


def test_run_health_checks_emits_extra_invariant_diagnostics(monkeypatch, tmp_path: Path):
    diag_path = tmp_path / "schema_health_diagnostics.jsonl"
    monkeypatch.setenv("CI_ARTIFACTS_DIR", str(tmp_path))
    monkeypatch.setattr(schema_health, "repo_head_ordinal", lambda *_args, **_kwargs: 2)
    monkeypatch.setattr(schema_health, "db_current_ordinal", lambda *_args, **_kwargs: 2)

    report = run_health_checks(
        MagicMock(),
        "ops_db",
        "migrations",
        extra_invariant=lambda *_args, **_kwargs: SchemaHealthExtraResult(
            name="diag_check",
            ok=False,
            message="diag failed",
            details={"failure_count": 3},
        ),
        extra_name="pkg.hooks:diag",
    )

    assert report.healthy is False
    records = [json.loads(line) for line in diag_path.read_text(encoding="utf-8").splitlines()]
    extra_records = [record for record in records if record["stage"] == "extra_invariant"]
    assert len(extra_records) == 1
    assert extra_records[0]["status"] == "fail"
    assert extra_records[0]["extra_name"] == "diag_check"
    assert extra_records[0]["error_message"] == "diag failed"
    assert extra_records[0]["failure_count"] == 3
