from __future__ import annotations

from pathlib import Path


def test_migrate_uses_callable_query_api_and_select_pipeline():
    migrate_content = Path("typedb_ops_spine/migrate.py").read_text(encoding="utf-8")
    schema_version_content = Path("typedb_ops_spine/schema_version.py").read_text(
        encoding="utf-8"
    )

    forbidden = [".query.define", ".query.get", ".query.insert", "get $o;", "fetch $o;"]
    for token in forbidden:
        assert token not in migrate_content, (
            f"Forbidden legacy driver surface or syntax found: {token}"
        )
        assert token not in schema_version_content, (
            f"Forbidden legacy driver surface or syntax found: {token}"
        )

    assert "tx.query(" in migrate_content
    assert "tx.query(" in schema_version_content
    assert (
        "match $v isa schema_version, has ordinal $o; select $o;"
        in schema_version_content
    )


def test_schema_health_uses_select_not_fetch_for_schema_version_queries():
    content = Path("typedb_ops_spine/schema_health.py").read_text(encoding="utf-8")

    assert "match $v isa schema_version, has ordinal $o; select $o;" in content
    assert "fetch $o;" not in content
