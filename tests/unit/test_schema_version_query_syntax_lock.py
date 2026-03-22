from __future__ import annotations

from pathlib import Path


def test_migrate_uses_callable_query_api_and_select_pipeline():
    content = Path("typedb_ops_spine/migrate.py").read_text(encoding="utf-8")

    forbidden = [".query.define", ".query.get", ".query.insert", "get $o;", "fetch $o;"]
    for token in forbidden:
        assert token not in content, f"Forbidden legacy driver surface or syntax found: {token}"

    assert "tx.query(" in content
    assert "match $v isa schema_version, has ordinal $o; select $o;" in content


def test_schema_health_uses_select_not_fetch_for_schema_version_queries():
    content = Path("typedb_ops_spine/schema_health.py").read_text(encoding="utf-8")

    assert "match $v isa schema_version, has ordinal $o; select $o;" in content
    assert "fetch $o;" not in content
