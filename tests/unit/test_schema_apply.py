from __future__ import annotations

import pytest

from typedb_ops_spine.schema_apply import (
    parse_canonical_caps,
    parse_undefine_owns_spec,
    parse_undefine_plays_spec,
    plan_auto_migrations,
    resolve_schema_files,
)


def test_resolve_schema_files_rejects_empty_inputs():
    with pytest.raises(ValueError, match="No schema paths provided"):
        resolve_schema_files([])

    with pytest.raises(ValueError, match="Empty schema argument is invalid"):
        resolve_schema_files([""])

    with pytest.raises(ValueError, match="Empty schema argument is invalid"):
        resolve_schema_files(["   "])


def test_resolve_schema_files_rejects_triple_star():
    with pytest.raises(FileNotFoundError, match="Invalid schema glob pattern"):
        resolve_schema_files(["schemas/***.tql"])


def test_plan_auto_migrations_detects_inherited_owns_redeclarations():
    schema = """
    entity evidence @abstract,
        owns template-id;

    entity validation-evidence sub evidence,
        owns template-id;
    """

    parent_of, owns_of, plays_of = parse_canonical_caps(schema)
    owns_specs, plays_specs = plan_auto_migrations(parent_of, owns_of, plays_of)

    assert parent_of.get("validation-evidence") == "evidence"
    assert ("validation-evidence", "template-id") in owns_specs
    assert plays_specs == []


def test_plan_auto_migrations_detects_inherited_plays_redeclarations():
    schema = """
    entity evidence @abstract,
        plays session-has-evidence:evidence;

    entity validation-evidence sub evidence,
        plays session-has-evidence:evidence;
    """

    parent_of, owns_of, plays_of = parse_canonical_caps(schema)
    owns_specs, plays_specs = plan_auto_migrations(parent_of, owns_of, plays_of)

    assert owns_specs == []
    assert ("validation-evidence", "session-has-evidence:evidence") in plays_specs


def test_plan_auto_migrations_handles_comments_and_edge_cases():
    schema = """
    # entity old-evidence sub entity, owns legacy-id;

    entity evidence sub entity,
        owns template-id; # inline comment

    entity validation-evidence sub evidence,
        owns template-id,
        owns validation-only-attr;

    relation session-has-evidence sub relation,
        relates session,
        relates evidence;

    entity owns-metadata sub entity,
        owns metadata-id;
    """

    parent_of, owns_of, plays_of = parse_canonical_caps(schema)
    owns_specs, plays_specs = plan_auto_migrations(parent_of, owns_of, plays_of)

    assert "old-evidence" not in parent_of
    assert parent_of.get("validation-evidence") == "evidence"
    assert "template-id" in owns_of.get("evidence", set())
    assert ("validation-evidence", "template-id") in owns_specs
    assert not plays_specs
    assert not any(type_label == "owns-metadata" for type_label, _ in owns_specs)


def test_parse_undefine_owns_spec_validates_format():
    assert parse_undefine_owns_spec("entity-name:attribute-name") == (
        "entity-name",
        "attribute-name",
    )

    with pytest.raises(ValueError, match="Invalid --undefine-owns spec"):
        parse_undefine_owns_spec("entity-only")


def test_parse_undefine_plays_spec_validates_format():
    assert parse_undefine_plays_spec("type-name:relation-name:role-name") == (
        "type-name",
        "relation-name:role-name",
    )

    with pytest.raises(ValueError, match="Invalid --undefine-plays spec"):
        parse_undefine_plays_spec("type-only")
