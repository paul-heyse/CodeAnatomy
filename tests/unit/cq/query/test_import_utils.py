"""Tests for shared query import parsing helpers."""

from __future__ import annotations

from tools.cq.query.import_utils import (
    extract_from_import_alias,
    extract_from_import_name,
    extract_from_module_name,
    extract_rust_use_import_name,
    extract_simple_import_alias,
    extract_simple_import_name,
    parse_import_bindings,
)

EXPECTED_FROM_BINDING_COUNT = 2


def test_extract_python_import_shapes() -> None:
    """Verify Python import helper parsing for common import syntaxes."""
    assert extract_simple_import_name("import foo.bar") == "foo.bar"
    assert extract_simple_import_alias("import foo.bar as fb") == "fb"
    assert extract_from_import_name("from pkg.mod import item") == "item"
    assert extract_from_import_alias("from pkg.mod import item as alias") == "alias"
    assert extract_from_module_name("from pkg.mod import item, other") == "pkg.mod"


def test_extract_rust_use_name() -> None:
    """Verify Rust `use` helper parsing for direct and aliased forms."""
    assert extract_rust_use_import_name("use std::collections::HashMap;") == "HashMap"
    assert extract_rust_use_import_name("use crate::foo::Bar as Baz;") == "Baz"


def test_parse_import_bindings_for_import_and_from_import() -> None:
    """Verify normalized import bindings for import/from-import statements."""
    import_bindings = parse_import_bindings("import pkg.mod as alias")
    assert len(import_bindings) == 1
    assert import_bindings[0].local_name == "alias"
    assert import_bindings[0].source_module == "pkg.mod"
    assert import_bindings[0].is_from_import is False

    from_bindings = parse_import_bindings("from pkg.mod import one, two as two_alias")
    assert len(from_bindings) == EXPECTED_FROM_BINDING_COUNT
    assert from_bindings[0].local_name == "one"
    assert from_bindings[0].source_module == "pkg.mod"
    assert from_bindings[1].local_name == "two_alias"
    assert from_bindings[1].source_name == "two"
    assert from_bindings[1].is_from_import is True
