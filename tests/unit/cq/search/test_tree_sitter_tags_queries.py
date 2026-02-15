"""Contract checks for Rust tags query pack."""

from __future__ import annotations

from pathlib import Path

_DEF_TOKEN = "@role.definition"
_REF_TOKEN = "@role.reference"


def test_rust_tags_query_contains_definition_and_reference_roles() -> None:
    source = Path("tools/cq/search/queries/rust/80_tags.scm").read_text(encoding="utf-8")
    assert _DEF_TOKEN in source
    assert _REF_TOKEN in source
    assert "cq.emit" in source
    assert "cq.kind" in source
    assert "cq.anchor" in source
