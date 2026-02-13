"""LDMD root alias behavior tests."""

from __future__ import annotations

from tools.cq.ldmd.format import build_index, get_neighbors, get_slice


def _sample_content() -> bytes:
    return b"""<!--LDMD:BEGIN id="section_a"-->
alpha
<!--LDMD:END id="section_a"-->
<!--LDMD:BEGIN id="section_b"-->
beta
<!--LDMD:END id="section_b"-->
"""


def test_get_slice_root_alias_uses_first_section() -> None:
    content = _sample_content()
    index = build_index(content)
    slice_data = get_slice(content, index, section_id="root")
    assert b"section_a" in slice_data
    assert b"section_b" not in slice_data


def test_get_neighbors_root_alias_uses_first_section() -> None:
    content = _sample_content()
    index = build_index(content)
    nav = get_neighbors(index, section_id="root")
    assert nav["section_id"] == "section_a"
    assert nav["prev"] is None
    assert nav["next"] == "section_b"
