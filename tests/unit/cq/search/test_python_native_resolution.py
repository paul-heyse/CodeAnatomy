"""Tests for native Python resolution enrichment."""

from __future__ import annotations

from pathlib import Path

from tools.cq.search.python.analysis_session import get_python_analysis_session
from tools.cq.search.python.resolution_index import enrich_python_resolution_by_byte_range

_SOURCE = """\
import os as operating_system
from pkg.mod import thing as alias_thing

class C:
    def method(self, value: int) -> int:
        local = alias_thing(value)
        return local

def outer():
    target = operating_system.path.join("a", "b")
    return target
"""


def _bytes() -> bytes:
    return _SOURCE.encode("utf-8")


def _span(token: bytes, *, occurrence: int = 1) -> tuple[int, int]:
    source_bytes = _bytes()
    if occurrence == 1:
        start = source_bytes.index(token)
    else:
        start = -1
        offset = 0
        for _ in range(occurrence):
            start = source_bytes.index(token, offset)
            offset = start + len(token)
    return start, start + len(token)


def test_resolution_for_imported_call_symbol() -> None:
    start, end = _span(b"alias_thing", occurrence=2)
    payload = enrich_python_resolution_by_byte_range(
        _SOURCE,
        source_bytes=_bytes(),
        file_path="sample.py",
        byte_start=start,
        byte_end=end,
    )
    assert payload.get("symbol_role") == "read"
    assert payload.get("enclosing_callable") == "method"
    assert payload.get("enclosing_class") == "C"

    qualified = payload.get("qualified_name_candidates")
    assert isinstance(qualified, list)
    names = {row.get("name") for row in qualified if isinstance(row, dict)}
    assert "alias_thing" in names
    assert "pkg.mod.thing" in names

    bindings = payload.get("binding_candidates")
    assert isinstance(bindings, list)
    assert bindings
    first = bindings[0]
    assert isinstance(first, dict)
    assert first.get("is_imported") is True


def test_resolution_for_assignment_symbol_role() -> None:
    start, end = _span(b"target", occurrence=1)
    payload = enrich_python_resolution_by_byte_range(
        _SOURCE,
        source_bytes=_bytes(),
        file_path="sample.py",
        byte_start=start,
        byte_end=end,
    )
    assert payload.get("symbol_role") == "write"
    bindings = payload.get("binding_candidates")
    assert isinstance(bindings, list)
    assert bindings
    first = bindings[0]
    assert isinstance(first, dict)
    assert first.get("name") == "target"
    assert first.get("is_assigned") is True


def test_resolution_import_alias_chain() -> None:
    start, end = _span(b"alias_thing", occurrence=1)
    payload = enrich_python_resolution_by_byte_range(
        _SOURCE,
        source_bytes=_bytes(),
        file_path="sample.py",
        byte_start=start,
        byte_end=end,
    )
    chain = payload.get("import_alias_chain")
    assert isinstance(chain, list)
    assert {"alias": "alias_thing"} in chain
    assert {"from": "pkg.mod"} in chain


def test_resolution_session_cache_populates_index() -> None:
    session = get_python_analysis_session(Path("sample.py"), _SOURCE)
    start, end = _span(b"operating_system", occurrence=2)
    payload = enrich_python_resolution_by_byte_range(
        _SOURCE,
        source_bytes=_bytes(),
        file_path="sample.py",
        byte_start=start,
        byte_end=end,
        session=session,
    )
    assert payload
    cached = session.ensure_resolution_index()
    assert isinstance(cached, dict)
    assert "import_alias_map" in cached
    assert "definition_index" in cached


def test_resolution_fail_open_for_invalid_span_and_syntax() -> None:
    invalid_span = enrich_python_resolution_by_byte_range(
        _SOURCE,
        source_bytes=_bytes(),
        file_path="sample.py",
        byte_start=-1,
        byte_end=2,
    )
    assert invalid_span == {}

    broken = enrich_python_resolution_by_byte_range(
        "def broken(",
        source_bytes=b"def broken(",
        file_path="broken.py",
        byte_start=0,
        byte_end=3,
    )
    assert broken == {}
