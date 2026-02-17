"""Tests for rust-lane payload assembly helpers."""

from __future__ import annotations

from tools.cq.search.tree_sitter.rust_lane.payload_assembly import assemble_query_pack_payload


class _Node:
    pass


def test_assemble_query_pack_payload_delegates_to_runtime(monkeypatch) -> None:
    expected = {"query_runtime": {"cancelled": False}}

    def _fake_collect(**kwargs: object) -> dict[str, object]:
        assert kwargs["byte_span"] == (1, 2)
        return expected

    monkeypatch.setattr(
        "tools.cq.search.tree_sitter.rust_lane.runtime._collect_query_pack_payload",
        _fake_collect,
    )

    payload = assemble_query_pack_payload(
        root=_Node(),
        source_bytes=b"fn x() {}",
        byte_span=(1, 2),
    )

    assert payload == expected
