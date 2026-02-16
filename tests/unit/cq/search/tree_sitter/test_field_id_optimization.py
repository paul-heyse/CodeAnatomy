"""Tests for field-id fast-path helpers."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from tools.cq.search.tree_sitter.core import infrastructure


def test_child_by_field_prefers_field_id_path() -> None:
    """Prefer field-id lookup path when field ID is available."""

    class _Node:
        def __init__(self) -> None:
            self.name_by_id = {1: "id_child"}
            self.name_by_name = {"name": "name_child"}

        def child_by_field_id(self, field_id: int) -> object | None:
            return self.name_by_id.get(field_id)

        def child_by_field_name(self, field_name: str) -> object | None:
            return self.name_by_name.get(field_name)

    node = _Node()
    child = infrastructure.child_by_field(node, "name", {"name": 1})
    assert child == "id_child"


def test_child_by_field_falls_back_to_field_name() -> None:
    """Fallback to name lookup when field-id lookup is unavailable."""

    class _Node:
        @staticmethod
        def child_by_field_id(_field_id: int) -> object | None:
            return None

        @staticmethod
        def child_by_field_name(field_name: str) -> object | None:
            return f"name:{field_name}"

    child = infrastructure.child_by_field(_Node(), "module", {"module": 2})
    assert child == "name:module"


def test_cached_field_ids_passes_schema_candidates(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test cached field ids passes schema candidates."""
    captured: dict[str, object] = {}
    monkeypatch.setattr(infrastructure, "load_language", lambda _language: object())
    monkeypatch.setattr(
        infrastructure,
        "load_grammar_schema",
        lambda _language: SimpleNamespace(
            node_types=(
                SimpleNamespace(fields=("name", "body")),
                SimpleNamespace(fields=("function",)),
            ),
        ),
    )

    def _fake_build_runtime_field_ids(
        _language: object,
        *,
        field_names: tuple[str, ...] | None = None,
    ) -> dict[str, int]:
        captured["field_names"] = field_names
        return {"name": 1}

    monkeypatch.setattr(infrastructure, "build_runtime_field_ids", _fake_build_runtime_field_ids)
    out = infrastructure.cached_field_ids("python_field_ids_test")
    assert out == {"name": 1}
    assert captured["field_names"] == ("body", "function", "name")
