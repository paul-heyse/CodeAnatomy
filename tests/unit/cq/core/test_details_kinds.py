"""Tests for details-kind registry."""

from __future__ import annotations

from tools.cq.core.details_kinds import preview_for_kind, resolve_kind


def test_details_kind_registry_resolves_known_kind() -> None:
    """Resolve a known details kind from the registry."""
    spec = resolve_kind("dis.cfg")
    assert spec is not None
    assert spec.rank > 0


def test_preview_for_kind_projects_keys() -> None:
    """Project only registered preview keys for a details kind."""
    preview = preview_for_kind("dis.cfg", {"edges_n": 5, "exc_edges_n": 1, "x": 2})
    assert preview == {"edges_n": 5, "exc_edges_n": 1}
