"""Tests for semantic output materialization helpers."""

from __future__ import annotations

from typing import cast

import pytest

from datafusion_engine.dataset.registry import DatasetLocation
from semantics.output_materialization import (
    ensure_semantic_output_locations,
    semantic_output_view_names,
)
from semantics.registry import SEMANTIC_MODEL


class _ManifestStub:
    output_name_map: dict[str, str] | None = None


def test_semantic_output_view_names_includes_relation_output() -> None:
    """Semantic output names include the synthetic relation output view."""
    names = semantic_output_view_names(model=SEMANTIC_MODEL, manifest=_ManifestStub())

    assert "relation_output" in names


def test_ensure_semantic_output_locations_requires_all_views() -> None:
    """Missing output locations raise a clear ValueError."""
    with pytest.raises(ValueError, match="Missing locations"):
        ensure_semantic_output_locations(
            ["relation_output", "cpg_nodes"],
            cast("dict[str, DatasetLocation]", {"relation_output": object()}),
        )
