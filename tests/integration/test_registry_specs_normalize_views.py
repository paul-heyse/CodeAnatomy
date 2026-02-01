"""Integration tests for registry_specs normalize view nodes."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING

import pyarrow as pa
import pytest

import semantics.catalog.dataset_rows as semantic_dataset_rows
from datafusion_engine.session.runtime import DataFusionRuntimeProfile
from datafusion_engine.views import registry_specs
from semantics.input_registry import SEMANTIC_INPUT_SPECS
from tests.test_helpers.arrow_seed import register_arrow_table

if TYPE_CHECKING:
    from datafusion import SessionContext

    from semantics.catalog.analysis_builders import DataFrameBuilder
    from semantics.config import SemanticConfig


def _seed_semantic_inputs(ctx: SessionContext) -> None:
    for spec in SEMANTIC_INPUT_SPECS:
        table = pa.table({"dummy": pa.array([], type=pa.string())})
        register_arrow_table(ctx, name=spec.extraction_source, value=table)


def test_normalize_view_nodes_builds_diagnostics_bundle(monkeypatch: pytest.MonkeyPatch) -> None:
    """Normalize view nodes build bundles with resolved semantic inputs."""
    profile = DataFusionRuntimeProfile()
    ctx = profile.session_context()
    _seed_semantic_inputs(ctx)

    row = semantic_dataset_rows.dataset_row("diagnostics_norm_v1", strict=True)
    monkeypatch.setattr(
        semantic_dataset_rows,
        "get_all_dataset_rows",
        lambda: (row,),
    )

    captured: dict[str, str] = {}
    import semantics.catalog.view_builders as view_builders_module

    original = view_builders_module.view_builders

    def _wrapped(
        *,
        input_mapping: Mapping[str, str],
        config: SemanticConfig | None,
    ) -> dict[str, DataFrameBuilder]:
        captured.update(input_mapping)
        return original(input_mapping=input_mapping, config=config)

    monkeypatch.setattr(view_builders_module, "view_builders", _wrapped)

    nodes = registry_specs.view_graph_nodes(
        ctx,
        snapshot={},
        runtime_profile=profile,
        stage="all",
    )
    assert len(nodes) == 1
    node = nodes[0]
    assert node.name == "diagnostics_norm_v1"
    assert node.plan_bundle is not None
    assert node.cache_policy == "delta_output"

    expected = {spec.canonical_name for spec in SEMANTIC_INPUT_SPECS}
    assert expected.issubset(set(captured))
