"""Tests for semantic cache policy resolution."""

from __future__ import annotations

import pytest

from datafusion_engine.views.registry_specs import _semantic_cache_policy_for_row
from semantics.catalog.dataset_rows import SemanticDatasetRow
from semantics.runtime import SemanticRuntimeConfig


@pytest.mark.parametrize(
    ("row", "runtime_config", "expected"),
    [
        (
            SemanticDatasetRow(
                name="example_v1",
                version=1,
                bundles=(),
                fields=("id",),
                category="semantic",
                supports_cdf=True,
                merge_keys=("id",),
            ),
            SemanticRuntimeConfig(cache_policy_overrides={"example_v1": "none"}),
            "none",
        ),
        (
            SemanticDatasetRow(
                name="example_v1",
                version=1,
                bundles=(),
                fields=("id",),
                category="semantic",
                supports_cdf=True,
                merge_keys=("id",),
            ),
            SemanticRuntimeConfig(output_locations={"example_v1": "/tmp/example"}),
            "delta_output",
        ),
        (
            SemanticDatasetRow(
                name="example_v1",
                version=1,
                bundles=(),
                fields=("id",),
                category="analysis",
                supports_cdf=False,
                merge_keys=None,
            ),
            SemanticRuntimeConfig(),
            "none",
        ),
        (
            SemanticDatasetRow(
                name="example_v1",
                version=1,
                bundles=(),
                fields=("id",),
                category="semantic",
                supports_cdf=True,
                merge_keys=("id",),
            ),
            SemanticRuntimeConfig(cdf_enabled=True),
            "delta_staging",
        ),
        (
            SemanticDatasetRow(
                name="example_v1",
                version=1,
                bundles=(),
                fields=("id",),
                category="diagnostic",
                supports_cdf=False,
                merge_keys=None,
            ),
            SemanticRuntimeConfig(),
            "none",
        ),
    ],
)
def test_semantic_cache_policy_for_row(
    row: SemanticDatasetRow,
    runtime_config: SemanticRuntimeConfig,
    expected: str,
) -> None:
    assert _semantic_cache_policy_for_row(row, runtime_config=runtime_config) == expected
