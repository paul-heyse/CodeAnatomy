"""Tests for semantic cache policy resolution."""

from __future__ import annotations

import msgspec
import pytest

from datafusion_engine.dataset.registry import DatasetLocation
from datafusion_engine.session.runtime import DataFusionRuntimeProfile
from datafusion_engine.views.registry_specs import _semantic_cache_policy_for_row
from semantics.catalog.dataset_rows import SemanticDatasetRow


@pytest.mark.parametrize(
    ("row", "profile", "expected"),
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
            DataFusionRuntimeProfile(
                data_sources=msgspec.structs.replace(
                    DataFusionRuntimeProfile().data_sources,
                    semantic_output=msgspec.structs.replace(
                        DataFusionRuntimeProfile().data_sources.semantic_output,
                        cache_overrides={"example_v1": "none"},
                    ),
                ),
            ),
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
            DataFusionRuntimeProfile(
                data_sources=msgspec.structs.replace(
                    DataFusionRuntimeProfile().data_sources,
                    semantic_output=msgspec.structs.replace(
                        DataFusionRuntimeProfile().data_sources.semantic_output,
                        locations={
                            "example_v1": DatasetLocation(
                                path="/tmp/example",
                                format="delta",
                            )
                        },
                    ),
                ),
            ),
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
            DataFusionRuntimeProfile(),
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
            DataFusionRuntimeProfile(
                features=msgspec.structs.replace(
                    DataFusionRuntimeProfile().features,
                    enable_delta_cdf=True,
                ),
            ),
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
            DataFusionRuntimeProfile(),
            "none",
        ),
    ],
)
def test_semantic_cache_policy_for_row(
    row: SemanticDatasetRow,
    profile: DataFusionRuntimeProfile,
    expected: str,
) -> None:
    assert _semantic_cache_policy_for_row(row, runtime_profile=profile) == expected
