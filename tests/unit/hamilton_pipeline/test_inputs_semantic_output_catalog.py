"""Tests for semantic output catalog wiring in inputs module."""

from __future__ import annotations

from pathlib import Path

from hamilton_pipeline.modules.inputs import runtime_profile_spec
from hamilton_pipeline.types import OutputConfig
from semantics.registry import SEMANTIC_MODEL


def test_runtime_profile_builds_semantic_output_catalog(tmp_path: Path) -> None:
    """runtime_profile_spec builds a default semantic output catalog."""
    output_config = OutputConfig(
        work_dir=str(tmp_path),
        output_dir=None,
        overwrite_intermediate_datasets=False,
    )
    spec = runtime_profile_spec(
        "dev",
        determinism_override=None,
        output_config=output_config,
    )
    profile = spec.datafusion
    assert profile.data_sources.semantic_output_catalog_name == "semantic_outputs"
    catalog = profile.catalog.registry_catalogs.get("semantic_outputs")
    assert catalog is not None
    expected_outputs = {spec.name for spec in SEMANTIC_MODEL.outputs}
    catalog_names = set(catalog)
    assert expected_outputs.issubset(catalog_names)
    sample_view = sorted(expected_outputs)[0]
    location = catalog.get(sample_view)
    assert str(location.path).endswith(str(Path(tmp_path) / "semantic" / sample_view))


def test_runtime_profile_respects_custom_catalog_name(tmp_path: Path) -> None:
    """runtime_profile_spec honors an explicit semantic output catalog name."""
    output_config = OutputConfig(
        work_dir=str(tmp_path),
        output_dir=None,
        overwrite_intermediate_datasets=False,
        semantic_output_catalog_name="custom_semantic_outputs",
    )
    spec = runtime_profile_spec(
        "dev",
        determinism_override=None,
        output_config=output_config,
    )
    profile = spec.datafusion
    assert profile.data_sources.semantic_output_catalog_name == "custom_semantic_outputs"
    catalog = profile.catalog.registry_catalogs.get("custom_semantic_outputs")
    assert catalog is not None
    expected_outputs = {spec.name for spec in SEMANTIC_MODEL.outputs}
    catalog_names = set(catalog)
    assert expected_outputs.issubset(catalog_names)
