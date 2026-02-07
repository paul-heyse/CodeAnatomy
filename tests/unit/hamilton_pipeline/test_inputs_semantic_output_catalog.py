"""Tests for semantic output catalog wiring in inputs module."""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import pytest

from hamilton_pipeline.modules import inputs as inputs_module
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
    assert profile.data_sources.semantic_output.output_catalog_name == "semantic_outputs"
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
    assert profile.data_sources.semantic_output.output_catalog_name == "custom_semantic_outputs"
    catalog = profile.catalog.registry_catalogs.get("custom_semantic_outputs")
    assert catalog is not None
    expected_outputs = {spec.name for spec in SEMANTIC_MODEL.outputs}
    catalog_names = set(catalog)
    assert expected_outputs.issubset(catalog_names)


def test_runtime_profile_spec_uses_bootstrap_datasource_builder(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """runtime_profile_spec routes data-source construction through bootstrap builder."""
    calls: list[dict[str, object]] = []
    original_builder = cast("Any", inputs_module.datasource_config_from_profile)

    def _builder(profile: Any, **kwargs: Any) -> Any:
        calls.append({"profile": profile, **kwargs})
        return original_builder(profile, **kwargs)

    monkeypatch.setattr(inputs_module, "datasource_config_from_profile", _builder)

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

    assert len(calls) == 1
    assert calls[0]["profile"] is not None
    assert calls[0]["semantic_output"] is not None
    assert calls[0]["extract_output"] is not None
    semantic_output = cast("Any", calls[0]["semantic_output"])
    extract_output = cast("Any", calls[0]["extract_output"])
    assert spec.datafusion.data_sources.semantic_output == semantic_output
    assert spec.datafusion.data_sources.extract_output == extract_output
