"""Tests for authority-mode DataSourceConfig builders."""

from __future__ import annotations

from pathlib import Path

import pytest

from datafusion_engine.dataset.registry import DatasetLocation
from datafusion_engine.session.runtime import (
    DataFusionRuntimeProfile,
    DataSourceConfig,
    ExtractOutputConfig,
    SemanticOutputConfig,
    datasource_config_from_manifest,
    datasource_config_from_profile,
)
from semantics.ir import SemanticIR
from semantics.program_manifest import ManifestDatasetBindings, SemanticProgramManifest


def _manifest_with_locations(locations: dict[str, DatasetLocation]) -> SemanticProgramManifest:
    return SemanticProgramManifest(
        semantic_ir=SemanticIR(views=()),
        requested_outputs=(),
        input_mapping={},
        validation_policy="schema_only",
        dataset_bindings=ManifestDatasetBindings(locations=locations),
    )


def test_datasource_config_from_manifest_fails_closed_when_bindings_empty() -> None:
    """Test datasource config from manifest fails closed when bindings empty."""
    profile = DataFusionRuntimeProfile()
    manifest = _manifest_with_locations({})
    with pytest.raises(ValueError, match="Manifest dataset bindings are empty"):
        _ = datasource_config_from_manifest(manifest, runtime_profile=profile)


def test_datasource_config_from_manifest_uses_manifest_bindings() -> None:
    """Test datasource config from manifest uses manifest bindings."""
    profile = DataFusionRuntimeProfile(
        data_sources=DataSourceConfig(
            extract_output=ExtractOutputConfig(output_root="/tmp/extract"),
            semantic_output=SemanticOutputConfig(output_root="/tmp/semantic"),
        )
    )
    manifest = _manifest_with_locations(
        {
            "repo_snapshot": DatasetLocation(path="/tmp/repo_snapshot", format="delta"),
            "cpg_nodes": DatasetLocation(path="/tmp/cpg_nodes", format="delta"),
        }
    )

    config = datasource_config_from_manifest(manifest, runtime_profile=profile)

    assert set(config.dataset_templates) == {"repo_snapshot", "cpg_nodes"}
    assert str(config.dataset_templates["cpg_nodes"].path) == "/tmp/cpg_nodes"
    assert config.extract_output.output_root == "/tmp/extract"
    assert config.semantic_output.output_root == "/tmp/semantic"


def test_datasource_config_from_profile_uses_bootstrap_derivations(tmp_path: Path) -> None:
    """Test datasource config from profile uses bootstrap derivations."""
    normalize_root = tmp_path / "normalize"
    profile = DataFusionRuntimeProfile(
        data_sources=DataSourceConfig(
            semantic_output=SemanticOutputConfig(
                normalize_output_root=str(normalize_root),
                output_root=str(tmp_path / "semantic"),
            ),
            extract_output=ExtractOutputConfig(output_root=str(tmp_path / "extract")),
        )
    )

    config = datasource_config_from_profile(profile)

    assert config.extract_output.output_root == str(tmp_path / "extract")
    assert config.semantic_output.normalize_output_root == str(normalize_root)
    assert config.dataset_templates
    sample_location = next(iter(config.dataset_templates.values()))
    assert str(sample_location.path).startswith(str(normalize_root))
