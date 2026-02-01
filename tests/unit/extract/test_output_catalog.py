"""Tests for extract output catalog construction."""

from __future__ import annotations

from pathlib import Path

from datafusion_engine.extract.metadata import extract_metadata_specs
from datafusion_engine.extract.output_catalog import build_extract_output_catalog
from datafusion_engine.extract.registry import dataset_spec as extract_dataset_spec
from datafusion_engine.session.runtime import (
    DataFusionRuntimeProfile,
    extract_output_locations_for_profile,
)
from tests.test_helpers.optional_deps import require_datafusion

require_datafusion()


def test_build_extract_output_catalog_registers_metadata(tmp_path: Path) -> None:
    """Ensure catalog registers extract metadata datasets with Delta locations."""
    catalog = build_extract_output_catalog(output_root=str(tmp_path))
    rows = extract_metadata_specs()
    assert rows
    sample = rows[0]
    location = catalog.get(sample.name)
    assert location.path == str(tmp_path / sample.name)
    assert location.format == "delta"
    assert location.dataset_spec == extract_dataset_spec(sample.name)


def test_extract_output_locations_for_profile_uses_root(tmp_path: Path) -> None:
    """Ensure runtime profile helper builds locations from extract output root."""
    profile = DataFusionRuntimeProfile(extract_output_root=str(tmp_path))
    locations = extract_output_locations_for_profile(profile)
    rows = extract_metadata_specs()
    assert rows
    sample = rows[0]
    assert sample.name in locations
    location = locations[sample.name]
    assert location.path == str(tmp_path / sample.name)
    assert location.format == "delta"
