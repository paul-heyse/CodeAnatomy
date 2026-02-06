"""Unified semantic dataset catalog builder.

This module provides a unified dataset catalog that merges semantic outputs
with extract outputs into a single DatasetCatalog. This enables a single
point of registration for all pipeline datasets.
"""

from __future__ import annotations

from pathlib import Path

from datafusion_engine.dataset.registry import (
    DatasetCatalog,
    DatasetLocation,
    DatasetLocationOverrides,
)
from semantics.catalog.dataset_rows import get_all_dataset_rows
from semantics.catalog.spec_builder import build_dataset_spec
from semantics.naming import canonical_output_name


def build_semantic_dataset_catalog(
    *,
    semantic_output_root: str | None = None,
    extract_output_root: str | None = None,
) -> DatasetCatalog:
    """Merge semantic + extract dataset locations into unified catalog.

    Args:
        semantic_output_root: Optional root directory for semantic outputs.
        extract_output_root: Optional root directory for extract outputs.

    Returns:
        DatasetCatalog: Result.

    Raises:
        ValueError: If a semantic dataset row is not canonically named.
    """
    from schema_spec.dataset_spec_ops import (
        dataset_spec_delta_cdf_policy,
        dataset_spec_delta_constraints,
        dataset_spec_delta_feature_gate,
        dataset_spec_delta_maintenance_policy,
        dataset_spec_delta_schema_policy,
        dataset_spec_delta_write_policy,
    )
    from schema_spec.system import DeltaPolicyBundle

    catalog = DatasetCatalog()

    if semantic_output_root is not None:
        semantic_root = Path(semantic_output_root)
        for row in get_all_dataset_rows():
            # Skip rows that don't register as views (internal intermediates)
            if not row.register_view:
                continue
            canonical_name = canonical_output_name(row.name)
            if canonical_name != row.name:
                msg = (
                    "Semantic dataset rows must use canonical output names. "
                    f"Got {row.name!r}, expected {canonical_name!r}."
                )
                raise ValueError(msg)
            spec = build_dataset_spec(row)
            catalog.register(
                row.name,
                DatasetLocation(
                    path=str(semantic_root / row.name),
                    format="delta",
                    dataset_spec=spec,
                    overrides=DatasetLocationOverrides(
                        delta=DeltaPolicyBundle(
                            cdf_policy=dataset_spec_delta_cdf_policy(spec),
                            maintenance_policy=dataset_spec_delta_maintenance_policy(spec),
                            write_policy=dataset_spec_delta_write_policy(spec),
                            schema_policy=dataset_spec_delta_schema_policy(spec),
                            feature_gate=dataset_spec_delta_feature_gate(spec),
                            constraints=dataset_spec_delta_constraints(spec),
                        ),
                    ),
                ),
            )

    if extract_output_root is not None:
        # Merge extract outputs
        from datafusion_engine.extract.output_catalog import build_extract_output_catalog

        extract_catalog = build_extract_output_catalog(output_root=extract_output_root)
        for name in extract_catalog.names():
            if catalog.has(name):
                # Semantic catalog takes precedence
                continue
            catalog.register(name, extract_catalog.get(name))

    return catalog


def semantic_dataset_location(
    name: str,
    *,
    output_root: str,
) -> DatasetLocation:
    """Return a DatasetLocation for a single semantic dataset.

    Args:
        name: Description.
        output_root: Description.

    Raises:
        KeyError: If the operation cannot be completed.
    """
    from schema_spec.dataset_spec_ops import (
        dataset_spec_delta_cdf_policy,
        dataset_spec_delta_constraints,
        dataset_spec_delta_feature_gate,
        dataset_spec_delta_maintenance_policy,
        dataset_spec_delta_schema_policy,
        dataset_spec_delta_write_policy,
    )
    from schema_spec.system import DeltaPolicyBundle
    from semantics.catalog.dataset_rows import dataset_row

    row = dataset_row(name, strict=False)
    if row is None:
        msg = f"Dataset not found: {name}"
        raise KeyError(msg)
    spec = build_dataset_spec(row)
    root = Path(output_root)

    return DatasetLocation(
        path=str(root / name),
        format="delta",
        dataset_spec=spec,
        overrides=DatasetLocationOverrides(
            delta=DeltaPolicyBundle(
                cdf_policy=dataset_spec_delta_cdf_policy(spec),
                maintenance_policy=dataset_spec_delta_maintenance_policy(spec),
                write_policy=dataset_spec_delta_write_policy(spec),
                schema_policy=dataset_spec_delta_schema_policy(spec),
                feature_gate=dataset_spec_delta_feature_gate(spec),
                constraints=dataset_spec_delta_constraints(spec),
            ),
        ),
    )


__all__ = [
    "build_semantic_dataset_catalog",
    "semantic_dataset_location",
]
