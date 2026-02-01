"""Unified semantic dataset catalog builder.

This module provides a unified dataset catalog that merges semantic outputs
with extract outputs into a single DatasetCatalog. This enables a single
point of registration for all pipeline datasets.
"""

from __future__ import annotations

from pathlib import Path

from datafusion_engine.dataset.registry import DatasetCatalog, DatasetLocation
from semantics.catalog.dataset_rows import get_all_dataset_rows
from semantics.catalog.spec_builder import build_dataset_spec
from semantics.naming import canonical_output_name


def build_semantic_dataset_catalog(
    *,
    semantic_output_root: str | None = None,
    extract_output_root: str | None = None,
) -> DatasetCatalog:
    """Merge semantic + extract dataset locations into unified catalog.

    Builds a complete dataset catalog containing:
    - Semantic pipeline outputs (normalization, relationships, CPG)
    - Extract outputs (optional, when extract_output_root is provided)

    Parameters
    ----------
    semantic_output_root
        Root directory for semantic output datasets. When provided,
        all semantic dataset rows are registered with paths under
        this root.
    extract_output_root
        Root directory for extract output datasets. When provided,
        extract datasets are merged into the catalog.

    Returns
    -------
    DatasetCatalog
        Unified catalog containing all registered dataset locations.

    Raises
    ------
    ValueError
        Raised when semantic dataset rows are not using canonical output names.

    Examples
    --------
    >>> catalog = build_semantic_dataset_catalog(
    ...     semantic_output_root="/data/semantic",
    ...     extract_output_root="/data/extract",
    ... )
    >>> catalog.has("cst_refs_norm_v1")
    True
    """
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
                    delta_cdf_policy=spec.delta_cdf_policy,
                    delta_maintenance_policy=spec.delta_maintenance_policy,
                    delta_write_policy=spec.delta_write_policy,
                    delta_schema_policy=spec.delta_schema_policy,
                    delta_feature_gate=spec.delta_feature_gate,
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

    Parameters
    ----------
    name
        Semantic dataset name to look up.
    output_root
        Root directory for semantic outputs.

    Returns
    -------
    DatasetLocation
        Location metadata for the semantic dataset.

    Raises
    ------
    KeyError
        Raised when the dataset name is not found in the semantic catalog.
    """
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
        delta_cdf_policy=spec.delta_cdf_policy,
        delta_maintenance_policy=spec.delta_maintenance_policy,
        delta_write_policy=spec.delta_write_policy,
        delta_schema_policy=spec.delta_schema_policy,
        delta_feature_gate=spec.delta_feature_gate,
    )


__all__ = [
    "build_semantic_dataset_catalog",
    "semantic_dataset_location",
]
