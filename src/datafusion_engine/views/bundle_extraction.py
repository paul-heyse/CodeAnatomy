"""Shared helpers for extracting metadata from plan bundles."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING

import pyarrow as pa

from datafusion_engine.udf.runtime import udf_names_from_snapshot

if TYPE_CHECKING:
    from datafusion.dataframe import DataFrame

    from datafusion_engine.lineage.datafusion import LineageReport
    from datafusion_engine.plan.bundle import DataFusionPlanBundle


def arrow_schema_from_df(df: DataFrame) -> pa.Schema:
    """Extract a PyArrow schema from a DataFusion DataFrame.

    Returns
    -------
    pyarrow.Schema
        Resolved schema for the DataFrame.

    Raises
    ------
    TypeError
        Raised when the schema cannot be resolved to a PyArrow schema.
    """
    schema = df.schema()
    if isinstance(schema, pa.Schema):
        return schema
    to_arrow = getattr(schema, "to_arrow", None)
    if callable(to_arrow):
        resolved = to_arrow()
        if isinstance(resolved, pa.Schema):
            return resolved
    msg = "Failed to resolve DataFusion schema."
    raise TypeError(msg)


def extract_lineage_from_bundle(bundle: DataFusionPlanBundle) -> LineageReport:
    """Extract a lineage report from a DataFusion plan bundle.

    Returns
    -------
    LineageReport
        Lineage report derived from the optimized logical plan.

    Raises
    ------
    ValueError
        Raised when the plan bundle lacks an optimized logical plan.
    """
    if bundle.optimized_logical_plan is None:
        msg = "DataFusion plan bundle missing optimized logical plan."
        raise ValueError(msg)
    from datafusion_engine.lineage.datafusion import extract_lineage

    snapshot = bundle.artifacts.udf_snapshot
    return extract_lineage(bundle.optimized_logical_plan, udf_snapshot=snapshot)


def resolve_required_udfs_from_bundle(
    bundle: DataFusionPlanBundle,
    *,
    snapshot: Mapping[str, object],
) -> tuple[str, ...]:
    """Resolve required UDF names from a plan bundle.

    Parameters
    ----------
    bundle
        DataFusion plan bundle.
    snapshot
        UDF snapshot used to resolve canonical names.

    Returns
    -------
    tuple[str, ...]
        Required UDF names in canonical form.
    """
    required_udfs = bundle.required_udfs
    if not required_udfs:
        lineage = extract_lineage_from_bundle(bundle)
        required_udfs = lineage.required_udfs
    if not required_udfs:
        return ()
    snapshot_names = udf_names_from_snapshot(snapshot)
    lookup = {name.lower(): name for name in snapshot_names}
    required = {
        lookup[name.lower()]
        for name in required_udfs
        if isinstance(name, str) and name.lower() in lookup
    }
    return tuple(sorted(required))


__all__ = [
    "arrow_schema_from_df",
    "extract_lineage_from_bundle",
    "resolve_required_udfs_from_bundle",
]
