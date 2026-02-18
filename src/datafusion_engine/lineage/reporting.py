"""Structured lineage extraction delegated to Rust."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

import msgspec

from datafusion_engine.plan.rust_bundle_bridge import extract_lineage_from_plan
from datafusion_engine.udf.extension_runtime import (
    udf_names_from_snapshot,
    validate_rust_udf_snapshot,
)
from datafusion_engine.udf.metadata import rewrite_tag_index
from serde_msgspec import StructBaseStrict


class ScanLineage(StructBaseStrict, frozen=True):
    """Lineage information for a single table scan."""

    dataset_name: str
    projected_columns: tuple[str, ...] = ()
    pushed_filters: tuple[str, ...] = ()


class JoinLineage(StructBaseStrict, frozen=True):
    """Expression-level join lineage payload."""

    join_type: str
    left_keys: tuple[str, ...] = ()
    right_keys: tuple[str, ...] = ()


class ExprInfo(StructBaseStrict, frozen=True):
    """Expression lineage payload."""

    kind: str
    referenced_columns: tuple[tuple[str, str], ...] = ()
    referenced_udfs: tuple[str, ...] = ()
    text: str | None = None


class LineageReport(StructBaseStrict, frozen=True):
    """Complete lineage report extracted from a DataFusion logical plan."""

    scans: tuple[ScanLineage, ...] = ()
    joins: tuple[JoinLineage, ...] = ()
    exprs: tuple[ExprInfo, ...] = ()
    required_udfs: tuple[str, ...] = ()
    required_rewrite_tags: tuple[str, ...] = ()
    required_columns_by_dataset: Mapping[str, tuple[str, ...]] = msgspec.field(default_factory=dict)
    filters: tuple[str, ...] = ()
    aggregations: tuple[str, ...] = ()
    window_functions: tuple[str, ...] = ()
    subqueries: tuple[str, ...] = ()

    @property
    def referenced_tables(self) -> tuple[str, ...]:
        """Return all table names referenced in the plan."""
        names = {scan.dataset_name for scan in self.scans}
        names.update(str(name) for name in self.required_columns_by_dataset)
        return tuple(sorted(names))

    @property
    def all_required_columns(self) -> tuple[tuple[str, str], ...]:
        """Return all required columns as (table, column) pairs."""
        pairs = [
            (table, column)
            for table, columns in self.required_columns_by_dataset.items()
            for column in columns
        ]
        return tuple(sorted(pairs))


def extract_lineage(
    plan: object,
    *,
    udf_snapshot: Mapping[str, object] | None = None,
) -> LineageReport:
    """Extract lineage information from a DataFusion logical plan."""
    payload: Mapping[str, object] = {}
    try:
        payload = extract_lineage_from_plan(plan)
    except (AttributeError, RuntimeError, TypeError, ValueError):
        payload = {}

    scans = _normalize_scans(payload.get("scans"))
    required_columns = _normalize_required_columns(payload.get("required_columns_by_dataset"))
    if not required_columns:
        required_columns = _required_columns_from_scans(scans)

    filters = _as_str_tuple(payload.get("filters"))
    required_udfs = _as_str_tuple(payload.get("required_udfs"))
    required_rewrite_tags = _as_str_tuple(payload.get("required_rewrite_tags"))

    if udf_snapshot is not None:
        validate_rust_udf_snapshot(udf_snapshot)
        available = udf_names_from_snapshot(udf_snapshot)
        if required_udfs:
            required_udfs = tuple(name for name in required_udfs if name in available)
        required_rewrite_tags = _resolve_required_rewrite_tags(
            required_udfs=required_udfs,
            snapshot=udf_snapshot,
            declared_tags=required_rewrite_tags,
        )

    return LineageReport(
        scans=scans,
        joins=(),
        exprs=(),
        required_udfs=required_udfs,
        required_rewrite_tags=required_rewrite_tags,
        required_columns_by_dataset=required_columns,
        filters=filters,
        aggregations=(),
        window_functions=(),
        subqueries=(),
    )


def referenced_tables_from_plan(plan: object) -> tuple[str, ...]:
    """Extract referenced table names from a DataFusion plan."""
    return extract_lineage(plan).referenced_tables


def required_columns_by_table(plan: object) -> Mapping[str, tuple[str, ...]]:
    """Extract required columns per table from a DataFusion plan."""
    return extract_lineage(plan).required_columns_by_dataset


def _as_str_tuple(value: object) -> tuple[str, ...]:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return tuple(str(item) for item in value if item is not None and str(item))
    return ()


def _normalize_required_columns(value: object) -> Mapping[str, tuple[str, ...]]:
    if not isinstance(value, Mapping):
        return {}
    normalized: dict[str, tuple[str, ...]] = {}
    for key, raw_columns in value.items():
        dataset = str(key)
        columns = _as_str_tuple(raw_columns)
        if dataset and columns:
            normalized[dataset] = tuple(dict.fromkeys(columns))
    return normalized


def _normalize_scans(value: object) -> tuple[ScanLineage, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return ()
    scans: list[ScanLineage] = []
    for entry in value:
        if not isinstance(entry, Mapping):
            continue
        dataset = entry.get("dataset_name")
        if dataset is None:
            dataset = entry.get("table_name")
        dataset_name = str(dataset).strip()
        if not dataset_name:
            continue
        projected_columns = _as_str_tuple(
            entry.get("projected_columns", entry.get("required_columns"))
        )
        pushed_filters = _as_str_tuple(entry.get("pushed_filters", entry.get("filters")))
        scans.append(
            ScanLineage(
                dataset_name=dataset_name,
                projected_columns=tuple(dict.fromkeys(projected_columns)),
                pushed_filters=tuple(dict.fromkeys(pushed_filters)),
            )
        )
    return tuple(scans)


def _resolve_required_rewrite_tags(
    *,
    required_udfs: Sequence[str],
    snapshot: Mapping[str, object] | None,
    declared_tags: Sequence[str],
) -> tuple[str, ...]:
    if declared_tags:
        return tuple(dict.fromkeys(str(tag) for tag in declared_tags if str(tag)))
    if snapshot is None or not required_udfs:
        return ()
    tag_index = rewrite_tag_index(snapshot)
    tags = sorted({tag for udf_name in required_udfs for tag in tag_index.get(udf_name, ())})
    return tuple(tags)


def _required_columns_from_scans(
    scans: Sequence[ScanLineage],
) -> Mapping[str, tuple[str, ...]]:
    by_dataset: dict[str, set[str]] = {}
    for scan in scans:
        dataset = scan.dataset_name
        if not dataset:
            continue
        bucket = by_dataset.setdefault(dataset, set())
        bucket.update(scan.projected_columns)
    return {dataset: tuple(sorted(columns)) for dataset, columns in by_dataset.items() if columns}


__all__ = [
    "ExprInfo",
    "JoinLineage",
    "LineageReport",
    "ScanLineage",
    "extract_lineage",
    "referenced_tables_from_plan",
    "required_columns_by_table",
]
