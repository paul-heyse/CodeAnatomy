"""Structured lineage extraction from DataFusion logical plans."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field

from datafusion_engine.udf_catalog import rewrite_tag_index
from datafusion_engine.udf_runtime import (
    udf_names_from_snapshot,
    validate_rust_udf_snapshot,
)

_PAIR_LEN = 2
_SINGLE_DATASET_COUNT = 1

_PLAN_EXPR_ATTRS: dict[str, tuple[str, ...]] = {
    "Aggregate": ("group_expr", "group_exprs", "aggr_expr", "aggr_exprs"),
    "Filter": ("predicate",),
    "Join": ("filter",),
    "Projection": ("projections",),
    "Sort": ("expr", "exprs", "sort_exprs"),
    "TableScan": ("filters",),
    "Window": ("window_expr", "window_exprs"),
}

_EXPR_CHILD_ATTRS: tuple[str, ...] = (
    "args",
    "expr",
    "exprs",
    "left",
    "right",
    "predicate",
    "filter",
    "when_then_expr",
    "then_expr",
    "else_expr",
    "partition_by",
    "order_by",
    "on",
)


@dataclass(frozen=True)
class ScanLineage:
    """Lineage information for a single table scan."""

    dataset_name: str
    projected_columns: tuple[str, ...] = ()
    pushed_filters: tuple[str, ...] = ()


@dataclass(frozen=True)
class JoinLineage:
    """Lineage information for a join operation."""

    join_type: str
    left_keys: tuple[str, ...] = ()
    right_keys: tuple[str, ...] = ()


@dataclass(frozen=True)
class ExprInfo:
    """Structured expression lineage extracted from a plan."""

    kind: str
    referenced_columns: tuple[tuple[str, str], ...]
    referenced_udfs: tuple[str, ...]
    text: str | None = None


@dataclass(frozen=True)
class LineageReport:
    """Complete lineage report extracted from a DataFusion logical plan."""

    scans: tuple[ScanLineage, ...] = ()
    joins: tuple[JoinLineage, ...] = ()
    exprs: tuple[ExprInfo, ...] = ()
    required_udfs: tuple[str, ...] = ()
    required_rewrite_tags: tuple[str, ...] = ()
    required_columns_by_dataset: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    filters: tuple[str, ...] = ()
    aggregations: tuple[str, ...] = ()
    window_functions: tuple[str, ...] = ()
    subqueries: tuple[str, ...] = ()
    referenced_udfs: tuple[str, ...] = ()

    @property
    def referenced_tables(self) -> tuple[str, ...]:
        """Return all table names referenced in the plan."""
        return tuple(sorted({scan.dataset_name for scan in self.scans}))

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
    """Extract lineage information from a DataFusion logical plan.

    Returns
    -------
    LineageReport
        Structured lineage report extracted from the plan.
    """
    udf_name_map = _udf_name_map(udf_snapshot)
    scans: list[ScanLineage] = []
    joins: list[JoinLineage] = []
    exprs: list[ExprInfo] = []
    stack: list[object] = [plan]

    while stack:
        node = stack.pop()
        variant = _plan_variant(node)
        tag = _variant_name(node=node, variant=variant)
        scans.extend(_extract_scan_lineage(tag=tag, variant=variant))
        joins.extend(_extract_join_lineage(tag=tag, variant=variant))
        exprs.extend(_extract_expr_infos(tag=tag, variant=variant, udf_name_map=udf_name_map))
        stack.extend(_plan_inputs(node))

    required_udfs = _required_udfs(exprs)
    required_tags = _required_rewrite_tags(required_udfs, udf_snapshot)
    required_columns = _required_columns_by_dataset(scans=scans, exprs=exprs)
    filters = _filters_from_exprs(exprs)
    aggregations = _aggregations_from_exprs(exprs)
    window_functions = _window_functions_from_exprs(exprs)

    return LineageReport(
        scans=tuple(scans),
        joins=tuple(joins),
        exprs=tuple(exprs),
        required_udfs=required_udfs,
        required_rewrite_tags=required_tags,
        required_columns_by_dataset=required_columns,
        filters=filters,
        aggregations=aggregations,
        window_functions=window_functions,
        referenced_udfs=required_udfs,
    )


def referenced_tables_from_plan(plan: object) -> tuple[str, ...]:
    """Extract referenced table names from a DataFusion plan.

    Returns
    -------
    tuple[str, ...]
        Sorted table names referenced in the plan.
    """
    return extract_lineage(plan).referenced_tables


def required_columns_by_table(plan: object) -> Mapping[str, tuple[str, ...]]:
    """Extract required columns per table from a DataFusion plan.

    Returns
    -------
    Mapping[str, tuple[str, ...]]
        Mapping of dataset name to required columns.
    """
    return extract_lineage(plan).required_columns_by_dataset


def _variant_name(*, node: object, variant: object | None) -> str:
    if variant is not None:
        return type(variant).__name__
    return type(node).__name__


def _plan_variant(plan: object) -> object | None:
    to_variant = getattr(plan, "to_variant", None)
    if not callable(to_variant):
        return None
    try:
        return to_variant()
    except (RuntimeError, TypeError, ValueError):
        return None


def _plan_inputs(plan: object) -> list[object]:
    inputs = getattr(plan, "inputs", None)
    if not callable(inputs):
        return []
    try:
        children = inputs()
    except (RuntimeError, TypeError, ValueError):
        return []
    if isinstance(children, Sequence) and not isinstance(children, (str, bytes)):
        return [child for child in children if child is not None]
    return []


def _safe_attr(obj: object | None, name: str) -> object | None:
    if obj is None:
        return None
    value = getattr(obj, name, None)
    if callable(value):
        try:
            return value()
        except (RuntimeError, TypeError, ValueError):
            return None
    return value


def _normalize_exprs(value: object | None) -> list[object]:
    if value is None:
        return []
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        exprs: list[object] = []
        for entry in value:
            if entry is None:
                continue
            if isinstance(entry, tuple) and len(entry) == _PAIR_LEN:
                exprs.extend((entry[0], entry[1]))
                continue
            exprs.append(entry)
        return exprs
    return [value]


def _projection_names(projection: object | None) -> tuple[str, ...]:
    if projection is None:
        return ()
    names: list[str] = []
    for entry in _normalize_exprs(projection):
        if isinstance(entry, tuple) and len(entry) >= _PAIR_LEN:
            names.append(str(entry[1]))
            continue
        names.append(str(entry))
    return tuple(dict.fromkeys(names))


def _extract_scan_lineage(*, tag: str, variant: object | None) -> list[ScanLineage]:
    if tag != "TableScan" or variant is None:
        return []
    dataset_name = _safe_attr(variant, "table_name") or _safe_attr(variant, "fqn")
    if dataset_name is None:
        return []
    projection = _safe_attr(variant, "projection")
    projected_columns = _projection_names(projection)
    filters = tuple(str(expr) for expr in _normalize_exprs(_safe_attr(variant, "filters")))
    return [
        ScanLineage(
            dataset_name=str(dataset_name),
            projected_columns=projected_columns,
            pushed_filters=filters,
        )
    ]


def _extract_join_lineage(*, tag: str, variant: object | None) -> list[JoinLineage]:
    if tag != "Join" or variant is None:
        return []
    join_type = _safe_attr(variant, "join_type")
    on_pairs = _safe_attr(variant, "on")
    left_keys: list[str] = []
    right_keys: list[str] = []
    for left_expr, right_expr in _normalize_on_pairs(on_pairs):
        left_keys.extend(_qualified_column_names(left_expr))
        right_keys.extend(_qualified_column_names(right_expr))
    return [
        JoinLineage(
            join_type=str(join_type).lower() if join_type is not None else "unknown",
            left_keys=tuple(sorted(dict.fromkeys(left_keys))),
            right_keys=tuple(sorted(dict.fromkeys(right_keys))),
        )
    ]


def _normalize_on_pairs(value: object | None) -> list[tuple[object, object]]:
    return [
        (entry[0], entry[1])
        for entry in _normalize_exprs(value)
        if isinstance(entry, tuple) and len(entry) == _PAIR_LEN
    ]


def _extract_expr_infos(
    *,
    tag: str,
    variant: object | None,
    udf_name_map: Mapping[str, str],
) -> list[ExprInfo]:
    if variant is None:
        return []
    exprs: list[object] = []
    for attr in _PLAN_EXPR_ATTRS.get(tag, ()):
        exprs.extend(_normalize_exprs(_safe_attr(variant, attr)))
    if tag == "Join":
        for left_expr, right_expr in _normalize_on_pairs(_safe_attr(variant, "on")):
            exprs.extend((left_expr, right_expr))
    return [_expr_info(expr=expr, kind=tag, udf_name_map=udf_name_map) for expr in exprs]


def _expr_info(*, expr: object, kind: str, udf_name_map: Mapping[str, str]) -> ExprInfo:
    columns = _column_refs_from_expr(expr)
    udfs = _udf_refs_from_expr(expr, udf_name_map)
    text = _expr_text(expr)
    return ExprInfo(
        kind=kind,
        referenced_columns=tuple(sorted(columns)),
        referenced_udfs=tuple(sorted(udfs)),
        text=text,
    )


def _expr_text(expr: object) -> str | None:
    try:
        return str(expr)
    except (RuntimeError, TypeError, ValueError):
        return None


def _expr_variant(expr: object) -> object | None:
    to_variant = getattr(expr, "to_variant", None)
    if not callable(to_variant):
        return None
    try:
        return to_variant()
    except (RuntimeError, TypeError, ValueError):
        return None


def _expr_variant_name(expr: object) -> str | None:
    variant_name = getattr(expr, "variant_name", None)
    if not callable(variant_name):
        return None
    try:
        return str(variant_name())
    except (RuntimeError, TypeError, ValueError):
        return None


def _expr_operands(expr: object) -> list[object]:
    operands = getattr(expr, "rex_call_operands", None)
    if not callable(operands):
        return []
    try:
        value = operands()
    except (RuntimeError, TypeError, ValueError):
        return []
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return [entry for entry in value if entry is not None]
    return []


def _rex_call_operator(expr: object) -> str | None:
    operator = getattr(expr, "rex_call_operator", None)
    if not callable(operator):
        return None
    try:
        return str(operator())
    except (RuntimeError, TypeError, ValueError):
        return None


def _column_refs_from_expr(expr: object) -> set[tuple[str, str]]:
    return _column_refs_from_expr_inner(expr, seen=set())


def _column_refs_from_expr_inner(
    expr: object,
    *,
    seen: set[int],
) -> set[tuple[str, str]]:
    expr_id = id(expr)
    if expr_id in seen:
        return set()
    seen.add(expr_id)
    variant = _expr_variant(expr)
    if variant is not None:
        name = type(variant).__name__
        if name == "Column":
            relation = _safe_attr(variant, "relation")
            column_name = _safe_attr(variant, "name")
            if column_name is None:
                return set()
            dataset = "" if relation is None else str(relation)
            column_value = str(column_name)
            if not dataset and ("(" in column_value or " " in column_value):
                return set()
            return {(dataset, column_value)}
        refs: set[tuple[str, str]] = set()
        for child in _expr_children(variant):
            refs.update(_column_refs_from_expr_inner(child, seen=seen))
        return refs
    refs: set[tuple[str, str]] = set()
    for child in _expr_operands(expr):
        if child is expr:
            continue
        refs.update(_column_refs_from_expr_inner(child, seen=seen))
    return refs


def _expr_children(variant: object) -> Iterable[object]:
    for name in _EXPR_CHILD_ATTRS:
        value = _safe_attr(variant, name)
        yield from _normalize_exprs(value)


def _udf_refs_from_expr(expr: object, udf_name_map: Mapping[str, str]) -> set[str]:
    if not udf_name_map:
        return set()
    return _udf_refs_from_expr_inner(expr, udf_name_map=udf_name_map, seen=set())


def _udf_refs_from_expr_inner(
    expr: object,
    *,
    udf_name_map: Mapping[str, str],
    seen: set[int],
) -> set[str]:
    expr_id = id(expr)
    if expr_id in seen:
        return set()
    seen.add(expr_id)
    refs: set[str] = set()
    udf_name = _udf_name_from_expr(expr)
    if udf_name is not None:
        key = udf_name.lower()
        if key in udf_name_map:
            refs.add(udf_name_map[key])
    for child in _expr_children_from_expr(expr):
        refs.update(_udf_refs_from_expr_inner(child, udf_name_map=udf_name_map, seen=seen))
    return refs


def _expr_children_from_expr(expr: object) -> list[object]:
    variant = _expr_variant(expr)
    if variant is not None:
        return list(_expr_children(variant))
    return _expr_operands(expr)


def _udf_name_from_expr(expr: object) -> str | None:
    variant = _expr_variant(expr)
    if variant is not None:
        variant_name = type(variant).__name__
        if variant_name == "AggregateFunction":
            name = _safe_attr(variant, "aggregate_type")
            return str(name) if name is not None else None
        if variant_name == "ScalarFunction":
            return _rex_call_operator(expr)
        return None
    variant_name = _expr_variant_name(expr)
    if variant_name == "ScalarFunction":
        return _rex_call_operator(expr)
    return None


def _qualified_column_names(expr: object) -> list[str]:
    names: list[str] = []
    for dataset, column in _column_refs_from_expr(expr):
        if dataset:
            names.append(f"{dataset}.{column}")
        else:
            names.append(column)
    return names


def _required_udfs(exprs: Sequence[ExprInfo]) -> tuple[str, ...]:
    names = {name for expr in exprs for name in expr.referenced_udfs}
    return tuple(sorted(names))


def _required_rewrite_tags(
    required_udfs: Sequence[str],
    snapshot: Mapping[str, object] | None,
) -> tuple[str, ...]:
    if not required_udfs or snapshot is None:
        return ()
    tag_index = rewrite_tag_index(snapshot)
    required = set(required_udfs)
    tags: set[str] = set()
    for tag, names in tag_index.items():
        if any(name in required for name in names):
            tags.add(tag)
    return tuple(sorted(tags))


def _required_columns_by_dataset(
    *,
    scans: Sequence[ScanLineage],
    exprs: Sequence[ExprInfo],
) -> dict[str, tuple[str, ...]]:
    dataset_names = {scan.dataset_name for scan in scans}
    columns_by_dataset: dict[str, set[str]] = {}
    for scan in scans:
        columns_by_dataset.setdefault(scan.dataset_name, set()).update(scan.projected_columns)
    for expr in exprs:
        for dataset, column in expr.referenced_columns:
            if not column:
                continue
            resolved_dataset = dataset
            if not resolved_dataset and len(dataset_names) == _SINGLE_DATASET_COUNT:
                resolved_dataset = next(iter(dataset_names))
            if not resolved_dataset:
                continue
            columns_by_dataset.setdefault(resolved_dataset, set()).add(column)
    return {dataset: tuple(sorted(columns)) for dataset, columns in columns_by_dataset.items()}


def _filters_from_exprs(exprs: Sequence[ExprInfo]) -> tuple[str, ...]:
    filters = {expr.text for expr in exprs if expr.kind == "Filter" and expr.text}
    return tuple(sorted(filters))


def _aggregations_from_exprs(exprs: Sequence[ExprInfo]) -> tuple[str, ...]:
    aggs = {expr.text for expr in exprs if expr.kind == "Aggregate" and expr.text}
    return tuple(sorted(aggs))


def _window_functions_from_exprs(exprs: Sequence[ExprInfo]) -> tuple[str, ...]:
    windows = {expr.text for expr in exprs if expr.kind == "Window" and expr.text}
    return tuple(sorted(windows))


def _udf_name_map(snapshot: Mapping[str, object] | None) -> dict[str, str]:
    if snapshot is None:
        return {}
    validate_rust_udf_snapshot(snapshot)
    names = udf_names_from_snapshot(snapshot)
    return {name.lower(): name for name in names}


__all__ = [
    "ExprInfo",
    "JoinLineage",
    "LineageReport",
    "ScanLineage",
    "extract_lineage",
    "referenced_tables_from_plan",
    "required_columns_by_table",
]
