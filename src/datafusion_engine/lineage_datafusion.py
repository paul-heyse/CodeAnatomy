"""DataFusion-native lineage extraction from logical plans.

This module provides lineage extraction directly from DataFusion LogicalPlan
variants, replacing SQLGlot-based lineage analysis.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field


@dataclass(frozen=True)
class ScanLineage:
    """Lineage information for a single table scan.

    Attributes
    ----------
    dataset_name : str
        Name of the scanned dataset/table.
    projected_columns : tuple[str, ...]
        Columns included in the projection (may be empty for SELECT *).
    pushed_filters : tuple[str, ...]
        Filter predicates pushed down to the scan.
    """

    dataset_name: str
    projected_columns: tuple[str, ...] = ()
    pushed_filters: tuple[str, ...] = ()


@dataclass(frozen=True)
class JoinLineage:
    """Lineage information for a join operation.

    Attributes
    ----------
    join_type : str
        Type of join (e.g., "inner", "left", "right", "full").
    left_keys : tuple[str, ...]
        Join keys from the left side.
    right_keys : tuple[str, ...]
        Join keys from the right side.
    """

    join_type: str
    left_keys: tuple[str, ...] = ()
    right_keys: tuple[str, ...] = ()


@dataclass(frozen=True)
class LineageReport:
    """Complete lineage report extracted from a DataFusion logical plan.

    This replaces SQLGlot-based lineage extraction with native DataFusion
    plan traversal.

    Attributes
    ----------
    scans : tuple[ScanLineage, ...]
        Information about table scans in the plan.
    joins : tuple[JoinLineage, ...]
        Information about join operations.
    filters : tuple[str, ...]
        Filter expressions in the plan.
    required_columns_by_dataset : Mapping[str, tuple[str, ...]]
        Per-table column requirements computed from projection propagation.
    aggregations : tuple[str, ...]
        Aggregation expressions in the plan.
    window_functions : tuple[str, ...]
        Window function expressions in the plan.
    subqueries : tuple[str, ...]
        Subquery references in the plan.
    referenced_udfs : tuple[str, ...]
        User-defined function names referenced in the plan.
    """

    scans: tuple[ScanLineage, ...] = ()
    joins: tuple[JoinLineage, ...] = ()
    filters: tuple[str, ...] = ()
    required_columns_by_dataset: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    aggregations: tuple[str, ...] = ()
    window_functions: tuple[str, ...] = ()
    subqueries: tuple[str, ...] = ()
    referenced_udfs: tuple[str, ...] = ()

    @property
    def referenced_tables(self) -> tuple[str, ...]:
        """Return all table names referenced in the plan.

        Returns
        -------
        tuple[str, ...]
            Sorted tuple of unique table names.
        """
        return tuple(sorted({scan.dataset_name for scan in self.scans}))

    @property
    def all_required_columns(self) -> tuple[tuple[str, str], ...]:
        """Return all required columns as (table, column) pairs.

        Returns
        -------
        tuple[tuple[str, str], ...]
            Sorted pairs of (table_name, column_name).
        """
        pairs = [
            (table, col)
            for table, columns in self.required_columns_by_dataset.items()
            for col in columns
        ]
        return tuple(sorted(pairs))


def extract_lineage(plan: object) -> LineageReport:
    """Extract lineage information from a DataFusion logical plan.

    This function traverses the logical plan tree to extract:
    - Table scan references and projections
    - Join operations and keys
    - Filter predicates
    - Required columns per dataset
    - Aggregations and window functions
    - User-defined function references

    Parameters
    ----------
    plan : object
        DataFusion LogicalPlan (untyped to handle import issues).

    Returns
    -------
    LineageReport
        Complete lineage information extracted from the plan.
    """
    nodes = _logical_nodes(plan)
    scans = _extract_scans(nodes)
    joins = _extract_joins(nodes)
    filters = _extract_filters(nodes)
    aggregations = _extract_aggregations(nodes)
    window_functions = _extract_window_functions(nodes)
    subqueries = _extract_subqueries(nodes)
    udfs = _extract_udfs(plan)

    # Compute required columns by propagating through the plan
    required_columns = _propagate_required_columns(nodes, scans)

    return LineageReport(
        scans=tuple(scans),
        joins=tuple(joins),
        filters=tuple(filters),
        required_columns_by_dataset=required_columns,
        aggregations=tuple(aggregations),
        window_functions=tuple(window_functions),
        subqueries=tuple(subqueries),
        referenced_udfs=tuple(udfs),
    )


def extract_lineage_from_display(plan_display: str) -> LineageReport:
    """Extract lineage from a plan display string.

    Fallback method when direct plan object traversal isn't available.
    Parses the display_indent_schema() output to extract lineage.

    Parameters
    ----------
    plan_display : str
        Output from plan.display_indent_schema().

    Returns
    -------
    LineageReport
        Lineage information parsed from the display.
    """
    scans = _parse_scans_from_display(plan_display)
    joins = _parse_joins_from_display(plan_display)
    filters = _parse_filters_from_display(plan_display)
    udfs = _parse_udfs_from_display(plan_display)
    required_columns = _required_by_dataset_from_scans(scans)

    return LineageReport(
        scans=tuple(scans),
        joins=tuple(joins),
        filters=tuple(filters),
        required_columns_by_dataset=required_columns,
        referenced_udfs=tuple(udfs),
    )


def _logical_nodes(plan: object) -> list[object]:
    """Traverse the logical plan tree and collect all nodes.

    Uses display_indent_schema() to get a textual representation,
    then parses it to understand the plan structure.

    Returns
    -------
    list[object]
        Collected plan nodes in traversal order.
    """
    nodes: list[object] = []
    _collect_nodes(plan, nodes)
    return nodes


def _collect_nodes(node: object, nodes: list[object]) -> None:
    """Recursively collect all nodes in the plan tree."""
    if node is None:
        return
    nodes.append(node)

    # Try to access child nodes
    inputs = getattr(node, "inputs", None)
    if inputs is not None:
        if callable(inputs):
            try:
                children = inputs()
                if isinstance(children, (list, tuple)):
                    for child in children:
                        _collect_nodes(child, nodes)
            except (RuntimeError, TypeError, ValueError):
                pass
        elif isinstance(inputs, (list, tuple)):
            for child in inputs:
                _collect_nodes(child, nodes)


def _extract_scans(nodes: Sequence[object]) -> list[ScanLineage]:
    """Extract TableScan nodes from the plan.

    Returns
    -------
    list[ScanLineage]
        Table scan lineage information.
    """
    scans: list[ScanLineage] = []
    seen_tables: set[str] = set()

    for node in nodes:
        # Check node type by class name or variant
        node_type = _node_type(node)
        if node_type in {"TableScan", "SubqueryAlias", "EmptyRelation"}:
            name = _extract_table_name(node)
            if name and name not in seen_tables:
                seen_tables.add(name)
                columns = _extract_projected_columns(node)
                filters = _extract_scan_filters(node)
                scans.append(
                    ScanLineage(
                        dataset_name=name,
                        projected_columns=tuple(columns),
                        pushed_filters=tuple(filters),
                    )
                )

    # Also parse from display string as fallback
    for node in nodes:
        display = _node_display(node)
        if display:
            parsed = _parse_scans_from_display(display)
            for scan in parsed:
                if scan.dataset_name not in seen_tables:
                    seen_tables.add(scan.dataset_name)
                    scans.append(scan)

    return scans


def _extract_joins(nodes: Sequence[object]) -> list[JoinLineage]:
    """Extract Join nodes from the plan.

    Returns
    -------
    list[JoinLineage]
        Join lineage information.
    """
    joins: list[JoinLineage] = []

    for node in nodes:
        node_type = _node_type(node)
        if "Join" in node_type:
            join_type = _extract_join_type(node)
            left_keys, right_keys = _extract_join_keys(node)
            joins.append(
                JoinLineage(
                    join_type=join_type,
                    left_keys=tuple(left_keys),
                    right_keys=tuple(right_keys),
                )
            )

    return joins


def _extract_filters(nodes: Sequence[object]) -> list[str]:
    """Extract Filter expressions from the plan.

    Returns
    -------
    list[str]
        Filter predicate expressions.
    """
    filters: list[str] = []

    for node in nodes:
        node_type = _node_type(node)
        if node_type == "Filter":
            predicate = _extract_predicate(node)
            if predicate:
                filters.append(predicate)

    return filters


def _extract_aggregations(nodes: Sequence[object]) -> list[str]:
    """Extract Aggregate expressions from the plan.

    Returns
    -------
    list[str]
        Aggregate expression strings.
    """
    aggregations: list[str] = []

    for node in nodes:
        node_type = _node_type(node)
        if node_type == "Aggregate":
            agg_exprs = _extract_aggregate_exprs(node)
            aggregations.extend(agg_exprs)

    return aggregations


def _extract_window_functions(nodes: Sequence[object]) -> list[str]:
    """Extract Window function expressions from the plan.

    Returns
    -------
    list[str]
        Window expression strings.
    """
    windows: list[str] = []

    for node in nodes:
        node_type = _node_type(node)
        if node_type == "Window":
            window_exprs = _extract_window_exprs(node)
            windows.extend(window_exprs)

    return windows


def _extract_subqueries(nodes: Sequence[object]) -> list[str]:
    """Extract Subquery references from the plan.

    Returns
    -------
    list[str]
        Subquery display strings.
    """
    subqueries: list[str] = []

    for node in nodes:
        node_type = _node_type(node)
        if node_type in {"Subquery", "ScalarSubquery", "InSubquery"}:
            subquery_display = _node_display(node)
            if subquery_display:
                subqueries.append(subquery_display)

    return subqueries


def _extract_udfs(plan: object) -> list[str]:
    """Extract UDF references from the logical plan.

    This parses the plan display string to identify ScalarUDF and AggregateUDF
    references. DataFusion's LogicalPlan display includes UDF information in
    patterns like:
    - ScalarUDF { name: "function_name", ... }
    - AggregateUDF { name: "function_name", ... }

    Returns
    -------
    list[str]
        Sorted list of unique UDF names referenced in the plan.
    """
    display = _node_display(plan)
    if not display:
        return []

    udfs: set[str] = set()

    # Pattern 1: ScalarUDF { ... name: "udf_name" ... }
    # Pattern 2: AggregateUDF { ... name: "udf_name" ... }
    # Pattern 3: ScalarFunction with user-defined functions
    for pattern in (
        r'ScalarUDF\s*\{[^}]*name:\s*"([^"]+)"',
        r"ScalarUDF\s*\{[^}]*name:\s*'([^']+)'",
        r'AggregateUDF\s*\{[^}]*name:\s*"([^"]+)"',
        r"AggregateUDF\s*\{[^}]*name:\s*'([^']+)'",
    ):
        matches = re.findall(pattern, display)
        udfs.update(matches)

    return sorted(udfs)


def _propagate_required_columns(
    nodes: Sequence[object],
    scans: Sequence[ScanLineage],
) -> dict[str, tuple[str, ...]]:
    """Propagate required columns through the plan to determine per-table needs.

    This implements column pruning analysis by tracking which columns
    from each source table are actually needed by the query.

    Returns
    -------
    dict[str, tuple[str, ...]]
        Per-table required columns.
    """
    required: dict[str, set[str]] = {}

    # Start with explicitly projected columns from scans
    for scan in scans:
        if scan.projected_columns:
            required.setdefault(scan.dataset_name, set()).update(scan.projected_columns)

    # Parse column references from node displays
    for node in nodes:
        display = _node_display(node)
        if display:
            refs = _parse_column_references(display)
            for table, col in refs:
                if table:
                    required.setdefault(table, set()).add(col)

    return {table: tuple(sorted(cols)) for table, cols in required.items()}


def _required_by_dataset_from_scans(scans: Sequence[ScanLineage]) -> dict[str, tuple[str, ...]]:
    """Build required columns map from scan lineage.

    Returns
    -------
    dict[str, tuple[str, ...]]
        Required columns keyed by dataset name.
    """
    required: dict[str, set[str]] = {}
    for scan in scans:
        required.setdefault(scan.dataset_name, set()).update(scan.projected_columns)
    return {table: tuple(sorted(cols)) for table, cols in required.items()}


def _node_type(node: object) -> str:
    """Get the type name of a logical plan node.

    Returns
    -------
    str
        Type or variant name for the node.
    """
    # Try variant name first (Rust enum style)
    variant = getattr(node, "variant_name", None)
    if variant is not None:
        return str(variant)

    # Try class name
    return type(node).__name__


def _node_display(node: object) -> str | None:
    """Get display string for a node.

    Returns
    -------
    str | None
        Display string for the node, if available.
    """
    for method_name in ("display_indent_schema", "display_indent", "__str__"):
        method = getattr(node, method_name, None)
        if callable(method):
            try:
                return str(method())
            except (RuntimeError, TypeError, ValueError):
                continue
    return None


def _extract_table_name(node: object) -> str | None:
    """Extract table name from a TableScan or SubqueryAlias node.

    Returns
    -------
    str | None
        Extracted table name, if found.
    """
    # Try table_name attribute
    name = getattr(node, "table_name", None)
    if name is not None:
        return str(name)

    # Try name attribute
    name = getattr(node, "name", None)
    if name is not None:
        return str(name)

    # Parse from display
    display = _node_display(node)
    if display:
        match = re.search(r"TableScan:\s*(\w+)", display)
        if match:
            return match.group(1)
        match = re.search(r"SubqueryAlias:\s*(\w+)", display)
        if match:
            return match.group(1)

    return None


def _extract_projected_columns(node: object) -> list[str]:
    """Extract projected column names from a scan node.

    Returns
    -------
    list[str]
        Column names referenced in the scan projection.
    """
    columns: list[str] = []

    # Try projection attribute
    projection = getattr(node, "projection", None)
    if isinstance(projection, (list, tuple)):
        for col in projection:
            col_name = _column_name(col)
            if col_name:
                columns.append(col_name)

    # Parse from display
    display = _node_display(node)
    if display:
        # Look for projection=[col1, col2, ...]
        match = re.search(r"projection=\[([^\]]+)\]", display)
        if match:
            col_str = match.group(1)
            columns.extend(c.strip() for c in col_str.split(",") if c.strip())

    return columns


def _extract_scan_filters(node: object) -> list[str]:
    """Extract pushed-down filter predicates from a scan node.

    Returns
    -------
    list[str]
        Filter predicate strings.
    """
    filters: list[str] = []

    # Try filters attribute
    node_filters = getattr(node, "filters", None)
    if isinstance(node_filters, (list, tuple)):
        filters.extend(str(f) for f in node_filters)

    # Parse from display
    display = _node_display(node)
    if display:
        match = re.search(r"filters=\[([^\]]+)\]", display)
        if match:
            filter_str = match.group(1)
            filters.extend(f.strip() for f in filter_str.split(",") if f.strip())

    return filters


def _extract_join_type(node: object) -> str:
    """Extract join type from a Join node.

    Returns
    -------
    str
        Join type in lowercase, or "unknown" if unavailable.
    """
    join_type = getattr(node, "join_type", None)
    if join_type is not None:
        return str(join_type).lower()

    # Parse from display
    display = _node_display(node)
    if display:
        for jtype in ("inner", "left", "right", "full", "semi", "anti", "cross"):
            if jtype in display.lower():
                return jtype

    return "unknown"


def _extract_join_keys(node: object) -> tuple[list[str], list[str]]:
    """Extract join keys from a Join node.

    Returns
    -------
    tuple[list[str], list[str]]
        Left and right join key references.
    """
    left_keys: list[str] = []
    right_keys: list[str] = []

    # Try on attribute (join condition)
    on = getattr(node, "on", None)
    if on is not None:
        # Parse join condition
        on_str = str(on)
        # Pattern uses left dot column equals right dot column.
        matches = re.findall(r"(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)", on_str)
        for left_table, left_col, right_table, right_col in matches:
            left_keys.append(f"{left_table}.{left_col}")
            right_keys.append(f"{right_table}.{right_col}")

    return left_keys, right_keys


def _extract_predicate(node: object) -> str | None:
    """Extract filter predicate from a Filter node.

    Returns
    -------
    str | None
        Filter predicate string if available.
    """
    predicate = getattr(node, "predicate", None)
    if predicate is not None:
        return str(predicate)

    # Parse from display
    display = _node_display(node)
    if display:
        match = re.search(r"Filter:\s*(.+)", display)
        if match:
            return match.group(1).strip()

    return None


def _extract_aggregate_exprs(node: object) -> list[str]:
    """Extract aggregation expressions from an Aggregate node.

    Returns
    -------
    list[str]
        Aggregate expression strings.
    """
    exprs: list[str] = []

    aggr_expr = getattr(node, "aggr_expr", None)
    if isinstance(aggr_expr, (list, tuple)):
        exprs.extend(str(e) for e in aggr_expr)

    return exprs


def _extract_window_exprs(node: object) -> list[str]:
    """Extract window function expressions from a Window node.

    Returns
    -------
    list[str]
        Window expression strings.
    """
    exprs: list[str] = []

    window_expr = getattr(node, "window_expr", None)
    if isinstance(window_expr, (list, tuple)):
        exprs.extend(str(e) for e in window_expr)

    return exprs


def _column_name(col: object) -> str | None:
    """Extract column name from a column reference.

    Returns
    -------
    str | None
        Column name if available.
    """
    name = getattr(col, "name", None)
    if name is not None:
        return str(name)
    return str(col)


def _parse_scans_from_display(display: str) -> list[ScanLineage]:
    """Parse table scans from plan display string.

    Returns
    -------
    list[ScanLineage]
        Parsed scan lineage entries.
    """
    scans: list[ScanLineage] = []
    seen: set[str] = set()

    # Pattern: TableScan: table_name projection=[col1, col2]
    pattern = r"TableScan:\s*(\w+)(?:\s+projection=\[([^\]]*)\])?"
    for match in re.finditer(pattern, display):
        table_name = match.group(1)
        if table_name in seen:
            continue
        seen.add(table_name)

        columns: tuple[str, ...] = ()
        if match.group(2):
            columns = tuple(c.strip() for c in match.group(2).split(",") if c.strip())

        scans.append(ScanLineage(dataset_name=table_name, projected_columns=columns))

    # Also look for SubqueryAlias
    alias_pattern = r"SubqueryAlias:\s*(\w+)"
    for match in re.finditer(alias_pattern, display):
        table_name = match.group(1)
        if table_name not in seen:
            seen.add(table_name)
            scans.append(ScanLineage(dataset_name=table_name))

    return scans


def _parse_joins_from_display(display: str) -> list[JoinLineage]:
    """Parse joins from plan display string.

    Returns
    -------
    list[JoinLineage]
        Parsed join lineage entries.
    """
    joins: list[JoinLineage] = []

    # Pattern: Join: type=Inner, on=[...]
    pattern = r"(\w+)Join"
    for match in re.finditer(pattern, display, re.IGNORECASE):
        join_type = match.group(1).lower()
        joins.append(JoinLineage(join_type=join_type))

    return joins


def _parse_filters_from_display(display: str) -> list[str]:
    """Parse filter predicates from plan display string.

    Returns
    -------
    list[str]
        Parsed filter predicate strings.
    """
    filters: list[str] = []

    # Pattern: Filter: predicate
    pattern = r"Filter:\s*(.+?)(?:\n|$)"
    for match in re.finditer(pattern, display):
        predicate = match.group(1).strip()
        if predicate:
            filters.append(predicate)

    return filters


def _parse_column_references(display: str) -> list[tuple[str | None, str]]:
    """Parse column references from plan display string.

    Returns (table_name, column_name) pairs. Table may be None for
    unqualified references.

    Returns
    -------
    list[tuple[str | None, str]]
        Parsed (table, column) references.
    """
    refs: list[tuple[str | None, str]] = []

    # Pattern: table.column or #column
    qualified_pattern = r"(\w+)\.(\w+)"
    for match in re.finditer(qualified_pattern, display):
        table = match.group(1)
        col = match.group(2)
        # Avoid matching type casts like Int32
        if table[0].islower():
            refs.append((table, col))

    # Pattern: #column
    ref_pattern = r"#(\w+)"
    refs.extend((None, match.group(1)) for match in re.finditer(ref_pattern, display))

    return refs


def _parse_udfs_from_display(display: str) -> list[str]:
    """Parse UDF references from plan display string.

    Extracts ScalarUDF and AggregateUDF function names from the display.

    Returns
    -------
    list[str]
        Sorted list of unique UDF names.
    """
    udfs: set[str] = set()

    # Pattern for UDF references in DataFusion plan display
    for pattern in (
        r'ScalarUDF\s*\{[^}]*name:\s*"([^"]+)"',
        r"ScalarUDF\s*\{[^}]*name:\s*'([^']+)'",
        r'AggregateUDF\s*\{[^}]*name:\s*"([^"]+)"',
        r"AggregateUDF\s*\{[^}]*name:\s*'([^']+)'",
    ):
        matches = re.findall(pattern, display)
        udfs.update(matches)

    return sorted(udfs)


def referenced_tables_from_plan(plan: object) -> tuple[str, ...]:
    """Extract referenced table names from a DataFusion plan.

    Convenience function for common use case.

    Parameters
    ----------
    plan : object
        DataFusion LogicalPlan.

    Returns
    -------
    tuple[str, ...]
        Sorted tuple of unique table names.
    """
    lineage = extract_lineage(plan)
    return lineage.referenced_tables


def required_columns_by_table(plan: object) -> Mapping[str, tuple[str, ...]]:
    """Extract required columns per table from a DataFusion plan.

    Convenience function for scheduling and validation.

    Parameters
    ----------
    plan : object
        DataFusion LogicalPlan.

    Returns
    -------
    Mapping[str, tuple[str, ...]]
        Per-table column requirements.
    """
    lineage = extract_lineage(plan)
    return lineage.required_columns_by_dataset


__all__ = [
    "JoinLineage",
    "LineageReport",
    "ScanLineage",
    "extract_lineage",
    "extract_lineage_from_display",
    "referenced_tables_from_plan",
    "required_columns_by_table",
]
