"""Shared table catalog and references for CPG builders."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field

import pyarrow as pa

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import ComputeExpression, TableLike, ensure_expression, pc
from arrowdsl.plan.plan import Plan
from cpg.plan_exprs import bitmask_is_set_expr, coalesce_expr
from cpg.plan_helpers import plan_from_dataset, set_or_append_column
from cpg.role_flags import ROLE_FLAG_SPECS
from cpg.sources import DatasetSource

type TableGetter = Callable[[Mapping[str, TableLike]], TableLike | None]
type TableDeriver = Callable[[TableCatalog, ExecutionContext], TableLike | None]
type PlanSource = Plan | TableLike | DatasetSource
type PlanGetter = Callable[[Mapping[str, PlanSource]], PlanSource | None]
type PlanDeriver = Callable[[PlanCatalog, ExecutionContext], PlanSource | None]


@dataclass(frozen=True)
class TableRef:
    """Reference to a table, optionally derived on demand."""

    name: str
    derive: TableDeriver | None = None

    def getter(self) -> TableGetter:
        """Return a TableGetter for use in spec objects.

        Returns
        -------
        TableGetter
            Getter that looks up the table by name.
        """

        def _get(tables: Mapping[str, TableLike]) -> TableLike | None:
            return tables.get(self.name)

        return _get


@dataclass
class TableCatalog:
    """Mutable table registry for build steps and derived tables."""

    tables: dict[str, TableLike] = field(default_factory=dict)

    def add(self, name: str, table: TableLike | None) -> None:
        """Add a table when present."""
        if table is not None:
            self.tables[name] = table

    def extend(self, tables: Mapping[str, TableLike]) -> None:
        """Add multiple tables to the catalog."""
        self.tables.update(tables)

    def resolve(self, ref: TableRef, *, ctx: ExecutionContext) -> TableLike | None:
        """Resolve a table, deriving it if needed.

        Returns
        -------
        TableLike | None
            Resolved table or ``None`` when unavailable.
        """
        if ref.name in self.tables:
            return self.tables[ref.name]
        if ref.derive is None:
            return None
        table = ref.derive(self, ctx)
        if table is not None:
            self.tables[ref.name] = table
        return table

    def snapshot(self) -> dict[str, TableLike]:
        """Return a shallow copy of the catalog tables.

        Returns
        -------
        dict[str, TableLike]
            Shallow copy of catalog tables.
        """
        return dict(self.tables)


@dataclass(frozen=True)
class PlanRef:
    """Reference to a plan source, optionally derived on demand."""

    name: str
    derive: PlanDeriver | None = None

    def getter(self) -> PlanGetter:
        """Return a PlanGetter for use in plan spec objects.

        Returns
        -------
        PlanGetter
            Getter that looks up the plan source by name.
        """

        def _get(tables: Mapping[str, PlanSource]) -> PlanSource | None:
            value = tables.get(self.name)
            if isinstance(value, Plan):
                return value
            if isinstance(value, DatasetSource):
                return value
            if value is None:
                return None
            return Plan.table_source(value, label=self.name)

        return _get


@dataclass
class PlanCatalog:
    """Mutable plan registry for build steps and derived sources."""

    tables: dict[str, PlanSource] = field(default_factory=dict)

    def add(self, name: str, table: PlanSource | None) -> None:
        """Add a plan source when present."""
        if table is not None:
            self.tables[name] = table

    def extend(self, tables: Mapping[str, PlanSource]) -> None:
        """Add multiple plan sources to the catalog."""
        self.tables.update(tables)

    def resolve(self, ref: PlanRef, *, ctx: ExecutionContext) -> Plan | None:
        """Resolve a plan source, deriving it if needed.

        Returns
        -------
        Plan | None
            Resolved plan or ``None`` when unavailable.
        """
        if ref.name in self.tables:
            existing = self.tables[ref.name]
            if isinstance(existing, Plan):
                return existing
            if isinstance(existing, DatasetSource):
                plan = plan_from_dataset(existing.dataset, spec=existing.spec, ctx=ctx)
                self.tables[ref.name] = plan
                return plan
            plan = Plan.table_source(existing, label=ref.name)
            self.tables[ref.name] = plan
            return plan
        if ref.derive is None:
            return None
        derived = ref.derive(self, ctx)
        if derived is None:
            return None
        if isinstance(derived, Plan):
            plan = derived
        elif isinstance(derived, DatasetSource):
            plan = plan_from_dataset(derived.dataset, spec=derived.spec, ctx=ctx)
        else:
            plan = Plan.table_source(derived, label=ref.name)
        self.tables[ref.name] = plan
        return plan

    def snapshot(self) -> dict[str, PlanSource]:
        """Return a shallow copy of the catalog sources.

        Returns
        -------
        dict[str, PlanSource]
            Shallow copy of catalog sources.
        """
        return dict(self.tables)


def derive_cst_defs_norm(catalog: PlanCatalog, ctx: ExecutionContext) -> Plan | None:
    """Derive a normalized CST definitions plan when available.

    Returns
    -------
    Plan | None
        Derived plan or ``None`` when unavailable.
    """
    defs = catalog.resolve(PlanRef("cst_defs"), ctx=ctx)
    if defs is None:
        return None
    available = set(defs.schema(ctx=ctx).names)
    expr = coalesce_expr(("def_kind", "kind"), dtype=pa.string(), available=available)
    return set_or_append_column(defs, name="def_kind_norm", expr=expr, ctx=ctx)


def derive_scip_role_flags(catalog: PlanCatalog, ctx: ExecutionContext) -> Plan | None:
    """Derive SCIP role flag aggregates when available.

    Returns
    -------
    Plan | None
        Derived plan or ``None`` when unavailable.
    """
    occurrences = catalog.resolve(PlanRef("scip_occurrences"), ctx=ctx)
    if occurrences is None:
        return None
    schema = occurrences.schema(ctx=ctx)
    available = set(schema.names)
    if "symbol" not in available or "symbol_roles" not in available:
        return None

    flag_exprs: list[ComputeExpression] = []
    flag_names: list[str] = []
    for name, mask, _ in ROLE_FLAG_SPECS:
        hit = bitmask_is_set_expr(pc.field("symbol_roles"), mask=mask)
        flag_exprs.append(ensure_expression(pc.cast(hit, pa.int32(), safe=False)))
        flag_names.append(name)

    project_exprs = [
        ensure_expression(pc.cast(pc.field("symbol"), pa.string(), safe=False)),
        *flag_exprs,
    ]
    project_names = ["symbol", *flag_names]
    projected = occurrences.project(project_exprs, project_names, ctx=ctx)
    aggregated = projected.aggregate(
        group_keys=("symbol",),
        aggs=[(name, "max") for name in flag_names],
        ctx=ctx,
    )
    rename_exprs = [pc.field("symbol")] + [pc.field(f"{name}_max") for name in flag_names]
    rename_names = ["symbol", *flag_names]
    return aggregated.project(rename_exprs, rename_names, ctx=ctx)


__all__ = [
    "PlanCatalog",
    "PlanDeriver",
    "PlanGetter",
    "PlanRef",
    "PlanSource",
    "TableCatalog",
    "TableDeriver",
    "TableGetter",
    "TableRef",
    "derive_cst_defs_norm",
    "derive_scip_role_flags",
]
