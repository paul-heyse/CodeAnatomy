"""Build CPG properties tables from extracted metadata."""

from __future__ import annotations

from dataclasses import dataclass

import pyarrow as pa

from arrowdsl.compute.kernels import (
    apply_aggregate,
    bitmask_flag_array,
    cast_array,
    coalesce_arrays,
)
from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import ArrayLike, ChunkedArrayLike, TableLike
from arrowdsl.plan.ops import AggregateSpec
from arrowdsl.schema.arrays import set_or_append_column
from cpg.artifacts import CpgBuildArtifacts
from cpg.builders import PropBuilder
from cpg.catalog import TableCatalog
from cpg.quality import quality_from_ids
from cpg.schemas import CPG_PROPS_SCHEMA, CPG_PROPS_SPEC, SCHEMA_VERSION, empty_props
from cpg.spec_registry import (
    ROLE_FLAG_SPECS,
    edge_prop_spec,
    prop_table_specs,
    scip_role_flag_prop_spec,
)
from cpg.specs import PropTableSpec


def _defs_table(table: TableLike | None) -> TableLike | None:
    if table is None or table.num_rows == 0:
        return None
    cols = [col for col in ("def_kind", "kind") if col in table.column_names]
    if cols:
        arrays = [table[col] for col in cols]
        def_kind = coalesce_arrays(arrays)
    else:
        def_kind = pa.nulls(table.num_rows, type=pa.string())
    return set_or_append_column(table, "def_kind_norm", def_kind)


def _scip_role_flags_table(scip_occurrences: TableLike | None) -> TableLike | None:
    if (
        scip_occurrences is None
        or scip_occurrences.num_rows == 0
        or "symbol" not in scip_occurrences.column_names
        or "symbol_roles" not in scip_occurrences.column_names
    ):
        return None

    symbols = cast_array(scip_occurrences["symbol"], pa.string())
    roles = scip_occurrences["symbol_roles"]
    flag_arrays: list[ArrayLike | ChunkedArrayLike] = []
    flag_names: list[str] = []
    for name, mask, _ in ROLE_FLAG_SPECS:
        flag_arrays.append(bitmask_flag_array(roles, mask=mask))
        flag_names.append(name)

    flags_table = pa.Table.from_arrays([symbols, *flag_arrays], names=["symbol", *flag_names])
    spec = AggregateSpec(
        keys=("symbol",),
        aggs=tuple((name, "max") for name in flag_names),
        use_threads=True,
        rename_aggregates=True,
    )
    return apply_aggregate(flags_table, spec=spec)


PROP_TABLE_SPECS: tuple[PropTableSpec, ...] = (
    *prop_table_specs(),
    scip_role_flag_prop_spec(),
    edge_prop_spec(),
)

PROP_BUILDER = PropBuilder(
    table_specs=PROP_TABLE_SPECS,
    schema_version=SCHEMA_VERSION,
    prop_schema=CPG_PROPS_SCHEMA,
)


@dataclass(frozen=True)
class PropsBuildOptions:
    """Configure which property families are emitted."""

    include_node_props: bool = True
    include_edge_props: bool = True
    include_heavy_json_props: bool = True


@dataclass(frozen=True)
class PropsInputTables:
    """Bundle of input tables for property extraction."""

    repo_files: TableLike | None = None
    cst_name_refs: TableLike | None = None
    cst_imports: TableLike | None = None
    cst_callsites: TableLike | None = None
    cst_defs: TableLike | None = None
    dim_qualified_names: TableLike | None = None
    scip_symbol_information: TableLike | None = None
    scip_occurrences: TableLike | None = None
    scip_external_symbol_information: TableLike | None = None
    ts_nodes: TableLike | None = None
    ts_errors: TableLike | None = None
    ts_missing: TableLike | None = None
    type_exprs_norm: TableLike | None = None
    types_norm: TableLike | None = None
    diagnostics_norm: TableLike | None = None
    rt_objects: TableLike | None = None
    rt_signatures: TableLike | None = None
    rt_signature_params: TableLike | None = None
    rt_members: TableLike | None = None
    cpg_edges: TableLike | None = None


def _prop_tables(inputs: PropsInputTables) -> dict[str, TableLike]:
    catalog = TableCatalog()
    catalog.extend(
        {
            name: table
            for name, table in {
                "repo_files": inputs.repo_files,
                "cst_name_refs": inputs.cst_name_refs,
                "cst_imports": inputs.cst_imports,
                "cst_callsites": inputs.cst_callsites,
                "dim_qualified_names": inputs.dim_qualified_names,
                "scip_symbol_information": inputs.scip_symbol_information,
                "scip_external_symbol_information": inputs.scip_external_symbol_information,
                "ts_nodes": inputs.ts_nodes,
                "ts_errors": inputs.ts_errors,
                "ts_missing": inputs.ts_missing,
                "type_exprs_norm": inputs.type_exprs_norm,
                "types_norm": inputs.types_norm,
                "diagnostics_norm": inputs.diagnostics_norm,
                "rt_objects": inputs.rt_objects,
                "rt_signatures": inputs.rt_signatures,
                "rt_signature_params": inputs.rt_signature_params,
                "rt_members": inputs.rt_members,
                "cpg_edges": inputs.cpg_edges,
            }.items()
            if table is not None
        }
    )

    if inputs.cst_defs is not None:
        catalog.add("cst_defs", inputs.cst_defs)
        defs_norm = _defs_table(inputs.cst_defs)
        catalog.add("cst_defs_norm", defs_norm)

    scip_role_flags = _scip_role_flags_table(inputs.scip_occurrences)
    catalog.add("scip_role_flags", scip_role_flags)
    return catalog.snapshot()


def build_cpg_props_raw(
    *,
    inputs: PropsInputTables | None = None,
    options: PropsBuildOptions | None = None,
) -> TableLike:
    """Build CPG properties without finalization.

    Returns
    -------
    pyarrow.Table
        Raw properties table.
    """
    options = options or PropsBuildOptions()
    tables = _prop_tables(inputs or PropsInputTables())
    out = PROP_BUILDER.build(tables=tables, options=options)

    if out.num_rows == 0:
        return empty_props()

    return out


def build_cpg_props(
    *,
    ctx: ExecutionContext,
    inputs: PropsInputTables | None = None,
    options: PropsBuildOptions | None = None,
) -> CpgBuildArtifacts:
    """Build and finalize CPG properties with quality artifacts.

    Returns
    -------
    CpgBuildArtifacts
        Finalize result plus quality table.
    """
    raw = build_cpg_props_raw(inputs=inputs, options=options)
    quality = quality_from_ids(
        raw,
        id_col="entity_id",
        entity_kind="prop",
        issue="invalid_entity_id",
        source_table="cpg_props_raw",
    )
    finalize = CPG_PROPS_SPEC.finalize_context(ctx).run(raw, ctx=ctx)
    return CpgBuildArtifacts(finalize=finalize, quality=quality)
