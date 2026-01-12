"""Build CPG edge tables from relationship outputs."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import TableLike
from arrowdsl.schema.schema import EncodingSpec, encode_columns
from cpg.artifacts import CpgBuildArtifacts
from cpg.builders import emit_edges_from_relation
from cpg.catalog import TableCatalog
from cpg.merge import unify_tables
from cpg.quality import quality_from_ids
from cpg.relations import EDGE_RELATION_SPECS
from cpg.schemas import CPG_EDGES_SCHEMA, CPG_EDGES_SPEC, SCHEMA_VERSION, empty_edges

EDGE_ENCODING_SPECS: tuple[EncodingSpec, ...] = (
    EncodingSpec(column="edge_kind"),
    EncodingSpec(column="origin"),
    EncodingSpec(column="resolution_method"),
    EncodingSpec(column="qname_source"),
    EncodingSpec(column="rule_name"),
)


@dataclass(frozen=True)
class EdgeBuildOptions:
    """Configure which edge families are emitted."""

    emit_symbol_role_edges: bool = True
    emit_scip_symbol_relationship_edges: bool = True
    emit_import_edges: bool = True
    emit_call_edges: bool = True
    emit_qname_fallback_call_edges: bool = True
    emit_diagnostic_edges: bool = True
    emit_type_edges: bool = True
    emit_runtime_edges: bool = True


@dataclass(frozen=True)
class EdgeBuildInputs:
    """Input tables for edge construction."""

    relationship_outputs: Mapping[str, TableLike] | None = None
    scip_symbol_relationships: TableLike | None = None
    diagnostics_norm: TableLike | None = None
    repo_files: TableLike | None = None
    type_exprs_norm: TableLike | None = None
    rt_signatures: TableLike | None = None
    rt_signature_params: TableLike | None = None
    rt_members: TableLike | None = None


def _edge_inputs_from_legacy(legacy: Mapping[str, object]) -> EdgeBuildInputs:
    relationship_outputs = legacy.get("relationship_outputs")
    scip_symbol_relationships = legacy.get("scip_symbol_relationships")
    diagnostics_norm = legacy.get("diagnostics_norm")
    repo_files = legacy.get("repo_files")
    type_exprs_norm = legacy.get("type_exprs_norm")
    rt_signatures = legacy.get("rt_signatures")
    rt_signature_params = legacy.get("rt_signature_params")
    rt_members = legacy.get("rt_members")
    return EdgeBuildInputs(
        relationship_outputs=relationship_outputs
        if isinstance(relationship_outputs, Mapping)
        else None,
        scip_symbol_relationships=scip_symbol_relationships
        if isinstance(scip_symbol_relationships, TableLike)
        else None,
        diagnostics_norm=diagnostics_norm if isinstance(diagnostics_norm, TableLike) else None,
        repo_files=repo_files if isinstance(repo_files, TableLike) else None,
        type_exprs_norm=type_exprs_norm if isinstance(type_exprs_norm, TableLike) else None,
        rt_signatures=rt_signatures if isinstance(rt_signatures, TableLike) else None,
        rt_signature_params=rt_signature_params
        if isinstance(rt_signature_params, TableLike)
        else None,
        rt_members=rt_members if isinstance(rt_members, TableLike) else None,
    )


def _edge_catalog(inputs: EdgeBuildInputs) -> TableCatalog:
    catalog = TableCatalog()
    if inputs.relationship_outputs:
        for name, table in inputs.relationship_outputs.items():
            if isinstance(table, TableLike):
                catalog.add(name, table)
    catalog.extend(
        {
            name: table
            for name, table in {
                "scip_symbol_relationships": inputs.scip_symbol_relationships,
                "diagnostics_norm": inputs.diagnostics_norm,
                "repo_files": inputs.repo_files,
                "type_exprs_norm": inputs.type_exprs_norm,
                "rt_signatures": inputs.rt_signatures,
                "rt_signature_params": inputs.rt_signature_params,
                "rt_members": inputs.rt_members,
            }.items()
            if table is not None
        }
    )
    return catalog


def build_cpg_edges_raw(
    *,
    inputs: EdgeBuildInputs | None = None,
    options: EdgeBuildOptions | None = None,
    ctx: ExecutionContext | None = None,
    **legacy: object,
) -> TableLike:
    """Emit raw CPG edges without finalization.

    Returns
    -------
    pyarrow.Table
        Raw edges table.

    Raises
    ------
    ValueError
        Raised when no execution context is provided.
    """
    if ctx is None:
        msg = "build_cpg_edges_raw requires an execution context."
        raise ValueError(msg)
    options = options or EdgeBuildOptions()
    if inputs is None and legacy:
        inputs = _edge_inputs_from_legacy(legacy)
    catalog = _edge_catalog(inputs or EdgeBuildInputs())

    parts: list[TableLike] = []
    for spec in EDGE_RELATION_SPECS:
        enabled = getattr(options, spec.option_flag, None)
        if enabled is None:
            msg = f"Unknown option flag: {spec.option_flag}"
            raise ValueError(msg)
        if not enabled:
            continue
        rel = spec.build(catalog, ctx=ctx)
        if rel is None:
            continue
        parts.append(
            emit_edges_from_relation(
                rel,
                spec=spec.emit,
                schema_version=SCHEMA_VERSION,
                edge_schema=CPG_EDGES_SCHEMA,
            )
        )

    parts = [part for part in parts if part.num_rows]
    if not parts:
        return empty_edges()

    combined = unify_tables(spec=CPG_EDGES_SPEC, tables=parts, ctx=ctx)
    return encode_columns(combined, specs=EDGE_ENCODING_SPECS)


def build_cpg_edges(
    *,
    ctx: ExecutionContext,
    inputs: EdgeBuildInputs | None = None,
    options: EdgeBuildOptions | None = None,
) -> CpgBuildArtifacts:
    """Build and finalize CPG edges with quality artifacts.

    Returns
    -------
    CpgBuildArtifacts
        Finalize result plus quality table.
    """
    raw = build_cpg_edges_raw(inputs=inputs, options=options, ctx=ctx)
    quality = quality_from_ids(
        raw,
        id_col="edge_id",
        entity_kind="edge",
        issue="invalid_edge_id",
        source_table="cpg_edges_raw",
    )
    finalize = CPG_EDGES_SPEC.finalize_context(ctx).run(raw, ctx=ctx)
    return CpgBuildArtifacts(finalize=finalize, quality=quality)
