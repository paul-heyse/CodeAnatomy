"""Compile/optimize/emit pipeline for the semantic IR."""

from __future__ import annotations

from collections.abc import Collection, Mapping, Sequence
from typing import TYPE_CHECKING

from relspec.inference_confidence import (
    InferenceConfidence,
    high_confidence,
    low_confidence,
)
from semantics.catalog.dataset_registry import DatasetRegistrySpec
from semantics.catalog.dataset_rows import SEMANTIC_SCHEMA_VERSION, SemanticDatasetRow
from semantics.ir import (
    GraphPosition,
    InferredViewProperties,
    SemanticIR,
    SemanticIRJoinGroup,
    SemanticIRView,
)
from semantics.ir_optimize import IRCost, order_join_groups, prune_ir
from semantics.naming import canonical_output_name
from semantics.registry import SEMANTIC_MODEL, SemanticModel, SemanticOutputSpec
from semantics.view_kinds import VIEW_KIND_ORDER
from utils.hashing import hash64_from_text, hash_msgpack_canonical

if TYPE_CHECKING:
    from semantics.exprs import ExprSpec
    from semantics.quality import JoinHow, QualityRelationshipSpec
    from semantics.registry import SemanticNormalizationSpec

# Backward-compatible alias.
_KIND_ORDER = VIEW_KIND_ORDER
_MIN_JOIN_GROUP_SIZE = 2


def _expr_repr(expr_spec: ExprSpec) -> str:
    from semantics.exprs import ExprContextImpl

    ctx = ExprContextImpl(left_alias="l", right_alias="r")
    try:
        expr = expr_spec(ctx)
    except (RuntimeError, TypeError, ValueError) as exc:
        return f"<error:{type(exc).__name__}:{exc}>"
    return str(expr)


def _relationship_payload(spec: QualityRelationshipSpec) -> Mapping[str, object]:
    signals = spec.signals
    rank = spec.rank
    return {
        "name": spec.name,
        "left_view": spec.left_view,
        "right_view": spec.right_view,
        "left_on": tuple(spec.left_on),
        "right_on": tuple(spec.right_on),
        "how": spec.how,
        "origin": spec.origin,
        "provider": spec.provider,
        "rule_name": spec.rule_name,
        "signals": {
            "base_score": signals.base_score,
            "base_confidence": signals.base_confidence,
            "quality_score_column": signals.quality_score_column,
            "quality_weight": signals.quality_weight,
            "hard": [_expr_repr(hard.predicate) for hard in signals.hard],
            "features": [
                {"name": feat.name, "weight": feat.weight, "expr": _expr_repr(feat.expr)}
                for feat in signals.features
            ],
        },
        "rank": None
        if rank is None
        else {
            "ambiguity_key_expr": _expr_repr(rank.ambiguity_key_expr),
            "ambiguity_group_id_expr": None
            if rank.ambiguity_group_id_expr is None
            else _expr_repr(rank.ambiguity_group_id_expr),
            "order_by": [
                {"direction": order.direction, "expr": _expr_repr(order.expr)}
                for order in rank.order_by
            ],
            "keep": rank.keep,
            "top_k": rank.top_k,
        },
        "select_exprs": [
            {"alias": sel.alias, "expr": _expr_repr(sel.expr)} for sel in spec.select_exprs
        ],
        "join_file_quality": spec.join_file_quality,
        "file_quality_view": spec.file_quality_view,
    }


def semantic_model_fingerprint(model: SemanticModel) -> str:
    """Return a deterministic fingerprint for a semantic model.

    Returns:
    -------
    str
        Stable hash for the semantic model inputs.
    """
    payload = {
        "normalization_specs": [
            {
                "source_table": spec.source_table,
                "output_name": spec.output_name,
                "include_in_cpg_nodes": spec.include_in_cpg_nodes,
            }
            for spec in model.normalization_specs
        ],
        "relationships": [_relationship_payload(spec) for spec in model.relationship_specs],
        "outputs": [
            {"name": spec.name, "kind": spec.kind, "contract_ref": spec.contract_ref}
            for spec in model.outputs
        ],
    }
    return hash_msgpack_canonical(payload)


def semantic_ir_fingerprint(ir: SemanticIR) -> str:
    """Return a deterministic fingerprint for a semantic IR.

    Returns:
    -------
    str
        Stable hash for the semantic IR structure.
    """
    payload = {
        "views": [
            {
                "name": view.name,
                "kind": view.kind,
                "inputs": view.inputs,
                "outputs": view.outputs,
            }
            for view in ir.views
        ],
        "join_groups": [
            {
                "name": group.name,
                "left_view": group.left_view,
                "right_view": group.right_view,
                "left_on": group.left_on,
                "right_on": group.right_on,
                "how": group.how,
                "relationship_names": group.relationship_names,
            }
            for group in ir.join_groups
        ],
    }
    return hash_msgpack_canonical(payload)


def _join_group_name(
    *,
    left_view: str,
    right_view: str,
    left_on: tuple[str, ...],
    right_on: tuple[str, ...],
    how: JoinHow,
) -> str:
    key_parts = (
        left_view,
        right_view,
        how,
        ",".join(left_on),
        ",".join(right_on),
    )
    signature = "|".join(key_parts)
    digest = hash64_from_text(signature)
    return f"join_{left_view}__{right_view}__{digest:x}"


def _resolve_keys_from_inferred(
    view: SemanticIRView,
) -> tuple[tuple[str, ...], tuple[str, ...]] | None:
    """Extract FILE_IDENTITY equi-join keys from inferred view properties.

    Mirrors the compiler's ``_resolve_join_keys`` filtering logic: only
    exact-name pairs from the FILE_IDENTITY compatibility group are
    returned.  This ensures join group optimization produces groupings
    consistent with runtime join resolution.

    Parameters
    ----------
    view
        IR view with optional inferred_properties.

    Returns:
    -------
    tuple[tuple[str, ...], tuple[str, ...]] | None
        ``(left_on, right_on)`` if FILE_IDENTITY keys were inferred,
        None otherwise.
    """
    props = view.inferred_properties
    if props is None or props.inferred_join_keys is None:
        return None
    # Filter to FILE_IDENTITY exact-name pairs only, matching the
    # compiler's _resolve_join_keys semantics.
    left_cols: list[str] = []
    right_cols: list[str] = []
    for left_col, right_col in props.inferred_join_keys:
        if left_col != right_col:
            continue
        if left_col in _FILE_IDENTITY_NAMES:
            left_cols.append(left_col)
            right_cols.append(right_col)
    if not left_cols:
        return None
    return tuple(left_cols), tuple(right_cols)


def _build_join_groups(
    views: Sequence[SemanticIRView],
    relationship_specs: Mapping[str, QualityRelationshipSpec],
    *,
    existing_names: set[str],
) -> tuple[tuple[SemanticIRJoinGroup, ...], tuple[SemanticIRView, ...]]:
    grouped: dict[tuple[str, str, tuple[str, ...], tuple[str, ...], JoinHow], list[str]] = {}
    for view in views:
        if view.kind != "relate":
            continue
        rel_spec = relationship_specs.get(view.name)
        if rel_spec is None:
            continue
        left_on = tuple(rel_spec.left_on)
        right_on = tuple(rel_spec.right_on)
        if not left_on or not right_on:
            # Attempt to resolve from inferred properties when the spec
            # declares empty join keys (the default for quality specs).
            resolved = _resolve_keys_from_inferred(view)
            if resolved is None:
                continue
            left_on, right_on = resolved
        key = (
            rel_spec.left_view,
            rel_spec.right_view,
            left_on,
            right_on,
            rel_spec.how,
        )
        grouped.setdefault(key, []).append(view.name)

    join_groups: list[SemanticIRJoinGroup] = []
    join_group_views: list[SemanticIRView] = []
    for (left_view, right_view, left_on, right_on, how), names in grouped.items():
        if len(names) < _MIN_JOIN_GROUP_SIZE:
            continue
        group_name = _join_group_name(
            left_view=left_view,
            right_view=right_view,
            left_on=left_on,
            right_on=right_on,
            how=how,
        )
        if group_name in existing_names:
            msg = f"Join group name collision for {group_name!r}."
            raise ValueError(msg)
        existing_names.add(group_name)
        join_groups.append(
            SemanticIRJoinGroup(
                name=group_name,
                left_view=left_view,
                right_view=right_view,
                left_on=left_on,
                right_on=right_on,
                how=how,
                relationship_names=tuple(names),
            )
        )
        join_group_views.append(
            SemanticIRView(
                name=group_name,
                kind="join_group",
                inputs=(left_view, right_view),
                outputs=(group_name,),
            )
        )

    return tuple(join_groups), tuple(join_group_views)


def _metadata_float(meta: Mapping[bytes, bytes], key: bytes) -> float | None:
    value = meta.get(key)
    if value is None:
        return None
    if isinstance(value, bytes):
        try:
            return float(value.decode("utf-8"))
        except (TypeError, ValueError):
            return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _cost_hints_from_rows(rows: Sequence[SemanticDatasetRow]) -> dict[str, IRCost]:
    hints: dict[str, IRCost] = {}
    for row in rows:
        name = getattr(row, "name", None)
        meta = getattr(row, "metadata_extra", None)
        if not isinstance(name, str) or not isinstance(meta, Mapping):
            continue
        row_count = _metadata_float(meta, b"row_count_hint")
        selectivity = _metadata_float(meta, b"selectivity_hint")
        if row_count is None and selectivity is None:
            continue
        hints[name] = IRCost(
            row_count=int(row_count) if row_count is not None else None,
            selectivity=selectivity,
        )
    return hints


def _row_from_registry_spec(
    spec: DatasetRegistrySpec,
    *,
    fields: tuple[str, ...] | None = None,
) -> SemanticDatasetRow:
    resolved_fields = fields if fields is not None else spec.fields
    return SemanticDatasetRow(
        name=spec.name,
        version=spec.version,
        bundles=spec.bundles,
        fields=resolved_fields,
        category=spec.category,
        supports_cdf=spec.supports_cdf,
        partition_cols=spec.partition_cols,
        merge_keys=spec.merge_keys,
        join_keys=spec.join_keys,
        template=spec.template,
        view_builder=spec.view_builder,
        kind=spec.kind,
        semantic_id=spec.semantic_id,
        entity=spec.entity,
        grain=spec.grain,
        stability=spec.stability,
        schema_ref=spec.schema_ref,
        materialization=spec.materialization,
        materialized_name=spec.materialized_name,
        metadata_extra=spec.metadata_extra,
        register_view=spec.register_view,
        source_dataset=spec.source_dataset,
        role=spec.role,
    )


def _build_normalize_rows() -> tuple[SemanticDatasetRow, ...]:
    from semantics.catalog.normalize_registry import NORMALIZE_DATASETS

    return tuple(_row_from_registry_spec(spec) for spec in NORMALIZE_DATASETS)


def _build_semantic_normalization_rows(
    model: SemanticModel,
) -> tuple[SemanticDatasetRow, ...]:
    def _normalized_fields(spec: SemanticNormalizationSpec) -> tuple[str, ...]:
        source_fields = _input_schema_fields(spec.source_table)
        derived_fields: list[str] = [
            spec.spec.primary_span.canonical_start,
            spec.spec.primary_span.canonical_end,
            spec.spec.primary_span.canonical_span,
            spec.spec.entity_id.out_col,
            "path",
        ]
        if spec.spec.entity_id.canonical_entity_id is not None:
            derived_fields.append(spec.spec.entity_id.canonical_entity_id)
        derived_fields.extend(foreign_key.out_col for foreign_key in spec.spec.foreign_keys)
        combined = tuple(name for name in (*source_fields, *derived_fields) if name)
        deduped = tuple(dict.fromkeys(combined))
        bundle_fields = {"file_id", "path", "span"}
        return tuple(name for name in deduped if name not in bundle_fields)

    return tuple(
        SemanticDatasetRow(
            name=spec.output_name,
            version=SEMANTIC_SCHEMA_VERSION,
            bundles=("file_identity", "span"),
            fields=_normalized_fields(spec),
            category="semantic",
            supports_cdf=True,
            partition_cols=(),
            merge_keys=(spec.spec.entity_id.out_col,),
            join_keys=(spec.spec.entity_id.out_col,),
            template="semantic_normalize",
            view_builder=f"{spec.source_table}_norm_df_builder",
            register_view=True,
            source_dataset=spec.source_table,
        )
        for spec in model.normalization_specs
    )


def _build_relationship_rows(
    model: SemanticModel,
) -> tuple[SemanticDatasetRow, ...]:
    feature_fields = tuple(
        f"feat_{name}"
        for name in sorted(
            {feature.name for spec in model.relationship_specs for feature in spec.signals.features}
        )
    )
    max_hard = max((len(spec.signals.hard) for spec in model.relationship_specs), default=0)
    hard_fields = tuple(f"hard_{index}" for index in range(1, max_hard + 1))

    def _relationship_fields() -> tuple[str, ...]:
        return (
            "entity_id",
            "symbol",
            "path",
            "bstart",
            "bend",
            "origin",
            "confidence",
            "score",
            "provider",
            "rule_name",
            "ambiguity_group_id",
            *hard_fields,
            *feature_fields,
        )

    rows: list[SemanticDatasetRow] = []
    for spec in model.relationship_specs:
        join_keys = ("entity_id", "symbol")
        rows.append(
            SemanticDatasetRow(
                name=spec.name,
                version=SEMANTIC_SCHEMA_VERSION,
                bundles=(),
                fields=_relationship_fields(),
                category="semantic",
                supports_cdf=True,
                partition_cols=(),
                merge_keys=join_keys,
                join_keys=join_keys,
                template="semantic_relationship",
                view_builder=f"{spec.name}_df_builder",
                register_view=True,
                source_dataset=None,
            )
        )
    return tuple(rows)


def _build_singleton_rows() -> tuple[SemanticDatasetRow, ...]:
    from semantics.catalog.semantic_singletons_registry import SEMANTIC_SINGLETON_DATASETS

    return tuple(_row_from_registry_spec(spec) for spec in SEMANTIC_SINGLETON_DATASETS)


def _build_diagnostic_rows() -> tuple[SemanticDatasetRow, ...]:
    from semantics.catalog.diagnostics_registry import DIAGNOSTIC_DATASETS
    from semantics.registry import RELATIONSHIP_SPECS

    feature_fields = tuple(
        f"feat_{name}"
        for name in sorted(
            {feature.name for spec in RELATIONSHIP_SPECS for feature in spec.signals.features}
        )
    )
    max_hard = max((len(spec.signals.hard) for spec in RELATIONSHIP_SPECS), default=0)
    hard_fields = tuple(f"hard_{index}" for index in range(1, max_hard + 1))
    dynamic_fields = {
        "relationship_candidates",
        "relationship_decisions",
    }
    rows: list[SemanticDatasetRow] = []
    for spec in DIAGNOSTIC_DATASETS:
        fields = spec.fields
        if spec.name in dynamic_fields:
            fields = (*fields, *hard_fields, *feature_fields)
        rows.append(_row_from_registry_spec(spec, fields=fields))
    return tuple(rows)


def _build_export_rows() -> tuple[SemanticDatasetRow, ...]:
    from semantics.catalog.export_registry import EXPORT_DATASETS

    return tuple(_row_from_registry_spec(spec) for spec in EXPORT_DATASETS)


def _build_cpg_output_rows() -> tuple[SemanticDatasetRow, ...]:
    from cpg.emit_specs import cpg_output_specs

    rows: list[SemanticDatasetRow] = []
    for spec in cpg_output_specs():
        canonical = canonical_output_name(spec.name)
        source_dataset = canonical_output_name(spec.source_dataset) if spec.source_dataset else None
        rows.append(
            SemanticDatasetRow(
                name=canonical,
                version=SEMANTIC_SCHEMA_VERSION,
                bundles=spec.bundles,
                fields=spec.fields,
                category="semantic",
                supports_cdf=True,
                partition_cols=spec.partition_cols,
                merge_keys=spec.merge_keys,
                join_keys=spec.join_keys,
                template=spec.template,
                view_builder=spec.view_builder,
                register_view=True,
                source_dataset=source_dataset,
                entity=spec.entity,
                grain=spec.grain,
                stability="design",
                materialization="delta",
                materialized_name=f"semantic.{canonical}",
            )
        )
    return tuple(rows)


def _build_semantic_dataset_rows(model: SemanticModel) -> tuple[SemanticDatasetRow, ...]:
    rows: list[SemanticDatasetRow] = []
    rows.extend(_build_input_rows())
    rows.extend(_build_normalize_rows())
    rows.extend(_build_semantic_normalization_rows(model))
    rows.extend(_build_singleton_rows())
    rows.extend(_build_relationship_rows(model))
    rows.extend(_build_diagnostic_rows())
    rows.extend(_build_export_rows())
    rows.extend(_build_cpg_output_rows())
    return tuple(rows)


def _input_dataset_names() -> tuple[str, ...]:
    from semantics.registry import SEMANTIC_TABLE_SPECS

    names = tuple(spec.table for spec in SEMANTIC_TABLE_SPECS.values())
    extras = ("repo_files_v1", "scip_occurrences", "file_line_index_v1")
    return tuple(dict.fromkeys((*names, *extras)))


def _input_schema_fields(name: str) -> tuple[str, ...]:
    from datafusion_engine.extract.registry import dataset_schema

    schema = dataset_schema(name)
    return tuple(getattr(schema, "names", ()))


def _build_input_rows() -> tuple[SemanticDatasetRow, ...]:
    return tuple(
        SemanticDatasetRow(
            name=name,
            version=SEMANTIC_SCHEMA_VERSION,
            bundles=(),
            fields=_input_schema_fields(name),
            category="analysis",
            supports_cdf=False,
            partition_cols=(),
            merge_keys=None,
            join_keys=(),
            template=None,
            view_builder=None,
            kind="table",
            semantic_id=None,
            entity=None,
            grain=None,
            stability="design",
            schema_ref=None,
            materialization=None,
            materialized_name=None,
            metadata_extra={},
            register_view=False,
            source_dataset=name,
            role="input",
        )
        for name in _input_dataset_names()
    )


def _dataset_rows_for_model(model: SemanticModel) -> tuple[SemanticDatasetRow, ...]:
    return _build_semantic_dataset_rows(model)


def compile_semantics(model: SemanticModel) -> SemanticIR:
    """Compile a semantic model into the IR.

    Parameters
    ----------
    model
        Semantic model describing normalization and relationships.

    Returns:
    -------
    SemanticIR
        Compiled semantic IR.
    """
    views = [
        SemanticIRView(
            name=spec.output_name,
            kind="normalize",
            inputs=(spec.source_table,),
            outputs=(spec.output_name,),
        )
        for spec in model.normalization_specs
    ]
    scip_norm = canonical_output_name("scip_occurrences_norm")
    views.append(
        SemanticIRView(
            name=scip_norm,
            kind="scip_normalize",
            inputs=("scip_occurrences", "file_line_index_v1"),
            outputs=(scip_norm,),
        )
    )
    views.append(
        SemanticIRView(
            name="py_bc_line_table_with_bytes",
            kind="bytecode_line_index",
            inputs=("py_bc_line_table", "file_line_index_v1"),
            outputs=("py_bc_line_table_with_bytes",),
        )
    )
    views.extend(
        (
            SemanticIRView(
                name="ast_span_unnest",
                kind="span_unnest",
                inputs=("ast_nodes",),
                outputs=("ast_span_unnest",),
            ),
            SemanticIRView(
                name="ts_span_unnest",
                kind="span_unnest",
                inputs=("ts_nodes",),
                outputs=("ts_span_unnest",),
            ),
            SemanticIRView(
                name="symtable_span_unnest",
                kind="span_unnest",
                inputs=("symtable_scopes",),
                outputs=("symtable_span_unnest",),
            ),
            SemanticIRView(
                name="py_bc_instruction_span_unnest",
                kind="span_unnest",
                inputs=("py_bc_instructions",),
                outputs=("py_bc_instruction_span_unnest",),
            ),
        )
    )
    views.extend(
        (
            SemanticIRView(
                name="symtable_bindings",
                kind="symtable",
                inputs=("symtable_scopes", "symtable_symbols"),
                outputs=("symtable_bindings",),
            ),
            SemanticIRView(
                name="symtable_def_sites",
                kind="symtable",
                inputs=("symtable_bindings", "cst_defs"),
                outputs=("symtable_def_sites",),
            ),
            SemanticIRView(
                name="symtable_use_sites",
                kind="symtable",
                inputs=("symtable_bindings", "cst_refs"),
                outputs=("symtable_use_sites",),
            ),
            SemanticIRView(
                name="symtable_type_params",
                kind="symtable",
                inputs=("symtable_scopes",),
                outputs=("symtable_type_params",),
            ),
            SemanticIRView(
                name="symtable_type_param_edges",
                kind="symtable",
                inputs=("symtable_scope_edges", "symtable_scopes"),
                outputs=("symtable_type_param_edges",),
            ),
        )
    )
    views.extend(
        SemanticIRView(
            name=spec.name,
            kind="relate",
            inputs=(spec.left_view, spec.right_view),
            outputs=(spec.name,),
        )
        for spec in model.relationship_specs
    )
    views.append(
        SemanticIRView(
            name=model.semantic_edges_union_name,
            kind="union_edges",
            inputs=tuple(spec.name for spec in model.relationship_specs),
            outputs=(model.semantic_edges_union_name,),
        )
    )
    views.append(
        SemanticIRView(
            name=model.semantic_nodes_union_name,
            kind="union_nodes",
            inputs=model.normalization_output_names(include_nodes_only=True),
            outputs=(model.semantic_nodes_union_name,),
        )
    )
    from relspec.view_defs import RELATION_OUTPUT_NAME

    views.append(
        SemanticIRView(
            name=RELATION_OUTPUT_NAME,
            kind="projection",
            inputs=tuple(spec.name for spec in model.relationship_specs),
            outputs=(RELATION_OUTPUT_NAME,),
        )
    )

    export_inputs: dict[str, tuple[str, ...]] = {
        "dim_exported_defs": (
            canonical_output_name("cst_defs_norm"),
            canonical_output_name("rel_def_symbol"),
        ),
    }
    diagnostic_inputs: dict[str, tuple[str, ...]] = {
        "relationship_quality_metrics": tuple(spec.name for spec in model.relationship_specs),
        "relationship_ambiguity_report": tuple(spec.name for spec in model.relationship_specs),
        "relationship_candidates": tuple(spec.name for spec in model.relationship_specs),
        "relationship_decisions": tuple(spec.name for spec in model.relationship_specs),
    }

    def _emit_output_spec(spec: SemanticOutputSpec) -> None:
        if spec.kind == "diagnostic":
            views.append(
                SemanticIRView(
                    name=spec.name,
                    kind="diagnostic",
                    inputs=diagnostic_inputs.get(spec.name, ()),
                    outputs=(spec.name,),
                )
            )
            return
        if spec.kind == "export":
            views.append(
                SemanticIRView(
                    name=spec.name,
                    kind="export",
                    inputs=export_inputs.get(spec.name, ()),
                    outputs=(spec.name,),
                )
            )

    for output_spec in model.outputs:
        _emit_output_spec(output_spec)

    cpg_entities = model.cpg_entity_specs()
    cpg_node_inputs = tuple(
        sorted({spec.node_table for spec in cpg_entities if spec.node_table is not None})
    )
    prop_inputs: set[str] = set()
    for spec in cpg_entities:
        if not spec.prop_source_map:
            continue
        table_name = spec.prop_table or spec.node_table
        if table_name is None:
            continue
        prop_inputs.add(table_name)
    cpg_prop_inputs = tuple(sorted(prop_inputs))
    cpg_nodes = canonical_output_name("cpg_nodes")
    cpg_edges = canonical_output_name("cpg_edges")
    cpg_props = canonical_output_name("cpg_props")
    cpg_props_map = canonical_output_name("cpg_props_map")
    cpg_edges_by_src = canonical_output_name("cpg_edges_by_src")
    cpg_edges_by_dst = canonical_output_name("cpg_edges_by_dst")

    views.extend(
        [
            SemanticIRView(
                name=cpg_nodes,
                kind="finalize",
                inputs=cpg_node_inputs,
                outputs=(cpg_nodes,),
            ),
            SemanticIRView(
                name=cpg_edges,
                kind="finalize",
                inputs=(RELATION_OUTPUT_NAME,),
                outputs=(cpg_edges,),
            ),
            SemanticIRView(
                name=cpg_props,
                kind="finalize",
                inputs=tuple(sorted({*cpg_prop_inputs, cpg_edges})),
                outputs=(cpg_props,),
            ),
            SemanticIRView(
                name=cpg_props_map,
                kind="finalize",
                inputs=(cpg_props,),
                outputs=(cpg_props_map,),
            ),
            SemanticIRView(
                name=cpg_edges_by_src,
                kind="finalize",
                inputs=(cpg_edges,),
                outputs=(cpg_edges_by_src,),
            ),
            SemanticIRView(
                name=cpg_edges_by_dst,
                kind="finalize",
                inputs=(cpg_edges,),
                outputs=(cpg_edges_by_dst,),
            ),
        ]
    )

    return SemanticIR(views=tuple(views))


def optimize_semantics(
    ir: SemanticIR,
    *,
    outputs: Collection[str] | None = None,
) -> SemanticIR:
    """Optimize the semantic IR with join fusion grouping and deduplication.

    Args:
        ir: Semantic IR to optimize.
        outputs: Optional output node allowlist.

    Returns:
        SemanticIR: Result.

    Raises:
        ValueError: If requested outputs are not present in the IR.
    """
    seen: dict[str, SemanticIRView] = {}
    for view in ir.views:
        prior = seen.get(view.name)
        if prior is None:
            seen[view.name] = view
            continue
        if prior != view:
            msg = f"Duplicate semantic IR view with different definitions: {view.name!r}."
            raise ValueError(msg)
    base_views = list(seen.values())
    relationship_specs = {spec.name: spec for spec in SEMANTIC_MODEL.relationship_specs}
    join_groups, join_group_views = _build_join_groups(
        base_views,
        relationship_specs,
        existing_names=set(seen),
    )
    cost_hints = _cost_hints_from_rows(_dataset_rows_for_model(SEMANTIC_MODEL))
    ordered_join_groups = order_join_groups(join_groups, costs=cost_hints)
    join_view_lookup = {view.name: view for view in join_group_views}
    ordered_join_group_views = tuple(
        join_view_lookup[group.name]
        for group in ordered_join_groups
        if group.name in join_view_lookup
    )

    ordered = sorted(
        (*base_views, *ordered_join_group_views),
        key=lambda view: (
            _KIND_ORDER.get(view.kind, 99),
            view.inputs if view.kind in {"relate", "join_group"} else (),
            view.name,
        ),
    )
    optimized = SemanticIR(
        views=tuple(ordered),
        dataset_rows=ir.dataset_rows,
        cpg_node_specs=ir.cpg_node_specs,
        cpg_prop_specs=ir.cpg_prop_specs,
        join_groups=ordered_join_groups,
    )
    return prune_ir(optimized, outputs=outputs)


def emit_semantics(ir: SemanticIR) -> SemanticIR:
    """Emit semantic IR artifacts (placeholder).

    Returns:
    -------
    SemanticIR
        Emitted semantic IR artifacts.
    """
    dataset_rows = _dataset_rows_for_model(SEMANTIC_MODEL)
    model_hash = semantic_model_fingerprint(SEMANTIC_MODEL)
    ir_hash = semantic_ir_fingerprint(ir)
    return SemanticIR(
        views=ir.views,
        dataset_rows=dataset_rows,
        cpg_node_specs=SEMANTIC_MODEL.cpg_node_specs(),
        cpg_prop_specs=SEMANTIC_MODEL.cpg_prop_specs(),
        join_groups=ir.join_groups,
        model_hash=model_hash,
        ir_hash=ir_hash,
    )


_SPAN_FIELD_NAMES: frozenset[str] = frozenset({"bstart", "bend"})
_FILE_IDENTITY_NAMES: frozenset[str] = frozenset({"file_id", "path"})
_SYMBOL_NAMES: frozenset[str] = frozenset({"symbol", "qname"})
_MIN_BINARY_INPUTS = 2
_HIGH_FAN_OUT_THRESHOLD = 3


def _build_consumer_map(
    views: Sequence[SemanticIRView],
) -> dict[str, list[str]]:
    consumers: dict[str, list[str]] = {}
    view_names = {view.name for view in views}
    for view in views:
        for inp in view.inputs:
            if inp in view_names:
                consumers.setdefault(inp, []).append(view.name)
    return consumers


def _classify_graph_position(
    view: SemanticIRView,
    view_names: frozenset[str],
    consumer_map: Mapping[str, list[str]],
) -> GraphPosition:
    has_upstream = any(inp in view_names for inp in view.inputs)
    downstream = consumer_map.get(view.name, [])
    if len(downstream) >= _HIGH_FAN_OUT_THRESHOLD:
        return "high_fan_out"
    if not has_upstream:
        return "source"
    if not downstream:
        return "terminal"
    return "intermediate"


def _infer_join_strategy_from_fields(
    left_fields: frozenset[str],
    right_fields: frozenset[str],
) -> str | None:
    left_has_spans = _SPAN_FIELD_NAMES.issubset(left_fields)
    right_has_spans = _SPAN_FIELD_NAMES.issubset(right_fields)
    left_has_file = bool(left_fields & _FILE_IDENTITY_NAMES)
    right_has_file = bool(right_fields & _FILE_IDENTITY_NAMES)

    if left_has_spans and right_has_spans and left_has_file and right_has_file:
        return "span_overlap"

    left_fk = {f for f in left_fields if f.endswith("_id") and f != "entity_id"}
    right_has_entity = "entity_id" in right_fields
    right_fk = {f for f in right_fields if f.endswith("_id") and f != "entity_id"}
    left_has_entity = "entity_id" in left_fields
    if (left_fk and right_has_entity) or (right_fk and left_has_entity):
        return "foreign_key"

    left_has_symbol = bool(left_fields & _SYMBOL_NAMES)
    right_has_symbol = bool(right_fields & _SYMBOL_NAMES)
    if left_has_symbol and right_has_symbol:
        return "symbol_match"

    if left_has_file and right_has_file:
        return "equi_join"

    return None


def _infer_join_keys_from_fields(
    left_fields: frozenset[str],
    right_fields: frozenset[str],
) -> tuple[tuple[str, str], ...] | None:
    common = sorted(left_fields & right_fields)
    if not common:
        return None
    priority_groups = [_FILE_IDENTITY_NAMES, _SPAN_FIELD_NAMES, _SYMBOL_NAMES]
    result: list[tuple[str, str]] = []
    for group in priority_groups:
        for name in sorted(group):
            if name in left_fields and name in right_fields and (name, name) not in result:
                result.append((name, name))
    for name in common:
        if (name, name) not in result:
            result.append((name, name))
    return tuple(result) if result else None


def _cache_policy_for_position(position: GraphPosition) -> str | None:
    if position == "high_fan_out":
        return "eager"
    if position == "terminal":
        return "lazy"
    return None


# Confidence scores mirror those in semantics.joins.inference so that
# the lightweight field-based inference in the IR pipeline produces
# comparable confidence values.
_STRATEGY_CONFIDENCE: dict[str, float] = {
    "span_overlap": 0.95,
    "foreign_key": 0.85,
    "symbol_match": 0.75,
    "equi_join": 0.6,
}
_CACHE_POLICY_CONFIDENCE: dict[str, float] = {
    "eager": 0.9,
    "lazy": 0.85,
}
_CONFIDENCE_THRESHOLD: float = 0.8
_DECISION_TYPE_JOIN_STRATEGY: str = "join_strategy"
_DECISION_TYPE_CACHE_POLICY: str = "cache_policy"


def _build_view_inference_confidence(
    *,
    strategy: str | None,
    cache_hint: str | None,
) -> InferenceConfidence | None:
    """Build structured confidence for the strongest inference signal.

    When both a join strategy and a cache policy hint were inferred,
    select the one with higher confidence as the representative.

    Returns:
    -------
    InferenceConfidence | None
        Confidence metadata, or ``None`` when no signal is available.
    """
    strategy_score = _STRATEGY_CONFIDENCE.get(strategy, 0.0) if strategy else 0.0
    cache_score = _CACHE_POLICY_CONFIDENCE.get(cache_hint, 0.0) if cache_hint else 0.0

    if strategy_score == 0.0 and cache_score == 0.0:
        return None

    # Pick the stronger signal.
    if strategy_score >= cache_score:
        decision_type = _DECISION_TYPE_JOIN_STRATEGY
        decision_value = strategy or ""
        score = strategy_score
        evidence = ("field_metadata", "schema")
    else:
        decision_type = _DECISION_TYPE_CACHE_POLICY
        decision_value = cache_hint or ""
        score = cache_score
        evidence = ("graph_topology",)

    if score >= _CONFIDENCE_THRESHOLD:
        return high_confidence(
            decision_type=decision_type,
            decision_value=decision_value,
            evidence_sources=evidence,
            score=score,
        )
    return low_confidence(
        decision_type=decision_type,
        decision_value=decision_value,
        fallback_reason="weak_field_evidence",
        evidence_sources=evidence,
        score=score,
    )


def _infer_view_properties(
    view: SemanticIRView,
    *,
    view_names: frozenset[str],
    consumer_map: Mapping[str, list[str]],
    field_index: Mapping[str, frozenset[str]],
    relationship_specs: Mapping[str, QualityRelationshipSpec],
) -> InferredViewProperties | None:
    position = _classify_graph_position(view, view_names, consumer_map)
    cache_hint = _cache_policy_for_position(position)

    if view.kind not in {"relate", "join_group"}:
        confidence = _build_view_inference_confidence(
            strategy=None,
            cache_hint=cache_hint,
        )
        return InferredViewProperties(
            graph_position=position,
            inferred_cache_policy=cache_hint,
            inference_confidence=confidence,
        )

    rel_spec = relationship_specs.get(view.name)
    if rel_spec is not None:
        left_name = rel_spec.left_view
        right_name = rel_spec.right_view
    elif len(view.inputs) >= _MIN_BINARY_INPUTS:
        left_name = view.inputs[0]
        right_name = view.inputs[1]
    else:
        confidence = _build_view_inference_confidence(
            strategy=None,
            cache_hint=cache_hint,
        )
        return InferredViewProperties(
            graph_position=position,
            inferred_cache_policy=cache_hint,
            inference_confidence=confidence,
        )

    left_fields = field_index.get(left_name, frozenset())
    right_fields = field_index.get(right_name, frozenset())

    if not left_fields or not right_fields:
        confidence = _build_view_inference_confidence(
            strategy=None,
            cache_hint=cache_hint,
        )
        return InferredViewProperties(
            graph_position=position,
            inferred_cache_policy=cache_hint,
            inference_confidence=confidence,
        )

    strategy = _infer_join_strategy_from_fields(left_fields, right_fields)
    keys = _infer_join_keys_from_fields(left_fields, right_fields)

    confidence = _build_view_inference_confidence(
        strategy=strategy,
        cache_hint=cache_hint,
    )
    return InferredViewProperties(
        inferred_join_strategy=strategy,
        inferred_join_keys=keys,
        inferred_cache_policy=cache_hint,
        graph_position=position,
        inference_confidence=confidence,
    )


_BUNDLE_IMPLIED_FIELDS: Mapping[str, frozenset[str]] = {
    "file_identity": frozenset({"file_id", "path"}),
    "span": frozenset({"bstart", "bend", "span"}),
}


def _build_field_index(
    model: SemanticModel,
) -> dict[str, frozenset[str]]:
    rows = _dataset_rows_for_model(model)
    index: dict[str, frozenset[str]] = {}
    for row in rows:
        explicit = frozenset(row.fields) if row.fields else frozenset()
        implied: frozenset[str] = frozenset()
        for bundle_name in row.bundles:
            implied |= _BUNDLE_IMPLIED_FIELDS.get(bundle_name, frozenset())
        combined = explicit | implied
        if combined:
            index[row.name] = combined
    return index


def infer_semantics(ir: SemanticIR) -> SemanticIR:
    """Infer derivable properties from schemas and graph topology.

    Insert between compile and optimize to enrich each view with
    inferred join strategies, join keys, cache policy hints, and
    graph position metadata.  This phase is **additive** -- it
    populates inferred_properties on each view without modifying
    any existing fields.

    Graceful degradation: if inference fails for any individual view,
    its inferred_properties is left as None.

    Parameters
    ----------
    ir
        Compiled semantic IR (output of compile_semantics).

    Returns:
    -------
    SemanticIR
        IR with inferred_properties populated on each view.
    """
    if not ir.views:
        return ir

    view_names = frozenset(view.name for view in ir.views)
    consumer_map = _build_consumer_map(ir.views)
    field_index = _build_field_index(SEMANTIC_MODEL)
    relationship_specs = {spec.name: spec for spec in SEMANTIC_MODEL.relationship_specs}

    enriched: list[SemanticIRView] = []
    for view in ir.views:
        try:
            props = _infer_view_properties(
                view,
                view_names=view_names,
                consumer_map=consumer_map,
                field_index=field_index,
                relationship_specs=relationship_specs,
            )
        except (AttributeError, KeyError, TypeError, ValueError):
            props = None
        enriched.append(
            SemanticIRView(
                name=view.name,
                kind=view.kind,
                inputs=view.inputs,
                outputs=view.outputs,
                inferred_properties=props,
            )
        )

    return SemanticIR(
        views=tuple(enriched),
        dataset_rows=ir.dataset_rows,
        cpg_node_specs=ir.cpg_node_specs,
        cpg_prop_specs=ir.cpg_prop_specs,
        join_groups=ir.join_groups,
        model_hash=ir.model_hash,
        ir_hash=ir.ir_hash,
    )


def build_semantic_ir(*, outputs: Collection[str] | None = None) -> SemanticIR:
    """Build the end-to-end semantic IR pipeline.

    Returns:
    -------
    SemanticIR
        Semantic IR after compile/infer/optimize/emit.
    """
    resolved_outputs = None
    if outputs is not None:
        resolved_outputs = {canonical_output_name(name) for name in outputs}
    compiled = compile_semantics(SEMANTIC_MODEL)
    inferred = infer_semantics(compiled)
    optimized = optimize_semantics(inferred, outputs=resolved_outputs)
    return emit_semantics(optimized)


__all__ = [
    "build_semantic_ir",
    "compile_semantics",
    "emit_semantics",
    "infer_semantics",
    "optimize_semantics",
    "semantic_ir_fingerprint",
    "semantic_model_fingerprint",
]
