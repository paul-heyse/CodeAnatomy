"""Compile/optimize/emit pipeline for the semantic IR."""

from __future__ import annotations

from collections.abc import Collection, Mapping, Sequence
from typing import TYPE_CHECKING, Final

from semantics.ir import SemanticIR, SemanticIRJoinGroup, SemanticIRView
from semantics.ir_optimize import IRCost, order_join_groups, prune_ir
from semantics.naming import canonical_output_name
from semantics.registry import SEMANTIC_MODEL, SemanticModel, SemanticOutputSpec
from utils.hashing import hash64_from_text, hash_msgpack_canonical

if TYPE_CHECKING:
    from semantics.exprs import ExprSpec
    from semantics.quality import JoinHow, QualityRelationshipSpec

_KIND_ORDER: Mapping[str, int] = {
    "normalize": 0,
    "scip_normalize": 1,
    "join_group": 2,
    "relate": 3,
    "union_edges": 4,
    "union_nodes": 5,
    "projection": 6,
    "diagnostic": 7,
    "export": 8,
    "finalize": 9,
    "artifact": 10,
}
_MIN_JOIN_GROUP_SIZE = 2
MIN_VERSIONED_ALIAS_COUNT: Final[int] = 2


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

    Returns
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

    Returns
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
            continue
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


def _cost_hints_from_rows(rows: Sequence[object]) -> dict[str, IRCost]:
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


def compile_semantics(model: SemanticModel) -> SemanticIR:
    """Compile a semantic model into the IR.

    Parameters
    ----------
    model
        Semantic model describing normalization and relationships.

    Returns
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
        "dim_exported_defs_v1": (
            canonical_output_name("cst_defs_norm"),
            canonical_output_name("rel_def_symbol"),
        ),
    }
    diagnostic_inputs: dict[str, tuple[str, ...]] = {
        "relationship_quality_metrics_v1": tuple(spec.name for spec in model.relationship_specs),
        "relationship_ambiguity_report_v1": tuple(spec.name for spec in model.relationship_specs),
        "relationship_candidates_v1": tuple(spec.name for spec in model.relationship_specs),
        "relationship_decisions_v1": tuple(spec.name for spec in model.relationship_specs),
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

    Returns
    -------
    SemanticIR
        Optimized semantic IR with join fusion group metadata.

    Raises
    ------
    ValueError
        Raised when duplicate IR views conflict on definition.
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
    cost_hints = _cost_hints_from_rows(SEMANTIC_MODEL.dataset_rows())
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


def _parse_versioned_name(name: str) -> tuple[str, int | None]:
    base, sep, suffix = name.rpartition("_v")
    if sep and suffix.isdigit():
        return base, int(suffix)
    return name, None


def _validate_schema_migrations(rows: Sequence[object]) -> None:
    from semantics.catalog.dataset_specs import dataset_spec
    from semantics.migrations import migration_for, migration_skeleton
    from semantics.schema_diff import diff_contract_specs

    grouped: dict[str, list[tuple[str, int | None]]] = {}
    for row in rows:
        row_name = getattr(row, "name", None)
        if row_name is None:
            continue
        base, version = _parse_versioned_name(str(row_name))
        grouped.setdefault(base, []).append((str(row_name), version))

    for alias, entries in grouped.items():
        if len(entries) < MIN_VERSIONED_ALIAS_COUNT:
            continue
        if any(version is None for _name, version in entries):
            msg = f"Duplicate semantic dataset alias with unversioned name: {alias!r}."
            raise ValueError(msg)
        entries_sorted = sorted(entries, key=lambda item: item[1] or 0)
        latest_name, _ = entries_sorted[-1]
        latest_contract = dataset_spec(latest_name).contract_spec_or_default()
        for name, _version in entries_sorted:
            if name == latest_name:
                continue
            if migration_for(name, latest_name) is not None:
                continue
            diff = diff_contract_specs(
                dataset_spec(name).contract_spec_or_default(),
                latest_contract,
            )
            if not diff.is_breaking:
                continue
            diff_lines = diff.summary_lines() or ("no schema changes detected",)
            diff_summary = "\n".join(f"- {line}" for line in diff_lines)
            skeleton = migration_skeleton(name, latest_name, diff)
            msg = (
                f"Missing migration from {name!r} to {latest_name!r} "
                f"for dataset alias {alias!r}.\n"
                f"Schema diff:\n{diff_summary}\n\n"
                f"Suggested skeleton:\n{skeleton}"
            )
            raise ValueError(msg)


def emit_semantics(ir: SemanticIR) -> SemanticIR:
    """Emit semantic IR artifacts (placeholder).

    Returns
    -------
    SemanticIR
        Emitted semantic IR artifacts.
    """
    dataset_rows = SEMANTIC_MODEL.dataset_rows()
    _validate_schema_migrations(dataset_rows)
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


def build_semantic_ir(*, outputs: Collection[str] | None = None) -> SemanticIR:
    """Build the end-to-end semantic IR pipeline.

    Returns
    -------
    SemanticIR
        Semantic IR after compile/optimize/emit.
    """
    resolved_outputs = None
    if outputs is not None:
        resolved_outputs = {canonical_output_name(name) for name in outputs}
    compiled = compile_semantics(SEMANTIC_MODEL)
    optimized = optimize_semantics(compiled, outputs=resolved_outputs)
    return emit_semantics(optimized)


__all__ = [
    "build_semantic_ir",
    "compile_semantics",
    "emit_semantics",
    "optimize_semantics",
    "semantic_ir_fingerprint",
    "semantic_model_fingerprint",
]
