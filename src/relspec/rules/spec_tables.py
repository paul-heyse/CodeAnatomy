"""Canonical spec tables for centralized rule definitions."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal

import pyarrow as pa

from arrowdsl.compute.expr_core import ExprSpec, ScalarValue
from arrowdsl.core.context import OrderingKey
from arrowdsl.plan.ops import JoinType, SortKey
from arrowdsl.plan.query import ProjectionSpec, QuerySpec
from arrowdsl.spec.codec import (
    decode_scalar_union,
    encode_scalar_union,
    parse_dedupe_strategy,
    parse_mapping_sequence,
    parse_sort_order,
    parse_string_tuple,
)
from arrowdsl.spec.expr_ir import ExprIR, expr_spec_from_json
from arrowdsl.spec.infra import SCALAR_UNION_TYPE
from arrowdsl.spec.io import table_from_rows
from arrowdsl.spec.tables.cpg import EDGE_EMIT_STRUCT
from arrowdsl.spec.tables.extract import (
    DERIVED_ID_STRUCT,
    ORDERING_KEY_STRUCT,
    ExtractDerivedIdSpec,
    ExtractOrderingKeySpec,
)
from arrowdsl.spec.tables.relspec import (
    HASH_JOIN_STRUCT,
    INTERVAL_ALIGN_STRUCT,
    KERNEL_SPEC_STRUCT,
    PROJECT_STRUCT,
    WINNER_SELECT_STRUCT,
    hash_join_row,
    interval_align_row,
    kernel_spec_row,
    project_row,
    winner_select_row,
)
from relspec.model import (
    AddLiteralSpec,
    CanonicalSortKernelSpec,
    DedupeKernelSpec,
    DedupeSpec,
    DropColumnsSpec,
    ExplodeListSpec,
    FilterKernelSpec,
    HashJoinConfig,
    IntervalAlignConfig,
    KernelSpecT,
    ProjectConfig,
    RenameColumnsSpec,
    WinnerSelectConfig,
)
from relspec.rules.definitions import (
    EdgeEmitPayload,
    EvidenceOutput,
    EvidenceSpec,
    ExecutionMode,
    ExtractPayload,
    NormalizePayload,
    PolicyOverrides,
    RelationshipPayload,
    RuleDefinition,
    RuleDomain,
    RulePayload,
    RuleStage,
)

IntervalMode = Literal["EXACT", "CONTAINED_BEST", "OVERLAP_BEST"]
IntervalHow = Literal["inner", "left"]
ScoreOrder = Literal["ascending", "descending"]


EVIDENCE_STRUCT = pa.struct(
    [
        pa.field("sources", pa.list_(pa.string()), nullable=True),
        pa.field("required_columns", pa.list_(pa.string()), nullable=True),
        pa.field("required_types", pa.map_(pa.string(), pa.string()), nullable=True),
        pa.field("required_metadata", pa.map_(pa.string(), pa.string()), nullable=True),
    ]
)

EVIDENCE_OUTPUT_STRUCT = pa.struct(
    [
        pa.field("column_map", pa.map_(pa.string(), pa.string()), nullable=True),
        pa.field(
            "literals",
            pa.list_(
                pa.struct(
                    [
                        pa.field("name", pa.string(), nullable=False),
                        pa.field("value_union", SCALAR_UNION_TYPE, nullable=True),
                    ]
                )
            ),
            nullable=True,
        ),
    ]
)

POLICY_OVERRIDE_STRUCT = pa.struct(
    [
        pa.field("confidence_policy", pa.string(), nullable=True),
        pa.field("ambiguity_policy", pa.string(), nullable=True),
    ]
)

QUERY_OP_STRUCT = pa.struct(
    [
        pa.field("kind", pa.string(), nullable=False),
        pa.field("columns", pa.list_(pa.string()), nullable=True),
        pa.field("name", pa.string(), nullable=True),
        pa.field("expr_json", pa.string(), nullable=True),
    ]
)

RELATIONSHIP_STRUCT = pa.struct(
    [
        pa.field("output_dataset", pa.string(), nullable=True),
        pa.field("contract_name", pa.string(), nullable=True),
        pa.field("hash_join", HASH_JOIN_STRUCT, nullable=True),
        pa.field("interval_align", INTERVAL_ALIGN_STRUCT, nullable=True),
        pa.field("winner_select", WINNER_SELECT_STRUCT, nullable=True),
        pa.field("predicate_expr", pa.string(), nullable=True),
        pa.field("project", PROJECT_STRUCT, nullable=True),
        pa.field("rule_name_col", pa.string(), nullable=True),
        pa.field("rule_priority_col", pa.string(), nullable=True),
        pa.field("edge_emit", EDGE_EMIT_STRUCT, nullable=True),
        pa.field("edge_option_flag", pa.string(), nullable=True),
    ]
)

NORMALIZE_STRUCT = pa.struct(
    [
        pa.field("plan_builder", pa.string(), nullable=True),
    ]
)

EXTRACT_STRUCT = pa.struct(
    [
        pa.field("version", pa.int32(), nullable=False),
        pa.field("template", pa.string(), nullable=True),
        pa.field("bundles", pa.list_(pa.string()), nullable=True),
        pa.field("fields", pa.list_(pa.string()), nullable=True),
        pa.field("derived", pa.list_(DERIVED_ID_STRUCT), nullable=True),
        pa.field("row_fields", pa.list_(pa.string()), nullable=True),
        pa.field("row_extras", pa.list_(pa.string()), nullable=True),
        pa.field("ordering_keys", pa.list_(ORDERING_KEY_STRUCT), nullable=True),
        pa.field("join_keys", pa.list_(pa.string()), nullable=True),
        pa.field("enabled_when", pa.string(), nullable=True),
        pa.field("feature_flag", pa.string(), nullable=True),
        pa.field("postprocess", pa.string(), nullable=True),
        pa.field("metadata_extra", pa.map_(pa.string(), pa.string()), nullable=True),
        pa.field("evidence_required_columns", pa.list_(pa.string()), nullable=True),
        pa.field("pipeline_name", pa.string(), nullable=True),
    ]
)

RULE_DEF_SCHEMA = pa.schema(
    [
        pa.field("name", pa.string(), nullable=False),
        pa.field("domain", pa.string(), nullable=False),
        pa.field("kind", pa.string(), nullable=False),
        pa.field("inputs", pa.list_(pa.string()), nullable=True),
        pa.field("output", pa.string(), nullable=False),
        pa.field("execution_mode", pa.string(), nullable=True),
        pa.field("priority", pa.int32(), nullable=True),
        pa.field("evidence", EVIDENCE_STRUCT, nullable=True),
        pa.field("evidence_output", EVIDENCE_OUTPUT_STRUCT, nullable=True),
        pa.field("policy_overrides", POLICY_OVERRIDE_STRUCT, nullable=True),
        pa.field("relationship_payload", RELATIONSHIP_STRUCT, nullable=True),
        pa.field("normalize_payload", NORMALIZE_STRUCT, nullable=True),
        pa.field("extract_payload", EXTRACT_STRUCT, nullable=True),
        pa.field("pipeline_ops", pa.list_(QUERY_OP_STRUCT), nullable=True),
        pa.field("post_kernels", pa.list_(KERNEL_SPEC_STRUCT), nullable=True),
    ],
    metadata={b"spec_kind": b"relspec_rule_definitions"},
)


def rule_definition_table(definitions: Sequence[RuleDefinition]) -> pa.Table:
    """Return a canonical rule definition table.

    Returns
    -------
    pyarrow.Table
        Rule definition table for centralized rules.
    """
    rows = [
        {
            "name": definition.name,
            "domain": definition.domain,
            "kind": definition.kind,
            "inputs": list(definition.inputs) or None,
            "output": definition.output,
            "execution_mode": definition.execution_mode,
            "priority": int(definition.priority),
            "evidence": _evidence_row(definition.evidence),
            "evidence_output": _evidence_output_row(definition.evidence_output),
            "policy_overrides": _policy_overrides_row(definition.policy_overrides),
            "relationship_payload": _relationship_payload_row(definition.payload),
            "normalize_payload": _normalize_payload_row(definition.payload),
            "extract_payload": _extract_payload_row(definition.payload),
            "pipeline_ops": _pipeline_ops_row(definition.pipeline_ops),
            "post_kernels": _kernel_rows(definition.post_kernels),
        }
        for definition in definitions
    ]
    return table_from_rows(RULE_DEF_SCHEMA, rows)


def rule_definitions_from_table(table: pa.Table) -> tuple[RuleDefinition, ...]:
    """Decode canonical rule definition table rows.

    Returns
    -------
    tuple[RuleDefinition, ...]
        Rule definitions decoded from the table.
    """
    definitions: list[RuleDefinition] = []
    for row in table.to_pylist():
        domain = _parse_domain(row.get("domain"))
        execution_mode = row.get("execution_mode")
        priority = row.get("priority")
        payload = _payload_from_row(domain, row)
        pipeline_ops = _pipeline_ops_from_row(row.get("pipeline_ops"))
        post_kernels = _kernels_from_row(row.get("post_kernels"))
        definitions.append(
            RuleDefinition(
                name=str(row["name"]),
                domain=domain,
                kind=str(row["kind"]),
                inputs=parse_string_tuple(row.get("inputs"), label="inputs"),
                output=str(row["output"]),
                execution_mode=_parse_execution_mode(execution_mode),
                priority=int(priority) if priority is not None else 100,
                evidence=_evidence_from_row(row.get("evidence")),
                evidence_output=_evidence_output_from_row(row.get("evidence_output")),
                policy_overrides=_policy_overrides_from_row(row.get("policy_overrides")),
                pipeline_ops=pipeline_ops,
                post_kernels=post_kernels,
                stages=_stages_from_payload(domain, payload),
                payload=payload,
            )
        )
    return tuple(definitions)


def _evidence_row(spec: EvidenceSpec | None) -> dict[str, object] | None:
    if spec is None:
        return None
    required_metadata = (
        {
            key.decode("utf-8"): value.decode("utf-8")
            for key, value in spec.required_metadata.items()
        }
        if spec.required_metadata
        else None
    )
    return {
        "sources": list(spec.sources) or None,
        "required_columns": list(spec.required_columns) or None,
        "required_types": dict(spec.required_types) or None,
        "required_metadata": required_metadata,
    }


def _evidence_from_row(payload: Mapping[str, Any] | None) -> EvidenceSpec | None:
    if payload is None:
        return None
    required_types = payload.get("required_types")
    required_types_map = dict(required_types) if isinstance(required_types, Mapping) else {}
    required_metadata = payload.get("required_metadata")
    metadata_map = dict(required_metadata) if isinstance(required_metadata, Mapping) else {}
    return EvidenceSpec(
        sources=parse_string_tuple(payload.get("sources"), label="sources"),
        required_columns=parse_string_tuple(
            payload.get("required_columns"), label="required_columns"
        ),
        required_types={str(key): str(val) for key, val in required_types_map.items()},
        required_metadata={
            str(key).encode("utf-8"): str(val).encode("utf-8") for key, val in metadata_map.items()
        },
    )


def _evidence_output_row(spec: EvidenceOutput | None) -> dict[str, object] | None:
    if spec is None:
        return None
    literals = [
        {"name": name, "value_union": encode_scalar_union(value)}
        for name, value in spec.literals.items()
    ]
    return {
        "column_map": dict(spec.column_map) or None,
        "literals": literals or None,
    }


def _evidence_output_from_row(payload: Mapping[str, Any] | None) -> EvidenceOutput | None:
    if payload is None:
        return None
    raw_map = payload.get("column_map")
    column_map = dict(raw_map) if isinstance(raw_map, Mapping) else {}
    literals: dict[str, ScalarValue] = {}
    for item in payload.get("literals") or ():
        name = item.get("name")
        if name is None:
            continue
        literals[str(name)] = decode_scalar_union(item.get("value_union"))
    if not column_map and not literals:
        return None
    return EvidenceOutput(column_map=column_map, literals=literals)


def _policy_overrides_row(overrides: PolicyOverrides) -> dict[str, object] | None:
    if overrides.confidence_policy is None and overrides.ambiguity_policy is None:
        return None
    return {
        "confidence_policy": overrides.confidence_policy,
        "ambiguity_policy": overrides.ambiguity_policy,
    }


def _policy_overrides_from_row(payload: Mapping[str, Any] | None) -> PolicyOverrides:
    if payload is None:
        return PolicyOverrides()
    return PolicyOverrides(
        confidence_policy=payload.get("confidence_policy"),
        ambiguity_policy=payload.get("ambiguity_policy"),
    )


def _relationship_payload_row(payload: object | None) -> dict[str, object] | None:
    if not isinstance(payload, RelationshipPayload):
        return None
    edge_emit = payload.edge_emit
    return {
        "output_dataset": payload.output_dataset,
        "contract_name": payload.contract_name,
        "hash_join": hash_join_row(payload.hash_join),
        "interval_align": interval_align_row(payload.interval_align),
        "winner_select": winner_select_row(payload.winner_select),
        "predicate_expr": payload.predicate.to_json() if payload.predicate is not None else None,
        "project": project_row(payload.project),
        "rule_name_col": payload.rule_name_col,
        "rule_priority_col": payload.rule_priority_col,
        "edge_emit": _edge_emit_row(edge_emit),
        "edge_option_flag": edge_emit.option_flag if edge_emit is not None else None,
    }


def _normalize_payload_row(payload: object | None) -> dict[str, object] | None:
    if not isinstance(payload, NormalizePayload):
        return None
    return {"plan_builder": payload.plan_builder}


def _extract_payload_row(payload: object | None) -> dict[str, object] | None:
    if not isinstance(payload, ExtractPayload):
        return None
    metadata_extra = (
        {
            key.decode("utf-8"): value.decode("utf-8")
            for key, value in payload.metadata_extra.items()
        }
        if payload.metadata_extra
        else None
    )
    return {
        "version": int(payload.version),
        "template": payload.template,
        "bundles": list(payload.bundles) or None,
        "fields": list(payload.fields) or None,
        "derived": _derived_rows(payload.derived_ids),
        "row_fields": list(payload.row_fields) or None,
        "row_extras": list(payload.row_extras) or None,
        "ordering_keys": _ordering_rows(payload.ordering_keys),
        "join_keys": list(payload.join_keys) or None,
        "enabled_when": payload.enabled_when,
        "feature_flag": payload.feature_flag,
        "postprocess": payload.postprocess,
        "metadata_extra": metadata_extra,
        "evidence_required_columns": list(payload.evidence_required_columns) or None,
        "pipeline_name": payload.pipeline_name,
    }


def _pipeline_ops_row(ops: Sequence[Mapping[str, object]]) -> list[dict[str, object]] | None:
    if not ops:
        return None
    return [dict(op) for op in ops]


def _pipeline_ops_from_row(
    payload: Sequence[Mapping[str, Any]] | None,
) -> tuple[Mapping[str, object], ...]:
    if not payload:
        return ()
    return tuple(dict(row) for row in payload)


def _kernel_rows(specs: Sequence[KernelSpecT]) -> list[dict[str, object]] | None:
    if not specs:
        return None
    return [kernel_spec_row(spec) for spec in specs]


def _kernels_from_row(
    payload: Sequence[Mapping[str, Any]] | None,
) -> tuple[KernelSpecT, ...]:
    if not payload:
        return ()
    return tuple(_kernel_from_row(spec) for spec in payload)


def _payload_from_row(domain: RuleDomain, row: Mapping[str, Any]) -> RulePayload | None:
    if domain == "cpg":
        return _relationship_payload_from_row(row.get("relationship_payload"))
    if domain == "normalize":
        normalize_payload = _normalize_payload_from_row(row.get("normalize_payload"))
        ops = row.get("pipeline_ops")
        if normalize_payload is None and ops:
            normalize_payload = NormalizePayload(query=query_from_ops(ops))
        return normalize_payload
    if domain == "extract":
        return _extract_payload_from_row(row.get("extract_payload"))
    return None


def _relationship_payload_from_row(payload: Mapping[str, Any] | None) -> RelationshipPayload | None:
    if payload is None:
        return None
    predicate_expr = payload.get("predicate_expr")
    predicate = ExprIR.from_json(str(predicate_expr)) if predicate_expr else None
    return RelationshipPayload(
        output_dataset=payload.get("output_dataset"),
        contract_name=payload.get("contract_name"),
        hash_join=_hash_join_from_row(payload.get("hash_join")),
        interval_align=_interval_align_from_row(payload.get("interval_align")),
        winner_select=_winner_select_from_row(payload.get("winner_select")),
        predicate=predicate,
        project=_project_from_payload(payload.get("project")),
        rule_name_col=str(payload.get("rule_name_col", "rule_name")),
        rule_priority_col=str(payload.get("rule_priority_col", "rule_priority")),
        edge_emit=_edge_emit_from_row(payload.get("edge_emit"), payload.get("edge_option_flag")),
    )


def _normalize_payload_from_row(payload: Mapping[str, Any] | None) -> NormalizePayload | None:
    if payload is None:
        return None
    return NormalizePayload(plan_builder=payload.get("plan_builder"))


def _extract_payload_from_row(payload: Mapping[str, Any] | None) -> ExtractPayload | None:
    if payload is None:
        return None
    return ExtractPayload(
        version=int(payload.get("version", 1)),
        template=payload.get("template"),
        bundles=parse_string_tuple(payload.get("bundles"), label="bundles"),
        fields=parse_string_tuple(payload.get("fields"), label="fields"),
        derived_ids=_derived_specs(payload.get("derived")),
        row_fields=parse_string_tuple(payload.get("row_fields"), label="row_fields"),
        row_extras=parse_string_tuple(payload.get("row_extras"), label="row_extras"),
        ordering_keys=_ordering_specs(payload.get("ordering_keys")),
        join_keys=parse_string_tuple(payload.get("join_keys"), label="join_keys"),
        enabled_when=payload.get("enabled_when"),
        feature_flag=payload.get("feature_flag"),
        postprocess=payload.get("postprocess"),
        metadata_extra=_metadata_bytes(payload.get("metadata_extra")),
        evidence_required_columns=parse_string_tuple(
            payload.get("evidence_required_columns"),
            label="evidence_required_columns",
        ),
        pipeline_name=payload.get("pipeline_name"),
    )


def _stages_from_payload(domain: str, payload: object | None) -> tuple[RuleStage, ...]:
    if domain != "extract" or not isinstance(payload, ExtractPayload):
        return ()
    enabled_when = _stage_enabled_when(payload)
    stages = [RuleStage(name="source", mode="source", enabled_when=enabled_when)]
    if payload.postprocess:
        stages.append(RuleStage(name=payload.postprocess, mode="post_kernel"))
    return tuple(stages)


def _stage_enabled_when(payload: ExtractPayload) -> str | None:
    if payload.enabled_when is None:
        return None
    if payload.enabled_when == "feature_flag" and payload.feature_flag:
        return f"feature_flag:{payload.feature_flag}"
    return payload.enabled_when


def _edge_emit_row(payload: EdgeEmitPayload | None) -> dict[str, object] | None:
    if payload is None:
        return None
    return {
        "edge_kind": payload.edge_kind,
        "src_cols": list(payload.src_cols),
        "dst_cols": list(payload.dst_cols),
        "path_cols": list(payload.path_cols),
        "bstart_cols": list(payload.bstart_cols),
        "bend_cols": list(payload.bend_cols),
        "origin": payload.origin,
        "default_resolution_method": payload.resolution_method,
    }


def _edge_emit_from_row(
    payload: Mapping[str, Any] | None,
    option_flag: object | None,
) -> EdgeEmitPayload | None:
    if payload is None:
        return None
    return EdgeEmitPayload(
        edge_kind=str(payload.get("edge_kind")),
        src_cols=parse_string_tuple(payload.get("src_cols"), label="src_cols"),
        dst_cols=parse_string_tuple(payload.get("dst_cols"), label="dst_cols"),
        origin=str(payload.get("origin")),
        resolution_method=str(payload.get("default_resolution_method")),
        option_flag=str(option_flag or ""),
        path_cols=parse_string_tuple(payload.get("path_cols"), label="path_cols"),
        bstart_cols=parse_string_tuple(payload.get("bstart_cols"), label="bstart_cols"),
        bend_cols=parse_string_tuple(payload.get("bend_cols"), label="bend_cols"),
    )


@dataclass
class _QueryOpState:
    base: tuple[str, ...] = ()
    derived: dict[str, ExprSpec] = field(default_factory=dict)
    predicate: ExprSpec | None = None
    pushdown: ExprSpec | None = None


def _handle_project(row: Mapping[str, Any], state: _QueryOpState) -> None:
    columns = parse_string_tuple(row.get("columns"), label="columns")
    if columns:
        state.base = columns


def _handle_derive(row: Mapping[str, Any], state: _QueryOpState) -> None:
    name = row.get("name")
    expr_json = row.get("expr_json")
    if name is None or expr_json is None:
        msg = "Query derive op requires name and expr_json."
        raise ValueError(msg)
    state.derived[str(name)] = expr_spec_from_json(str(expr_json))


def _handle_filter(row: Mapping[str, Any], state: _QueryOpState) -> None:
    expr_json = row.get("expr_json")
    if expr_json is None:
        msg = "Query filter op requires expr_json."
        raise ValueError(msg)
    state.predicate = expr_spec_from_json(str(expr_json))


def _handle_pushdown_filter(row: Mapping[str, Any], state: _QueryOpState) -> None:
    expr_json = row.get("expr_json")
    if expr_json is None:
        msg = "Query pushdown_filter op requires expr_json."
        raise ValueError(msg)
    state.pushdown = expr_spec_from_json(str(expr_json))


_QUERY_OP_HANDLERS: dict[str, Callable[[Mapping[str, Any], _QueryOpState], None]] = {
    "project": _handle_project,
    "derive": _handle_derive,
    "filter": _handle_filter,
    "pushdown_filter": _handle_pushdown_filter,
}


def query_from_ops(payload: Sequence[Mapping[str, Any]] | None) -> QuerySpec | None:
    """Decode a query spec from pipeline ops.

    Returns
    -------
    QuerySpec | None
        Query spec constructed from ops.

    Raises
    ------
    ValueError
        Raised when an unsupported op kind is encountered.
    """
    if not payload:
        return None
    state = _QueryOpState()
    for row in payload:
        kind = str(row.get("kind", ""))
        handler = _QUERY_OP_HANDLERS.get(kind)
        if handler is None:
            msg = f"Unsupported query op kind: {kind!r}"
            raise ValueError(msg)
        handler(row, state)
    if not state.base:
        state.base = tuple(state.derived)
    return QuerySpec(
        projection=ProjectionSpec(base=state.base, derived=state.derived),
        predicate=state.predicate,
        pushdown_predicate=state.pushdown,
    )


def _project_from_payload(payload: Mapping[str, Any] | None) -> ProjectConfig | None:
    if payload is None:
        return None
    exprs = {
        str(item["name"]): ExprIR.from_json(str(item["expr_json"]))
        for item in payload.get("exprs") or ()
    }
    return ProjectConfig(select=tuple(payload.get("select") or ()), exprs=exprs)


def _hash_join_from_row(payload: Mapping[str, Any] | None) -> HashJoinConfig | None:
    if payload is None:
        return None
    return HashJoinConfig(
        join_type=_parse_join_type(payload.get("join_type")),
        left_keys=parse_string_tuple(payload.get("left_keys"), label="left_keys"),
        right_keys=parse_string_tuple(payload.get("right_keys"), label="right_keys"),
        left_output=parse_string_tuple(payload.get("left_output"), label="left_output"),
        right_output=parse_string_tuple(payload.get("right_output"), label="right_output"),
        output_suffix_for_left=str(payload.get("output_suffix_for_left", "")),
        output_suffix_for_right=str(payload.get("output_suffix_for_right", "")),
    )


def _interval_align_from_row(payload: Mapping[str, Any] | None) -> IntervalAlignConfig | None:
    if payload is None:
        return None
    tie_breakers_payload = parse_mapping_sequence(payload.get("tie_breakers"), label="tie_breakers")
    return IntervalAlignConfig(
        mode=_parse_interval_mode(payload.get("mode")),
        how=_parse_interval_how(payload.get("how")),
        left_path_col=str(payload.get("left_path_col", "path")),
        left_start_col=str(payload.get("left_start_col", "bstart")),
        left_end_col=str(payload.get("left_end_col", "bend")),
        right_path_col=str(payload.get("right_path_col", "path")),
        right_start_col=str(payload.get("right_start_col", "bstart")),
        right_end_col=str(payload.get("right_end_col", "bend")),
        select_left=parse_string_tuple(payload.get("select_left"), label="select_left"),
        select_right=parse_string_tuple(payload.get("select_right"), label="select_right"),
        tie_breakers=tuple(
            SortKey(column=str(row["column"]), order=parse_sort_order(row.get("order")))
            for row in tie_breakers_payload
        ),
        emit_match_meta=bool(payload.get("emit_match_meta", True)),
        match_kind_col=str(payload.get("match_kind_col", "match_kind")),
        match_score_col=str(payload.get("match_score_col", "match_score")),
    )


def _winner_select_from_row(payload: Mapping[str, Any] | None) -> WinnerSelectConfig | None:
    if payload is None:
        return None
    tie_breakers_payload = parse_mapping_sequence(payload.get("tie_breakers"), label="tie_breakers")
    return WinnerSelectConfig(
        keys=parse_string_tuple(payload.get("keys"), label="keys"),
        score_col=str(payload.get("score_col", "score")),
        score_order=_parse_score_order(payload.get("score_order")),
        tie_breakers=tuple(
            SortKey(column=str(row["column"]), order=parse_sort_order(row.get("order")))
            for row in tie_breakers_payload
        ),
    )


def _kernel_from_row(payload: Mapping[str, Any]) -> KernelSpecT:
    kind = str(payload["kind"])
    if kind == "add_literal":
        spec = payload.get("add_literal") or {}
        result: KernelSpecT = AddLiteralSpec(
            name=str(spec.get("name", "")),
            value=decode_scalar_union(spec.get("value_union")),
        )
    elif kind == "filter":
        spec = payload.get("filter") or {}
        expr_json = str(spec.get("expr_json", ""))
        result = FilterKernelSpec(predicate=ExprIR.from_json(expr_json))
    elif kind == "drop_columns":
        spec = payload.get("drop_columns") or {}
        result = DropColumnsSpec(columns=parse_string_tuple(spec.get("columns"), label="columns"))
    elif kind == "rename_columns":
        spec = payload.get("rename_columns") or {}
        mapping = spec.get("mapping") or {}
        result = RenameColumnsSpec(mapping=dict(mapping))
    elif kind == "explode_list":
        spec = payload.get("explode_list") or {}
        result = ExplodeListSpec(
            parent_id_col=str(spec.get("parent_id_col", "src_id")),
            list_col=str(spec.get("list_col", "dst_ids")),
            out_parent_col=str(spec.get("out_parent_col", "src_id")),
            out_value_col=str(spec.get("out_value_col", "dst_id")),
        )
    elif kind == "dedupe":
        spec = payload.get("dedupe") or {}
        tie_breakers = _decode_sort_keys(spec.get("tie_breakers"))
        result = DedupeKernelSpec(
            spec=DedupeSpec(
                keys=parse_string_tuple(spec.get("keys"), label="keys"),
                tie_breakers=tie_breakers,
                strategy=parse_dedupe_strategy(spec.get("strategy", "KEEP_FIRST")),
            )
        )
    elif kind == "canonical_sort":
        spec = payload.get("canonical_sort") or {}
        result = CanonicalSortKernelSpec(sort_keys=_decode_sort_keys(spec.get("sort_keys")))
    else:
        msg = f"Unsupported kernel kind: {kind!r}"
        raise ValueError(msg)
    return result


def _decode_sort_keys(payload: object | None) -> tuple[SortKey, ...]:
    rows = parse_mapping_sequence(payload, label="sort_keys")
    return tuple(
        SortKey(column=str(row["column"]), order=parse_sort_order(row.get("order"))) for row in rows
    )


def _parse_domain(value: object) -> RuleDomain:
    domain_map: dict[str, RuleDomain] = {
        "cpg": "cpg",
        "normalize": "normalize",
        "extract": "extract",
    }
    normalized = str(value)
    if normalized in domain_map:
        return domain_map[normalized]
    msg = f"Unsupported rule domain: {value!r}"
    raise ValueError(msg)


def _parse_execution_mode(value: object | None) -> ExecutionMode:
    mode_map: dict[str, ExecutionMode] = {
        "auto": "auto",
        "plan": "plan",
        "table": "table",
        "external": "external",
        "hybrid": "hybrid",
    }
    normalized = "auto" if value is None else str(value)
    if normalized in mode_map:
        return mode_map[normalized]
    msg = f"Unsupported execution mode: {normalized!r}"
    raise ValueError(msg)


def _parse_join_type(value: object | None) -> JoinType:
    join_map: dict[str, JoinType] = {
        "inner": "inner",
        "left outer": "left outer",
        "right outer": "right outer",
        "full outer": "full outer",
        "left semi": "left semi",
        "right semi": "right semi",
        "left anti": "left anti",
        "right anti": "right anti",
    }
    normalized = "inner" if value is None else str(value)
    if normalized in join_map:
        return join_map[normalized]
    msg = f"Unsupported join type: {normalized!r}"
    raise ValueError(msg)


def _parse_interval_mode(value: object | None) -> IntervalMode:
    mode_map: dict[str, IntervalMode] = {
        "EXACT": "EXACT",
        "CONTAINED_BEST": "CONTAINED_BEST",
        "OVERLAP_BEST": "OVERLAP_BEST",
    }
    normalized = "CONTAINED_BEST" if value is None else str(value)
    if normalized in mode_map:
        return mode_map[normalized]
    msg = f"Unsupported interval align mode: {normalized!r}"
    raise ValueError(msg)


def _parse_interval_how(value: object | None) -> IntervalHow:
    how_map: dict[str, IntervalHow] = {
        "inner": "inner",
        "left": "left",
    }
    normalized = "inner" if value is None else str(value)
    if normalized in how_map:
        return how_map[normalized]
    msg = f"Unsupported interval align how: {normalized!r}"
    raise ValueError(msg)


def _parse_score_order(value: object | None) -> ScoreOrder:
    order_map: dict[str, ScoreOrder] = {
        "ascending": "ascending",
        "descending": "descending",
    }
    normalized = "descending" if value is None else str(value)
    if normalized in order_map:
        return order_map[normalized]
    msg = f"Unsupported score order: {normalized!r}"
    raise ValueError(msg)


def _derived_rows(values: Sequence[ExtractDerivedIdSpec]) -> list[dict[str, object]] | None:
    if not values:
        return None
    return [
        {
            "name": spec.name,
            "spec": spec.spec,
            "kind": spec.kind,
            "required": list(spec.required) or None,
        }
        for spec in values
    ]


def _derived_specs(value: object | None) -> tuple[ExtractDerivedIdSpec, ...]:
    if not value:
        return ()
    items: list[ExtractDerivedIdSpec] = []
    if not isinstance(value, Sequence):
        msg = "derived specs must be a sequence of mappings."
        raise TypeError(msg)
    for item in value:
        if not isinstance(item, Mapping):
            msg = "derived spec entries must be mappings."
            raise TypeError(msg)
        items.append(
            ExtractDerivedIdSpec(
                name=str(item.get("name")),
                spec=str(item.get("spec")),
                kind=str(item.get("kind", "masked_hash")),
                required=parse_string_tuple(item.get("required"), label="required"),
            )
        )
    return tuple(items)


def _ordering_rows(values: Sequence[OrderingKey]) -> list[dict[str, object]] | None:
    if not values:
        return None
    return [{"column": col, "order": order} for col, order in values]


def _ordering_specs(value: object | None) -> tuple[OrderingKey, ...]:
    if not value:
        return ()
    if not isinstance(value, Sequence):
        msg = "ordering_keys must be a sequence of mappings."
        raise TypeError(msg)
    items: list[ExtractOrderingKeySpec] = []
    for item in value:
        if not isinstance(item, Mapping):
            msg = "ordering key entries must be mappings."
            raise TypeError(msg)
        items.append(
            ExtractOrderingKeySpec(
                column=str(item.get("column")),
                order=str(item.get("order", "ascending")),
            )
        )
    return tuple((item.column, item.order) for item in items)


def _metadata_bytes(metadata: object | None) -> dict[bytes, bytes]:
    if metadata is None:
        return {}
    if not isinstance(metadata, Mapping):
        msg = "metadata_extra must be a mapping of strings."
        raise TypeError(msg)
    return {str(key).encode("utf-8"): str(value).encode("utf-8") for key, value in metadata.items()}


__all__ = [
    "EXTRACT_STRUCT",
    "NORMALIZE_STRUCT",
    "RELATIONSHIP_STRUCT",
    "RULE_DEF_SCHEMA",
    "query_from_ops",
    "rule_definition_table",
    "rule_definitions_from_table",
]
