"""Canonical spec tables for centralized rule definitions."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Literal, cast

import pyarrow as pa

from arrowdsl.compute.expr_core import ScalarValue
from arrowdsl.core.context import OrderingKey
from arrowdsl.plan.ops import JoinType, SortKey
from arrowdsl.schema.build import list_view_type
from arrowdsl.schema.schema import EncodingPolicy, EncodingSpec
from arrowdsl.spec.codec import (
    decode_scalar_union,
    encode_scalar_union,
    parse_dedupe_strategy,
    parse_mapping_sequence,
    parse_sort_order,
    parse_string_tuple,
)
from arrowdsl.spec.expr_ir import ExprIR
from arrowdsl.spec.infra import (
    DATASET_REF_STRUCT,
    SCALAR_UNION_TYPE,
    SORT_KEY_STRUCT,
)
from arrowdsl.spec.io import table_from_rows
from cpg.spec_tables import EDGE_EMIT_STRUCT
from extract.spec_tables import (
    DERIVED_ID_STRUCT,
    ORDERING_KEY_STRUCT,
    ExtractDerivedIdSpec,
    ExtractOrderingKeySpec,
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
from relspec.rules.rel_ops import (
    query_spec_from_rel_ops,
    rel_ops_from_rows,
    rel_ops_to_rows,
)

IntervalMode = Literal["EXACT", "CONTAINED_BEST", "OVERLAP_BEST"]
IntervalHow = Literal["inner", "left"]
ScoreOrder = Literal["ascending", "descending"]
_UNION_TAG_KEY = "__union_tag__"
_UNION_VALUE_KEY = "value"

HASH_JOIN_STRUCT = pa.struct(
    [
        pa.field("join_type", pa.string(), nullable=False),
        pa.field("left_keys", list_view_type(pa.string()), nullable=False),
        pa.field("right_keys", list_view_type(pa.string()), nullable=True),
        pa.field("left_output", list_view_type(pa.string()), nullable=True),
        pa.field("right_output", list_view_type(pa.string()), nullable=True),
        pa.field("output_suffix_for_left", pa.string(), nullable=False),
        pa.field("output_suffix_for_right", pa.string(), nullable=False),
    ]
)

INTERVAL_ALIGN_STRUCT = pa.struct(
    [
        pa.field("mode", pa.string(), nullable=False),
        pa.field("how", pa.string(), nullable=False),
        pa.field("left_path_col", pa.string(), nullable=False),
        pa.field("left_start_col", pa.string(), nullable=False),
        pa.field("left_end_col", pa.string(), nullable=False),
        pa.field("right_path_col", pa.string(), nullable=False),
        pa.field("right_start_col", pa.string(), nullable=False),
        pa.field("right_end_col", pa.string(), nullable=False),
        pa.field("select_left", list_view_type(pa.string()), nullable=True),
        pa.field("select_right", list_view_type(pa.string()), nullable=True),
        pa.field("tie_breakers", list_view_type(SORT_KEY_STRUCT), nullable=True),
        pa.field("emit_match_meta", pa.bool_(), nullable=False),
        pa.field("match_kind_col", pa.string(), nullable=False),
        pa.field("match_score_col", pa.string(), nullable=False),
    ]
)

WINNER_SELECT_STRUCT = pa.struct(
    [
        pa.field("keys", list_view_type(pa.string()), nullable=True),
        pa.field("score_col", pa.string(), nullable=False),
        pa.field("score_order", pa.string(), nullable=False),
        pa.field("tie_breakers", list_view_type(SORT_KEY_STRUCT), nullable=True),
    ]
)

PROJECT_EXPR_STRUCT = pa.struct(
    [
        pa.field("name", pa.string(), nullable=False),
        pa.field("expr_json", pa.string(), nullable=False),
    ]
)

PROJECT_STRUCT = pa.struct(
    [
        pa.field("select", list_view_type(pa.string()), nullable=True),
        pa.field("exprs", list_view_type(PROJECT_EXPR_STRUCT), nullable=True),
    ]
)

CONFIDENCE_STRUCT = pa.struct(
    [
        pa.field("base", pa.float64(), nullable=False),
        pa.field("source_weight", pa.map_(pa.string(), pa.float64()), nullable=True),
        pa.field("penalty", pa.float64(), nullable=False),
    ]
)

AMBIGUITY_STRUCT = pa.struct(
    [
        pa.field("winner_select", WINNER_SELECT_STRUCT, nullable=True),
        pa.field("tie_breakers", list_view_type(SORT_KEY_STRUCT), nullable=True),
    ]
)

KERNEL_ADD_LITERAL_STRUCT = pa.struct(
    [
        pa.field("name", pa.string(), nullable=False),
        pa.field("value_union", SCALAR_UNION_TYPE, nullable=True),
    ]
)

KERNEL_FILTER_STRUCT = pa.struct([pa.field("expr_json", pa.string(), nullable=False)])

KERNEL_DROP_COLUMNS_STRUCT = pa.struct(
    [pa.field("columns", list_view_type(pa.string()), nullable=True)]
)

KERNEL_RENAME_COLUMNS_STRUCT = pa.struct(
    [pa.field("mapping", pa.map_(pa.string(), pa.string()), nullable=True)]
)

KERNEL_EXPLODE_LIST_STRUCT = pa.struct(
    [
        pa.field("parent_id_col", pa.string(), nullable=False),
        pa.field("list_col", pa.string(), nullable=False),
        pa.field("out_parent_col", pa.string(), nullable=False),
        pa.field("out_value_col", pa.string(), nullable=False),
    ]
)

KERNEL_DEDUPE_STRUCT = pa.struct(
    [
        pa.field("keys", list_view_type(pa.string()), nullable=False),
        pa.field("tie_breakers", list_view_type(SORT_KEY_STRUCT), nullable=True),
        pa.field("strategy", pa.string(), nullable=False),
    ]
)

KERNEL_CANONICAL_SORT_STRUCT = pa.struct(
    [pa.field("sort_keys", list_view_type(SORT_KEY_STRUCT), nullable=True)]
)

KERNEL_PAYLOAD_UNION = pa.union(
    [
        pa.field("add_literal", KERNEL_ADD_LITERAL_STRUCT),
        pa.field("filter", KERNEL_FILTER_STRUCT),
        pa.field("drop_columns", KERNEL_DROP_COLUMNS_STRUCT),
        pa.field("rename_columns", KERNEL_RENAME_COLUMNS_STRUCT),
        pa.field("explode_list", KERNEL_EXPLODE_LIST_STRUCT),
        pa.field("dedupe", KERNEL_DEDUPE_STRUCT),
        pa.field("canonical_sort", KERNEL_CANONICAL_SORT_STRUCT),
    ],
    mode="dense",
)

KERNEL_SPEC_STRUCT = pa.struct(
    [
        pa.field("kind", pa.string(), nullable=False),
        pa.field("payload", KERNEL_PAYLOAD_UNION, nullable=True),
    ]
)


EVIDENCE_STRUCT = pa.struct(
    [
        pa.field("sources", list_view_type(pa.string()), nullable=True),
        pa.field("required_columns", list_view_type(pa.string()), nullable=True),
        pa.field("required_types", pa.map_(pa.string(), pa.string()), nullable=True),
        pa.field("required_metadata", pa.map_(pa.string(), pa.string()), nullable=True),
    ]
)

EVIDENCE_OUTPUT_STRUCT = pa.struct(
    [
        pa.field("column_map", pa.map_(pa.string(), pa.string()), nullable=True),
        pa.field("provenance_columns", list_view_type(pa.string()), nullable=True),
        pa.field(
            "literals",
            list_view_type(
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

RULES_SCHEMA = pa.schema(
    [
        pa.field("name", pa.string(), nullable=False),
        pa.field("kind", pa.string(), nullable=False),
        pa.field("output_dataset", pa.string(), nullable=False),
        pa.field("contract_name", pa.string(), nullable=True),
        pa.field("inputs", list_view_type(DATASET_REF_STRUCT), nullable=False),
        pa.field("hash_join", HASH_JOIN_STRUCT, nullable=True),
        pa.field("interval_align", INTERVAL_ALIGN_STRUCT, nullable=True),
        pa.field("winner_select", WINNER_SELECT_STRUCT, nullable=True),
        pa.field("project", PROJECT_STRUCT, nullable=True),
        pa.field("post_kernels", list_view_type(KERNEL_SPEC_STRUCT), nullable=True),
        pa.field("evidence", EVIDENCE_STRUCT, nullable=True),
        pa.field("confidence_policy", CONFIDENCE_STRUCT, nullable=True),
        pa.field("ambiguity_policy", AMBIGUITY_STRUCT, nullable=True),
        pa.field("priority", pa.int32(), nullable=False),
        pa.field("emit_rule_meta", pa.bool_(), nullable=False),
        pa.field("rule_name_col", pa.string(), nullable=False),
        pa.field("rule_priority_col", pa.string(), nullable=False),
        pa.field("execution_mode", pa.string(), nullable=False),
    ],
    metadata={b"spec_kind": b"relationship_rules"},
)

POLICY_OVERRIDE_STRUCT = pa.struct(
    [
        pa.field("confidence_policy", pa.string(), nullable=True),
        pa.field("ambiguity_policy", pa.string(), nullable=True),
    ]
)

REL_OP_STRUCT = pa.struct(
    [
        pa.field("kind", pa.string(), nullable=False),
        pa.field("columns", list_view_type(pa.string()), nullable=True),
        pa.field("name", pa.string(), nullable=True),
        pa.field("expr_json", pa.string(), nullable=True),
        pa.field("payload_json", pa.string(), nullable=True),
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
        pa.field("bundles", list_view_type(pa.string()), nullable=True),
        pa.field("fields", list_view_type(pa.string()), nullable=True),
        pa.field("derived", list_view_type(DERIVED_ID_STRUCT), nullable=True),
        pa.field("row_fields", list_view_type(pa.string()), nullable=True),
        pa.field("row_extras", list_view_type(pa.string()), nullable=True),
        pa.field("ordering_keys", list_view_type(ORDERING_KEY_STRUCT), nullable=True),
        pa.field("join_keys", list_view_type(pa.string()), nullable=True),
        pa.field("enabled_when", pa.string(), nullable=True),
        pa.field("feature_flag", pa.string(), nullable=True),
        pa.field("postprocess", pa.string(), nullable=True),
        pa.field("metadata_extra", pa.map_(pa.string(), pa.string()), nullable=True),
        pa.field("evidence_required_columns", list_view_type(pa.string()), nullable=True),
        pa.field("pipeline_name", pa.string(), nullable=True),
    ]
)

RULE_DEF_SCHEMA = pa.schema(
    [
        pa.field("name", pa.string(), nullable=False),
        pa.field("domain", pa.string(), nullable=False),
        pa.field("kind", pa.string(), nullable=False),
        pa.field("inputs", list_view_type(pa.string()), nullable=True),
        pa.field("output", pa.string(), nullable=False),
        pa.field("execution_mode", pa.string(), nullable=True),
        pa.field("priority", pa.int32(), nullable=True),
        pa.field("evidence", EVIDENCE_STRUCT, nullable=True),
        pa.field("evidence_output", EVIDENCE_OUTPUT_STRUCT, nullable=True),
        pa.field("policy_overrides", POLICY_OVERRIDE_STRUCT, nullable=True),
        pa.field("relationship_payload", RELATIONSHIP_STRUCT, nullable=True),
        pa.field("normalize_payload", NORMALIZE_STRUCT, nullable=True),
        pa.field("extract_payload", EXTRACT_STRUCT, nullable=True),
        pa.field("rel_ops", list_view_type(REL_OP_STRUCT), nullable=True),
        pa.field("post_kernels", list_view_type(KERNEL_SPEC_STRUCT), nullable=True),
    ],
    metadata={b"spec_kind": b"relspec_rule_definitions"},
)

RULE_DEF_ENCODING_POLICY = EncodingPolicy(
    specs=(
        EncodingSpec(column="domain"),
        EncodingSpec(column="kind"),
        EncodingSpec(column="output"),
        EncodingSpec(column="execution_mode"),
    )
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
            "rel_ops": rel_ops_to_rows(definition.rel_ops),
            "post_kernels": _kernel_rows(definition.post_kernels),
        }
        for definition in definitions
    ]
    table = table_from_rows(RULE_DEF_SCHEMA, rows)
    return RULE_DEF_ENCODING_POLICY.apply(table)


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
        rel_ops = rel_ops_from_rows(row.get("rel_ops"))
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
                rel_ops=rel_ops,
                post_kernels=post_kernels,
                stages=_stages_from_payload(domain, payload),
                payload=payload,
            )
        )
    return tuple(definitions)


def _evidence_row(spec: EvidenceSpec | None) -> dict[str, object] | None:
    """Serialize evidence spec to a row payload.

    Parameters
    ----------
    spec
        Evidence spec to serialize.

    Returns
    -------
    dict[str, object] | None
        Row payload for the evidence spec.
    """
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
    """Deserialize evidence spec from a row payload.

    Parameters
    ----------
    payload
        Row payload to deserialize.

    Returns
    -------
    EvidenceSpec | None
        Evidence spec parsed from the payload.
    """
    if payload is None:
        return None
    required_types = payload.get("required_types")
    required_types_map: dict[str, Any] = (
        dict(required_types) if isinstance(required_types, Mapping) else {}
    )
    required_metadata = payload.get("required_metadata")
    metadata_map: dict[str, Any] = (
        dict(required_metadata) if isinstance(required_metadata, Mapping) else {}
    )
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
    """Serialize evidence output settings to a row payload.

    Parameters
    ----------
    spec
        Evidence output settings.

    Returns
    -------
    dict[str, object] | None
        Row payload for evidence output settings.
    """
    if spec is None:
        return None
    literals = [
        {"name": name, "value_union": encode_scalar_union(value)}
        for name, value in spec.literals.items()
    ]
    return {
        "column_map": dict(spec.column_map) or None,
        "provenance_columns": list(spec.provenance_columns) or None,
        "literals": literals or None,
    }


def _evidence_output_from_row(payload: Mapping[str, Any] | None) -> EvidenceOutput | None:
    """Deserialize evidence output settings from a row payload.

    Parameters
    ----------
    payload
        Row payload to deserialize.

    Returns
    -------
    EvidenceOutput | None
        Evidence output settings parsed from the payload.
    """
    if payload is None:
        return None
    raw_map = payload.get("column_map")
    column_map: dict[str, str] = (
        {str(key): str(val) for key, val in raw_map.items()} if isinstance(raw_map, Mapping) else {}
    )
    provenance_columns = parse_string_tuple(
        payload.get("provenance_columns"),
        label="provenance_columns",
    )
    literals: dict[str, ScalarValue] = {}
    for item in payload.get("literals") or ():
        name = item.get("name")
        if name is None:
            continue
        literals[str(name)] = decode_scalar_union(item.get("value_union"))
    if not column_map and not literals and not provenance_columns:
        return None
    return EvidenceOutput(
        column_map=column_map,
        literals=literals,
        provenance_columns=provenance_columns,
    )


def _policy_overrides_row(overrides: PolicyOverrides) -> dict[str, object] | None:
    """Serialize policy overrides to a row payload.

    Parameters
    ----------
    overrides
        Policy overrides to serialize.

    Returns
    -------
    dict[str, object] | None
        Row payload for policy overrides.
    """
    if overrides.confidence_policy is None and overrides.ambiguity_policy is None:
        return None
    return {
        "confidence_policy": overrides.confidence_policy,
        "ambiguity_policy": overrides.ambiguity_policy,
    }


def _policy_overrides_from_row(payload: Mapping[str, Any] | None) -> PolicyOverrides:
    """Deserialize policy overrides from a row payload.

    Parameters
    ----------
    payload
        Row payload to deserialize.

    Returns
    -------
    PolicyOverrides
        Policy overrides parsed from the payload.
    """
    if payload is None:
        return PolicyOverrides()
    return PolicyOverrides(
        confidence_policy=payload.get("confidence_policy"),
        ambiguity_policy=payload.get("ambiguity_policy"),
    )


def _encode_expr(expr: ExprIR) -> str:
    """Serialize an expression to JSON text.

    Parameters
    ----------
    expr
        Expression to serialize.

    Returns
    -------
    str
        JSON representation of the expression.
    """
    return expr.to_json()


def _encode_sort_key(column: str, order: str) -> dict[str, object]:
    """Encode a sort key to a row payload.

    Parameters
    ----------
    column
        Column name for the sort key.
    order
        Sort order string.

    Returns
    -------
    dict[str, object]
        Sort key payload.
    """
    return {"column": column, "order": order}


def _encode_sort_keys(keys: Sequence[SortKey]) -> list[dict[str, object]]:
    """Encode sort keys to a list of row payloads.

    Parameters
    ----------
    keys
        Sort keys to encode.

    Returns
    -------
    list[dict[str, object]]
        Encoded sort key payloads.
    """
    return [_encode_sort_key(key.column, key.order) for key in keys]


def _encode_literal(value: object | None) -> ScalarValue | None:
    """Encode a literal value to a scalar union.

    Parameters
    ----------
    value
        Value to encode.

    Returns
    -------
    ScalarValue | None
        Encoded scalar union value.
    """
    return encode_scalar_union(cast("ScalarValue | None", value))


def _tagged_union_payload(tag: str, payload: Mapping[str, object] | None) -> dict[str, object]:
    """Wrap a payload in a tagged union envelope.

    Parameters
    ----------
    tag
        Union tag key.
    payload
        Payload to wrap.

    Returns
    -------
    dict[str, object]
        Tagged union payload.
    """
    return {_UNION_TAG_KEY: tag, _UNION_VALUE_KEY: payload}


def hash_join_row(config: HashJoinConfig | None) -> dict[str, object] | None:
    if config is None:
        return None

    def _output(value: tuple[str, ...] | None) -> list[str] | None:
        if value is None:
            return None
        return list(value)

    return {
        "join_type": config.join_type,
        "left_keys": list(config.left_keys),
        "right_keys": list(config.right_keys) or None,
        "left_output": _output(config.left_output),
        "right_output": _output(config.right_output),
        "output_suffix_for_left": config.output_suffix_for_left,
        "output_suffix_for_right": config.output_suffix_for_right,
    }


def interval_align_row(config: IntervalAlignConfig | None) -> dict[str, object] | None:
    if config is None:
        return None
    return {
        "mode": config.mode,
        "how": config.how,
        "left_path_col": config.left_path_col,
        "left_start_col": config.left_start_col,
        "left_end_col": config.left_end_col,
        "right_path_col": config.right_path_col,
        "right_start_col": config.right_start_col,
        "right_end_col": config.right_end_col,
        "select_left": list(config.select_left) or None,
        "select_right": list(config.select_right) or None,
        "tie_breakers": _encode_sort_keys(config.tie_breakers) or None,
        "emit_match_meta": config.emit_match_meta,
        "match_kind_col": config.match_kind_col,
        "match_score_col": config.match_score_col,
    }


def winner_select_row(config: WinnerSelectConfig | None) -> dict[str, object] | None:
    if config is None:
        return None
    return {
        "keys": list(config.keys) or None,
        "score_col": config.score_col,
        "score_order": config.score_order,
        "tie_breakers": _encode_sort_keys(config.tie_breakers) or None,
    }


def project_row(config: ProjectConfig | None) -> dict[str, object] | None:
    if config is None:
        return None
    expr_rows = [
        {"name": name, "expr_json": _encode_expr(expr)} for name, expr in config.exprs.items()
    ]
    return {
        "select": list(config.select) or None,
        "exprs": expr_rows or None,
    }


def kernel_spec_row(spec: KernelSpecT) -> dict[str, object]:
    kind = spec.kind
    base: dict[str, object] = {"kind": kind}
    if isinstance(spec, AddLiteralSpec):
        payload = {"name": spec.name, "value_union": _encode_literal(spec.value)}
    elif isinstance(spec, FilterKernelSpec):
        payload = {"expr_json": _encode_expr(spec.predicate)}
    elif isinstance(spec, DropColumnsSpec):
        payload = {"columns": list(spec.columns) or None}
    elif isinstance(spec, RenameColumnsSpec):
        payload = {"mapping": dict(spec.mapping) or None}
    elif isinstance(spec, ExplodeListSpec):
        payload = {
            "parent_id_col": spec.parent_id_col,
            "list_col": spec.list_col,
            "out_parent_col": spec.out_parent_col,
            "out_value_col": spec.out_value_col,
            "idx_col": spec.idx_col,
            "keep_empty": spec.keep_empty,
        }
    elif isinstance(spec, DedupeKernelSpec):
        payload = {
            "keys": list(spec.spec.keys),
            "tie_breakers": _encode_sort_keys(spec.spec.tie_breakers) or None,
            "strategy": spec.spec.strategy,
        }
    elif isinstance(spec, CanonicalSortKernelSpec):
        payload = {"sort_keys": _encode_sort_keys(spec.sort_keys) or None}
    else:
        msg = f"Unsupported KernelSpec type: {type(spec).__name__}."
        raise TypeError(msg)
    base["payload"] = _tagged_union_payload(kind, payload)
    return base


def _relationship_payload_row(payload: object | None) -> dict[str, object] | None:
    """Serialize a relationship payload to a row payload.

    Parameters
    ----------
    payload
        Relationship payload to serialize.

    Returns
    -------
    dict[str, object] | None
        Serialized relationship payload.
    """
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
    """Serialize a normalize payload to a row payload.

    Parameters
    ----------
    payload
        Normalize payload to serialize.

    Returns
    -------
    dict[str, object] | None
        Serialized normalize payload.
    """
    if not isinstance(payload, NormalizePayload):
        return None
    return {"plan_builder": payload.plan_builder}


def _extract_payload_row(payload: object | None) -> dict[str, object] | None:
    """Serialize an extract payload to a row payload.

    Parameters
    ----------
    payload
        Extract payload to serialize.

    Returns
    -------
    dict[str, object] | None
        Serialized extract payload.
    """
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


def _kernel_rows(specs: Sequence[KernelSpecT]) -> list[dict[str, object]] | None:
    """Serialize kernel specs to a list of row payloads.

    Parameters
    ----------
    specs
        Kernel specs to serialize.

    Returns
    -------
    list[dict[str, object]] | None
        Serialized kernel rows.
    """
    if not specs:
        return None
    return [kernel_spec_row(spec) for spec in specs]


def _kernels_from_row(
    payload: Sequence[Mapping[str, Any]] | None,
) -> tuple[KernelSpecT, ...]:
    """Deserialize kernel specs from row payloads.

    Parameters
    ----------
    payload
        Kernel payload rows.

    Returns
    -------
    tuple[KernelSpecT, ...]
        Parsed kernel specs.
    """
    if not payload:
        return ()
    return tuple(_kernel_from_row(spec) for spec in payload)


def _payload_from_row(domain: RuleDomain, row: Mapping[str, Any]) -> RulePayload | None:
    """Deserialize a rule payload based on domain.

    Parameters
    ----------
    domain
        Rule domain.
    row
        Rule row payload.

    Returns
    -------
    RulePayload | None
        Parsed rule payload.
    """
    if domain == "cpg":
        return _relationship_payload_from_row(row.get("relationship_payload"))
    if domain == "normalize":
        normalize_payload = _normalize_payload_from_row(row.get("normalize_payload"))
        ops = row.get("rel_ops")
        if normalize_payload is None and ops:
            rel_ops = rel_ops_from_rows(ops)
            normalize_payload = NormalizePayload(query=query_spec_from_rel_ops(rel_ops))
        return normalize_payload
    if domain == "extract":
        return _extract_payload_from_row(row.get("extract_payload"))
    return None


def _relationship_payload_from_row(payload: Mapping[str, Any] | None) -> RelationshipPayload | None:
    """Deserialize a relationship payload from row data.

    Parameters
    ----------
    payload
        Relationship payload row data.

    Returns
    -------
    RelationshipPayload | None
        Parsed relationship payload.
    """
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
    """Deserialize a normalize payload from row data.

    Parameters
    ----------
    payload
        Normalize payload row data.

    Returns
    -------
    NormalizePayload | None
        Parsed normalize payload.
    """
    if payload is None:
        return None
    return NormalizePayload(plan_builder=payload.get("plan_builder"))


def _extract_payload_from_row(payload: Mapping[str, Any] | None) -> ExtractPayload | None:
    """Deserialize an extract payload from row data.

    Parameters
    ----------
    payload
        Extract payload row data.

    Returns
    -------
    ExtractPayload | None
        Parsed extract payload.
    """
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
    """Derive rule stages from a payload.

    Parameters
    ----------
    domain
        Rule domain.
    payload
        Rule payload to inspect.

    Returns
    -------
    tuple[RuleStage, ...]
        Derived rule stages.
    """
    if domain != "extract" or not isinstance(payload, ExtractPayload):
        return ()
    enabled_when = _stage_enabled_when(payload)
    stages = [RuleStage(name="source", mode="source", enabled_when=enabled_when)]
    if payload.postprocess:
        stages.append(RuleStage(name=payload.postprocess, mode="post_kernel"))
    return tuple(stages)


def _stage_enabled_when(payload: ExtractPayload) -> str | None:
    """Return the enabled_when expression for extract stages.

    Parameters
    ----------
    payload
        Extract payload to inspect.

    Returns
    -------
    str | None
        Enabled-when expression, if set.
    """
    if payload.enabled_when is None:
        return None
    if payload.enabled_when == "feature_flag" and payload.feature_flag:
        return f"feature_flag:{payload.feature_flag}"
    return payload.enabled_when


def _edge_emit_row(payload: EdgeEmitPayload | None) -> dict[str, object] | None:
    """Serialize edge emit payload to a row payload.

    Parameters
    ----------
    payload
        Edge emit payload to serialize.

    Returns
    -------
    dict[str, object] | None
        Serialized edge emit payload.
    """
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
    """Deserialize edge emit payload from row data.

    Parameters
    ----------
    payload
        Edge emit payload row data.
    option_flag
        Option flag value from row data.

    Returns
    -------
    EdgeEmitPayload | None
        Parsed edge emit payload.
    """
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


def _project_from_payload(payload: Mapping[str, Any] | None) -> ProjectConfig | None:
    """Deserialize a project config from payload data.

    Parameters
    ----------
    payload
        Project payload row data.

    Returns
    -------
    ProjectConfig | None
        Parsed project config.
    """
    if payload is None:
        return None
    exprs = {
        str(item["name"]): ExprIR.from_json(str(item["expr_json"]))
        for item in payload.get("exprs") or ()
    }
    return ProjectConfig(select=tuple(payload.get("select") or ()), exprs=exprs)


def _hash_join_from_row(payload: Mapping[str, Any] | None) -> HashJoinConfig | None:
    """Deserialize a hash join config from row data.

    Parameters
    ----------
    payload
        Hash join payload row data.

    Returns
    -------
    HashJoinConfig | None
        Parsed hash join config.
    """
    if payload is None:
        return None

    def _optional_string_tuple(value: object | None, *, label: str) -> tuple[str, ...] | None:
        if value is None:
            return None
        return parse_string_tuple(value, label=label)

    return HashJoinConfig(
        join_type=_parse_join_type(payload.get("join_type")),
        left_keys=parse_string_tuple(payload.get("left_keys"), label="left_keys"),
        right_keys=parse_string_tuple(payload.get("right_keys"), label="right_keys"),
        left_output=_optional_string_tuple(payload.get("left_output"), label="left_output"),
        right_output=_optional_string_tuple(payload.get("right_output"), label="right_output"),
        output_suffix_for_left=str(payload.get("output_suffix_for_left", "")),
        output_suffix_for_right=str(payload.get("output_suffix_for_right", "")),
    )


def _interval_align_from_row(payload: Mapping[str, Any] | None) -> IntervalAlignConfig | None:
    """Deserialize an interval align config from row data.

    Parameters
    ----------
    payload
        Interval align payload row data.

    Returns
    -------
    IntervalAlignConfig | None
        Parsed interval align config.
    """
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
    """Deserialize a winner select config from row data.

    Parameters
    ----------
    payload
        Winner select payload row data.

    Returns
    -------
    WinnerSelectConfig | None
        Parsed winner select config.
    """
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
    """Deserialize a kernel spec from row payload data.

    Parameters
    ----------
    payload
        Kernel payload row data.

    Returns
    -------
    KernelSpecT
        Parsed kernel spec.

    Raises
    ------
    ValueError
        Raised when the kernel kind is unsupported.
    """
    kind = str(payload["kind"])
    raw_payload = payload.get("payload")
    spec: Mapping[str, Any] = raw_payload if isinstance(raw_payload, Mapping) else {}
    if kind == "add_literal":
        result: KernelSpecT = AddLiteralSpec(
            name=str(spec.get("name", "")),
            value=decode_scalar_union(spec.get("value_union")),
        )
    elif kind == "filter":
        expr_json = str(spec.get("expr_json", ""))
        result = FilterKernelSpec(predicate=ExprIR.from_json(expr_json))
    elif kind == "drop_columns":
        result = DropColumnsSpec(columns=parse_string_tuple(spec.get("columns"), label="columns"))
    elif kind == "rename_columns":
        mapping_payload = spec.get("mapping")
        mapping: Mapping[str, Any] = mapping_payload if isinstance(mapping_payload, Mapping) else {}
        result = RenameColumnsSpec(mapping={str(key): str(val) for key, val in mapping.items()})
    elif kind == "explode_list":
        idx_payload = spec.get("idx_col")
        result = ExplodeListSpec(
            parent_id_col=str(spec.get("parent_id_col", "src_id")),
            list_col=str(spec.get("list_col", "dst_ids")),
            out_parent_col=str(spec.get("out_parent_col", "src_id")),
            out_value_col=str(spec.get("out_value_col", "dst_id")),
            idx_col=str(idx_payload) if idx_payload is not None else None,
            keep_empty=bool(spec.get("keep_empty", True)),
        )
    elif kind == "dedupe":
        tie_breakers = _decode_sort_keys(spec.get("tie_breakers"))
        result = DedupeKernelSpec(
            spec=DedupeSpec(
                keys=parse_string_tuple(spec.get("keys"), label="keys"),
                tie_breakers=tie_breakers,
                strategy=parse_dedupe_strategy(spec.get("strategy", "KEEP_FIRST")),
            )
        )
    elif kind == "canonical_sort":
        result = CanonicalSortKernelSpec(sort_keys=_decode_sort_keys(spec.get("sort_keys")))
    else:
        msg = f"Unsupported kernel kind: {kind!r}"
        raise ValueError(msg)
    return result


def _decode_sort_keys(payload: object | None) -> tuple[SortKey, ...]:
    """Decode sort keys from payload data.

    Parameters
    ----------
    payload
        Payload containing sort key rows.

    Returns
    -------
    tuple[SortKey, ...]
        Parsed sort keys.
    """
    rows = parse_mapping_sequence(payload, label="sort_keys")
    return tuple(
        SortKey(column=str(row["column"]), order=parse_sort_order(row.get("order"))) for row in rows
    )


def _parse_domain(value: object) -> RuleDomain:
    """Parse a rule domain from a payload value.

    Parameters
    ----------
    value
        Payload value to parse.

    Returns
    -------
    RuleDomain
        Parsed rule domain.

    Raises
    ------
    ValueError
        Raised when the rule domain is unsupported.
    """
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
    """Parse execution mode from a payload value.

    Parameters
    ----------
    value
        Payload value to parse.

    Returns
    -------
    ExecutionMode
        Parsed execution mode.

    Raises
    ------
    ValueError
        Raised when the execution mode is unsupported.
    """
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
    """Parse join type from a payload value.

    Parameters
    ----------
    value
        Payload value to parse.

    Returns
    -------
    JoinType
        Parsed join type.

    Raises
    ------
    ValueError
        Raised when the join type is unsupported.
    """
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
    """Parse interval mode from a payload value.

    Parameters
    ----------
    value
        Payload value to parse.

    Returns
    -------
    IntervalMode
        Parsed interval mode.

    Raises
    ------
    ValueError
        Raised when the interval mode is unsupported.
    """
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
    """Parse interval join mode from a payload value.

    Parameters
    ----------
    value
        Payload value to parse.

    Returns
    -------
    IntervalHow
        Parsed interval join mode.

    Raises
    ------
    ValueError
        Raised when the interval join mode is unsupported.
    """
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
    """Parse score order from a payload value.

    Parameters
    ----------
    value
        Payload value to parse.

    Returns
    -------
    ScoreOrder
        Parsed score order.

    Raises
    ------
    ValueError
        Raised when the score order is unsupported.
    """
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
    """Serialize derived ID specs to row payloads.

    Parameters
    ----------
    values
        Derived ID specs to serialize.

    Returns
    -------
    list[dict[str, object]] | None
        Serialized derived ID rows.
    """
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
    """Deserialize derived ID specs from payload data.

    Parameters
    ----------
    value
        Payload value to parse.

    Returns
    -------
    tuple[ExtractDerivedIdSpec, ...]
        Parsed derived ID specs.

    Raises
    ------
    TypeError
        Raised when the derived ID payload is not a sequence of mappings.
    """
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
    """Serialize ordering keys to row payloads.

    Parameters
    ----------
    values
        Ordering keys to serialize.

    Returns
    -------
    list[dict[str, object]] | None
        Serialized ordering key rows.
    """
    if not values:
        return None
    return [{"column": col, "order": order} for col, order in values]


def _ordering_specs(value: object | None) -> tuple[OrderingKey, ...]:
    """Deserialize ordering keys from payload data.

    Parameters
    ----------
    value
        Payload value to parse.

    Returns
    -------
    tuple[OrderingKey, ...]
        Parsed ordering keys.

    Raises
    ------
    TypeError
        Raised when the ordering payload is not a sequence of mappings.
    """
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
    """Convert metadata payload to bytes mapping.

    Parameters
    ----------
    metadata
        Metadata payload to convert.

    Returns
    -------
    dict[bytes, bytes]
        Metadata mapping with byte keys and values.

    Raises
    ------
    TypeError
        Raised when metadata payload is not a mapping.
    """
    if metadata is None:
        return {}
    if not isinstance(metadata, Mapping):
        msg = "metadata_extra must be a mapping of strings."
        raise TypeError(msg)
    return {str(key).encode("utf-8"): str(value).encode("utf-8") for key, value in metadata.items()}


__all__ = [
    "AMBIGUITY_STRUCT",
    "CONFIDENCE_STRUCT",
    "EVIDENCE_OUTPUT_STRUCT",
    "EVIDENCE_STRUCT",
    "EXTRACT_STRUCT",
    "HASH_JOIN_STRUCT",
    "INTERVAL_ALIGN_STRUCT",
    "KERNEL_SPEC_STRUCT",
    "NORMALIZE_STRUCT",
    "PROJECT_EXPR_STRUCT",
    "PROJECT_STRUCT",
    "RELATIONSHIP_STRUCT",
    "REL_OP_STRUCT",
    "RULES_SCHEMA",
    "RULE_DEF_SCHEMA",
    "WINNER_SELECT_STRUCT",
    "rule_definition_table",
    "rule_definitions_from_table",
]
