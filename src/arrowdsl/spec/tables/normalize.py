"""Arrow spec tables for normalize rule definitions and families."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import pyarrow as pa

from arrowdsl.plan.query import ProjectionSpec, QuerySpec
from arrowdsl.spec.codec import (
    decode_scalar_union,
    encode_scalar_union,
    parse_string_tuple,
)
from arrowdsl.spec.expr_ir import ExprSpec, expr_spec_from_json
from arrowdsl.spec.infra import SCALAR_UNION_TYPE
from arrowdsl.spec.io import table_from_rows
from normalize.rule_definitions import NormalizeRuleDefinition
from normalize.rule_model import EvidenceOutput, EvidenceSpec
from normalize.rule_specs import NormalizeRuleFamilySpec

if TYPE_CHECKING:
    from arrowdsl.compute.expr_core import ScalarValue
    from normalize.rule_model import ExecutionMode

RULE_FAMILY_SCHEMA = pa.schema(
    [
        pa.field("name", pa.string(), nullable=False),
        pa.field("factory", pa.string(), nullable=False),
        pa.field("inputs", pa.list_(pa.string()), nullable=True),
        pa.field("output", pa.string(), nullable=True),
        pa.field("confidence_policy", pa.string(), nullable=True),
        pa.field("ambiguity_policy", pa.string(), nullable=True),
        pa.field("option_flag", pa.string(), nullable=True),
        pa.field("execution_mode", pa.string(), nullable=True),
    ],
    metadata={b"spec_kind": b"normalize_rule_families"},
)

QUERY_OP_STRUCT = pa.struct(
    [
        pa.field("kind", pa.string(), nullable=False),
        pa.field("columns", pa.list_(pa.string()), nullable=True),
        pa.field("name", pa.string(), nullable=True),
        pa.field("expr_json", pa.string(), nullable=True),
    ]
)

EVIDENCE_STRUCT = pa.struct(
    [
        pa.field("sources", pa.list_(pa.string()), nullable=True),
        pa.field("required_columns", pa.list_(pa.string()), nullable=True),
        pa.field("required_types", pa.map_(pa.string(), pa.string()), nullable=True),
        pa.field("required_metadata", pa.map_(pa.binary(), pa.binary()), nullable=True),
    ]
)

EVIDENCE_LITERAL_STRUCT = pa.struct(
    [
        pa.field("name", pa.string(), nullable=False),
        pa.field("value_union", SCALAR_UNION_TYPE, nullable=True),
    ]
)

EVIDENCE_OUTPUT_STRUCT = pa.struct(
    [
        pa.field("column_map", pa.map_(pa.string(), pa.string()), nullable=True),
        pa.field("literals", pa.list_(EVIDENCE_LITERAL_STRUCT), nullable=True),
    ]
)

POLICY_OVERRIDE_STRUCT = pa.struct(
    [
        pa.field("confidence_policy", pa.string(), nullable=True),
        pa.field("ambiguity_policy", pa.string(), nullable=True),
    ]
)

NORMALIZE_RULE_DEF_SCHEMA = pa.schema(
    [
        pa.field("name", pa.string(), nullable=False),
        pa.field("inputs", pa.list_(pa.string()), nullable=True),
        pa.field("output", pa.string(), nullable=False),
        pa.field("execution_mode", pa.string(), nullable=True),
        pa.field("plan_builder", pa.string(), nullable=True),
        pa.field("query_ops", pa.list_(QUERY_OP_STRUCT), nullable=True),
        pa.field("evidence", EVIDENCE_STRUCT, nullable=True),
        pa.field("evidence_output", EVIDENCE_OUTPUT_STRUCT, nullable=True),
        pa.field("policy_overrides", POLICY_OVERRIDE_STRUCT, nullable=True),
        pa.field("priority", pa.int32(), nullable=True),
        pa.field("emit_rule_meta", pa.bool_(), nullable=True),
    ],
    metadata={b"spec_kind": b"normalize_rule_definitions"},
)


@dataclass(frozen=True)
class _PolicyOverrides:
    confidence_policy: str | None = None
    ambiguity_policy: str | None = None


def _parse_execution_mode(value: object) -> ExecutionMode:
    if value is None:
        return "auto"
    normalized = str(value)
    if normalized == "auto":
        return "auto"
    if normalized == "plan":
        return "plan"
    if normalized == "table":
        return "table"
    msg = f"Unsupported execution_mode: {value!r}"
    raise ValueError(msg)


def _encode_literal(value: ScalarValue | None) -> ScalarValue | None:
    return encode_scalar_union(value)


def _decode_literal(payload: object | None) -> ScalarValue | None:
    return decode_scalar_union(payload)


def _query_ops_row(spec: QuerySpec | None) -> list[dict[str, object]] | None:
    if spec is None:
        return None
    if spec.projection.derived or spec.predicate or spec.pushdown_predicate:
        msg = "Normalize rule query ops do not support derived or predicate expressions."
        raise ValueError(msg)
    return [{"kind": "project", "columns": list(spec.projection.base) or None}]


@dataclass
class _QueryOpState:
    base: tuple[str, ...] = ()
    derived: dict[str, ExprSpec] = field(default_factory=dict)
    predicate: ExprSpec | None = None
    pushdown: ExprSpec | None = None


def _handle_project(row: Mapping[str, Any], state: _QueryOpState) -> None:
    state.base = parse_string_tuple(row.get("columns"), label="columns")


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


def _query_from_ops(payload: Sequence[Mapping[str, Any]] | None) -> QuerySpec | None:
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


def _evidence_row(spec: EvidenceSpec | None) -> dict[str, object] | None:
    if spec is None:
        return None
    return {
        "sources": list(spec.sources) or None,
        "required_columns": list(spec.required_columns) or None,
        "required_types": dict(spec.required_types) or None,
        "required_metadata": dict(spec.required_metadata) or None,
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
        required_types={str(key): str(value) for key, value in required_types_map.items()},
        required_metadata={
            _ensure_metadata_bytes(key): _ensure_metadata_bytes(value)
            for key, value in metadata_map.items()
        },
    )


def _evidence_output_row(spec: EvidenceOutput | None) -> dict[str, object] | None:
    if spec is None:
        return None
    literals = [
        {"name": name, "value_union": _encode_literal(value)}
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
        literals[str(name)] = _decode_literal(item.get("value_union"))
    if not column_map and not literals:
        return None
    return EvidenceOutput(column_map=column_map, literals=literals)


def _policy_overrides_row(definition: NormalizeRuleDefinition) -> dict[str, object] | None:
    if definition.confidence_policy is None and definition.ambiguity_policy is None:
        return None
    return {
        "confidence_policy": definition.confidence_policy,
        "ambiguity_policy": definition.ambiguity_policy,
    }


def _policy_overrides_from_row(payload: Mapping[str, Any] | None) -> _PolicyOverrides:
    if payload is None:
        return _PolicyOverrides()
    return _PolicyOverrides(
        confidence_policy=payload.get("confidence_policy"),
        ambiguity_policy=payload.get("ambiguity_policy"),
    )


def _ensure_metadata_bytes(value: object) -> bytes:
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    return str(value).encode("utf-8")


def normalize_rule_definition_table(
    definitions: Sequence[NormalizeRuleDefinition],
) -> pa.Table:
    """Build a normalize rule definition spec table.

    Returns
    -------
    pa.Table
        Arrow table with normalize rule definition specs.
    """
    rows = [
        {
            "name": definition.name,
            "inputs": list(definition.inputs) or None,
            "output": definition.output,
            "execution_mode": definition.execution_mode,
            "plan_builder": definition.plan_builder,
            "query_ops": _query_ops_row(definition.query),
            "evidence": _evidence_row(definition.evidence),
            "evidence_output": _evidence_output_row(definition.evidence_output),
            "policy_overrides": _policy_overrides_row(definition),
            "priority": int(definition.priority),
            "emit_rule_meta": definition.emit_rule_meta,
        }
        for definition in definitions
    ]
    return table_from_rows(NORMALIZE_RULE_DEF_SCHEMA, rows)


def normalize_rule_definitions_from_table(
    table: pa.Table,
) -> tuple[NormalizeRuleDefinition, ...]:
    """Compile NormalizeRuleDefinition objects from a spec table.

    Returns
    -------
    tuple[NormalizeRuleDefinition, ...]
        Rule definitions parsed from the table.
    """
    definitions: list[NormalizeRuleDefinition] = []
    for row in table.to_pylist():
        policies = _policy_overrides_from_row(row.get("policy_overrides"))
        definitions.append(
            NormalizeRuleDefinition(
                name=str(row["name"]),
                inputs=parse_string_tuple(row.get("inputs"), label="inputs"),
                output=str(row["output"]),
                execution_mode=_parse_execution_mode(row.get("execution_mode")),
                plan_builder=row.get("plan_builder"),
                query=_query_from_ops(row.get("query_ops")),
                evidence=_evidence_from_row(row.get("evidence")),
                evidence_output=_evidence_output_from_row(row.get("evidence_output")),
                confidence_policy=policies.confidence_policy,
                ambiguity_policy=policies.ambiguity_policy,
                priority=int(row.get("priority", 100)),
                emit_rule_meta=bool(row.get("emit_rule_meta", True)),
            )
        )
    return tuple(definitions)


def normalize_rule_family_table(
    specs: Sequence[NormalizeRuleFamilySpec],
) -> pa.Table:
    """Build a normalize rule family spec table.

    Returns
    -------
    pa.Table
        Arrow table with normalize rule family specs.
    """
    rows = [
        {
            "name": spec.name,
            "factory": spec.factory,
            "inputs": list(spec.inputs) or None,
            "output": spec.output,
            "confidence_policy": spec.confidence_policy,
            "ambiguity_policy": spec.ambiguity_policy,
            "option_flag": spec.option_flag,
            "execution_mode": spec.execution_mode,
        }
        for spec in specs
    ]
    return table_from_rows(RULE_FAMILY_SCHEMA, rows)


def normalize_rule_family_specs_from_table(
    table: pa.Table,
) -> tuple[NormalizeRuleFamilySpec, ...]:
    """Compile NormalizeRuleFamilySpec objects from a spec table.

    Returns
    -------
    tuple[NormalizeRuleFamilySpec, ...]
        Rule family specs parsed from the table.
    """
    return tuple(
        NormalizeRuleFamilySpec(
            name=str(row["name"]),
            factory=str(row["factory"]),
            inputs=parse_string_tuple(row.get("inputs"), label="inputs"),
            output=row.get("output"),
            confidence_policy=row.get("confidence_policy"),
            ambiguity_policy=row.get("ambiguity_policy"),
            option_flag=row.get("option_flag"),
            execution_mode=row.get("execution_mode"),
        )
        for row in table.to_pylist()
    )


__all__ = [
    "NORMALIZE_RULE_DEF_SCHEMA",
    "normalize_rule_definition_table",
    "normalize_rule_definitions_from_table",
    "normalize_rule_family_specs_from_table",
    "normalize_rule_family_table",
]
