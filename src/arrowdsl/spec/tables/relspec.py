"""Arrow spec tables for relationship rules."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from typing import Any, Literal, cast

import pyarrow as pa

from arrowdsl.compute.expr_core import ScalarValue
from arrowdsl.json_factory import JsonPolicy, dumps_text
from arrowdsl.plan.ops import DedupeSpec, JoinType, SortKey
from arrowdsl.plan.query import QuerySpec
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
    DEDUPE_STRUCT,
    SCALAR_UNION_TYPE,
    SORT_KEY_STRUCT,
)
from arrowdsl.spec.io import table_from_rows
from relspec.model import (
    AddLiteralSpec,
    AmbiguityPolicy,
    CanonicalSortKernelSpec,
    ConfidencePolicy,
    DatasetRef,
    DedupeKernelSpec,
    DropColumnsSpec,
    EvidenceSpec,
    ExecutionMode,
    ExplodeListSpec,
    FilterKernelSpec,
    HashJoinConfig,
    IntervalAlignConfig,
    KernelSpecT,
    ProjectConfig,
    RelationshipRule,
    RenameColumnsSpec,
    RuleFamilySpec,
    RuleKind,
    WinnerSelectConfig,
)
from relspec.policies import confidence_expr

HASH_JOIN_STRUCT = pa.struct(
    [
        pa.field("join_type", pa.string(), nullable=False),
        pa.field("left_keys", pa.list_(pa.string()), nullable=False),
        pa.field("right_keys", pa.list_(pa.string()), nullable=True),
        pa.field("left_output", pa.list_(pa.string()), nullable=True),
        pa.field("right_output", pa.list_(pa.string()), nullable=True),
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
        pa.field("select_left", pa.list_(pa.string()), nullable=True),
        pa.field("select_right", pa.list_(pa.string()), nullable=True),
        pa.field("tie_breakers", pa.list_(SORT_KEY_STRUCT), nullable=True),
        pa.field("emit_match_meta", pa.bool_(), nullable=False),
        pa.field("match_kind_col", pa.string(), nullable=False),
        pa.field("match_score_col", pa.string(), nullable=False),
    ]
)

WINNER_SELECT_STRUCT = pa.struct(
    [
        pa.field("keys", pa.list_(pa.string()), nullable=True),
        pa.field("score_col", pa.string(), nullable=False),
        pa.field("score_order", pa.string(), nullable=False),
        pa.field("tie_breakers", pa.list_(SORT_KEY_STRUCT), nullable=True),
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
        pa.field("select", pa.list_(pa.string()), nullable=True),
        pa.field("exprs", pa.list_(PROJECT_EXPR_STRUCT), nullable=True),
    ]
)

EVIDENCE_STRUCT = pa.struct(
    [
        pa.field("sources", pa.list_(pa.string()), nullable=True),
        pa.field("required_columns", pa.list_(pa.string()), nullable=True),
        pa.field("required_types", pa.map_(pa.string(), pa.string()), nullable=True),
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
        pa.field("tie_breakers", pa.list_(SORT_KEY_STRUCT), nullable=True),
    ]
)

KERNEL_SPEC_STRUCT = pa.struct(
    [
        pa.field("kind", pa.string(), nullable=False),
        pa.field(
            "add_literal",
            pa.struct([("name", pa.string()), ("value_union", SCALAR_UNION_TYPE)]),
        ),
        pa.field("filter", pa.struct([("expr_json", pa.string())])),
        pa.field("drop_columns", pa.struct([("columns", pa.list_(pa.string()))])),
        pa.field("rename_columns", pa.struct([("mapping", pa.map_(pa.string(), pa.string()))])),
        pa.field(
            "explode_list",
            pa.struct(
                [
                    ("parent_id_col", pa.string()),
                    ("list_col", pa.string()),
                    ("out_parent_col", pa.string()),
                    ("out_value_col", pa.string()),
                ]
            ),
        ),
        pa.field("dedupe", DEDUPE_STRUCT),
        pa.field("canonical_sort", pa.struct([("sort_keys", pa.list_(SORT_KEY_STRUCT))])),
    ]
)

RULE_DEFINITION_SCHEMA = pa.schema(
    [
        pa.field("name", pa.string(), nullable=False),
        pa.field("kind", pa.string(), nullable=False),
        pa.field("output_dataset", pa.string(), nullable=True),
        pa.field("contract_name", pa.string(), nullable=True),
        pa.field("inputs", pa.list_(pa.string()), nullable=True),
        pa.field("hash_join", HASH_JOIN_STRUCT, nullable=True),
        pa.field("interval_align", INTERVAL_ALIGN_STRUCT, nullable=True),
        pa.field("winner_select", WINNER_SELECT_STRUCT, nullable=True),
        pa.field("predicate_expr", pa.string(), nullable=True),
        pa.field("project", PROJECT_STRUCT, nullable=True),
        pa.field("post_kernels", pa.list_(KERNEL_SPEC_STRUCT), nullable=True),
        pa.field("evidence", EVIDENCE_STRUCT, nullable=True),
        pa.field("confidence_policy", pa.string(), nullable=True),
        pa.field("ambiguity_policy", pa.string(), nullable=True),
        pa.field("priority", pa.int32(), nullable=False),
        pa.field("emit_rule_meta", pa.bool_(), nullable=False),
        pa.field("rule_name_col", pa.string(), nullable=False),
        pa.field("rule_priority_col", pa.string(), nullable=False),
        pa.field("execution_mode", pa.string(), nullable=False),
    ],
    metadata={b"spec_kind": b"relationship_rule_definitions"},
)

RULES_SCHEMA = pa.schema(
    [
        pa.field("name", pa.string(), nullable=False),
        pa.field("kind", pa.string(), nullable=False),
        pa.field("output_dataset", pa.string(), nullable=False),
        pa.field("contract_name", pa.string(), nullable=True),
        pa.field("inputs", pa.list_(DATASET_REF_STRUCT), nullable=False),
        pa.field("hash_join", HASH_JOIN_STRUCT, nullable=True),
        pa.field("interval_align", INTERVAL_ALIGN_STRUCT, nullable=True),
        pa.field("winner_select", WINNER_SELECT_STRUCT, nullable=True),
        pa.field("project", PROJECT_STRUCT, nullable=True),
        pa.field("post_kernels", pa.list_(KERNEL_SPEC_STRUCT), nullable=True),
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

RULE_FAMILY_SCHEMA = pa.schema(
    [
        pa.field("name", pa.string(), nullable=False),
        pa.field("factory", pa.string(), nullable=False),
        pa.field("inputs", pa.list_(pa.string()), nullable=True),
        pa.field("confidence_policy", pa.string(), nullable=True),
        pa.field("ambiguity_policy", pa.string(), nullable=True),
        pa.field("option_flag", pa.string(), nullable=True),
    ],
    metadata={b"spec_kind": b"relationship_rule_families"},
)


def _encode_expr(expr: ExprIR) -> str:
    return expr.to_json()


def _decode_expr(payload: str) -> ExprIR:
    return ExprIR.from_json(payload)


def _encode_query(spec: QuerySpec) -> str:
    if spec.projection.derived:
        msg = "QuerySpec serialization does not support derived projections."
        raise ValueError(msg)
    if spec.predicate is not None or spec.pushdown_predicate is not None:
        msg = "QuerySpec serialization does not support predicate expressions."
        raise ValueError(msg)
    payload = {"projection": {"base": list(spec.projection.base)}}
    policy = JsonPolicy(ascii_only=True)
    return dumps_text(payload, policy=policy)


def _decode_query(payload: str) -> QuerySpec:
    data = json.loads(payload)
    if not isinstance(data, Mapping):
        msg = "QuerySpec JSON must decode to a mapping."
        raise TypeError(msg)
    projection = data.get("projection", data)
    if not isinstance(projection, Mapping):
        msg = "QuerySpec projection payload must be a mapping."
        raise TypeError(msg)
    base = projection.get("base")
    if not isinstance(base, (list, tuple)):
        msg = "QuerySpec projection base must be a list."
        raise TypeError(msg)
    cols = tuple(str(item) for item in base)
    return QuerySpec.simple(*cols)


def _encode_sort_key(column: str, order: str) -> dict[str, object]:
    return {"column": column, "order": order}


def _encode_sort_keys(keys: Sequence[SortKey]) -> list[dict[str, object]]:
    return [_encode_sort_key(key.column, key.order) for key in keys]


def _decode_sort_keys(payload: Sequence[Mapping[str, Any]] | None) -> tuple[SortKey, ...]:
    if not payload:
        return ()
    return tuple(
        SortKey(column=str(row["column"]), order=parse_sort_order(row.get("order")))
        for row in payload
    )


def _parse_join_type(value: object) -> JoinType:
    if value is None:
        return "inner"
    normalized = str(value)
    allowed: set[JoinType] = {
        "inner",
        "left outer",
        "right outer",
        "full outer",
        "left semi",
        "right semi",
        "left anti",
        "right anti",
    }
    if normalized in allowed:
        return cast("JoinType", normalized)
    msg = f"Unsupported join_type: {value!r}"
    raise ValueError(msg)


def _parse_execution_mode(value: object) -> ExecutionMode:
    normalized = str(value)
    if normalized == "auto":
        return "auto"
    if normalized == "plan":
        return "plan"
    if normalized == "table":
        return "table"
    msg = f"Unsupported execution_mode: {normalized!r}"
    raise ValueError(msg)


def _parse_interval_mode(value: object) -> Literal["EXACT", "CONTAINED_BEST", "OVERLAP_BEST"]:
    if value is None:
        return "CONTAINED_BEST"
    normalized = str(value).upper()
    allowed = {"EXACT", "CONTAINED_BEST", "OVERLAP_BEST"}
    if normalized in allowed:
        return cast("Literal['EXACT', 'CONTAINED_BEST', 'OVERLAP_BEST']", normalized)
    msg = f"Unsupported interval mode: {value!r}"
    raise ValueError(msg)


def _parse_interval_how(value: object) -> Literal["inner", "left"]:
    if value is None:
        return "inner"
    normalized = str(value).lower()
    options: dict[str, Literal["inner", "left"]] = {"inner": "inner", "left": "left"}
    mapped = options.get(normalized)
    if mapped is not None:
        return mapped
    msg = f"Unsupported interval how: {value!r}"
    raise ValueError(msg)


def _parse_score_order(value: object) -> Literal["ascending", "descending"]:
    return parse_sort_order(value)


def _encode_literal(value: object | None) -> ScalarValue | None:
    return encode_scalar_union(cast("ScalarValue | None", value))


def _decode_literal(payload: object | None) -> ScalarValue | None:
    return decode_scalar_union(payload)


def _dataset_ref_row(ref: DatasetRef) -> dict[str, object]:
    query_json = _encode_query(ref.query) if ref.query is not None else None
    return {"name": ref.name, "label": ref.label, "query_json": query_json}


def _dataset_ref_from_row(payload: Mapping[str, Any]) -> DatasetRef:
    query_json = payload.get("query_json")
    query = _decode_query(str(query_json)) if query_json else None
    return DatasetRef(
        name=str(payload["name"]),
        label=str(payload.get("label", "")),
        query=query,
    )


def _hash_join_row(config: HashJoinConfig | None) -> dict[str, object] | None:
    if config is None:
        return None
    return {
        "join_type": config.join_type,
        "left_keys": list(config.left_keys),
        "right_keys": list(config.right_keys) or None,
        "left_output": list(config.left_output) or None,
        "right_output": list(config.right_output) or None,
        "output_suffix_for_left": config.output_suffix_for_left,
        "output_suffix_for_right": config.output_suffix_for_right,
    }


def _interval_align_row(config: IntervalAlignConfig | None) -> dict[str, object] | None:
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


def _winner_select_row(config: WinnerSelectConfig | None) -> dict[str, object] | None:
    if config is None:
        return None
    return {
        "keys": list(config.keys) or None,
        "score_col": config.score_col,
        "score_order": config.score_order,
        "tie_breakers": _encode_sort_keys(config.tie_breakers) or None,
    }


def _evidence_row(spec: EvidenceSpec | None) -> dict[str, object] | None:
    if spec is None:
        return None
    return {
        "sources": list(spec.sources) or None,
        "required_columns": list(spec.required_columns) or None,
        "required_types": dict(spec.required_types) or None,
    }


def _confidence_row(policy: ConfidencePolicy | None) -> dict[str, object] | None:
    if policy is None:
        return None
    return {
        "base": float(policy.base),
        "source_weight": dict(policy.source_weight) or None,
        "penalty": float(policy.penalty),
    }


def _ambiguity_row(policy: AmbiguityPolicy | None) -> dict[str, object] | None:
    if policy is None:
        return None
    return {
        "winner_select": _winner_select_row(policy.winner_select),
        "tie_breakers": _encode_sort_keys(policy.tie_breakers) or None,
    }


def _project_row(config: ProjectConfig | None) -> dict[str, object] | None:
    if config is None:
        return None
    expr_rows = [
        {"name": name, "expr_json": _encode_expr(expr)} for name, expr in config.exprs.items()
    ]
    return {
        "select": list(config.select) or None,
        "exprs": expr_rows or None,
    }


def _kernel_row(spec: KernelSpecT) -> dict[str, object]:
    base: dict[str, object] = {"kind": spec.kind}
    if isinstance(spec, AddLiteralSpec):
        base["add_literal"] = {"name": spec.name, "value_union": _encode_literal(spec.value)}
    elif isinstance(spec, FilterKernelSpec):
        base["filter"] = {"expr_json": _encode_expr(spec.predicate)}
    elif isinstance(spec, DropColumnsSpec):
        base["drop_columns"] = {"columns": list(spec.columns) or None}
    elif isinstance(spec, RenameColumnsSpec):
        base["rename_columns"] = {"mapping": dict(spec.mapping) or None}
    elif isinstance(spec, ExplodeListSpec):
        base["explode_list"] = {
            "parent_id_col": spec.parent_id_col,
            "list_col": spec.list_col,
            "out_parent_col": spec.out_parent_col,
            "out_value_col": spec.out_value_col,
        }
    elif isinstance(spec, DedupeKernelSpec):
        base["dedupe"] = {
            "keys": list(spec.spec.keys),
            "tie_breakers": _encode_sort_keys(spec.spec.tie_breakers) or None,
            "strategy": spec.spec.strategy,
        }
    elif isinstance(spec, CanonicalSortKernelSpec):
        base["canonical_sort"] = {"sort_keys": _encode_sort_keys(spec.sort_keys) or None}
    else:
        msg = f"Unsupported KernelSpec type: {type(spec).__name__}."
        raise TypeError(msg)
    return base


def _kernel_from_row(payload: Mapping[str, Any]) -> KernelSpecT:
    kind = str(payload["kind"])
    if kind == "add_literal":
        spec = payload.get("add_literal") or {}
        result: KernelSpecT = AddLiteralSpec(
            name=str(spec.get("name", "")),
            value=_decode_literal(spec.get("value_union")),
        )
    elif kind == "filter":
        spec = payload.get("filter") or {}
        expr_json = str(spec.get("expr_json", ""))
        result = FilterKernelSpec(predicate=_decode_expr(expr_json))
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
                strategy=parse_dedupe_strategy(spec.get("strategy")),
            )
        )
    elif kind == "canonical_sort":
        spec = payload.get("canonical_sort") or {}
        result = CanonicalSortKernelSpec(sort_keys=_decode_sort_keys(spec.get("sort_keys")))
    else:
        msg = f"Unsupported kernel kind: {kind!r}"
        raise ValueError(msg)
    return result


def hash_join_row(config: HashJoinConfig | None) -> dict[str, object] | None:
    """Return a row mapping for a hash join config."""
    return _hash_join_row(config)


def interval_align_row(config: IntervalAlignConfig | None) -> dict[str, object] | None:
    """Return a row mapping for an interval align config."""
    return _interval_align_row(config)


def winner_select_row(config: WinnerSelectConfig | None) -> dict[str, object] | None:
    """Return a row mapping for a winner select config."""
    return _winner_select_row(config)


def project_row(config: ProjectConfig | None) -> dict[str, object] | None:
    """Return a row mapping for a projection config."""
    return _project_row(config)


def evidence_row(spec: EvidenceSpec | None) -> dict[str, object] | None:
    """Return a row mapping for evidence metadata."""
    return _evidence_row(spec)


def kernel_spec_row(spec: KernelSpecT) -> dict[str, object]:
    """Return a row mapping for a kernel spec."""
    return _kernel_row(spec)


def _project_from_payload(payload: Mapping[str, Any] | None) -> ProjectConfig | None:
    if payload is None:
        return None
    exprs = {
        str(item["name"]): _decode_expr(str(item["expr_json"]))
        for item in payload.get("exprs") or ()
    }
    return ProjectConfig(
        select=tuple(payload.get("select") or ()),
        exprs=exprs,
    )


def _project_required_columns(project: ProjectConfig | None) -> set[str]:
    if project is None:
        return set()
    required = set(project.select)
    for expr in project.exprs.values():
        required.update(_expr_fields(expr))
    return required


def _expr_fields(expr: ExprIR | None) -> set[str]:
    if expr is None:
        return set()
    if expr.op == "field":
        return {expr.name} if expr.name else set()
    fields: set[str] = set()
    for arg in expr.args:
        fields.update(_expr_fields(arg))
    return fields


def _derived_required_columns(
    *,
    kind: RuleKind,
    inputs: Sequence[str],
    project: ProjectConfig | None,
    predicate: ExprIR | None,
) -> set[str]:
    if kind != RuleKind.FILTER_PROJECT or len(inputs) != 1:
        return set()
    required = _project_required_columns(project)
    required.update(_expr_fields(predicate))
    return required


def _with_confidence(
    project: ProjectConfig,
    policy: ConfidencePolicy | None,
    *,
    score_expr: ExprIR | None = None,
    source_field: str | None = None,
) -> ProjectConfig:
    if policy is None and score_expr is None:
        return project
    expr = score_expr or confidence_expr(policy or ConfidencePolicy(), source_field=source_field)
    exprs = dict(project.exprs)
    exprs.setdefault("confidence", expr)
    exprs.setdefault("score", expr)
    return ProjectConfig(select=project.select, exprs=exprs)


def _resolve_evidence_spec(
    payload: Mapping[str, Any] | None,
    *,
    kind: RuleKind,
    inputs: Sequence[str],
    project: ProjectConfig | None,
    predicate: ExprIR | None,
) -> EvidenceSpec | None:
    evidence = _evidence_from_row(payload)
    sources = evidence.sources if evidence is not None else tuple(inputs)
    required_columns = evidence.required_columns if evidence is not None else ()
    required_types = dict(evidence.required_types) if evidence is not None else {}
    derived = _derived_required_columns(
        kind=kind,
        inputs=inputs,
        project=project,
        predicate=predicate,
    )
    if derived:
        required_columns = tuple(sorted(set(required_columns).union(derived)))
    if not sources and not required_columns and not required_types:
        return None
    return EvidenceSpec(
        sources=tuple(sources),
        required_columns=tuple(sorted(set(required_columns))),
        required_types=required_types,
    )


def relationship_rule_definition_table(rows: Sequence[Mapping[str, Any]]) -> pa.Table:
    """Build a relationship rule definition table.

    Returns
    -------
    pa.Table
        Arrow table with rule definition rows.
    """
    return table_from_rows(RULE_DEFINITION_SCHEMA, rows)


def relationship_rule_table(rules: Sequence[RelationshipRule]) -> pa.Table:
    """Build a relationship rule table.

    Returns
    -------
    pa.Table
        Arrow table with relationship rules.
    """
    rows = [
        {
            "name": rule.name,
            "kind": rule.kind.value,
            "output_dataset": rule.output_dataset,
            "contract_name": rule.contract_name,
            "inputs": [_dataset_ref_row(ref) for ref in rule.inputs],
            "hash_join": _hash_join_row(rule.hash_join),
            "interval_align": _interval_align_row(rule.interval_align),
            "winner_select": _winner_select_row(rule.winner_select),
            "project": _project_row(rule.project),
            "post_kernels": [_kernel_row(spec) for spec in rule.post_kernels] or None,
            "evidence": _evidence_row(rule.evidence),
            "confidence_policy": _confidence_row(rule.confidence_policy),
            "ambiguity_policy": _ambiguity_row(rule.ambiguity_policy),
            "priority": int(rule.priority),
            "emit_rule_meta": rule.emit_rule_meta,
            "rule_name_col": rule.rule_name_col,
            "rule_priority_col": rule.rule_priority_col,
            "execution_mode": rule.execution_mode,
        }
        for rule in rules
    ]
    return table_from_rows(RULES_SCHEMA, rows)


def _relationship_rules_from_rule_table(table: pa.Table) -> tuple[RelationshipRule, ...]:
    """Compile RelationshipRule objects from a rule spec table.

    Returns
    -------
    tuple[RelationshipRule, ...]
        Relationship rules parsed from the rules table.
    """
    rules: list[RelationshipRule] = []
    for row in table.to_pylist():
        inputs = tuple(_dataset_ref_from_row(item) for item in row.get("inputs") or ())
        project_spec = _project_from_payload(row.get("project"))
        post_kernels = tuple(_kernel_from_row(item) for item in row.get("post_kernels") or ())
        execution_mode = _parse_execution_mode(row.get("execution_mode", "auto"))
        rules.append(
            RelationshipRule(
                name=str(row["name"]),
                kind=RuleKind(str(row["kind"])),
                output_dataset=str(row["output_dataset"]),
                contract_name=row.get("contract_name"),
                inputs=inputs,
                hash_join=_hash_join_from_row(row.get("hash_join")),
                interval_align=_interval_align_from_row(row.get("interval_align")),
                winner_select=_winner_select_from_row(row.get("winner_select")),
                project=project_spec,
                post_kernels=post_kernels,
                evidence=_evidence_from_row(row.get("evidence")),
                confidence_policy=_confidence_from_row(row.get("confidence_policy")),
                ambiguity_policy=_ambiguity_from_row(row.get("ambiguity_policy")),
                priority=int(row.get("priority", 100)),
                emit_rule_meta=bool(row.get("emit_rule_meta", True)),
                rule_name_col=str(row.get("rule_name_col", "rule_name")),
                rule_priority_col=str(row.get("rule_priority_col", "rule_priority")),
                execution_mode=execution_mode,
            )
        )
    return tuple(rules)


def _relationship_rules_from_definition_table(table: pa.Table) -> tuple[RelationshipRule, ...]:
    rules: list[RelationshipRule] = []
    for row in table.to_pylist():
        inputs = parse_string_tuple(row.get("inputs"), label="inputs")
        predicate_json = row.get("predicate_expr")
        predicate = _decode_expr(str(predicate_json)) if predicate_json else None
        base_project = _project_from_payload(row.get("project"))
        kind = RuleKind(str(row["kind"]))
        confidence_name = row.get("confidence_policy")
        ambiguity_name = row.get("ambiguity_policy")
        confidence_policy = resolve_confidence_policy(str(confidence_name)) if confidence_name else None
        ambiguity_policy = resolve_ambiguity_policy(str(ambiguity_name)) if ambiguity_name else None
        project = base_project
        if project is None and confidence_policy is not None:
            project = ProjectConfig()
        if project is not None:
            project = _with_confidence(project, confidence_policy)
        post_kernels = tuple(_kernel_from_row(item) for item in row.get("post_kernels") or ())
        if predicate is not None:
            post_kernels = (FilterKernelSpec(predicate=predicate), *post_kernels)
        post_kernels = (*post_kernels, *ambiguity_kernels(ambiguity_policy))
        evidence = _resolve_evidence_spec(
            row.get("evidence"),
            kind=kind,
            inputs=inputs,
            project=base_project,
            predicate=predicate,
        )
        execution_mode = _parse_execution_mode(row.get("execution_mode", "auto"))
        output_dataset = row.get("output_dataset") or str(row["name"])
        contract_name = row.get("contract_name") or RELATION_OUTPUT_NAME
        rules.append(
            RelationshipRule(
                name=str(row["name"]),
                kind=kind,
                output_dataset=str(output_dataset),
                contract_name=str(contract_name),
                inputs=tuple(DatasetRef(name=name) for name in inputs),
                hash_join=_hash_join_from_row(row.get("hash_join")),
                interval_align=_interval_align_from_row(row.get("interval_align")),
                winner_select=_winner_select_from_row(row.get("winner_select")),
                project=project,
                post_kernels=post_kernels,
                evidence=evidence,
                confidence_policy=confidence_policy,
                ambiguity_policy=ambiguity_policy,
                priority=int(row.get("priority", 100)),
                emit_rule_meta=bool(row.get("emit_rule_meta", True)),
                rule_name_col=str(row.get("rule_name_col", "rule_name")),
                rule_priority_col=str(row.get("rule_priority_col", "rule_priority")),
                execution_mode=execution_mode,
            )
        )
    return tuple(rules)


def relationship_rules_from_table(table: pa.Table) -> tuple[RelationshipRule, ...]:
    """Compile RelationshipRule objects from a spec table.

    Returns
    -------
    tuple[RelationshipRule, ...]
        Relationship rules parsed from the table.
    """
    meta = table.schema.metadata or {}
    spec_kind = meta.get(b"spec_kind", b"").decode("utf-8")
    if spec_kind == "relationship_rule_definitions":
        return _relationship_rules_from_definition_table(table)
    return _relationship_rules_from_rule_table(table)


def rule_family_spec_table(specs: Sequence[RuleFamilySpec]) -> pa.Table:
    """Build a relationship rule family spec table.

    Returns
    -------
    pa.Table
        Arrow table with rule family specs.
    """
    rows = [
        {
            "name": spec.name,
            "factory": spec.factory,
            "inputs": list(spec.inputs) or None,
            "confidence_policy": spec.confidence_policy,
            "ambiguity_policy": spec.ambiguity_policy,
            "option_flag": spec.option_flag,
        }
        for spec in specs
    ]
    return table_from_rows(RULE_FAMILY_SCHEMA, rows)


def rule_family_specs_from_table(table: pa.Table) -> tuple[RuleFamilySpec, ...]:
    """Compile RuleFamilySpec objects from a spec table.

    Returns
    -------
    tuple[RuleFamilySpec, ...]
        Rule family specs parsed from the table.
    """
    return tuple(
        RuleFamilySpec(
            name=str(row["name"]),
            factory=str(row["factory"]),
            inputs=parse_string_tuple(row.get("inputs"), label="inputs"),
            confidence_policy=row.get("confidence_policy"),
            ambiguity_policy=row.get("ambiguity_policy"),
            option_flag=row.get("option_flag"),
        )
        for row in table.to_pylist()
    )


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


def _evidence_from_row(payload: Mapping[str, Any] | None) -> EvidenceSpec | None:
    if payload is None:
        return None
    required_types = payload.get("required_types")
    required_types_map = dict(required_types) if isinstance(required_types, Mapping) else {}
    return EvidenceSpec(
        sources=parse_string_tuple(payload.get("sources"), label="sources"),
        required_columns=parse_string_tuple(
            payload.get("required_columns"), label="required_columns"
        ),
        required_types={str(k): str(v) for k, v in required_types_map.items()},
    )


def _confidence_from_row(payload: Mapping[str, Any] | None) -> ConfidencePolicy | None:
    if payload is None:
        return None
    weights = payload.get("source_weight")
    source_weight = dict(weights) if isinstance(weights, Mapping) else {}
    return ConfidencePolicy(
        base=float(payload.get("base", 0.5)),
        source_weight=source_weight,
        penalty=float(payload.get("penalty", 0.0)),
    )


def _ambiguity_from_row(payload: Mapping[str, Any] | None) -> AmbiguityPolicy | None:
    if payload is None:
        return None
    tie_breakers = _decode_sort_keys(payload.get("tie_breakers"))
    winner_payload = payload.get("winner_select")
    return AmbiguityPolicy(
        winner_select=_winner_select_from_row(winner_payload),
        tie_breakers=tie_breakers,
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


__all__ = [
    "AMBIGUITY_STRUCT",
    "CONFIDENCE_STRUCT",
    "DATASET_REF_STRUCT",
    "EVIDENCE_STRUCT",
    "HASH_JOIN_STRUCT",
    "INTERVAL_ALIGN_STRUCT",
    "PROJECT_EXPR_STRUCT",
    "PROJECT_STRUCT",
    "RULE_DEFINITION_SCHEMA",
    "RULES_SCHEMA",
    "RULE_FAMILY_SCHEMA",
    "SORT_KEY_STRUCT",
    "WINNER_SELECT_STRUCT",
    "evidence_row",
    "hash_join_row",
    "interval_align_row",
    "kernel_spec_row",
    "project_row",
    "relationship_rule_table",
    "relationship_rule_definition_table",
    "relationship_rules_from_table",
    "rule_family_spec_table",
    "rule_family_specs_from_table",
    "winner_select_row",
]
