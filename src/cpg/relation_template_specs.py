"""Template-driven rule definitions for relationship rules."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field

import pyarrow as pa

from arrowdsl.compute.expr_core import ScalarValue
from arrowdsl.spec.expr_ir import ExprIR
from cpg.kinds_ultimate import (
    SCIP_ROLE_DEFINITION,
    SCIP_ROLE_IMPORT,
    SCIP_ROLE_READ,
    SCIP_ROLE_WRITE,
    EdgeKind,
)
from relspec.contracts import RELATION_OUTPUT_NAME
from relspec.model import (
    AddLiteralSpec,
    EvidenceSpec,
    ExecutionMode,
    HashJoinConfig,
    IntervalAlignConfig,
    KernelSpecT,
    ProjectConfig,
    RuleKind,
    WinnerSelectConfig,
)


@dataclass(frozen=True)
class EdgeDefinitionSpec:
    """Edge emission definition tied to a relationship rule."""

    edge_kind: EdgeKind
    src_cols: tuple[str, ...]
    dst_cols: tuple[str, ...]
    origin: str
    resolution_method: str
    option_flag: str
    path_cols: tuple[str, ...] = ("path",)
    bstart_cols: tuple[str, ...] = ("bstart",)
    bend_cols: tuple[str, ...] = ("bend",)


@dataclass(frozen=True)
class RuleDefinitionSpec:
    """Declarative rule definition for relationship rules."""

    name: str
    kind: RuleKind
    inputs: tuple[str, ...] = ()
    output_dataset: str | None = None
    contract_name: str | None = RELATION_OUTPUT_NAME

    hash_join: HashJoinConfig | None = None
    interval_align: IntervalAlignConfig | None = None
    winner_select: WinnerSelectConfig | None = None

    predicate: ExprIR | None = None
    project: ProjectConfig | None = None
    post_kernels: tuple[KernelSpecT, ...] = ()
    evidence: EvidenceSpec | None = None

    confidence_policy: str | None = None
    ambiguity_policy: str | None = None

    priority: int = 100
    emit_rule_meta: bool = True
    rule_name_col: str = "rule_name"
    rule_priority_col: str = "rule_priority"
    execution_mode: ExecutionMode = "auto"

    edge: EdgeDefinitionSpec | None = None


@dataclass(frozen=True)
class RuleTemplateSpec:
    """Template spec for generating relationship rules."""

    name: str
    factory: str
    inputs: tuple[str, ...]
    params: Mapping[str, object] = field(default_factory=dict)


TemplateFactory = Callable[[RuleTemplateSpec], tuple[RuleDefinitionSpec, ...]]


def _field(name: str) -> ExprIR:
    return ExprIR(op="field", name=name)


def _literal(value: ScalarValue) -> ExprIR:
    return ExprIR(op="literal", value=value)


def _call(name: str, *args: ExprIR) -> ExprIR:
    return ExprIR(op="call", name=name, args=tuple(args))


def _fill_null(expr: ExprIR, value: ScalarValue) -> ExprIR:
    return _call("fill_null", expr, _literal(value))


def _bitmask_predicate(field_name: str, mask: int, *, invert: bool = False) -> ExprIR:
    masked = _call("bit_wise_and", _field(field_name), _literal(mask))
    hit = _call("not_equal", masked, _literal(0))
    hit = _fill_null(hit, value=False)
    if invert:
        return _call("invert", hit)
    return hit


def _flag_predicate(field_name: str) -> ExprIR:
    return _fill_null(_field(field_name), value=False)


def _severity_score_expr() -> ExprIR:
    severity = _fill_null(_field("severity"), "ERROR")
    is_error = _call("equal", severity, _literal("ERROR"))
    is_warning = _call("equal", severity, _literal("WARNING"))
    return _call(
        "if_else",
        is_error,
        _literal(1.0),
        _call("if_else", is_warning, _literal(0.7), _literal(0.5)),
    )


def _require_inputs(spec: RuleTemplateSpec, *, count: int) -> tuple[str, ...]:
    if len(spec.inputs) != count:
        msg = f"RuleTemplateSpec {spec.name!r} requires {count} inputs."
        raise ValueError(msg)
    return spec.inputs


def _param_str(spec: RuleTemplateSpec, key: str) -> str | None:
    value = spec.params.get(key)
    if value is None:
        return None
    return str(value)


def _required_param_str(spec: RuleTemplateSpec, key: str) -> str:
    value = _param_str(spec, key)
    if not value:
        msg = f"RuleTemplateSpec {spec.name!r} missing required param: {key}"
        raise ValueError(msg)
    return value


def _symbol_role_template(spec: RuleTemplateSpec) -> tuple[RuleDefinitionSpec, ...]:
    input_name = _require_inputs(spec, count=1)[0]
    confidence_policy = _param_str(spec, "confidence_policy")
    option_flag = _required_param_str(spec, "option_flag")
    base_project = ProjectConfig(
        select=("name_ref_id", "symbol", "path", "bstart", "bend", "symbol_roles")
    )
    role_specs = (
        (
            "symbol_role_defines",
            SCIP_ROLE_DEFINITION,
            False,
            EdgeKind.PY_DEFINES_SYMBOL,
        ),
        (
            "symbol_role_references",
            SCIP_ROLE_DEFINITION,
            True,
            EdgeKind.PY_REFERENCES_SYMBOL,
        ),
        ("symbol_role_reads", SCIP_ROLE_READ, False, EdgeKind.PY_READS_SYMBOL),
        ("symbol_role_writes", SCIP_ROLE_WRITE, False, EdgeKind.PY_WRITES_SYMBOL),
    )
    return tuple(
        RuleDefinitionSpec(
            name=name,
            kind=RuleKind.FILTER_PROJECT,
            inputs=(input_name,),
            predicate=_bitmask_predicate("symbol_roles", mask, invert=invert),
            project=base_project,
            confidence_policy=confidence_policy,
            edge=EdgeDefinitionSpec(
                edge_kind=edge_kind,
                src_cols=("name_ref_id",),
                dst_cols=("symbol",),
                origin="scip",
                resolution_method="SPAN_EXACT",
                option_flag=option_flag,
            ),
        )
        for name, mask, invert, edge_kind in role_specs
    )


def _scip_symbol_template(spec: RuleTemplateSpec) -> tuple[RuleDefinitionSpec, ...]:
    input_name = _require_inputs(spec, count=1)[0]
    confidence_policy = _param_str(spec, "confidence_policy")
    option_flag = _required_param_str(spec, "option_flag")
    rules = (
        (
            "scip_symbol_reference",
            "is_reference",
            EdgeKind.SCIP_SYMBOL_REFERENCE,
            "SCIP_SYMBOL_REFERENCE",
        ),
        (
            "scip_symbol_implementation",
            "is_implementation",
            EdgeKind.SCIP_SYMBOL_IMPLEMENTATION,
            "SCIP_SYMBOL_IMPLEMENTATION",
        ),
        (
            "scip_symbol_type_definition",
            "is_type_definition",
            EdgeKind.SCIP_SYMBOL_TYPE_DEFINITION,
            "SCIP_SYMBOL_TYPE_DEFINITION",
        ),
        (
            "scip_symbol_definition",
            "is_definition",
            EdgeKind.SCIP_SYMBOL_DEFINITION,
            "SCIP_SYMBOL_DEFINITION",
        ),
    )
    return tuple(
        RuleDefinitionSpec(
            name=rule_name,
            kind=RuleKind.FILTER_PROJECT,
            inputs=(input_name,),
            predicate=_flag_predicate(flag_col),
            project=ProjectConfig(select=("symbol", "related_symbol", flag_col)),
            confidence_policy=confidence_policy,
            edge=EdgeDefinitionSpec(
                edge_kind=edge_kind,
                src_cols=("symbol",),
                dst_cols=("related_symbol",),
                origin="scip",
                resolution_method=resolution_method,
                option_flag=option_flag,
            ),
        )
        for rule_name, flag_col, edge_kind, resolution_method in rules
    )


def _type_template(spec: RuleTemplateSpec) -> tuple[RuleDefinitionSpec, ...]:
    input_name = _require_inputs(spec, count=1)[0]
    confidence_policy = _param_str(spec, "confidence_policy")
    option_flag = _required_param_str(spec, "option_flag")
    annotation_edge = EdgeDefinitionSpec(
        edge_kind=EdgeKind.HAS_ANNOTATION,
        src_cols=("owner_def_id",),
        dst_cols=("type_expr_id",),
        origin="annotation",
        resolution_method="TYPE_ANNOTATION",
        option_flag=option_flag,
    )
    inferred_edge = EdgeDefinitionSpec(
        edge_kind=EdgeKind.INFERRED_TYPE,
        src_cols=("owner_def_id",),
        dst_cols=("type_id",),
        origin="inferred",
        resolution_method="ANNOTATION_INFER",
        option_flag=option_flag,
    )
    return (
        RuleDefinitionSpec(
            name="type_annotation_edges",
            kind=RuleKind.FILTER_PROJECT,
            inputs=(input_name,),
            project=ProjectConfig(
                select=("owner_def_id", "type_expr_id", "path", "bstart", "bend")
            ),
            post_kernels=(
                AddLiteralSpec(name="confidence", value=pa.scalar(1.0, type=pa.float32())),
                AddLiteralSpec(name="score", value=pa.scalar(1.0, type=pa.float32())),
            ),
            confidence_policy=confidence_policy,
            edge=annotation_edge,
        ),
        RuleDefinitionSpec(
            name="inferred_type_edges",
            kind=RuleKind.FILTER_PROJECT,
            inputs=(input_name,),
            project=ProjectConfig(select=("owner_def_id", "type_id")),
            post_kernels=(
                AddLiteralSpec(name="path", value=pa.scalar(None, type=pa.string())),
                AddLiteralSpec(name="bstart", value=pa.scalar(None, type=pa.int64())),
                AddLiteralSpec(name="bend", value=pa.scalar(None, type=pa.int64())),
                AddLiteralSpec(name="confidence", value=pa.scalar(1.0, type=pa.float32())),
                AddLiteralSpec(name="score", value=pa.scalar(1.0, type=pa.float32())),
            ),
            confidence_policy=confidence_policy,
            edge=inferred_edge,
        ),
    )


def _runtime_template(spec: RuleTemplateSpec) -> tuple[RuleDefinitionSpec, ...]:
    signatures, params, members = _require_inputs(spec, count=3)
    confidence_policy = _param_str(spec, "confidence_policy")
    option_flag = _required_param_str(spec, "option_flag")
    base_kernels = (
        AddLiteralSpec(name="path", value=pa.scalar(None, type=pa.string())),
        AddLiteralSpec(name="bstart", value=pa.scalar(None, type=pa.int64())),
        AddLiteralSpec(name="bend", value=pa.scalar(None, type=pa.int64())),
        AddLiteralSpec(name="confidence", value=pa.scalar(1.0, type=pa.float32())),
        AddLiteralSpec(name="score", value=pa.scalar(1.0, type=pa.float32())),
    )
    return (
        RuleDefinitionSpec(
            name="runtime_signatures",
            kind=RuleKind.FILTER_PROJECT,
            inputs=(signatures,),
            project=ProjectConfig(select=("rt_id", "sig_id")),
            post_kernels=base_kernels,
            confidence_policy=confidence_policy,
            edge=EdgeDefinitionSpec(
                edge_kind=EdgeKind.RT_HAS_SIGNATURE,
                src_cols=("rt_id",),
                dst_cols=("sig_id",),
                origin="inspect",
                resolution_method="RUNTIME_INSPECT",
                option_flag=option_flag,
            ),
        ),
        RuleDefinitionSpec(
            name="runtime_params",
            kind=RuleKind.FILTER_PROJECT,
            inputs=(params,),
            project=ProjectConfig(select=("sig_id", "param_id")),
            post_kernels=base_kernels,
            confidence_policy=confidence_policy,
            edge=EdgeDefinitionSpec(
                edge_kind=EdgeKind.RT_HAS_PARAM,
                src_cols=("sig_id",),
                dst_cols=("param_id",),
                origin="inspect",
                resolution_method="RUNTIME_INSPECT",
                option_flag=option_flag,
            ),
        ),
        RuleDefinitionSpec(
            name="runtime_members",
            kind=RuleKind.FILTER_PROJECT,
            inputs=(members,),
            project=ProjectConfig(select=("rt_id", "member_id")),
            post_kernels=base_kernels,
            confidence_policy=confidence_policy,
            edge=EdgeDefinitionSpec(
                edge_kind=EdgeKind.RT_HAS_MEMBER,
                src_cols=("rt_id",),
                dst_cols=("member_id",),
                origin="inspect",
                resolution_method="RUNTIME_INSPECT",
                option_flag=option_flag,
            ),
        ),
    )


def _import_template(spec: RuleTemplateSpec) -> tuple[RuleDefinitionSpec, ...]:
    input_name = _require_inputs(spec, count=1)[0]
    confidence_policy = _param_str(spec, "confidence_policy")
    option_flag = _required_param_str(spec, "option_flag")
    return (
        RuleDefinitionSpec(
            name="import_edges",
            kind=RuleKind.FILTER_PROJECT,
            inputs=(input_name,),
            predicate=_bitmask_predicate("symbol_roles", SCIP_ROLE_IMPORT),
            project=ProjectConfig(
                select=(
                    "import_alias_id",
                    "import_id",
                    "symbol",
                    "path",
                    "bstart",
                    "bend",
                    "alias_bstart",
                    "alias_bend",
                    "symbol_roles",
                )
            ),
            confidence_policy=confidence_policy,
            edge=EdgeDefinitionSpec(
                edge_kind=EdgeKind.PY_IMPORTS_SYMBOL,
                src_cols=("import_alias_id", "import_id"),
                dst_cols=("symbol",),
                path_cols=("path",),
                bstart_cols=("bstart", "alias_bstart"),
                bend_cols=("bend", "alias_bend"),
                origin="scip",
                resolution_method="SPAN_EXACT",
                option_flag=option_flag,
            ),
        ),
    )


def _call_template(spec: RuleTemplateSpec) -> tuple[RuleDefinitionSpec, ...]:
    input_name = _require_inputs(spec, count=1)[0]
    confidence_policy = _param_str(spec, "confidence_policy")
    option_flag = _required_param_str(spec, "option_flag")
    return (
        RuleDefinitionSpec(
            name="call_edges",
            kind=RuleKind.FILTER_PROJECT,
            inputs=(input_name,),
            project=ProjectConfig(
                select=(
                    "call_id",
                    "symbol",
                    "path",
                    "call_bstart",
                    "call_bend",
                    "bstart",
                    "bend",
                )
            ),
            confidence_policy=confidence_policy,
            edge=EdgeDefinitionSpec(
                edge_kind=EdgeKind.PY_CALLS_SYMBOL,
                src_cols=("call_id",),
                dst_cols=("symbol",),
                path_cols=("path",),
                bstart_cols=("call_bstart", "bstart"),
                bend_cols=("call_bend", "bend"),
                origin="scip",
                resolution_method="CALLEE_SPAN_EXACT",
                option_flag=option_flag,
            ),
        ),
    )


def _diagnostic_template(spec: RuleTemplateSpec) -> tuple[RuleDefinitionSpec, ...]:
    input_name = _require_inputs(spec, count=1)[0]
    option_flag = _required_param_str(spec, "option_flag")
    project = ProjectConfig(
        select=("diag_id", "file_id", "path", "bstart", "bend", "severity"),
        exprs={
            "origin": _fill_null(_field("diag_source"), "diagnostic"),
            "confidence": _severity_score_expr(),
            "score": _severity_score_expr(),
        },
    )
    return (
        RuleDefinitionSpec(
            name="diagnostic_edges",
            kind=RuleKind.FILTER_PROJECT,
            inputs=(input_name,),
            project=project,
            edge=EdgeDefinitionSpec(
                edge_kind=EdgeKind.HAS_DIAGNOSTIC,
                src_cols=("file_id",),
                dst_cols=("diag_id",),
                origin="diagnostic",
                resolution_method="DIAGNOSTIC",
                option_flag=option_flag,
            ),
        ),
    )


TEMPLATE_FACTORIES: dict[str, TemplateFactory] = {
    "symbol_role": _symbol_role_template,
    "scip_symbol": _scip_symbol_template,
    "type": _type_template,
    "runtime": _runtime_template,
    "import": _import_template,
    "call": _call_template,
    "diagnostic": _diagnostic_template,
}


def expand_rule_templates(specs: Sequence[RuleTemplateSpec]) -> tuple[RuleDefinitionSpec, ...]:
    """Expand template specs into concrete rule definitions.

    Parameters
    ----------
    specs:
        Template specs to expand.

    Returns
    -------
    tuple[RuleDefinitionSpec, ...]
        Expanded rule definitions.

    Raises
    ------
    ValueError
        Raised when an unknown template factory is requested.
    """
    expanded: list[RuleDefinitionSpec] = []
    for spec in specs:
        factory = TEMPLATE_FACTORIES.get(spec.factory)
        if factory is None:
            msg = f"Unknown rule template factory: {spec.factory!r}."
            raise ValueError(msg)
        expanded.extend(factory(spec))
    return tuple(expanded)


__all__ = [
    "EdgeDefinitionSpec",
    "RuleDefinitionSpec",
    "RuleTemplateSpec",
    "expand_rule_templates",
]
