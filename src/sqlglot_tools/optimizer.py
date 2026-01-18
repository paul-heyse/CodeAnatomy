"""SQLGlot optimization helpers."""

from __future__ import annotations

import hashlib
import importlib
import json
import math
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Literal, TypedDict, Unpack, cast

from sqlglot import Dialect, ErrorLevel, Expression, exp, parse_one
from sqlglot.generator import Generator as SqlGlotGenerator
from sqlglot.optimizer import RULES, optimize
from sqlglot.optimizer.annotate_types import annotate_types
from sqlglot.optimizer.canonicalize import canonicalize
from sqlglot.optimizer.normalize import normalization_distance
from sqlglot.optimizer.normalize import normalize as normalize_predicates
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers
from sqlglot.optimizer.pushdown_predicates import pushdown_predicates
from sqlglot.optimizer.pushdown_projections import pushdown_projections
from sqlglot.optimizer.qualify import qualify
from sqlglot.optimizer.qualify_columns import (
    qualify_outputs,
    quote_identifiers,
    validate_qualify_columns,
)
from sqlglot.optimizer.simplify import simplify
from sqlglot.planner import Step
from sqlglot.serde import dump
from sqlglot.transforms import (
    eliminate_qualify,
    ensure_bools,
    move_ctes_to_top_level,
    unnest_to_explode,
)

SchemaMapping = Mapping[str, Mapping[str, str]]
SqlGlotRewriteLane = Literal["transforms", "dialect_shim"]


def _schema_requires_quoting(schema: SchemaMapping | None) -> bool:
    if not schema:
        return False
    for table_name, columns in schema.items():
        if table_name != table_name.lower():
            return True
        for column_name in columns:
            if column_name != column_name.lower():
                return True
    return False


class SqlGlotSurface(StrEnum):
    """Surface identifiers for SQLGlot compilation."""

    DATAFUSION_COMPILE = "datafusion_compile"
    DATAFUSION_DML = "datafusion_dml"
    DATAFUSION_EXTERNAL_TABLE = "datafusion_external_table"
    DATAFUSION_DIAGNOSTICS = "datafusion_diagnostics"


@dataclass(frozen=True)
class SqlGlotSurfacePolicy:
    """Policy for SQLGlot dialect selection per surface."""

    dialect: str
    lane: SqlGlotRewriteLane


_TEMPLATE_TOKEN = "__templated__"
_TEMPLATE_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\{\{.*?\}\}", flags=re.DOTALL),
    re.compile(r"\{%.+?%\}", flags=re.DOTALL),
    re.compile(r"\$\{.*?\}", flags=re.DOTALL),
)
_PARAM_PATTERN = re.compile(r":[A-Za-z_][A-Za-z0-9_]*")


class GeneratorInitKwargs(TypedDict, total=False):
    pretty: bool | None
    identify: bool | str
    normalize: bool
    pad: int
    indent: int
    normalize_functions: bool | str | None
    unsupported_level: ErrorLevel
    max_unsupported: int
    leading_comma: bool
    max_text_width: int
    comments: bool
    dialect: Dialect | str | type[Dialect] | None


@dataclass(frozen=True)
class CanonicalizationRules:
    """Rules for canonicalizing SQLGlot expressions."""

    column_renames: Mapping[str, str] = field(default_factory=dict)
    function_renames: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class SqlGlotPolicySnapshot:
    """Snapshot of SQLGlot compiler policy settings."""

    read_dialect: str
    write_dialect: str
    rules: tuple[str, ...]
    rules_hash: str
    generator: dict[str, object]
    normalization_distance: int
    error_level: str
    unsupported_level: str
    tokenizer_mode: str
    transforms: tuple[str, ...]
    identify: bool
    policy_hash: str

    def payload(self) -> dict[str, object]:
        """Return the policy payload for diagnostics.

        Returns
        -------
        dict[str, object]
            Serialized policy payload.
        """
        return {
            "read_dialect": self.read_dialect,
            "write_dialect": self.write_dialect,
            "rules": list(self.rules),
            "rules_hash": self.rules_hash,
            "generator": dict(self.generator),
            "normalization_distance": self.normalization_distance,
            "error_level": self.error_level,
            "unsupported_level": self.unsupported_level,
            "tokenizer_mode": self.tokenizer_mode,
            "transforms": list(self.transforms),
            "identify": self.identify,
            "policy_hash": self.policy_hash,
        }


@dataclass(frozen=True)
class PlannerDagSnapshot:
    """Snapshot of a SQLGlot planner DAG."""

    steps: tuple[Mapping[str, object], ...]
    edges: tuple[Mapping[str, str], ...]
    dag_hash: str

    def payload(self) -> dict[str, object]:
        """Return the planner DAG payload for diagnostics.

        Returns
        -------
        dict[str, object]
            Serialized planner DAG payload.
        """
        return {
            "steps": [dict(step) for step in self.steps],
            "edges": [dict(edge) for edge in self.edges],
            "dag_hash": self.dag_hash,
        }


@dataclass(frozen=True)
class SqlGlotPolicy:
    """SQLGlot policy configuration for compilation and normalization."""

    read_dialect: str
    write_dialect: str
    rules: Sequence[Callable[..., Expression]]
    generator: GeneratorInitKwargs
    normalization_distance: int
    error_level: ErrorLevel
    unsupported_level: ErrorLevel
    transforms: Sequence[Callable[[Expression], Expression]]
    expand_stars: bool = True
    validate_qualify_columns: bool = True
    identify: bool = False


class SqlGlotQualificationError(ValueError):
    """Error raised when SQLGlot qualification fails."""

    payload: dict[str, object]

    def __init__(self, message: str, payload: Mapping[str, object]) -> None:
        super().__init__(message)
        self.payload = dict(payload)


@dataclass(frozen=True)
class QualifyStrictOptions:
    """Options for strict qualification."""

    schema: SchemaMapping | None
    dialect: str
    sql: str | None = None
    expand_stars: bool = True
    validate_columns: bool = True
    identify: bool = False


@dataclass(frozen=True)
class NormalizeExprOptions:
    """Options for normalizing SQLGlot expressions."""

    schema: SchemaMapping | None = None
    rules: CanonicalizationRules | None = None
    rewrite_hook: Callable[[Expression], Expression] | None = None
    enable_rewrites: bool = True
    policy: SqlGlotPolicy | None = None
    sql: str | None = None


@dataclass(frozen=True)
class NormalizationStats:
    """Normalization stats for predicate normalization decisions."""

    distance: int
    max_distance: int
    applied: bool


@dataclass(frozen=True)
class NormalizeExprResult:
    """Normalized expression bundle with predicate normalization stats."""

    expr: Expression
    stats: NormalizationStats


DEFAULT_READ_DIALECT: str = "datafusion"
DEFAULT_WRITE_DIALECT: str = "datafusion_ext"
DEFAULT_NORMALIZATION_DISTANCE: int = 1000
DEFAULT_ERROR_LEVEL: ErrorLevel = ErrorLevel.IMMEDIATE
DEFAULT_UNSUPPORTED_LEVEL: ErrorLevel = ErrorLevel.RAISE
DEFAULT_OPTIMIZE_RULES: tuple[Callable[..., Expression], ...] = tuple(RULES)
DEFAULT_SURFACE_POLICIES: dict[SqlGlotSurface, SqlGlotSurfacePolicy] = {
    SqlGlotSurface.DATAFUSION_COMPILE: SqlGlotSurfacePolicy(
        dialect=DEFAULT_WRITE_DIALECT,
        lane="dialect_shim",
    ),
    SqlGlotSurface.DATAFUSION_DML: SqlGlotSurfacePolicy(
        dialect=DEFAULT_READ_DIALECT,
        lane="transforms",
    ),
    SqlGlotSurface.DATAFUSION_EXTERNAL_TABLE: SqlGlotSurfacePolicy(
        dialect=DEFAULT_WRITE_DIALECT,
        lane="dialect_shim",
    ),
    SqlGlotSurface.DATAFUSION_DIAGNOSTICS: SqlGlotSurfacePolicy(
        dialect=DEFAULT_WRITE_DIALECT,
        lane="dialect_shim",
    ),
}
DEFAULT_GENERATOR_KWARGS: GeneratorInitKwargs = {
    "pretty": False,
    "identify": False,
    "comments": False,
    "unsupported_level": DEFAULT_UNSUPPORTED_LEVEL,
}


def default_sqlglot_policy() -> SqlGlotPolicy:
    """Return the default SQLGlot policy configuration.

    Returns
    -------
    SqlGlotPolicy
        Default policy definition.
    """
    return SqlGlotPolicy(
        read_dialect=DEFAULT_READ_DIALECT,
        write_dialect=DEFAULT_WRITE_DIALECT,
        rules=DEFAULT_OPTIMIZE_RULES,
        generator=cast("GeneratorInitKwargs", dict(DEFAULT_GENERATOR_KWARGS)),
        normalization_distance=DEFAULT_NORMALIZATION_DISTANCE,
        error_level=DEFAULT_ERROR_LEVEL,
        unsupported_level=DEFAULT_UNSUPPORTED_LEVEL,
        transforms=(
            eliminate_qualify,
            move_ctes_to_top_level,
            unnest_to_explode,
            ensure_bools,
        ),
        expand_stars=True,
        validate_qualify_columns=True,
        identify=False,
    )


def sqlglot_surface_policy(surface: SqlGlotSurface) -> SqlGlotSurfacePolicy:
    """Return the SQLGlot policy for a compilation surface.

    Parameters
    ----------
    surface
        Compilation surface identifier.

    Returns
    -------
    SqlGlotSurfacePolicy
        Policy describing dialect selection and rewrite lane.

    Raises
    ------
    ValueError
        Raised when the surface identifier is unknown.
    """
    policy = DEFAULT_SURFACE_POLICIES.get(surface)
    if policy is None:
        msg = f"Unknown SQLGlot surface: {surface!r}."
        raise ValueError(msg)
    return policy


def sqlglot_policy_snapshot() -> SqlGlotPolicySnapshot:
    """Return the default SQLGlot policy snapshot.

    Returns
    -------
    SqlGlotPolicySnapshot
        Snapshot of compiler policy settings.
    """
    policy = default_sqlglot_policy()
    rules = tuple(_rule_name(rule) for rule in policy.rules)
    rules_hash = _hash_payload({"rules": rules})
    generator = dict(policy.generator)
    payload = {
        "read_dialect": policy.read_dialect,
        "write_dialect": policy.write_dialect,
        "rules": list(rules),
        "rules_hash": rules_hash,
        "generator": dict(generator),
        "normalization_distance": policy.normalization_distance,
        "error_level": policy.error_level.name,
        "unsupported_level": policy.unsupported_level.name,
        "tokenizer_mode": _tokenizer_mode(),
        "transforms": [_rule_name(transform) for transform in policy.transforms],
        "identify": policy.identify,
    }
    policy_hash = _hash_payload(payload)
    return SqlGlotPolicySnapshot(
        read_dialect=policy.read_dialect,
        write_dialect=policy.write_dialect,
        rules=rules,
        rules_hash=rules_hash,
        generator=generator,
        normalization_distance=policy.normalization_distance,
        error_level=policy.error_level.name,
        unsupported_level=policy.unsupported_level.name,
        tokenizer_mode=_tokenizer_mode(),
        transforms=tuple(_rule_name(transform) for transform in policy.transforms),
        identify=policy.identify,
        policy_hash=policy_hash,
    )


def _rule_name(rule: object) -> str:
    name = getattr(rule, "__name__", None)
    if isinstance(name, str):
        return name
    return rule.__class__.__name__


def _tokenizer_mode() -> str:
    try:
        importlib.import_module("sqlglot.rs")
    except ModuleNotFoundError:
        return "python"
    else:
        return "rust"


def _hash_payload(payload: Mapping[str, object]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _generator_kwargs(policy: SqlGlotPolicy) -> GeneratorInitKwargs:
    generator = dict(policy.generator)
    generator.pop("dialect", None)
    return cast("GeneratorInitKwargs", generator)


def canonical_ast_fingerprint(expr: Expression) -> str:
    """Return a stable fingerprint for a SQLGlot expression AST.

    Returns
    -------
    str
        SHA-256 fingerprint for the serialized AST payload.
    """
    payload = dump(expr)
    return _hash_payload({"ast": payload})


def apply_transforms(
    expr: Expression, *, transforms: Sequence[Callable[[Expression], Expression]]
) -> Expression:
    """Apply a sequence of SQLGlot transforms to an expression.

    Returns
    -------
    sqlglot.Expression
        Expression after applying each transform.
    """
    transformed = expr
    for transform in transforms:
        transformed = transform(transformed)
    return transformed


def sqlglot_sql(expr: Expression, *, policy: SqlGlotPolicy | None = None) -> str:
    """Return SQL text for an expression under a policy.

    Returns
    -------
    str
        SQL string generated with policy dialect and generator settings.
    """
    policy = policy or default_sqlglot_policy()
    generator = dict(_generator_kwargs(policy))
    generator["dialect"] = policy.write_dialect
    generator["unsupported_level"] = policy.unsupported_level
    return expr.sql(**cast("GeneratorInitKwargs", generator))


def qualify_strict(
    expr: Expression,
    *,
    options: QualifyStrictOptions,
) -> Expression:
    """Return a strictly qualified SQLGlot expression.

    Returns
    -------
    sqlglot.Expression
        Qualified expression with validated columns and aliased outputs.

    Raises
    ------
    SqlGlotQualificationError
        Raised when qualification fails or validation errors are detected.
    """
    schema_map = (
        cast("dict[object, object]", options.schema) if options.schema is not None else None
    )
    try:
        qualified = qualify(
            expr,
            schema=schema_map,
            dialect=options.dialect,
            expand_stars=options.expand_stars,
            validate_qualify_columns=options.validate_columns,
        )
        if options.validate_columns:
            validate_qualify_columns(qualified, sql=options.sql)
        qualify_outputs(qualified)
        return quote_identifiers(qualified, dialect=options.dialect, identify=options.identify)
    except SqlGlotQualificationError:
        raise
    except Exception as exc:
        payload = _qualification_failure_payload(expr, options=options, error=exc)
        msg = "SQLGlot qualification failed."
        raise SqlGlotQualificationError(msg, payload) from exc


def canonicalize_expr(
    expr: Expression, *, rules: CanonicalizationRules | None = None
) -> Expression:
    """Return a canonicalized SQLGlot expression.

    Applies deterministic renames for columns and functions when rules are provided.

    Returns
    -------
    sqlglot.Expression
        Canonicalized expression tree.
    """
    if rules is None:
        return expr
    rules_local = rules

    def _rewrite(node: Expression) -> Expression:
        if isinstance(node, exp.Column):
            target = rules_local.column_renames.get(node.name)
            if target is None:
                return node
            return exp.column(target, table=node.table)
        if isinstance(node, exp.Func):
            name = node.sql_name().lower()
            target = rules_local.function_renames.get(name)
            if target is None:
                return node
            return exp.Anonymous(this=target, expressions=list(node.expressions))
        return node

    return expr.transform(_rewrite)


def _array_transform(generator: SqlGlotGenerator, expression: Expression) -> str:
    return "ARRAY[" + generator.expressions(expression) + "]"


class DataFusionGenerator(SqlGlotGenerator):
    """SQL generator overrides for DataFusion."""

    def __init__(self, **kwargs: Unpack[GeneratorInitKwargs]) -> None:
        super().__init__(**kwargs)
        self.TRANSFORMS = {**self.TRANSFORMS, exp.Array: _array_transform}


class DataFusionDialect(Dialect):
    """Optional DataFusion-focused dialect overrides."""

    generator_class = DataFusionGenerator


def register_datafusion_dialect(name: str = "datafusion_ext") -> None:
    """Register the DataFusion dialect overrides under a custom name."""
    Dialect.classes[name] = DataFusionDialect


def sanitize_templated_sql(sql: str) -> str:
    """Return SQL text with templated segments replaced for parsing.

    Parameters
    ----------
    sql
        Raw SQL text that may contain templated segments.

    Returns
    -------
    str
        Sanitized SQL string suitable for parsing.
    """
    sanitized = sql
    for pattern in _TEMPLATE_PATTERNS:
        sanitized = pattern.sub(_TEMPLATE_TOKEN, sanitized)
    return _PARAM_PATTERN.sub(_TEMPLATE_TOKEN, sanitized)


def parse_sql_strict(
    sql: str,
    *,
    dialect: str,
    error_level: ErrorLevel | None = None,
) -> Expression:
    """Parse SQL with strict error handling and templated sanitization.

    Parameters
    ----------
    sql
        SQL text to parse.
    dialect
        SQLGlot dialect used for parsing.
    error_level
        Optional SQLGlot error level override.

    Returns
    -------
    sqlglot.Expression
        Parsed SQLGlot expression.
    """
    sanitized = sanitize_templated_sql(sql)
    level = error_level or DEFAULT_ERROR_LEVEL
    return parse_one(sanitized, dialect=dialect, error_level=level)


def qualify_expr(
    expr: Expression,
    *,
    schema: SchemaMapping | None = None,
    dialect: str | None = None,
) -> Expression:
    """Return a qualified SQLGlot expression.

    Returns
    -------
    sqlglot.Expression
        Qualified expression with fully qualified columns.
    """
    if schema is None:
        return qualify(expr, dialect=dialect)
    schema_map = cast("dict[object, object]", schema)
    return qualify(expr, schema=schema_map, dialect=dialect)


def optimize_expr(
    expr: Expression,
    *,
    schema: SchemaMapping | None = None,
    policy: SqlGlotPolicy | None = None,
) -> Expression:
    """Return an optimized SQLGlot expression.

    Returns
    -------
    sqlglot.Expression
        Optimized expression tree.
    """
    policy = policy or default_sqlglot_policy()
    schema_map = cast("dict[object, object]", schema) if schema is not None else None
    return optimize(
        expr,
        schema=schema_map,
        dialect=policy.read_dialect,
        rules=policy.rules,
    )


def normalize_expr(
    expr: Expression,
    *,
    options: NormalizeExprOptions,
) -> Expression:
    """Return a qualified + optimized SQLGlot expression.

    Returns
    -------
    sqlglot.Expression
        Normalized expression tree.
    """
    return normalize_expr_with_stats(expr, options=options).expr


def normalize_expr_with_stats(
    expr: Expression,
    *,
    options: NormalizeExprOptions,
) -> NormalizeExprResult:
    """Return a normalized expression along with normalization stats.

    Returns
    -------
    NormalizeExprResult
        Normalized expression and predicate normalization stats.
    """
    canonical = canonicalize_expr(expr, rules=options.rules)
    rewritten = rewrite_expr(
        canonical,
        rewrite_hook=options.rewrite_hook,
        enabled=options.enable_rewrites,
    )
    policy = options.policy or default_sqlglot_policy()
    identify = policy.identify or _schema_requires_quoting(options.schema)
    transformed = apply_transforms(rewritten, transforms=policy.transforms)
    qualify_options = QualifyStrictOptions(
        schema=options.schema,
        dialect=policy.read_dialect,
        sql=options.sql,
        expand_stars=policy.expand_stars,
        validate_columns=policy.validate_qualify_columns and options.schema is not None,
        identify=identify,
    )
    qualified = qualify_strict(transformed, options=qualify_options)
    normalized = normalize_identifiers(
        qualified,
        dialect=policy.read_dialect,
        store_original_column_identifiers=identify,
    )
    normalized = quote_identifiers(normalized, dialect=policy.read_dialect, identify=identify)
    annotated = annotate_types(
        normalized,
        schema=cast("dict[object, object]", options.schema) if options.schema else None,
        dialect=policy.read_dialect,
    )
    canonicalized = canonicalize(annotated, dialect=policy.read_dialect)
    simplified = simplify_expr(canonicalized, dialect=policy.read_dialect)
    normalized_predicates, stats = _normalize_predicates_with_stats(
        simplified,
        max_distance=policy.normalization_distance,
    )
    projected = pushdown_projections(
        normalized_predicates,
        schema=cast("dict[object, object]", options.schema) if options.schema else None,
        dialect=policy.read_dialect,
    )
    expr_out = pushdown_predicates(projected, dialect=policy.read_dialect)
    return NormalizeExprResult(expr=expr_out, stats=stats)


def simplify_expr(expr: Expression, *, dialect: str | None = None) -> Expression:
    """Return a simplified SQLGlot expression.

    Returns
    -------
    sqlglot.Expression
        Simplified expression tree.
    """
    return simplify(expr, dialect=dialect)


def _normalize_predicates_with_stats(
    expr: Expression, *, max_distance: int
) -> tuple[Expression, NormalizationStats]:
    distance = normalization_distance(expr, max_=max_distance)
    applied = distance <= max_distance
    if applied:
        return normalize_predicates(expr), NormalizationStats(
            distance=distance,
            max_distance=max_distance,
            applied=True,
        )
    return expr, NormalizationStats(
        distance=distance,
        max_distance=max_distance,
        applied=False,
    )


def rewrite_expr(
    expr: Expression,
    *,
    rewrite_hook: Callable[[Expression], Expression] | None = None,
    enabled: bool = True,
) -> Expression:
    """Return a SQLGlot expression after applying a rewrite hook.

    Returns
    -------
    sqlglot.Expression
        Rewritten expression tree.
    """
    if not enabled or rewrite_hook is None:
        return expr
    return rewrite_hook(expr)


def bind_params(expr: Expression, *, params: Mapping[str, object]) -> Expression:
    """Return a SQLGlot expression with parameter nodes bound to literals.

    Returns
    -------
    sqlglot.Expression
        Expression tree with parameters replaced by literals.
    """
    if not params:
        return expr

    def _rewrite(node: Expression) -> Expression:
        if isinstance(node, exp.Parameter):
            name = _param_name(node)
            if name not in params:
                msg = f"Missing parameter binding for {name!r}."
                raise KeyError(msg)
            return _literal_from_value(params[name])
        return node

    return expr.transform(_rewrite)


def _param_name(node: exp.Parameter) -> str:
    name = getattr(node, "name", None)
    if isinstance(name, str):
        return name
    value = node.this
    return value if isinstance(value, str) else str(value)


def _literal_from_value(value: object) -> Expression:
    if value is None:
        return exp.Null()
    if isinstance(value, bool):
        return exp.Boolean(this=value)
    if isinstance(value, int):
        return exp.Literal.number(value)
    if isinstance(value, float):
        return exp.Literal.number(value)
    return exp.Literal.string(str(value))


def plan_fingerprint(
    expr: Expression,
    *,
    dialect: str = "datafusion_ext",
    policy_hash: str | None = None,
) -> str:
    """Return a stable fingerprint for a SQLGlot expression.

    Returns
    -------
    str
        SHA-256 fingerprint for the canonical SQL representation.
    """
    if policy_hash is None:
        policy_hash = sqlglot_policy_snapshot().policy_hash
    payload = {"dialect": dialect, "ast": dump(expr), "policy_hash": policy_hash}
    return _hash_payload(payload)


def planner_dag_snapshot(
    expr: Expression,
    *,
    dialect: str = DEFAULT_WRITE_DIALECT,
) -> PlannerDagSnapshot:
    """Return a stable snapshot of the SQLGlot planner DAG.

    Parameters
    ----------
    expr
        SQLGlot expression to plan.
    dialect
        Dialect used for canonicalization prior to planning.

    Returns
    -------
    PlannerDagSnapshot
        Planner DAG snapshot with stable step identifiers.
    """
    canonical = canonicalize(expr, dialect=dialect)
    root = Step.from_expression(canonical)
    steps = _collect_planner_steps(root)
    step_payloads: dict[Step, dict[str, object]] = {
        step: _planner_step_payload(step) for step in steps
    }
    step_ids = {step: _hash_payload(payload) for step, payload in step_payloads.items()}
    step_rows = [
        {
            "step_id": step_ids[step],
            "kind": payload.get("kind"),
            "name": payload.get("name"),
            "payload": payload,
        }
        for step, payload in step_payloads.items()
    ]
    edge_rows = [
        {"source": step_ids[dependency], "target": step_ids[step]}
        for step in steps
        for dependency in step.dependencies
        if dependency in step_ids
    ]
    step_rows_sorted = tuple(sorted(step_rows, key=lambda row: str(row["step_id"])))
    edge_rows_sorted = tuple(sorted(edge_rows, key=lambda row: (row["source"], row["target"])))
    dag_hash = _hash_payload({"steps": step_rows_sorted, "edges": edge_rows_sorted})
    return PlannerDagSnapshot(
        steps=step_rows_sorted,
        edges=edge_rows_sorted,
        dag_hash=dag_hash,
    )


def _collect_planner_steps(root: Step) -> list[Step]:
    seen: set[Step] = set()
    stack = [root]
    while stack:
        step = stack.pop()
        if step in seen:
            continue
        seen.add(step)
        stack.extend(dependency for dependency in step.dependencies if dependency not in seen)
    return list(seen)


def _planner_step_payload(step: Step) -> dict[str, object]:
    payload: dict[str, object] = {"kind": step.__class__.__name__}
    for key, value in step.__dict__.items():
        if key in {"dependencies", "dependents"}:
            continue
        payload[key] = _normalize_planner_value(value)
    return payload


def _normalize_planner_value(value: object) -> object:
    if isinstance(value, Expression):
        normalized: object = dump(value)
    elif isinstance(value, float) and not math.isfinite(value):
        normalized = str(value)
    elif isinstance(value, Mapping):
        normalized = {str(key): _normalize_planner_value(item) for key, item in value.items()}
    elif isinstance(value, (list, tuple, set)):
        items = [_normalize_planner_value(item) for item in value]
        normalized = sorted(items, key=_planner_value_sort_key) if isinstance(value, set) else items
    elif isinstance(value, Step):
        normalized = {"kind": value.__class__.__name__, "name": getattr(value, "name", None)}
    else:
        normalized = value
    return normalized


def _planner_value_sort_key(value: object) -> str:
    try:
        return json.dumps(
            value,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )
    except TypeError:
        return str(value)


def _qualification_failure_payload(
    expr: Expression,
    *,
    options: QualifyStrictOptions,
    error: Exception,
) -> dict[str, object]:
    schema = (
        {name: dict(values) for name, values in options.schema.items()}
        if options.schema is not None
        else None
    )
    return {
        "error": str(error),
        "error_type": error.__class__.__name__,
        "sql": options.sql,
        "dialect": options.dialect,
        "schema": schema,
        "expand_stars": options.expand_stars,
        "validate_columns": options.validate_columns,
        "identify": options.identify,
        "ast_payload": dump(expr),
    }


__all__ = [
    "CanonicalizationRules",
    "DataFusionDialect",
    "NormalizationStats",
    "NormalizeExprOptions",
    "NormalizeExprResult",
    "PlannerDagSnapshot",
    "QualifyStrictOptions",
    "SchemaMapping",
    "SqlGlotPolicy",
    "SqlGlotPolicySnapshot",
    "SqlGlotQualificationError",
    "SqlGlotRewriteLane",
    "SqlGlotSurface",
    "SqlGlotSurfacePolicy",
    "apply_transforms",
    "bind_params",
    "canonical_ast_fingerprint",
    "canonicalize_expr",
    "default_sqlglot_policy",
    "normalize_expr",
    "normalize_expr_with_stats",
    "optimize_expr",
    "parse_sql_strict",
    "plan_fingerprint",
    "planner_dag_snapshot",
    "qualify_expr",
    "qualify_strict",
    "register_datafusion_dialect",
    "rewrite_expr",
    "sanitize_templated_sql",
    "simplify_expr",
    "sqlglot_policy_snapshot",
    "sqlglot_sql",
    "sqlglot_surface_policy",
]
