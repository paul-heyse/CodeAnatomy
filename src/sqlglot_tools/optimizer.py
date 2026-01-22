"""SQLGlot optimization helpers."""

from __future__ import annotations

import hashlib
import importlib
import json
import math
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from enum import StrEnum
from typing import Final, Literal, TypedDict, Unpack, cast

import pyarrow as pa
import sqlglot
from sqlglot.errors import SqlglotError
from sqlglot.generator import Generator as SqlGlotGenerator
from sqlglot.optimizer import RULES, optimize
from sqlglot.optimizer.annotate_types import annotate_types
from sqlglot.optimizer.canonicalize import canonicalize
from sqlglot.optimizer.normalize import normalization_distance
from sqlglot.optimizer.normalize import normalize as normalize_predicates
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers
from sqlglot.optimizer.qualify import qualify
from sqlglot.optimizer.qualify_columns import (
    qualify_outputs,
    quote_identifiers,
    validate_qualify_columns,
)
from sqlglot.optimizer.simplify import simplify
from sqlglot.planner import Step
from sqlglot.schema import Schema as SqlGlotSchema
from sqlglot.schema import ensure_schema
from sqlglot.serde import dump, load
from sqlglot.transforms import (
    eliminate_full_outer_join,
    eliminate_qualify,
    eliminate_semi_and_anti_joins,
    ensure_bools,
    explode_projection_to_unnest,
    move_ctes_to_top_level,
    preprocess,
    unnest_to_explode,
)

from arrowdsl.io.ipc import payload_hash
from sqlglot_tools.compat import Dialect, DialectType, ErrorLevel, Expression, exp, parse_one

type SchemaMappingNode = Mapping[str, str] | Mapping[str, SchemaMappingNode]
type SchemaMapping = Mapping[str, SchemaMappingNode]
SqlGlotRewriteLane = Literal["transforms", "dialect_shim"]


def _ensure_mapping_schema(
    schema: SchemaMapping | None,
    *,
    dialect: str | None,
) -> SqlGlotSchema | None:
    if schema is None:
        return None
    schema_map = cast("dict[object, object]", schema)
    return ensure_schema(schema_map, dialect=dialect)


def _schema_requires_quoting(schema: SchemaMapping | None) -> bool:
    if not schema:
        return False
    return _schema_contains_uppercase(schema)


def _schema_contains_uppercase(schema: SchemaMappingNode) -> bool:
    for key, value in schema.items():
        if isinstance(key, str) and key != key.lower():
            return True
        if isinstance(value, Mapping):
            if _is_leaf_schema(value):
                for column_name in value:
                    if isinstance(column_name, str) and column_name != column_name.lower():
                        return True
                continue
            if _schema_contains_uppercase(value):
                return True
    return False


def _is_leaf_schema(value: SchemaMappingNode) -> bool:
    if not value:
        return False
    return all(isinstance(dtype, str) for dtype in value.values())


class SqlGlotSurface(StrEnum):
    """Surface identifiers for SQLGlot compilation."""

    DATAFUSION_COMPILE = "datafusion_compile"
    DATAFUSION_DML = "datafusion_dml"
    DATAFUSION_EXTERNAL_TABLE = "datafusion_external_table"
    DATAFUSION_DIAGNOSTICS = "datafusion_diagnostics"
    DATAFUSION_DDL = "datafusion_ddl"


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

HASH_PAYLOAD_VERSION: int = 1

_MAP_ENTRY_SCHEMA = pa.struct(
    [
        pa.field("key", pa.string()),
        pa.field("value_kind", pa.string()),
        pa.field("value", pa.string()),
    ]
)
_RULES_HASH_SCHEMA = pa.schema(
    [
        pa.field("version", pa.int32()),
        pa.field("rules", pa.list_(pa.string())),
    ]
)
_POLICY_HASH_SCHEMA = pa.schema(
    [
        pa.field("version", pa.int32()),
        pa.field("read_dialect", pa.string()),
        pa.field("write_dialect", pa.string()),
        pa.field("rules", pa.list_(pa.string())),
        pa.field("rules_hash", pa.string()),
        pa.field("generator", pa.list_(_MAP_ENTRY_SCHEMA)),
        pa.field("normalization_distance", pa.int64()),
        pa.field("error_level", pa.string()),
        pa.field("unsupported_level", pa.string()),
        pa.field("tokenizer_mode", pa.string()),
        pa.field("transforms", pa.list_(pa.string())),
        pa.field("identify", pa.bool_()),
    ]
)
_AST_HASH_SCHEMA = pa.schema(
    [
        pa.field("version", pa.int32()),
        pa.field("dialect", pa.string()),
        pa.field("sql", pa.string()),
    ]
)
_PLAN_HASH_SCHEMA = pa.schema(
    [
        pa.field("version", pa.int32()),
        pa.field("dialect", pa.string()),
        pa.field("sql", pa.string()),
        pa.field("policy_hash", pa.string()),
        pa.field("schema_map_hash", pa.string()),
    ]
)
_STEP_HASH_SCHEMA = pa.schema(
    [
        pa.field("version", pa.int32()),
        pa.field("entries", pa.list_(_MAP_ENTRY_SCHEMA)),
    ]
)
_EDGE_SCHEMA = pa.struct(
    [
        pa.field("source", pa.string()),
        pa.field("target", pa.string()),
    ]
)
_DAG_HASH_SCHEMA = pa.schema(
    [
        pa.field("version", pa.int32()),
        pa.field("steps", pa.list_(pa.string())),
        pa.field("edges", pa.list_(_EDGE_SCHEMA)),
    ]
)
SCHEMA_MAP_HASH_VERSION: int = 1
_SCHEMA_MAP_COLUMN_SCHEMA = pa.struct(
    [
        pa.field("name", pa.string()),
        pa.field("dtype", pa.string()),
    ]
)
_SCHEMA_MAP_ENTRY_SCHEMA = pa.struct(
    [
        pa.field("table", pa.string()),
        pa.field("columns", pa.list_(_SCHEMA_MAP_COLUMN_SCHEMA)),
    ]
)
_SCHEMA_MAP_HASH_SCHEMA = pa.schema(
    [
        pa.field("version", pa.int32()),
        pa.field("entries", pa.list_(_SCHEMA_MAP_ENTRY_SCHEMA)),
    ]
)


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
    dialect: DialectType | str | type[DialectType] | None


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
class SqlGlotCompileOptions:
    """Options for compiling SQLGlot expressions."""

    schema: SchemaMapping | None = None
    rules: CanonicalizationRules | None = None
    rewrite_hook: Callable[[Expression], Expression] | None = None
    enable_rewrites: bool = True
    policy: SqlGlotPolicy | None = None
    sql: str | None = None


@dataclass(frozen=True)
class ParseSqlOptions:
    """Options for parsing SQL text."""

    dialect: str
    error_level: ErrorLevel | None = None
    sanitize_templated: bool = True
    preserve_params: bool = False


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


@dataclass(frozen=True)
class AstArtifact:
    """SQLGlot AST artifact with serialization metadata."""

    sql: str
    ast_json: str
    policy_hash: str

    def to_dict(self) -> dict[str, str]:
        """Return dictionary representation for serialization.

        Returns
        -------
        dict[str, str]
            Dictionary with sql, ast_json, and policy_hash fields.
        """
        return {
            "sql": self.sql,
            "ast_json": self.ast_json,
            "policy_hash": self.policy_hash,
        }


@dataclass(frozen=True)
class PreflightResult:
    """Result of SQL preflight qualification."""

    original_sql: str
    qualified_expr: Expression | None
    annotated_expr: Expression | None
    canonicalized_expr: Expression | None
    errors: tuple[str, ...]
    warnings: tuple[str, ...]
    policy_hash: str | None
    schema_map_hash: str | None


DEFAULT_CANONICAL_DIALECT: str = "duckdb"
DEFAULT_READ_DIALECT: str = DEFAULT_CANONICAL_DIALECT
DEFAULT_WRITE_DIALECT: str = DEFAULT_CANONICAL_DIALECT
DEFAULT_NORMALIZATION_DISTANCE: int = 1000
DEFAULT_ERROR_LEVEL: ErrorLevel = ErrorLevel.IMMEDIATE
DEFAULT_UNSUPPORTED_LEVEL: ErrorLevel = ErrorLevel.RAISE
DEFAULT_OPTIMIZE_RULES: tuple[Callable[..., Expression], ...] = tuple(RULES)
DEFAULT_SURFACE_POLICIES: dict[SqlGlotSurface, SqlGlotSurfacePolicy] = {
    SqlGlotSurface.DATAFUSION_COMPILE: SqlGlotSurfacePolicy(
        dialect="datafusion_ext",
        lane="dialect_shim",
    ),
    SqlGlotSurface.DATAFUSION_DML: SqlGlotSurfacePolicy(
        dialect="datafusion_ext",
        lane="transforms",
    ),
    SqlGlotSurface.DATAFUSION_EXTERNAL_TABLE: SqlGlotSurfacePolicy(
        dialect="datafusion_ext",
        lane="dialect_shim",
    ),
    SqlGlotSurface.DATAFUSION_DIAGNOSTICS: SqlGlotSurfacePolicy(
        dialect="datafusion_ext",
        lane="dialect_shim",
    ),
    SqlGlotSurface.DATAFUSION_DDL: SqlGlotSurfacePolicy(
        dialect="datafusion_ext",
        lane="dialect_shim",
    ),
}
DEFAULT_GENERATOR_KWARGS: GeneratorInitKwargs = {
    "pretty": False,
    "identify": False,
    "comments": False,
    "unsupported_level": DEFAULT_UNSUPPORTED_LEVEL,
}
MAP_EXTRACT_ARG_COUNT = 2


def _rewrite_full_outer_join(expr: Expression) -> Expression:
    return eliminate_full_outer_join(expr)


def _rewrite_semi_anti_join(expr: Expression) -> Expression:
    return eliminate_semi_and_anti_joins(expr)


def _rewrite_explode_projection(expr: Expression) -> Expression:
    return explode_projection_to_unnest()(expr)


def _rewrite_map_access(expr: Expression) -> Expression:
    def _rewrite(node: Expression) -> Expression:
        if isinstance(node, exp.Anonymous) and node.name.lower() == "map_extract":
            args = list(node.expressions)
            if len(args) == MAP_EXTRACT_ARG_COUNT:
                return exp.Anonymous(
                    this="list_extract",
                    expressions=[node.copy(), exp.Literal.number(1)],
                )
        if isinstance(node, exp.Bracket):
            base = node.this
            keys = list(node.expressions)
            if len(keys) == 1:
                key = keys[0]
                if isinstance(key, exp.Literal) and key.is_string:
                    map_extract = exp.Anonymous(
                        this="map_extract",
                        expressions=[base.copy(), key.copy()],
                    )
                    return exp.Anonymous(
                        this="list_extract",
                        expressions=[map_extract, exp.Literal.number(1)],
                    )
        return node

    return expr.transform(_rewrite)


def _rewrite_span_named_struct(expr: Expression) -> Expression:
    def _rewrite(node: Expression) -> Expression:
        if not isinstance(node, exp.Anonymous) or node.name.lower() != "named_struct":
            return node
        parts = list(node.expressions)
        ordered = _span_struct_order(parts)
        if ordered is None:
            return node
        return exp.Anonymous(this="named_struct", expressions=ordered)

    return expr.transform(_rewrite)


def _span_struct_order(parts: Sequence[Expression]) -> list[Expression] | None:
    if len(parts) % 2:
        return None
    pairs = _span_struct_pairs(parts)
    if pairs is None:
        return None
    keys = {name for name, _, _ in pairs}
    if not {"bstart", "bend"}.issubset(keys):
        return None
    return _span_struct_ordered_pairs(pairs)


def _span_struct_pairs(
    parts: Sequence[Expression],
) -> list[tuple[str, Expression, Expression]] | None:
    pairs: list[tuple[str, Expression, Expression]] = []
    for idx in range(0, len(parts), 2):
        key_expr = parts[idx]
        value_expr = parts[idx + 1]
        if not isinstance(key_expr, exp.Literal) or not key_expr.is_string:
            return None
        pairs.append((key_expr.this, key_expr, value_expr))
    return pairs


def _span_struct_ordered_pairs(
    pairs: Sequence[tuple[str, Expression, Expression]],
) -> list[Expression]:
    ordered: list[Expression] = []
    for key in ("bstart", "bend"):
        for name, key_expr, value_expr in pairs:
            if name == key:
                ordered.extend([key_expr, value_expr])
    for name, key_expr, value_expr in pairs:
        if name in {"bstart", "bend"}:
            continue
        ordered.extend([key_expr, value_expr])
    return ordered


def _normalize_table_aliases(expr: Expression) -> Expression:
    def _rewrite(node: Expression) -> Expression:
        if isinstance(node, exp.Table):
            alias = node.args.get("alias")
            if isinstance(alias, exp.Identifier):
                node.set("alias", exp.TableAlias(this=alias))
            elif isinstance(alias, str):
                node.set("alias", exp.TableAlias(this=exp.to_identifier(alias)))
        return node

    return expr.transform(_rewrite)


def default_sqlglot_policy() -> SqlGlotPolicy:
    """Return the default SQLGlot policy configuration.

    Returns
    -------
    SqlGlotPolicy
        Default policy definition.
    """
    register_datafusion_dialect()
    return SqlGlotPolicy(
        read_dialect=DEFAULT_READ_DIALECT,
        write_dialect=DEFAULT_WRITE_DIALECT,
        rules=DEFAULT_OPTIMIZE_RULES,
        generator=cast("GeneratorInitKwargs", dict(DEFAULT_GENERATOR_KWARGS)),
        normalization_distance=DEFAULT_NORMALIZATION_DISTANCE,
        error_level=DEFAULT_ERROR_LEVEL,
        unsupported_level=DEFAULT_UNSUPPORTED_LEVEL,
        transforms=(
            _rewrite_full_outer_join,
            _rewrite_semi_anti_join,
            eliminate_qualify,
            move_ctes_to_top_level,
            _rewrite_explode_projection,
            _rewrite_map_access,
            _rewrite_span_named_struct,
            unnest_to_explode,
            ensure_bools,
        ),
        expand_stars=True,
        validate_qualify_columns=True,
        identify=False,
    )


def _surface_policy_factory(surface: SqlGlotSurface) -> Callable[[], SqlGlotPolicy]:
    def _factory() -> SqlGlotPolicy:
        base = default_sqlglot_policy()
        surface_policy = sqlglot_surface_policy(surface)
        return replace(
            base,
            read_dialect=surface_policy.dialect,
            write_dialect=surface_policy.dialect,
        )

    return _factory


_SQLGLOT_POLICY_PRESETS: Final[Mapping[str, Callable[[], SqlGlotPolicy]]] = {
    "default": default_sqlglot_policy,
    "datafusion_compile": _surface_policy_factory(SqlGlotSurface.DATAFUSION_COMPILE),
    "datafusion_dml": _surface_policy_factory(SqlGlotSurface.DATAFUSION_DML),
    "datafusion_external_table": _surface_policy_factory(SqlGlotSurface.DATAFUSION_EXTERNAL_TABLE),
    "datafusion_diagnostics": _surface_policy_factory(SqlGlotSurface.DATAFUSION_DIAGNOSTICS),
    "datafusion_ddl": _surface_policy_factory(SqlGlotSurface.DATAFUSION_DDL),
}


def sqlglot_policy_by_name(name: str) -> SqlGlotPolicy:
    """Return a SQLGlot policy preset by name.

    Parameters
    ----------
    name
        Named policy preset.

    Returns
    -------
    SqlGlotPolicy
        Resolved policy instance.

    Raises
    ------
    ValueError
        Raised when the policy name is not recognized.
    """
    normalized = name.strip().lower()
    factory = _SQLGLOT_POLICY_PRESETS.get(normalized)
    if factory is None:
        msg = f"Unknown SQLGlot policy name: {name!r}."
        raise ValueError(msg)
    return factory()


def resolve_sqlglot_policy(
    *,
    name: str | None = None,
    policy: SqlGlotPolicy | None = None,
) -> SqlGlotPolicy:
    """Resolve a SQLGlot policy from explicit or named overrides.

    Parameters
    ----------
    name
        Optional preset name for resolving a policy.
    policy
        Explicit policy override.

    Returns
    -------
    SqlGlotPolicy
        Resolved policy instance.
    """
    if policy is not None:
        return policy
    if name:
        return sqlglot_policy_by_name(name)
    return default_sqlglot_policy()


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


def sqlglot_policy_snapshot_for(policy: SqlGlotPolicy) -> SqlGlotPolicySnapshot:
    """Return a SQLGlot policy snapshot for a provided policy.

    Returns
    -------
    SqlGlotPolicySnapshot
        Snapshot of compiler policy settings.
    """
    rules = tuple(_rule_name(rule) for rule in policy.rules)
    rules_hash = payload_hash(
        {"version": HASH_PAYLOAD_VERSION, "rules": list(rules)},
        _RULES_HASH_SCHEMA,
    )
    generator = dict(policy.generator)
    payload = {
        "version": HASH_PAYLOAD_VERSION,
        "read_dialect": policy.read_dialect,
        "write_dialect": policy.write_dialect,
        "rules": list(rules),
        "rules_hash": rules_hash,
        "generator": _map_entries(generator),
        "normalization_distance": policy.normalization_distance,
        "error_level": policy.error_level.name,
        "unsupported_level": policy.unsupported_level.name,
        "tokenizer_mode": _tokenizer_mode(),
        "transforms": [_rule_name(transform) for transform in policy.transforms],
        "identify": policy.identify,
    }
    policy_hash = payload_hash(payload, _POLICY_HASH_SCHEMA)
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


def sqlglot_policy_snapshot() -> SqlGlotPolicySnapshot:
    """Return the default SQLGlot policy snapshot.

    Returns
    -------
    SqlGlotPolicySnapshot
        Snapshot of compiler policy settings.
    """
    return sqlglot_policy_snapshot_for(default_sqlglot_policy())


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


def _map_entries(payload: Mapping[str, object]) -> list[dict[str, object]]:
    return [
        _map_entry(key, value)
        for key, value in sorted(payload.items(), key=lambda item: str(item[0]))
    ]


def _map_entry(key: object, value: object) -> dict[str, object]:
    return {
        "key": str(key),
        "value_kind": _value_kind(value),
        "value": _value_text(value),
    }


def _value_kind(value: object) -> str:
    kind = "string"
    if value is None:
        kind = "null"
    elif isinstance(value, bool):
        kind = "bool"
    elif isinstance(value, int):
        kind = "int64"
    elif isinstance(value, float):
        kind = "float64"
    elif isinstance(value, bytes):
        kind = "binary"
    return kind


def _value_text(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, str):
        return value
    if isinstance(value, (bool, int, float)):
        return str(value)
    return _stable_repr(value)


def _stable_repr(value: object) -> str:
    if isinstance(value, Mapping):
        items = ", ".join(
            f"{_stable_repr(key)}:{_stable_repr(val)}"
            for key, val in sorted(value.items(), key=lambda item: str(item[0]))
        )
        return f"{{{items}}}"
    if isinstance(value, (list, tuple, set)):
        rendered = [_stable_repr(item) for item in value]
        if isinstance(value, set):
            rendered = sorted(rendered)
        items = ", ".join(rendered)
        bracket = "()" if isinstance(value, tuple) else "[]"
        return f"{bracket[0]}{items}{bracket[1]}"
    return repr(value)


def _generator_kwargs(policy: SqlGlotPolicy) -> GeneratorInitKwargs:
    generator = dict(policy.generator)
    generator.pop("dialect", None)
    return cast("GeneratorInitKwargs", generator)


def canonical_ast_fingerprint(expr: Expression) -> str:
    """Return a stable fingerprint for a SQLGlot expression.

    Uses SQLGlot's serde.dump to serialize the AST to JSON,
    then hashes for a stable fingerprint.

    Returns
    -------
    str
        SHA-256 fingerprint for the canonical AST.
    """
    try:
        payload = dump(expr)
        serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(serialized.encode()).hexdigest()
    except (SqlglotError, TypeError, ValueError):
        # Fallback to SQL text hash
        sql_text = expr.sql(dialect=DEFAULT_WRITE_DIALECT)
        payload_dict = {
            "version": HASH_PAYLOAD_VERSION,
            "dialect": DEFAULT_WRITE_DIALECT,
            "sql": sql_text,
        }
        return payload_hash(payload_dict, _AST_HASH_SCHEMA)


def schema_map_fingerprint_from_mapping(mapping: SchemaMapping) -> str:
    """Return a stable fingerprint for a schema mapping.

    Returns
    -------
    str
        SHA-256 fingerprint for the schema mapping payload.
    """
    flat = _flatten_schema_mapping(mapping)
    entries: list[dict[str, object]] = []
    for table_name, columns in sorted(flat.items(), key=lambda item: item[0]):
        column_entries = [
            {"name": name, "dtype": dtype}
            for name, dtype in sorted(columns.items(), key=lambda item: item[0])
        ]
        entries.append({"table": table_name, "columns": column_entries})
    payload = {"version": SCHEMA_MAP_HASH_VERSION, "entries": entries}
    return payload_hash(payload, _SCHEMA_MAP_HASH_SCHEMA)


def _is_leaf_schema_node(node: SchemaMappingNode) -> bool:
    return all(not isinstance(value, Mapping) for value in node.values())


def _flatten_schema_mapping(mapping: SchemaMapping) -> dict[str, dict[str, str]]:
    flat: dict[str, dict[str, str]] = {}

    def _visit(node: SchemaMappingNode, path: list[str]) -> None:
        if _is_leaf_schema_node(node):
            table_key = ".".join(path)
            flat[table_key] = {str(key): str(value) for key, value in node.items()}
            return
        for key, value in node.items():
            if not isinstance(value, Mapping):
                continue
            _visit(value, [*path, str(key)])

    for key, value in mapping.items():
        if not isinstance(value, Mapping):
            continue
        _visit(value, [str(key)])
    return flat


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


def sqlglot_sql(
    expr: Expression,
    *,
    policy: SqlGlotPolicy | None = None,
    pretty: bool | None = None,
) -> str:
    """Return SQL text for an expression under a policy.

    Returns
    -------
    str
        SQL string generated with policy dialect and generator settings.
    """
    policy = policy or default_sqlglot_policy()
    return sqlglot_emit(expr, policy=policy, pretty=pretty)


def sqlglot_emit(
    expr: Expression,
    *,
    policy: SqlGlotPolicy,
    pretty: bool | None = None,
) -> str:
    """Emit SQL from an expression using the policy generator settings.

    Returns
    -------
    str
        SQL string emitted using the policy generator.
    """
    generator = dict(_generator_kwargs(policy))
    if pretty is not None:
        generator["pretty"] = pretty
    generator["dialect"] = policy.write_dialect
    generator["identify"] = policy.identify
    generator["unsupported_level"] = policy.unsupported_level
    return expr.sql(**cast("GeneratorInitKwargs", generator))


def transpile_sql(
    sql: str,
    *,
    policy: SqlGlotPolicy | None = None,
) -> str:
    """Return transpiled SQL using policy read/write dialects.

    Returns
    -------
    str
        SQL string transpiled into the policy write dialect.

    Raises
    ------
    TypeError
        Raised when SQLGlot transpile is unavailable or returns invalid output.
    ValueError
        Raised when transpilation fails or yields multiple statements.
    """
    policy = policy or default_sqlglot_policy()
    transpile = getattr(sqlglot, "transpile", None)
    if not callable(transpile):
        msg = "SQLGlot transpile is unavailable."
        raise TypeError(msg)
    try:
        results = transpile(
            sql,
            read=policy.read_dialect,
            write=policy.write_dialect,
            error_level=policy.error_level,
        )
    except (SqlglotError, TypeError, ValueError) as exc:
        msg = f"SQLGlot transpile failed: {exc}"
        raise ValueError(msg) from exc
    if not isinstance(results, list):
        msg = "SQLGlot transpile returned non-list results."
        raise TypeError(msg)
    if not all(isinstance(item, str) for item in results):
        msg = "SQLGlot transpile returned non-string statements."
        raise TypeError(msg)
    if not results:
        msg = "SQLGlot transpile returned no statements."
        raise ValueError(msg)
    if len(results) > 1:
        msg = "SQLGlot transpile returned multiple statements."
        raise ValueError(msg)
    return results[0]


def normalize_ddl_sql(sql: str, *, surface: SqlGlotSurface = SqlGlotSurface.DATAFUSION_DDL) -> str:
    """Return normalized DDL SQL for a given surface policy.

    Returns
    -------
    str
        SQL string normalized under the surface dialect policy.
    """
    policy = default_sqlglot_policy()
    surface_policy = sqlglot_surface_policy(surface)
    dialect = surface_policy.dialect
    expr = parse_sql_strict(sql, dialect=dialect, error_level=policy.error_level)
    transformed = apply_transforms(expr, transforms=policy.transforms)
    policy = replace(policy, read_dialect=dialect, write_dialect=dialect)
    return sqlglot_sql(transformed, policy=policy)


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
    schema_map = _ensure_mapping_schema(options.schema, dialect=options.dialect)
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
    except (SqlglotError, TypeError, ValueError) as exc:
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


EXTERNAL_TABLE_COMPRESSION_ARG_TYPES: Final[dict[str, bool]] = {"this": True}
EXTERNAL_TABLE_ORDER_ARG_TYPES: Final[dict[str, bool]] = {"expressions": True}
EXTERNAL_TABLE_OPTIONS_ARG_TYPES: Final[dict[str, bool]] = {"expressions": True}


class ExternalTableCompressionProperty(exp.Property):
    """Property expression for DataFusion external table compression."""

    arg_types: dict[str, bool] = EXTERNAL_TABLE_COMPRESSION_ARG_TYPES


class ExternalTableOrderProperty(exp.Property):
    """Property expression for DataFusion external table ordering."""

    arg_types: dict[str, bool] = EXTERNAL_TABLE_ORDER_ARG_TYPES


class ExternalTableOptionsProperty(exp.Property):
    """Property expression for DataFusion external table options."""

    arg_types: dict[str, bool] = EXTERNAL_TABLE_OPTIONS_ARG_TYPES


def _file_format_property_sql(
    generator: SqlGlotGenerator,
    expression: exp.FileFormatProperty,
) -> str:
    return f"STORED AS {generator.sql(expression, 'this')}"


def _partitioned_by_property_sql(
    generator: SqlGlotGenerator,
    expression: exp.PartitionedByProperty,
) -> str:
    return f"PARTITIONED BY {generator.sql(expression, 'this')}"


def _external_table_order_sql(
    generator: SqlGlotGenerator,
    expression: ExternalTableOrderProperty,
) -> str:
    order_exprs = generator.expressions(expression, indent=False)
    return f"WITH ORDER ({order_exprs})"


def _external_table_options_sql(
    generator: SqlGlotGenerator,
    expression: ExternalTableOptionsProperty,
) -> str:
    rendered: list[str] = []
    for entry in expression.expressions:
        if isinstance(entry, exp.Tuple):
            parts = [generator.sql(part) for part in entry.expressions]
            rendered.append(" ".join(parts))
        else:
            rendered.append(generator.sql(entry))
    return f"OPTIONS ({', '.join(rendered)})"


def _external_table_compression_sql(
    generator: SqlGlotGenerator,
    expression: ExternalTableCompressionProperty,
) -> str:
    return f"COMPRESSION TYPE {generator.sql(expression, 'this')}"


DATAFUSION_SELECT_TRANSFORMS: Callable[[SqlGlotGenerator, Expression], str] = preprocess(
    [
        _rewrite_full_outer_join,
        _rewrite_semi_anti_join,
        eliminate_qualify,
        move_ctes_to_top_level,
        ensure_bools,
    ]
)


class DataFusionGenerator(SqlGlotGenerator):
    """SQL generator overrides for DataFusion."""

    def __init__(self, **kwargs: Unpack[GeneratorInitKwargs]) -> None:
        super().__init__(**kwargs)
        self.PROPERTIES_LOCATION = {
            **self.PROPERTIES_LOCATION,
            exp.FileFormatProperty: exp.Properties.Location.POST_SCHEMA,
            exp.PartitionedByProperty: exp.Properties.Location.POST_SCHEMA,
            ExternalTableCompressionProperty: exp.Properties.Location.POST_SCHEMA,
            ExternalTableOrderProperty: exp.Properties.Location.POST_SCHEMA,
            ExternalTableOptionsProperty: exp.Properties.Location.POST_SCHEMA,
        }
        self.TRANSFORMS = {
            **self.TRANSFORMS,
            exp.Select: DATAFUSION_SELECT_TRANSFORMS,
            exp.Array: _array_transform,
            exp.FileFormatProperty: _file_format_property_sql,
            exp.PartitionedByProperty: _partitioned_by_property_sql,
            ExternalTableCompressionProperty: _external_table_compression_sql,
            ExternalTableOrderProperty: _external_table_order_sql,
            ExternalTableOptionsProperty: _external_table_options_sql,
        }


class DataFusionDialect(Dialect):
    """Optional DataFusion-focused dialect overrides."""

    generator_class = DataFusionGenerator


def register_datafusion_dialect(name: str = "datafusion_ext") -> None:
    """Register the DataFusion dialect overrides under a custom name."""
    Dialect.classes[name] = DataFusionDialect
    Dialect.classes.setdefault("datafusion", DataFusionDialect)


def sanitize_templated_sql(sql: str, *, preserve_params: bool = False) -> str:
    """Return SQL text with templated segments replaced for parsing.

    Parameters
    ----------
    sql
        Raw SQL text that may contain templated segments.

    preserve_params
        Whether to preserve parameter placeholders like ``:name``.

    Returns
    -------
    str
        Sanitized SQL string suitable for parsing.
    """
    sanitized = sql
    for pattern in _TEMPLATE_PATTERNS:
        sanitized = pattern.sub(_TEMPLATE_TOKEN, sanitized)
    if preserve_params:
        return sanitized
    return _PARAM_PATTERN.sub(_TEMPLATE_TOKEN, sanitized)


def parse_sql(
    sql: str,
    *,
    options: ParseSqlOptions,
) -> Expression:
    """Parse SQL with configurable sanitization and error handling.

    Parameters
    ----------
    sql
        SQL text to parse.
    options
        Parsing options controlling dialect, strictness, and sanitization.

    Returns
    -------
    sqlglot.Expression
        Parsed SQLGlot expression.
    """
    sanitized = (
        sanitize_templated_sql(sql, preserve_params=options.preserve_params)
        if options.sanitize_templated
        else sql
    )
    level = options.error_level or DEFAULT_ERROR_LEVEL
    return parse_one(sanitized, dialect=options.dialect, error_level=level)


def parse_sql_strict(
    sql: str,
    *,
    dialect: str,
    error_level: ErrorLevel | None = None,
    preserve_params: bool = False,
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
    preserve_params
        Whether to preserve templated parameters during sanitization.

    Returns
    -------
    sqlglot.Expression
        Parsed SQLGlot expression.
    """
    return parse_sql(
        sql,
        options=ParseSqlOptions(
            dialect=dialect,
            error_level=error_level,
            sanitize_templated=True,
            preserve_params=preserve_params,
        ),
    )


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
    schema_map = _ensure_mapping_schema(schema, dialect=dialect)
    return qualify(expr, schema=schema_map, dialect=dialect)


def optimize_expr(
    expr: Expression,
    *,
    schema: SchemaMapping | None = None,
    policy: SqlGlotPolicy | None = None,
    sql: str | None = None,
) -> Expression:
    """Return an optimized SQLGlot expression.

    Returns
    -------
    sqlglot.Expression
        Optimized expression tree.
    """
    policy = policy or default_sqlglot_policy()
    schema_map = _ensure_mapping_schema(schema, dialect=policy.read_dialect)
    return optimize(
        expr,
        schema=schema_map,
        dialect=policy.read_dialect,
        rules=policy.rules,
        sql=sql,
        expand_stars=policy.expand_stars,
        validate_qualify_columns=policy.validate_qualify_columns and schema_map is not None,
        identify=policy.identify,
        max_distance=policy.normalization_distance,
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
    compile_options = _compile_options_from_normalize(options)
    policy = compile_options.policy or default_sqlglot_policy()
    prepared = _prepare_for_qualification(expr, options=compile_options, policy=policy)
    distance = normalization_distance(prepared, max_=policy.normalization_distance)
    optimized = optimize_expr(
        prepared,
        schema=compile_options.schema,
        policy=policy,
        sql=compile_options.sql,
    )
    stats = NormalizationStats(
        distance=distance,
        max_distance=policy.normalization_distance,
        applied=distance <= policy.normalization_distance,
    )
    return NormalizeExprResult(expr=optimized, stats=stats)


def compile_expr(
    expr: Expression,
    *,
    options: SqlGlotCompileOptions,
) -> Expression:
    """Return an optimized SQLGlot expression using the canonical pipeline.

    Returns
    -------
    sqlglot.Expression
        Optimized expression tree.
    """
    policy = options.policy or default_sqlglot_policy()
    prepared = _prepare_for_qualification(expr, options=options, policy=policy)
    return optimize_expr(
        prepared,
        schema=options.schema,
        policy=policy,
        sql=options.sql,
    )


def _compile_options_from_normalize(options: NormalizeExprOptions) -> SqlGlotCompileOptions:
    return SqlGlotCompileOptions(
        schema=options.schema,
        rules=options.rules,
        rewrite_hook=options.rewrite_hook,
        enable_rewrites=options.enable_rewrites,
        policy=options.policy,
        sql=options.sql,
    )


def _normalize_policy_context(
    options: NormalizeExprOptions,
) -> tuple[SqlGlotPolicy, bool, SqlGlotSchema | None]:
    policy = options.policy or default_sqlglot_policy()
    identify = policy.identify or _schema_requires_quoting(options.schema)
    schema_map = _ensure_mapping_schema(options.schema, dialect=policy.read_dialect)
    return policy, identify, schema_map


def _prepare_for_qualification(
    expr: Expression,
    *,
    options: SqlGlotCompileOptions,
    policy: SqlGlotPolicy,
) -> Expression:
    canonical = canonicalize_expr(expr, rules=options.rules)
    rewritten = rewrite_expr(
        canonical,
        rewrite_hook=options.rewrite_hook,
        enabled=options.enable_rewrites,
    )
    transformed = apply_transforms(rewritten, transforms=policy.transforms)
    return _normalize_table_aliases(transformed)


def _qualify_expression(
    expr: Expression,
    *,
    options: NormalizeExprOptions,
    policy: SqlGlotPolicy,
    identify: bool,
) -> Expression:
    qualify_options = QualifyStrictOptions(
        schema=options.schema,
        dialect=policy.read_dialect,
        sql=options.sql,
        expand_stars=policy.expand_stars,
        validate_columns=policy.validate_qualify_columns and options.schema is not None,
        identify=identify,
    )
    return qualify_strict(expr, options=qualify_options)


def _annotate_expression(
    expr: Expression,
    *,
    policy: SqlGlotPolicy,
    identify: bool,
    schema_map: SqlGlotSchema | None,
) -> Expression:
    normalized = normalize_identifiers(
        expr,
        dialect=policy.read_dialect,
        store_original_column_identifiers=identify,
    )
    quoted = quote_identifiers(normalized, dialect=policy.read_dialect, identify=identify)
    return annotate_types(
        quoted,
        schema=schema_map,
        dialect=policy.read_dialect,
    )


def _simplify_expression(expr: Expression, *, policy: SqlGlotPolicy) -> Expression:
    canonicalized = canonicalize(expr, dialect=policy.read_dialect)
    return simplify_expr(canonicalized, dialect=policy.read_dialect)


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
        return normalize_predicates(expr, dnf=False, max_distance=max_distance), NormalizationStats(
            distance=distance,
            max_distance=max_distance,
            applied=True,
        )
    return expr, NormalizationStats(
        distance=distance,
        max_distance=max_distance,
        applied=False,
    )


@dataclass(frozen=True)
class PreflightOptions:
    """Options for preflight SQL validation."""

    schema: SchemaMapping | None = None
    dialect: str = "datafusion"
    strict: bool = True
    preserve_params: bool = False
    policy: SqlGlotPolicy | None = None


def preflight_sql(
    sql: str,
    *,
    options: PreflightOptions | None = None,
) -> PreflightResult:
    """Preflight SQL through qualify + annotate + canonicalize pipeline.

    Parameters
    ----------
    sql
        SQL text to preflight.
    options
        Preflight options controlling schema, dialect, and policy behavior.

    Returns
    -------
    PreflightResult
        Result capturing all qualification stages and diagnostics.
    """
    # Compute policy and schema hashes
    resolved_options = options or PreflightOptions()
    policy_obj = resolved_options.policy or default_sqlglot_policy()
    policy_snapshot = sqlglot_policy_snapshot_for(policy_obj)
    policy_hash = policy_snapshot.policy_hash

    schema_map_hash = (
        schema_map_fingerprint_from_mapping(resolved_options.schema)
        if resolved_options.schema is not None
        else None
    )

    state = PreflightState(
        original_sql=sql,
        policy_hash=policy_hash,
        schema_map_hash=schema_map_hash,
    )
    context = PreflightContext(
        schema=resolved_options.schema,
        policy=policy_obj,
        dialect=resolved_options.dialect,
        strict=resolved_options.strict,
        preserve_params=resolved_options.preserve_params,
    )

    expr, result = _preflight_parse(sql, context=context, state=state)
    if result is None and expr is not None:
        qualified_expr, result = _preflight_qualify(expr, context=context, state=state)
        if result is None and qualified_expr is not None:
            annotated_expr, result = _preflight_annotate(
                qualified_expr,
                context=context,
                state=state,
            )
            if result is None and annotated_expr is not None:
                canonicalized_expr, result = _preflight_canonicalize(
                    annotated_expr,
                    context=context,
                    state=state,
                )
                state.canonicalized_expr = canonicalized_expr

    return result or _finalize_preflight(state)


@dataclass
class PreflightState:
    """Mutable state container for SQL preflight stages."""

    original_sql: str
    policy_hash: str
    schema_map_hash: str | None
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    qualified_expr: Expression | None = None
    annotated_expr: Expression | None = None
    canonicalized_expr: Expression | None = None


@dataclass(frozen=True)
class PreflightContext:
    schema: SchemaMapping | None
    policy: SqlGlotPolicy
    dialect: str
    strict: bool
    preserve_params: bool


def _finalize_preflight(state: PreflightState) -> PreflightResult:
    return PreflightResult(
        original_sql=state.original_sql,
        qualified_expr=state.qualified_expr,
        annotated_expr=state.annotated_expr,
        canonicalized_expr=state.canonicalized_expr,
        errors=tuple(state.errors),
        warnings=tuple(state.warnings),
        policy_hash=state.policy_hash,
        schema_map_hash=state.schema_map_hash,
    )


def _handle_preflight_error(
    state: PreflightState,
    *,
    error_msg: str,
    strict: bool,
) -> PreflightResult | None:
    state.errors.append(error_msg)
    if strict:
        return _finalize_preflight(state)
    state.warnings.append(error_msg)
    return None


def _preflight_parse(
    sql: str,
    *,
    context: PreflightContext,
    state: PreflightState,
) -> tuple[Expression | None, PreflightResult | None]:
    try:
        expr = parse_sql_strict(
            sql,
            dialect=context.dialect,
            error_level=context.policy.error_level,
            preserve_params=context.preserve_params,
        )
    except (SqlglotError, TypeError, ValueError) as exc:
        error_msg = f"Parse failed: {exc}"
        return None, _handle_preflight_error(state, error_msg=error_msg, strict=context.strict)
    return expr, None


def _preflight_qualify(
    expr: Expression,
    *,
    context: PreflightContext,
    state: PreflightState,
) -> tuple[Expression | None, PreflightResult | None]:
    identify = context.policy.identify or _schema_requires_quoting(context.schema)
    qualify_options = QualifyStrictOptions(
        schema=context.schema,
        dialect=context.dialect,
        sql=state.original_sql,
        expand_stars=context.policy.expand_stars,
        validate_columns=context.policy.validate_qualify_columns and context.schema is not None,
        identify=identify,
    )
    prepared = _prepare_for_qualification(
        expr,
        options=SqlGlotCompileOptions(
            schema=context.schema,
            policy=context.policy,
            sql=state.original_sql,
        ),
        policy=context.policy,
    )
    try:
        state.qualified_expr = qualify_strict(prepared, options=qualify_options)
    except SqlGlotQualificationError as exc:
        error_msg = f"Qualification failed: {exc}"
        return None, _handle_preflight_error(
            state,
            error_msg=error_msg,
            strict=context.strict,
        )
    except (SqlglotError, TypeError, ValueError) as exc:
        error_msg = f"Qualification failed: {exc}"
        return None, _handle_preflight_error(
            state,
            error_msg=error_msg,
            strict=context.strict,
        )
    return state.qualified_expr, None


def _preflight_annotate(
    expr: Expression,
    *,
    context: PreflightContext,
    state: PreflightState,
) -> tuple[Expression | None, PreflightResult | None]:
    identify = context.policy.identify or _schema_requires_quoting(context.schema)
    schema_map = _ensure_mapping_schema(context.schema, dialect=context.dialect)
    try:
        state.annotated_expr = _annotate_expression(
            expr,
            policy=context.policy,
            identify=identify,
            schema_map=schema_map,
        )
    except (SqlglotError, TypeError, ValueError) as exc:
        error_msg = f"Annotation failed: {exc}"
        return None, _handle_preflight_error(
            state,
            error_msg=error_msg,
            strict=context.strict,
        )
    return state.annotated_expr, None


def _preflight_canonicalize(
    expr: Expression,
    *,
    context: PreflightContext,
    state: PreflightState,
) -> tuple[Expression | None, PreflightResult | None]:
    try:
        canonicalized = canonicalize(expr, dialect=context.dialect)
    except (SqlglotError, TypeError, ValueError) as exc:
        error_msg = f"Canonicalization failed: {exc}"
        return None, _handle_preflight_error(
            state,
            error_msg=error_msg,
            strict=context.strict,
        )
    return canonicalized, None


def emit_preflight_diagnostics(result: PreflightResult) -> dict[str, object]:
    """Emit structured diagnostics from a preflight result.

    Parameters
    ----------
    result
        Preflight result to extract diagnostics from.

    Returns
    -------
    dict[str, object]
        Structured diagnostics payload for logging or storage.
    """
    diagnostics: dict[str, object] = {
        "original_sql": result.original_sql,
        "policy_hash": result.policy_hash,
        "schema_map_hash": result.schema_map_hash,
        "errors": list(result.errors),
        "warnings": list(result.warnings),
        "has_errors": len(result.errors) > 0,
        "has_warnings": len(result.warnings) > 0,
        "stages_completed": {},
    }

    # Track which stages completed successfully
    stages_completed = {
        "qualified": result.qualified_expr is not None,
        "annotated": result.annotated_expr is not None,
        "canonicalized": result.canonicalized_expr is not None,
    }
    diagnostics["stages_completed"] = stages_completed

    # Include SQL for each successful stage
    if result.qualified_expr is not None:
        try:
            diagnostics["qualified_sql"] = result.qualified_expr.sql(dialect=DEFAULT_WRITE_DIALECT)
        except (SqlglotError, TypeError, ValueError):
            diagnostics["qualified_sql"] = None

    if result.annotated_expr is not None:
        try:
            diagnostics["annotated_sql"] = result.annotated_expr.sql(dialect=DEFAULT_WRITE_DIALECT)
        except (SqlglotError, TypeError, ValueError):
            diagnostics["annotated_sql"] = None

    if result.canonicalized_expr is not None:
        try:
            diagnostics["canonicalized_sql"] = result.canonicalized_expr.sql(
                dialect=DEFAULT_WRITE_DIALECT
            )
        except (SqlglotError, TypeError, ValueError):
            diagnostics["canonicalized_sql"] = None

    return diagnostics


def serialize_ast_artifact(artifact: AstArtifact) -> str:
    """Serialize an AstArtifact to JSON string.

    Parameters
    ----------
    artifact
        AstArtifact to serialize.

    Returns
    -------
    str
        JSON-serialized artifact.
    """
    return json.dumps(artifact.to_dict(), sort_keys=True)


def deserialize_ast_artifact(serialized: str) -> AstArtifact:
    """Deserialize an AstArtifact from JSON string.

    Parameters
    ----------
    serialized
        JSON-serialized artifact string.

    Returns
    -------
    AstArtifact
        Deserialized artifact instance.

    Raises
    ------
    ValueError
        Raised when the serialized data is invalid.
    """
    try:
        data = json.loads(serialized)
        if not isinstance(data, dict):
            _raise_invalid_artifact_type()
        return AstArtifact(
            sql=data["sql"],
            ast_json=data["ast_json"],
            policy_hash=data["policy_hash"],
        )
    except (KeyError, TypeError) as exc:
        msg = f"Invalid AstArtifact serialization: {exc}"
        raise ValueError(msg) from exc


def ast_to_artifact(
    expr: Expression,
    *,
    sql: str | None = None,
    policy: SqlGlotPolicy | None = None,
) -> AstArtifact:
    """Convert a SQLGlot expression to an AstArtifact.

    Parameters
    ----------
    expr
        SQLGlot expression to serialize.
    sql
        Optional original SQL text (defaults to expr.sql()).
    policy
        Optional policy for hash computation.

    Returns
    -------
    AstArtifact
        Artifact with serialized AST and metadata.
    """
    policy_obj = policy or default_sqlglot_policy()
    policy_snapshot = sqlglot_policy_snapshot_for(policy_obj)
    ast_data = dump(expr)
    ast_json = json.dumps(ast_data, sort_keys=True)
    sql_text = sql or expr.sql(dialect=policy_obj.write_dialect)
    return AstArtifact(
        sql=sql_text,
        ast_json=ast_json,
        policy_hash=policy_snapshot.policy_hash,
    )


def _raise_invalid_artifact_type() -> None:
    msg = "Serialized artifact must be a JSON object."
    raise ValueError(msg)


def artifact_to_ast(artifact: AstArtifact) -> Expression:
    """Convert an AstArtifact back to a SQLGlot expression.

    Parameters
    ----------
    artifact
        Artifact to deserialize.

    Returns
    -------
    Expression
        Deserialized SQLGlot expression.

    Raises
    ------
    ValueError
        Raised when the AST JSON cannot be deserialized.
    """
    try:
        ast_data = json.loads(artifact.ast_json)
        result = load(ast_data)
    except (SqlglotError, TypeError, ValueError) as exc:
        msg = f"Failed to deserialize AST: {exc}"
        raise ValueError(msg) from exc
    if result is None:
        msg = "Failed to deserialize AST: result was None."
        raise ValueError(msg)
    return result


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
    binder = _ParamBinder(
        params=_normalize_param_bindings(params),
        used_names=_explicit_param_names(expr),
    )
    return binder.bind(expr)


@dataclass
class _ParamBinder:
    params: Mapping[str, object]
    used_names: set[str]
    ordinal: int = 0

    def bind(self, expr: Expression) -> Expression:
        return expr.transform(self._rewrite)

    def _rewrite(self, node: Expression) -> Expression:
        if not isinstance(node, (exp.Placeholder, exp.Parameter)):
            return node
        name = self._param_key(node)
        if name not in self.params:
            msg = f"Missing parameter binding for {name!r}."
            raise KeyError(msg)
        return _literal_from_value(self.params[name])

    def _param_key(self, node: exp.Placeholder | exp.Parameter) -> str:
        raw = _param_raw_name(node)
        if raw is None:
            return self._next_ordinal()
        return _normalize_param_key(raw)

    def _next_ordinal(self) -> str:
        self.ordinal += 1
        while str(self.ordinal) in self.used_names:
            self.ordinal += 1
        name = str(self.ordinal)
        self.used_names.add(name)
        return name


def _normalize_param_bindings(params: Mapping[str, object]) -> dict[str, object]:
    return {_normalize_param_key(str(key)): value for key, value in params.items()}


def _explicit_param_names(expr: Expression) -> set[str]:
    names: set[str] = set()
    for node in expr.find_all(exp.Placeholder):
        raw = _param_raw_name(node)
        if raw is not None:
            names.add(_normalize_param_key(raw))
    for node in expr.find_all(exp.Parameter):
        raw = _param_raw_name(node)
        if raw is not None:
            names.add(_normalize_param_key(raw))
    return names


def _param_raw_name(node: exp.Placeholder | exp.Parameter) -> str | None:
    if isinstance(node, exp.Placeholder):
        raw = node.this
        if raw is None:
            return None
        return raw if isinstance(raw, str) else str(raw)
    name = getattr(node, "name", None)
    if isinstance(name, str) and name:
        return name
    value = node.this
    if value is None:
        return None
    return value if isinstance(value, str) else str(value)


def _normalize_param_key(name: str) -> str:
    return name.lstrip(":$")


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
    schema_map_hash: str | None = None,
) -> str:
    """Return a stable fingerprint for a SQLGlot expression.

    Returns
    -------
    str
        SHA-256 fingerprint for the canonical SQL representation.
    """
    if policy_hash is None:
        policy_hash = sqlglot_policy_snapshot().policy_hash
    sql_text = expr.sql(dialect=dialect)
    payload = {
        "version": HASH_PAYLOAD_VERSION,
        "dialect": dialect,
        "sql": sql_text,
        "policy_hash": policy_hash,
        "schema_map_hash": schema_map_hash,
    }
    return payload_hash(payload, _PLAN_HASH_SCHEMA)


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
    step_ids = {
        step: payload_hash(
            {"version": HASH_PAYLOAD_VERSION, "entries": _map_entries(payload)},
            _STEP_HASH_SCHEMA,
        )
        for step, payload in step_payloads.items()
    }
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
    dag_hash = payload_hash(
        {
            "version": HASH_PAYLOAD_VERSION,
            "steps": [str(row["step_id"]) for row in step_rows_sorted],
            "edges": edge_rows_sorted,
        },
        _DAG_HASH_SCHEMA,
    )
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
    return _stable_repr(value)


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
        "sql_text": expr.sql(dialect=options.dialect),
    }


__all__ = [
    "AstArtifact",
    "CanonicalizationRules",
    "DataFusionDialect",
    "ExternalTableCompressionProperty",
    "ExternalTableOptionsProperty",
    "ExternalTableOrderProperty",
    "NormalizationStats",
    "NormalizeExprOptions",
    "NormalizeExprResult",
    "ParseSqlOptions",
    "PlannerDagSnapshot",
    "PreflightOptions",
    "PreflightResult",
    "QualifyStrictOptions",
    "SchemaMapping",
    "SqlGlotCompileOptions",
    "SqlGlotPolicy",
    "SqlGlotPolicySnapshot",
    "SqlGlotQualificationError",
    "SqlGlotRewriteLane",
    "SqlGlotSurface",
    "SqlGlotSurfacePolicy",
    "apply_transforms",
    "artifact_to_ast",
    "ast_to_artifact",
    "bind_params",
    "canonical_ast_fingerprint",
    "canonicalize_expr",
    "compile_expr",
    "default_sqlglot_policy",
    "deserialize_ast_artifact",
    "emit_preflight_diagnostics",
    "normalize_ddl_sql",
    "normalize_expr",
    "normalize_expr_with_stats",
    "optimize_expr",
    "parse_sql",
    "parse_sql_strict",
    "plan_fingerprint",
    "planner_dag_snapshot",
    "preflight_sql",
    "qualify_expr",
    "qualify_strict",
    "register_datafusion_dialect",
    "resolve_sqlglot_policy",
    "rewrite_expr",
    "sanitize_templated_sql",
    "schema_map_fingerprint_from_mapping",
    "serialize_ast_artifact",
    "simplify_expr",
    "sqlglot_emit",
    "sqlglot_policy_by_name",
    "sqlglot_policy_snapshot",
    "sqlglot_policy_snapshot_for",
    "sqlglot_sql",
    "sqlglot_surface_policy",
    "transpile_sql",
]
