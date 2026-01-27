"""SQL policy engine for deterministic canonicalization and optimization.

DEPRECATED: This module is deprecated in favor of DataFusion-native planning.
SQL strings should be treated as external ingress only, gated by DataFusion
SQLOptions. Internal execution should use builder/plan-based approaches.

This module is retained for backward compatibility during migration only.
Use DataFusion DataFrame builders and logical plan construction instead.
"""

from __future__ import annotations

import hashlib
import importlib
import json
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol, cast

import sqlglot.expressions as exp
from sqlglot.schema import MappingSchema

from sqlglot_tools.compat import ErrorLevel, Expression
from sqlglot_tools.optimizer import (
    NormalizeExprOptions,
    SqlGlotPolicy,
    _annotate_expression,
    _compile_options_from_normalize,
    _normalize_predicates_with_stats,
    _prepare_for_qualification,
    _qualify_expression,
    _simplify_expression,
    pushdown_predicates_transform,
    pushdown_projections_transform,
    resolve_sqlglot_policy,
    sqlglot_emit,
)

if TYPE_CHECKING:
    from sqlglot_tools.optimizer import GeneratorInitKwargs, SchemaMapping


class _SqlglotSerde(Protocol):
    def dump(self, expression: exp.Expression) -> list[dict[str, Any]]:
        """Return a JSON-serializable AST payload."""
        ...


def _serde() -> _SqlglotSerde:
    module = importlib.import_module("sqlglot.serde")
    return cast("_SqlglotSerde", module)


def _merge_generator_kwargs(
    base: GeneratorInitKwargs | None,
    override: GeneratorInitKwargs | None,
) -> GeneratorInitKwargs:
    generator: GeneratorInitKwargs = {}
    if base:
        generator.update(base)
    if override:
        generator.update(override)
    return generator


@dataclass(frozen=True)
class SQLPolicyProfile:
    """Compiler policy contract pinned for determinism.

    This profile is a thin adapter over ``SqlGlotPolicy`` so that all
    compilation paths share the same canonical policy lane.
    """

    policy: SqlGlotPolicy | None = None
    read_dialect: str | None = None
    write_dialect: str | None = None
    optimizer_rules: Sequence[Callable[[Expression], Expression]] | None = None
    normalize_distance_limit: int | None = None
    pushdown_projections: bool | None = None
    pushdown_predicates: bool | None = None
    expand_stars: bool | None = None
    validate_qualify_columns: bool | None = None
    identify_mode: bool | None = None
    pretty_output: bool = False
    error_level: ErrorLevel | None = None
    unsupported_level: ErrorLevel | None = None
    transforms: Sequence[Callable[[Expression], Expression]] | None = None
    generator: GeneratorInitKwargs | None = None

    def to_sqlglot_policy(self) -> SqlGlotPolicy:
        """Resolve the effective SqlGlotPolicy for this profile.

        Returns
        -------
        SqlGlotPolicy
            Resolved SQLGlot policy for compilation.
        """
        base = resolve_sqlglot_policy(name="datafusion_compile", policy=self.policy)
        transforms = list(self.transforms or base.transforms)
        if self.pushdown_projections is False:
            transforms = [t for t in transforms if t is not pushdown_projections_transform]
        if self.pushdown_predicates is False:
            transforms = [t for t in transforms if t is not pushdown_predicates_transform]
        generator = _merge_generator_kwargs(base.generator, self.generator)
        rules = tuple(self.optimizer_rules) if self.optimizer_rules is not None else base.rules
        return SqlGlotPolicy(
            read_dialect=self.read_dialect or base.read_dialect,
            write_dialect=self.write_dialect or base.write_dialect,
            rules=rules,
            generator=generator,
            normalization_distance=self.normalize_distance_limit or base.normalization_distance,
            error_level=self.error_level or base.error_level,
            unsupported_level=self.unsupported_level or base.unsupported_level,
            transforms=tuple(transforms),
            expand_stars=base.expand_stars if self.expand_stars is None else self.expand_stars,
            validate_qualify_columns=(
                base.validate_qualify_columns
                if self.validate_qualify_columns is None
                else self.validate_qualify_columns
            ),
            identify=base.identify if self.identify_mode is None else self.identify_mode,
        )

    def policy_hash(self) -> str:
        """Generate hash of policy settings for cache keys.

        DEPRECATED: Use plan_fingerprint from DataFusionPlanBundle instead.

        Returns
        -------
        str
            Stable fingerprint for policy settings.
        """
        policy = self.to_sqlglot_policy()
        policy_dict = {
            "read_dialect": policy.read_dialect,
            "write_dialect": policy.write_dialect,
            "rules": [rule.__name__ for rule in policy.rules],
            "normalization_distance": policy.normalization_distance,
            "expand_stars": policy.expand_stars,
            "validate_qualify_columns": policy.validate_qualify_columns,
            "identify": policy.identify,
            "error_level": policy.error_level.name,
            "unsupported_level": policy.unsupported_level.name,
            "transforms": [transform.__name__ for transform in policy.transforms],
        }
        return hashlib.sha256(json.dumps(policy_dict, sort_keys=True).encode()).hexdigest()[:16]

    def policy_fingerprint(self) -> str:
        """Return the policy hash for compatibility.

        DEPRECATED: Use plan_fingerprint from DataFusionPlanBundle instead.

        Returns
        -------
        str
            Stable fingerprint for policy settings.
        """
        return self.policy_hash()


@dataclass
class CompilationArtifacts:
    """Artifacts produced during SQL compilation.

    Parameters
    ----------
    serde_payload
        JSON-serializable AST representation via sqlglot.serde.
    ast_fingerprint
        SHA256 hash of serialized AST for caching.
    lineage_by_column
        Column-level lineage: output_col -> {(source_table, source_col)}.
    qualification_errors
        List of qualification errors encountered.
    normalization_skipped
        Whether normalization was skipped due to complexity limit.
    normalization_distance
        Measured complexity of normalization operation.
    """

    serde_payload: list[dict[str, Any]]
    ast_fingerprint: str
    lineage_by_column: dict[str, set[tuple[str, str]]]
    qualification_errors: list[str] = field(default_factory=list)
    normalization_skipped: bool = False
    normalization_distance: int = 0

    @classmethod
    def from_ast(
        cls,
        ast: exp.Expression,
        schema: MappingSchema,
    ) -> CompilationArtifacts:
        """Build artifacts from canonicalized AST.

        Parameters
        ----------
        ast
            Canonicalized SQLGlot expression.
        schema
            Schema for lineage extraction.

        Returns
        -------
        CompilationArtifacts
            Compilation artifacts with fingerprint and lineage.
        """
        payload = _serde().dump(ast)
        fingerprint = hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()

        # Extract lineage for each output column
        lineage = extract_column_lineage(ast, schema)

        return cls(
            serde_payload=payload,
            ast_fingerprint=fingerprint,
            lineage_by_column=lineage,
        )


def compile_sql_policy(
    expr: exp.Expression,
    *,
    schema: MappingSchema | SchemaMapping,
    profile: SQLPolicyProfile,
    original_sql: str | None = None,
) -> tuple[exp.Expression, CompilationArtifacts]:
    """Execute the unified policy-aware SQLGlot canonicalization pipeline.

    DEPRECATED: Use DataFusion-native planning with DataFrame builders instead.
    This function is retained for backward compatibility during migration only.

    Returns
    -------
    tuple[sqlglot.expressions.Expression, CompilationArtifacts]
        Canonicalized expression and compilation artifacts.
    """
    schema_map: SchemaMapping = schema.mapping if isinstance(schema, MappingSchema) else schema
    policy = profile.to_sqlglot_policy()
    options = NormalizeExprOptions(
        schema=schema_map,
        policy=policy,
        sql=original_sql,
        enable_rewrites=True,
    )
    compile_options = _compile_options_from_normalize(options)
    prepared = _prepare_for_qualification(expr, options=compile_options, policy=policy)
    qualified = _qualify_expression(
        prepared,
        options=options,
        policy=policy,
        identify=policy.identify,
    )
    mapping = (
        MappingSchema.from_mapping_schema(schema)
        if isinstance(schema, MappingSchema)
        else MappingSchema(dict(schema_map))
    )
    annotated = _annotate_expression(
        qualified,
        policy=policy,
        identify=policy.identify,
        schema_map=mapping,
    )
    simplified = _simplify_expression(annotated, policy=policy)
    normalized, stats = _normalize_predicates_with_stats(
        simplified,
        max_distance=policy.normalization_distance,
    )
    artifacts = CompilationArtifacts.from_ast(normalized, mapping)
    artifacts.normalization_distance = stats.distance
    artifacts.normalization_skipped = not stats.applied
    return normalized, artifacts


def extract_column_lineage(
    ast: exp.Expression,
    schema: MappingSchema,
) -> dict[str, set[tuple[str, str]]]:
    """Extract column-level lineage from qualified AST.

    Uses SQLGlot's lineage analysis to trace each output column back
    to its source tables and columns.

    Parameters
    ----------
    ast
        Qualified SQLGlot expression (must be qualified for accurate results).
    schema
        Schema for lineage resolution.

    Returns
    -------
    dict[str, set[tuple[str, str]]]
        Mapping of output_column -> {(source_table, source_column)}.
    """
    from sqlglot.lineage import lineage

    from sqlglot_tools.lineage import build_scope_cached

    result: dict[str, set[tuple[str, str]]] = {}

    # Get output columns from SELECT
    select = ast.find(exp.Select)
    if not select:
        return result

    # Build scope once for efficiency
    scope = build_scope_cached(ast)
    if scope is None:
        return result

    # Extract lineage for each output column
    for expr in select.expressions:
        col_name = expr.alias_or_name
        if not col_name:
            continue

        try:
            lin = lineage(
                col_name,
                ast,
                schema=schema,
                scope=scope,
                copy=False,
            )

            # Collect source columns from lineage graph
            sources: set[tuple[str, str]] = set()
            for node in lin.walk():
                source = getattr(node, "source", None)
                column = getattr(node, "column", None)
                if source is None or column is None:
                    continue
                source_name = getattr(source, "name", None)
                if not isinstance(source_name, str) or not isinstance(column, str):
                    continue
                sources.add((source_name, column))

            result[col_name] = sources
        except (ValueError, TypeError, AttributeError, KeyError):
            # Lineage may fail for complex or unsupported expressions
            result[col_name] = set()

    return result


def render_for_execution(
    ast: exp.Expression,
    profile: SQLPolicyProfile,
) -> str:
    """Render canonicalized AST to SQL string for execution.

    Parameters
    ----------
    ast
        Canonicalized SQLGlot expression.
    profile
        SQL policy controlling dialect and formatting.

    Returns
    -------
    str
        SQL string ready for execution.
    """
    policy = profile.to_sqlglot_policy()
    return sqlglot_emit(ast, policy=policy, pretty=profile.pretty_output)
