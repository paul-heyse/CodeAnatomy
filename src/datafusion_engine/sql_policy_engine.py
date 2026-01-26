"""SQL policy engine for deterministic canonicalization and optimization.

This module implements SQLGlot-based SQL canonicalization with pinned
optimizer rules for reproducible query compilation. The policy profile
defines how SQL is normalized and must be included in cache keys to
ensure deterministic builds.
"""

from __future__ import annotations

import hashlib
import importlib
import json
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol, cast

import sqlglot.expressions as exp
from sqlglot.optimizer import RULES, optimize
from sqlglot.optimizer.annotate_types import annotate_types
from sqlglot.optimizer.canonicalize import canonicalize
from sqlglot.optimizer.normalize import normalization_distance, normalize
from sqlglot.optimizer.pushdown_predicates import pushdown_predicates
from sqlglot.optimizer.pushdown_projections import pushdown_projections
from sqlglot.optimizer.qualify_columns import validate_qualify_columns
from sqlglot.optimizer.simplify import simplify
from sqlglot.schema import MappingSchema

from sqlglot_tools.compat import ErrorLevel

if TYPE_CHECKING:
    from sqlglot_tools.optimizer import SchemaMapping


class _SqlglotSerde(Protocol):
    def dump(self, expression: exp.Expression) -> list[dict[str, Any]]:
        """Return a JSON-serializable AST payload."""
        ...


def _serde() -> _SqlglotSerde:
    module = importlib.import_module("sqlglot.serde")
    return cast("_SqlglotSerde", module)


@dataclass(frozen=True)
class SQLPolicyProfile:
    """Compiler policy contract pinned for determinism.

    This profile defines how SQL is canonicalized and must be
    included in cache keys to ensure reproducibility.

    Parameters
    ----------
    read_dialect
        SQLGlot dialect for parsing input SQL.
    write_dialect
        SQLGlot dialect for rendering output SQL.
    optimizer_rules
        Ordered sequence of optimizer rules to apply.
    normalize_distance_limit
        Maximum complexity threshold for predicate normalization.
    pushdown_projections
        Enable projection pushdown optimization.
    pushdown_predicates
        Enable predicate pushdown optimization.
    expand_stars
        Expand SELECT * during qualification.
    validate_qualify_columns
        Make column ambiguity errors fatal.
    identify_mode
        Quote mode: False (no quotes), True (quote all), "safe" (quote when needed).
    pretty_output
        Enable pretty-printing for rendered SQL.
    error_level
        Strictness for SQL parsing/compilation errors.
    unsupported_level
        Strictness for unsupported SQL constructs.
    """

    read_dialect: str = "postgres"
    write_dialect: str = "postgres"
    optimizer_rules: tuple[Callable[..., object], ...] = field(
        default_factory=lambda: cast("tuple[Callable[..., object], ...]", RULES),
    )
    normalize_distance_limit: int = 128
    pushdown_projections: bool = True
    pushdown_predicates: bool = True
    expand_stars: bool = True
    validate_qualify_columns: bool = True
    identify_mode: bool | str = False
    pretty_output: bool = False
    error_level: ErrorLevel = ErrorLevel.IMMEDIATE
    unsupported_level: ErrorLevel = ErrorLevel.RAISE

    def policy_fingerprint(self) -> str:
        """Generate hash of policy settings for cache keys.

        Returns
        -------
        str
            16-character hex digest of policy configuration.
        """
        policy_dict = {
            "read_dialect": self.read_dialect,
            "write_dialect": self.write_dialect,
            "rules": [r.__name__ for r in self.optimizer_rules],
            "normalize_limit": self.normalize_distance_limit,
            "pushdown_proj": self.pushdown_projections,
            "pushdown_pred": self.pushdown_predicates,
            "expand_stars": self.expand_stars,
            "identify": self.identify_mode,
            "error_level": self.error_level.name,
            "unsupported_level": self.unsupported_level.name,
        }
        return hashlib.sha256(json.dumps(policy_dict, sort_keys=True).encode()).hexdigest()[:16]


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
    """Execute full policy-aware SQL canonicalization pipeline.

    This function applies the complete SQLGlot optimizer stack with
    controlled normalization and policy-driven optimizations.

    Parameters
    ----------
    expr
        Parsed SQLGlot expression.
    schema
        Schema for qualification and type inference.
    profile
        Compiler policy settings controlling optimization behavior.
    original_sql
        Original SQL string for error highlighting and diagnostics.

    Returns
    -------
    tuple[exp.Expression, CompilationArtifacts]
        Canonicalized AST and compilation artifacts including fingerprint and lineage.
    """
    # Ensure schema is MappingSchema
    if not isinstance(schema, MappingSchema):
        schema = MappingSchema(dict(schema))

    # 1. Run optimizer with pinned rules
    qualified = optimize(
        expr,
        schema=schema,
        dialect=profile.read_dialect,
        rules=profile.optimizer_rules,
    )

    # 2. Validate qualification (makes ambiguity fatal)
    if profile.validate_qualify_columns:
        validate_qualify_columns(qualified, sql=original_sql)

    # 3. Type-driven canonicalization
    typed = annotate_types(qualified, schema=schema, dialect=profile.read_dialect)
    canonical = canonicalize(typed, dialect=profile.read_dialect)

    # 4. Controlled predicate normalization
    artifacts = CompilationArtifacts.from_ast(canonical, schema)

    # Check normalization cost before applying
    where_clause = canonical.find(exp.Where)
    if where_clause:
        distance = normalization_distance(where_clause.this)
        artifacts.normalization_distance = distance
        if distance <= profile.normalize_distance_limit:
            canonical = normalize(canonical, max_distance=profile.normalize_distance_limit)
        else:
            artifacts.normalization_skipped = True

    # 5. Simplification (respects FINAL markers)
    simplified = simplify(canonical, dialect=profile.read_dialect)

    # 6. Optional pushdowns
    if profile.pushdown_predicates:
        simplified = pushdown_predicates(simplified)
    if profile.pushdown_projections:
        simplified = pushdown_projections(simplified, schema=schema)

    # Rebuild artifacts with final AST
    artifacts = CompilationArtifacts.from_ast(simplified, schema)

    return simplified, artifacts


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
    from sqlglot.optimizer.scope import build_scope

    result: dict[str, set[tuple[str, str]]] = {}

    # Get output columns from SELECT
    select = ast.find(exp.Select)
    if not select:
        return result

    # Build scope once for efficiency
    try:
        scope = build_scope(ast)
    except (ValueError, TypeError, AttributeError):
        # Scope building may fail for complex or malformed queries
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
    return ast.sql(
        dialect=profile.write_dialect,
        pretty=profile.pretty_output,
        identify=profile.identify_mode,
    )
