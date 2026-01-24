# DataFusion Unifier Modularization Plan v2

## Goals

- Consolidate DataFusion, Ibis, and SQLGlot integration into a single, programmatic execution substrate.
- Shift from stringly-typed SQL to SQLGlot AST-first plan and DDL generation with full optimizer integration.
- Centralize IO, catalog, write, and introspection behavior so every execution path is consistent.
- Establish streaming-first execution via Arrow C Stream protocol for memory-bounded pipelines.
- Enable incremental rebuild via semantic diff and deterministic AST fingerprinting.
- Reduce duplication and decommission legacy helpers once the unifier is in place.

## Non-Goals

- Do not change business semantics of existing datasets.
- Do not change external API contracts unless explicitly listed.
- Do not remove DataFusion runtime safeguards or diagnostics coverage.

---

## Phase 1: Core Unification

### 1) Unified DataFusion IO Adapter (Catalog + Registration + Object Stores)

**Intent**
Create a single IO adapter used by all DataFusion access paths. This consolidates:
object store registration, DDL-based external table registration, and dataset metadata capture.

**Representative pattern**
```python
# src/datafusion_engine/io_adapter.py
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from datafusion import SessionContext

if TYPE_CHECKING:
    from datafusion_engine.runtime import DataFusionRuntimeProfile
    from ibis_engine.registry import DatasetLocation


@dataclass(frozen=True)
class DataFusionIOAdapter:
    """Unified IO adapter for all DataFusion access paths."""

    ctx: SessionContext
    profile: DataFusionRuntimeProfile | None

    def register_object_store(
        self,
        *,
        scheme: str,
        store: object,
        host: str | None = None,
    ) -> None:
        """Register object store with diagnostics capture."""
        self.ctx.register_object_store(scheme, store, host)
        self._record_artifact("object_store_registered", {
            "scheme": scheme,
            "host": host,
        })

    def external_table_ddl(
        self,
        *,
        name: str,
        location: DatasetLocation,
    ) -> str:
        """Generate DDL via SQLGlot AST builder."""
        from sqlglot_tools.ddl_builders import build_external_table_ddl
        return build_external_table_ddl(
            name=name,
            location=location,
            profile=self.profile,
        )

    def register_external_table(
        self,
        *,
        name: str,
        location: DatasetLocation,
    ) -> None:
        """Register external table with full diagnostics."""
        ddl = self.external_table_ddl(name=name, location=location)
        options = self._statement_options()
        self.ctx.sql_with_options(ddl, options).collect()
        self._record_artifact("external_table_registered", {
            "name": name,
            "location": str(location),
            "ddl": ddl,
        })

    def register_arrow_table(
        self,
        name: str,
        table: pa.Table,
        *,
        overwrite: bool = False,
    ) -> None:
        """Register in-memory Arrow table."""
        if overwrite and self.ctx.table_exist(name):
            self._deregister_table(name)
        self.ctx.register_table(name, table)

    def register_dataset(
        self,
        name: str,
        dataset: ds.Dataset,
    ) -> None:
        """Register PyArrow Dataset as table provider."""
        self.ctx.register_dataset(name, dataset)

    def _deregister_table(self, name: str) -> None:
        """Centralized table deregistration."""
        catalog = self.ctx.catalog()
        schema = catalog.schema()
        schema.deregister_table(name)

    def _statement_options(self) -> SQLOptions:
        """Build SQL options from runtime profile."""
        from datafusion_engine.sql_safety import statement_sql_options_for_profile
        return statement_sql_options_for_profile(self.profile)

    def _record_artifact(self, name: str, payload: dict) -> None:
        """Record diagnostics artifact."""
        if self.profile and self.profile.diagnostics_sink:
            self.profile.diagnostics_sink.record_artifact(name, payload)
```

**Target files**
- `src/datafusion_engine/io_adapter.py` (new)
- `src/datafusion_engine/registry_bridge.py`
- `src/ibis_engine/backend.py`
- `src/engine/session_factory.py`
- `src/datafusion_engine/runtime.py`

**Implementation checklist**
- [ ] Create `DataFusionIOAdapter` with object-store registration logic.
- [ ] Route `register_dataset_df` and external table DDL through the adapter.
- [ ] Replace direct calls to `ctx.register_object_store` with adapter calls.
- [ ] Implement centralized `_deregister_table` helper.
- [ ] Ensure diagnostics artifacts flow through adapter for all registration paths.
- [ ] Add integration tests for object store + external table registration.

---

### 2) SQLGlot AST-First DDL/COPY Generation

**Intent**
Replace string SQL assembly with SQLGlot AST generation for DDL/COPY/INSERT. This improves
stability, safety, and dialect consistency.

**Representative pattern**
```python
# src/sqlglot_tools/ddl_builders.py
from __future__ import annotations

from typing import TYPE_CHECKING

from sqlglot import exp
from sqlglot_tools.compat import parse_one

if TYPE_CHECKING:
    from datafusion_engine.runtime import DataFusionRuntimeProfile
    from ibis_engine.registry import DatasetLocation


def build_copy_to_ast(
    *,
    query: exp.Expression,
    path: str,
    file_format: str = "PARQUET",
    options: dict[str, object] | None = None,
    partition_by: tuple[str, ...] = (),
) -> exp.Copy:
    """
    Build COPY TO statement as SQLGlot AST.

    Parameters
    ----------
    query
        The source query expression.
    path
        Destination path (local or object store URI).
    file_format
        Output format (PARQUET, CSV, JSON, ARROW).
    options
        Format-specific options (compression, delimiter, etc.).
    partition_by
        Columns for hive-style partitioning.

    Returns
    -------
    exp.Copy
        SQLGlot COPY expression ready for dialect rendering.
    """
    option_exprs = []

    # Format option
    option_exprs.append(
        exp.Property(this=exp.Var(this="FORMAT"), value=exp.Var(this=file_format))
    )

    # User-provided options
    for key, value in (options or {}).items():
        option_exprs.append(
            exp.Property(
                this=exp.Var(this=key.upper()),
                value=exp.Literal.string(str(value)),
            )
        )

    copy_expr = exp.Copy(
        this=exp.Subquery(this=query),
        kind=exp.Var(this="TO"),
        files=[exp.Literal.string(path)],
        options=option_exprs,
    )

    # Add PARTITIONED BY if specified
    if partition_by:
        copy_expr.set(
            "partition",
            [exp.to_identifier(col) for col in partition_by],
        )

    return copy_expr


def build_external_table_ddl(
    *,
    name: str,
    location: DatasetLocation,
    profile: DataFusionRuntimeProfile | None = None,
    schema: dict[str, str] | None = None,
    partition_cols: tuple[str, ...] = (),
    file_format: str = "PARQUET",
    options: dict[str, str] | None = None,
) -> str:
    """
    Build CREATE EXTERNAL TABLE DDL via SQLGlot AST.

    Returns rendered SQL string for the target dialect.
    """
    # Build column definitions if schema provided
    columns = None
    if schema:
        columns = exp.Schema(
            expressions=[
                exp.ColumnDef(
                    this=exp.to_identifier(col_name),
                    kind=exp.DataType.build(col_type),
                )
                for col_name, col_type in schema.items()
            ]
        )

    # Build CREATE EXTERNAL TABLE expression
    create_expr = exp.Create(
        this=exp.Table(this=exp.to_identifier(name)),
        kind="EXTERNAL TABLE",
        expression=columns,
        properties=exp.Properties(
            expressions=[
                exp.FileFormatProperty(this=exp.Var(this=file_format)),
                exp.LocationProperty(this=exp.Literal.string(str(location.path))),
            ]
        ),
    )

    # Add OPTIONS if provided
    if options:
        option_props = [
            exp.Property(
                this=exp.Literal.string(k),
                value=exp.Literal.string(v),
            )
            for k, v in options.items()
        ]
        create_expr.args["properties"].append(
            exp.AnonymousProperty(this="OPTIONS", expressions=option_props)
        )

    # Render with appropriate dialect
    dialect = profile.sql_dialect if profile else "postgres"
    return create_expr.sql(dialect=dialect)


def build_insert_into_ast(
    *,
    target_table: str,
    source_query: exp.Expression,
    columns: tuple[str, ...] | None = None,
    overwrite: bool = False,
) -> exp.Insert:
    """Build INSERT INTO statement as SQLGlot AST."""
    insert_expr = exp.Insert(
        this=exp.Table(this=exp.to_identifier(target_table)),
        expression=source_query,
        overwrite=overwrite,
    )

    if columns:
        insert_expr.set(
            "columns",
            [exp.to_identifier(col) for col in columns],
        )

    return insert_expr
```

**Target files**
- `src/sqlglot_tools/ddl_builders.py` (new)
- `src/datafusion_engine/registry_bridge.py`
- `src/datafusion_engine/bridge.py`
- `src/engine/materialize_pipeline.py`
- `src/ibis_engine/io_bridge.py`

**Implementation checklist**
- [ ] Implement `build_copy_to_ast` with partition and options support.
- [ ] Implement `build_external_table_ddl` with schema and options support.
- [ ] Implement `build_insert_into_ast` for DML operations.
- [ ] Replace `_copy_select_sql`, `_sql_identifier`, and ad-hoc SQL assembly.
- [ ] Ensure dialect is always explicit and aligned with DataFusion policy.
- [ ] Add tests comparing AST fingerprints rather than formatted SQL strings.
- [ ] Add golden tests for DDL output stability.

---

### 3) Parameter Binding Unification (Scalar + Table-like)

**Intent**
Standardize parameter handling for all SQL execution paths. Enforce a two-lane model:
scalar params use `param_values`, table-like params use named parameter registration.

**Representative pattern**
```python
# src/datafusion_engine/param_binding.py
from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Mapping

import pyarrow as pa

if TYPE_CHECKING:
    from ibis.expr.types import Value
    from datafusion import DataFrame


@dataclass(frozen=True)
class DataFusionParamBindings:
    """Resolved parameter bindings for SQL execution."""

    param_values: dict[str, Any] = field(default_factory=dict)
    named_tables: dict[str, DataFrame | pa.Table] = field(default_factory=dict)

    def merge(self, other: DataFusionParamBindings) -> DataFusionParamBindings:
        """Merge two binding sets, raising on conflicts."""
        overlapping = set(self.param_values) & set(other.param_values)
        if overlapping:
            raise ValueError(f"Conflicting scalar params: {overlapping}")

        overlapping_tables = set(self.named_tables) & set(other.named_tables)
        if overlapping_tables:
            raise ValueError(f"Conflicting table params: {overlapping_tables}")

        return DataFusionParamBindings(
            param_values={**self.param_values, **other.param_values},
            named_tables={**self.named_tables, **other.named_tables},
        )


# Allowlist for parameter names (security boundary)
ALLOWED_PARAM_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def validate_param_name(name: str) -> None:
    """Validate parameter name against allowlist."""
    if not ALLOWED_PARAM_PATTERN.match(name):
        raise ValueError(
            f"Invalid parameter name '{name}': must match [a-zA-Z_][a-zA-Z0-9_]*"
        )


def resolve_param_bindings(
    values: Mapping[str, Any] | Mapping[Value, Any] | None,
    *,
    validate_names: bool = True,
) -> DataFusionParamBindings:
    """
    Resolve parameter bindings into scalar vs table-like lanes.

    Parameters
    ----------
    values
        Mapping of parameter names/expressions to values.
        - Scalar values (int, str, float, etc.) → param_values
        - Table-like values (DataFrame, Table) → named_tables
    validate_names
        Whether to validate parameter names against allowlist.

    Returns
    -------
    DataFusionParamBindings
        Resolved bindings ready for execution.
    """
    if values is None:
        return DataFusionParamBindings()

    param_values: dict[str, Any] = {}
    named_tables: dict[str, Any] = {}

    for key, value in values.items():
        # Extract name from Ibis scalar param or use string key
        if hasattr(key, "get_name"):
            name = key.get_name()
        else:
            name = str(key)

        if validate_names:
            validate_param_name(name)

        # Route based on value type
        if isinstance(value, (pa.Table, pa.RecordBatch)):
            named_tables[name] = value
        elif hasattr(value, "to_pyarrow"):
            # DataFrame-like with Arrow export
            named_tables[name] = value
        else:
            # Scalar value
            param_values[name] = value

    return DataFusionParamBindings(
        param_values=param_values,
        named_tables=named_tables,
    )


def apply_bindings_to_context(
    ctx: SessionContext,
    bindings: DataFusionParamBindings,
) -> None:
    """Register table-like bindings into context before execution."""
    for name, table in bindings.named_tables.items():
        if isinstance(table, pa.Table):
            ctx.register_table(name, table)
        elif hasattr(table, "to_pyarrow"):
            ctx.register_table(name, table.to_pyarrow())
        else:
            ctx.register_table(name, table)
```

**Target files**
- `src/datafusion_engine/param_binding.py` (new)
- `src/datafusion_engine/bridge.py`
- `src/ibis_engine/params_bridge.py`
- `src/engine/materialize_pipeline.py`

**Implementation checklist**
- [ ] Build unified resolver returning `param_values` + `named_tables`.
- [ ] Enforce allowlists for parameter names in one place.
- [ ] Route all SQL execution through this resolver.
- [ ] Delete ad-hoc parameter checks from bridge modules.
- [ ] Add diagnostics for param modes and signatures.
- [ ] Add tests for Ibis scalar params, Arrow tables, and DataFrames.

---

### 4) SQLGlot Optimizer Integration (Canonicalization Pipeline)

**Intent**
Integrate SQLGlot's full optimizer as a canonicalization engine. Pin the optimizer rule
list as part of the compiler contract for deterministic SQL shapes and stable fingerprints.

**Representative pattern**
```python
# src/datafusion_engine/sql_policy_engine.py
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable
import hashlib
import json

from sqlglot import exp
from sqlglot.optimizer import RULES, optimize
from sqlglot.optimizer.qualify import qualify
from sqlglot.optimizer.qualify_columns import validate_qualify_columns
from sqlglot.optimizer.annotate_types import annotate_types
from sqlglot.optimizer.canonicalize import canonicalize
from sqlglot.optimizer.simplify import simplify
from sqlglot.optimizer.normalize import normalize, normalization_distance
from sqlglot.optimizer.pushdown_predicates import pushdown_predicates
from sqlglot.optimizer.pushdown_projections import pushdown_projections
from sqlglot.schema import MappingSchema
from sqlglot import serde


@dataclass(frozen=True)
class SQLPolicyProfile:
    """
    Compiler policy contract - pinned for determinism.

    This profile defines how SQL is canonicalized and must be
    included in cache keys to ensure reproducibility.
    """

    read_dialect: str = "postgres"
    write_dialect: str = "postgres"
    optimizer_rules: tuple[Callable, ...] = RULES
    normalize_distance_limit: int = 128
    pushdown_projections: bool = True
    pushdown_predicates: bool = True
    expand_stars: bool = True
    validate_qualify_columns: bool = True
    identify_mode: bool | str = False  # False, True, or "safe"
    pretty_output: bool = False

    def policy_fingerprint(self) -> str:
        """Hash of policy settings for cache keys."""
        policy_dict = {
            "read_dialect": self.read_dialect,
            "write_dialect": self.write_dialect,
            "rules": [r.__name__ for r in self.optimizer_rules],
            "normalize_limit": self.normalize_distance_limit,
            "pushdown_proj": self.pushdown_projections,
            "pushdown_pred": self.pushdown_predicates,
            "expand_stars": self.expand_stars,
            "identify": self.identify_mode,
        }
        return hashlib.sha256(
            json.dumps(policy_dict, sort_keys=True).encode()
        ).hexdigest()[:16]


@dataclass
class CompilationArtifacts:
    """Artifacts produced during SQL compilation."""

    serde_payload: list  # JSON-serializable AST
    ast_fingerprint: str
    lineage_by_column: dict[str, set[tuple[str, str]]]  # col -> {(table, col)}
    qualification_errors: list[str] = field(default_factory=list)
    normalization_skipped: bool = False
    normalization_distance: int = 0

    @classmethod
    def from_ast(
        cls,
        ast: exp.Expression,
        schema: MappingSchema,
    ) -> CompilationArtifacts:
        """Build artifacts from canonicalized AST."""
        payload = serde.dump(ast)
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
    schema: MappingSchema | dict,
    profile: SQLPolicyProfile,
    original_sql: str | None = None,
) -> tuple[exp.Expression, CompilationArtifacts]:
    """
    Full policy-aware SQL canonicalization pipeline.

    Parameters
    ----------
    expr
        Parsed SQLGlot expression.
    schema
        Schema for qualification and type inference.
    profile
        Compiler policy settings.
    original_sql
        Original SQL string for error highlighting.

    Returns
    -------
    tuple[exp.Expression, CompilationArtifacts]
        Canonicalized AST and compilation artifacts.

    Raises
    ------
    OptimizeError
        If qualification fails and validate_qualify_columns is True.
    """
    # Ensure schema is MappingSchema
    if isinstance(schema, dict):
        schema = MappingSchema(schema)

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
    """
    Extract column-level lineage from qualified AST.

    Returns mapping of output column -> set of (source_table, source_column).
    """
    from sqlglot.lineage import lineage
    from sqlglot.optimizer.scope import build_scope

    result: dict[str, set[tuple[str, str]]] = {}

    # Get output columns from SELECT
    select = ast.find(exp.Select)
    if not select:
        return result

    # Build scope once for efficiency
    scope = build_scope(ast)

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
                if hasattr(node, "source") and hasattr(node, "column"):
                    sources.add((node.source.name, node.column))

            result[col_name] = sources
        except Exception:
            # Lineage may fail for complex expressions
            result[col_name] = set()

    return result


def render_for_execution(
    ast: exp.Expression,
    profile: SQLPolicyProfile,
) -> str:
    """Render canonicalized AST to SQL string for execution."""
    return ast.sql(
        dialect=profile.write_dialect,
        pretty=profile.pretty_output,
        identify=profile.identify_mode,
    )
```

**Target files**
- `src/datafusion_engine/sql_policy_engine.py` (new)
- `src/datafusion_engine/compile_pipeline.py`
- `src/sqlglot_tools/lineage.py`
- `src/datafusion_engine/bridge.py`

**Implementation checklist**
- [ ] Create `SQLPolicyProfile` with all canonicalization knobs.
- [ ] Implement `compile_sql_policy` with full optimizer integration.
- [ ] Implement `extract_column_lineage` using SQLGlot lineage with scope caching.
- [ ] Implement `CompilationArtifacts` with serde-based fingerprinting.
- [ ] Add `policy_fingerprint` method for cache key inclusion.
- [ ] Add tests for normalization distance gating.
- [ ] Add tests for lineage extraction accuracy.
- [ ] Add golden tests for canonical SQL output stability.

---

### 5) Single Compilation Lane Orchestration (Substrait → AST → SQL)

**Intent**
Make compilation and execution flow deterministic and centralized. Every plan must pass
through a single orchestration entrypoint that produces checkpointed artifacts.

**Representative pattern**
```python
# src/datafusion_engine/compile_pipeline.py
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from datafusion import DataFrame, SessionContext
from sqlglot import exp

if TYPE_CHECKING:
    from ibis.expr.types import Table as IbisTable
    from datafusion_engine.sql_policy_engine import (
        SQLPolicyProfile,
        CompilationArtifacts,
    )


@dataclass(frozen=True)
class CompiledExpression:
    """
    Triple checkpoint: Ibis IR + SQLGlot AST + rendered SQL.

    This is the canonical compilation result that flows through
    the execution pipeline.
    """

    ibis_expr: IbisTable | None
    sqlglot_ast: exp.Expression
    rendered_sql: str
    artifacts: CompilationArtifacts

    @property
    def fingerprint(self) -> str:
        """Cache key combining AST fingerprint and policy."""
        return self.artifacts.ast_fingerprint

    @property
    def lineage(self) -> dict[str, set[tuple[str, str]]]:
        """Column-level lineage graph."""
        return self.artifacts.lineage_by_column


@dataclass(frozen=True)
class CompileOptions:
    """Options controlling compilation behavior."""

    prefer_substrait: bool = False
    prefer_ast_execution: bool = True  # Use raw_sql(ast) when available
    profile: SQLPolicyProfile = None
    schema: dict | None = None

    def __post_init__(self):
        if self.profile is None:
            object.__setattr__(self, "profile", SQLPolicyProfile())


class CompilationPipeline:
    """
    Unified compilation pipeline for all expression types.

    Supports Ibis expressions, raw SQL, and SQLGlot ASTs.
    """

    def __init__(
        self,
        ctx: SessionContext,
        options: CompileOptions,
    ):
        self.ctx = ctx
        self.options = options
        self._schema_cache: dict | None = None

    def compile_ibis(
        self,
        expr: IbisTable,
        *,
        backend: Backend | None = None,
    ) -> CompiledExpression:
        """Compile Ibis expression through SQLGlot canonicalization."""
        from ibis.backends.datafusion import Backend

        if backend is None:
            backend = Backend.from_connection(self.ctx)

        # 1. Ibis → SQLGlot AST (documented internal API)
        raw_ast = backend.compiler.to_sqlglot(expr)

        # 2. Get schema from introspection or cache
        schema = self._get_schema()

        # 3. Apply policy canonicalization
        from datafusion_engine.sql_policy_engine import (
            compile_sql_policy,
            render_for_execution,
        )

        canonical_ast, artifacts = compile_sql_policy(
            raw_ast,
            schema=schema,
            profile=self.options.profile,
        )

        # 4. Render for execution
        rendered = render_for_execution(canonical_ast, self.options.profile)

        return CompiledExpression(
            ibis_expr=expr,
            sqlglot_ast=canonical_ast,
            rendered_sql=rendered,
            artifacts=artifacts,
        )

    def compile_sql(
        self,
        sql: str,
    ) -> CompiledExpression:
        """Compile raw SQL through SQLGlot canonicalization."""
        from sqlglot import parse_one
        from datafusion_engine.sql_policy_engine import (
            compile_sql_policy,
            render_for_execution,
        )

        # Parse with source dialect
        raw_ast = parse_one(sql, dialect=self.options.profile.read_dialect)

        # Get schema
        schema = self._get_schema()

        # Canonicalize
        canonical_ast, artifacts = compile_sql_policy(
            raw_ast,
            schema=schema,
            profile=self.options.profile,
            original_sql=sql,
        )

        # Render
        rendered = render_for_execution(canonical_ast, self.options.profile)

        return CompiledExpression(
            ibis_expr=None,
            sqlglot_ast=canonical_ast,
            rendered_sql=rendered,
            artifacts=artifacts,
        )

    def execute(
        self,
        compiled: CompiledExpression,
        **params,
    ) -> DataFrame:
        """Execute compiled expression and return DataFrame."""
        from datafusion_engine.param_binding import (
            resolve_param_bindings,
            apply_bindings_to_context,
        )

        # Resolve parameters
        bindings = resolve_param_bindings(params)

        # Register table params
        apply_bindings_to_context(self.ctx, bindings)

        # Execute with scalar params
        if self.options.prefer_ast_execution:
            # DataFusion backend supports raw_sql(sge.Expression)
            return self.ctx.sql(compiled.rendered_sql, **bindings.param_values)
        else:
            return self.ctx.sql(compiled.rendered_sql, **bindings.param_values)

    def compile_and_execute(
        self,
        expr: IbisTable | str,
        **params,
    ) -> DataFrame:
        """Convenience method for compile + execute."""
        if isinstance(expr, str):
            compiled = self.compile_sql(expr)
        else:
            compiled = self.compile_ibis(expr)

        return self.execute(compiled, **params)

    def _get_schema(self) -> dict:
        """Get schema from introspection or cache."""
        if self._schema_cache is not None:
            return self._schema_cache

        if self.options.schema:
            self._schema_cache = self.options.schema
        else:
            # Build from introspection
            from datafusion_engine.schema_introspection import (
                build_schema_from_introspection,
            )
            self._schema_cache = build_schema_from_introspection(self.ctx)

        return self._schema_cache
```

**Target files**
- `src/datafusion_engine/compile_pipeline.py` (new)
- `src/datafusion_engine/bridge.py`
- `src/ibis_engine/execution.py`
- `src/ibis_engine/query_compiler.py`

**Implementation checklist**
- [ ] Create `CompiledExpression` dataclass with triple checkpoint.
- [ ] Create `CompilationPipeline` class with Ibis and SQL paths.
- [ ] Implement schema caching from introspection.
- [ ] Route all execution through `CompilationPipeline.execute`.
- [ ] Remove duplicate compile logic from Ibis execution and DataFusion bridge.
- [ ] Record lane selection and fallback reasons in diagnostics.
- [ ] Add integration tests for Ibis → SQLGlot → DataFusion flow.

---

## Phase 2: Streaming Execution

### 6) Arrow C Stream Execution Model

**Intent**
Establish streaming-first execution via Arrow C Stream protocol. DataFusion DataFrames
implement `__arrow_c_stream__`, enabling zero-copy streaming without full materialization.

**Representative pattern**
```python
# src/datafusion_engine/streaming_executor.py
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Iterator, Callable

import pyarrow as pa
import pyarrow.dataset as ds

if TYPE_CHECKING:
    from datafusion import DataFrame, SessionContext


@dataclass(frozen=True)
class StreamingExecutionResult:
    """
    Result that can be consumed as Arrow stream OR collected.

    Implements lazy evaluation - no data is materialized until
    a terminal operation is called.
    """

    df: DataFrame

    @property
    def schema(self) -> pa.Schema:
        """Get schema without executing."""
        return self.df.schema()

    def to_arrow_stream(self) -> pa.RecordBatchReader:
        """
        Zero-copy streaming via Arrow C Stream protocol.

        DataFusion DataFrames implement __arrow_c_stream__,
        allowing direct consumption as Arrow streams.
        """
        return pa.RecordBatchReader.from_stream(self.df)

    def to_batches(
        self,
        chunk_size: int | None = None,
    ) -> Iterator[pa.RecordBatch]:
        """
        Iterate over record batches.

        Note: chunk_size is advisory; actual batch sizes depend on
        upstream operators and DataFusion's execution config.
        """
        reader = self.to_arrow_stream()
        for batch in reader:
            yield batch

    def to_table(self) -> pa.Table:
        """Materialize full result as Arrow Table."""
        return pa.Table.from_batches(
            list(self.to_batches()),
            schema=self.schema,
        )

    def to_pandas(self) -> pd.DataFrame:
        """Materialize as pandas DataFrame."""
        return self.to_table().to_pandas()

    def pipe_to_dataset(
        self,
        base_dir: str,
        *,
        format: str = "parquet",
        partitioning: ds.Partitioning | list[str] | None = None,
        existing_data_behavior: str = "error",
        file_visitor: Callable[[str], None] | None = None,
        max_partitions: int = 1024,
        max_open_files: int = 1024,
        max_rows_per_file: int = 10_000_000,
        min_rows_per_group: int = 0,
        max_rows_per_group: int = 1_000_000,
        **format_options,
    ) -> None:
        """
        Stream directly to partitioned dataset.

        Parameters
        ----------
        base_dir
            Root directory for output dataset.
        format
            Output format (parquet, csv, feather).
        partitioning
            Partitioning scheme (hive-style by default).
        existing_data_behavior
            How to handle existing data: "error", "overwrite_or_ignore", "delete_matching".
        file_visitor
            Callback for each written file (useful for metadata sidecars).
        max_partitions
            Maximum number of partitions to write.
        max_open_files
            Maximum number of files open simultaneously.
        max_rows_per_file
            Maximum rows per output file.
        min_rows_per_group
            Minimum rows per row group (Parquet).
        max_rows_per_group
            Maximum rows per row group (Parquet).
        **format_options
            Format-specific options (compression, etc.).
        """
        # Build partitioning if list of column names
        if isinstance(partitioning, list):
            partitioning = ds.partitioning(
                pa.schema([(col, pa.string()) for col in partitioning]),
                flavor="hive",
            )

        # Build format options
        if format == "parquet":
            file_options = ds.ParquetFileFormat().make_write_options(**format_options)
        else:
            file_options = None

        ds.write_dataset(
            self.to_arrow_stream(),
            base_dir=base_dir,
            format=format,
            partitioning=partitioning,
            existing_data_behavior=existing_data_behavior,
            file_visitor=file_visitor,
            max_partitions=max_partitions,
            max_open_files=max_open_files,
            max_rows_per_file=max_rows_per_file,
            min_rows_per_group=min_rows_per_group,
            max_rows_per_group=max_rows_per_group,
            file_options=file_options,
        )

    def pipe_to_parquet(
        self,
        path: str,
        *,
        compression: str = "zstd",
        compression_level: int | None = None,
        row_group_size: int = 1_000_000,
    ) -> None:
        """Stream to single Parquet file."""
        import pyarrow.parquet as pq

        with pq.ParquetWriter(
            path,
            self.schema,
            compression=compression,
            compression_level=compression_level,
        ) as writer:
            for batch in self.to_batches():
                writer.write_batch(batch)


class StreamingExecutor:
    """
    Executor that produces streaming results.

    All execution paths return StreamingExecutionResult,
    deferring materialization to the caller.
    """

    def __init__(self, ctx: SessionContext):
        self.ctx = ctx

    def execute_sql(
        self,
        sql: str,
        **params,
    ) -> StreamingExecutionResult:
        """Execute SQL and return streaming result."""
        df = self.ctx.sql(sql, **params)
        return StreamingExecutionResult(df=df)

    def execute_compiled(
        self,
        compiled: CompiledExpression,
        **params,
    ) -> StreamingExecutionResult:
        """Execute compiled expression and return streaming result."""
        df = self.ctx.sql(compiled.rendered_sql, **params)
        return StreamingExecutionResult(df=df)

    def from_table(
        self,
        table_name: str,
    ) -> StreamingExecutionResult:
        """Get streaming result from registered table."""
        df = self.ctx.table(table_name)
        return StreamingExecutionResult(df=df)
```

**Target files**
- `src/datafusion_engine/streaming_executor.py` (new)
- `src/datafusion_engine/write_pipeline.py`
- `src/engine/materialize_pipeline.py`

**Implementation checklist**
- [ ] Create `StreamingExecutionResult` with Arrow C Stream support.
- [ ] Implement `to_arrow_stream` using `pa.RecordBatchReader.from_stream`.
- [ ] Implement `pipe_to_dataset` with full partitioning support.
- [ ] Implement `pipe_to_parquet` for single-file output.
- [ ] Create `StreamingExecutor` class wrapping SessionContext.
- [ ] Add file visitor support for metadata sidecar generation.
- [ ] Add tests for streaming through large datasets.
- [ ] Add benchmarks comparing streaming vs collect performance.

---

### 7) Unified Write Pipeline (COPY/INSERT/Parquet Policy)

**Intent**
Provide a single writing surface with explicit format policy, partitioning, and schema
constraints, while avoiding inconsistent Ibis vs DataFusion write behavior.

**Representative pattern**
```python
# src/datafusion_engine/write_pipeline.py
from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING
from enum import Enum, auto

from sqlglot import exp

if TYPE_CHECKING:
    from datafusion import SessionContext
    from datafusion_engine.sql_policy_engine import SQLPolicyProfile


class WriteFormat(Enum):
    """Supported output formats."""
    PARQUET = auto()
    CSV = auto()
    JSON = auto()
    ARROW = auto()


class WriteMode(Enum):
    """Write behavior for existing data."""
    ERROR = auto()
    OVERWRITE = auto()
    APPEND = auto()


@dataclass(frozen=True)
class ParquetWritePolicy:
    """Parquet-specific write options."""

    compression: str = "zstd"
    compression_level: int | None = None
    row_group_size: int = 1_000_000
    data_page_size: int = 1_048_576
    dictionary_enabled: bool = True
    statistics_enabled: str = "page"  # none, chunk, page
    bloom_filter_enabled: bool = False
    bloom_filter_fpp: float = 0.05

    # Per-column overrides: column_name -> {option: value}
    column_overrides: dict[str, dict[str, str]] = field(default_factory=dict)

    def to_copy_options(self) -> dict[str, str]:
        """Convert to COPY statement options."""
        options = {
            "compression": self.compression,
            "row_group_size": str(self.row_group_size),
            "data_page_size": str(self.data_page_size),
            "dictionary_enabled": str(self.dictionary_enabled).lower(),
            "statistics_enabled": self.statistics_enabled,
        }

        if self.compression_level is not None:
            # Format: zstd(5)
            options["compression"] = f"{self.compression}({self.compression_level})"

        if self.bloom_filter_enabled:
            options["bloom_filter_enabled"] = "true"
            options["bloom_filter_fpp"] = str(self.bloom_filter_fpp)

        # Add per-column overrides
        for col, overrides in self.column_overrides.items():
            for opt, val in overrides.items():
                options[f"{opt}::{col}"] = val

        return options


@dataclass(frozen=True)
class WriteRequest:
    """
    Unified write request specification.

    Encapsulates all information needed to write a dataset,
    regardless of the underlying mechanism (COPY, INSERT, Arrow writer).
    """

    source: str | exp.Expression  # SQL query or AST
    destination: str  # Path or table name
    format: WriteFormat = WriteFormat.PARQUET
    mode: WriteMode = WriteMode.ERROR
    partition_by: tuple[str, ...] = ()
    parquet_policy: ParquetWritePolicy | None = None

    def to_copy_ast(
        self,
        profile: SQLPolicyProfile,
    ) -> exp.Copy:
        """Build COPY statement as SQLGlot AST."""
        from sqlglot_tools.ddl_builders import build_copy_to_ast
        from sqlglot import parse_one

        # Parse source if string
        if isinstance(self.source, str):
            query = parse_one(self.source, dialect=profile.read_dialect)
        else:
            query = self.source

        # Build options
        options = {"format": self.format.name}

        if self.parquet_policy:
            options.update(self.parquet_policy.to_copy_options())

        return build_copy_to_ast(
            query=query,
            path=self.destination,
            file_format=self.format.name,
            options=options,
            partition_by=self.partition_by,
        )


class WritePipeline:
    """
    Unified write pipeline for all output paths.

    Provides consistent write semantics across COPY, INSERT,
    and streaming Arrow writers.
    """

    def __init__(
        self,
        ctx: SessionContext,
        profile: SQLPolicyProfile,
    ):
        self.ctx = ctx
        self.profile = profile

    def write_via_copy(
        self,
        request: WriteRequest,
    ) -> None:
        """Write using SQL COPY statement."""
        copy_ast = request.to_copy_ast(self.profile)
        sql = copy_ast.sql(dialect=self.profile.write_dialect)

        # Execute COPY
        self.ctx.sql(sql).collect()

        # Record artifact
        self._record_write_artifact(request, sql)

    def write_via_streaming(
        self,
        request: WriteRequest,
    ) -> None:
        """Write using streaming Arrow writer."""
        from datafusion_engine.streaming_executor import (
            StreamingExecutor,
            StreamingExecutionResult,
        )

        executor = StreamingExecutor(self.ctx)

        # Get streaming result from source
        if isinstance(request.source, str):
            result = executor.execute_sql(request.source)
        else:
            sql = request.source.sql(dialect=self.profile.write_dialect)
            result = executor.execute_sql(sql)

        # Write based on format
        if request.format == WriteFormat.PARQUET:
            if request.partition_by:
                result.pipe_to_dataset(
                    request.destination,
                    partitioning=list(request.partition_by),
                    existing_data_behavior=self._mode_to_behavior(request.mode),
                    **(request.parquet_policy.to_copy_options() if request.parquet_policy else {}),
                )
            else:
                policy = request.parquet_policy or ParquetWritePolicy()
                result.pipe_to_parquet(
                    request.destination,
                    compression=policy.compression,
                    compression_level=policy.compression_level,
                    row_group_size=policy.row_group_size,
                )
        else:
            raise NotImplementedError(f"Streaming write for {request.format}")

        self._record_write_artifact(request, "streaming")

    def write(
        self,
        request: WriteRequest,
        *,
        prefer_streaming: bool = True,
    ) -> None:
        """
        Write using best available method.

        Prefers streaming for large outputs, COPY for simplicity.
        """
        if prefer_streaming and request.format == WriteFormat.PARQUET:
            self.write_via_streaming(request)
        else:
            self.write_via_copy(request)

    def _mode_to_behavior(self, mode: WriteMode) -> str:
        """Convert WriteMode to PyArrow existing_data_behavior."""
        return {
            WriteMode.ERROR: "error",
            WriteMode.OVERWRITE: "delete_matching",
            WriteMode.APPEND: "overwrite_or_ignore",
        }[mode]

    def _record_write_artifact(
        self,
        request: WriteRequest,
        method: str,
    ) -> None:
        """Record write operation in diagnostics."""
        # Implementation depends on diagnostics infrastructure
        pass
```

**Target files**
- `src/datafusion_engine/write_pipeline.py` (new)
- `src/engine/materialize_pipeline.py`
- `src/ibis_engine/io_bridge.py`
- `src/datafusion_engine/bridge.py`

**Implementation checklist**
- [ ] Define `WriteRequest` and `ParquetWritePolicy` dataclasses.
- [ ] Implement `WritePipeline` with COPY and streaming paths.
- [ ] Support per-column compression overrides in Parquet policy.
- [ ] Remove direct Parquet write paths where COPY/streaming is available.
- [ ] Ensure DataFusion write policy is applied consistently.
- [ ] Record write artifacts from a single source.
- [ ] Add tests for partitioned writes.
- [ ] Add tests for per-column compression options.

---

## Phase 3: Contracts and Validation

### 8) Unified Information Schema + Introspection Cache

**Intent**
Make information_schema-derived metadata the single source of truth for schema,
function catalog, and settings. Avoid separate metadata caches.

**Representative pattern**
```python
# src/datafusion_engine/introspection.py
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyarrow as pa

if TYPE_CHECKING:
    from datafusion import SessionContext


@dataclass(frozen=True)
class IntrospectionSnapshot:
    """
    Point-in-time snapshot of DataFusion catalog state.

    All schema/function queries should go through this snapshot
    to ensure consistency within a compilation unit.
    """

    tables: pa.Table
    columns: pa.Table
    routines: pa.Table | None
    parameters: pa.Table | None
    settings: pa.Table

    @classmethod
    def capture(cls, ctx: SessionContext) -> IntrospectionSnapshot:
        """Capture snapshot from SessionContext."""
        tables = ctx.sql("""
            SELECT table_catalog, table_schema, table_name, table_type
            FROM information_schema.tables
        """).to_arrow_table()

        columns = ctx.sql("""
            SELECT
                table_catalog, table_schema, table_name,
                column_name, ordinal_position, data_type,
                is_nullable, column_default
            FROM information_schema.columns
            ORDER BY table_catalog, table_schema, table_name, ordinal_position
        """).to_arrow_table()

        settings = ctx.sql("""
            SELECT name, value
            FROM information_schema.df_settings
        """).to_arrow_table()

        # Routines may not be available in all configurations
        try:
            routines = ctx.sql("""
                SELECT
                    specific_catalog, specific_schema, specific_name,
                    routine_catalog, routine_schema, routine_name,
                    routine_type, data_type
                FROM information_schema.routines
            """).to_arrow_table()

            parameters = ctx.sql("""
                SELECT
                    specific_catalog, specific_schema, specific_name,
                    ordinal_position, parameter_mode, parameter_name,
                    data_type
                FROM information_schema.parameters
                ORDER BY specific_name, ordinal_position
            """).to_arrow_table()
        except Exception:
            routines = None
            parameters = None

        return cls(
            tables=tables,
            columns=columns,
            routines=routines,
            parameters=parameters,
            settings=settings,
        )

    def schema_map(self) -> dict[str, dict[str, str]]:
        """
        Build schema mapping for SQLGlot optimizer.

        Returns
        -------
        dict[str, dict[str, str]]
            Mapping of table_name -> {column_name: data_type}
        """
        result: dict[str, dict[str, str]] = {}

        for row in self.columns.to_pylist():
            table_name = row["table_name"]
            if table_name not in result:
                result[table_name] = {}
            result[table_name][row["column_name"]] = row["data_type"]

        return result

    def qualified_schema_map(self) -> dict[str, dict[str, dict[str, str]]]:
        """
        Build fully qualified schema mapping.

        Returns
        -------
        dict[str, dict[str, dict[str, str]]]
            Mapping of catalog.schema -> table -> {column: type}
        """
        result: dict[str, dict[str, dict[str, str]]] = {}

        for row in self.columns.to_pylist():
            db_key = f"{row['table_catalog']}.{row['table_schema']}"
            table_name = row["table_name"]

            if db_key not in result:
                result[db_key] = {}
            if table_name not in result[db_key]:
                result[db_key][table_name] = {}

            result[db_key][table_name][row["column_name"]] = row["data_type"]

        return result

    def table_exists(self, name: str) -> bool:
        """Check if table exists in snapshot."""
        import pyarrow.compute as pc
        return pc.any(pc.equal(self.tables["table_name"], name)).as_py()

    def get_table_columns(self, name: str) -> list[tuple[str, str]]:
        """Get columns for a table as (name, type) pairs."""
        import pyarrow.compute as pc

        mask = pc.equal(self.columns["table_name"], name)
        filtered = self.columns.filter(mask)

        return [
            (row["column_name"], row["data_type"])
            for row in filtered.to_pylist()
        ]

    def function_signatures(self) -> dict[str, list[tuple[list[str], str]]]:
        """
        Build function signature map.

        Returns
        -------
        dict[str, list[tuple[list[str], str]]]
            Mapping of function_name -> [(param_types, return_type), ...]
        """
        if self.routines is None or self.parameters is None:
            return {}

        result: dict[str, list[tuple[list[str], str]]] = {}

        # Group parameters by function
        param_map: dict[str, list[str]] = {}
        for row in self.parameters.to_pylist():
            key = row["specific_name"]
            if key not in param_map:
                param_map[key] = []
            param_map[key].append(row["data_type"])

        # Build signatures
        for row in self.routines.to_pylist():
            name = row["routine_name"]
            key = row["specific_name"]
            return_type = row["data_type"] or "void"
            param_types = param_map.get(key, [])

            if name not in result:
                result[name] = []
            result[name].append((param_types, return_type))

        return result


class IntrospectionCache:
    """
    Cached introspection with invalidation support.

    Use this when you need multiple queries against the same
    catalog state within a single operation.
    """

    def __init__(self, ctx: SessionContext):
        self._ctx = ctx
        self._snapshot: IntrospectionSnapshot | None = None
        self._invalidated = False

    @property
    def snapshot(self) -> IntrospectionSnapshot:
        """Get or capture snapshot."""
        if self._snapshot is None or self._invalidated:
            self._snapshot = IntrospectionSnapshot.capture(self._ctx)
            self._invalidated = False
        return self._snapshot

    def invalidate(self) -> None:
        """Mark cache as stale (call after DDL changes)."""
        self._invalidated = True

    def schema_map(self) -> dict[str, dict[str, str]]:
        """Convenience accessor for schema map."""
        return self.snapshot.schema_map()
```

**Target files**
- `src/datafusion_engine/introspection.py` (new)
- `src/datafusion_engine/schema_introspection.py`
- `src/datafusion_engine/udf_catalog.py`
- `src/engine/function_registry.py`

**Implementation checklist**
- [ ] Create `IntrospectionSnapshot` dataclass with all info_schema queries.
- [ ] Implement `schema_map` for SQLGlot optimizer integration.
- [ ] Implement `function_signatures` for UDF registry.
- [ ] Create `IntrospectionCache` with invalidation support.
- [ ] Route function registry build to use snapshot, not direct queries.
- [ ] Invalidate snapshots centrally when DDL changes tables.
- [ ] Add tests for snapshot accuracy.
- [ ] Add tests for invalidation behavior.

---

### 9) Schema Contract Validation Layer

**Intent**
Provide declarative schema contracts that can be validated against introspection
and used to generate DDL. Catch schema drift at compile time.

**Representative pattern**
```python
# src/datafusion_engine/schema_contracts.py
from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING
from enum import Enum, auto

import pyarrow as pa

if TYPE_CHECKING:
    from datafusion_engine.introspection import IntrospectionSnapshot


class SchemaViolationType(Enum):
    """Types of schema violations."""
    MISSING_COLUMN = auto()
    EXTRA_COLUMN = auto()
    TYPE_MISMATCH = auto()
    NULLABILITY_MISMATCH = auto()
    MISSING_TABLE = auto()


@dataclass(frozen=True)
class SchemaViolation:
    """A single schema contract violation."""

    violation_type: SchemaViolationType
    table_name: str
    column_name: str | None
    expected: str | None
    actual: str | None

    def __str__(self) -> str:
        if self.violation_type == SchemaViolationType.MISSING_TABLE:
            return f"Table '{self.table_name}' not found"
        elif self.violation_type == SchemaViolationType.MISSING_COLUMN:
            return f"Column '{self.table_name}.{self.column_name}' not found"
        elif self.violation_type == SchemaViolationType.EXTRA_COLUMN:
            return f"Unexpected column '{self.table_name}.{self.column_name}'"
        elif self.violation_type == SchemaViolationType.TYPE_MISMATCH:
            return (
                f"Type mismatch for '{self.table_name}.{self.column_name}': "
                f"expected {self.expected}, got {self.actual}"
            )
        elif self.violation_type == SchemaViolationType.NULLABILITY_MISMATCH:
            return (
                f"Nullability mismatch for '{self.table_name}.{self.column_name}': "
                f"expected {self.expected}, got {self.actual}"
            )
        return f"Unknown violation: {self.violation_type}"


class EvolutionPolicy(Enum):
    """Schema evolution policies."""
    STRICT = auto()      # No changes allowed
    ADDITIVE = auto()    # New columns allowed, no removals
    RELAXED = auto()     # Any compatible change allowed


@dataclass(frozen=True)
class ColumnContract:
    """Contract for a single column."""

    name: str
    arrow_type: pa.DataType
    nullable: bool = True
    description: str | None = None

    @classmethod
    def from_arrow_field(cls, field: pa.Field) -> ColumnContract:
        """Create from PyArrow field."""
        return cls(
            name=field.name,
            arrow_type=field.type,
            nullable=field.nullable,
        )

    def to_arrow_field(self) -> pa.Field:
        """Convert to PyArrow field."""
        return pa.field(self.name, self.arrow_type, nullable=self.nullable)


@dataclass(frozen=True)
class SchemaContract:
    """
    Declared schema contract for a dataset.

    Use this to define expected schemas and validate against
    actual catalog state.
    """

    table_name: str
    columns: tuple[ColumnContract, ...]
    partition_cols: tuple[str, ...] = ()
    ordering: tuple[str, ...] = ()
    evolution_policy: EvolutionPolicy = EvolutionPolicy.STRICT

    @classmethod
    def from_arrow_schema(
        cls,
        table_name: str,
        schema: pa.Schema,
        **kwargs,
    ) -> SchemaContract:
        """Create from PyArrow schema."""
        columns = tuple(
            ColumnContract.from_arrow_field(field)
            for field in schema
        )
        return cls(table_name=table_name, columns=columns, **kwargs)

    def to_arrow_schema(self) -> pa.Schema:
        """Convert to PyArrow schema."""
        return pa.schema([col.to_arrow_field() for col in self.columns])

    def validate_against_introspection(
        self,
        snapshot: IntrospectionSnapshot,
    ) -> list[SchemaViolation]:
        """
        Validate contract against actual catalog state.

        Returns list of violations (empty if valid).
        """
        violations: list[SchemaViolation] = []

        # Check table exists
        if not snapshot.table_exists(self.table_name):
            violations.append(SchemaViolation(
                violation_type=SchemaViolationType.MISSING_TABLE,
                table_name=self.table_name,
                column_name=None,
                expected=None,
                actual=None,
            ))
            return violations

        # Get actual columns
        actual_cols = {
            col_name: col_type
            for col_name, col_type in snapshot.get_table_columns(self.table_name)
        }

        expected_cols = {col.name: col for col in self.columns}

        # Check for missing columns
        for col_name, contract in expected_cols.items():
            if col_name not in actual_cols:
                violations.append(SchemaViolation(
                    violation_type=SchemaViolationType.MISSING_COLUMN,
                    table_name=self.table_name,
                    column_name=col_name,
                    expected=str(contract.arrow_type),
                    actual=None,
                ))
            else:
                # Check type compatibility
                actual_type = actual_cols[col_name]
                expected_type = self._arrow_type_to_sql(contract.arrow_type)
                if not self._types_compatible(expected_type, actual_type):
                    violations.append(SchemaViolation(
                        violation_type=SchemaViolationType.TYPE_MISMATCH,
                        table_name=self.table_name,
                        column_name=col_name,
                        expected=expected_type,
                        actual=actual_type,
                    ))

        # Check for extra columns (if strict policy)
        if self.evolution_policy == EvolutionPolicy.STRICT:
            for col_name in actual_cols:
                if col_name not in expected_cols:
                    violations.append(SchemaViolation(
                        violation_type=SchemaViolationType.EXTRA_COLUMN,
                        table_name=self.table_name,
                        column_name=col_name,
                        expected=None,
                        actual=actual_cols[col_name],
                    ))

        return violations

    def to_sqlglot_ddl(self, dialect: str = "postgres") -> str:
        """
        Generate CREATE TABLE DDL via SQLGlot.

        Uses Ibis schema → SQLGlot interop for type mapping.
        """
        import ibis
        from sqlglot import exp

        # Build Ibis schema
        ibis_schema = ibis.schema({
            col.name: self._arrow_type_to_ibis(col.arrow_type)
            for col in self.columns
        })

        # Get SQLGlot column defs
        column_defs = ibis_schema.to_sqlglot_column_defs(dialect)

        # Build CREATE TABLE
        create = exp.Create(
            this=exp.Table(this=exp.to_identifier(self.table_name)),
            kind="TABLE",
            expression=exp.Schema(expressions=column_defs),
        )

        return create.sql(dialect=dialect)

    def _arrow_type_to_sql(self, arrow_type: pa.DataType) -> str:
        """Convert Arrow type to SQL type string."""
        # Simplified mapping - extend as needed
        type_map = {
            pa.int64(): "Int64",
            pa.int32(): "Int32",
            pa.string(): "Utf8",
            pa.float64(): "Float64",
            pa.bool_(): "Boolean",
        }
        return type_map.get(arrow_type, str(arrow_type))

    def _arrow_type_to_ibis(self, arrow_type: pa.DataType) -> str:
        """Convert Arrow type to Ibis type string."""
        type_map = {
            pa.int64(): "int64",
            pa.int32(): "int32",
            pa.string(): "string",
            pa.float64(): "float64",
            pa.bool_(): "boolean",
        }
        return type_map.get(arrow_type, "string")

    def _types_compatible(self, expected: str, actual: str) -> bool:
        """Check if types are compatible."""
        # Normalize for comparison
        expected_norm = expected.lower().replace(" ", "")
        actual_norm = actual.lower().replace(" ", "")
        return expected_norm == actual_norm


@dataclass
class ContractRegistry:
    """Registry of schema contracts for validation."""

    contracts: dict[str, SchemaContract] = field(default_factory=dict)

    def register(self, contract: SchemaContract) -> None:
        """Register a schema contract."""
        self.contracts[contract.table_name] = contract

    def validate_all(
        self,
        snapshot: IntrospectionSnapshot,
    ) -> dict[str, list[SchemaViolation]]:
        """Validate all registered contracts."""
        return {
            name: contract.validate_against_introspection(snapshot)
            for name, contract in self.contracts.items()
        }

    def get_violations(
        self,
        snapshot: IntrospectionSnapshot,
    ) -> list[SchemaViolation]:
        """Get all violations across all contracts."""
        violations = []
        for contract in self.contracts.values():
            violations.extend(contract.validate_against_introspection(snapshot))
        return violations
```

**Target files**
- `src/datafusion_engine/schema_contracts.py` (new)
- `src/relspec/evidence.py`
- `src/cpg/schema_definitions.py`

**Implementation checklist**
- [ ] Create `SchemaContract` and `ColumnContract` dataclasses.
- [ ] Implement `validate_against_introspection` method.
- [ ] Implement `to_sqlglot_ddl` using Ibis schema → SQLGlot interop.
- [ ] Create `ContractRegistry` for managing multiple contracts.
- [ ] Define evolution policies (STRICT, ADDITIVE, RELAXED).
- [ ] Add tests for validation with various violation types.
- [ ] Add tests for DDL generation.

---

### 10) Semantic Diff for Incremental Rebuild

**Intent**
Use SQLGlot diff to detect semantic changes between query versions. Enable fine-grained
rebuild decisions based on what actually changed, not just text differences.

**Representative pattern**
```python
# src/datafusion_engine/semantic_diff.py
from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING
from enum import Enum, auto

from sqlglot import exp
from sqlglot.diff import diff, Insert, Remove, Move, Update, Keep

if TYPE_CHECKING:
    from datafusion_engine.sql_policy_engine import SQLPolicyProfile


class ChangeCategory(Enum):
    """Categories of semantic changes."""
    NONE = auto()               # No changes
    METADATA_ONLY = auto()      # Comments, formatting
    ADDITIVE = auto()           # New columns, no removals
    BREAKING = auto()           # Removals, type changes, join changes
    ROW_MULTIPLYING = auto()    # Changes that may affect row count


@dataclass(frozen=True)
class SemanticChange:
    """A single semantic change between query versions."""

    edit_type: str  # Insert, Remove, Move, Update, Keep
    node_type: str  # Expression type that changed
    description: str
    category: ChangeCategory

    @classmethod
    def from_edit(cls, edit) -> SemanticChange:
        """Create from SQLGlot diff edit."""
        edit_type = type(edit).__name__

        if isinstance(edit, Keep):
            return cls(
                edit_type=edit_type,
                node_type=type(edit.source).__name__,
                description="No change",
                category=ChangeCategory.NONE,
            )

        if isinstance(edit, Insert):
            node = edit.expression
            category = cls._categorize_insert(node)
            return cls(
                edit_type=edit_type,
                node_type=type(node).__name__,
                description=f"Added {type(node).__name__}",
                category=category,
            )

        if isinstance(edit, Remove):
            node = edit.expression
            return cls(
                edit_type=edit_type,
                node_type=type(node).__name__,
                description=f"Removed {type(node).__name__}",
                category=ChangeCategory.BREAKING,
            )

        if isinstance(edit, Move):
            return cls(
                edit_type=edit_type,
                node_type=type(edit.expression).__name__,
                description=f"Moved {type(edit.expression).__name__}",
                category=ChangeCategory.BREAKING,
            )

        if isinstance(edit, Update):
            return cls(
                edit_type=edit_type,
                node_type=type(edit.source).__name__,
                description=f"Updated {type(edit.source).__name__}",
                category=ChangeCategory.BREAKING,
            )

        return cls(
            edit_type=edit_type,
            node_type="Unknown",
            description="Unknown change",
            category=ChangeCategory.BREAKING,
        )

    @staticmethod
    def _categorize_insert(node: exp.Expression) -> ChangeCategory:
        """Categorize an insert based on what was added."""
        # Projections are typically additive
        if isinstance(node, (exp.Column, exp.Alias)):
            return ChangeCategory.ADDITIVE

        # UDTFs can multiply rows
        if isinstance(node, exp.Unnest):
            return ChangeCategory.ROW_MULTIPLYING

        # Joins can multiply rows
        if isinstance(node, exp.Join):
            return ChangeCategory.ROW_MULTIPLYING

        # Default to breaking for safety
        return ChangeCategory.BREAKING


@dataclass
class SemanticDiff:
    """
    Semantic diff between two query versions.

    Provides fine-grained change detection for incremental rebuild decisions.
    """

    edits: list = field(default_factory=list)
    changes: list[SemanticChange] = field(default_factory=list)

    @classmethod
    def compute(
        cls,
        old_ast: exp.Expression,
        new_ast: exp.Expression,
        *,
        matchings: set | None = None,
    ) -> SemanticDiff:
        """
        Compute semantic diff between two ASTs.

        Parameters
        ----------
        old_ast
            Previous query AST.
        new_ast
            New query AST.
        matchings
            Optional pre-matched node pairs to guide diff.

        Returns
        -------
        SemanticDiff
            Diff result with categorized changes.
        """
        edits = diff(old_ast, new_ast, matchings=matchings)
        changes = [SemanticChange.from_edit(edit) for edit in edits]

        return cls(edits=edits, changes=changes)

    @property
    def overall_category(self) -> ChangeCategory:
        """Get the most severe change category."""
        categories = [c.category for c in self.changes]

        if not categories or all(c == ChangeCategory.NONE for c in categories):
            return ChangeCategory.NONE

        if ChangeCategory.ROW_MULTIPLYING in categories:
            return ChangeCategory.ROW_MULTIPLYING

        if ChangeCategory.BREAKING in categories:
            return ChangeCategory.BREAKING

        if ChangeCategory.ADDITIVE in categories:
            return ChangeCategory.ADDITIVE

        return ChangeCategory.METADATA_ONLY

    def is_breaking(self) -> bool:
        """Check if diff contains breaking changes."""
        return self.overall_category in (
            ChangeCategory.BREAKING,
            ChangeCategory.ROW_MULTIPLYING,
        )

    def requires_rebuild(self, policy: RebuildPolicy) -> bool:
        """
        Check if diff requires downstream rebuild.

        Policy determines what counts as "requiring rebuild".
        """
        if policy == RebuildPolicy.ALWAYS:
            return self.overall_category != ChangeCategory.NONE

        if policy == RebuildPolicy.BREAKING_ONLY:
            return self.is_breaking()

        if policy == RebuildPolicy.CONSERVATIVE:
            # Rebuild for anything except metadata changes
            return self.overall_category not in (
                ChangeCategory.NONE,
                ChangeCategory.METADATA_ONLY,
            )

        return False

    def summary(self) -> str:
        """Generate human-readable summary of changes."""
        if self.overall_category == ChangeCategory.NONE:
            return "No semantic changes"

        by_type = {}
        for change in self.changes:
            if change.category != ChangeCategory.NONE:
                key = change.edit_type
                by_type[key] = by_type.get(key, 0) + 1

        parts = [f"{count} {typ}" for typ, count in by_type.items()]
        return f"Changes: {', '.join(parts)} ({self.overall_category.name})"


class RebuildPolicy(Enum):
    """Policies for when to trigger rebuilds."""
    ALWAYS = auto()           # Rebuild on any change
    BREAKING_ONLY = auto()    # Rebuild only on breaking changes
    CONSERVATIVE = auto()     # Rebuild unless purely metadata


def compute_rebuild_needed(
    old_sql: str,
    new_sql: str,
    *,
    profile: SQLPolicyProfile,
    schema: dict,
    policy: RebuildPolicy = RebuildPolicy.CONSERVATIVE,
) -> tuple[bool, SemanticDiff]:
    """
    Determine if SQL change requires downstream rebuild.

    Parameters
    ----------
    old_sql
        Previous SQL query.
    new_sql
        New SQL query.
    profile
        SQL policy for canonicalization.
    schema
        Schema for qualification.
    policy
        Rebuild decision policy.

    Returns
    -------
    tuple[bool, SemanticDiff]
        Whether rebuild is needed and the diff details.
    """
    from sqlglot import parse_one
    from datafusion_engine.sql_policy_engine import compile_sql_policy

    # Parse and canonicalize both versions
    old_ast = parse_one(old_sql, dialect=profile.read_dialect)
    new_ast = parse_one(new_sql, dialect=profile.read_dialect)

    old_canonical, _ = compile_sql_policy(old_ast, schema=schema, profile=profile)
    new_canonical, _ = compile_sql_policy(new_ast, schema=schema, profile=profile)

    # Compute diff on canonical forms
    diff_result = SemanticDiff.compute(old_canonical, new_canonical)

    return diff_result.requires_rebuild(policy), diff_result
```

**Target files**
- `src/datafusion_engine/semantic_diff.py` (new)
- `src/incremental/sqlglot_artifacts.py`
- `src/relspec/rustworkx_schedule.py`

**Implementation checklist**
- [ ] Create `SemanticDiff` class wrapping SQLGlot diff.
- [ ] Implement `SemanticChange` with categorization logic.
- [ ] Implement `ChangeCategory` enum with rebuild semantics.
- [ ] Create `RebuildPolicy` enum for configurable rebuild decisions.
- [ ] Implement `compute_rebuild_needed` end-to-end function.
- [ ] Add tests for various change types (additive, breaking, etc.).
- [ ] Add tests for row-multiplying detection (joins, unnest).
- [ ] Add golden tests for diff stability.

---

## Phase 4: Advanced Features

### 11) Parameterized Execution Model

**Intent**
Leverage Ibis `ibis.param` for templated rulepacks that compile once and execute
with different parameter values. Enables efficient multi-configuration runs.

**Representative pattern**
```python
# src/datafusion_engine/parameterized_execution.py
from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import ibis
from ibis.expr.types import Scalar

if TYPE_CHECKING:
    from datafusion import SessionContext
    from ibis.expr.types import Table as IbisTable


@dataclass
class ParameterSpec:
    """Specification for a typed parameter."""

    name: str
    ibis_type: str
    description: str | None = None
    default: Any = None

    def create_param(self) -> Scalar:
        """Create Ibis parameter scalar."""
        return ibis.param(self.ibis_type)


@dataclass
class ParameterizedRulepack:
    """
    Rulepack with typed parameters for dynamic execution.

    Compile once, execute with different parameter configurations.
    """

    name: str
    expr: IbisTable
    param_specs: dict[str, ParameterSpec] = field(default_factory=dict)
    _params: dict[str, Scalar] = field(default_factory=dict, repr=False)

    @classmethod
    def create(
        cls,
        name: str,
        builder: Callable[[dict[str, Scalar]], IbisTable],
        param_specs: list[ParameterSpec],
    ) -> ParameterizedRulepack:
        """
        Create parameterized rulepack from builder function.

        Parameters
        ----------
        name
            Rulepack identifier.
        builder
            Function that takes params dict and returns Ibis expression.
        param_specs
            Parameter specifications.

        Returns
        -------
        ParameterizedRulepack
            Ready-to-execute rulepack.
        """
        # Create param objects
        params = {spec.name: spec.create_param() for spec in param_specs}

        # Build expression with params
        expr = builder(params)

        return cls(
            name=name,
            expr=expr,
            param_specs={spec.name: spec for spec in param_specs},
            _params=params,
        )

    def execute(
        self,
        backend: Backend,
        values: dict[str, Any],
        *,
        validate: bool = True,
    ) -> pa.Table:
        """
        Execute with specific parameter values.

        Parameters
        ----------
        backend
            Ibis DataFusion backend.
        values
            Parameter name -> value mapping.
        validate
            Whether to validate parameter types.

        Returns
        -------
        pa.Table
            Execution result.
        """
        if validate:
            self._validate_values(values)

        # Map param objects to values
        param_mapping = {
            self._params[name]: value
            for name, value in values.items()
        }

        # Execute with params
        return self.expr.to_pyarrow(params=param_mapping)

    def execute_streaming(
        self,
        backend: Backend,
        values: dict[str, Any],
        *,
        chunk_size: int = 250_000,
    ) -> pa.RecordBatchReader:
        """Execute with streaming output."""
        param_mapping = {
            self._params[name]: value
            for name, value in values.items()
        }

        return self.expr.to_pyarrow_batches(
            params=param_mapping,
            chunk_size=chunk_size,
        )

    def compile_sql(
        self,
        backend: Backend,
        values: dict[str, Any] | None = None,
    ) -> str:
        """
        Compile to SQL, optionally with specific values.

        If values not provided, returns parameterized SQL.
        """
        if values:
            param_mapping = {
                self._params[name]: value
                for name, value in values.items()
            }
            return self.expr.compile(params=param_mapping)
        else:
            return self.expr.compile()

    def _validate_values(self, values: dict[str, Any]) -> None:
        """Validate parameter values against specs."""
        for name, spec in self.param_specs.items():
            if name not in values and spec.default is None:
                raise ValueError(f"Missing required parameter: {name}")

        for name in values:
            if name not in self.param_specs:
                raise ValueError(f"Unknown parameter: {name}")


# Example usage
def create_edge_filter_rulepack(edges: IbisTable) -> ParameterizedRulepack:
    """Create parameterized edge filter rulepack."""
    return ParameterizedRulepack.create(
        name="edge_filter",
        builder=lambda params: edges.filter(
            (edges.kind == params["kind"]) &
            (edges.confidence > params["threshold"])
        ),
        param_specs=[
            ParameterSpec(
                name="kind",
                ibis_type="string",
                description="Edge kind to filter",
            ),
            ParameterSpec(
                name="threshold",
                ibis_type="float64",
                description="Minimum confidence threshold",
                default=0.5,
            ),
        ],
    )


# Execute with different configurations
# call_edges = rulepack.execute(backend, {"kind": "CALL", "threshold": 0.8})
# import_edges = rulepack.execute(backend, {"kind": "IMPORT", "threshold": 0.5})
```

**Target files**
- `src/datafusion_engine/parameterized_execution.py` (new)
- `src/hamilton_pipeline/modules/task_execution.py`
- `src/relspec/runtime_artifacts.py`

**Implementation checklist**
- [ ] Create `ParameterSpec` for typed parameter definitions.
- [ ] Create `ParameterizedRulepack` with execute methods.
- [ ] Implement parameter validation against specs.
- [ ] Support streaming execution with params.
- [ ] Support SQL compilation with/without values.
- [ ] Add tests for various parameter types.
- [ ] Add tests for missing/unknown parameter handling.

---

### 12) Unified Function Registry (UDF Performance Ladder)

**Intent**
Define one function registry with explicit lane precedence (builtin → pyarrow → pandas → python)
and a single source-of-truth for built-ins, UDFs, and SQL expressions.

**Representative pattern**
```python
# src/engine/udf_registry.py
from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Callable, Any
from enum import Enum, auto
import logging

import ibis

if TYPE_CHECKING:
    from ibis.backends.datafusion import Backend


logger = logging.getLogger(__name__)


class UDFLane(Enum):
    """
    Performance tier for UDF execution.

    Listed in order of preference (fastest to slowest).
    """
    BUILTIN = auto()      # Backend native - fastest
    PYARROW = auto()      # Arrow compute kernels - vectorized
    PANDAS = auto()       # Pandas vectorized - good
    PYTHON = auto()       # Row-by-row - slow (last resort)


@dataclass(frozen=True)
class UDFSignature:
    """Type signature for a UDF."""

    arg_types: tuple[str, ...]
    return_type: str

    @classmethod
    def from_annotations(cls, func: Callable) -> UDFSignature:
        """Extract signature from function annotations."""
        hints = get_type_hints(func)
        return_hint = hints.pop("return", "string")
        arg_hints = tuple(hints.values())

        return cls(
            arg_types=tuple(cls._hint_to_ibis(h) for h in arg_hints),
            return_type=cls._hint_to_ibis(return_hint),
        )

    @staticmethod
    def _hint_to_ibis(hint: Any) -> str:
        """Convert Python type hint to Ibis type string."""
        type_map = {
            int: "int64",
            float: "float64",
            str: "string",
            bool: "boolean",
        }
        return type_map.get(hint, "string")


@dataclass(frozen=True)
class UDFSpec:
    """
    Specification for a user-defined function.

    Includes implementation, lane selection, and metadata.
    """

    name: str
    lane: UDFLane
    implementation: Callable
    signature: UDFSignature
    description: str | None = None
    catalog: str | None = None
    database: str | None = None

    def register(self, backend: Backend) -> None:
        """Register UDF with Ibis backend using appropriate decorator."""
        match self.lane:
            case UDFLane.BUILTIN:
                # Reference existing backend function
                @ibis.udf.scalar.builtin(
                    name=self.name,
                    catalog=self.catalog,
                    database=self.database,
                )
                def _builtin():
                    ...

            case UDFLane.PYARROW:
                # Vectorized over PyArrow arrays
                ibis.udf.scalar.pyarrow(
                    self.implementation,
                    name=self.name,
                    signature=(self.signature.arg_types, self.signature.return_type),
                )

            case UDFLane.PANDAS:
                # Vectorized over Pandas Series
                ibis.udf.scalar.pandas(
                    self.implementation,
                    name=self.name,
                    signature=(self.signature.arg_types, self.signature.return_type),
                )

            case UDFLane.PYTHON:
                # Row-by-row - emit warning
                logger.warning(
                    f"UDF '{self.name}' uses slow Python lane. "
                    "Consider upgrading to pyarrow or pandas."
                )
                ibis.udf.scalar.python(
                    self.implementation,
                    name=self.name,
                    signature=(self.signature.arg_types, self.signature.return_type),
                )


@dataclass
class FunctionRegistry:
    """
    Unified function registry for all UDFs.

    Provides single source of truth for function availability
    and lane selection.
    """

    _specs: dict[str, UDFSpec] = field(default_factory=dict)
    _registered: set[str] = field(default_factory=set)

    def register_spec(self, spec: UDFSpec) -> None:
        """Register a UDF specification."""
        if spec.name in self._specs:
            existing = self._specs[spec.name]
            if existing.lane.value < spec.lane.value:
                logger.info(
                    f"Keeping faster lane for '{spec.name}': "
                    f"{existing.lane.name} over {spec.lane.name}"
                )
                return

        self._specs[spec.name] = spec

    def register_builtin(
        self,
        name: str,
        *,
        catalog: str | None = None,
        database: str | None = None,
    ) -> None:
        """Register reference to backend builtin function."""
        self.register_spec(UDFSpec(
            name=name,
            lane=UDFLane.BUILTIN,
            implementation=lambda: None,  # Placeholder
            signature=UDFSignature((), "any"),
            catalog=catalog,
            database=database,
        ))

    def register_pyarrow(
        self,
        name: str,
        func: Callable,
        signature: UDFSignature | None = None,
    ) -> None:
        """Register PyArrow-based UDF (preferred for custom functions)."""
        if signature is None:
            signature = UDFSignature.from_annotations(func)

        self.register_spec(UDFSpec(
            name=name,
            lane=UDFLane.PYARROW,
            implementation=func,
            signature=signature,
        ))

    def register_pandas(
        self,
        name: str,
        func: Callable,
        signature: UDFSignature | None = None,
    ) -> None:
        """Register Pandas-based UDF."""
        if signature is None:
            signature = UDFSignature.from_annotations(func)

        self.register_spec(UDFSpec(
            name=name,
            lane=UDFLane.PANDAS,
            implementation=func,
            signature=signature,
        ))

    def register_python(
        self,
        name: str,
        func: Callable,
        signature: UDFSignature | None = None,
    ) -> None:
        """Register Python row-by-row UDF (slow - use as last resort)."""
        if signature is None:
            signature = UDFSignature.from_annotations(func)

        self.register_spec(UDFSpec(
            name=name,
            lane=UDFLane.PYTHON,
            implementation=func,
            signature=signature,
        ))

    def apply_to_backend(self, backend: Backend) -> None:
        """Register all UDFs with the backend."""
        for name, spec in self._specs.items():
            if name not in self._registered:
                spec.register(backend)
                self._registered.add(name)

    def merge_from_introspection(
        self,
        snapshot: IntrospectionSnapshot,
    ) -> None:
        """
        Merge builtin functions from introspection.

        This populates the registry with backend-native functions
        that don't need custom implementations.
        """
        for name in snapshot.function_signatures():
            if name not in self._specs:
                self.register_builtin(name)

    def get_lane_stats(self) -> dict[UDFLane, int]:
        """Get count of functions by lane."""
        stats = {lane: 0 for lane in UDFLane}
        for spec in self._specs.values():
            stats[spec.lane] += 1
        return stats

    def list_slow_functions(self) -> list[str]:
        """List functions using slow Python lane."""
        return [
            name for name, spec in self._specs.items()
            if spec.lane == UDFLane.PYTHON
        ]
```

**Target files**
- `src/engine/udf_registry.py` (new or refactored)
- `src/datafusion_engine/udf_registry.py`
- `src/ibis_engine/builtin_udfs.py`

**Implementation checklist**
- [ ] Define `UDFLane` enum with performance tiers.
- [ ] Create `UDFSpec` with lane selection and registration.
- [ ] Create `FunctionRegistry` with lane-aware registration.
- [ ] Implement lane precedence (prefer faster lanes).
- [ ] Add warning for Python lane usage.
- [ ] Merge builtins from introspection.
- [ ] Add tests for lane selection precedence.
- [ ] Add tests for backend registration.

---

### 13) SQL Execution Safety Gates

**Intent**
Use DataFusion's SQLOptions to gate SQL execution. Provide defense-in-depth even
when SQL is generated correctly.

**Representative pattern**
```python
# src/datafusion_engine/sql_safety.py
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING
from enum import Enum, auto

from datafusion import SQLOptions

if TYPE_CHECKING:
    from datafusion import SessionContext, DataFrame
    from datafusion_engine.runtime import DataFusionRuntimeProfile


class ExecutionContext(Enum):
    """Context determining what SQL operations are allowed."""

    QUERY_ONLY = auto()       # SELECT only
    QUERY_WITH_TEMP = auto()  # SELECT + temp table creation
    FULL_DDL = auto()         # All DDL operations
    FULL_DML = auto()         # All DML operations
    ADMIN = auto()            # Everything including statements


@dataclass(frozen=True)
class ExecutionPolicy:
    """
    Policy for what SQL operations are allowed.

    Use this to enforce principle of least privilege for SQL execution.
    """

    allow_ddl: bool = False
    allow_dml: bool = False
    allow_statements: bool = False  # SET, BEGIN TRANSACTION, etc.

    @classmethod
    def for_context(cls, context: ExecutionContext) -> ExecutionPolicy:
        """Create policy for a specific context."""
        match context:
            case ExecutionContext.QUERY_ONLY:
                return cls(allow_ddl=False, allow_dml=False, allow_statements=False)
            case ExecutionContext.QUERY_WITH_TEMP:
                return cls(allow_ddl=True, allow_dml=False, allow_statements=False)
            case ExecutionContext.FULL_DDL:
                return cls(allow_ddl=True, allow_dml=False, allow_statements=True)
            case ExecutionContext.FULL_DML:
                return cls(allow_ddl=True, allow_dml=True, allow_statements=True)
            case ExecutionContext.ADMIN:
                return cls(allow_ddl=True, allow_dml=True, allow_statements=True)

    def to_sql_options(self) -> SQLOptions:
        """Convert to DataFusion SQLOptions."""
        return (
            SQLOptions()
            .with_allow_ddl(self.allow_ddl)
            .with_allow_dml(self.allow_dml)
            .with_allow_statements(self.allow_statements)
        )


def statement_sql_options_for_profile(
    profile: DataFusionRuntimeProfile | None,
) -> SQLOptions:
    """Build SQLOptions from runtime profile."""
    if profile is None:
        # Default: query only
        return ExecutionPolicy.for_context(ExecutionContext.QUERY_ONLY).to_sql_options()

    return profile.execution_policy.to_sql_options()


def execute_with_policy(
    ctx: SessionContext,
    sql: str,
    policy: ExecutionPolicy,
    **params,
) -> DataFrame:
    """
    Execute SQL with policy validation.

    The query is validated against the policy before execution.
    Raises an error if the query would perform disallowed operations.
    """
    return ctx.sql_with_options(
        sql,
        policy.to_sql_options(),
        **params,
    )


def validate_sql_safety(
    sql: str,
    policy: ExecutionPolicy,
    *,
    dialect: str = "postgres",
) -> list[str]:
    """
    Validate SQL against policy without executing.

    Returns list of policy violations (empty if valid).
    """
    from sqlglot import parse_one
    from sqlglot import exp

    violations = []

    try:
        ast = parse_one(sql, dialect=dialect)
    except Exception as e:
        violations.append(f"Parse error: {e}")
        return violations

    # Check for DDL
    if not policy.allow_ddl:
        if isinstance(ast, (exp.Create, exp.Drop, exp.Alter)):
            violations.append(f"DDL not allowed: {type(ast).__name__}")

    # Check for DML
    if not policy.allow_dml:
        if isinstance(ast, (exp.Insert, exp.Update, exp.Delete)):
            violations.append(f"DML not allowed: {type(ast).__name__}")

        # COPY is also DML
        if isinstance(ast, exp.Copy):
            violations.append("COPY not allowed (DML)")

    # Check for statements
    if not policy.allow_statements:
        if isinstance(ast, (exp.Set, exp.Transaction)):
            violations.append(f"Statement not allowed: {type(ast).__name__}")

    # Check for external locations (high-risk)
    for location in ast.find_all(exp.LocationProperty):
        path = location.this.this if hasattr(location.this, "this") else str(location.this)
        if path.startswith(("s3://", "gs://", "az://", "http://", "https://")):
            violations.append(f"External location detected: {path[:50]}...")

    return violations


@dataclass
class SafeExecutor:
    """
    Executor with built-in safety policies.

    Provides a safe interface for SQL execution with
    configurable policies.
    """

    ctx: SessionContext
    default_policy: ExecutionPolicy

    def execute(
        self,
        sql: str,
        *,
        policy: ExecutionPolicy | None = None,
        validate_first: bool = True,
        **params,
    ) -> DataFrame:
        """
        Execute SQL with safety validation.

        Parameters
        ----------
        sql
            SQL query to execute.
        policy
            Override policy (uses default if not provided).
        validate_first
            Whether to validate before execution.
        **params
            Parameter values for the query.

        Returns
        -------
        DataFrame
            Execution result.

        Raises
        ------
        ValueError
            If SQL violates policy.
        """
        effective_policy = policy or self.default_policy

        if validate_first:
            violations = validate_sql_safety(sql, effective_policy)
            if violations:
                raise ValueError(
                    f"SQL policy violations: {'; '.join(violations)}"
                )

        return execute_with_policy(
            self.ctx,
            sql,
            effective_policy,
            **params,
        )
```

**Target files**
- `src/datafusion_engine/sql_safety.py` (new)
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/bridge.py`

**Implementation checklist**
- [ ] Create `ExecutionPolicy` with DDL/DML/statement flags.
- [ ] Create `ExecutionContext` enum for common policy patterns.
- [ ] Implement `execute_with_policy` using SQLOptions.
- [ ] Implement `validate_sql_safety` for pre-execution checks.
- [ ] Add external location detection for high-risk operations.
- [ ] Create `SafeExecutor` class for convenient safe execution.
- [ ] Add tests for each policy level.
- [ ] Add tests for violation detection.

---

### 14) Diagnostics and Observability Consolidation

**Intent**
All diagnostics should be emitted from a shared adapter/pipeline rather than from each
local subsystem. Ensure consistent payload shape across execution lanes.

**Representative pattern**
```python
# src/datafusion_engine/diagnostics.py
from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol
from datetime import datetime
import json

if TYPE_CHECKING:
    from datafusion_engine.runtime import DataFusionRuntimeProfile


class DiagnosticsSink(Protocol):
    """Protocol for diagnostics sinks."""

    def record_artifact(self, name: str, payload: dict[str, Any]) -> None:
        """Record a named artifact."""
        ...

    def record_metric(self, name: str, value: float, tags: dict[str, str]) -> None:
        """Record a metric value."""
        ...

    def record_event(self, name: str, properties: dict[str, Any]) -> None:
        """Record an event."""
        ...


@dataclass
class InMemoryDiagnosticsSink:
    """In-memory diagnostics sink for testing and debugging."""

    artifacts: list[tuple[str, dict]] = field(default_factory=list)
    metrics: list[tuple[str, float, dict]] = field(default_factory=list)
    events: list[tuple[str, dict]] = field(default_factory=list)

    def record_artifact(self, name: str, payload: dict[str, Any]) -> None:
        self.artifacts.append((name, payload))

    def record_metric(self, name: str, value: float, tags: dict[str, str]) -> None:
        self.metrics.append((name, value, tags))

    def record_event(self, name: str, properties: dict[str, Any]) -> None:
        self.events.append((name, properties))

    def get_artifacts(self, name: str) -> list[dict]:
        """Get all artifacts with given name."""
        return [p for n, p in self.artifacts if n == name]


@dataclass(frozen=True)
class DiagnosticsContext:
    """Context for diagnostics recording."""

    session_id: str
    operation_id: str
    start_time: datetime = field(default_factory=datetime.utcnow)
    tags: dict[str, str] = field(default_factory=dict)


class DiagnosticsRecorder:
    """
    Centralized diagnostics recorder.

    All diagnostics emission should go through this class
    to ensure consistent payload shapes.
    """

    def __init__(
        self,
        sink: DiagnosticsSink | None,
        context: DiagnosticsContext | None = None,
    ):
        self._sink = sink
        self._context = context or DiagnosticsContext(
            session_id="default",
            operation_id="default",
        )

    @property
    def enabled(self) -> bool:
        """Check if diagnostics are enabled."""
        return self._sink is not None

    def record_compilation(
        self,
        *,
        input_sql: str,
        output_sql: str,
        ast_fingerprint: str,
        policy_fingerprint: str,
        dialect: str,
        duration_ms: float,
        lineage: dict[str, Any] | None = None,
    ) -> None:
        """Record SQL compilation diagnostics."""
        if not self.enabled:
            return

        self._sink.record_artifact("sql_compilation", {
            "session_id": self._context.session_id,
            "operation_id": self._context.operation_id,
            "timestamp": datetime.utcnow().isoformat(),
            "input_sql": input_sql,
            "output_sql": output_sql,
            "ast_fingerprint": ast_fingerprint,
            "policy_fingerprint": policy_fingerprint,
            "dialect": dialect,
            "duration_ms": duration_ms,
            "lineage": lineage,
        })

        self._sink.record_metric(
            "compilation_duration_ms",
            duration_ms,
            {"dialect": dialect, **self._context.tags},
        )

    def record_execution(
        self,
        *,
        sql: str,
        ast_fingerprint: str,
        duration_ms: float,
        rows_produced: int | None = None,
        bytes_scanned: int | None = None,
        error: str | None = None,
    ) -> None:
        """Record SQL execution diagnostics."""
        if not self.enabled:
            return

        self._sink.record_artifact("sql_execution", {
            "session_id": self._context.session_id,
            "operation_id": self._context.operation_id,
            "timestamp": datetime.utcnow().isoformat(),
            "sql": sql,
            "ast_fingerprint": ast_fingerprint,
            "duration_ms": duration_ms,
            "rows_produced": rows_produced,
            "bytes_scanned": bytes_scanned,
            "error": error,
            "success": error is None,
        })

        self._sink.record_metric(
            "execution_duration_ms",
            duration_ms,
            {"success": str(error is None), **self._context.tags},
        )

    def record_write(
        self,
        *,
        destination: str,
        format: str,
        method: str,  # "copy", "streaming", "insert"
        rows_written: int | None = None,
        bytes_written: int | None = None,
        partitions: int | None = None,
        duration_ms: float,
    ) -> None:
        """Record write operation diagnostics."""
        if not self.enabled:
            return

        self._sink.record_artifact("write_operation", {
            "session_id": self._context.session_id,
            "operation_id": self._context.operation_id,
            "timestamp": datetime.utcnow().isoformat(),
            "destination": destination,
            "format": format,
            "method": method,
            "rows_written": rows_written,
            "bytes_written": bytes_written,
            "partitions": partitions,
            "duration_ms": duration_ms,
        })

    def record_registration(
        self,
        *,
        name: str,
        registration_type: str,  # "table", "view", "object_store"
        location: str | None = None,
        schema: dict | None = None,
    ) -> None:
        """Record table/view/store registration diagnostics."""
        if not self.enabled:
            return

        self._sink.record_artifact("registration", {
            "session_id": self._context.session_id,
            "operation_id": self._context.operation_id,
            "timestamp": datetime.utcnow().isoformat(),
            "name": name,
            "type": registration_type,
            "location": location,
            "schema": schema,
        })

    def record_cache_event(
        self,
        *,
        key: str,
        hit: bool,
        fingerprint: str,
    ) -> None:
        """Record cache hit/miss event."""
        if not self.enabled:
            return

        self._sink.record_event("cache_access", {
            "session_id": self._context.session_id,
            "key": key,
            "hit": hit,
            "fingerprint": fingerprint,
        })

        self._sink.record_metric(
            "cache_hit_rate",
            1.0 if hit else 0.0,
            self._context.tags,
        )


def record_artifact(
    profile: DataFusionRuntimeProfile | None,
    name: str,
    payload: dict[str, Any],
) -> None:
    """
    Convenience function for recording artifacts.

    Use this when you don't have a DiagnosticsRecorder instance.
    """
    if profile is None or profile.diagnostics_sink is None:
        return
    profile.diagnostics_sink.record_artifact(name, payload)
```

**Target files**
- `src/datafusion_engine/diagnostics.py` (new)
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/bridge.py`
- `src/engine/materialize_pipeline.py`
- `src/obs/*`

**Implementation checklist**
- [ ] Define `DiagnosticsSink` protocol.
- [ ] Create `InMemoryDiagnosticsSink` for testing.
- [ ] Create `DiagnosticsRecorder` with typed recording methods.
- [ ] Implement consistent payload shapes for each artifact type.
- [ ] Route all existing diagnostics through `DiagnosticsRecorder`.
- [ ] Add metrics recording alongside artifacts.
- [ ] Add tests for artifact recording.
- [ ] Add tests for payload shape consistency.

---

## Decommission and Delete List (Post-Implementation)

The following items should be removed once the above refactors are complete. This is a
decommission plan, not immediate deletion.

**Duplicate helpers**
- `src/engine/materialize_pipeline.py`: `_sql_identifier`, `_copy_select_sql`,
  `_copy_statement_overrides`, `_copy_options_payload`, `_deregister_table`
- `src/ibis_engine/io_bridge.py`: `_deregister_table`
- `src/datafusion_engine/runtime.py`: `_deregister_table` (replace with centralized helper)

**Legacy SQL assembly**
- Any direct string SQL assembly for COPY or CREATE EXTERNAL TABLE in:
  - `src/engine/materialize_pipeline.py`
  - `src/datafusion_engine/registry_bridge.py`
  - `src/ibis_engine/io_bridge.py`

**Obsolete metadata layers**
- Any direct `information_schema` querying that bypasses the new introspection snapshot.
  Target functions for removal or relocation:
  - `src/datafusion_engine/schema_introspection.py` query helpers once centralized
  - `src/datafusion_engine/udf_catalog.py` direct routine/parameter queries

**Execution duplication**
- Any duplicated compile/execution lane logic in:
  - `src/datafusion_engine/bridge.py`
  - `src/ibis_engine/execution.py`
  - `src/ibis_engine/query_compiler.py`

---

## Execution Phases (Summary)

### Phase 1: Core Unification
1. IO Adapter (catalog, registration, object stores)
2. SQLGlot AST-first DDL/COPY generation
3. Parameter binding unification
4. SQLGlot optimizer integration (canonicalization)
5. Single compilation lane orchestration

### Phase 2: Streaming Execution
6. Arrow C Stream execution model
7. Unified write pipeline (COPY/INSERT/streaming)

### Phase 3: Contracts and Validation
8. Information schema introspection cache
9. Schema contract validation layer
10. Semantic diff for incremental rebuild

### Phase 4: Advanced Features
11. Parameterized execution model
12. Unified function registry (UDF ladder)
13. SQL execution safety gates
14. Diagnostics consolidation

### Phase 5: Cleanup
- Remove decommissioned functions
- Run full lint/type gates
- Update documentation

---

## Acceptance Gates

- `uv run ruff check --fix`
- `uv run pyrefly check`
- `uv run pyright --warnings --pythonversion=3.13`
- All unit tests pass: `uv run pytest tests/unit/`
- No plan cache keys or diagnostics payloads change unexpectedly without approval.
- Streaming execution benchmarks show no regression.
- Schema contract violations caught at compile time (golden tests).
- Semantic diff correctly identifies breaking vs additive changes (golden tests).
