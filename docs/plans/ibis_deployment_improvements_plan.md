# Ibis Deployment Improvements Plan (Non-UDF Scope)

## Scope

This plan implements the Ibis deployment improvements from the review, **excluding any new builtin Ibis UDF work**. The focus is on consolidation, consistency, and artifact completeness across Ibis, DataFusion, and SQLGlot integration.

## Item 1: Centralize Ibis Backend + Execution Context Construction

### Goal
Make Ibis backend creation and execution context wiring consistent across all pipelines (validation, materialization, repro, incremental). This avoids drift in SQL options, DataFusion profiles, and object-store registration.

### Status
- Completed: factory used across all call sites, including session/runtime/normalize/relspec/hamilton/incremental/DataFusion helpers.

### Design / Architecture
Introduce a single factory module that builds `BaseBackend` and `IbisExecutionContext` from `ExecutionContext` or `DataFusionRuntimeProfile`, and require all call sites to use it.

```python
# src/ibis_engine/execution_factory.py
from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING

from ibis.backends import BaseBackend
from ibis.expr.types import Value as IbisValue

from arrowdsl.core.execution_context import ExecutionContext
from ibis_engine.backend import build_backend
from ibis_engine.config import IbisBackendConfig
from ibis_engine.execution import IbisExecutionContext

if TYPE_CHECKING:
    from datafusion_engine.runtime import AdapterExecutionPolicy, ExecutionLabel


def _backend_config_from_ctx(ctx: ExecutionContext) -> IbisBackendConfig:
    runtime = ctx.runtime
    return IbisBackendConfig(
        datafusion_profile=runtime.datafusion,
        fuse_selects=runtime.ibis_fuse_selects,
        default_limit=runtime.ibis_default_limit,
        default_dialect=runtime.ibis_default_dialect,
        interactive=runtime.ibis_interactive,
        # object_stores can be wired from runtime profile if needed
    )


def ibis_backend_from_ctx(ctx: ExecutionContext) -> BaseBackend:
    return build_backend(_backend_config_from_ctx(ctx))


def ibis_execution_from_ctx(
    ctx: ExecutionContext,
    *,
    params: Mapping[IbisValue, object] | None = None,
    execution_policy: AdapterExecutionPolicy | None = None,
    execution_label: ExecutionLabel | None = None,
) -> IbisExecutionContext:
    backend = ibis_backend_from_ctx(ctx)
    return IbisExecutionContext(
        ctx=ctx,
        ibis_backend=backend,
        params=params,
        execution_policy=execution_policy,
        execution_label=execution_label,
    )
```

### Target Files
- `src/ibis_engine/execution_factory.py` (new)
- `src/schema_spec/system.py`
- `src/engine/materialize_pipeline.py`
- `src/hamilton_pipeline/modules/outputs.py`
- `src/obs/repro.py`
- `src/incremental/relspec_update.py`
- `src/extract/session.py`
- `src/extract/helpers.py`
- `src/incremental/runtime.py`
- `src/relspec/rules/validation.py`
- `src/engine/session_factory.py`
- `src/relspec/graph.py`
- `src/relspec/compiler.py`
- `src/relspec/cpg/build_edges.py`
- `src/relspec/cpg/build_nodes.py`
- `src/relspec/cpg/build_props.py`
- `src/normalize/runner.py`
- `src/normalize/ibis_api.py`
- `src/hamilton_pipeline/modules/inputs.py`
- `src/hamilton_pipeline/modules/normalization.py`
- `src/incremental/publish.py`
- `src/datafusion_engine/runtime.py`

### Implementation Checklist
- [x] Add `execution_factory` module with ctx/profile helpers for backend + execution context.
- [x] Replace local helper functions that build backends or execution contexts in all call sites.
- [x] Ensure `IbisBackendConfig` values consistently reflect `ExecutionContext.runtime` settings for factory outputs.
- [x] Add a unit test verifying that SQL options (fuse_selects/default_limit/default_dialect) match runtime settings for factory outputs.

## Item 2: Consolidate Ibis Plan Catalog Implementations

### Goal
Avoid duplicated `IbisPlanCatalog` classes with diverging semantics. Provide a single, shared catalog that supports both plan and expression resolution with optional schema-based empty inputs.

### Status
- Completed: canonical `src/ibis_engine/catalog.py` added; local catalog implementations removed in normalize and CPG paths; call sites updated to `resolve_plan` / `resolve_expr`.

### Design / Architecture
Create a canonical `IbisPlanCatalog` in `ibis_engine.catalog` and replace per-module copies. Provide both `resolve_plan` and `resolve_expr` APIs.

```python
# src/ibis_engine/catalog.py
from __future__ import annotations

from dataclasses import dataclass, field
from typing import cast

import pyarrow as pa
from ibis.backends import BaseBackend
from ibis.expr.types import Table

from arrowdsl.core.execution_context import ExecutionContext
from arrowdsl.core.interop import TableLike
from arrowdsl.core.ordering import Ordering
from arrowdsl.schema.build import empty_table
from datafusion_engine.nested_tables import ViewReference
from ibis_engine.plan import IbisPlan
from ibis_engine.sources import (
    DatasetSource,
    SourceToIbisOptions,
    namespace_recorder_from_ctx,
    register_ibis_table,
    source_to_ibis,
)

IbisPlanSource = IbisPlan | Table | TableLike | ViewReference | DatasetSource


@dataclass
class IbisPlanCatalog:
    backend: BaseBackend
    tables: dict[str, IbisPlanSource] = field(default_factory=dict)

    def resolve_plan(
        self,
        name: str,
        *,
        ctx: ExecutionContext,
        label: str | None = None,
    ) -> IbisPlan | None:
        source = self.tables.get(name)
        if source is None:
            return None
        if isinstance(source, IbisPlan):
            return source
        if isinstance(source, ViewReference):
            expr = self.backend.table(source.name)
            plan = IbisPlan(expr=expr, ordering=Ordering.unordered())
            self.tables[name] = plan
            return plan
        if isinstance(source, Table):
            return IbisPlan(expr=source, ordering=Ordering.unordered())
        if isinstance(source, DatasetSource):
            msg = f"DatasetSource {name!r} must be materialized before Ibis compilation."
            raise TypeError(msg)
        plan = source_to_ibis(
            cast("TableLike", source),
            options=SourceToIbisOptions(
                backend=self.backend,
                name=label or name,
                ordering=Ordering.unordered(),
                namespace_recorder=namespace_recorder_from_ctx(ctx),
            ),
        )
        self.tables[name] = plan
        return plan

    def resolve_expr(
        self,
        name: str,
        *,
        ctx: ExecutionContext,
        schema: pa.Schema | None = None,
    ) -> Table:
        plan = self.resolve_plan(name, ctx=ctx, label=name)
        if plan is not None:
            return plan.expr
        if schema is None:
            msg = f"Unknown dataset reference: {name!r}."
            raise KeyError(msg)
        empty = empty_table(schema)
        plan = register_ibis_table(
            empty,
            options=SourceToIbisOptions(
                backend=self.backend,
                name=None,
                ordering=Ordering.unordered(),
                namespace_recorder=namespace_recorder_from_ctx(ctx),
            ),
        )
        return plan.expr
```

### Target Files
- `src/ibis_engine/catalog.py` (new)
- `src/normalize/ibis_plan_builders.py`
- `src/cpg/relationship_plans.py`
- `src/normalize/catalog.py`
- `src/normalize/ibis_bridge.py`

### Implementation Checklist
- [x] Introduce `ibis_engine.catalog.IbisPlanCatalog`.
- [x] Replace local `IbisPlanCatalog` definitions with imports from `ibis_engine.catalog`.
- [x] Update call sites to use `resolve_plan` / `resolve_expr` consistently.
- [x] Ensure missing-source handling behavior matches existing behavior (empty table for normalize, None or KeyError where expected).

## Item 3: Unify Arrow -> Ibis -> SQL Type Mapping

### Goal
Ensure a single, consistent schema/type mapping path for Ibis schemas and SQL/DDL generation (including nested types), avoiding drift between Arrow, SQLGlot, and DataFusion DDL logic.

### Status
- Completed: `sqlglot_column_sql` added; DDL generation now routes through SQLGlot column defs without Arrow-type fallbacks; schema-map helpers updated; coverage added for column-def parity.

### Design / Architecture
Promote a canonical mapping in `ibis_engine.schema_utils` and route all DDL generation through Ibis schema + SQLGlot column defs instead of bespoke string maps.

```python
# src/ibis_engine/schema_utils.py
from __future__ import annotations

import ibis
import pyarrow as pa
from sqlglot_tools.compat import Expression


def sqlglot_column_defs(
    schema: pa.Schema,
    *,
    dialect: str = "datafusion",
) -> list[Expression]:
    ibis_schema = ibis_schema_from_arrow(schema)
    return list(ibis_schema.to_sqlglot_column_defs(dialect=dialect))


def sqlglot_column_sql(
    schema: pa.Schema,
    *,
    dialect: str = "datafusion",
) -> list[str]:
    return [col.sql(dialect=dialect) for col in sqlglot_column_defs(schema, dialect=dialect)]
```

Use this for external table DDL assembly and any SQL column definition needs.

### Target Files
- `src/ibis_engine/schema_utils.py`
- `src/datafusion_engine/schema_introspection.py`
- `src/schema_spec/specs.py`
- `src/ibis_engine/params_bridge.py` (if SQL types for params should align with the same canonical map)

### Implementation Checklist
- [x] Add `sqlglot_column_sql` helper to `schema_utils`.
- [x] Route external table DDL through SQLGlot-based defs with partition and NOT NULL behavior.
- [x] Remove `_arrow_type_to_sql_type` fallback and route all DDL through SQLGlot column defs.
- [x] Ensure `TableSchemaSpec.to_sqlglot_column_defs` remains the authoritative place for constraints.
- [x] Add a test that verifies SQL column defs are identical across all DDL entrypoints.

## Item 4: Standardize List-Parameter Joins

### Goal
Use a single, consistent join policy for list-parameter filtering to avoid collision drift and simplify behavior (especially for semi-joins).

### Status
- Completed: `list_param_join` supports `JoinOptions`, `FileIdParamMacro` routed through it, and collision policy coverage added.

### Design / Architecture
Route all list-param joins through `list_param_join` and allow a `JoinOptions` override for consistent name-collision handling.

```python
# src/ibis_engine/params_bridge.py

def list_param_join(
    table: Table,
    *,
    param_table: Table,
    left_col: str,
    right_col: str | None = None,
    options: JoinOptions | None = None,
) -> Table:
    key = right_col or left_col
    return stable_join(
        table,
        param_table,
        [table[left_col] == param_table[key]],
        options=options or JoinOptions(how="semi"),
    )
```

Update `FileIdParamMacro` to use this entrypoint so the join policy is centralized.

### Target Files
- `src/ibis_engine/params_bridge.py`
- `src/ibis_engine/query_compiler.py`
- `src/incremental/relspec_update.py` (if it uses file-id filtered query specs)

### Implementation Checklist
- [x] Extend `list_param_join` to accept `JoinOptions`.
- [x] Replace `FileIdParamMacro` manual `semi_join` with `list_param_join`.
- [x] Add a unit test that confirms rname/lname policy is applied for list-param joins.

## Item 5: Consolidate Selector Pattern Translation

### Goal
Avoid multiple implementations for `SelectorPattern -> ibis.selectors` translation so selector behavior is consistent everywhere.

### Status
- Completed: selector helper consolidated and test coverage added for numeric/string/temporal/regex patterns.

### Design / Architecture
Create a single helper in `selector_utils` and use it from both `selector_utils` and `macros`.

```python
# src/ibis_engine/selector_utils.py

def selector_from_pattern(pattern: SelectorPattern) -> object:
    import ibis.selectors as s

    if pattern.column_type == "numeric":
        return s.numeric()
    if pattern.column_type == "string":
        return s.of_type("string")
    if pattern.column_type == "temporal":
        return s.of_type("timestamp") | s.of_type("date") | s.of_type("time")
    if pattern.regex_pattern:
        return s.matches(pattern.regex_pattern)
    return s.all()
```

### Target Files
- `src/ibis_engine/selector_utils.py`
- `src/ibis_engine/macros.py`

### Implementation Checklist
- [x] Add `selector_from_pattern` to `selector_utils`.
- [x] Replace duplicated pattern translation logic in `across_columns` and `bind_columns`.
- [x] Add a small test verifying numeric/string/temporal/regex selectors match expected columns.

## Item 6: Expand Ibis Plan Artifacts (Decompile + SQL)

### Goal
Capture Ibis expression source and rendered SQL in the same artifact lane as SQLGlot checkpoints for reproducibility and review.

### Status
- Completed: shared `ibis_plan_artifacts` helper added with safe fallbacks and integrated into rule validation and DataFusion plan-artifact capture.

### Design / Architecture
Extend compiler checkpoint or diagnostics hooks to attach:
- `ibis.decompile(expr)` output
- `expr.to_sql(dialect=..., pretty=True)` or `ibis.to_sql(expr, dialect=...)`

```python
# src/ibis_engine/compiler_checkpoint.py (or a new helper module)
from ibis_engine.sql_bridge import decompile_expr


def ibis_plan_artifacts(expr: IbisTable, *, dialect: str) -> dict[str, object]:
    sql = expr.to_sql(dialect=dialect, pretty=True)
    return {
        "ibis_decompiled": decompile_expr(expr),
        "ibis_sql": sql,
    }
```

Wire these into the diagnostics path where SQLGlot artifacts are already produced.

### Target Files
- `src/ibis_engine/compiler_checkpoint.py`
- `src/ibis_engine/sql_bridge.py`
- `src/datafusion_engine/bridge.py`
- `src/engine/materialize_pipeline.py` (if diagnostics are captured here)
- `src/relspec/rules/validation.py`

### Implementation Checklist
- [x] Add Ibis SQL/decompile artifacts to the existing SQLGlot artifact payload.
- [x] Add `ibis_plan_artifacts` helper and integrate into diagnostics recording.
- [x] Guard decompile with a safe fallback if `ibis.decompile` raises.
- [x] Add a test that checks artifacts include both SQLGlot and Ibis-level data.

## Rollout Order (Recommended)

1. Centralize backend/execution factory (Item 1).
2. Consolidate `IbisPlanCatalog` (Item 2).
3. Unify schema/type mapping (Item 3).
4. Standardize list-param joins (Item 4).
5. Consolidate selector pattern translation (Item 5).
6. Expand plan artifacts (Item 6).

## Validation Strategy

- Unit tests for backend/execution configuration consistency.
- Snapshot tests for SQL column definitions and plan artifacts.
- Integration test using a small Ibis plan to ensure SQL/AST/decompile artifacts remain stable.
