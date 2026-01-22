# Relspec Advanced Integration Overhaul Plan

## Goals
- Promote SQLGlot AST to the canonical rule IR and unify plan identity across Ibis, DataFusion, and Substrait.
- Improve robustness, dynamic adaptation, and error handling through information_schema-driven introspection and policy-gated execution.
- Align relspec execution with DataFusion/Delta/Ibis/SQLGlot best-in-class integrations for higher performance and better diagnostics.
- Reduce bespoke legacy logic by replacing it with engine-native capabilities and runtime feature discovery.

## Non-goals
- Rewrite core DataFusion or Ibis internals.
- Change end-user rule semantics without a backwards-compatibility phase.

## Guiding Principles
- Policy-first execution: every SQL action goes through SQLOptions and explicit surface policy.
- Canonical IR: one deterministic AST for caching, diffs, and diagnostics.
- Information_schema is source of truth for runtime capabilities.
- Prefer DataFusion TableProvider and DDL registration over bespoke registration paths.
- Stream when possible; materialize only when necessary and gated.

## Progress Update (2026-01-22)
- ExecutionBundle signatures exist in `src/relspec/execution_bundle.py`, but compiler/plan signatures still use RelPlan-only hashing.
- DataFusion execution lane helpers are present and used in `src/relspec/compiler.py` and `src/relspec/graph.py`.
- Schema introspection and TableProvider metadata are already wired via `src/relspec/schema_context.py`.
- Runtime tuning propagation improvements landed: DataFusion runtime profiles align batch size/partitions with scan/cpu_threads, and Ibis write batch sizes inherit execution defaults.
- Error taxonomy rollout started (compiler/graph/validation/registry/validate), with remaining ValueError sites pending.

---

## Scope 1: Canonical SQLGlot IR + ExecutionBundle

### Objective
Promote SQLGlot AST to the canonical rule IR and emit a unified ExecutionBundle that captures AST, lineage, Substrait bytes, DataFusion plans, schema maps, and plan fingerprints.

Status: In progress (ExecutionBundle exists but is not yet used by compiler/plan signatures).

### Representative code
```python
# src/relspec/execution_bundle.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from sqlglot_tools.compat import Expression
from sqlglot_tools.lineage import LineagePayload
from sqlglot_tools.optimizer import SqlGlotPolicySnapshot

@dataclass(frozen=True)
class ExecutionBundle:
    sqlglot_expr: Expression
    sqlglot_policy: SqlGlotPolicySnapshot
    schema_map_hash: str
    plan_fingerprint: str
    lineage: LineagePayload | None
    substrait_bytes: bytes | None
    df_logical_plan: str | None
    df_physical_plan: str | None
    ibis_sqlglot_expr: Expression | None
    artifacts: Mapping[str, object]
```

```python
# src/relspec/compiler.py
from relspec.execution_bundle import ExecutionBundle
from sqlglot_tools.optimizer import canonical_ast_fingerprint, plan_fingerprint

bundle = ExecutionBundle(
    sqlglot_expr=normalized_expr,
    sqlglot_policy=policy_snapshot,
    schema_map_hash=schema_map_hash,
    plan_fingerprint=plan_fingerprint(normalized_expr, dialect=policy.write_dialect),
    lineage=lineage_payload,
    substrait_bytes=substrait_bytes,
    df_logical_plan=str(plans["logical"]) if plans else None,
    df_physical_plan=str(plans["physical"]) if plans else None,
    ibis_sqlglot_expr=ibis_expr,
    artifacts={"ast_hash": canonical_ast_fingerprint(normalized_expr)},
)
```

### Target files
- src/relspec/execution_bundle.py (new)
- src/relspec/compiler.py
- src/relspec/plan.py
- src/relspec/engine.py
- src/sqlglot_tools/optimizer.py
- src/sqlglot_tools/lineage.py

### Implementation checklist
- [x] Define ExecutionBundle and unify plan fingerprint inputs (policy hash + schema hash + AST hash).
- [ ] Generate SQLGlot AST from relspec rules and normalize via SQLGlot policy.
- [ ] Attach lineage metadata and AST diffs to bundle artifacts.
- [ ] Make plan signatures use ExecutionBundle fingerprints instead of RelPlan-only hashing.

### Legacy decommission
- src/relspec/plan.py (rel_plan_signature as canonical plan identity)
- src/relspec/rules/rel_ops.py (as the primary IR)

---

## Scope 2: DataFusion-first execution lanes + policy-gated SQL

### Objective
Make DataFusion the primary execution lane with Substrait-first compilation and policy-gated SQL fallback. Ibis remains as a compatibility lane but no longer defines the canonical path.

Status: In progress (DataFusion lane exists but policy gating and diagnostics are incomplete).

### Representative code
```python
# src/relspec/execution_lanes.py
from datafusion_engine.bridge import sqlglot_to_datafusion
from datafusion_engine.compile_options import DataFusionCompileOptions

options = profile.compile_options(options=DataFusionCompileOptions())
expr = normalize_expr(...)

# Substrait-first path
substrait_bytes = try_ibis_to_substrait_bytes(...)
if substrait_bytes is not None:
    df = ctx.sql_with_options("SELECT 1", options.sql_options)  # warm policy
    df = ctx.from_substrait(substrait_bytes)
else:
    df = sqlglot_to_datafusion(expr, ctx=ctx, options=options)
```

```python
# src/relspec/rules/validation.py
if not sql_policy.allow_statements:
    raise ValueError("SQL surface policy does not permit execution.")
```

### Target files
- src/relspec/execution_lanes.py (new)
- src/relspec/rules/validation.py
- src/datafusion_engine/bridge.py
- src/datafusion_engine/runtime.py
- src/ibis_engine/substrait_bridge.py

### Implementation checklist
- [ ] Add a lane selection layer with explicit reason codes and diagnostics (lane exists, reason codes not yet surfaced).
- [ ] Route all SQL through SQLOptions from runtime profile (partial wiring via DataFusion compile options).
- [ ] Enforce policy gates for DDL/DML and parameter use.
- [ ] Emit Substrait plan bytes when supported, with SQL fallback on failure (substrait-first exists, artifacts not captured).

### Legacy decommission
- Any Ibis-only execution path in src/relspec/engine.py that does not respect SQLOptions.

---

## Scope 3: Schema introspection + TableProvider metadata as source of truth

### Objective
Replace static schema assumptions with information_schema-driven schema maps and TableProvider metadata to power qualification, validation, and adaptive compilation.

Status: In progress (schema context and SQLGlot qualification are wired; artifact capture remains).

### Representative code
```python
# src/relspec/schema_context.py
from datafusion_engine.schema_introspection import SchemaIntrospector

schema_map = introspector.schema_mapping_snapshot()
capabilities = table_provider_metadata(ctx)
```

```python
# src/sqlglot_tools/optimizer.py
qualified = qualify(
    expr,
    schema=schema_map,
    dialect=policy.write_dialect,
    validate_qualify_columns=True,
)
```

### Target files
- src/relspec/schema_context.py
- src/datafusion_engine/schema_introspection.py
- src/datafusion_engine/table_provider_metadata.py
- src/sqlglot_tools/optimizer.py

### Implementation checklist
- [x] Extend schema context to expose schema maps and provider metadata.
- [x] Feed schema maps into SQLGlot qualification and star expansion.
- [x] Validate column presence from information_schema before execution.
- [ ] Record provider metadata in execution bundle artifacts.

### Legacy decommission
- Any static, hard-coded schema tables in relspec rules (favor info_schema snapshots).

---

## Scope 4: Runtime capability registry + adaptive function resolution

### Objective
Build a runtime capability registry from information_schema and backend features, then adapt function resolution across UDF tiers and execution lanes.

Status: Not started (capability snapshot type exists but is not wired).

### Representative code
```python
# src/relspec/capabilities.py
from datafusion_engine.udf_catalog import FunctionCatalog

catalog = FunctionCatalog.from_information_schema(
    routines=introspector.routines_snapshot(),
    parameters=introspector.parameters_snapshot(),
    parameters_available=True,
)

capabilities = RuntimeCapabilities(
    function_names=catalog.function_names,
    supports_substrait=profile.supports_substrait,
    supports_insert=provider_meta.supports_insert,
)
```

```python
# src/relspec/rules/coverage.py
is_builtin = func_name in capabilities.function_names
```

### Target files
- src/relspec/capabilities.py (new)
- src/relspec/rules/coverage.py
- src/datafusion_engine/udf_catalog.py
- src/engine/function_registry.py

### Implementation checklist
- [ ] Build capability snapshots from information_schema and provider metadata.
- [ ] Use the catalog to drive rule coverage and function selection (coverage uses FunctionCatalog, not wired to relspec capabilities).
- [ ] Prefer builtin -> rust -> pyarrow -> python tiers at resolution time.
- [ ] Surface missing capabilities in diagnostics output.

### Legacy decommission
- Static builtin registries or function allowlists not tied to runtime catalog.

---

## Scope 5: Diagnostics and artifact pipeline (AST, lineage, EXPLAIN, Substrait)

### Objective
Standardize diagnostics as structured artifacts for traceability, reproducibility, and debugging.

Status: In progress (rule diagnostics exist; artifacts not standardized per execution).

### Representative code
```python
# src/relspec/rules/diagnostics.py
metadata = {
    "sqlglot_ast": dump(expr),
    "lineage": lineage_payload,
    "substrait_plan_b64": base64.b64encode(substrait_bytes).decode("ascii"),
    "df_explain": explain_rows,
}
```

```python
# src/datafusion_engine/runtime.py
runtime_profile.diagnostics_sink.record_artifact("relspec_execution_v1", payload)
```

### Target files
- src/relspec/rules/diagnostics.py
- src/relspec/rules/validation.py
- src/sqlglot_tools/lineage.py
- src/datafusion_engine/runtime.py

### Implementation checklist
- [ ] Emit AST, lineage, and SQL text artifacts for every rule execution (diagnostics exist but are not enforced per execution).
- [ ] Attach DataFusion EXPLAIN and plan snapshots when available (compile options capture exists, not wired into relspec artifacts).
- [ ] Add Substrait bytes and fingerprint metadata to diagnostics.
- [ ] Standardize artifact names and payload versions.

### Legacy decommission
- Ad-hoc debug payloads that are not recorded as structured artifacts.

---

## Scope 6: Delta-first incremental model and mutation semantics

### Objective
Introduce Delta CDF, time travel, and DataFusion INSERT semantics into relspec incremental flows and output writes.

Status: Not started.

### Representative code
```python
# src/relspec/incremental.py
from storage.deltalake.delta import read_delta_cdf

cdf_table = read_delta_cdf(
    table_path,
    cdf_options=DeltaCdfOptions(
        starting_version=last_version,
        ending_version=current_version,
    ),
)
```

```python
# src/datafusion_engine/bridge.py
sql = f"INSERT INTO {table_name} {select_sql}"
ctx.sql_with_options(sql, sql_options).collect()
```

### Target files
- src/relspec/incremental.py
- src/storage/deltalake/delta.py
- src/datafusion_engine/bridge.py
- src/ibis_engine/sources.py

### Implementation checklist
- [ ] Add Delta CDF and time-travel selectors to incremental specs.
- [ ] Prefer INSERT INTO for Delta where provider supports inserts; fallback to write_deltalake.
- [ ] Record commit properties (app_id/version) for idempotent writes.
- [ ] Surface Delta protocol/features in diagnostics.

### Legacy decommission
- File-id-only incremental paths that ignore Delta CDF or snapshot semantics.

---

## Scope 7: Streaming output + runtime tuning propagation

### Objective
Prefer streaming outputs and propagate runtime tuning (memory pools, partitions, spill) into relspec execution for predictable performance.

Status: In progress (batch size propagation improved; streaming policy enforcement pending).

### Representative code
```python
# src/ibis_engine/sources.py
reader = expr.to_pyarrow_batches(chunk_size=250_000)
write_dataset(reader, base_dir=output_path, format="parquet")
```

```python
# src/datafusion_engine/runtime.py
SessionConfig()
    .set("datafusion.execution.target_partitions", str(target_partitions))
    .set("datafusion.execution.batch_size", str(batch_size))
```

### Target files
- src/ibis_engine/sources.py
- src/ibis_engine/io_bridge.py
- src/datafusion_engine/runtime.py
- src/relspec/engine.py

### Implementation checklist
- [ ] Prefer to_pyarrow_batches and dataset writers for large outputs (partial usage in io_bridge only).
- [ ] Propagate runtime tuning from ExecutionContext into DataFusion SessionConfig (batch_size/target_partitions alignment done; spill/memory tuning pending).
- [ ] Capture runtime tuning settings in execution artifacts.
- [ ] Avoid full materialization unless explicitly allowed by policy.

### Legacy decommission
- Unconditional to_arrow_table() materialization in execution paths.

---

## Scope 8: Robust error taxonomy + preflight validation

### Objective
Standardize error classes and enforce preflight checks for schema, constraints, and capability mismatches before execution.

Status: In progress (core error types wired in key modules; remaining ValueError sites pending).

### Representative code
```python
# src/relspec/errors.py
class RelspecValidationError(ValueError):
    """Raised for rule or schema validation failures."""

class RelspecExecutionError(RuntimeError):
    """Raised for execution lane failures."""
```

```python
# src/relspec/rules/validation.py
if missing_columns:
    raise RelspecValidationError(f"Missing columns: {missing_columns}")
```

### Target files
- src/relspec/errors.py (new)
- src/relspec/rules/validation.py
- src/relspec/validate.py
- src/relspec/schema_context.py

### Implementation checklist
- [ ] Introduce explicit error types and use them consistently (compiler/graph/validation/registry/validate updated; other relspec modules pending).
- [ ] Add preflight validation for schema, constraints, and policy gates (partial in SQLGlot validation paths).
- [ ] Emit failure diagnostics with context and remediation hints.
- [ ] Ensure errors are deterministic and non-suppressive.

### Legacy decommission
- Generic ValueError/RuntimeError usage without structured context.

---

## Scope 9: Customization surfaces and adapter extensibility

### Objective
Expose explicit customization points for rule execution policy, SQL dialects, and adapter integrations.

Status: Not started.

### Representative code
```python
# src/relspec/config.py
@dataclass(frozen=True)
class RelspecConfig:
    sqlglot_policy_name: str | None = None
    sql_policy_name: str | None = None
    execution_lane: str | None = None
```

```python
# src/relspec/rules/compiler.py
class RuleHandler(Protocol):
    def compile_rule(self, rule: RuleDefinition, *, ctx: ExecutionContext) -> object: ...
```

### Target files
- src/relspec/config.py
- src/relspec/runtime.py
- src/relspec/rules/compiler.py
- src/relspec/rules/handlers/__init__.py

### Implementation checklist
- [ ] Add config-driven overrides for SQLGlot policies and SQL execution lanes.
- [ ] Allow adapter-provided schema or capability overrides for specific domains.
- [ ] Document the customization contract and how adapters register.

### Legacy decommission
- Hard-coded execution mode defaults that ignore config overrides.

---

## Global Legacy Decommission List
Status: Not started.

- [ ] src/relspec/rules/rel_ops.py as primary rule IR (replace with SQLGlot AST bundles).
- [ ] src/relspec/plan.py rel_plan_signature as canonical identity (replace with bundle fingerprints).
- [ ] Ibis-only execution in src/relspec/engine.py when DataFusion lanes are available.
- [ ] Static builtin function tables replaced by information_schema-derived catalogs.
- [ ] File-id-only incremental logic when Delta CDF/time travel is available.

---

## Migration Phases

### Phase 1: Foundation
- Implement ExecutionBundle, schema maps, and capability registry.
- Wire SQLGlot canonicalization and plan fingerprints.

### Phase 2: Execution Lanes
- Add DataFusion-first lane with Substrait fallback.
- Policy-gated SQL execution and diagnostics artifacts.

### Phase 3: Delta and Performance
- Delta CDF/time travel integration and INSERT-based writes.
- Streaming output and runtime tuning propagation.

### Phase 4: Customization and Cleanup
- Expose configuration overrides and adapter extensibility.
- Decommission legacy modules and remove unused code paths.

---

## Quality Gates
- Run `uv run ruff check --fix` on touched files.
- Run `uv run pyrefly check` for type and contract validation.
- Run `uv run pyright --warnings --pythonversion=3.13` for strict typing.
