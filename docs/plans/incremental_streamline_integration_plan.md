# Incremental Streamline Integration Plan (DataFusion/DeltaLake/Ibis/SQLGlot)

This plan consolidates incremental execution with the existing DataFusion, DeltaLake, Ibis,
and SQLGlot capabilities, aligning runtime configuration, execution, and diagnostics with
the rest of the stack.

Date: January 24, 2026

Goals
- Use canonical ExecutionContext + DataFusion runtime configuration (no ad-hoc profiles).
- Materialize Ibis plans via ibis_engine execution (no direct expr.to_pyarrow path).
- Align SQLGlot artifacts with datafusion_compile policy + schema maps.
- Route DeltaLake reads/writes through storage helpers with consistent storage options.
- Reduce duplication in Ibis table registration helpers.

----------------------------------------------------------------------
Scope 1: Incremental runtime uses canonical ExecutionContext configuration
----------------------------------------------------------------------
Status: Implemented
Why
- IncrementalRuntime currently creates its own DataFusionRuntimeProfile and ExecutionContext.
- This bypasses object-store config, ibis options, and thread-pool settings used elsewhere.

Representative pattern
```python
from arrowdsl.core.execution_context import ExecutionContext, execution_context_factory
from ibis_engine.execution_factory import ibis_backend_from_ctx, ibis_execution_from_ctx

def build_incremental_runtime(
    ctx: ExecutionContext | None,
    *,
    profile: str = "default",
) -> IncrementalRuntime:
    exec_ctx = ctx or execution_context_factory(profile)
    backend = ibis_backend_from_ctx(exec_ctx)
    execution = ibis_execution_from_ctx(exec_ctx, backend=backend)
    return IncrementalRuntime(
        execution_ctx=exec_ctx,
        ibis_backend=backend,
        ibis_execution=execution,
        runtime_profile=exec_ctx.runtime.datafusion,
    )
```

Target files
- src/incremental/runtime.py
- src/arrowdsl/core/execution_context.py (use existing factory)
- src/ibis_engine/execution_factory.py (no behavior change; used by incremental)

Implementation checklist
- [x] Add a runtime builder that accepts or derives an ExecutionContext.
- [x] Replace DataFusionRuntimeProfile-only construction with runtime profile from context.
- [x] Ensure diagnostics sink and SQL options derive from the runtime profile.
- [x] Provide a migration path for existing IncrementalRuntime.build() call sites.

Decommission candidates
- src/incremental/runtime.py: IncrementalRuntime.build() default profile creation path.

----------------------------------------------------------------------
Scope 2: Materialize Ibis plans via ibis_engine execution
----------------------------------------------------------------------
Status: Implemented
Why
- incremental.ibis_exec uses expr.to_pyarrow(), bypassing execution policies and ordering
  metadata and diverging from normalize.

Representative pattern
```python
from ibis_engine.plan import IbisPlan
from ibis_engine.execution import materialize_ibis_plan
from arrowdsl.core.ordering import Ordering

plan = IbisPlan(expr=expr, ordering=Ordering.unordered())
table = materialize_ibis_plan(plan, execution=runtime.ibis_execution())
```

Target files
- src/incremental/ibis_exec.py
- src/incremental/changes.py
- src/incremental/deltas.py
- src/incremental/scip_snapshot.py
- src/incremental/impact.py
- src/incremental/props.py
- src/incremental/exports.py

Implementation checklist
- [x] Replace expr.to_pyarrow() with materialize_ibis_plan via IbisExecutionContext.
- [x] Preserve ordering metadata behavior consistent with ibis_engine execution.
- [x] Ensure SQLGlot artifacts are still recorded before materialization.
- [x] Audit any direct Table conversions to avoid redundant materializations.

Decommission candidates
- src/incremental/ibis_exec.py: direct expr.to_pyarrow execution path.

----------------------------------------------------------------------
Scope 3: Align SQLGlot artifacts with datafusion_compile policy + schema map
----------------------------------------------------------------------
Status: Implemented
Why
- Incremental diagnostics currently use default_sqlglot_policy and omit schema maps,
  plan hashes, lineage, and AST payloads.

Representative pattern
```python
from datafusion_engine.schema_introspection import schema_map_snapshot
from datafusion_engine.runtime import sql_options_for_profile
from sqlglot_tools.bridge import collect_sqlglot_plan_artifacts, SqlGlotPlanOptions

schema_map, schema_map_hash = schema_map_snapshot(
    runtime.session_context(),
    sql_options=sql_options_for_profile(runtime.profile),
)
artifacts = collect_sqlglot_plan_artifacts(
    expr,
    backend=compiler_backend,
    options=SqlGlotPlanOptions(
        policy_name="datafusion_compile",
        schema_map=schema_map,
        schema_map_hash=schema_map_hash,
    ),
)
```

Target files
- src/incremental/sqlglot_artifacts.py
- src/incremental/invalidations.py
- src/incremental/metadata.py
- src/sqlglot_tools/bridge.py (reuse existing helper)

Implementation checklist
- [x] Replace sqlglot_diagnostics-only payload with collect_sqlglot_plan_artifacts.
- [x] Persist plan_hash, policy_hash, schema_map_hash, and AST/lineage payloads.
- [x] Update invalidation hashing to use plan_hashes (not diagnostics payload hash).
- [x] Ensure policy name is datafusion_compile to match DataFusion compilation.
- [x] Add tests for plan-hash stability if/when incremental tests exist.

Decommission candidates
- src/incremental/sqlglot_artifacts.py:
  - sqlglot_artifact_hash
  - sqlglot_artifact_payload
  - record_sqlglot_artifact (replaced by plan-artifact recorder)

----------------------------------------------------------------------
Scope 4: Route DeltaLake access through storage helpers + storage options
----------------------------------------------------------------------
Status: Implemented
Why
- Direct DeltaTable usage lacks storage/log storage options and diverges from storage layer.

Representative pattern
```python
from storage.deltalake import open_delta_table, delta_table_version

table = open_delta_table(path, storage_options=storage, log_storage_options=log_storage)
version = delta_table_version(path, storage_options=storage, log_storage_options=log_storage)
```

Target files
- src/incremental/snapshot.py
- src/incremental/delta_updates.py
- src/incremental/pruning.py
- src/incremental/diff.py
- src/incremental/cdf_runtime.py
- src/storage/deltalake/delta.py (no behavior change; used by incremental)

Implementation checklist
- [x] Replace direct DeltaTable(...) with storage.deltalake.open_delta_table.
- [x] Thread storage/log storage options through Delta reads and CDF registration.
- [x] Replace DeltaTable.is_deltatable checks with delta_table_version / delta_table_features.
- [x] Ensure delete/merge operations use commit metadata from runtime profile where needed.

Decommission candidates
- Direct deltalake.DeltaTable usage in incremental modules.

----------------------------------------------------------------------
Scope 5: Use runtime-aware Delta readers + metadata snapshots
----------------------------------------------------------------------
Status: Implemented
Why
- Incremental reads use read_delta_as_reader() with a fresh DataFusionRuntimeProfile,
  ignoring runtime settings and object stores.

Representative pattern
```python
from ibis_engine.io_bridge import ibis_table_to_reader
from ibis_engine.sources import IbisDeltaReadOptions, read_delta_ibis

backend = runtime.ibis_backend()
table = read_delta_ibis(backend, path, options=IbisDeltaReadOptions(storage_options=storage))
reader = ibis_table_to_reader(table)
```

Target files
- src/incremental/snapshot.py
- src/incremental/invalidations.py
- src/incremental/fingerprint_changes.py
- src/datafusion_engine/runtime.py (no behavior change; existing helper stays)

Implementation checklist
- [x] Replace read_delta_as_reader() with runtime-aware Ibis read paths.
- [x] Pass storage/log storage options from runtime or dataset location.
- [x] Ensure schema metadata consistency between reads and writes.

Decommission candidates
- src/incremental/*: calls to datafusion_engine.runtime.read_delta_as_reader().

----------------------------------------------------------------------
Scope 6: Consolidate Ibis table registration helpers
----------------------------------------------------------------------
Status: Implemented
Why
- Multiple local _table_expr helpers duplicate registration logic and can drift.

Representative pattern
```python
from incremental.ibis_utils import ibis_table_from_arrow

expr = ibis_table_from_arrow(runtime.ibis_backend(), table, name="inputs")
```

Target files
- src/incremental/deltas.py
- src/incremental/impact.py
- src/incremental/ibis_utils.py

Implementation checklist
- [x] Replace local _table_expr helpers with ibis_table_from_arrow.
- [x] Ensure ordering and naming conventions are consistent.

Decommission candidates
- src/incremental/deltas.py: _table_expr
- src/incremental/impact.py: _table_expr

----------------------------------------------------------------------
Scope 7: Strengthen invalidation metadata with runtime profile snapshots
----------------------------------------------------------------------
Status: Implemented
Why
- Invalidation metadata only hashes DataFusion settings + SQLGlot policy hash,
  missing broader runtime profile changes (scan profile, ibis options, etc).

Representative pattern
```python
from engine.runtime_profile import runtime_profile_snapshot

snapshot = runtime_profile_snapshot(runtime.execution_context().runtime)
metadata_hash = snapshot.profile_hash
```

Target files
- src/incremental/invalidations.py
- src/engine/runtime_profile.py (existing helper)

Implementation checklist
- [x] Include runtime profile hash in incremental metadata hash payload.
- [x] Store profile snapshot artifact where diagnostics sink is available. (Stored in incremental metadata payload.)
- [x] Update invalidation reasons to include profile changes.

Decommission candidates
- None (additive change).

----------------------------------------------------------------------
Decommission and Deletion Summary (Consolidated)
----------------------------------------------------------------------
Functions to remove (after new helpers land)
- src/incremental/runtime.py:
  - IncrementalRuntime.build() default profile constructor path
- src/incremental/ibis_exec.py:
  - ibis_expr_to_table (direct expr.to_pyarrow path)
- src/incremental/sqlglot_artifacts.py:
  - sqlglot_artifact_hash
  - sqlglot_artifact_payload
  - record_sqlglot_artifact (replace with plan-artifact recorder)
- src/incremental/deltas.py:
  - _table_expr
- src/incremental/impact.py:
  - _table_expr

Completed additions
- src/incremental/delta_context.py:
  - DeltaAccessContext
  - DeltaStorageOptions
- src/incremental/invalidations.py:
  - InvalidationSnapshot
  - InvalidationResult
  - build/read/write/diff invalidation helpers

Files to delete
- None required; expect refactors and shared helper reuse rather than file removal.
