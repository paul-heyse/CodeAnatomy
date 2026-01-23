# DataFusion Methodology Expansion Integration Plan

## Goals
- Integrate the remaining high-impact DataFusion, Delta Lake, SQLGlot, and Ibis methodologies identified in the gap review.
- Provide first-class, configurable integration points that are compatible with the current runtime profile architecture.
- Keep all additions additive and opt-in until the new surfaces are validated in diagnostics and golden tests.

## Preconditions / gates
- `datafusion` Python >= 51 (existing baseline).
- `deltalake` crate features include `datafusion` (already enabled in `rust/datafusion_ext/Cargo.toml`).
- `ibis-framework[datafusion]` remains the backend for AST execution and SQLGlot bridging.
- SQLGlot version aligned with `src/sqlglot_tools/optimizer.py` policy.

## Status legend
- [x] Complete
- [~] Partially complete
- [ ] Remaining

## Execution phases (ordered)
Phase 1: Delta-native enhancements (Scopes 1-6).
Phase 2: DataFusion SQL execution upgrades (Scopes 7-8).
Phase 3: SQLGlot compiler hygiene + correctness harness (Scopes 9-10).
Phase 4: Ibis debugging artifacts (Scope 11).

## Scope 1: Delta QueryBuilder adapter (embedded DataFusion in deltalake)
Objective: add an optional, minimal SQL execution path that uses `deltalake.QueryBuilder` for Delta-only queries without `datafusion` Python dependency.

Status: [x] Complete.

Code patterns
```python
from deltalake import DeltaTable
from deltalake.query import QueryBuilder

def query_delta_via_querybuilder(path: str, sql: str):
    table = DeltaTable(path)
    builder = QueryBuilder().register("t", table)
    return builder.execute(sql)
```

Target files
- `src/storage/deltalake/query_builder.py` (new)
- `src/storage/deltalake/__init__.py`
- `src/engine/delta_tools.py`

Implementation checklist
- [x] Add a small adapter that wraps `QueryBuilder.register(...).execute(...)`.
- [x] Define a feature flag in runtime profile to enable QueryBuilder path.
- [x] Add diagnostics artifact capturing QueryBuilder usage (SQL + table path).
- [x] Ensure returned `RecordBatchReader` integrates with existing Arrow streaming flows.

## Scope 2: DeltaDataChecker integration for constraint enforcement
Objective: leverage delta-rs embedded DataFusion checker for invariants/constraints on write and merge paths.

Status: [x] Complete.

Code patterns
```rust
use deltalake::delta_datafusion::DeltaDataChecker;
use datafusion::execution::context::SessionContext;

fn validate_constraints(ctx: &SessionContext, table: &deltalake::DeltaTable) -> deltalake::errors::DeltaResult<()> {
    let checker = DeltaDataChecker::try_new(table.snapshot()?)?;
    checker.check_invariants(ctx.state()).map(|_| ())
}
```

Target files
- `rust/datafusion_ext/src/lib.rs`
- `src/storage/deltalake/delta.py`
- `src/ibis_engine/io_bridge.py`

Implementation checklist
- [x] Expose a PyO3 function in `datafusion_ext` to run `DeltaDataChecker`.
- [x] Add a Python wrapper to call the checker after DataFusion writes.
- [x] Wire into Delta write/merge flows with explicit opt-in.
- [x] Emit diagnostics for constraint failures with table path + rule name.

## Scope 3: Delta plan codecs (Logical + Physical) for plan serialization
Objective: enable DataFusion plan (de)serialization for Delta table providers to support caching and distributed execution.

Status: [x] Complete.

Code patterns
```rust
use deltalake::delta_datafusion::{DeltaLogicalCodec, DeltaPhysicalCodec};
use datafusion::execution::context::SessionContext;

fn install_delta_codecs(ctx: &SessionContext) {
    ctx.register_logical_extension_codec(Arc::new(DeltaLogicalCodec::default()));
    ctx.register_physical_extension_codec(Arc::new(DeltaPhysicalCodec::default()));
}
```

Target files
- `rust/datafusion_ext/src/lib.rs`
- `src/datafusion_engine/runtime.py`

Implementation checklist
- [x] Expose codec installation helper via `datafusion_ext`.
- [x] Add runtime profile switches for codec installation.
- [x] Record codec enablement in diagnostics snapshots.

## Scope 4: Delta session defaults + case sensitivity utilities
Objective: enforce consistent Delta session defaults and avoid casing drift between Delta schema and DataFusion parsing.

Status: [x] Complete.

Code patterns
```rust
use deltalake::delta_datafusion::{DeltaSessionConfig, DeltaParserOptions, DeltaColumn};
use datafusion::execution::context::SessionContext;

fn delta_session(ctx: SessionContext) -> SessionContext {
    let config = DeltaSessionConfig::new().with_parser_options(DeltaParserOptions::default());
    config.apply_to_session(ctx)
}
```

Target files
- `rust/datafusion_ext/src/lib.rs`
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/registry_bridge.py`

Implementation checklist
- [x] Add a `datafusion_ext` helper to apply Delta session defaults to a `SessionContext`.
- [x] Add `DataFusionRuntimeProfile` toggle for Delta session defaults.
- [x] Normalize identifier casing for Delta table registration in registry bridge.

## Scope 5: Delta object-store registry seam
Objective: split Delta log-store configuration from bulk parquet read configuration and make registry-based object stores explicit.

Status: [x] Complete.

Code patterns
```python
from deltalake import DeltaTable

def open_delta_with_logstore(path: str, *, logstore_options: dict[str, str]):
    return DeltaTable(path, storage_options=logstore_options)
```

Target files
- `src/storage/deltalake/delta.py`
- `src/schema_spec/system.py`
- `src/datafusion_engine/registry_bridge.py`

Implementation checklist
- [x] Extend `DatasetLocation` to accept log-store vs data-store options explicitly.
- [x] Thread log-store options to DeltaTable creation; keep filesystem for data reads separate.
- [x] Emit diagnostics showing which object store was bound for each Delta registration.

## Scope 6: Delta Sharing provider integration (optional)
Objective: add an optional integration path for Delta Sharing via `datafusion_delta_sharing` when enabled.

Status: [ ] Optional (deferred).

Code patterns
```rust
use datafusion_delta_sharing::DeltaSharingTableProvider;
use datafusion::execution::context::SessionContext;

fn register_delta_sharing(ctx: &SessionContext, name: &str, provider: DeltaSharingTableProvider) {
    ctx.register_table(name, Arc::new(provider)).unwrap();
}
```

Target files
- `rust/datafusion_ext/src/lib.rs` (feature-gated)
- `src/datafusion_engine/runtime.py`
- `src/schema_spec/system.py`

Implementation checklist
- [ ] Add optional Rust feature + PyO3 wrapper for Delta Sharing provider.
- [ ] Add DatasetLocation variant for Delta Sharing endpoints.
- [ ] Gate registration behind explicit runtime profile flag.

## Scope 7: DataFusion named parameters for safe table injection
Objective: add `named_params` support to SQL execution paths to allow safe DataFrame/table injection without string templating.

Status: [x] Complete.

Code patterns
```python
from datafusion import SessionContext

def query_with_named_params(ctx: SessionContext, sql: str, *, params: dict[str, object]):
    return ctx.sql_with_options(sql, sql_options, named_params=params)
```

Target files
- `src/datafusion_engine/bridge.py`
- `src/datafusion_engine/registry_bridge.py`
- `src/ibis_engine/sql_bridge.py`

Implementation checklist
- [x] Extend SQL execution helpers to accept `named_params`.
- [x] Add validation for DataFrame/table params and enforce read-only SQL options.
- [x] Add diagnostics for param binding shapes (names + types).

## Scope 8: SessionContext.read_table direct path
Objective: add a helper for direct TableProvider reads without catalog registration for ephemeral scans.

Status: [x] Complete.

Code patterns
```python
from datafusion import SessionContext
from datafusion.catalog import Table

def read_provider(ctx: SessionContext, provider: Table):
    return ctx.read_table(provider)
```

Target files
- `src/datafusion_engine/bridge.py`
- `src/datafusion_engine/registry_bridge.py`

Implementation checklist
- [x] Add a `datafusion_read_table` helper that wraps `SessionContext.read_table`.
- [x] Use in ephemeral validation / diagnostics paths where registration is unnecessary.
- [x] Record usage in diagnostics for visibility.

## Scope 9: SQLGlot pushdown transforms + recursive CTE hygiene
Objective: introduce compiler-level pushdown transformations and recursive CTE column normalization.

Status: [x] Complete.

Code patterns
```python
from sqlglot.transforms import pushdown_predicates, pushdown_projections, add_recursive_cte_column_names

def _extra_transforms(expr):
    expr = pushdown_projections()(expr)
    expr = pushdown_predicates()(expr)
    expr = add_recursive_cte_column_names()(expr)
    return expr
```

Target files
- `src/sqlglot_tools/optimizer.py`
- `src/sqlglot_tools/compat.py`

Implementation checklist
- [x] Add pushdown transforms to the SQLGlot rewrite lane.
- [x] Add recursive CTE name normalization transform (dialect-guarded).
- [x] Update optimizer policy hashes to include new transforms.

## Scope 10: SQLGlot executor for golden tests
Objective: add correctness tests that execute SQLGlot ASTs without DataFusion, ensuring rewrite validity.

Status: [x] Complete.

Code patterns
```python
from sqlglot.executor import execute

def execute_sqlglot(expr):
    return execute(expr)
```

Target files
- `tests/sqlglot/test_executor_golden.py` (new)
- `src/sqlglot_tools/optimizer.py`

Implementation checklist
- [x] Add minimal fixtures for AST execution.
- [x] Compare baseline SQL vs rewritten SQL for functional equivalence.
- [x] Gate rewrite additions on executor golden tests.

## Scope 11: Ibis GraphViz artifacts
Objective: emit Ibis IR graph artifacts (`to_graph`) alongside existing SQLGlot and DataFusion artifacts for debugging.

Status: [x] Complete.

Code patterns
```python
import ibis

def ibis_graphviz(expr):
    return ibis.expr.visualize.to_graph(expr)
```

Target files
- `src/ibis_engine/compiler_checkpoint.py`
- `src/engine/runtime_profile.py`
- `src/obs/repro.py`

Implementation checklist
- [x] Add optional graph artifact emission to compiler checkpoints.
- [x] Store graph output in diagnostics bundle when enabled.
- [x] Ensure graph generation is skipped when GraphViz is unavailable.

## Files to decommission and delete
None identified. This plan is additive; no existing modules are superseded. If the
QueryBuilder path becomes the only supported Delta SQL engine, we can revisit
deprecating Delta-only SQL wrappers at that time.
