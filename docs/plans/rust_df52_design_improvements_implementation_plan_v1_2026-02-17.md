# Rust Pivot + DF52 Migration + Design Improvements Implementation Plan v1 (2026-02-17)

## Scope Summary

Three integrated workstreams targeting ~7,000+ Python LOC reduction, full DataFusion 52
adoption, and DF52-native planning/runtime architecture:

- **Workstream 1 (Rust Pivot):** Migrate redundant Python code to Rust bridge payloads
  (~4,000 LOC reduction across lineage walker, UDF snapshot, extraction loops, tree-sitter,
  Delta write, byte-span canonicalization, interval-align kernel, and policy compiler)
- **Workstream 2 (DF52 Migration):** 5-step critical path from DF51 → DF52 with strict
  dependency ordering across all Rust crates and the Python extension layer
- **Workstream 3 (Design Improvements):** 43 quick wins plus design-principle hardening
  (~1,500 LOC reduction, directly addressable before or after the DF52 sprint)

**Design stance:** Hard cutover to DF52 with no compatibility shims, no dual-version
branches, and no transitional re-export layers. Rust migrations depend on a stable DF52
bridge. Design improvements are independent and can be interleaved with any phase.

---

## Design Principles

1. DF52 migration proceeds in strict dependency order: `datafusion_python` → `datafusion_ext`
   → `codeanatomy_engine` → Python extension layer → `deltalake` alignment.
2. No ignore-style suppression comments in runtime code — fix structurally.
3. All Rust bridge payloads use `serde` derive for JSON/msgpack serialization; never emit
   untyped dicts across the FFI boundary.
4. Python-side types that receive Rust payloads use `msgspec.Struct` for zero-copy decode;
   no bespoke JSON re-parsing.
5. Bare `SessionContext()` construction is banned — all contexts must be created through
   `_ephemeral_context_phases` or an injected `DataFusionRuntimeProfile`.
6. `from __future__ import annotations` in every module.
7. Tests required for every new module; no new module lands without at least one unit test.
8. No version-conditioned DF51/DF52 branching in runtime code; remove DF51 paths outright.
9. All planner policy knobs must be explicit and typed (no hidden defaults in ad-hoc dicts).
10. Streaming-first boundaries are mandatory for large-result reads/writes; eager
    materialization must be explicit and justified.

---

## Current Baseline

**Rust crate versions before cutover:**
- `rust/datafusion_python/Cargo.toml`: `datafusion = "52.1.0"`,
  `datafusion-ffi = "52.1.0"`, `deltalake = "0.31.0"`
- `rust/datafusion_ext/Cargo.toml`: `datafusion = "52.1.0"`, `deltalake = "0.31.0"`
- `rust/codeanatomy_engine/Cargo.toml`: `datafusion = "52.1.0"`,
  `datafusion-substrait = "52.1.0"`, `datafusion-proto = "52.1.0"`

**Critical FFI call sites using the DF51 zero-argument `.call0()` convention:**
- `rust/datafusion_python/src/utils.rs:170-171` —
  `obj.getattr("__datafusion_table_provider__")?.call0()?`
- `rust/datafusion_python/src/context.rs:607-628` —
  `provider.getattr("__datafusion_catalog_provider__")?.call0()?`
- `rust/datafusion_python/src/catalog.rs:115-139,363-383` —
  `schema_provider.getattr("__datafusion_schema_provider__")?.call0()?`
- `rust/datafusion_python/src/codeanatomy_ext/helpers.rs:132-141` — `FFI_TableProvider::new(...)`
- `rust/datafusion_python/src/codeanatomy_ext/schema_evolution.rs:370-383` —
  `FFI_TableProvider::new(...)`
- `rust/df_plugin_codeanatomy/src/lib.rs:431,462` — `FFI_TableProvider::new(...)`

**`CoalesceBatchesExec` removal targets (compile-time break in DF52):**
- `rust/datafusion_ext/src/physical_rules.rs:6` —
  `use datafusion::physical_optimizer::coalesce_batches::CoalesceBatches;`
- `rust/codeanatomy_engine/src/rules/physical.rs:133-140` —
  `apply_post_filter_coalescing` function

**Python parallel implementations confirmed:**
- `src/datafusion_engine/plan/walk.py` (~110 LOC) +
  `src/datafusion_engine/lineage/reporting.py` (~570 LOC) duplicates
  `rust/codeanatomy_engine/src/compiler/lineage.rs` (`TreeNode::apply`)
- `src/datafusion_engine/plan/plan_introspection.py` (12 functions, ~220 LOC) is
  byte-for-byte duplicated in
  `src/datafusion_engine/plan/bundle_environment.py:17-219`
- `src/datafusion_engine/encoding/policy.py:138-139` — bare `SessionContext()` constructor
- `src/datafusion_engine/udf/extension_snapshot_runtime.py:42-270` — 14 deferred-import
  wrapper functions delegating to `extension_validation.py`
- All four `src/extract/extractors/*/builders_runtime.py` files — import ~13 private
  symbols from their sibling `builders.py` and add ~1,000 LOC of orchestration ceremony
  each

**`LineageReport` in Rust already has `serde` derives:**
```rust
// rust/codeanatomy_engine/src/compiler/lineage.rs (excerpt)
pub struct LineageReport {
    pub scans: Vec<ScanLineage>,
    pub required_columns_by_dataset: BTreeMap<String, Vec<String>>,
    pub filters: Vec<String>,
    pub referenced_tables: Vec<String>,
    pub required_udfs: Vec<String>,
    pub required_rewrite_tags: Vec<String>,
    pub referenced_udfs: Vec<String>,
}
```

**Observability gaps confirmed:**
- `rust/df_plugin_codeanatomy/src/lib.rs:293,317` — `eprintln!` on error paths
- `rust/datafusion_ext/src/planner_rules.rs:69-78` — no trace event when blocking DDL/DML
- `rust/datafusion_ext/src/physical_rules.rs:87-114` — no span on optimize
- `src/datafusion_engine/delta/maintenance.py:103` — vacuum error recorded as string
  attribute, not `span.record_exception`

---

## S1. Delta delete None-predicate safety guard (QW-05)

### Goal

Add an explicit `None`-predicate guard in `delta_delete` and `delta_delete_where` that
raises `ValueError` with a clear message before any statement reaches the Delta engine.
DF52 Issue #19840 worsens the accidental full-table deletion risk.

### Representative Code Snippets

```python
# src/datafusion_engine/delta/control_plane_mutation.py
def delta_delete(
    table_uri: str,
    predicate: str,          # remove Optional; never accept None
    *,
    storage_options: dict[str, str] | None = None,
) -> DeltaDeleteResult:
    """Delete rows from a Delta table matching predicate.

    Parameters
    ----------
    table_uri : str
        URI of the Delta table.
    predicate : str
        Non-empty SQL WHERE clause. Must not be None or empty string.
    storage_options : dict[str, str] | None
        Optional storage options.

    Raises
    ------
    ValueError
        If predicate is None, empty, or whitespace-only.
    """
    if not predicate or not predicate.strip():
        raise ValueError(
            "delta_delete requires a non-empty predicate. "
            "To delete all rows use delta_delete_all() explicitly."
        )
    ...
```

```python
# src/storage/deltalake/delta_write.py — same guard in delta_delete_where
def delta_delete_where(
    table_uri: str,
    predicate: str,      # str, not str | None
    ...
) -> ...:
    if not predicate or not predicate.strip():
        raise ValueError(
            "delta_delete_where requires a non-empty predicate. "
            "Use delta_delete_all() to delete all rows."
        )
```

### Files to Edit

- `src/datafusion_engine/delta/control_plane_mutation.py:41-56` — tighten `predicate`
  type and add guard
- `src/storage/deltalake/delta_write.py:105-217` — add guard in `delta_delete_where`

### New Files to Create

- `tests/unit/datafusion_engine/delta/test_delete_predicate_guard.py`

### Legacy Decommission/Delete Scope

Remove `predicate: str | None` overload from any stub or type alias that allows `None`.

---

## S2. Exception narrowing in infer_semantics (QW-27)

### Goal

Narrow the broad `(AttributeError, KeyError, TypeError, ValueError)` catch in
`infer_semantics()` to only catch `KeyError`. Programming errors (`TypeError`,
`AttributeError`) must surface so they cannot silently degrade CPG quality.

### Representative Code Snippets

```python
# src/semantics/ir_pipeline.py:1292-1301 — before
try:
    result = _apply_join_inference(view, semantic_state)
except (AttributeError, KeyError, TypeError, ValueError):
    logger.debug("join inference skipped for %s", view.name)
    result = None

# After
try:
    result = _apply_join_inference(view, semantic_state)
except KeyError:
    logger.warning(
        "join inference skipped for %s: missing key in semantic state",
        view.name,
        exc_info=True,
    )
    result = None
```

### Files to Edit

- `src/semantics/ir_pipeline.py:1292-1301` — narrow exception tuple

### New Files to Create

None (covered by existing `tests/unit/semantics/` suite; add parametrize case).

### Legacy Decommission/Delete Scope

Remove the `AttributeError, TypeError, ValueError` entries from the except clause.

---

## S3. Session extension idempotency guards (QW-02)

### Goal

Add `WeakKeyDictionary` sentinels to `_install_planner_rules` and `_install_cache_tables`
so duplicate extension registration is impossible on pool-reused contexts.

### Representative Code Snippets

```python
# src/datafusion_engine/session/runtime_extensions.py
import weakref

_PLANNER_RULES_INSTALLED: weakref.WeakKeyDictionary[object, bool] = weakref.WeakKeyDictionary()
_CACHE_TABLES_INSTALLED: weakref.WeakKeyDictionary[object, bool] = weakref.WeakKeyDictionary()

def _install_planner_rules(ctx: SessionContext, *, profile: DataFusionRuntimeProfile) -> None:
    """Install planner rules into ctx; idempotent."""
    if _PLANNER_RULES_INSTALLED.get(ctx):
        return
    _PLANNER_RULES_INSTALLED[ctx] = True
    # ... existing installation logic ...

def _install_cache_tables(ctx: SessionContext, *, profile: DataFusionRuntimeProfile) -> None:
    """Install cache tables; idempotent."""
    if _CACHE_TABLES_INSTALLED.get(ctx):
        return
    _CACHE_TABLES_INSTALLED[ctx] = True
    # ... existing installation logic ...
```

### Files to Edit

- `src/datafusion_engine/session/runtime_extensions.py:200-240` — `_install_planner_rules`
- `src/datafusion_engine/session/runtime_extensions.py:813-836` — `_install_cache_tables`

### New Files to Create

- `tests/unit/datafusion_engine/session/test_extension_idempotency.py`

### Legacy Decommission/Delete Scope

None.

---

## S4. plan_introspection.py deletion / DRY consolidation (QW-08)

### Goal

Delete `plan_introspection.py` entirely. Migrate `function_registry_hash_for_context` and
`suppress_introspection_errors` into `bundle_environment.py` where the 10 other duplicated
functions already live.

### Representative Code Snippets

```python
# src/datafusion_engine/plan/bundle_environment.py — add after existing imports
# (Migrate from plan_introspection.py — these were the two non-duplicated functions)

def function_registry_hash_for_context(ctx: SessionContext) -> str:
    """Compute a stable hash of all registered scalar and aggregate UDFs.

    Parameters
    ----------
    ctx : SessionContext
        The DataFusion session context to inspect.

    Returns
    -------
    str
        Hex digest of the sorted UDF name list.
    """
    names = sorted(ctx.catalog("datafusion").schema("public").table_names())
    return hashlib.sha256("|".join(names).encode()).hexdigest()


def suppress_introspection_errors(fn: Callable[..., T]) -> Callable[..., T | None]:
    """Wrap fn so IntrospectionError is caught and None returned."""
    @functools.wraps(fn)
    def _wrapper(*args: object, **kwargs: object) -> T | None:
        try:
            return fn(*args, **kwargs)
        except IntrospectionError:
            return None
    return _wrapper
```

### Files to Edit

- `src/datafusion_engine/plan/bundle_environment.py` — add `function_registry_hash_for_context`
  and `suppress_introspection_errors`
- Any file importing from `plan_introspection.py` — update to import from `bundle_environment`

### New Files to Create

None.

### Legacy Decommission/Delete Scope

Delete `src/datafusion_engine/plan/plan_introspection.py` in its entirety (~220 LOC).

---

## S5. CoalesceBatches pre-removal from datafusion_ext (QW-21)

### Goal

Remove the `CoalesceBatches` import and its wrapping rule from
`datafusion_ext/src/physical_rules.rs` and `codeanatomy_engine/src/rules/physical.rs`
**before** the DF52 upgrade lands so the DF52 compile is cleaner. This is safe in DF51
because coalescing is already redundant when DataFusion handles it internally.

### Representative Code Snippets

```rust
// rust/datafusion_ext/src/physical_rules.rs — remove lines 6-7
// REMOVE: use datafusion::physical_optimizer::coalesce_batches::CoalesceBatches;

// rust/codeanatomy_engine/src/rules/physical.rs — remove apply_post_filter_coalescing
// REMOVE: fn apply_post_filter_coalescing(plan: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
//     Arc::new(CoalesceBatchesExec::new(plan, target_batch_size))
// }
// Remove the call site in the optimizer pipeline that wraps filters.
```

```rust
// rust/datafusion_ext/src/physical_rules.rs — before (DF51 code removed in DF52)
pub struct CodeAnatomyPhysicalConfig {
    pub coalesce_batches: bool,  // REMOVE this field
    ...
}

// After — no coalesce field; coalescing is now built into operators
pub struct CodeAnatomyPhysicalConfig {
    // coalesce_batches removed: DF52 integrates coalescing into operators directly
    ...
}
```

### Files to Edit

- `rust/datafusion_ext/src/physical_rules.rs` — remove `CoalesceBatches` import (line 6),
  remove wrapping rule (~line 103)
- `rust/codeanatomy_engine/src/rules/physical.rs:133-140` — remove
  `apply_post_filter_coalescing`

### New Files to Create

None (compile verification is sufficient).

### Legacy Decommission/Delete Scope

`CoalesceBatchesExec` wrapper and the `coalesce_batches` field in
`CodeAnatomyPhysicalConfig`.

---

## S6. Rust plugin observability — eprintln to tracing (QW-20)

### Goal

Replace `eprintln!` error paths in `df_plugin_codeanatomy/src/lib.rs` with
`tracing::error!` so plugin initialization failures are visible to OTLP log aggregators.
Add `tracing::instrument` spans to `CodeAnatomyPolicyRule::analyze` and
`CodeAnatomyPhysicalRule::optimize` so rule application is observable.

### Representative Code Snippets

```rust
// rust/df_plugin_codeanatomy/src/lib.rs — before (lines 293, 317)
eprintln!("Failed to register UDF bundle: {:?}", e);
eprintln!("Plugin initialization error: {:?}", e);

// After
tracing::error!(error = ?e, "Failed to register UDF bundle");
tracing::error!(error = ?e, "Plugin initialization error");
```

```rust
// rust/datafusion_ext/src/planner_rules.rs — add span to analyze
#[tracing::instrument(level = "debug", skip_all, fields(rule = "CodeAnatomyPolicyRule"))]
fn analyze(
    &self,
    plan: LogicalPlan,
    config: &ConfigOptions,
) -> datafusion::error::Result<Transformed<LogicalPlan>> {
    ...
    // When blocking a DDL/DML statement, emit a structured event:
    tracing::info!(
        statement_type = %stmt_type,
        "CodeAnatomyPolicyRule: blocking DDL/DML statement"
    );
    ...
}
```

```rust
// rust/datafusion_ext/src/physical_rules.rs — add span to optimize
#[tracing::instrument(level = "debug", skip_all, fields(rule = "CodeAnatomyPhysicalRule"))]
fn optimize(
    &self,
    plan: Arc<dyn ExecutionPlan>,
    config: &ConfigOptions,
) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
    ...
}
```

### Files to Edit

- `rust/df_plugin_codeanatomy/src/lib.rs:293,317` — replace `eprintln!`
- `rust/datafusion_ext/src/planner_rules.rs:69-78` — add `tracing::instrument`
- `rust/datafusion_ext/src/physical_rules.rs:87-114` — add `tracing::instrument`

### New Files to Create

None.

### Legacy Decommission/Delete Scope

All `eprintln!` calls in `lib.rs` plugin error paths.

---

## S7. Coercion module consolidation (QW-31, QW-42)

### Goal

Make `src/utils/value_coercion.py` the single canonical coercion module and delete
`src/utils/coercion.py` in the same scope item. Replace the 4 private coercion helpers in
`src/cli/commands/build.py:862-902` with imports from `value_coercion`.

### Representative Code Snippets

```python
# src/utils/value_coercion.py — canonical module (add missing cases from coercion.py)
# Ensure bool handling is consistent: "true"/"1"/"yes" -> True; "false"/"0"/"no" -> False
def coerce_bool(value: object, *, default: bool | None = None) -> bool | None:
    """Coerce value to bool with canonical string handling.

    Parameters
    ----------
    value : object
        Raw value from CLI, env, or config.
    default : bool | None
        Returned if value is None and no coercion is possible.

    Returns
    -------
    bool | None
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normed = value.strip().lower()
        if normed in {"true", "1", "yes", "on"}:
            return True
        if normed in {"false", "0", "no", "off"}:
            return False
    return default
```

```python
# src/cli/commands/build.py — replace private helpers (lines 862-902)
# REMOVE: def _coerce_bool_flag(...): ...
# REMOVE: def _coerce_int_param(...): ...
# ADD:
from src.utils.value_coercion import coerce_bool, coerce_int
```

### Files to Edit

- `src/utils/value_coercion.py` — add missing coercion cases from `coercion.py`; ensure
  consistent `bool` handling
- `src/utils/coercion.py` — delete after moving any remaining behavior into
  `value_coercion.py`
- `src/cli/commands/build.py:862-902` — replace private helpers with `value_coercion`
  imports

### New Files to Create

- `tests/unit/utils/test_value_coercion.py` (if not already thorough)

### Legacy Decommission/Delete Scope

The 4 private coercion helpers in `src/cli/commands/build.py:862-902` and
`src/utils/coercion.py` module-level re-export/duplicate helpers.

---

## S8. Remaining small quick wins batch (QW-01,03,04,06,07,09-19,22-33,35,43)

### Goal

Land all remaining quick wins from the synthesis that are not covered by S1-S7. These are
batched into a single scope item because each change is local, non-breaking, and
independently reviewable.

### Files to Edit (grouped by theme)

**Session construction (QW-01):**
- `src/datafusion_engine/session/context_pool.py:47-66,263-382` — remove `_apply_setting`
  dual method+key path; replace all 12 call sites with `config.set(key, str(value))`

**Encoding bare session (QW-03):**
- `src/datafusion_engine/encoding/policy.py:83-139` — replace `_datafusion_context()` +
  DataFusion temp-table path with `pa.ChunkedArray.dictionary_encode().cast(dict_type)`

**Arrow alias normalization (QW-04):**
- `src/datafusion_engine/schema/contracts.py:492-499` — merge `_normalize_type_string`
  rewrites into `schema/type_normalization.py`

**_WriterPort consolidation (QW-06):**
- `src/datafusion_engine/cache/inventory.py:91-94` — remove local `_WriterPort`
- `src/datafusion_engine/cache/ledger.py:46-49` — remove local `_WriterPort`
- Create `src/datafusion_engine/cache/_ports.py` — canonical `_WriterPort` definition

**Span recording for errors (QW-07):**
- `src/datafusion_engine/delta/maintenance.py:103` — add `span.record_exception(e)`
- `src/datafusion_engine/cache/metadata_snapshots.py:109-113` — add `span.record_exception`

**Plan identity constant (QW-10):**
- `src/datafusion_engine/plan/plan_identity.py:108` — declare
  `PLAN_IDENTITY_PAYLOAD_VERSION = 4` as module constant

**DDL type consolidation (QW-11):**
- `src/datafusion_engine/udf/extension_ddl.py:75-127` — remove `_ddl_type_name_from_arrow`
  and `_ddl_type_name_from_string`; import from `dataset/ddl_types.py`

**UDF expr kwargs (QW-12):**
- `src/datafusion_engine/udf/expr.py:136-145` — remove `**kwargs` or enforce
  keyword-to-position semantics

**Dead tier branches (QW-13):**
- `src/datafusion_engine/udf/metadata.py:721-737` — remove dead `tier != "builtin"`
  branches

**Expr dispatch extraction (QW-14):**
- `src/datafusion_engine/expr/spec.py:616-723` — extract `_EXPR_CALLS`, `_SQL_CALLS`
  dispatch dicts into `expr/dispatch.py`

**Required entrypoints (QW-15):**
- `src/datafusion_engine/extensions/required_entrypoints.py:5-20` — add
  `install_planner_rules` and `install_physical_rules`

**SQL policy exception type (QW-16):**
- `src/datafusion_engine/sql/guard.py:227-233` — unify to `SqlPolicyViolation` replacing
  mixed `ValueError`/`PermissionError`

**Catalog default import (QW-17):**
- `src/datafusion_engine/catalog/introspection.py:470` — remove `DEFAULT_DF_POLICY`
  import; pass defaults as parameter

**Env var rename (QW-09):**
- `src/datafusion_engine/plan/profiler.py:64` — rename `CODEANATOMY_DISABLE_DF_EXPLAIN`
  to `CODEANATOMY_ENABLE_DF_EXPLAIN` with `"0"` default

**Rust quick wins (QW-22,23,24,25):**
- `rust/datafusion_ext/src/lib.rs:55-90` — make `install_expr_planners_native` consume
  `domain_expr_planners()` (QW-22)
- `rust/datafusion_python/src/codeanatomy_ext/plugin_bridge.rs:291-333` — cache
  `extract_session_ctx` result locally (QW-23)
- `rust/datafusion_python/src/codeanatomy_ext/schema_evolution.rs:44-53` — remove
  `SchemaEvolutionAdapterFactory`; use `DefaultPhysicalExprAdapterFactory` (QW-24)
- `rust/codeanatomy_engine_py/src/result.rs:32,49,70,81` — parse `inner_json` once
  at construction; store `serde_json::Value` (QW-25)

**Rust scan config (QW-19):**
- `rust/codeanatomy_engine/src/providers/scan_config.rs:119-155` — replace
  `ProviderCapabilities` bool flags with `Option<FilterPushdownStatus>`

**Rust plan display (QW-18):**
- `rust/codeanatomy_engine/src/compiler/plan_compiler.rs:443,447,451` — replace
  `format!("{:?}", df.logical_plan())` with `display_indent()`

**Coordination cleanup (QW-28,29,30):**
- `src/extract/coordination/extract_plan_builder.py` — remove any residual runtime
  signature probing and rely on typed extractor contracts
- `src/extract/coordination/materialization.py` — replace inline schema literals with
  `dataset_schema("repo_files_v1")`
- `src/extract/coordination/extract_session.py` — capture per-extractor start times before
  scheduling futures

**Confidence constants (QW-26):**
- `src/semantics/joins/inference.py:49-54` — move constants to `strategies.py`
- `src/semantics/ir_pipeline.py:1086-1091` — import from `strategies.py`

**OTel dict-cell (QW-32):**
- `src/obs/otel/bootstrap.py:263` — replace `_STATE: dict[str, ...]` with direct
  nullable module variable

**SemanticIR hints (QW-33):**
- `src/relspec/policy_compiler.py:451-483` — add `join_strategy_hints()`,
  `cache_policy_hints()`, `confidence_hints()` to `SemanticIR`

**Delta retry extraction (QW-35):**
- `src/storage/deltalake/delta_write.py:165-191` — replace inline retry loop with
  `retry_with_policy(...)`
- `src/storage/deltalake/delta_runtime_ops.py:275-286` — extract shared
  `retry_with_policy(fn, *, policy, span)`

**SCIP config extraction (QW-43):**
- `src/cli/commands/build.py:735-860` — move `_build_scip_config` logic into
  `ScipIndexConfig.from_cli_overrides(...)`
- `src/extract/extractors/scip/config.py` — add `from_cli_overrides(...)` constructor and
  helper normalizers

**BuildResult deserialization (from synthesis Section 5):**
- `src/graph/product_build.py:473-491` — replace bespoke JSON parsing with
  `msgspec.json.decode(bytes, type=FinalizeDeltaReport)`

### New Files to Create

- `src/datafusion_engine/cache/_ports.py` — `_WriterPort` protocol
- `tests/unit/storage/test_delta_retry_with_policy.py` — retry extraction tests
- `tests/unit/cli/test_scip_index_config_overrides.py` — SCIP override parsing tests

### Legacy Decommission/Delete Scope

- `_apply_setting` method from `context_pool.py`
- Local `_WriterPort` definitions in `inventory.py` and `ledger.py`
- `_datafusion_context()` helper and DataFusion temp-table path in `encoding/policy.py`
- Private coercion helper duplication in `extension_ddl.py`
- Inline retry loops duplicated across `delta_write.py` and `delta_runtime_ops.py`
- `_build_scip_config` monolith in `src/cli/commands/build.py`

---

## S9. datafusion_python FFI provider signature update — THE GATE

### Goal

Update all `__datafusion_*_provider__` call sites in `datafusion_python` to pass `session`
as the first argument per DF52 §K. Update `FFI_TableProvider::new()` to accept
`TaskContextProvider`. Bump `datafusion` and `datafusion-ffi` pins to `"52"`. This is the
sole compile-time prerequisite for all downstream crate upgrades.

### Representative Code Snippets

```rust
// rust/datafusion_python/src/utils.rs:170-171 — before (DF51)
let provider = obj.getattr("__datafusion_table_provider__")?.call0()?;

// After (DF52): pass session as first positional arg
let provider = obj.getattr("__datafusion_table_provider__")?.call1((session.clone(),))?;
```

```rust
// rust/datafusion_python/src/context.rs:607-609 — before (DF51)
let cat_provider = provider.getattr("__datafusion_catalog_provider__")?.call0()?;

// After (DF52)
let cat_provider = provider
    .getattr("__datafusion_catalog_provider__")?
    .call1((session.clone(),))?;
```

```rust
// rust/datafusion_python/src/catalog.rs:115-118 — before (DF51)
let schema_prov = schema_provider
    .getattr("__datafusion_schema_provider__")?
    .call0()?;

// After (DF52)
let schema_prov = schema_provider
    .getattr("__datafusion_schema_provider__")?
    .call1((session.clone(),))?;
```

```rust
// rust/datafusion_python/src/codeanatomy_ext/helpers.rs:132-141
// FFI_TableProvider::new() now requires TaskContextProvider
// DF52 signature: FFI_TableProvider::new(provider, supports_pushdown, extension_codec, task_ctx_provider)
let ffi_provider = FFI_TableProvider::new(
    py_provider,
    supports_pushdown,
    extension_codec.clone(),
    task_ctx_provider.clone(),  // new in DF52
);
```

```toml
# rust/datafusion_python/Cargo.toml — bump versions
[dependencies]
datafusion = "52"
datafusion-ffi = "52"
# deltalake is aligned to "0.31.0" with DataFusion 52.1.0
```

### Files to Edit

- `rust/datafusion_python/src/utils.rs:170-171` — update call to pass `session`
- `rust/datafusion_python/src/context.rs:607-628` — update catalog provider call
- `rust/datafusion_python/src/catalog.rs:115-139,363-383` — update schema provider calls
- `rust/datafusion_python/src/codeanatomy_ext/helpers.rs:132-141` — update
  `FFI_TableProvider::new()`
- `rust/datafusion_python/src/codeanatomy_ext/schema_evolution.rs:370-383` — update
  `FFI_TableProvider::new()`
- `rust/datafusion_python/Cargo.toml` — bump `datafusion`, `datafusion-ffi` to `"52"`
- `rust/datafusion_python/src/common/df_schema.rs` — audit `DFSchema::field()` return
  type (`&FieldRef` vs `Field` clone)
- `rust/datafusion_python/src/common/schema.rs` — audit same

### New Files to Create

None (compilation is the verification gate).

### Legacy Decommission/Delete Scope

All `.call0()` invocations on `__datafusion_*_provider__` method names.

---

## S10. datafusion_ext DF52 upgrade

### Goal

Upgrade `rust/datafusion_ext` to DataFusion 52. This builds on S5 (CoalesceBatches
pre-removal) and S9 (the FFI gate). Key changes: `FFI_TableProvider::new()` at the
`df_plugin_codeanatomy` call sites, auditing delta-control-plane scan config against DF52
`FileSource` projection API.

### Representative Code Snippets

```rust
// rust/df_plugin_codeanatomy/src/lib.rs:431,462 — update FFI_TableProvider::new
// Before (DF51): FFI_TableProvider::new(provider, supports_pushdown, extension_codec)
// After (DF52): supply task_ctx_provider from session
let ffi = FFI_TableProvider::new(
    provider,
    supports_pushdown,
    extension_codec,
    session.task_ctx(),  // new TaskContextProvider argument
);
```

```rust
// rust/datafusion_ext/src/delta_control_plane.rs — audit DeltaScanConfigBuilder
// DF52 removes FileSource::with_projection -> use try_pushdown_projection
// deltalake 0.31.0 alignment is handled in S13
impl DeltaTableScanProvider {
    fn create_scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        // DF52: projection pushdown via try_pushdown_projection
        let pushed = self.file_source.try_pushdown_projection(&projection_exprs)?;
        ...
    }
}
```

```toml
# rust/datafusion_ext/Cargo.toml
[dependencies]
datafusion = "52.1.0"
# deltalake 0.31.0 alignment handled in S13
```

### Files to Edit

- `rust/datafusion_ext/Cargo.toml` — bump `datafusion` to `"52.1.0"`
- `rust/df_plugin_codeanatomy/src/lib.rs:431,462` — update `FFI_TableProvider::new()`
- `rust/datafusion_ext/src/delta_control_plane.rs` — audit `DeltaScanConfigBuilder`
  against DF52 `FileSource::try_pushdown_projection` (may be stub-only until S13)
- `rust/datafusion_ext/src/physical_rules.rs` — verify CoalesceBatches is gone (S5 done)

### New Files to Create

None.

### Legacy Decommission/Delete Scope

DF51 `FFI_TableProvider::new()` three-argument form.

---

## S11. codeanatomy_engine DF52 upgrade

### Goal

Upgrade `rust/codeanatomy_engine` to DataFusion 52. Remove
`apply_post_filter_coalescing` (built on S5), update `standard_scan_config` in
`scan_config.rs` against the DF52 `FileSource`-based projection API, and bump
`datafusion-substrait` and `datafusion-proto` pins.

### Representative Code Snippets

```rust
// rust/codeanatomy_engine/src/providers/scan_config.rs:33-45 — DF52 projection API
// Before (DF51): scan_config.with_projection(indices)
// After (DF52): builder.with_projection_indices(indices)? — returns Result<Self>
fn standard_scan_config(
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    file_groups: Vec<Vec<PartitionedFile>>,
) -> datafusion::error::Result<FileScanConfig> {
    let mut builder = FileScanConfigBuilder::new(object_store_url, schema, file_source);
    if let Some(indices) = projection {
        builder = builder.with_projection_indices(indices)?;  // DF52: returns Result
    }
    Ok(builder.build())
}
```

```rust
// rust/codeanatomy_engine/src/rules/physical.rs — remove apply_post_filter_coalescing
// DF52 embeds coalescing into operators; this function becomes a no-op, then delete it.
// REMOVE the function and all call sites in optimizer_pipeline.rs.
```

```toml
# rust/codeanatomy_engine/Cargo.toml
[dependencies]
datafusion = "52.1.0"
datafusion-substrait = "52.1.0"
datafusion-proto = "52.1.0"
```

### Files to Edit

- `rust/codeanatomy_engine/Cargo.toml` — bump all three version pins
- `rust/codeanatomy_engine/src/rules/physical.rs:133-140` — remove
  `apply_post_filter_coalescing`
- `rust/codeanatomy_engine/src/providers/scan_config.rs:33-45` — update to
  `FileScanConfigBuilder::with_projection_indices` returning `Result<Self>`
- `rust/codeanatomy_engine/src/compiler/plan_compiler.rs` — audit any
  `build_row_filter` calls (Parquet row-filter builder drops schema parameter in DF52)

### New Files to Create

None.

### Legacy Decommission/Delete Scope

`apply_post_filter_coalescing` function and all call sites.

---

## S12. Python extension layer DF52 update

### Goal

Update the Python-side FFI glue in `extensions/datafusion_ext.py` to handle DF52's
`(py, session)` PyCapsule signature for `__datafusion_*_provider__` methods. Audit
`catalog/provider.py` for `__datafusion_table_provider__` compatibility. This is a DF52-only
path: `_normalize_args` must inject `session` for provider methods unconditionally.

### Representative Code Snippets

```python
# src/datafusion_engine/extensions/datafusion_ext.py:76-93
# DF52: provider methods receive (py, session) instead of (py,)
def _normalize_args(
    provider_method: str,
    args: tuple[object, ...],
    *,
    session: SessionContext | None,
) -> tuple[object, ...]:
    """Inject session argument for DF52 PyCapsule provider signatures.

    Parameters
    ----------
    provider_method : str
        One of __datafusion_table_provider__, __datafusion_catalog_provider__, etc.
    args : tuple[object, ...]
        Current argument tuple (from PyO3 call site).
    session : SessionContext | None
        Active session; required for provider-method calls.

    Returns
    -------
    tuple[object, ...]
        args with session prepended when appropriate.
    """
    _DF52_PROVIDER_METHODS = {
        "__datafusion_table_provider__",
        "__datafusion_catalog_provider__",
        "__datafusion_schema_provider__",
    }
    if provider_method in _DF52_PROVIDER_METHODS:
        if session is None:
            msg = f"{provider_method} requires session for DF52 provider capsule contract."
            raise ValueError(msg)
        return (session, *args)
    return args
```

```python
# src/datafusion_engine/catalog/provider.py:76 — audit _table_from_dataset
# Ensure __datafusion_table_provider__ is called with session (DF52 hard cutover).
```

### Files to Edit

- `src/datafusion_engine/extensions/datafusion_ext.py:76-78,83-93` — update
  `_normalize_args` to inject `session`
- `src/datafusion_engine/catalog/provider.py:76` — audit `_table_from_dataset`

### New Files to Create

- `tests/unit/datafusion_engine/extensions/test_normalize_args_df52.py`

### Legacy Decommission/Delete Scope

All DF51 single-argument provider call paths.

---

## S13. deltalake 0.31.0 alignment

### Goal

Align all Rust crates to `deltalake = "0.31.0"` (DF52-compatible baseline). Audit
`delta_control_plane.rs` and `providers/scan_config.rs` against DF52
`FileSource`-based scan pushdown API.

### Representative Code Snippets

```toml
# rust/datafusion_python/Cargo.toml — DF52-compatible deltalake baseline
[dependencies]
deltalake = "0.31.0"

# rust/datafusion_ext/Cargo.toml
[dependencies]
deltalake = "0.31.0"
```

```rust
// rust/datafusion_ext/src/delta_control_plane.rs — audit DeltaScanConfigBuilder
// With deltalake 0.31.0 on DF52:
// FileSource::with_statistics and FileSource::with_projection removed
// Use FileScanConfig::statistics() and FileSource::try_pushdown_projection instead
// Also: FilePruner::try_new() drops partition_fields parameter
```

### Files to Edit

- `rust/datafusion_python/Cargo.toml` — align `deltalake = "0.31.0"`
- `rust/datafusion_ext/Cargo.toml` — align `deltalake = "0.31.0"`
- `rust/datafusion_ext/src/delta_control_plane.rs` — audit full scan config chain
- `rust/codeanatomy_engine/src/providers/scan_config.rs` — audit partition handling
  (`replace_columns_with_literals()` pre-adapter pattern)

### New Files to Create

None.

### Legacy Decommission/Delete Scope

`FileSource::with_statistics`, `FileSource::with_projection` call sites (moved to
`FileScanConfig::statistics()` and `FileSource::try_pushdown_projection` respectively).

---

## S14. FileStatisticsCache + DefaultListFilesCache adoption

### Goal

Replace the bespoke SQL-based cache queries in `cache/metadata_snapshots.py` with DF52's
native `ctx.statistics_cache()` and `ctx.list_files_cache()` APIs. Partially retire
`cache/inventory.py` and `cache/ledger.py` file-statistics Delta tables for the
file-metadata use case (~200 LOC reduction). Retain plan-fingerprint cache hit logic.

### Representative Code Snippets

```python
# src/datafusion_engine/cache/metadata_snapshots.py — after DF52
# Before: SELECT * FROM metadata_cache() -> try/except SQL failure
# After:
from datafusion import SessionContext

def snapshot_datafusion_caches(ctx: SessionContext) -> CacheSnapshot:
    """Snapshot DF52 built-in file statistics and list-files caches.

    Parameters
    ----------
    ctx : SessionContext
        Active DataFusion session.

    Returns
    -------
    CacheSnapshot
        Typed snapshot of statistics cache and list-files cache state.
    """
    stats_cache = ctx.statistics_cache()
    list_cache = ctx.list_files_cache()
    return CacheSnapshot(
        statistics_entries=list(stats_cache.entries()),
        list_files_entries=list(list_cache.entries()),
        captured_at=datetime.utcnow().isoformat(),
    )
```

```python
# src/datafusion_engine/session/runtime_extensions.py:737-770 — replace snapshot_datafusion_caches
# Remove the try/except SQL path; call snapshot_datafusion_caches() directly.
```

### Files to Edit

- `src/datafusion_engine/cache/metadata_snapshots.py:26-35,84` — replace SQL cache
  queries with `ctx.statistics_cache()` / `ctx.list_files_cache()`
- `src/datafusion_engine/session/runtime_extensions.py:737-770` — update
  `snapshot_datafusion_caches` caller

### New Files to Create

- `tests/unit/datafusion_engine/cache/test_df52_cache_snapshot.py`

### Legacy Decommission/Delete Scope

SQL `SELECT * FROM metadata_cache()` try/except pattern in `metadata_snapshots.py`.
File-statistics Delta tables in `inventory.py` and `ledger.py` for the file-metadata
use case (retain plan-fingerprint hit logic).

---

## S15. df.cache() adoption for semantic pipeline high-fan-out views

### Goal

Add `"memory"` as a new `CachePolicy` variant backed by DF52's `df.cache()`. Apply it to
the top-3 high-fan-out views in the semantic pipeline where the view is small (<10 MB)
and consumed 3+ times per CPG build. This eliminates redundant scan re-execution.

### Representative Code Snippets

```python
# src/semantics/pipeline_cache.py — add new CachePolicy variant
class CachePolicy(str, Enum):
    """Cache placement policy for semantic views."""
    NONE = "none"
    DELTA_STAGING = "delta_staging"
    MEMORY = "memory"   # DF52: df.cache() materialization


def apply_cache_policy(
    df: DataFrame,
    policy: CachePolicy,
    *,
    view_name: str,
) -> DataFrame:
    """Apply cache policy to DataFrame.

    Parameters
    ----------
    df : DataFrame
        Source DataFrame.
    policy : CachePolicy
        Policy to apply.
    view_name : str
        For diagnostic logging.

    Returns
    -------
    DataFrame
        Potentially cached DataFrame.
    """
    if policy == CachePolicy.MEMORY:
        logger.debug("Applying df.cache() to view %s", view_name)
        return df.cache()
    if policy == CachePolicy.DELTA_STAGING:
        # existing delta staging logic
        ...
    return df
```

```python
# src/semantics/ir_pipeline.py — _cache_policy_for_position
def _cache_policy_for_position(view_name: str, fan_out: int) -> CachePolicy:
    """Return appropriate cache policy for a view given its fan-out count."""
    if fan_out >= 3 and _is_small_view(view_name):
        return CachePolicy.MEMORY
    return CachePolicy.NONE
```

### Files to Edit

- `src/semantics/pipeline_cache.py` (or wherever `CachePolicy` is defined) — add
  `MEMORY` variant and `apply_cache_policy` dispatch
- `src/semantics/ir_pipeline.py` — update `_cache_policy_for_position` to emit
  `CachePolicy.MEMORY` for eligible views

### New Files to Create

- `tests/unit/semantics/test_memory_cache_policy.py`

### Legacy Decommission/Delete Scope

None (additive).

---

## S16. RelationPlanner port and adapter

### Goal

Define a `RelationPlannerPort` protocol in `src/datafusion_engine/expr/relation_planner.py`
and a `CodeAnatomyRelationPlanner` default implementation. Register it in
`DfPluginManifestV1::capabilities` in `rust/df_plugin_api/src/manifest.rs`. This enables
FROM-clause extensions (DF52 §F) without ad-hoc probing.

### Representative Code Snippets

```python
# src/datafusion_engine/expr/relation_planner.py — new file
from __future__ import annotations

from typing import Protocol


class RelationPlannerPort(Protocol):
    """Protocol for DF52 RelationPlanner FROM-clause extension hooks.

    Any object implementing this protocol can be registered as a relation planner
    with the DataFusion session to intercept FROM-clause constructs.
    """

    def plan_relation(
        self,
        relation: object,
        schema: object,
    ) -> object | None:
        """Attempt to plan a FROM-clause construct.

        Parameters
        ----------
        relation : object
            The SQL relation node from the planner.
        schema : object
            The current DFSchema context.

        Returns
        -------
        object | None
            A LogicalPlan if the relation is handled, else None.
        """
        ...
```

```rust
// rust/df_plugin_api/src/manifest.rs — add relation_planner capability
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DfPluginManifestV1 {
    pub capabilities: PluginCapabilities,
    ...
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PluginCapabilities {
    pub install_expr_planners: bool,
    pub install_physical_rules: bool,
    pub install_relation_planner: bool,  // new for DF52
    ...
}
```

### Files to Edit

- `rust/df_plugin_api/src/manifest.rs` — add `install_relation_planner` to
  `PluginCapabilities`

### New Files to Create

- `src/datafusion_engine/expr/relation_planner.py` — `RelationPlannerPort` protocol +
  `CodeAnatomyRelationPlanner`
- `tests/unit/datafusion_engine/expr/test_relation_planner.py`

### Legacy Decommission/Delete Scope

None (additive).

---

## S17. TableProvider DML hooks cutover

### Goal

Cut over to DF52 `TableProvider DELETE/UPDATE` hooks (§E) as the canonical DML mutation
path. Remove parallel Python IPC payload routing once parity is validated by tests.

### Representative Code Snippets

```python
# src/datafusion_engine/delta/control_plane_mutation.py — cutover target
# Route delete/update through provider-backed DML hooks; no dual mutation path.
```

```rust
// rust/datafusion_ext/src/delta_control_plane.rs — DF52 hook stubs
// impl TableProvider for DeltaCodeAnatomyProvider {
//     async fn delete_rows(&self, predicate: &Expr) -> Result<u64> { ... }
//     async fn update_rows(&self, assignments: &[(Column, Expr)], predicate: &Expr) -> Result<u64> { ... }
// }
```

### Files to Edit

- `rust/datafusion_ext/src/delta_control_plane.rs` — implement `delete_rows` /
  `update_rows` hooks and wire provider path
- `src/datafusion_engine/delta/control_plane_mutation.py` — remove parallel payload
  dispatch branches

### New Files to Create

- `tests/unit/datafusion_engine/delta/test_dml_hooks_cutover.py`

### Legacy Decommission/Delete Scope

`delta_delete_request_payload` and any parallel delete/update IPC payload path.

---

## S18. Sort pushdown adoption for interval_align_kernel

### Goal

Leverage DF52's native sort pushdown to scans (§D.2) in `kernels.py` for the
`interval_align_kernel` join. This eliminates the explicit Python sort step (~50 LOC).
If the Rust `TableProvider` migration in S26 is completed first, this becomes a no-op;
otherwise adopt the DF52 sort-pushdown path at the Python DataFrame level.

### Representative Code Snippets

```python
# src/datafusion_engine/kernels.py — before (explicit sort)
sorted_df = df.sort(col("ts").sort(ascending=True))
result = sorted_df.join(...)

# After (DF52 sort pushdown): remove explicit sort; DataFusion's PushdownSort rule
# propagates the sort requirement into the scan automatically when the source is sorted.
# Just define the join requirement; the planner handles sort ordering.
result = df.join(
    other_df,
    join_type="inner",
    left_cols=["entity_id"],
    right_cols=["entity_id"],
)
# Sort requirement is expressed via .sort() on the output if needed for determinism,
# but the scan-level pushdown eliminates the intermediate materialization.
```

### Files to Edit

- `src/datafusion_engine/kernels.py` — remove explicit pre-sort in `interval_align_kernel`

### New Files to Create

- `tests/unit/datafusion_engine/test_interval_align_kernel.py` (verify sort correctness
  after removing explicit sort)

### Legacy Decommission/Delete Scope

Explicit `sorted_df = df.sort(...)` pre-join step in `kernels.py`.

---

## S19. ExprPlanner/RelationPlanner canonical extension points

### Goal

Consolidate the ImportError-guarded probe in `_install_expr_planners` and the
`expr_planner_hook` in `PolicyBundleConfig` into a canonical `PlannerExtensionPort`
protocol. This eliminates attribute probing and makes planner extension deterministic.
Implemented alongside S16 (RelationPlanner port).

### Representative Code Snippets

```python
# src/datafusion_engine/session/protocols.py — define PlannerExtensionPort
from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class PlannerExtensionPort(Protocol):
    """Protocol for injecting ExprPlanner and RelationPlanner into a DataFusion session.

    Implementations replace the ad-hoc ImportError probe pattern in runtime_extensions.py.
    """

    def install_expr_planners(self, ctx: object) -> None:
        """Install custom ExprPlanner instances into ctx."""
        ...

    def install_relation_planner(self, ctx: object) -> None:
        """Install custom RelationPlanner into ctx."""
        ...
```

```python
# src/datafusion_engine/session/runtime_extensions.py:935-972
# Replace ImportError-guarded probe with isinstance(plugin, PlannerExtensionPort) check
from src.datafusion_engine.session.protocols import PlannerExtensionPort

def _install_expr_planners(ctx: SessionContext, plugin: object) -> None:
    if isinstance(plugin, PlannerExtensionPort):
        plugin.install_expr_planners(ctx)
    # no probe, no try/except ImportError
```

### Files to Edit

- `src/datafusion_engine/session/protocols.py` — add `PlannerExtensionPort`
- `src/datafusion_engine/session/runtime_extensions.py:935-972` — replace ImportError
  probe with `isinstance(plugin, PlannerExtensionPort)`

### New Files to Create

None (`protocols.py` already exists; extend it).

### Legacy Decommission/Delete Scope

ImportError-guarded `expr_planner_hook` probe in `runtime_extensions.py`.

---

## S20. Lineage walker retirement — plan/walk.py + lineage/reporting.py → Rust bridge

### Goal

Extend the Rust bridge to return `LineageReport` as structured JSON from
`compiler/lineage.rs`. Retire `plan/walk.py` (~110 LOC) and the visitor logic in
`lineage/reporting.py` (~570 LOC). Retain Python types (`ScanLineage`, `JoinLineage`,
`LineageReport`) as pure `msgspec.Struct` deserialization targets. Total: ~680 LOC
reduction.

### Representative Code Snippets

```rust
// rust/codeanatomy_engine/src/compiler/lineage.rs — expose JSON serialization
// LineageReport already derives Serialize; add a public entry point:
pub fn extract_lineage_json(plan: &LogicalPlan) -> datafusion::error::Result<String> {
    let report = extract_lineage(plan)?;
    serde_json::to_string(&report).map_err(|e| {
        DataFusionError::Internal(format!("lineage serialization failed: {e}"))
    })
}
```

```python
# src/datafusion_engine/plan/rust_bundle_bridge.py — call Rust and decode
import msgspec

from src.datafusion_engine.lineage.protocols import LineageReport as LineageReportType

def extract_lineage_from_plan(plan_bytes: bytes) -> LineageReportType:
    """Extract lineage by delegating to the Rust bridge.

    Parameters
    ----------
    plan_bytes : bytes
        Serialized LogicalPlan (substrait or proto bytes).

    Returns
    -------
    LineageReportType
        Decoded lineage report from the Rust engine.
    """
    raw_json = _codeanatomy_engine.extract_lineage_json(plan_bytes)
    return msgspec.json.decode(raw_json.encode(), type=LineageReportType)
```

```python
# src/datafusion_engine/lineage/protocols.py — msgspec target types (KEEP, update)
import msgspec

class ScanLineage(msgspec.Struct, frozen=True):
    table_name: str
    required_columns: list[str]
    filters: list[str]

class LineageReport(msgspec.Struct, frozen=True):
    scans: list[ScanLineage]
    required_columns_by_dataset: dict[str, list[str]]
    filters: list[str]
    referenced_tables: list[str]
    required_udfs: list[str]
    required_rewrite_tags: list[str]
    referenced_udfs: list[str]
```

```python
# src/datafusion_engine/lineage/reporting.py — reduce to thin delegator
# Remove Python visitor (~570 LOC); replace with:
from src.datafusion_engine.plan.rust_bundle_bridge import extract_lineage_from_plan

def build_lineage_report(plan: object) -> LineageReport:
    """Delegate lineage extraction to Rust bridge."""
    plan_bytes = _serialize_plan(plan)
    return extract_lineage_from_plan(plan_bytes)
```

### Files to Edit

- `rust/codeanatomy_engine/src/compiler/lineage.rs` — add `extract_lineage_json` public
  entry point
- `rust/codeanatomy_engine_py/src/` — expose `extract_lineage_json` to Python via PyO3
- `src/datafusion_engine/plan/rust_bundle_bridge.py` — add `extract_lineage_from_plan`
- `src/datafusion_engine/lineage/reporting.py` — replace visitor with delegation call
- `src/datafusion_engine/lineage/protocols.py` — update `LineageReport` to `msgspec.Struct`

### New Files to Create

- `tests/unit/datafusion_engine/lineage/test_lineage_rust_bridge.py`

### Legacy Decommission/Delete Scope

- `src/datafusion_engine/plan/walk.py` — entire file (~110 LOC)
- Python plan visitor logic in `lineage/reporting.py` (~570 LOC); retain only the thin
  delegation wrapper and the msgspec target types

---

## S21. UDF snapshot contract upgrade — typed msgpack from Rust

### Goal

Emit a typed, msgpack-encoded `RustUdfSnapshot` from `datafusion_ext` instead of a
generic Python dict. This allows `udf/extension_validation.py` (~481 LOC),
`udf/extension_snapshot_runtime.py` (~495 LOC), and most of `udf/signature.py` (~205 LOC)
to be replaced by `msgspec.convert(raw_bytes, type=RustUdfSnapshot)`. Estimated ~800-900
LOC reduction.

### Representative Code Snippets

```rust
// rust/datafusion_ext/src/registry_snapshot.rs — typed contract
#[derive(Debug, Serialize, Deserialize)]
pub struct RustUdfSnapshot {
    pub scalar_udfs: Vec<UdfEntry>,
    pub aggregate_udfs: Vec<UdafEntry>,
    pub table_udfs: Vec<UdtfEntry>,
    pub window_udfs: Vec<UdwfEntry>,
    pub plugin_version: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UdfEntry {
    pub name: String,
    pub signature: UdfSignatureContract,
    pub return_type_ddl: String,
    pub documentation: Option<String>,
    pub aliases: Vec<String>,
}

// Emit as msgpack bytes from the PyO3 boundary:
pub fn emit_snapshot_msgpack(registry: &UdfRegistry) -> PyResult<PyBytes> {
    let snapshot = build_snapshot(registry);
    let bytes = rmp_serde::to_vec_named(&snapshot)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
    Ok(PyBytes::new(py, &bytes))
}
```

```python
# src/datafusion_engine/udf/runtime_snapshot_types.py — replace with msgspec contract
import msgspec


class UdfSignatureContract(msgspec.Struct, frozen=True):
    """UDF signature as emitted by the Rust registry snapshot."""
    arg_types: list[str]
    return_type: str
    volatility: str


class UdfEntry(msgspec.Struct, frozen=True):
    name: str
    signature: UdfSignatureContract
    return_type_ddl: str
    documentation: str | None = None
    aliases: list[str] = []


class RustUdfSnapshot(msgspec.Struct, frozen=True):
    """Typed contract for the msgpack payload emitted by datafusion_ext."""
    scalar_udfs: list[UdfEntry]
    aggregate_udfs: list[UdfEntry]
    table_udfs: list[UdfEntry]
    window_udfs: list[UdfEntry]
    plugin_version: str
```

```python
# src/datafusion_engine/udf/extension_core.py — replace snapshot loading
import msgspec
from src.datafusion_engine.udf.runtime_snapshot_types import RustUdfSnapshot

def load_udf_snapshot(raw_bytes: bytes) -> RustUdfSnapshot:
    """Decode typed UDF snapshot from Rust msgpack payload."""
    return msgspec.convert(
        msgspec.msgpack.decode(raw_bytes),
        type=RustUdfSnapshot,
    )
```

### Files to Edit

- `rust/datafusion_ext/src/registry_snapshot.rs` — add typed `RustUdfSnapshot` struct
  with `serde` derives + `emit_snapshot_msgpack` function
- `rust/datafusion_ext_py/src/` — expose `emit_snapshot_msgpack` via PyO3
- `src/datafusion_engine/udf/runtime_snapshot_types.py` — replace with `msgspec.Struct`
  contracts
- `src/datafusion_engine/udf/extension_core.py` — use `load_udf_snapshot`

### New Files to Create

- `tests/unit/datafusion_engine/udf/test_udf_snapshot_contract.py`
- `tests/msgspec_contract/test_rust_udf_snapshot_roundtrip.py`

### Legacy Decommission/Delete Scope

- `src/datafusion_engine/udf/extension_snapshot_runtime.py` — 14 deferred-import wrapper
  functions (~200 LOC of ceremony)
- Most of `src/datafusion_engine/udf/extension_validation.py` — Python dict validation
  logic replaced by `msgspec.convert` schema enforcement
- Type-string parsing in `src/datafusion_engine/udf/signature.py` — Rust now pre-computes
  DDL type strings

---

## S22. Extraction runtime loop consolidation

### Goal

Extract a shared `ExtractionRuntime` loop into
`src/extract/coordination/extraction_runtime_loop.py`. Reduce each of the 4
`builders_runtime.py` files to ~100-150 LOC adapters. Eliminate ~2,500 LOC of duplicated
orchestration ceremony.

### Representative Code Snippets

```python
# src/extract/coordination/extraction_runtime_loop.py — new shared loop
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

import pyarrow as pa


@dataclass(frozen=True)
class ExtractionStages:
    """Injectable stages for the extraction runtime loop.

    Allows each extractor (AST, bytecode, CST, tree-sitter) to plug in their
    specific setup, execute, and teardown logic without duplicating the loop frame.
    """
    setup: Callable[[], object]
    execute: Callable[[object, str], pa.RecordBatch]
    teardown: Callable[[object], None] | None = None


def run_extraction_loop(
    files: list[str],
    stages: ExtractionStages,
    *,
    parallelism: int = 1,
) -> list[pa.RecordBatch]:
    """Run the extraction loop with injectable stages.

    Parameters
    ----------
    files : list[str]
        Source files to process.
    stages : ExtractionStages
        Extractor-specific setup/execute/teardown functions.
    parallelism : int
        Degree of parallelism (1 = sequential).

    Returns
    -------
    list[pa.RecordBatch]
        All output record batches concatenated.
    """
    ctx = stages.setup()
    batches: list[pa.RecordBatch] = []
    try:
        for f in files:
            batch = stages.execute(ctx, f)
            batches.append(batch)
    finally:
        if stages.teardown:
            stages.teardown(ctx)
    return batches
```

```python
# src/extract/extractors/ast/builders_runtime.py — after consolidation (~100 LOC adapter)
from src.extract.coordination.extraction_runtime_loop import ExtractionStages, run_extraction_loop
from src.extract.extractors.ast.builders import AstExtractorContext, setup_ast, execute_ast

def run_ast_extraction(files: list[str], *, parallelism: int = 1) -> list[pa.RecordBatch]:
    """Run AST extraction using shared runtime loop."""
    return run_extraction_loop(
        files,
        ExtractionStages(setup=setup_ast, execute=execute_ast),
        parallelism=parallelism,
    )
```

### Files to Edit

- `src/extract/extractors/ast/builders_runtime.py` — reduce to adapter using shared loop
- `src/extract/extractors/bytecode/builders_runtime.py` — reduce to adapter
- `src/extract/extractors/cst/builders_runtime.py` — reduce to adapter
- `src/extract/extractors/tree_sitter/builders_runtime.py` — reduce to adapter

### New Files to Create

- `src/extract/coordination/extraction_runtime_loop.py` — shared loop
- `tests/unit/extract/coordination/test_extraction_runtime_loop.py`

### Legacy Decommission/Delete Scope

The ~1,000 LOC of orchestration ceremony per `builders_runtime.py` (keeping ~100-150 LOC
adapter shell in each).

---

## S23. Tree-sitter extraction Rust migration

### Goal

Migrate tree-sitter extraction to Rust using the `tree-sitter-python` crate. Emit Arrow
`RecordBatch` directly from Rust, eliminating the Python accumulation layer. Target:
~1,100 Python LOC → ~300 Rust LOC bridge + entrypoint.

### Representative Code Snippets

```rust
// rust/codeanatomy_engine/src/python/tree_sitter_extractor.rs — new module
use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;
use tree_sitter::{Language, Parser};

pub fn extract_tree_sitter_batch(
    source: &str,
    file_path: &str,
) -> Result<RecordBatch, Box<dyn std::error::Error>> {
    let mut parser = Parser::new();
    parser.set_language(&tree_sitter_python::language())?;
    let tree = parser.parse(source, None).ok_or("parse failed")?;
    // Walk tree, collect node types, byte spans, parent relationships
    // Emit as Arrow RecordBatch with schema:
    //   node_type: Utf8, bstart: Int64, bend: Int64, parent_id: Int64, file: Utf8
    let schema = Arc::new(Schema::new(vec![
        Field::new("node_type", DataType::Utf8, false),
        Field::new("bstart", DataType::Int64, false),
        Field::new("bend", DataType::Int64, false),
        Field::new("parent_id", DataType::Int64, true),
        Field::new("file", DataType::Utf8, false),
    ]));
    // ... builder logic ...
    todo!("implement batch emission")
}
```

```python
# src/extract/extractors/tree_sitter/builders.py — thin Python shim after migration
from src._rust_ext import extract_tree_sitter_batch as _extract_rust

def extract_tree_sitter(source: str, file_path: str) -> pa.RecordBatch:
    """Extract tree-sitter nodes via Rust. Returns Arrow RecordBatch."""
    return _extract_rust(source, file_path)
```

### Files to Edit

- `src/extract/extractors/tree_sitter/builders.py` — reduce to thin Rust shim
- `src/extract/extractors/tree_sitter/builders_runtime.py` — reduce to adapter (S22 done
  first)

### New Files to Create

- `rust/codeanatomy_engine/src/python/tree_sitter_extractor.rs` — Rust implementation
- `tests/unit/extract/tree_sitter/test_rust_tree_sitter_extractor.py`

### Legacy Decommission/Delete Scope

Python tree-sitter accumulation layer in `builders.py` (~1,100 LOC), retaining only the
schema definition and thin bridge.

---

## S24. Delta write integration — io/write_delta.py → Rust delta_writer.rs

### Goal

Connect Python `io/write_delta.py` write orchestration to the existing Rust
`executor/delta_writer.rs` via IPC payload. Reduce Python write orchestration by ~400 LOC;
the Rust infrastructure (461 LOC, `delta_writer.rs`) already exists.

### Representative Code Snippets

```rust
// rust/codeanatomy_engine/src/executor/delta_writer.rs — add IPC entry point
// (delta_writer.rs already exists; add a public fn that accepts a write payload struct)
#[derive(Debug, Serialize, Deserialize)]
pub struct DeltaWritePayload {
    pub table_uri: String,
    pub storage_options: HashMap<String, String>,
    pub schema_json: String,
    pub partition_by: Vec<String>,
    pub write_mode: String,  // "append" | "overwrite" | "error_if_exists"
}

pub async fn execute_delta_write(payload: DeltaWritePayload, batches: Vec<RecordBatch>) -> Result<DeltaWriteResult> {
    // existing delta_writer logic, now callable from Python via IPC payload
    ...
}
```

```python
# src/datafusion_engine/io/write_delta.py — after migration
import msgspec
from src._rust_ext import execute_delta_write as _execute_rust_write

class DeltaWritePayload(msgspec.Struct, frozen=True):
    """IPC payload for Rust delta_writer.rs."""
    table_uri: str
    storage_options: dict[str, str]
    schema_json: str
    partition_by: list[str]
    write_mode: str

def write_delta(
    df: DataFrame,
    *,
    table_uri: str,
    storage_options: dict[str, str] | None = None,
    partition_by: list[str] | None = None,
    write_mode: str = "append",
) -> DeltaWriteResult:
    """Write DataFrame to Delta table via Rust executor."""
    payload = DeltaWritePayload(
        table_uri=table_uri,
        storage_options=storage_options or {},
        schema_json=df.schema().to_json(),
        partition_by=partition_by or [],
        write_mode=write_mode,
    )
    payload_bytes = msgspec.msgpack.encode(payload)
    return _execute_rust_write(payload_bytes, df.collect())
```

### Files to Edit

- `rust/codeanatomy_engine/src/executor/delta_writer.rs` — add `DeltaWritePayload` struct
  and public `execute_delta_write` entry point
- `rust/codeanatomy_engine_py/src/` — expose `execute_delta_write` to Python
- `src/datafusion_engine/io/write_delta.py` — replace orchestration with IPC delegation

### New Files to Create

- `tests/unit/datafusion_engine/io/test_delta_write_rust_bridge.py`

### Legacy Decommission/Delete Scope

Python write orchestration logic in `io/write_delta.py` (~400+ LOC of retry/schema
negotiation/error-handling code that lives in Rust now).

---

## S25. Byte-span canonicalization to Rust UDF

### Goal

Replace Python byte-span canonicalization in `src/semantics/span_normalize.py` and
`src/semantics/normalization_helpers.py` (~120 LOC) with a Rust `ScalarUDF` or
`TableProvider` that performs line-index joins natively in the DataFusion execution engine.

### Representative Code Snippets

```rust
// rust/datafusion_ext/src/udf/span_normalize.rs — new ScalarUDF
use datafusion::logical_expr::{ScalarUDF, ScalarUDFImpl, Signature, Volatility};

/// ScalarUDF: canonicalize_byte_span(bstart, bend, file_path) -> (line, col_start, col_end)
pub struct CanonicalizeByteSpanUDF;

impl ScalarUDFImpl for CanonicalizeByteSpanUDF {
    fn name(&self) -> &str { "canonicalize_byte_span" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Struct(Fields::from(vec![
            Field::new("line", DataType::Int64, false),
            Field::new("col_start", DataType::Int64, false),
            Field::new("col_end", DataType::Int64, false),
        ])))
    }
    fn invoke_batch(&self, args: &[ColumnarValue], _batch_size: usize) -> Result<ColumnarValue> {
        // byte offset → (line, col) via pre-built line-index table
        todo!("implement via line-index join")
    }
}
```

```python
# src/semantics/span_normalize.py — thin wrapper after migration
# Register the Rust UDF and expose Python helper:
def register_byte_span_udf(ctx: SessionContext) -> None:
    """Register canonicalize_byte_span Rust UDF with the DataFusion session."""
    from src._rust_ext import canonicalize_byte_span_udf
    ctx.register_udf(canonicalize_byte_span_udf())
```

### Files to Edit

- `src/semantics/span_normalize.py` — replace implementation with UDF registration shim
- `src/semantics/normalization_helpers.py` — remove byte-offset calculation logic

### New Files to Create

- `rust/datafusion_ext/src/udf/span_normalize.rs` — `CanonicalizeByteSpanUDF`
- `tests/unit/semantics/test_byte_span_canonicalization.py`

### Legacy Decommission/Delete Scope

Python byte-offset-to-line-col calculation logic in `span_normalize.py` and
`normalization_helpers.py` (~120 LOC).

---

## S26. kernels.py interval-align to Rust TableProvider

### Goal

Replace the Python interval-align join logic in `kernels.py` (~290 LOC) with a Rust
`TableProvider` using a sort-merge join and DF52 sort pushdown. This eliminates the
multi-level attribute chain access to `DataFusionRuntimeProfile` and makes the join
deterministic by construction.

### Representative Code Snippets

```rust
// rust/codeanatomy_engine/src/providers/interval_align_provider.rs — new module
use datafusion::datasource::TableProvider;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::*;

/// TableProvider that implements interval-alignment join semantics.
/// Replaces Python kernels.py interval_align_kernel (~290 LOC).
pub struct IntervalAlignProvider {
    left_schema: SchemaRef,
    right_schema: SchemaRef,
    target_partitions: usize,
}

#[async_trait]
impl TableProvider for IntervalAlignProvider {
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // sort-merge join implementation with DF52 PushdownSort rule
        todo!("interval-align sort-merge join")
    }
}
```

```python
# src/datafusion_engine/kernels.py — thin shim after migration
from src._rust_ext import IntervalAlignProvider as _RustIntervalAlignProvider

def interval_align_kernel(
    left_df: DataFrame,
    right_df: DataFrame,
    *,
    runtime_profile: DataFusionRuntimeProfile,
) -> DataFrame:
    """Interval-align two DataFrames via Rust TableProvider."""
    target_partitions = runtime_profile.execution.target_partitions
    provider = _RustIntervalAlignProvider(target_partitions=target_partitions)
    # register and query
    ...
```

### Files to Edit

- `src/datafusion_engine/kernels.py` — replace `interval_align_kernel` implementation
  with thin Rust shim (~290 → ~30 LOC)

### New Files to Create

- `rust/codeanatomy_engine/src/providers/interval_align_provider.rs`
- `tests/unit/datafusion_engine/test_kernels_interval_align.py`

### Legacy Decommission/Delete Scope

Python interval-align join logic (~290 LOC from `kernels.py`), leaving only the thin
Rust shim wrapper.

---

## S27. relspec/policy_compiler.py cache-policy to Rust

### Goal

Migrate the ~80 LOC cache-policy graph traversal in `src/relspec/policy_compiler.py` to
Rust via `codeanatomy_engine::compiler::scheduling.rs`. This completes the Rust Pivot for
the scheduling subsystem and eliminates the remaining Python graph traversal surface.

### Representative Code Snippets

```rust
// rust/codeanatomy_engine/src/compiler/scheduling.rs — add cache policy traversal
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct CachePolicyDecision {
    pub view_name: String,
    pub policy: String,  // "none" | "delta_staging" | "memory"
    pub confidence: f64,
    pub rationale: String,
}

pub fn derive_cache_policies(
    graph_json: &str,
) -> datafusion::error::Result<Vec<CachePolicyDecision>> {
    // Graph traversal for cache policy derivation
    todo!("port from Python policy_compiler.py")
}
```

```python
# src/relspec/policy_compiler.py — thin delegator after migration
import msgspec
from src._rust_ext import derive_cache_policies as _rust_derive

class CachePolicyDecision(msgspec.Struct, frozen=True):
    view_name: str
    policy: str
    confidence: float
    rationale: str

def derive_cache_policies_from_graph(graph: TaskGraph) -> list[CachePolicyDecision]:
    """Derive cache policies by delegating to Rust scheduling traversal."""
    graph_json = _serialize_task_graph(graph)
    raw = _rust_derive(graph_json)
    return msgspec.json.decode(raw, type=list[CachePolicyDecision])
```

### Files to Edit

- `rust/codeanatomy_engine/src/compiler/scheduling.rs` — add `derive_cache_policies`
- `rust/codeanatomy_engine_py/src/` — expose to Python
- `src/relspec/policy_compiler.py` — replace traversal with delegation

### New Files to Create

- `tests/unit/relspec/test_cache_policy_rust_bridge.py`

### Legacy Decommission/Delete Scope

Cache-policy graph traversal logic in `policy_compiler.py` (~80 LOC).

---

## S28. Session construction restructuring — ephemeral_context_phases canonical path (QW-34)

### Goal

Route all `session_context()` construction through `_ephemeral_context_phases` as the
sole path, eliminating the 15-step inline pipeline in
`src/datafusion_engine/session/runtime.py:403-437` and the three module-level mutable
globals in `src/datafusion_engine/session/_session_caches.py`. Split
`runtime_extensions.py` into `runtime_udf_install.py` + `runtime_observability.py` for
separation of concerns.

### Representative Code Snippets

```python
# src/datafusion_engine/session/runtime_context.py — make _ephemeral_context_phases canonical
def _ephemeral_context_phases(
    profile: DataFusionRuntimeProfile,
    *,
    phase_overrides: dict[str, Callable[..., None]] | None = None,
) -> SessionContext:
    """Construct a DataFusion session via the canonical 8-phase pipeline.

    This is THE sole construction path for all session types (ephemeral, pooled,
    delta-backed). The 15-step inline path in runtime.py is removed.

    Parameters
    ----------
    profile : DataFusionRuntimeProfile
        Runtime profile providing all session configuration.
    phase_overrides : dict[str, Callable] | None
        Optional per-phase overrides for testing.

    Returns
    -------
    SessionContext
        Fully configured session context.
    """
    phases = _build_phase_pipeline(profile, overrides=phase_overrides)
    ctx = SessionContext(config=_config_from_profile(profile))
    for phase_name, phase_fn in phases:
        phase_fn(ctx, profile)
    return ctx
```

```python
# src/datafusion_engine/session/runtime.py — remove 15-step inline pipeline
# REMOVE: lines 403-437 (inline imperative session construction)
# REPLACE WITH:
def session_context(profile: DataFusionRuntimeProfile) -> SessionContext:
    """Return a configured SessionContext using the canonical phase pipeline."""
    return _ephemeral_context_phases(profile)
```

```python
# src/datafusion_engine/session/_session_caches.py — remove global mutable state
# REMOVE: SESSION_CONTEXT_CACHE, SESSION_RUNTIME_CACHE, RUNTIME_SETTINGS_OVERLAY
# Replace with per-pool instance caches in context_pool.py
```

### Files to Edit

- `src/datafusion_engine/session/runtime.py:403-437` — replace inline pipeline with
  `_ephemeral_context_phases` call
- `src/datafusion_engine/session/runtime_context.py:64-105` — promote to canonical path
  for all context types
- `src/datafusion_engine/session/_session_caches.py:9-11` — remove module-level globals

### New Files to Create

- `src/datafusion_engine/session/runtime_udf_install.py` — UDF installation logic split
  from `runtime_extensions.py`
- `src/datafusion_engine/session/runtime_observability.py` — telemetry/OTel split
- `tests/unit/datafusion_engine/session/test_ephemeral_context_phases.py`

### Legacy Decommission/Delete Scope

- 15-step inline imperative pipeline in `src/datafusion_engine/session/runtime.py:403-437`
- Three module-level mutable globals in
  `src/datafusion_engine/session/_session_caches.py:9-11`

---

## S29. UDF circular import resolution — merge extension_core + extension_snapshot_runtime (QW-36)

### Goal

Merge `udf/extension_core.py` and `udf/extension_snapshot_runtime.py` into
`udf/extension_runtime.py` to break the circular import triangle. Remove ~200 LOC of
deferred-import wrapper functions. Depends on S21 (UDF snapshot contract), which
eliminates the need for much of `extension_snapshot_runtime.py`.

### Representative Code Snippets

```python
# src/datafusion_engine/udf/extension_runtime.py — new merged module
from __future__ import annotations

# Absorbs:
# - extension_core.py public surface (UdfRegistration, UdfLifecycle, etc.)
# - extension_snapshot_runtime.py public surface (now thin wrappers post-S21)
# - extension_validation.py delegations (now msgspec.convert post-S21)

# No deferred imports; the circular triangle is broken by moving public APIs
# directly into extension_runtime.py and deleting legacy entry modules.
```

```python
# src/datafusion_engine/udf/__init__.py — export from extension_runtime directly
from src.datafusion_engine.udf.extension_runtime import UdfLifecycle, UdfRegistration
```

### Files to Edit

- `src/datafusion_engine/udf/__init__.py` — switch exports/import surface to
  `extension_runtime.py`
- call sites importing `extension_core` / `extension_snapshot_runtime` — update to
  `extension_runtime`

### New Files to Create

- `src/datafusion_engine/udf/extension_runtime.py`
- `tests/unit/datafusion_engine/udf/test_extension_runtime_import_order.py`

### Legacy Decommission/Delete Scope

- `src/datafusion_engine/udf/extension_snapshot_runtime.py` — 14 deferred-import wrappers
  (~200 LOC pure ceremony)
- `src/datafusion_engine/udf/extension_core.py` — delete

---

## S30. registration_core.py alias block removal (QW-37)

### Goal

Remove the 31-alias re-export block from
`src/datafusion_engine/dataset/registration_core.py:616-647`. Update all callers to
import from the authoritative modules directly.

### Representative Code Snippets

```python
# src/datafusion_engine/dataset/registration_core.py:616-647 — REMOVE alias block
# REMOVE: from src.datafusion_engine.session.runtime import session_context_for_ops
# REMOVE: from src.datafusion_engine.catalog.provider import register_table_provider
# ... 29 more re-exports ...
```

```bash
# Identify all callers importing from registration_core that use aliased names:
# rg -n "from\\s+datafusion_engine\\.dataset\\.registration_core\\s+import" src tests
```

### Files to Edit

- `src/datafusion_engine/dataset/registration_core.py:616-647` — remove alias block
- All files importing the aliased names — update to import from authoritative modules

### New Files to Create

None.

### Legacy Decommission/Delete Scope

31-alias re-export block in `src/datafusion_engine/dataset/registration_core.py:616-647`.

---

## S31. df_plugin_codeanatomy lib.rs split (QW-38)

### Goal

Split `rust/df_plugin_codeanatomy/src/lib.rs` (694 LOC) into four focused modules:
`options.rs`, `udf_bundle.rs`, `providers.rs`, and a thin `lib.rs` coordination layer.
This makes the plugin testable at the unit level and eliminates the single-file God module.

### Representative Code Snippets

```rust
// rust/df_plugin_codeanatomy/src/options.rs — plugin configuration
pub struct CodeAnatomyPluginOptions {
    pub target_batch_size: usize,
    pub enable_delta_provider: bool,
    pub udf_bundle_path: Option<PathBuf>,
}

// rust/df_plugin_codeanatomy/src/udf_bundle.rs — UDF registration logic
pub fn register_udf_bundle(
    ctx: &SessionContext,
    options: &CodeAnatomyPluginOptions,
) -> datafusion::error::Result<()> {
    // extracted from lib.rs (was inline in the monolith)
    ...
}

// rust/df_plugin_codeanatomy/src/providers.rs — DeltaProvider, CdfProvider registration
pub fn register_delta_providers(
    ctx: &SessionContext,
    options: &CodeAnatomyPluginOptions,
) -> datafusion::error::Result<()> {
    ...
}

// rust/df_plugin_codeanatomy/src/lib.rs — thin coordination layer (~50 LOC)
mod options;
mod udf_bundle;
mod providers;

pub use options::CodeAnatomyPluginOptions;
use udf_bundle::register_udf_bundle;
use providers::register_delta_providers;
```

### Files to Edit

- `rust/df_plugin_codeanatomy/src/lib.rs` — reduce to thin coordination layer

### New Files to Create

- `rust/df_plugin_codeanatomy/src/options.rs`
- `rust/df_plugin_codeanatomy/src/udf_bundle.rs`
- `rust/df_plugin_codeanatomy/src/providers.rs`
- `tests/rust/df_plugin_codeanatomy/` — unit tests for each module

### Legacy Decommission/Delete Scope

The 694-LOC monolithic `lib.rs` (replaced by four focused modules, net LOC unchanged
but each module is independently testable).

---

## S32. SemanticCompiler TableRegistry extraction (QW-39)

### Goal

Extract the mutable `_tables` registry from `SemanticCompiler` into a standalone
`TableRegistry` class. Make `SemanticCompiler` stateless (no mutable instance state).
This separates registration concerns from compilation concerns.

### Representative Code Snippets

```python
# src/semantics/compiler.py — before (lines 223-225)
class SemanticCompiler:
    def __init__(self, config: SemanticConfig) -> None:
        self._config = config
        self._tables: dict[str, TableInfo] = {}  # EXTRACT THIS

# After: SemanticCompiler receives an injected TableRegistry
class SemanticCompiler:
    def __init__(
        self,
        config: SemanticConfig,
        *,
        table_registry: TableRegistry,
    ) -> None:
        self._config = config
        self._registry = table_registry  # injected, not owned
```

```python
# src/semantics/table_registry.py — new module
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class TableRegistry:
    """Mutable registry of semantic tables.

    Extracted from SemanticCompiler to separate registration from compilation.
    """
    _tables: dict[str, TableInfo] = field(default_factory=dict)

    def register(self, name: str, info: TableInfo) -> None:
        """Register a table. Raises if name already registered."""
        if name in self._tables:
            raise ValueError(f"Table already registered: {name!r}")
        self._tables[name] = info

    def get(self, name: str) -> TableInfo | None:
        """Look up a table by name."""
        return self._tables.get(name)

    def all_names(self) -> list[str]:
        """Return sorted list of all registered table names."""
        return sorted(self._tables)
```

### Files to Edit

- `src/semantics/compiler.py:223-225,592-625` — remove `_tables` dict; accept injected
  `TableRegistry`; update `get_or_register` to use registry

### New Files to Create

- `src/semantics/table_registry.py`
- `tests/unit/semantics/test_table_registry.py`

### Legacy Decommission/Delete Scope

`self._tables` mutable dict on `SemanticCompiler`; `get_or_register` mutation path.

---

## S33. Extraction builders.py/builders_runtime.py merge (QW-40)

### Goal

Merge `builders.py` and `builders_runtime.py` for all 4 extractors. After S22 reduces
`builders_runtime.py` to a thin adapter, the remaining adapter logic can be absorbed into
`builders.py` and the runtime file deleted. Expose the public API through each
extractor's `__init__.py`.

### Representative Code Snippets

```python
# src/extract/extractors/ast/__init__.py — expose public API
from src.extract.extractors.ast.builders import (
    AstExtractorContext,
    setup_ast,
    execute_ast,
    run_ast_extraction,  # moved from builders_runtime.py
)

__all__ = ["AstExtractorContext", "setup_ast", "execute_ast", "run_ast_extraction"]
```

### Files to Edit

- `src/extract/extractors/ast/__init__.py` — expose public API
- `src/extract/extractors/bytecode/__init__.py` — expose public API
- `src/extract/extractors/cst/__init__.py` — expose public API
- `src/extract/extractors/tree_sitter/__init__.py` — expose public API
- `src/extract/extractors/ast/builders.py` — absorb `run_ast_extraction`
- `src/extract/extractors/bytecode/builders.py` — absorb runtime function
- `src/extract/extractors/cst/builders.py` — absorb runtime function
- `src/extract/extractors/tree_sitter/builders.py` — absorb runtime function (post-S23)

### New Files to Create

None.

### Legacy Decommission/Delete Scope

All four `builders_runtime.py` files after their logic is absorbed (post-S22 and S23).

---

## S34. DataFusionRuntimeProfile facade query methods

### Goal

Add facade query methods to `DataFusionRuntimeProfile` — `dataset_candidates(destination)`,
`join_repartition_enabled(keys)`, `effective_sql_options()`,
`schema_hardening_view_types()` — to eliminate the two- and three-level attribute chain
violations in `WritePipeline` and `kernels.py`.

### Representative Code Snippets

```python
# src/datafusion_engine/session/runtime_profile_config.py (or wherever RuntimeProfile is)
class DataFusionRuntimeProfile:
    ...

    def dataset_candidates(
        self,
        destination: str,
    ) -> list[DatasetTemplate]:
        """Return dataset templates matching destination.

        Replaces: profile.data_sources.dataset_templates (two-level chain)
        """
        return [
            t for t in self.data_sources.dataset_templates
            if t.destination == destination
        ]

    def join_repartition_enabled(self, keys: list[str]) -> bool:
        """Return whether join repartition is enabled for these keys.

        Replaces: profile.policies.join_policy.repartition_enabled (two-level chain)
        """
        return self.policies.join_policy.repartition_enabled_for(keys)

    def effective_sql_options(self) -> SqlOptions:
        """Return effective SQL options for the current runtime profile."""
        return self.policies.delta_store_policy.sql_options

    def schema_hardening_view_types(self) -> frozenset[str]:
        """Return the set of view types requiring schema hardening."""
        return frozenset(self.policies.schema_hardening_policy.view_types)
```

```python
# src/datafusion_engine/io/write_pipeline.py — update call sites
# Before: profile.data_sources.dataset_templates (line 189)
# After:
candidates = runtime_profile.dataset_candidates(destination)

# Before: self.runtime_profile.policies.delta_store_policy (line 209)
# After:
sql_opts = self.runtime_profile.effective_sql_options()
```

### Files to Edit

- `src/datafusion_engine/session/runtime_profile_config.py` (or relevant profile file) —
  add facade methods
- `src/datafusion_engine/io/write_pipeline.py:189,190,209` — update call sites
- `src/datafusion_engine/kernels.py:189,193` — update call sites

### New Files to Create

- `tests/unit/datafusion_engine/session/test_runtime_profile_facade.py`

### Legacy Decommission/Delete Scope

Two- and three-level attribute chain access patterns in `write_pipeline.py` and
`kernels.py`.

---

## S35. DF52 planner control-plane hardening

### Goal

Make DF52 planning knobs explicit, typed, and deterministic across Python and Rust session
construction. Include dynamic-filter toggles, sort-pushdown toggle, and
`allow_symmetric_joins_without_pruning` in canonical runtime profile surfaces.

### Representative Code Snippets

```python
# src/datafusion_engine/session/runtime_config_policies.py
class DataFusionFeatureGates(StructBaseStrict, frozen=True):
    enable_dynamic_filter_pushdown: bool = True
    enable_join_dynamic_filter_pushdown: bool = True
    enable_aggregate_dynamic_filter_pushdown: bool = True
    enable_topk_dynamic_filter_pushdown: bool = True
    enable_sort_pushdown: bool = True
    allow_symmetric_joins_without_pruning: bool = True

    def settings(self) -> dict[str, str]:
        return {
            "datafusion.optimizer.enable_dynamic_filter_pushdown": str(
                self.enable_dynamic_filter_pushdown
            ).lower(),
            "datafusion.optimizer.enable_join_dynamic_filter_pushdown": str(
                self.enable_join_dynamic_filter_pushdown
            ).lower(),
            "datafusion.optimizer.enable_aggregate_dynamic_filter_pushdown": str(
                self.enable_aggregate_dynamic_filter_pushdown
            ).lower(),
            "datafusion.optimizer.enable_topk_dynamic_filter_pushdown": str(
                self.enable_topk_dynamic_filter_pushdown
            ).lower(),
            "datafusion.optimizer.enable_sort_pushdown": str(self.enable_sort_pushdown).lower(),
            "datafusion.optimizer.allow_symmetric_joins_without_pruning": str(
                self.allow_symmetric_joins_without_pruning
            ).lower(),
        }
```

```rust
// rust/codeanatomy_engine/src/session/runtime_profiles.rs
pub struct RuntimeProfile {
    pub enable_dynamic_filter_pushdown: bool,
    pub enable_topk_dynamic_filter_pushdown: bool,
    pub enable_sort_pushdown: bool,
    pub allow_symmetric_joins_without_pruning: bool,
    // ...
}
```

### Files to Edit

- `src/datafusion_engine/session/runtime_config_policies.py` — add/normalize DF52 planning
  knobs and remove version-conditioned optimizer-key skipping
- `src/datafusion_engine/session/runtime_profile_config.py` — surface planner control-plane
  knobs in profile contracts and fingerprint payloads
- `rust/codeanatomy_engine/src/session/runtime_profiles.rs` — add matching typed knobs
- `rust/codeanatomy_engine/src/session/factory.rs` — apply all planner knobs to DataFusion
  `ConfigOptions`

### New Files to Create

- `tests/unit/datafusion_engine/session/test_df52_planner_feature_gates.py`
- `rust/codeanatomy_engine/tests/runtime_profile_planner_flags.rs`

### Legacy Decommission/Delete Scope

- Version-conditioned skipping of optimizer keys in `runtime_config_policies.py`
- Any untyped dict overlays that set planner knobs outside runtime profile contracts

---

## S36. Pushdown-contract conformance hardening

### Goal

Make `supports_filters_pushdown` semantics (`Unsupported`, `Inexact`, `Exact`) a tested
contract across provider registration and artifact capture paths. This makes DF52 planning
behavior predictable and auditable.

### Representative Code Snippets

```rust
// rust/codeanatomy_engine/src/providers/pushdown_contract.rs
pub fn pushdown_statuses(
    provider: Arc<dyn TableProvider>,
    filters: &[Expr],
) -> Result<Vec<FilterPushdownStatus>, DataFusionError> {
    let refs: Vec<&Expr> = filters.iter().collect();
    let raw = provider.supports_filters_pushdown(&refs)?;
    Ok(raw.into_iter().map(FilterPushdownStatus::from).collect())
}
```

```python
# src/datafusion_engine/dataset/registration_core.py
def _record_pushdown_contract(statuses: Sequence[str], *, dataset: str) -> None:
    # Persist exact/inexact/unsupported status vectors into runtime artifacts.
    ...
```

### Files to Edit

- `rust/codeanatomy_engine/src/providers/pushdown_contract.rs` — enforce complete
  truth-table mapping + serialization
- `rust/codeanatomy_engine/src/providers/registration.rs` — wire pushdown status capture on
  provider registration paths
- `src/datafusion_engine/dataset/registration_core.py` — consume/report pushdown contract
  payloads for diagnostics
- `src/datafusion_engine/views/artifacts.py` — include pushdown contract metadata in view
  artifacts

### New Files to Create

- `rust/codeanatomy_engine/tests/pushdown_contract_conformance.rs`
- `tests/unit/test_datafusion_filter_pushdown_contract.py`

### Legacy Decommission/Delete Scope

- Bool-only provider capability flags that lose Exact/Inexact distinction
- Any placeholder pushdown metadata that does not reflect per-filter status

---

## S37. Optimizer-observer deterministic plan artifacts

### Goal

Capture rule-by-rule optimizer traces via observer hooks and persist them as deterministic
plan artifacts. This becomes the canonical surface for planning regressions and rulepack
audits.

### Representative Code Snippets

```rust
// rust/codeanatomy_engine/src/compiler/optimizer_pipeline.rs
let mut step_index: usize = 0;
let optimized = optimizer.optimize(unoptimized_plan, &context, |plan, rule| {
    traces.push(OptimizerPassTrace {
        step_index,
        rule_name: rule.name().to_string(),
        plan: plan.display_indent().to_string(),
    });
    step_index += 1;
})?;
```

```python
# src/datafusion_engine/extensions/datafusion_ext.py
artifact = build_plan_bundle_artifact_with_warnings(
    ctx,
    payload={**payload, "deterministic_optimizer": True},
    df=df,
)
```

### Files to Edit

- `rust/codeanatomy_engine/src/compiler/optimizer_pipeline.rs` — canonical observer trace
  capture shape (stable ordering and payload)
- `rust/codeanatomy_engine/src/compiler/plan_bundle.rs` — persist observer traces in bundle
  artifacts
- `rust/codeanatomy_engine/src/compliance/capture.rs` — include observer traces in
  compliance payloads
- `src/datafusion_engine/extensions/datafusion_ext.py` — always request deterministic
  optimizer artifacts in plan bundle build path

### New Files to Create

- `rust/codeanatomy_engine/tests/optimizer_observer_artifacts.rs`
- `tests/unit/test_plan_bundle_optimizer_trace_contract.py`

### Legacy Decommission/Delete Scope

- Fallback branches that silently emit empty optimizer traces where observer capture is
  expected
- Ad-hoc/non-canonical optimizer debug string capture paths

---

## S38. SessionStateBuilder planning-surface cutover

### Goal

Route all Rust-side session construction through a single planning-surface builder that
enforces DF52 ordering rules: `with_default_features()` first, then
`with_file_formats(...)`, then `with_table_options(...)`.

### Representative Code Snippets

```rust
// rust/codeanatomy_engine/src/session/planning_surface.rs
pub fn apply_planning_surface(
    mut builder: SessionStateBuilder,
    spec: &PlanningSurfaceSpec,
) -> SessionStateBuilder {
    if spec.with_default_features {
        builder = builder.with_default_features();
    }
    if !spec.file_formats.is_empty() {
        builder = builder.with_file_formats(spec.file_formats.clone());
    }
    if let Some(options) = spec.table_options.clone() {
        builder = builder.with_table_options(options);
    }
    builder
}
```

### Files to Edit

- `rust/codeanatomy_engine/src/session/planning_surface.rs` — make ordering contract
  explicit and mandatory
- `rust/codeanatomy_engine/src/session/factory.rs` — remove direct builder mutations and
  route through `apply_planning_surface(...)`
- `rust/codeanatomy_engine/src/session/format_policy.rs` — align default format/table-option
  policy surfaces with planning spec
- `rust/codeanatomy_engine/src/session/planning_manifest.rs` — include planning-surface
  digest fields in artifact identity

### New Files to Create

- `rust/codeanatomy_engine/tests/planning_surface_ordering.rs`
- `tests/unit/test_session_planning_manifest_hash.py`

### Legacy Decommission/Delete Scope

- Direct `SessionStateBuilder` mutation paths in `session/factory.rs` that bypass
  planning-surface policy

---

## S39. Streaming-first Arrow C stream boundary

### Goal

Make streaming (`__arrow_c_stream__` / `RecordBatchReader.from_stream`) the default boundary
for high-volume read/write paths. Avoid eager `collect()` / `to_arrow_table()` materialization
unless explicitly requested.

### Representative Code Snippets

```python
# src/datafusion_engine/session/streaming.py
def as_record_batch_reader(df: DataFrame) -> pa.RecordBatchReader:
    """Return streaming reader; avoid eager table materialization."""
    return pa.RecordBatchReader.from_stream(df)
```

```python
# src/datafusion_engine/io/write_pipeline.py
reader = as_record_batch_reader(df)
write_deltalake(path, reader, mode="append", schema_mode=schema_mode)
```

### Files to Edit

- `src/datafusion_engine/session/streaming.py` — expose canonical streaming adapter helpers
- `src/datafusion_engine/io/write_pipeline.py` — replace eager collection paths in write
  flow with streaming reader paths
- `src/datafusion_engine/io/delta_write_handler.py` — consume streaming readers by default
- `src/storage/deltalake/delta_write.py` — preserve streaming semantics through mutation
  boundary

### New Files to Create

- `tests/unit/test_write_pipeline_streaming_default.py`
- `tests/integration/test_datafusion_streaming_write_path.py`

### Legacy Decommission/Delete Scope

- Eager `collect()`/`to_arrow_table()` conversions in default write pipeline paths

---

## S40. Runtime architecture hardening (pooling + immutable policy)

### Goal

Consolidate runtime lifecycle around pooled contexts with immutable per-session policy.
Eliminate process-global mutable session caches and ad-hoc post-construction mutations.

### Representative Code Snippets

```python
# src/datafusion_engine/session/context_pool.py
@dataclass(frozen=True)
class ContextPoolPolicy:
    settings: Mapping[str, str]
    planner_flags_fingerprint: str

def create_context(policy: ContextPoolPolicy) -> SessionContext:
    config = SessionConfig()
    for key, value in policy.settings.items():
        config = config.set(key, value)
    return SessionContext(config)
```

### Files to Edit

- `src/datafusion_engine/session/context_pool.py` — enforce immutable policy-at-construction
  model
- `src/datafusion_engine/session/runtime_context.py` — bind context creation to pool policy
- `src/datafusion_engine/session/_session_caches.py` — delete process-global mutable cache
  dicts
- `src/datafusion_engine/session/runtime.py` — remove inline mutable session bootstrapping
  and route through pooled/context factory path

### New Files to Create

- `tests/unit/datafusion_engine/session/test_context_pool_policy_immutability.py`
- `tests/integration/test_session_pool_isolation.py`

### Legacy Decommission/Delete Scope

- `SESSION_CONTEXT_CACHE`, `SESSION_RUNTIME_CACHE`, `RUNTIME_SETTINGS_OVERLAY` in
  `src/datafusion_engine/session/_session_caches.py`
- Any inline session mutation pipeline that bypasses pool policy

---

## S41. DF52 conformance matrix and deployment-grade integration harness

### Goal

Add integration/conformance coverage for deterministic plan artifacts, storage semantics,
and retry/lock correctness across local filesystem, MinIO-style S3, and LocalStack-style AWS
emulation. This is the release gate for the design-phase hard cutover.

### Representative Code Snippets

```python
# tests/integration/conformance/test_plan_bundle_determinism.py
def test_plan_bundle_is_deterministic(engine: EngineFacade) -> None:
    a = engine.capture_plan_bundle("SELECT * FROM dataset")
    b = engine.capture_plan_bundle("SELECT * FROM dataset")
    assert a.logical_plan_hash == b.logical_plan_hash
    assert a.optimizer_traces == b.optimizer_traces
```

```python
# tests/integration/conformance/test_delta_retry_locking.py
def test_delta_conflict_retry_contract(...) -> None:
    # Assert classifier + retry policy produce deterministic outcomes under conflict.
    ...
```

### Files to Edit

- `tests/integration/` harness utilities — add environment-aware fixtures for FS/MinIO/
  LocalStack conformance runs
- `tests/unit/test_build_pipeline_artifact_contract.py` — align with deterministic observer
  traces and planning manifest fields
- `src/datafusion_engine/extensions/datafusion_ext.py` — ensure plan capture endpoints expose
  conformance-required artifact fields
- `rust/codeanatomy_engine/tests/end_to_end.rs` — assert deterministic plan bundle and
  optimizer-trace surfaces in Rust integration path

### New Files to Create

- `tests/integration/conformance/test_plan_bundle_determinism.py`
- `tests/integration/conformance/test_delta_retry_locking.py`
- `tests/integration/conformance/test_pushdown_contract_matrix.py`

### Legacy Decommission/Delete Scope

- Non-deterministic/underspecified integration assertions that do not validate plan artifact
  identity, retry policy behavior, or pushdown contracts

---

## Cross-Scope Legacy Decommission and Deletion Plan

### Batch D1: Pre-migration dead code (after S4, S7, S8)

- `src/datafusion_engine/plan/plan_introspection.py` (220 LOC) — deleted in S4
- Private coercion helpers in `src/cli/commands/build.py:862-902` (50 LOC) — replaced in S7
- `_WriterPort` local definitions in `inventory.py` and `ledger.py` — replaced in S8
- `_apply_setting` from `context_pool.py` (120 LOC) — replaced in S8

### Batch D2: DF51 remnants (after S9, S10, S11, S12, S13)

- All `.call0()` invocations on `__datafusion_*_provider__` method names
- `FFI_TableProvider::new()` three-argument form
- `CoalesceBatchesExec` wrapper in `datafusion_ext` and `codeanatomy_engine`
- `FileSource::with_projection` and `FileSource::with_statistics` call sites
- Any remaining `deltalake = "0.30.x"` pins in `Cargo.toml` files

### Batch D3: Post-DF52 bespoke cache infrastructure (after S14)

- SQL `SELECT * FROM metadata_cache()` try/except path in `metadata_snapshots.py`
- File-statistics Delta tables in `inventory.py`/`ledger.py` (file-metadata use case)
- `snapshot_datafusion_caches` SQL fallback path in `runtime_extensions.py`

### Batch D4: Lineage walker retirement (after S20)

- `src/datafusion_engine/plan/walk.py` — entire file (110 LOC)
- Python plan visitor logic in `lineage/reporting.py` (~570 LOC of visitor machinery)

### Batch D5: UDF snapshot Python validation (after S21)

- `src/datafusion_engine/udf/extension_snapshot_runtime.py` — 14 deferred-import wrappers
- Python dict validation in `extension_validation.py` — replaced by `msgspec.convert`
- Type-string parser in `udf/signature.py` — replaced by Rust DDL pre-computation

### Batch D6: Extraction runtime duplication (after S22, S33)

- All four `builders_runtime.py` files — merged into shared loop + `builders.py`
- Residual runtime signature-probing logic in extraction coordination modules

### Batch D7: Tree-sitter Python accumulation (after S23)

- Python tree-sitter accumulation layer in `extractors/tree_sitter/builders.py` (~1,100 LOC)

### Batch D8: Delta write Python orchestration (after S24)

- Python write orchestration logic in `io/write_delta.py` (~400 LOC of retry/schema
  negotiation/error handling absorbed by Rust)

### Batch D9: Session global state (after S28, S40)

- Three module-level mutable globals in `src/datafusion_engine/session/_session_caches.py:9-11`
- Inline imperative session pipeline in `src/datafusion_engine/session/runtime.py` that
  bypasses pooled policy

### Batch D10: UDF and registration import legacy (after S29, S30)

- `src/datafusion_engine/udf/extension_core.py`
- `src/datafusion_engine/udf/extension_snapshot_runtime.py`
- 31-alias re-export block in `src/datafusion_engine/dataset/registration_core.py:616-647`

### Batch D11: Planner control-plane fallback cleanup (after S35, S38)

- Version-gated optimizer-key skipping in
  `src/datafusion_engine/session/runtime_config_policies.py`
- Builder-order variance paths outside
  `rust/codeanatomy_engine/src/session/planning_surface.rs`

### Batch D12: Pushdown metadata legacy cleanup (after S36)

- Bool-only pushdown capability metadata that discards Exact/Inexact distinction

### Batch D13: Non-deterministic planning diagnostics cleanup (after S37, S41)

- Plan artifact capture paths that omit observer traces or emit unstable ordering

---

## Implementation Sequence

1. **S1** (Delta delete guard) — correctness fix, zero risk, deploy immediately
2. **S2** (Exception narrowing) — correctness fix, zero risk, deploy immediately
3. **S3** (Session idempotency guards) — correctness fix, deploy with S1/S2
4. **S5** (CoalesceBatches pre-removal) — must precede S10 and S11; safe in DF51
5. **S6** (Rust plugin observability) — safe in DF51; deploy with S5
6. **S4** (plan_introspection deletion) — pure DRY fix, no dependencies
7. **S7** (Coercion consolidation) — pure DRY fix, no dependencies
8. **S8** (Remaining quick wins batch, incl. QW-35/QW-43) — independent; split by theme
9. **S9** (datafusion_python FFI gate) — **THE GATE**; all DF52 work blocked on this
10. **S10** (datafusion_ext DF52 upgrade) — requires S9 and S5
11. **S11** (codeanatomy_engine DF52 upgrade) — requires S9 and S10
12. **S12** (Python extension DF52 hard cutover) — requires S9; run parallel with S10/S11
13. **S13** (deltalake alignment) — requires S9, S10, S11
14. **S14** (FileStatisticsCache adoption) — requires S13
15. **S15** (df.cache() adoption) — requires S13
16. **S16** (RelationPlanner port) — requires S13
17. **S17** (DML hooks cutover) — requires S13
18. **S18** (Sort pushdown for interval-align) — requires S13
19. **S19** (Planner extension protocol cutover) — requires S16
20. **S35** (DF52 planner control-plane hardening) — requires S12 and S13
21. **S38** (SessionStateBuilder planning-surface cutover) — requires S10/S11/S13
22. **S36** (Pushdown-contract conformance hardening) — requires S10/S11/S38
23. **S37** (Optimizer-observer deterministic artifacts) — requires S10/S11/S13
24. **S20** (Lineage walker retirement) — requires S13 for stable Rust bridge
25. **S21** (UDF snapshot contract) — requires S13; blocks S29
26. **S22** (Extraction runtime loop) — independent of DF52; parallel with S20/S21
27. **S23** (Tree-sitter Rust migration) — requires S22
28. **S24** (Delta write Rust integration) — requires S13
29. **S25** (Byte-span canonicalization UDF) — requires S13
30. **S26** (Interval-align Rust TableProvider) — requires S13
31. **S27** (Policy compiler Rust) — parallel with S24/S25
32. **S28** (Session construction restructuring) — requires S3
33. **S29** (UDF circular import resolution) — requires S21
34. **S30** (registration_core alias block removal) — independent
35. **S31** (df_plugin_codeanatomy split) — requires S10
36. **S32** (SemanticCompiler TableRegistry extraction) — independent
37. **S33** (builders merge) — requires S22 and S23
38. **S34** (RuntimeProfile facade methods) — independent
39. **S39** (Streaming-first Arrow C stream boundary) — requires S13 and S24
40. **S40** (Runtime architecture hardening) — requires S28 and S35
41. **S41** (Conformance/deployment integration harness) — final gate; requires S35-S40

---

## Implementation Checklist

### Phase 0: Pre-DF52 Safety and Quick Wins

- [x] S1: Add None-predicate guard in `delta_delete` / `delta_delete_where`
- [x] S2: Narrow exception catch in `infer_semantics()` to `KeyError`
- [x] S3: Add `WeakKeyDictionary` idempotency guards to session extension installation
- [x] S4: Delete `plan_introspection.py`; migrate two unique functions to `bundle_environment.py`
- [x] S5: Remove `CoalesceBatches` import and rule from `physical_rules.rs` and
      `rules/physical.rs`
- [x] S6: Replace `eprintln!` with `tracing::error!`; add spans to `analyze` and `optimize`
- [x] S7: Consolidate coercion modules; `value_coercion.py` is canonical and `coercion.py` is removed
- [x] S8: Land remaining quick wins batch (including QW-35 retry extraction and QW-43 SCIP config extraction)

### Phase 1: DF52 Blocking Migration

- [x] S9: Update all `__datafusion_*_provider__` call sites to pass `session`; bump
      Rust crates to `datafusion = "52"`
- [x] S10: Upgrade `datafusion_ext` to DF52; update `FFI_TableProvider::new()` call sites
- [x] S11: Upgrade `codeanatomy_engine` to DF52; remove `apply_post_filter_coalescing`;
      update scan config for `with_projection_indices -> Result<Self>`
- [x] S12: Update Python `_normalize_args` for DF52 `(py, session)` PyCapsule signatures
      with no version branching, and move runtime behavior gates to capability-first
      engine-version detection (independent of wheel/package label)
- [x] S13: Align workspace crates to `deltalake = "0.31.0"`

### Phase 2: DF52 Planning and Optimizer Surfaces

- [x] S14: Replace SQL cache queries with `ctx.statistics_cache()` / `ctx.list_files_cache()`
- [x] S15: Add `CachePolicy.MEMORY` variant backed by `df.cache()`
- [x] S16: Define `RelationPlannerPort`; register RelationPlanner through canonical extension path
- [x] S17: Cut over to DF52 DML hooks; remove parallel `delta_delete_request_payload` flow
- [x] S18: Remove explicit pre-sort in `interval_align_kernel` if S26 not yet done
- [x] S19: Replace ImportError probing with `isinstance(plugin, PlannerExtensionPort)`
- [x] S35: Add typed DF52 planner knobs (dynamic filters, sort pushdown, symmetric join pruning)
- [x] S36: Enforce `supports_filters_pushdown` truth-table conformance + artifact capture
- [x] S37: Persist optimizer observer traces as deterministic plan artifacts
- [x] S38: Route session building through `planning_surface.rs` ordering contract

### Phase 3: Rust Pivot and Streaming Boundaries

- [x] S20: Add `extract_lineage_json` to Rust; retire `plan/walk.py` and visitor in
      `lineage/reporting.py`
- [x] S21: Emit `RustUdfSnapshot` msgpack from Rust; replace Python validation chain
      (validation/capability interfaces now centralized in `extension_runtime.py`;
      `extension_validation.py` deleted)
- [x] S22: Create shared `ExtractionRuntime` loop; reduce all four `builders_runtime.py`
      to adapters
- [x] S23: Migrate tree-sitter extraction to Rust; emit Arrow `RecordBatch` directly
- [x] S24: Connect Python `write_delta.py` to Rust `delta_writer.rs` via IPC payload
- [x] S25: Implement `canonicalize_byte_span` Rust `ScalarUDF`
- [x] S26: Implement `IntervalAlignProvider` as Rust `TableProvider`
- [x] S27: Migrate cache-policy graph traversal to Rust `scheduling.rs`
- [x] S39: Make streaming (`RecordBatchReader.from_stream`) the default read/write boundary

### Phase 4: Design Principle Cleanup

- [x] S28: Route `session_context()` through `_ephemeral_context_phases`; remove global
      mutable caches
- [x] S29: Merge to `extension_runtime.py`; delete legacy UDF entry modules
- [x] S30: Remove 31-alias block from
      `src/datafusion_engine/dataset/registration_core.py:616-647`
- [x] S31: Split `df_plugin_codeanatomy/src/lib.rs` into `options.rs`, `udf_bundle.rs`,
      `providers.rs`, thin `lib.rs`
- [x] S32: Extract `TableRegistry` from `SemanticCompiler`; make compiler stateless
- [x] S33: Merge `builders_runtime.py` into `builders.py` for all 4 extractors
- [x] S34: Add facade query methods to `DataFusionRuntimeProfile`
- [x] S40: Enforce pooled-session immutability and remove process-global runtime caches

### Phase 5: Conformance and Deployment Gate

- [x] S41: Add deterministic plan/pushdown/retry conformance harness across FS/MinIO/LocalStack
      (backend-resolved URI/storage contracts, backend-aware conformance assertions,
      and executable matrix driver script in place)

### Cross-Scope Deletion Batches

- [x] D1: Pre-migration dead code (post S4, S7, S8)
- [x] D2: DF51 remnants (post S9, S10, S11, S12, S13)
      (runtime behavior no longer keyed on package label; remaining wheel-label `51.0.0`
      treated as packaging artifact while capability probe reports core `52.1.0`)
- [x] D3: Bespoke cache infrastructure (post S14)
- [x] D4: Lineage walker (post S20)
- [x] D5: UDF snapshot Python validation (post S21)
      (`src/datafusion_engine/udf/extension_validation.py` deleted; imports rewired to
      `extension_runtime.py`)
- [x] D6: Extraction runtime duplication (post S22, S33)
- [x] D7: Tree-sitter Python accumulation (post S23)
- [x] D8: Delta write Python orchestration (post S24)
- [x] D9: Session global state (post S28, S40)
- [x] D10: UDF and registration import legacy (post S29, S30)
- [x] D11: Planner control-plane fallback cleanup (post S35, S38)
- [x] D12: Pushdown metadata legacy cleanup (post S36)
- [x] D13: Non-deterministic planning diagnostics cleanup (post S37, S41)

### Audit Delta (2026-02-18)

- `S23` closed:
  `rust/codeanatomy_engine/src/python/tree_sitter_extractor.rs` now emits full
  `tree_sitter_files_v1` row payloads, `rust/datafusion_python/src/codeanatomy_ext/rust_pivot.rs`
  parses bridge payloads, and
  `src/extract/extractors/tree_sitter/builders.py` is now a bridge-oriented shim.
- `S24` closed:
  `rust/datafusion_python/src/codeanatomy_ext/delta_mutations.rs` now returns expanded
  `delta_write_ipc_request` outcomes (`final_version`, `mutation_report`,
  feature/constraint/metadata outcomes), and
  `src/datafusion_engine/io/delta_write_handler.py` routes write execution through a single
  Rust bridge path (`execute_delta_write_bridge`).
- `S26` closed:
  `rust/codeanatomy_engine/src/providers/interval_align_provider.rs` now builds execution
  plans in `scan(...)` (projection/filter/limit aware) with explicit
  `supports_filters_pushdown` status, and production
  `src/datafusion_engine/kernels.py` no longer contains
  `interval_align_kernel_datafusion`.
- `S27` closed: scheduling traversal is executed in Rust and exposed via
  `derive_cache_policies` in the production extension surface.
- `D7` closed:
  production `src/extract/extractors/tree_sitter/builders.py` no longer carries Python
  tree walking/query accumulation helpers.
- `D8` closed:
  Python Delta write modules are now spec/payload/recording adapters, with write execution
  orchestrated through Rust bridge responses.
- `S12` closed:
  runtime feature gates moved to capability-first version resolution in
  `src/datafusion_engine/session/runtime_config_policies.py`,
  `src/datafusion_engine/session/runtime_compile.py`, and
  `src/datafusion_engine/session/runtime_telemetry.py`, with explicit telemetry split
  (`datafusion_version` binding label + `datafusion_engine_version` capability version).
- `S21` / `D5` closed:
  production imports now target `src/datafusion_engine/udf/extension_runtime.py`;
  `src/datafusion_engine/udf/extension_validation.py` removed; new runtime capability/snapshot
  tests cover canonical interfaces.
- `S41` closed:
  backend-resolved conformance contract is implemented through
  `tests/harness/profiles.py`, `tests/integration/conformance/conftest.py`, and
  `tests/harness/delta_smoke.py`; conformance tests now consume backend URI/storage context;
  matrix execution is available via `scripts/run_conformance_matrix.sh`.
- `D2` closed:
  DF51-specific Rust/session wording and assertions were updated in planning/envelope/compat
  surfaces, and wheel-manifest producer now emits capability-derived
  `datafusion_core_version` in `build/datafusion_plugin_manifest.json`.

### Reconciliation Delta (2026-02-18, checklist reconciliation)
- `S12`, `S21`, `S41`, `D2`, and `D5` are now reconciled as complete based on merged
  code-path updates, deletion batches, and backend-aware conformance harness upgrades.
