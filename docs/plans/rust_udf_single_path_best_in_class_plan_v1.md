# Rust UDF Single-Path Unification — Best‑in‑Class Implementation Plan (v1)

## Purpose
Unify all function execution around **Rust UDFs only**, remove alternate execution lanes and fallback paths, and make the Rust registry snapshot the **single source of truth** for function metadata, docs, signatures, and named‑argument behavior. This plan aligns with the relspec + CPG unifier architecture by enforcing a facade‑first execution surface, AST‑first planning, contract‑first outputs, and registry‑driven introspection across all pipelines.

## Guiding principles
- **Rust‑only UDF execution**: no Python/PyArrow/Pandas UDF paths or legacy lanes.
- **Registry snapshot is canonical**: all metadata comes from `datafusion_ext.registry_snapshot` and `datafusion_ext.udf_docs_snapshot`.
- **AST‑first planning**: UDF invocation uses ExprIR/SQLGlot builders only (no SQL strings).
- **Single orchestration surface**: UDF registration is centralized in the facade/session wiring; all pipelines inherit it.
- **Contract‑first outputs**: every plan output is bound to DatasetSpec/ContractSpec schemas.
- **Evidence + info_schema parity**: function availability and signatures are validated through registry snapshot + information_schema only.
- **Delta‑native semantics**: Delta providers are first‑class for IO, schema authority, and CDF workflows.
- **SQLGlot canonicalization pipeline**: deterministic plan fingerprints and lineage are mandatory.

---

# Scope 1 — Centralized Rust UDF platform hook (single entry point)

## Goal
Create one authoritative UDF platform initialization path that installs Rust UDFs, UDTFs, SQL macro factory, and Expr planners on every DataFusion session.

## Status
Completed.

## Representative pattern
```python
# datafusion_engine/udf_platform.py (new)
def install_rust_udf_platform(ctx: SessionContext) -> RustUdfPlatform:
    register_rust_udfs(ctx)                 # datafusion_ext.register_udfs
    register_builtin_udtfs(ctx)             # registry/docs/cache UDTFs
    install_expr_planners(ctx, ["codeanatomy_domain"])
    install_sql_macro_factory(ctx)
    snapshot = rust_udf_snapshot(ctx)
    docs = rust_udf_docs(ctx)
    return RustUdfPlatform(snapshot=snapshot, docs=docs)
```

## Target files
- New: `src/datafusion_engine/udf_platform.py`
- Modify: `src/datafusion_engine/execution_facade.py`
- Modify: `src/datafusion_engine/udf_runtime.py`
- Modify: `src/datafusion_engine/runtime.py`

## Deletions
- Remove any ad‑hoc UDF registration calls outside the facade/session factory.

## Implementation checklist
- [x] Create `install_rust_udf_platform(ctx)` as the single entry point.
- [x] Ensure all DataFusion sessions call the platform hook exactly once.
- [x] Surface snapshot + docs through facade diagnostics for evidence validation.

---

# Scope 2 — Rust UDF quality contract (performance + metadata correctness)

## Goal
Enforce best‑in‑class Rust UDF implementation standards: correct metadata, scalar fast paths, return field correctness, and documentation coverage.

## Status
Not started.

## Representative pattern
```rust
#[user_doc(
    doc_section(label = "Other Functions"),
    description = "Extract Arrow metadata.",
    syntax_example = "arrow_metadata(expr [, key])",
    argument(name = "expr", description = "Expression to inspect."),
    argument(name = "key", description = "Optional metadata key.")
)]
struct ArrowMetadataUdf { signature: Signature }

impl ScalarUDFImpl for ArrowMetadataUdf {
    fn name(&self) -> &str { "arrow_metadata" }
    fn signature(&self) -> &Signature { &self.signature }
    fn documentation(&self) -> Option<&Documentation> { self.doc() }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let out_type = if args.arg_fields.len() == 2 { DataType::Utf8 } else { map_type() };
        Ok(Arc::new(Field::new("ignored", out_type, true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if let ColumnarValue::Scalar(value) = &args.args[0] {
            return Ok(ColumnarValue::Scalar(value.clone())); // scalar fast path
        }
        // array path...
        Ok(ColumnarValue::Array(build_array(args.number_rows)?))
    }
}
```

## Target files
- Modify: `rust/datafusion_ext/src/udf_custom.rs`
- Modify: `rust/datafusion_ext/src/udf_builtin.rs`
- Modify: `rust/datafusion_ext/src/udaf_builtin.rs`
- Modify: `rust/datafusion_ext/src/udwf_builtin.rs`
- Modify: `rust/datafusion_ext/src/udf_async.rs`

## Deletions
- Remove legacy doc definitions that duplicate `#[user_doc]`.
- Remove scalar implementations that always expand to arrays without scalar fast paths.

## Implementation checklist
- [ ] Use `return_field_from_args` for value‑dependent types/metadata.
- [ ] Add scalar fast paths for `ColumnarValue::Scalar`.
- [ ] Enforce row‑count invariant for array outputs.
- [ ] Require `#[user_doc]` on all custom Rust UDFs/UDAFs/UDWFs.

---

# Scope 3 — Named‑argument discipline (planner‑level)

## Goal
Ensure named‑argument metadata is accurate and enforced via registry snapshots and planner validation.

## Status
Completed.

## Representative pattern
```rust
let signature = Signature::one_of(variants, Volatility::Immutable)
    .with_parameter_names(vec!["expr".to_string(), "key".to_string()])?;
```

```python
# datafusion_engine/udf_runtime.py
snapshot = rust_udf_snapshot(ctx)
assert "parameter_names" in snapshot
```

## Target files
- Modify: `rust/datafusion_ext/src/udf_custom.rs`
- Modify: `rust/datafusion_ext/src/registry_snapshot.rs`
- Modify: `src/datafusion_engine/udf_runtime.py`
- Modify: `src/datafusion_engine/schema_registry.py`

## Deletions
- Remove Python‑side fallback maps for parameter names.

## Implementation checklist
- [x] Ensure `Signature.parameter_names` is populated for all eligible functions.
- [x] Validate named‑arg parity in schema registry checks.
- [x] Reject variadic signatures with parameter names.

---

# Scope 4 — Async UDF policy (explicit, gated)

## Goal
Make async UDF usage an explicit policy decision with concurrency + timeout guards.

## Status
Completed.

## Representative pattern
```python
# datafusion_engine/udf_platform.py
if policy.allow_async_udfs:
    register_rust_udfs(ctx)  # async UDFs are feature-gated in Rust
else:
    register_rust_udfs(ctx)  # async UDFs remain unregistered by feature gate
```

## Target files
- Modify: `rust/datafusion_ext/src/udf_async.rs`
- Modify: `src/datafusion_engine/runtime.py`
- Modify: `src/datafusion_engine/udf_platform.py`

## Deletions
- Remove implicit async UDF registration paths without policy checks.

## Implementation checklist
- [x] Add policy flags for async UDF enablement.
- [x] Require `ideal_batch_size` and timeout config for async UDFs.
- [x] Disallow async UDFs in production unless explicitly enabled.

---

# Scope 5 — Remove alternate lanes (Rust‑only execution)

## Goal
Collapse execution lanes to a single Rust lane and delete all non‑Rust UDF paths.

## Status
Completed.

## Representative pattern
```python
# engine/function_registry.py
ExecutionLane = Literal["df_rust"]
DEFAULT_LANE_PRECEDENCE = ("df_rust",)
LANE_UDF_TIER = {"df_rust": "builtin"}
```

## Target files
- Modify: `src/engine/function_registry.py`
- Modify: `src/engine/unified_registry.py`
- Modify: `src/engine/udf_registry.py`
- Modify: `src/datafusion_engine/udf_catalog.py`
- Modify: `src/datafusion_engine/schema_registry.py`

## Deletions
- Remove `ibis_pyarrow`, `ibis_pandas`, `ibis_python`, and `df_udf` lanes.
- Remove PyArrow/Pandas/Python UDF tier handling and policy branches.
- Remove Arrow compute kernel snapshot paths when used as alternate lanes.

## Implementation checklist
- [x] Reduce lane definitions to `df_rust` only.
- [x] Remove tier ladders and policies that refer to non‑Rust tiers.
- [x] Simplify registry payload schemas accordingly.

---

# Scope 6 — Snapshot‑driven Ibis specs (no static builtin lists)

## Goal
Generate Ibis builtin specs solely from the Rust registry snapshot (and docs snapshot).

## Status
Completed (snapshot‑driven registry + dynamic Ibis callables).

## Representative pattern
```python
# ibis_engine/builtin_udfs.py
def ibis_udf_specs(*, registry_snapshot: Mapping[str, object]) -> tuple[IbisUdfSpec, ...]:
    snapshot = normalize_registry_snapshot(registry_snapshot)
    return tuple(spec_from_snapshot(snapshot, name) for name in snapshot["scalar"])
```

## Target files
- Modify: `src/ibis_engine/builtin_udfs.py`
- Modify: `src/ibis_engine/backend.py`
- Modify: `src/engine/unified_registry.py`

## Deletions
- Remove static `@ibis.udf.scalar.builtin` definitions (stable_hash64, stable_id, etc.).
- Remove any hard‑coded Ibis builtin name lists.

## Implementation checklist
- [x] Create a snapshot‑to‑Ibis spec builder.
- [x] Use registry snapshot for named args, volatility, rewrite tags.
- [x] Ensure Ibis registrations are fully derived from the snapshot.

---

# Scope 7 — Rust‑first hash/ID utilities (ExprIR only)

## Goal
Eliminate Python/Ibis ad‑hoc hash helpers and standardize on Rust UDF calls via ExprIR/SQLGlot builders.

## Status
Completed (hash/ID helpers now call Rust UDFs via ExprIR or registry).

## Representative pattern
```python
# ibis_engine/hashing.py
stable_id_expr = _call_expr("stable_id", _literal_expr(prefix), joined_parts)
```

## Target files
- Modify: `src/ibis_engine/hashing.py`
- Modify: `src/ibis_engine/hash_exprs.py`
- Modify: `src/ibis_engine/ids.py`
- Modify: `src/cpg/*`
- Modify: `src/relspec/*`
- Modify: `src/normalize/*`

## Deletions
- Remove Python implementations of stable ID/key hashing.
- Remove any non‑Rust hashing fallbacks.

## Implementation checklist
- [x] Ensure all ID/key generation flows through ExprIR builders.
- [x] Remove all remaining non‑UDF hash helpers from call sites.

---

# Scope 8 — Registry parity enforcement (evidence + info_schema)

## Goal
Treat Rust registry snapshot as authoritative; validate it against information_schema in evidence/diagnostics.

## Status
Completed.

## Representative pattern
```python
# datafusion_engine/schema_registry.py
snapshot = rust_udf_snapshot(ctx)
ensure_info_schema_parity(snapshot, information_schema)
```

## Target files
- Modify: `src/datafusion_engine/schema_registry.py`
- Modify: `src/datafusion_engine/udf_catalog.py`
- Modify: `src/datafusion_engine/introspection.py`
- Modify: `src/relspec/evidence.py`

## Deletions
- Remove fallback paths that read UDF metadata from non‑registry sources.

## Implementation checklist
- [x] Require snapshot + info_schema parity before execution (runtime validation hook).
- [x] Export parity diagnostics through facade recorder.
- [x] Enforce info_schema parity in schema registry validation and evidence workflows.

---

# Scope 9 — AST‑only UDF registration (FunctionFactory‑only)

## Goal
Use FunctionFactory + AST builders for CREATE FUNCTION; remove SQL‑string registration entirely.

## Status
Completed.

## Representative pattern
```python
# datafusion_engine/function_factory.py
factory.register_function(spec)  # scalar/agg/window/table
```

## Target files
- Modify: `src/datafusion_engine/function_factory.py`
- Modify: `src/datafusion_engine/bridge.py`
- Modify: `rust/datafusion_ext/src/function_factory.rs`

## Deletions
- Remove raw SQL UDF creation paths in Python or Rust.

## Implementation checklist
- [x] Ensure all UDF registration uses AST builders.
- [x] Ban SQL‑string UDF creation in production code paths.

---

# Scope 10 — Schema function adoption (DataFusion built‑ins first)

## Goal
Prefer DataFusion’s built‑in schema functions over bespoke UDFs where possible (arrow_typeof, arrow_cast, named_struct, map_*, union_*, unnest).

## Status
Not started.

## Representative pattern
```python
# arrowdsl/expr builders
expr = call("arrow_cast", col("payload"), literal("struct<key:string,value:string>"))
```

## Target files
- Modify: `src/arrowdsl/*`
- Modify: `src/datafusion_engine/expr_functions.py`
- Modify: `src/datafusion_engine/registry_bridge.py`
- Modify: `src/cpg/*`

## Deletions
- Remove redundant custom UDFs that duplicate built‑ins (map_* / union_* / struct helpers).

## Implementation checklist
- [ ] Replace bespoke schema utilities with built‑in DataFusion functions.
- [ ] Add ExprIR builders for these functions (no SQL strings).

---

# Scope 11 — Catalog + information_schema policy

## Goal
Standardize catalog/schema configuration and require information_schema as canonical introspection surface.

## Status
Partially complete (information_schema parity enforced; catalog/schema defaults still pending).

## Representative pattern
```python
cfg = SessionConfig() \
    .with_default_catalog_and_schema("datafusion", "public") \
    .with_information_schema(True)
ctx = SessionContext(cfg)
```

## Target files
- Modify: `src/datafusion_engine/runtime.py`
- Modify: `src/datafusion_engine/execution_facade.py`
- Modify: `src/datafusion_engine/schema_registry.py`

## Deletions
- Remove custom catalog/schema inference paths outside DataFusion config.

## Implementation checklist
- [ ] Enforce information_schema in all production sessions.
- [ ] Standardize catalog/schema defaults in the runtime profile.

---

# Scope 12 — Delta‑native integration (providers + CDF)

## Goal
Treat Delta as first‑class: register DeltaTableProvider and DeltaCdfTableProvider as canonical IO surfaces, and use Delta schema as contract authority.

## Status
Not started.

## Representative pattern
```rust
let provider = DeltaTableProvider::try_new(snapshot, log_store, DeltaScanConfig::default())?;
ctx.register_table("delta_table", Arc::new(provider))?;
```

## Target files
- Modify: `rust/datafusion_ext/src/lib.rs`
- Modify: `src/datafusion_engine/registry_bridge.py`
- Modify: `src/datafusion_engine/io_adapter.py`
- Modify: `src/incremental/*`

## Deletions
- Remove Arrow‑dataset fallback paths for Delta when provider is available.

## Implementation checklist
- [ ] Register Delta providers through the facade/runtime profile.
- [ ] Expose CDF tables via DeltaCdfTableProvider where applicable.
- [ ] Enforce Delta schema/version policies in schema registry.

---

# Scope 13 — Ibis integration hardening (SessionContext‑owned)

## Goal
Ensure Ibis always uses the shared SessionContext and uses Arrow batch export for deterministic outputs.

## Status
Not started.

## Representative pattern
```python
ctx = build_session_context(runtime_profile)
con = ibis.datafusion.connect(ctx)
reader = expr.to_pyarrow_batches(chunk_size=250_000)
```

## Target files
- Modify: `src/ibis_engine/backend.py`
- Modify: `src/ibis_engine/execution.py`
- Modify: `src/normalize/*`
- Modify: `src/extract/*`

## Deletions
- Remove named memtable patterns and raw SQL execution paths.

## Implementation checklist
- [ ] Always pass SessionContext into Ibis backend.
- [ ] Prefer `to_pyarrow_batches` + dataset writer for partitioned outputs.
- [ ] Ban `raw_sql` unless SQLGlot AST is used and cursor is safely closed.

---

# Scope 14 — SQLGlot policy engine expansion

## Goal
Make SQLGlot canonicalization a mandatory pipeline: qualify, normalize, annotate types, canonicalize, and normalize predicates with deterministic artifacts.

## Status
Not started.

## Representative pattern
```python
expr = parse_one(sql, dialect=read_dialect)
expr = qualify(expr, schema=schema, dialect=read_dialect, expand_stars=True)
expr = normalize_identifiers(expr, dialect=read_dialect)
expr = annotate_types(expr, schema=schema)
expr = canonicalize(expr)
expr = normalize(expr, max_distance=policy.max_distance)
expr = pushdown_projections(expr, schema=schema)
```

## Target files
- Modify: `src/sqlglot_tools/*`
- Modify: `src/ibis_engine/compiler_checkpoint.py`
- Modify: `src/relspec/plan_catalog.py`

## Deletions
- Remove ad‑hoc SQL normalization passes outside this pipeline.

## Implementation checklist
- [ ] Persist canonical SQLGlot AST as the plan fingerprint input.
- [ ] Record dialect/normalization policy in artifacts.

---

# Scope 15 — Unified session wiring across relspec/normalize/cpg/extract

## Goal
Ensure all execution paths use the centralized Rust UDF platform hook and the facade execution surface.

## Status
Not started.

## Representative pattern
```python
facade = DataFusionExecutionFacade.from_execution_context(ctx, backend)
facade.install_rust_udf_platform()
result = facade.execute(plan)
```

## Target files
- Modify: `src/relspec/*`
- Modify: `src/normalize/*`
- Modify: `src/extract/*`
- Modify: `src/cpg/*`
- Modify: `src/datafusion_engine/execution_facade.py`

## Deletions
- Remove any direct DataFusion SessionContext setup that bypasses the facade.

## Implementation checklist
- [ ] Make facade initialization mandatory for all plan execution.
- [ ] Ensure UDF platform hook is always installed before execution.

---

# Scope 16 — Registry‑driven rewrite tags and domain planners

## Goal
Encode DataFusion‑only rewrite tags in the Rust registry snapshot and wire planner/rewrite behavior exclusively from Rust metadata.

## Status
In progress (rewrite tags already in snapshot; planner wiring still pending).

## Representative pattern
```rust
// registry_snapshot.rs
snapshot.rewrite_tags.insert(name.to_string(), vec!["df_only".to_string()]);
```

## Target files
- Modify: `rust/datafusion_ext/src/registry_snapshot.rs`
- Modify: `rust/datafusion_ext/src/function_rewrite.rs`
- Modify: `src/datafusion_engine/udf_catalog.py`
- Modify: `src/engine/function_registry.py`

## Deletions
- Remove Python‑side rewrite tag maps.

## Implementation checklist
- [ ] Store rewrite tags in snapshot.
- [ ] Use rewrite tags to control planner behavior and Ibis spec metadata.

---

# Scope 17 — Plugin‑only extension path (no Python UDFs)

## Goal
Use the ABI‑stable Rust plugin path for any extension UDFs/UDTFs and disallow Python UDFs entirely.

## Status
Not started.

## Representative pattern
```python
handle = datafusion_ext.load_df_plugin(path)
datafusion_ext.register_df_plugin(ctx, handle, table_names, options)
```

## Target files
- Modify: `src/datafusion_engine/bridge.py`
- Modify: `src/datafusion_engine/runtime.py`
- Modify: `rust/df_plugin_api/*`
- Modify: `rust/df_plugin_host/*`

## Deletions
- Remove any Python UDF registration hooks or adapters.

## Implementation checklist
- [ ] Enforce plugin registration only.
- [ ] Ensure ABI compatibility guardrails are enabled by default.

---

# Deferred deletions (after all scopes complete)

These must remain until all migrations and parity checks are validated:
- Any remaining non‑ExprIR hash code (if reintroduced).
- Any remaining non‑facade SessionContext initialization in `src/relspec/*`, `src/normalize/*`, `src/extract/*`, `src/cpg/*`.
- Any fallback UDF metadata sources outside the Rust registry snapshot.
- Any SQL‑string UDF creation paths.
- Any bespoke schema utilities duplicating DataFusion built‑ins (map/union/struct helpers).
- Any PyArrow/Pandas/Python UDF scaffolding, registries, or tests.

---

# Acceptance gates
- `uv run ruff check --fix`
- `uv run pyrefly check`
- `uv run pyright --warnings --pythonversion=3.13`
