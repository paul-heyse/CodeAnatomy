# DataFusion Rust UDF + Delta Additional Best-in-Class Capabilities Plan (v1)

> Goal: Extend the current best‑in‑class DataFusion/Delta architecture with the additional capabilities identified in the latest review of `docs/python_library_reference/datafusion_rust_UDFs.md`, `docs/python_library_reference/deltalake.md`, and `docs/python_library_reference/deltalake_datafusion_integration.md`.
>
> Design stance: We are explicitly in design phase. Breaking changes are acceptable to reach the cleanest long‑term architecture.
>
> End‑state principles:
> - Planner‑level correctness is prioritized over runtime patching.
> - All UDF/UDAF/UDWF/UDTFs participate in optimizer reasoning (ordering, bounds, constraints, short‑circuiting).
> - Delta session configuration is canonical, not hand‑assembled per call site.
> - Delta protocol features are surfaced intentionally via explicit control‑plane APIs.
> - Every write path is protocol‑correct and mutation‑safe (constraints, generated columns, deletion vectors, row tracking).
>
---

## Scope 1 — Short‑circuit semantics + argument laziness for scalar UDFs

**Intent**: Mark lazy semantics explicitly so the planner does not reorder or evaluate non‑eager arguments incorrectly.

**Status**: Complete. Short-circuit semantics and conformance coverage are in place.

### Representative patterns

```rust
// rust/datafusion_ext/src/udf_custom.rs
impl ScalarUDFImpl for CoalesceLikeUdf {
    fn short_circuits(&self) -> bool {
        true
    }

    fn conditional_arguments(&self, args: &[Expr]) -> Result<ConditionalEvaluation>
    {
        // Example: evaluate args left‑to‑right, stop on first non‑null.
        Ok(ConditionalEvaluation::new(args.len()).stop_after_first_truthy())
    }
}
```

### Target files
- `rust/datafusion_ext/src/udf_custom.rs`
- `rust/datafusion_ext/src/udf_builtin.rs`
- `rust/datafusion_ext/src/function_factory.rs`
- `rust/datafusion_ext/tests/udf_conformance.rs`

### Deletions
- None.

### Implementation checklist
- [x] Identify all UDFs that may not evaluate every argument.
- [x] Implement `short_circuits()` and `conditional_arguments()` as required.
- [x] Add conformance tests covering lazy arguments and side‑effect safety.

---

## Scope 2 — Ordering/bounds/constraint propagation for monotonic UDFs

**Intent**: Preserve planner‑level ordering and constraint reasoning through UDFs that are monotonic or bounds‑preserving.

**Status**: Complete. Identity UDFs now propagate bounds/ordering with conformance coverage.

### Representative patterns

```rust
// rust/datafusion_ext/src/udf_custom.rs
impl ScalarUDFImpl for ClampUdf {
    fn evaluate_bounds(&self, args: &[Expr], info: &dyn SimplifyInfo) -> Result<Interval> {
        // Preserve bounds under clamp semantics.
        derive_clamp_interval(args, info)
    }

    fn output_ordering(&self, input: &[Expr]) -> Result<SortProperties> {
        Ok(SortProperties::Ordered)
    }

    fn preserves_lex_ordering(&self) -> bool {
        true
    }
}
```

### Target files
- `rust/datafusion_ext/src/udf_custom.rs`
- `rust/datafusion_ext/src/udf_builtin.rs`
- `rust/datafusion_ext/src/registry_snapshot.rs`
- `rust/datafusion_ext/tests/udf_conformance.rs`

### Deletions
- None.

### Implementation checklist
- [x] Classify monotonic/bounds‑preserving functions.
- [x] Implement ordering + bounds propagation for those functions.
- [x] Add plan snapshot tests proving ordering propagation is stable.

---

## Scope 3 — Config‑specialized UDF instances (`with_updated_config`)

**Intent**: Allow UDF behavior to vary by session configuration without global state or runtime branching.

**Status**: Complete. Config-specialized registration uses session config defaults and conformance coverage validates specialization.

### Representative patterns

```rust
// rust/datafusion_ext/src/udf_custom.rs
impl ScalarUDFImpl for LocaleAwareUdf {
    fn with_updated_config(&self, config: &ConfigOptions) -> Option<ScalarUDF> {
        let locale = config.execution.locale.clone()?;
        Some(ScalarUDF::new_from_shared_impl(Arc::new(Self::new(locale))))
    }
}
```

### Target files
- `rust/datafusion_ext/src/udf_custom.rs`
- `rust/datafusion_ext/src/udf_registry.rs`
- `rust/datafusion_ext/src/registry_snapshot.rs`
- `src/datafusion_engine/runtime.py`

### Deletions
- None.

### Implementation checklist
- [x] Identify UDFs whose semantics depend on `ConfigOptions`.
- [x] Implement `with_updated_config()` for those UDFs.
- [x] Verify registry snapshot includes config‑specialized UDFs.

---

## Scope 4 — Async UDF batch shaping (`ideal_batch_size`)

**Intent**: Control async evaluation chunking to avoid oversized payloads for remote calls.

**Status**: Complete. Async batch sizing and remainder handling are covered by conformance tests.

### Representative patterns

```rust
// rust/datafusion_ext/src/udf_async.rs
#[async_trait]
impl AsyncScalarUDFImpl for RemoteLookupUdf {
    fn ideal_batch_size(&self) -> Option<usize> {
        Some(256)
    }
}
```

### Target files
- `rust/datafusion_ext/src/udf_async.rs`
- `rust/datafusion_ext/src/udf_registry.rs`
- `src/datafusion_engine/udf_runtime.py`

### Deletions
- None.

### Implementation checklist
- [x] Define per‑UDF batch size policy.
- [x] Surface a configuration mapping for async batch size defaults.
- [x] Add conformance tests for remainder chunk handling.

---

## Scope 5 — UDTF argument validation + coercion discipline

**Intent**: Enforce literal‑only UDTF arguments and perform explicit type coercion to avoid runtime panics.

**Status**: Complete. Literal-only validation and conformance coverage are in place.

### Representative patterns

```rust
// rust/datafusion_ext/src/udtf_external.rs
impl TableFunctionImpl for ReadCsvTableFunction {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let literals = parse_literal_args(args)?; // reject non‑literal expressions
        let path = coerce_string(&literals[0])?;
        build_csv_provider(path, &literals[1..])
    }
}
```

### Target files
- `rust/datafusion_ext/src/udtf_external.rs`
- `rust/datafusion_ext/src/udtf_builtin.rs`
- `rust/datafusion_ext/tests/udf_conformance.rs`

### Deletions
- None.

### Implementation checklist
- [x] Add literal‑only parsing helpers for UDTFs.
- [x] Implement coercion for common literal shapes (string, integer, boolean).
- [x] Add tests for invalid (non‑literal) UDTF arguments.

---

## Scope 6 — Return‑type parity for `return_field_from_args`

**Intent**: Maintain `information_schema`/function listings by implementing `return_type` alongside field‑based typing.

**Status**: Complete. All custom scalar UDFs, async UDFs, and SQL macros now implement `return_type` to match `return_field_from_args`.

### Representative patterns

```rust
// rust/datafusion_ext/src/udf_custom.rs
impl ScalarUDFImpl for DynamicTypeUdf {
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        build_dynamic_field(args)
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        // Must match the type returned by return_field_from_args.
        Ok(dynamic_type_from_args(arg_types))
    }
}
```

### Target files
- `rust/datafusion_ext/src/udf_custom.rs`
- `rust/datafusion_ext/src/function_factory.rs`
- `rust/datafusion_ext/src/registry_snapshot.rs`

### Deletions
- None.

### Implementation checklist
- [x] Audit all UDFs that rely on `return_field_from_args` only.
- [x] Implement matching `return_type` for those UDFs.
- [x] Update documentation snapshot generation to reflect parity.

---

## Scope 7 — Delta session wrappers as canonical runtime configuration

**Intent**: Use delta‑rs session wrappers (`DeltaSessionConfig`, `DeltaParserOptions`, `DeltaSessionContext`) as the default session builder for all Delta‑aware runtimes.

**Status**: Complete. Delta sessions are built via the Rust Delta session context and ad-hoc parser overrides are removed.

### Representative patterns

```rust
// rust/datafusion_ext/src/delta_control_plane.rs
let session = DeltaSessionContext::new_with_config(DeltaSessionConfig::default());
let ctx: SessionContext = session.into();
```

### Target files
- `rust/datafusion_ext/src/delta_control_plane.rs`
- `rust/datafusion_ext/src/lib.rs`
- `src/datafusion_engine/runtime.py`
- `src/datafusion_engine/registry_bridge.py`

### Deletions
- Remove ad‑hoc per‑call SQL parser configuration in Delta‑specific paths.

### Implementation checklist
- [x] Implement a single Delta session builder in Rust.
- [x] Route all Delta registrations through this builder.
- [x] Deprecate and delete any Delta‑specific parser overrides elsewhere.

---

## Scope 8 — Embedded DataFusion `QueryBuilder` fallback path

**Intent**: Support SQL‑over‑Delta without `datafusion-python` by exposing delta‑rs’ embedded QueryBuilder.

**Status**: Complete. QueryBuilder fallback path and test coverage are wired.

### Representative patterns

```python
# src/storage/deltalake/query_builder.py
builder = QueryBuilder().register("table", delta_table)
result = builder.execute("select * from table where ...")
```

### Target files
- `src/storage/deltalake/query_builder.py` (new)
- `src/storage/deltalake/delta.py`
- `src/datafusion_engine/runtime.py`

### Deletions
- None.

### Implementation checklist
- [x] Add a QueryBuilder wrapper with explicit storage option wiring.
- [x] Provide a controlled fallback path when native DataFusion is unavailable.
- [x] Add tests for QueryBuilder registration + SQL execution.

---

## Scope 9 — Delta protocol feature control‑plane surface

**Intent**: Provide explicit APIs to enable/manage protocol‑level features:
column mapping, generated columns, CHECK constraints/invariants, deletion vectors, row tracking, and in‑commit timestamps.

**Status**: Complete. Storage wrappers and observability payloads capture feature state and protocol metadata.

### Representative patterns

```rust
// rust/datafusion_ext/src/delta_control_plane.rs
pub async fn enable_column_mapping(table: &mut DeltaTable) -> Result<(), DeltaTableError> {
    table.update_table_property("delta.columnMapping.mode", "name").await?;
    table.update_table_property("delta.minReaderVersion", "2").await?;
    table.update_table_property("delta.minWriterVersion", "5").await?;
    Ok(())
}
```

### Target files
- `rust/datafusion_ext/src/delta_control_plane.rs`
- `rust/datafusion_ext/src/delta_mutations.rs`
- `rust/datafusion_ext/src/delta_maintenance.rs`
- `src/storage/deltalake/delta.py`
- `src/datafusion_engine/delta_control_plane.py`

### Deletions
- None.

### Implementation checklist
- [x] Add explicit enable/disable APIs for each feature.
- [x] Validate protocol gates before mutating table properties.
- [x] Add observability payloads describing feature state and protocol versions.

---

## Scope 10 — Maintenance + log hygiene (v2 checkpoints, checkpoint protection, vacuum protocol check)

**Intent**: Make log and checkpoint lifecycle first‑class, including v2 checkpoint creation and vacuum safety checks.

**Status**: Complete. Maintenance observability captures log retention and checkpoint metadata.

### Representative patterns

```rust
// rust/datafusion_ext/src/delta_maintenance.rs
pub async fn create_v2_checkpoint(table: DeltaTable) -> Result<DeltaTable, DeltaTableError> {
    table.create_checkpoint().await
}
```

### Target files
- `rust/datafusion_ext/src/delta_maintenance.rs`
- `rust/datafusion_ext/src/delta_observability.rs`
- `src/storage/deltalake/delta.py`
- `src/datafusion_engine/plan_artifact_store.py`

### Deletions
- None.

### Implementation checklist
- [x] Add explicit APIs for v2 checkpoint creation + checkpoint protection.
- [x] Enforce `vacuumProtocolCheck` before vacuum actions.
- [x] Record log retention + checkpoint state in observability artifacts.

---

## Scope 11 — Delta Sharing provider integration (removed)

**Intent**: Dropped. Delta Sharing support is considered low‑value for now and is removed from the active scope.

---

## Scope 12 — Deferred deletions (only after scopes 1–11 are complete)

**Intent**: Remove transitional paths after all new capabilities are stabilized.

**Status**: Not started. Deferred until scopes 1–10 are fully complete and tested.

### Deferred deletion candidates
- Legacy Delta session configuration overrides in `src/datafusion_engine/runtime.py` once `DeltaSessionConfig` is canonical.
- Any duplicated UDTF argument parsing helpers superseded by new literal‑only validation utilities.
- Any compatibility wrappers around `return_type` or `return_field_from_args` that are no longer needed after parity enforcement.

### Target files
- `src/datafusion_engine/runtime.py`
- `rust/datafusion_ext/src/udtf_external.rs`
- `rust/datafusion_ext/src/udf_custom.rs`

### Deletions
- Deferred until full adoption and test coverage are complete.

### Implementation checklist
- [ ] Run full conformance + integration suite with new surfaces enabled.
- [ ] Remove deprecated helpers only after downstream consumers are migrated.
- [ ] Update documentation and registry snapshots to remove legacy references.
