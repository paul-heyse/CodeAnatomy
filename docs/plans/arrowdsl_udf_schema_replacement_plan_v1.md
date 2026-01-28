---
name: arrowdsl-udf-schema-replacement-plan-v1
overview: "Replace remaining ArrowDSL functionality with Rust UDFs and DataFusion-first schema
  validation/encoding, while completing the ArrowDSL removal plan."
isProject: false
---

# ArrowDSL UDF + Schema Replacement Plan (v1)

## References

- `/home/paul/.cursor/plans/arrowdsl-final-design_f194b575.plan.md`
- `rust/datafusion_ext/src/*` (Rust UDF sources + registration)
- `src/datafusion_engine/udf_runtime.py` (Python registration + validation)
- `src/datafusion_engine/kernels.py` (kernel usage + required UDFs)

## Scope Item 1: Inventory + mapping of existing Rust UDFs

**Goal:** Use already-implemented Rust UDFs/UDWFs/UDAFs/UDTFs to replace ArrowDSL helpers and
document the mapping so call sites converge on the UDF-backed equivalents.

**Existing UDF inventory (Rust registry)**

Scalars (from `udf_registry.rs` + `udf_custom.rs`):
- Hashing/IDs: `stable_hash64` (`hash64`), `stable_hash128` (`hash128`), `prefixed_hash64`
  (`prefixed_hash`), `stable_hash_any` (`stable_hash`), `stable_id`, `stable_id_parts`
  (`stable_id_multi`), `prefixed_hash_parts64` (`prefixed_hash_parts`)
- Spans: `span_make` (`span`), `span_len`, `span_overlaps`, `span_contains`, `span_id`
- Text: `utf8_normalize` (`normalize_utf8`), `utf8_null_if_blank` (`null_if_blank`),
  `qname_normalize` (`qualname_normalize`)
- Collections: `map_get_default`, `map_normalize`, `list_compact` (`array_compact`),
  `list_unique_sorted` (`array_unique_sorted`), `struct_pick` (`struct_select`)
- Delta CDF: `cdf_change_rank`, `cdf_is_upsert`, `cdf_is_delete`
- Misc: `arrow_metadata`, `col_to_byte`

Aggregates (from `udaf_builtin.rs`):
- `list_unique`, `collect_set`, `count_distinct_agg`, `count_if`, `any_value_det`,
  `arg_max`, `arg_min`, `first_value_agg`, `last_value_agg`, `string_agg`

Window functions (from `udwf_builtin.rs`):
- `row_number_window`, `lag_window`, `lead_window`

Table functions (from `udtf_builtin.rs` + `udtf_external.rs` + `udf_registry.rs`):
- `udf_registry`, `udf_docs`, `read_parquet`, `read_csv`, `range_table`

**ArrowDSL → UDF mapping (call-site convergence)**

| ArrowDSL helper | Replacement |
| --- | --- |
| `arrowdsl.core.ids.hash64_from_text` | `datafusion_engine.hash_utils.hash64_from_text` (Python) / `stable_hash64` (UDF) |
| `arrowdsl.core.ids.hash128_from_text` | `datafusion_engine.hash_utils.hash128_from_text` (Python) / `stable_hash128` (UDF) |
| `arrowdsl.core.ids.stable_int64` | `stable_hash64` (UDF) |
| `arrowdsl.core.ids.stable_id` | `stable_id` (UDF) / `datafusion_engine.id_utils.stable_id` (Python) |
| `arrowdsl.core.ids.stable_id_parts` | `stable_id_parts` (UDF) |
| `arrowdsl.core.ids.prefixed_hash_parts64` | `prefixed_hash_parts64` (UDF) |
| `arrowdsl.core.ids.span_id` | `span_id` (UDF) / `datafusion_engine.id_utils.span_id` (Python) |

**Key pattern snippet (Python validation + snapshot)**

```python
from datafusion_engine.udf_runtime import register_rust_udfs, validate_required_udfs

snapshot = register_rust_udfs(session_runtime.ctx)
validate_required_udfs(
    snapshot,
    required=[
        "span_id",
        "utf8_normalize",
        "stable_id",
        "list_unique_sorted",
    ],
)
```

**Target files**
- `rust/datafusion_ext/src/udf_registry.rs`
- `rust/datafusion_ext/src/udf_custom.rs`
- `rust/datafusion_ext/src/udaf_builtin.rs`
- `rust/datafusion_ext/src/udwf_builtin.rs`
- `rust/datafusion_ext/src/udtf_builtin.rs`
- `rust/datafusion_ext/src/udtf_external.rs`
- `src/datafusion_engine/udf_runtime.py`
- `src/datafusion_engine/udf_catalog.py`

**Implementation checklist**
- [x] Build a concrete mapping table from ArrowDSL helper names to the Rust UDF names above.
- [x] Update Python-side required UDF lists (validation and tests) to use the registry names.
- [x] Remove any remaining pure-Python stand-ins where a Rust UDF already exists.

## Scope Item 2: New Rust UDFs for as-of/interval/dedupe semantics

**Goal:** Implement missing semantics currently covered by ArrowDSL (as-of joins, interval
alignment scoring, dedupe strategies) as Rust UDFs/UDWFs/UDAFs and register them.

**Key pattern snippets (Rust UDF registration)**

```rust
// rust/datafusion_ext/src/udf_registry.rs
UdfSpec {
    name: "asof_select",
    kind: UdfKind::Window,
    builder: || UdfHandle::Window(udwf_custom::asof_select_udwf()),
    aliases: &[],
}
```

```rust
// rust/datafusion_ext/src/udaf_builtin.rs
pub fn builtin_udafs() -> Vec<AggregateUDF> {
    vec![
        // ...
        asof_best_match_udaf(),
        dedupe_best_by_score_udaf(),
    ]
}
```

```rust
// rust/datafusion_ext/src/udf_custom.rs (scalar helper)
#[pyfunction]
pub fn interval_align_score(expr: PyExpr, bucket: PyExpr) -> PyExpr {
    // Produces a score used by UDAF/UDWF selection.
    Expr::ScalarFunction(/* custom kernel */).into()
}
```

**Target files**
- `rust/datafusion_ext/src/udf_custom.rs` (scalar helpers, ranking/score kernels)
- `rust/datafusion_ext/src/udaf_builtin.rs` (aggregate strategies)
- `rust/datafusion_ext/src/udwf_builtin.rs` or new `udwf_custom.rs` (as-of window logic)
- `rust/datafusion_ext/src/udf_registry.rs` (registration + aliases)
- `rust/datafusion_ext/src/udf_docs.rs` (documentation metadata)
- `rust/datafusion_ext/src/registry_snapshot.rs` (ensure signatures/return types are exposed)
- `src/datafusion_engine/udf_runtime.py` (required UDF validation)
- `src/datafusion_engine/kernels.py` (kernel integration)

**Implementation checklist**
- [x] Define concrete Rust UDF specs for:
  - As-of join selector (`asof_select` UDWF or UDAF).
  - Interval alignment score (`interval_align_score` scalar UDF).
  - Dedupe selection (`dedupe_best_by_score` UDAF or UDWF).
- [x] Register new UDFs in `udf_registry.rs` with aliases as needed.
- [x] Add doc metadata to `udf_docs.rs` for discoverability.
- [x] Update Python `required_udfs` lists and kernel usage paths.
- [x] Add Rust conformance tests under `rust/datafusion_ext/tests/udf_conformance.rs`.

## Scope Item 3: Integrate new UDFs into DataFusion kernel paths

**Goal:** Wire the new UDFs into `datafusion_engine.kernels` and ensure plan bundles
capture required UDF metadata for deterministic planning.

**Key pattern snippet (kernel usage)**

```python
from datafusion_engine.kernel_specs import DedupeSpec, SortKey
from datafusion_engine.udf_runtime import register_rust_udfs, validate_required_udfs

snapshot = register_rust_udfs(session_runtime.ctx)
validate_required_udfs(snapshot, required=["dedupe_best_by_score"])

spec = DedupeSpec(
    keys=("entity_id",),
    strategy="KEEP_BEST_BY_SCORE",
    tie_breakers=(SortKey("score", "DESC"),),
)
output = dedupe_kernel(table, spec=spec, runtime_profile=runtime_profile)
```

**Target files**
- `src/datafusion_engine/kernels.py`
- `src/datafusion_engine/kernel_specs.py`
- `src/datafusion_engine/udf_runtime.py`
- `src/datafusion_engine/plan_bundle.py` (required UDF tracking)
- `src/datafusion_engine/view_registry_specs.py` (UDF dependency capture)

**Implementation checklist**
- [x] Add required UDF names to kernel call paths (as-of, interval align, dedupe).
- [x] Ensure plan bundles include required UDF names + snapshot hashes.
- [ ] Update tests that assert UDF coverage for plan bundles.

## Scope Item 4: DataFusion-first schema validation and encoding

**Goal:** Replace ArrowDSL schema validation/encoding with DataFusion-native helpers that
operate on `SessionRuntime` + `DataFusionRuntimeProfile`, using `information_schema`.

**Key pattern snippet (new schema validation helper)**

```python
from datafusion_engine.schema_introspection import SchemaIntrospector
from datafusion_engine.sql_options import sql_options_for_profile

def validate_dataset_schema(
    session_runtime: SessionRuntime,
    *,
    table_key: str,
    schema_spec: SchemaSpec,
) -> list[SchemaViolation]:
    introspector = SchemaIntrospector(
        session_runtime.ctx,
        sql_options=sql_options_for_profile(session_runtime.profile),
    )
    actual = introspector.table_schema(table_key)
    return compare_schema(schema_spec, actual)
```

**Key pattern snippet (encoding via write pipeline)**

```python
from datafusion_engine.write_pipeline import WritePipeline

WritePipeline(gateway).write(
    request,
    encoding_policy=encoding_policy,
    schema_mode=schema_mode,
)
```

**Target files**
- `src/datafusion_engine/schema_validation.py` (new)
- `src/datafusion_engine/encoding.py` (new or folded into write pipeline)
- `src/datafusion_engine/write_pipeline.py`
- `src/extract/schema_ops.py`
- `src/datafusion_engine/finalize.py`
- `src/schema_spec/system.py`
- `src/storage/deltalake/delta.py`
- `src/arrowdsl/schema/validation.py` (remove usage)
- `src/arrowdsl/schema/encoding_policy.py` (remove usage)
- `src/arrowdsl/schema/schema.py` (remove usage)

**Implementation checklist**
- [x] Add DataFusion-native validation helper module and wire call sites.
- [x] Centralize encoding policy in `write_pipeline` or `encoding.py`.
- [x] Remove ArrowDSL schema validation/encoding imports from all call sites.
- [x] Ensure schema validation errors flow through `FinalizeContext` consistently.

## Scope Item 5: Finish ArrowDSL removal + ExecutionContext cleanup

**Goal:** Complete removal of `src/arrowdsl/**`, delete all remaining imports, and ensure
the `SessionRuntime` + `DataFusionRuntimeProfile` boundary is the sole runtime surface.

**Key pattern snippet (runtime boundary)**

```python
session_runtime = runtime_profile.session_runtime()
ctx = session_runtime.ctx
snapshot = register_rust_udfs(ctx)
```

**Target files**
- `src/arrowdsl/**` (delete after final import removal)
- `src/datafusion_engine/__init__.py` (export cleanup)
- `src/engine/runtime_profile.py`
- `src/engine/session.py`
- `src/normalize/runtime.py`
- `src/incremental/runtime.py`
- `src/extract/session.py`
- `src/hamilton_pipeline/modules/*`

**Implementation checklist**
- [x] Remove any last `arrowdsl.*` imports from Python modules.
- [x] Delete `src/arrowdsl/**` once imports are fully migrated.
- [x] Confirm no `ExecutionContext` or ArrowDSL runtime profile types remain.
- [ ] Update exports and docs to remove ArrowDSL references.

## Scope Item 6: Tests, lint, and type gates

**Goal:** Ensure all new UDFs and DataFusion-first schema paths are verified and pass
repo quality gates.

**Key pattern snippet (UDF snapshot assertion)**

```python
snapshot = register_rust_udfs(session_runtime.ctx)
validate_required_udfs(snapshot, required=["asof_select", "interval_align_score"])
```

**Target files**
- `tests/unit/**` and `tests/integration/**` as touched by changes
- `rust/datafusion_ext/tests/udf_conformance.rs`

**Implementation checklist**
- [x] Add tests for new UDF registration and signature metadata.
- [x] Update kernel tests to exercise the new UDF-based code paths.
- [ ] Run `uv run ruff check --fix`, `uv run pyrefly check`,
  `uv run pyright --warnings --pythonversion=3.13.11`.
