# Semantic IR Pytest Alignment Plan v1

> **Purpose**: Resolve all pytest failures/skips/warnings while further aligning the runtime and tests with the semantic‑compiled IR architecture.

---

## Scope Item S1 — Delta extension compatibility gate (eliminate ctx conversion errors)

**Why**: The Rust delta extension is ABI‑sensitive. When the compiled module does not match the installed DataFusion Python types, entrypoints fail with `SessionContext` conversion errors. We want explicit compatibility detection instead of late failures.

**Representative code pattern to implement**
```python
# datafusion_engine/delta/capabilities.py
@dataclass(frozen=True)
class DeltaExtensionCompatibility:
    available: bool
    compatible: bool
    error: str | None


def is_delta_extension_compatible(ctx: SessionContext) -> DeltaExtensionCompatibility:
    try:
        module = _resolve_extension_module(entrypoint="delta_scan_config_from_session")
        probe = getattr(module, "delta_scan_config_from_session", None)
        if not callable(probe):
            return DeltaExtensionCompatibility(available=False, compatible=False, error="missing entrypoint")
        probe(_internal_ctx(ctx))
        return DeltaExtensionCompatibility(available=True, compatible=True, error=None)
    except (TypeError, RuntimeError, ValueError) as exc:
        return DeltaExtensionCompatibility(available=True, compatible=False, error=str(exc))
```

**Target files**
- `src/datafusion_engine/delta/control_plane.py`
- `src/datafusion_engine/delta/capabilities.py` (new)
- `src/datafusion_engine/dataset/resolution.py`
- `tests/test_helpers/optional_deps.py` (add compatibility check helper)
- `tests/*` delta‑dependent tests (skip when incompatible)

**Deprecate / delete after completion**
- Any ad‑hoc try/except fallback for delta provider registration that masks compatibility errors.

**Implementation checklist**
- [ ] Add `DeltaExtensionCompatibility` + `is_delta_extension_compatible()`.
- [ ] Gate all delta entrypoints in `control_plane.py` with compatibility check.
- [ ] Update delta tests to skip when incompatible (not just when module exists).

---

## Scope Item S2 — Substrait plan normalization for lineage/planning

**Why**: Certain DataFusion logical plans (EXPLAIN/ANALYZE/UNNEST/recursive) cannot be encoded as Substrait. We normalize the logical plan before encoding rather than falling back.

**Representative code pattern to implement**
```python
# datafusion_engine/plan/bundle.py

def _normalize_for_substrait(plan: DataFusionLogicalPlan) -> DataFusionLogicalPlan:
    plan = _strip_explain_nodes(plan)
    plan = _strip_analyze_nodes(plan)
    plan = _collapse_limit_wrappers(plan)
    plan = _inline_unnest(plan)
    return plan


def _to_substrait_bytes(ctx: SessionContext, optimized: object | None) -> bytes:
    normalized = _normalize_for_substrait(cast("DataFusionLogicalPlan", optimized))
    return _encode_substrait(ctx, normalized)
```

**Target files**
- `src/datafusion_engine/plan/bundle.py`
- `src/datafusion_engine/plan/normalization.py` (new helper module if needed)
- `tests/unit/test_lineage_plan_variants.py`

**Deprecate / delete after completion**
- Any per‑test patching of Substrait requirements.

**Implementation checklist**
- [ ] Add plan normalization helpers.
- [ ] Normalize before Substrait encoding.
- [ ] Keep error reporting structured when encoding still fails.

---

## Scope Item S3 — Idempotent dataset registration (avoid “table already exists”)

**Why**: Tests (and IR workflows) should be able to re‑register tables deterministically without name collisions.

**Representative code pattern to implement**
```python
# datafusion_engine/dataset/registration.py
if overwrite or _table_exists(ctx, name):
    _safe_deregister(ctx, name)
register_dataset_df(...)
```

**Target files**
- `src/datafusion_engine/dataset/registration.py`
- `tests/test_helpers/datafusion_runtime.py` (use isolated contexts)

**Deprecate / delete after completion**
- Any test‑level workarounds that rely on global shared contexts.

**Implementation checklist**
- [ ] Make registration idempotent when `overwrite=True`.
- [ ] Ensure tests use isolated session contexts.

---

## Scope Item S4 — Arrow ingestion always streams (DataFusion 51 compatibility)

**Why**: `SessionContext.from_arrow` expects a stream/reader in newer DataFusion versions; passing a Table can raise errors.

**Representative code pattern to implement**
```python
# datafusion_engine/io/ingest.py
if isinstance(table, pa.Table):
    reader = pa.RecordBatchReader.from_batches(table.schema, table.to_batches())
    df = ctx.from_arrow(reader, name=name)
```

**Target files**
- `src/datafusion_engine/io/ingest.py`
- `src/datafusion_engine/kernels.py` (indirect use)
- `tests/unit/test_winner_select_determinism.py`

**Deprecate / delete after completion**
- Table‑based `from_arrow` calls (replace with reader).

**Implementation checklist**
- [ ] Normalize `pa.Table` inputs to a `RecordBatchReader` before `from_arrow`.

---

## Scope Item S5 — One‑shot dataset scanning (avoid schema on reader)

**Why**: `ds.Scanner.from_batches` rejects a `schema` argument when the reader already has one.

**Representative code pattern to implement**
```python
# storage/dataset_sources.py
if self.reader is not None:
    return ds.Scanner.from_batches(self.reader, **kwargs)
```

**Target files**
- `src/storage/dataset_sources.py`
- `tests/unit/test_scan_from_batches.py`

**Deprecate / delete after completion**
- `schema=` argument passed into `Scanner.from_batches`.

**Implementation checklist**
- [ ] Remove schema override for reader‑backed scanners.

---

## Scope Item S6 — Extension‑safe empty tables (pyarrow extension types)

**Why**: `pa.array([], type=extension)` raises `ArrowNotImplementedError` for extension types. We should materialize empty storage arrays and wrap them.

**Representative code pattern to implement**
```python
# datafusion_engine/arrow/build.py
if isinstance(field.type, pa.ExtensionType):
    storage = pa.array([], type=field.type.storage_type)
    columns.append(pa.ExtensionArray.from_storage(field.type, storage))
else:
    columns.append(pa.array([], type=field.type))
```

**Target files**
- `src/datafusion_engine/arrow/build.py`
- `tests/unit/test_datafusion_nested_registry.py`
- `tests/unit/test_datafusion_schema_registry.py`

**Deprecate / delete after completion**
- None.

**Implementation checklist**
- [ ] Handle extension types explicitly in `empty_table()`.

---

## Scope Item S7 — Typed extract schemas (repo_blobs + AST/CST nested outputs)

**Why**: Extract schemas currently default to string types, which breaks payloads with ints/bytes/nested structs and causes Arrow type errors.

**Representative code pattern to implement**
```python
# datafusion_engine/extract/registry.py
_TYPED_SCHEMA_OVERRIDES: dict[str, pa.Schema] = {
    "repo_file_blobs_v1": pa.schema([
        ("abs_path", pa.string()),
        ("size_bytes", pa.int64()),
        ("mtime_ns", pa.int64()),
        ("encoding", pa.string()),
        ("text", pa.string()),
        ("bytes", pa.binary()),
        ("file_id", pa.string()),
    ]),
}

if name in _TYPED_SCHEMA_OVERRIDES:
    return _TYPED_SCHEMA_OVERRIDES[name]
```

**Target files**
- `src/datafusion_engine/extract/registry.py`
- `src/datafusion_engine/extract/templates.py` (optional typed schema catalog)
- `src/extract/extractors/ast_extract.py` (nested schema enforcement)
- `tests/unit/test_repo_blobs_git.py`
- `tests/unit/test_ast_extract.py`

**Deprecate / delete after completion**
- Reliance on implicit string‑typed extract schema for nested outputs.

**Implementation checklist**
- [ ] Add typed schema overrides for repo blobs and nested AST/CST outputs.
- [ ] Ensure AST outputs are emitted with typed nested schema fields.

---

## Scope Item S8 — Evidence seeding aligned to Semantic IR roles

**Why**: We should not seed evidence with intermediate/IR datasets. Evidence should only include external inputs and view‑backed sources.

**Representative code pattern to implement**
```python
# semantics/catalog/dataset_rows.py
class SemanticDatasetRow:
    ...
    role: Literal["input", "intermediate", "output"] = "output"

# relspec/evidence.py
spec_sources = {name for name, spec in spec_map.items() if spec.role == "input"}
```

**Target files**
- `src/semantics/catalog/dataset_rows.py`
- `src/semantics/catalog/spec_builder.py`
- `src/semantics/ir_pipeline.py`
- `src/relspec/evidence.py`
- `tests/integration/test_evidence_semantic_catalog.py`

**Deprecate / delete after completion**
- Any implicit seeding of all dataset specs into evidence.

**Implementation checklist**
- [ ] Add `role` to `SemanticDatasetRow` and propagate from registries.
- [ ] Filter evidence seeding to role == `input`.

---

## Scope Item S9 — Semantic input registry + column validation derived from IR

**Why**: Input validation must reflect canonical IR schemas and names (including `_v1`), not legacy names/columns.

**Representative code pattern to implement**
```python
# semantics/input_registry.py
SEMANTIC_INPUT_SPECS = tuple(
    SemanticInputSpec(
        canonical_name=row.name,
        extraction_source=row.source_dataset or row.name,
        required=True,
        fallback_names=(row.name, canonical_output_name(row.name)),
    )
    for row in build_semantic_ir().dataset_rows
    if row.role == "input"
)
```

**Target files**
- `src/semantics/input_registry.py`
- `src/semantics/validation/catalog_validation.py`
- `tests/integration/test_semantic_pipeline.py`

**Deprecate / delete after completion**
- Hard‑coded input registry lists and legacy column specs.

**Implementation checklist**
- [ ] Derive input specs from IR dataset rows.
- [ ] Update required column specs to match IR‑aligned schemas.

---

## Scope Item S10 — Hamilton type normalization fixes

**Why**: Dynamic plan modules now include forward references that Hamilton can’t resolve (e.g., `Mapping`). Also avoid self‑dependency type mismatch in `materialized_outputs`.

**Representative code pattern to implement**
```python
# hamilton_pipeline/modules/execution_plan.py
resolved = get_type_hints(fn, globalns=fn.__globals__, localns=fn.__globals__)
normalized[key] = origin if isinstance(origin, type) else value

# hamilton_pipeline/modules/inputs.py
def materialized_outputs(requested_outputs: Sequence[str] | None = None) -> tuple[str, ...]:
    return tuple(str(name) for name in requested_outputs or ())
```

**Target files**
- `src/hamilton_pipeline/modules/execution_plan.py`
- `src/hamilton_pipeline/modules/inputs.py`
- `tests/unit/test_execution_plan_artifacts_module.py`
- `tests/unit/test_hamilton_cache_behavior.py`

**Deprecate / delete after completion**
- Self‑dependency of input parameter names matching node names.

**Implementation checklist**
- [ ] Normalize `Mapping`/`Sequence` annotations using function globals.
- [ ] Rename the `materialized_outputs` input parameter.

---

## Scope Item S11 — Safe SQL policy preflight

**Why**: SQL policy violations should be detected before DataFusion execution to avoid opaque errors.

**Representative code pattern to implement**
```python
# datafusion_engine/sql/guard.py
if _contains_named_args(sql) and not policy.allow_named_args:
    raise ValueError("SQL execution failed under safe options: named arguments")
if _is_ddl(sql) and not policy.allow_ddl:
    raise ValueError("SQL execution failed under safe options.")
```

**Target files**
- `src/datafusion_engine/sql/guard.py`
- `tests/unit/test_safe_sql.py`
- `tests/integration/test_expr_planner_hooks.py`

**Deprecate / delete after completion**
- Reliance on DataFusion error strings for policy enforcement.

**Implementation checklist**
- [ ] Add pre‑parsing for DDL/DML/named‑args.
- [ ] Raise consistent errors before execution.

---

## Scope Item S12 — Information‑schema metadata encoding

**Why**: DataFusion’s information_schema expects UTF‑8 metadata; binary msgpack causes decode errors.

**Representative code pattern to implement**
```python
# datafusion_engine/arrow/metadata.py
if isinstance(value, bytes):
    return base64.b64encode(value).decode("utf-8")
```

**Target files**
- `src/datafusion_engine/arrow/metadata.py`
- `src/datafusion_engine/schema/finalize.py` (metadata application)
- `tests/integration/test_information_schema_defaults.py`

**Deprecate / delete after completion**
- Emitting raw binary metadata into information_schema.

**Implementation checklist**
- [ ] Encode binary metadata to UTF‑8 safe strings.
- [ ] Preserve round‑trip decoding for internal consumers.

---

## Scope Item S13 — Cache diagnostics stability

**Why**: Cache diagnostics should always emit config + state snapshots when diagnostics are enabled.

**Representative code pattern to implement**
```python
# datafusion_engine/session/runtime.py
if self.diagnostics_sink is not None:
    self._record_cache_diagnostics(ctx)
```

**Target files**
- `src/datafusion_engine/session/runtime.py`
- `tests/unit/test_cache_introspection.py`

**Deprecate / delete after completion**
- None.

**Implementation checklist**
- [ ] Ensure cache diagnostics always emit when diagnostics sink is present.
- [ ] Relax tests to assert minimum expected cache snapshots rather than exact count.

---

## Scope Item S14 — OTel test harness resets

**Why**: Tests must be able to reset OpenTelemetry providers between runs to capture spans/logs/metrics deterministically.

**Representative code pattern to implement**
```python
# obs/otel/bootstrap.py

def reset_providers_for_tests() -> None:
    trace.set_tracer_provider(TracerProvider())
    metrics.set_meter_provider(MeterProvider())
    set_logger_provider(LoggerProvider())
    reset_metrics_registry()

# tests/obs/_support/otel_harness.py
reset_providers_for_tests()
```

**Target files**
- `src/obs/otel/bootstrap.py`
- `tests/obs/_support/otel_harness.py`
- `tests/obs/test_otel_*_contract.py`

**Deprecate / delete after completion**
- Ad‑hoc test‑level resets or reliance on global providers from previous tests.

**Implementation checklist**
- [ ] Add test reset helper in `obs.otel.bootstrap`.
- [ ] Call it in the OTel harness before provider install.

---

## Scope Item S15 — Update golden fixtures (CLI, IR snapshot, plan artifacts)

**Why**: Intentional IR and CLI changes must be reflected in golden snapshots.

**Representative code pattern to implement**
```bash
uv run pytest tests/cli_golden/ --update-golden
uv run pytest tests/semantics/test_semantic_ir_snapshot.py --update-golden
uv run pytest tests/plan_golden/ --update-goldens
```

**Target files**
- `tests/cli_golden/fixtures/help_build.txt`
- `tests/fixtures/semantic_ir_snapshot.json`
- `tests/plan_golden/*`

**Deprecate / delete after completion**
- Old golden files.

**Implementation checklist**
- [ ] Re‑generate and commit updated goldens.

---

## Scope Item S16 — Test‑only fixtures (remove missing CSV dependency)

**Why**: Tests should be hermetic and avoid missing file fixtures.

**Representative code pattern to implement**
```python
# tests/unit/test_inferred_deps.py
register_arrow_table(ctx, name="table_a", value=pa.table({"id": [1], "x": ["a"]}))
register_arrow_table(ctx, name="table_b", value=pa.table({"id": [1], "y": ["b"]}))
```

**Target files**
- `tests/unit/test_inferred_deps.py`
- `tests/test_helpers/arrow_seed.py` (if a reusable helper is needed)

**Deprecate / delete after completion**
- `tests/fixtures/test.csv` usage (no longer referenced).

**Implementation checklist**
- [ ] Replace missing CSV fixture usage with in‑memory tables.

---

## Cross‑Cutting Acceptance Gates

1. **All pytest failures resolved** (including skips + warnings where applicable).
2. **IR‑first alignment**: semantic input/evidence registries are derived from IR roles.
3. **Extension safety**: delta/udf hooks fail fast with explicit compatibility gates.
4. **Telemetry stability**: OTel contracts pass deterministically.

---

## Implementation Checklist (Summary)

- [ ] S1 Delta extension compatibility gate
- [ ] S2 Substrait plan normalization
- [ ] S3 Idempotent dataset registration
- [ ] S4 Arrow ingestion via streaming
- [ ] S5 One‑shot dataset scanner fix
- [ ] S6 Extension‑safe empty tables
- [ ] S7 Typed extract schemas
- [ ] S8 IR‑aligned evidence seeding
- [ ] S9 IR‑derived input registry & validation
- [ ] S10 Hamilton type normalization fixes
- [ ] S11 Safe SQL preflight
- [ ] S12 Metadata UTF‑8 encoding for information_schema
- [ ] S13 Cache diagnostics stability
- [ ] S14 OTel provider resets for tests
- [ ] S15 Golden fixture updates
- [ ] S16 Test fixture hermeticity
