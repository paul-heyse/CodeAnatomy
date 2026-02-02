# Semantic IR Pytest Alignment Plan v2

> **Purpose**: Resolve all pytest failures/skips/warnings from the latest run while reinforcing the semantic‑compiled IR architecture and improving determinism, observability, and DataFusion integration.

---

## Scope Item S1 — CLI config typing (runtime JsonValue + msgspec conversion)

**Why**: `msgspec` evaluates annotations at runtime. `JsonValue` is currently imported only under `TYPE_CHECKING`, causing `NameError` in CLI config validation and golden tests.

**Representative code pattern to implement**
```python
# src/cli/config_loader.py (and other CLI config modules)
from core_types import JsonValue  # runtime import for msgspec


def normalize_config_contents(config: Mapping[str, JsonValue]) -> dict[str, JsonValue]:
    flat: dict[str, JsonValue] = {}
    ...
```

**Target files**
- `src/cli/config_loader.py`
- `src/cli/config_models.py`
- `src/cli/config_source.py`
- `src/cli/kv_parser.py`
- `src/cli/context.py`
- `src/cli/validation.py`

**Deprecate / delete after completion**
- None.

**Implementation checklist**
- [ ] Convert `JsonValue` imports from `TYPE_CHECKING` to runtime imports.
- [ ] Confirm msgspec validation paths resolve types without `NameError`.

---

## Scope Item S2 — Delta extension compatibility + rebuild gate

**Why**: Delta tests are skipped due to ABI mismatch between the Rust plugin and the DataFusion Python bindings. We want explicit compatibility and a rebuild gate.

**Representative code pattern to implement**
```python
# src/datafusion_engine/delta/capabilities.py
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
            return DeltaExtensionCompatibility(False, False, "missing entrypoint")
        probe(_internal_ctx(ctx))
        return DeltaExtensionCompatibility(True, True, None)
    except (TypeError, RuntimeError, ValueError) as exc:
        return DeltaExtensionCompatibility(True, False, str(exc))
```

**Target files**
- `src/datafusion_engine/delta/capabilities.py` (new)
- `src/datafusion_engine/delta/control_plane.py`
- `tests/test_helpers/optional_deps.py`
- `scripts/rebuild_rust_artifacts.sh`

**Deprecate / delete after completion**
- Any ad‑hoc try/except fallback that hides ABI incompatibility.

**Implementation checklist**
- [ ] Add compatibility probe and surface explicit diagnostics.
- [ ] Gate delta tests on compatibility result, not just import success.
- [ ] Rebuild plugin when ABI mismatch is detected.

---

## Scope Item S3 — CQ golden stability + toolchain metadata normalization

**Why**: CQ golden files drift because toolchain metadata changed (rg→rpygrep, sg→sgpy) and query scopes widened. This affects golden output and performance.

**Representative code pattern to implement**
```python
# tools/cq/core/toolchain.py
return {
    "python": sys.version.split()[0],
    "rpygrep": _version("rpygrep"),
    "sgpy": _version("sgpy"),
}

# tools/cq/query/executor.py (scope enforcement)
query = query.with_scope(scope)  # apply before scanning
```

**Target files**
- `tools/cq/core/toolchain.py`
- `tools/cq/query/executor.py`
- `tools/cq/query/scanner.py`
- `tests/e2e/cq/test_query_golden.py` (regenerate goldens after stabilization)

**Deprecate / delete after completion**
- Legacy toolchain reporting fields (`rg`, `sg`).

**Implementation checklist**
- [ ] Normalize toolchain metadata keys.
- [ ] Apply query scope filtering before scanning.
- [ ] Add caching/prefiltering for hazard scan.
- [ ] Regenerate golden files after behavior stabilizes.

---

## Scope Item S4 — OTel metric gauges (dataset rows/columns)

**Why**: The metrics contract test expects dataset row/column gauges to be visible in the `InMemoryMetricReader` output.

**Representative code pattern to implement**
```python
# src/obs/otel/metrics.py
registry = MetricsRegistry(
    ...,
    dataset_rows_gauge=meter.create_observable_gauge(...),
    dataset_columns_gauge=meter.create_observable_gauge(...),
)
```

**Target files**
- `src/obs/otel/metrics.py`
- `tests/obs/_support/otel_harness.py` (force collection if needed)

**Deprecate / delete after completion**
- None.

**Implementation checklist**
- [ ] Ensure gauges are registered before any `set_dataset_stats` call.
- [ ] Trigger collection/flush in the harness if required.

---

## Scope Item S5 — Substrait plan normalization for lineage and views

**Why**: Certain logical plans (EXPLAIN/ANALYZE) fail Substrait encoding. Normalize before encoding to keep lineage stable.

**Representative code pattern to implement**
```python
# src/datafusion_engine/plan/bundle.py

def _normalize_for_substrait(plan: DataFusionLogicalPlan) -> DataFusionLogicalPlan:
    plan = _strip_explain_nodes(plan)
    plan = _strip_analyze_nodes(plan)
    return plan


def _encode_substrait(ctx: SessionContext, plan: DataFusionLogicalPlan) -> bytes:
    normalized = _normalize_for_substrait(plan)
    return ctx.substrait_logical_plan(normalized)
```

**Target files**
- `src/datafusion_engine/plan/bundle.py`
- `src/datafusion_engine/plan/normalization.py` (new helper)
- `tests/unit/test_lineage_plan_variants.py`
- `tests/unit/test_view_registry_snapshot.py`

**Deprecate / delete after completion**
- Per‑test Substrait fallbacks.

**Implementation checklist**
- [ ] Normalize logical plans before Substrait encoding.
- [ ] Preserve structured diagnostics when encoding still fails.

---

## Scope Item S6 — Deterministic plan artifacts (information_schema_hash)

**Why**: Golden plan artifacts drift due to nondeterministic schema and function ordering.

**Representative code pattern to implement**
```python
# src/datafusion_engine/schema/introspection.py
rows = sorted(rows, key=lambda row: (row["table"], row["column"]))
metadata = _stable_metadata(metadata)
```

**Target files**
- `src/datafusion_engine/schema/introspection.py`
- `src/datafusion_engine/identity.py`
- `tests/plan_golden/test_plan_artifacts.py` (golden update after fix)

**Deprecate / delete after completion**
- None.

**Implementation checklist**
- [ ] Sort schema/introspection rows before hashing.
- [ ] Normalize metadata ordering before hashing.
- [ ] Update golden snapshots after determinism is restored.

---

## Scope Item S7 — DataFusion stats policy defaults

**Why**: `list_files_cache_limit` is missing in the session config, causing policy tests to fail.

**Representative code pattern to implement**
```python
# src/datafusion_engine/session/runtime.py
config = SessionConfig()
config = config.set("datafusion.runtime.list_files_cache_limit", "1024")
```

**Target files**
- `src/datafusion_engine/session/runtime.py`
- `tests/unit/test_datafusion_stats_policy.py`

**Deprecate / delete after completion**
- None.

**Implementation checklist**
- [ ] Ensure list_files cache defaults are always set.
- [ ] Ensure determinism audit includes these defaults.

---

## Scope Item S8 — View artifact payload normalization

**Why**: Artifacts serialize empty tuples instead of JSON‑friendly lists.

**Representative code pattern to implement**
```python
# src/datafusion_engine/lineage/diagnostics.py
payload["items"] = list(payload.get("items", []))
```

**Target files**
- `src/datafusion_engine/lineage/diagnostics.py`
- `tests/unit/test_datafusion_view_artifact.py`

**Deprecate / delete after completion**
- None.

**Implementation checklist**
- [ ] Normalize payload sequences to lists before serialization.

---

## Scope Item S9 — Hamilton cache graph: always include ctx node

**Why**: Cache tests assume `ctx` is present and marked ignore_for_cache.

**Representative code pattern to implement**
```python
# src/hamilton_pipeline/modules/inputs.py
@config.when(...)  # ensure ctx always present

def ctx(...) -> OutputRuntimeContext: ...
```

**Target files**
- `src/hamilton_pipeline/modules/inputs.py`
- `src/hamilton_pipeline/driver_factory.py`
- `tests/unit/test_hamilton_cache_behavior.py`

**Deprecate / delete after completion**
- Any legacy alias nodes for ctx.

**Implementation checklist**
- [ ] Ensure ctx is a DAG node for every pipeline build.
- [ ] Mark ctx ignore_for_cache in Hamilton metadata.

---

## Scope Item S10 — Register or deprecate rel_name_symbol_v1

**Why**: Inferred deps tests fail because a required dataset schema is missing.

**Representative code pattern to implement**
```python
# src/datafusion_engine/schema/registry.py
REL_NAME_SYMBOL_SCHEMA = ...
register_schema("rel_name_symbol_v1", REL_NAME_SYMBOL_SCHEMA)
```

**Target files**
- `src/datafusion_engine/schema/registry.py`
- `src/semantics/catalog/dataset_registry.py`
- `tests/unit/test_inferred_deps.py`

**Deprecate / delete after completion**
- Any non‑IR dataset aliases replaced by canonical semantic datasets.

**Implementation checklist**
- [ ] Register schema or map to canonical dataset.
- [ ] Update inferred‑deps logic to use canonical names.

---

## Scope Item S11 — SessionRuntime threading in plan compilation

**Why**: plan compilation now requires `SessionRuntime`, but some call sites still omit it.

**Representative code pattern to implement**
```python
# src/datafusion_engine/plan/bundle.py

def build_plan_bundle(..., session_runtime: SessionRuntime, ...):
    ...
```

**Target files**
- `src/datafusion_engine/plan/bundle.py`
- `src/datafusion_engine/session/facade.py`
- `tests/unit/test_prepared_statements.py`
- `tests/unit/test_sql_param_binding.py`

**Deprecate / delete after completion**
- Implicit runtime creation within plan compilation.

**Implementation checklist**
- [ ] Thread SessionRuntime through all compile entrypoints.
- [ ] Update tests to pass the runtime explicitly.

---

## Scope Item S12 — scan_from_batches input normalization

**Why**: `pyarrow.dataset.Scanner.from_batches` expects list/dict columns, not tuple.

**Representative code pattern to implement**
```python
# src/datafusion_engine/io/adapter.py
if isinstance(columns, tuple):
    columns = list(columns)
```

**Target files**
- `src/datafusion_engine/io/adapter.py`
- `tests/unit/test_scan_from_batches.py`

**Deprecate / delete after completion**
- None.

**Implementation checklist**
- [ ] Normalize tuple columns to list prior to scanner creation.

---

## Scope Item S13 — Nested dataset schema resolution (IR‑aligned registration)

**Why**: `cst_nodes` is missing because legacy `ViewSpec.register` is removed.

**Representative code pattern to implement**
```python
# src/datafusion_engine/views/registration.py
ensure_view_graph(ctx, runtime_profile=profile, semantic_ir=semantic_ir)
```

**Target files**
- `src/datafusion_engine/views/registration.py`
- `src/datafusion_engine/schema/registry.py`
- `tests/unit/test_schema_catalog_registry.py`

**Deprecate / delete after completion**
- Direct `ViewSpec.register()` usage.

**Implementation checklist**
- [ ] Register nested views via view graph.
- [ ] Update schema catalog lookup paths accordingly.

---

## Scope Item S14 — SQL policy guard blocks DML pre‑planning

**Why**: DML should be blocked before planning to avoid “missing table” errors.

**Representative code pattern to implement**
```python
# src/datafusion_engine/sql/guard.py
if policy.read_only and _is_dml(sql):
    raise PermissionError("DML is blocked by SQL policy")
```

**Target files**
- `src/datafusion_engine/sql/guard.py`
- `tests/unit/test_sql_policy_matrix.py`

**Deprecate / delete after completion**
- Any late‑stage DML filtering in DataFusion execution.

**Implementation checklist**
- [ ] Parse SQL and enforce policy before planning.

---

## Scope Item S15 — Streaming adapter schema error normalization

**Why**: Tests expect a deterministic error message when schema negotiation is unsupported.

**Representative code pattern to implement**
```python
# src/datafusion_engine/arrow/interop.py
raise ValueError("Schema negotiation is not supported")
```

**Target files**
- `src/datafusion_engine/arrow/interop.py`
- `tests/unit/test_streaming_adapter.py`

**Deprecate / delete after completion**
- None.

**Implementation checklist**
- [ ] Raise explicit error message when schema is passed with batches.

---

## Scope Item S16 — Runtime profile mutation API

**Why**: `dataclasses.replace()` fails when runtime profile isn’t a dataclass.

**Representative code pattern to implement**
```python
# src/datafusion_engine/session/runtime.py
@dataclass(frozen=True)
class DataFusionRuntimeProfile: ...

# or
profile = profile.with_updates(custom_catalog_name=...)
```

**Target files**
- `src/datafusion_engine/session/runtime.py`
- `tests/unit/hamilton_pipeline/test_inputs_semantic_output_catalog.py`

**Deprecate / delete after completion**
- Direct `replace()` calls against non‑dataclass runtime objects.

**Implementation checklist**
- [ ] Ensure runtime profile supports immutable updates.
- [ ] Update call sites to use the new mutation API.

---

## Cross‑cutting acceptance gates

- All failing tests in `build/test-results/junit.xml` addressed by an explicit scope item above.
- Deterministic hashing verified by stable golden outputs (plan and CQ goldens).
- Delta extension compatibility is detectable and recoverable (rebuild or fail fast).
- IR‑aligned view registration is used everywhere (no `ViewSpec.register`).

