# Zero-Row Bootstrap Implementation Plan v1

Date: 2026-02-06
Related findings: `docs/plans/zero_row_bootstrap_feasibility_review_2026-02-06.md`

## Objective

Implement a production-grade zero-row bootstrap capability that:

1. Resolves metadata/constraint checks that should not fail purely because a table has `0` rows.
2. Bootstraps and validates the full end-to-end runtime graph using schema-correct Delta tables with `0` rows.

This plan is intentionally implementation-level and includes representative (not full) code snippets, target files, and a delivery checklist.

## Success Criteria

- Session startup succeeds in explicit zero-row bootstrap mode without row-dependent false failures.
- All required extract/semantic/relationship datasets can be materialized as Delta tables with `_delta_log` present and `0` rows.
- Full semantic compile + planning + runtime validations execute against the bootstrapped environment.
- Observability emits deterministic bootstrap artifacts for auditability.

## Scope Overview

## Scope Item 1: Make Semantic Metadata Validation Zero-Row Safe

### Problem

`src/datafusion_engine/schema/registry.py` currently checks semantic metadata using row probes (`arrow_metadata(...)` + `limit(1)`), which fails for empty tables even when schema metadata is present.

### Approach

Shift semantic metadata validation to schema-level field metadata first, with optional row-probe fallback only when explicitly requested.

### Representative snippet

```python
# src/datafusion_engine/schema/registry.py

def _semantic_type_from_field_metadata(field: pa.Field) -> str | None:
    meta = field.metadata or {}
    raw = meta.get(SEMANTIC_TYPE_META)
    if raw is None:
        return None
    try:
        return raw.decode("utf-8")
    except Exception:
        return str(raw)


def _require_semantic_type(
    ctx: SessionContext,
    *,
    table_name: str,
    column_name: str,
    expected: str,
    allow_row_probe_fallback: bool = False,
) -> None:
    schema = ctx.table(table_name).schema()
    field = schema.field(column_name)
    observed = _semantic_type_from_field_metadata(field)
    if observed is None and allow_row_probe_fallback:
        observed = _semantic_type_via_row_probe(ctx, table_name=table_name, column_name=column_name)
    if observed is None:
        raise ValueError(f"Missing semantic type metadata on {table_name}.{column_name}.")
    if observed != expected:
        raise ValueError(
            f"Semantic type mismatch for {table_name}.{column_name}: expected {expected!r}, got {observed!r}."
        )
```

### Target files

- Edit: `src/datafusion_engine/schema/registry.py`
- Edit: `tests/unit/test_datafusion_schema_registry.py`
- Edit: `tests/integration/semantics/test_compiler_type_validation.py`

### Checklist

- [ ] Add schema-metadata decode helper for semantic type tags.
- [ ] Update semantic type validation path to schema-first behavior.
- [ ] Keep row-probe fallback optional and disabled in zero-row mode.
- [ ] Add tests proving validation passes on empty tables with valid field metadata.

---

## Scope Item 2: Introduce Validation Policy for Zero-Row Runtime Modes

### Problem

Schema/view validation errors are currently escalated uniformly during schema registry installation, including errors that are artifacts of empty-row probing.

### Approach

Add an explicit runtime validation policy to separate:
- hard failures (missing tables/columns/type mismatches),
- advisory issues (row-dependent checks in zero-row bootstrap mode).

### Representative snippet

```python
# src/datafusion_engine/session/runtime.py

class ZeroRowValidationMode(msgspec.Struct, frozen=True):
    # "off" = current behavior, "bootstrap" = zero-row tolerant
    mode: str = "off"


def _schema_registry_issues(
    validation: SchemaRegistryValidationResult,
    *,
    zero_row_mode: ZeroRowValidationMode,
) -> dict[str, object]:
    issues: dict[str, object] = {}
    if validation.missing:
        issues["missing"] = list(validation.missing)
    if validation.type_errors:
        issues["type_errors"] = dict(validation.type_errors)
    if validation.constraint_drift:
        issues["constraint_drift"] = list(validation.constraint_drift)
    if validation.relationship_constraint_errors:
        issues["relationship_constraint_errors"] = dict(validation.relationship_constraint_errors)
    if validation.view_errors:
        if zero_row_mode.mode == "bootstrap":
            issues["advisory_view_errors"] = dict(validation.view_errors)
        else:
            issues["view_errors"] = dict(validation.view_errors)
    return issues
```

### Target files

- Edit: `src/datafusion_engine/session/runtime.py`
- Edit: `tests/unit/test_runtime_profile_snapshot.py`
- Edit: `tests/integration/runtime/test_runtime_context_smoke.py`

### Checklist

- [ ] Add zero-row validation mode configuration to runtime profile config.
- [ ] Update issue assembly to downgrade row-dependent view errors in bootstrap mode.
- [ ] Emit advisory diagnostics artifact when downgrade occurs.
- [ ] Preserve strict behavior for default runtime modes.

---

## Scope Item 3: Align Semantic Validation Table Set with Canonical Names

### Problem

`_semantic_validation_tables()` includes versioned names (`cpg_nodes_v1`, `cpg_edges_v1`) while semantic naming has canonical unversioned outputs (`cpg_nodes`, `cpg_edges`).

### Approach

Use canonical output names and include legacy aliases only where backward compatibility is required.

### Representative snippet

```python
# src/datafusion_engine/schema/registry.py
from semantics.naming import canonical_output_name


def _semantic_validation_tables() -> tuple[str, ...]:
    tables = set(extract_base_schema_names())
    tables.update(
        {
            canonical_output_name("cpg_nodes"),
            canonical_output_name("cpg_edges"),
            # optional temporary compatibility aliases:
            "cpg_nodes_v1",
            "cpg_edges_v1",
        }
    )
    return tuple(sorted(tables))
```

### Target files

- Edit: `src/datafusion_engine/schema/registry.py`
- Edit: `tests/unit/test_datafusion_schema_registry.py`
- Edit: `tests/unit/semantics/catalog/test_dataset_specs.py`

### Checklist

- [ ] Update semantic validation table list to canonical names.
- [ ] Keep compatibility aliases behind explicit temporary branch.
- [ ] Add regression tests for both canonical and alias presence behavior.

---

## Scope Item 4: Implement Zero-Row Bootstrap Planner (Dataset Inventory + Contracts)

### Problem

No dedicated planner currently produces an authoritative bootstrap dataset plan from existing extract/semantic/relationship registries and runtime location policies.

### Approach

Add a planner that composes:
- extract inputs (`SEMANTIC_INPUT_SPECS`),
- nested extract datasets and relationship schemas,
- semantic output datasets (optional mode),
- observability/cache internal tables (optional mode).

### Representative snippet

```python
# src/datafusion_engine/bootstrap/zero_row.py

@dataclass(frozen=True)
class ZeroRowDatasetPlan:
    name: str
    role: str
    schema: pa.Schema
    location: DatasetLocation | None
    required: bool


def build_zero_row_plan(profile: DataFusionRuntimeProfile) -> tuple[ZeroRowDatasetPlan, ...]:
    from datafusion_engine.extract.registry import dataset_schema as extract_dataset_schema
    from datafusion_engine.schema.registry import extract_nested_dataset_names, relationship_schema_for
    from semantics.input_registry import SEMANTIC_INPUT_SPECS
    from semantics.catalog.dataset_specs import dataset_names as semantic_dataset_names, dataset_schema

    plans: list[ZeroRowDatasetPlan] = []

    for spec in SEMANTIC_INPUT_SPECS:
        source = spec.extraction_source
        plans.append(
            ZeroRowDatasetPlan(
                name=source,
                role="semantic_input",
                schema=pa.schema(extract_dataset_schema(source)),
                location=profile.catalog_ops.dataset_location(source),
                required=True,
            )
        )

    for name in extract_nested_dataset_names():
        plans.append(
            ZeroRowDatasetPlan(
                name=name,
                role="extract_nested",
                schema=pa.schema(extract_dataset_schema(name)),
                location=profile.catalog_ops.dataset_location(name),
                required=False,
            )
        )

    for name in semantic_dataset_names():
        plans.append(
            ZeroRowDatasetPlan(
                name=name,
                role="semantic_output",
                schema=pa.schema(dataset_schema(name)),
                location=profile.catalog_ops.dataset_location(name),
                required=False,
            )
        )
    return tuple(plans)
```

### Target files

- New: `src/datafusion_engine/bootstrap/__init__.py`
- New: `src/datafusion_engine/bootstrap/zero_row.py`
- Edit: `src/datafusion_engine/session/runtime.py` (entrypoint wiring)
- Edit: `src/datafusion_engine/session/facade.py` (optional facade helper)

### Checklist

- [ ] Define bootstrap plan dataclasses.
- [ ] Build deterministic dataset inventory from existing registries/specs.
- [ ] Tag each planned dataset with role + requiredness.
- [ ] Resolve runtime location per dataset using existing catalog ops.

---

## Scope Item 5: Implement Zero-Row Bootstrap Executor (Materialize + Register)

### Problem

No unified executor currently creates missing zero-row Delta datasets then registers them for runtime use.

### Approach

For each planned dataset:
- resolve/create location,
- materialize empty Delta if missing `_delta_log`,
- register through existing facade/registry path,
- record per-dataset outcome.

### Representative snippet

```python
# src/datafusion_engine/bootstrap/zero_row.py

def _materialize_empty_delta(path: str, schema: pa.Schema) -> None:
    from deltalake.writer import write_deltalake
    empty = empty_table_for_schema(schema)
    write_deltalake(path, empty, mode="overwrite", schema_mode="overwrite")


def execute_zero_row_bootstrap(
    profile: DataFusionRuntimeProfile,
    *,
    ctx: SessionContext,
    plan: Sequence[ZeroRowDatasetPlan],
) -> tuple[dict[str, object], ...]:
    from datafusion_engine.session.facade import DataFusionExecutionFacade

    facade = DataFusionExecutionFacade(ctx=ctx, runtime_profile=profile)
    events: list[dict[str, object]] = []
    for item in plan:
        location = item.location
        if location is None:
            events.append({"dataset": item.name, "status": "skipped_no_location"})
            continue
        table_path = Path(str(location.path))
        delta_log = table_path / "_delta_log"
        if not delta_log.exists():
            table_path.parent.mkdir(parents=True, exist_ok=True)
            _materialize_empty_delta(str(table_path), item.schema)
        facade.register_dataset(name=item.name, location=location)
        events.append({"dataset": item.name, "status": "registered", "path": str(table_path)})
    return tuple(events)
```

### Target files

- New: `src/datafusion_engine/bootstrap/zero_row.py`
- Edit: `src/datafusion_engine/session/runtime.py`
- Edit: `src/datafusion_engine/session/facade.py`
- Edit: `tests/test_helpers/datafusion_runtime.py` (optional helper to invoke bootstrap)

### Checklist

- [ ] Add empty Delta materializer utility.
- [ ] Add deterministic create/register loop with per-dataset status.
- [ ] Keep strict failures for required datasets; advisory status for optional datasets.
- [ ] Record bootstrap events with stable payload shape.

---

## Scope Item 6: Unify Missing-Log Bootstrap Behavior for Cache + Observability Tables

### Problem

`cache/inventory.py` and `delta/observability.py` currently skip on missing `_delta_log`, while other internal tables already bootstrap automatically.

### Approach

When `_delta_log` is missing:
- bootstrap table with canonical schema,
- continue with registration,
- keep diagnostic event for traceability.

### Representative snippet

```python
# src/datafusion_engine/cache/inventory.py

if not has_delta_log:
    profile.record_artifact("cache_inventory_bootstrap_started_v1", {"path": str(table_path)})
    _bootstrap_cache_inventory_table(
        ctx,
        profile,
        table_path=table_path,
        schema=_cache_inventory_schema(),
    )
```

```python
# src/datafusion_engine/delta/observability.py

if not has_delta_log:
    profile.record_artifact("delta_observability_bootstrap_started_v1", {"table": name, "path": str(table_path)})
    _bootstrap_observability_table(
        ctx,
        profile,
        table_path=table_path,
        schema=schema,
        operation="delta_observability_bootstrap",
    )
```

### Target files

- Edit: `src/datafusion_engine/cache/inventory.py`
- Edit: `src/datafusion_engine/delta/observability.py`
- Edit: `tests/unit/datafusion_engine/test_delta_provider_artifacts.py`
- New: `tests/integration/runtime/test_zero_row_internal_table_bootstrap.py`

### Checklist

- [ ] Replace missing-log skip branches with bootstrap-and-continue behavior.
- [ ] Add explicit artifacts for bootstrap started/completed/failed.
- [ ] Verify registration succeeds immediately after bootstrap.
- [ ] Add tests for missing-log -> bootstrap -> append lifecycle.

---

## Scope Item 7: Add End-to-End Zero-Row Validation Entry Point

### Problem

No first-class runtime API currently composes:
- plan generation,
- zero-row materialization/registration,
- semantic input validation,
- schema registry and semantic compile dry run.

### Approach

Add a runtime-level entrypoint returning a structured report artifact.

### Representative snippet

```python
# src/datafusion_engine/session/runtime.py

def run_zero_row_bootstrap_validation(
    self,
    *,
    include_semantic_outputs: bool = True,
) -> Mapping[str, object]:
    ctx = self.session_context()
    plan = build_zero_row_plan(self)
    events = execute_zero_row_bootstrap(self, ctx=ctx, plan=plan)

    from semantics.pipeline import _resolve_semantic_input_mapping
    input_mapping, _ = _resolve_semantic_input_mapping(ctx, runtime_profile=self, use_cdf=False, cdf_inputs=None)
    from semantics.validation import require_semantic_inputs
    require_semantic_inputs(ctx, input_mapping=input_mapping)

    report = {
        "event_time_unix_ms": int(time.time() * 1000),
        "planned": len(plan),
        "events": list(events),
        "status": "ok",
    }
    self.record_artifact("zero_row_bootstrap_validation_v1", report)
    return report
```

### Target files

- Edit: `src/datafusion_engine/session/runtime.py`
- Edit: `src/datafusion_engine/session/__init__.py` (export)
- New: `tests/integration/test_zero_row_bootstrap_e2e.py`

### Checklist

- [ ] Add runtime entrypoint returning stable report structure.
- [ ] Verify semantic input validation and schema registry checks execute post-bootstrap.
- [ ] Emit `zero_row_bootstrap_validation_v1` artifact.
- [ ] Add end-to-end integration test covering full flow on fresh temp roots.

---

## Scope Item 8: Optional Seeded-Minimal Mode for Truly Row-Dependent Contracts

### Problem

Some future validations may still require a physical row for strict contracts.

### Approach

Add optional mode:
- default: `strict_zero_rows`
- optional: `seeded_minimal_rows` for selected datasets only.

### Representative snippet

```python
# src/datafusion_engine/bootstrap/zero_row.py

def _bootstrap_table(
    *,
    path: str,
    schema: pa.Schema,
    mode: str,
    seed_row_factory: Callable[[pa.Schema], Mapping[str, object]] | None = None,
) -> None:
    if mode == "seeded_minimal_rows" and seed_row_factory is not None:
        table = pa.Table.from_pylist([dict(seed_row_factory(schema))], schema=schema)
    else:
        table = empty_table_for_schema(schema)
    write_deltalake(path, table, mode="overwrite", schema_mode="overwrite")
```

### Target files

- New: `src/datafusion_engine/bootstrap/zero_row.py`
- Edit: `src/datafusion_engine/session/runtime.py`
- New: `tests/unit/datafusion_engine/test_zero_row_bootstrap.py`

### Checklist

- [ ] Add mode enum/config.
- [ ] Keep `strict_zero_rows` as default.
- [ ] Add dataset-scoped seed hooks only where required.
- [ ] Ensure report clearly marks seeded datasets.

---

## Consolidated Target File List

### New files

- `src/datafusion_engine/bootstrap/__init__.py`
- `src/datafusion_engine/bootstrap/zero_row.py`
- `tests/unit/datafusion_engine/test_zero_row_bootstrap.py`
- `tests/integration/test_zero_row_bootstrap_e2e.py`
- `tests/integration/runtime/test_zero_row_internal_table_bootstrap.py`

### Edited files

- `src/datafusion_engine/schema/registry.py`
- `src/datafusion_engine/session/runtime.py`
- `src/datafusion_engine/session/facade.py`
- `src/datafusion_engine/session/__init__.py`
- `src/datafusion_engine/cache/inventory.py`
- `src/datafusion_engine/delta/observability.py`
- `tests/unit/test_datafusion_schema_registry.py`
- `tests/integration/semantics/test_compiler_type_validation.py`
- `tests/integration/runtime/test_runtime_context_smoke.py`
- `tests/test_helpers/datafusion_runtime.py`

## Delivery Plan (Execution Order)

1. Schema validation refactor (`Scope 1` + `Scope 3`).
2. Runtime validation policy (`Scope 2`).
3. Planner/executor module (`Scope 4` + `Scope 5`).
4. Internal-table bootstrap consistency (`Scope 6`).
5. E2E bootstrap entrypoint + reporting (`Scope 7`).
6. Optional seeded mode hardening (`Scope 8`).
7. Full integration validation and artifact contract checks.

## End-to-End Implementation Checklist

- [ ] Implement schema-first semantic metadata validation.
- [ ] Add zero-row validation mode and advisory error channel.
- [ ] Align semantic validation table naming with canonical outputs.
- [ ] Implement deterministic bootstrap planner from existing registries/specs.
- [ ] Implement executor for materialize/register/report loop.
- [ ] Unify missing-log bootstrap behavior in cache inventory and observability tables.
- [ ] Add runtime API for full zero-row bootstrap validation run.
- [ ] Add unit + integration tests for strict zero-row mode.
- [ ] Add optional seeded-minimal mode (guarded, explicit).
- [ ] Run full quality gate and verify deterministic artifact outputs.

## Notes on Built-In vs Bespoke

Built-in capabilities remain primary:
- Delta table creation from Arrow (including empty tables),
- DataFusion registration/query surfaces,
- existing dataset schema/spec registries,
- existing runtime catalog/location resolution.

Bespoke additions are intentionally thin:
- planner/executor orchestration,
- zero-row validation policy,
- consistency fixes for missing-log bootstrap behavior,
- reporting/test harness.
