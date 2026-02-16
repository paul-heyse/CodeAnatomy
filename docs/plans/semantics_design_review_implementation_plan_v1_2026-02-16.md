# Semantics Design Review Implementation Plan v1 (2026-02-16)

## Scope Summary

This plan implements **all actionable findings** from the comprehensive design review of `src/semantics/` (~23,400 LOC, 75 files). It consolidates the 27 items from `docs/reviews/semantics-synthesis.md` with additional non-overlapping items from subsystem reviews and follow-up code validation, yielding **44 actionable scope items plus 1 tracker item (S40)** organized into 14 logical groups.

**Design stance:** Incremental migration with no compatibility shims. Each scope item is independently deployable. Public API renames use hard cutover (update all callers in the same commit), but avoid unnecessary public signature churn when an internal helper extraction solves the same duplication. No new dependencies introduced. Test files required for all new modules.

**Deliberately excluded:** The "flatten builder-dispatch to two-level" suggestion from the core-pipeline review is deferred — it overlaps structurally with S31-S33 (pipeline.py decomposition) and should be revisited after those extractions land.

## Design Principles

1. **No `# noqa` or `# type: ignore`** — fix structurally.
2. **Every new module gets `from __future__ import annotations`** and a corresponding test file.
3. **`__all__` is mandatory** on every new module.
4. **Frozen dataclasses by default** — mutable only when caching requires it.
5. **`table_names_snapshot(ctx)` is the canonical table-existence check** — no ad-hoc `_table_exists` patterns.
6. **`SEMANTIC_MODEL` global access is legacy** — new code accepts `model: SemanticModel` as a parameter.
7. **Deferred imports are acceptable** for breaking circular dependencies but must have a `# deferred: <reason>` comment.

## Current Baseline

- `pipeline.py` is 2,256 LOC with 6 concerns; `__all__` lists only 6 symbols but 10+ are used externally.
- `compiler.py` is 1,818 LOC; `get()` method silently mutates state on cache miss.
- `ir_pipeline.py` has 5 direct `SEMANTIC_MODEL` global accesses; `compile_semantics` already accepts `model` as param.
- `diagnostics.py` is 773 LOC mixing 5 distinct responsibilities with zero structured logging.
- CDF read logic is duplicated across `cdf_reader.py` and `cdf_runtime.py` (distinct `CdfReadResult` shapes, duplicate `read_cdf_changes` entry points, duplicate fallback `DeltaTable.load_cdf` paths).
- `cdf_runtime.py` uses `Path(dataset_path).exists()` for availability checks, which is invalid for object-store Delta URIs.
- `cdf_runtime.py` fallback applies `_change_type` filtering post-read even though the filter policy already supports SQL predicate pushdown (`to_sql_predicate()`).
- `_table_exists` is duplicated in `diagnostics.py:36`, `signals.py:53`; `table_names_snapshot` is the superior pattern used in `scip_normalize.py` and `bytecode_line_table.py`.
- `DatasetRegistrySpec` duplicates all 20 fields of `SemanticDatasetRow` with a 30-line copy function in `ir_pipeline.py:319-349`.
- `metrics.py` and `stats/collector.py` have no production callsites but do have package exports and active tests; removal must be audit-gated.
- `_get_alias_maps()` in `dataset_specs.py` builds identity mappings (`{name: name for name in names}`).
- `JoinStrategy` and `SemanticIncrementalConfig` lack `__post_init__` validation.
- The `TableType`-to-entity-grain mapping is duplicated byte-for-byte in two functions within `catalog/tags.py:171-189`.

---

## S1. Rename `_cpg_view_specs` and Audit `__all__` Exports

### Goal
Promote `_cpg_view_specs` to a public name, fix incomplete `__all__` lists in `pipeline.py` and `__init__.py`, and add `build_cpg`/`CpgBuildOptions` to the package-level re-exports.

### Representative Code Snippets

```python
# src/semantics/pipeline.py — rename function
def cpg_view_specs(                    # was: _cpg_view_specs
    request: CpgViewSpecsRequest,
    ...
) -> tuple[SemanticViewSpec, ...]:
    ...

# src/semantics/pipeline.py — updated __all__
__all__ = [
    "CPG_INPUT_TABLES",
    "CachePolicy",
    "CpgBuildOptions",
    "CpgViewNodesRequest",
    "CpgViewSpecsRequest",
    "RelationshipSpec",
    "SemanticOutputWriteContext",
    "build_cpg",
    "build_cpg_from_inferred_deps",
    "cpg_view_specs",
]

# src/semantics/__init__.py — add pipeline entry points
from semantics.pipeline import CpgBuildOptions, build_cpg, build_cpg_from_inferred_deps

__all__ = [
    ...  # existing entries
    "CpgBuildOptions",
    "build_cpg",
    "build_cpg_from_inferred_deps",
]

# src/datafusion_engine/views/registry_specs.py — update import
from semantics.pipeline import CpgViewSpecsRequest, cpg_view_specs  # was: _cpg_view_specs
```

### Files to Edit
- `src/semantics/pipeline.py` — rename function + expand `__all__`
- `src/semantics/__init__.py` — add re-exports
- `src/datafusion_engine/views/registry_specs.py` — update import

### New Files to Create
None.

### Legacy Decommission/Delete Scope
- Delete the `_cpg_view_specs` name (replaced by `cpg_view_specs`) in `src/semantics/pipeline.py`.

---

## S2. Rename `SemanticCompiler.get()` to `get_or_register()`

### Goal
Signal the dual query+command behavior of the method that silently registers tables on cache miss.

### Representative Code Snippets

```python
# src/semantics/compiler.py
def get_or_register(self, name: str) -> TableInfo:  # was: get
    """Get a table, registering it first if not already cached.

    Parameters
    ----------
    name
        Table name to retrieve or register.
    """
    if name in self._tables:
        return self._tables[name]
    return self.register(name)
```

### Files to Edit
- `src/semantics/compiler.py` — rename method
- All callers of `compiler.get(...)` — update to `compiler.get_or_register(...)`

### New Files to Create
None.

### Legacy Decommission/Delete Scope
- Delete the `get` method name (replaced by `get_or_register`) in `src/semantics/compiler.py`.

---

## S3. Remove Dead `_builder_for_artifact_spec` and Update `emit_semantics` Docstring

### Goal
Remove the speculative stub that always raises, and correct the misleading "placeholder" docstring on `emit_semantics`.

### Representative Code Snippets

```python
# src/semantics/pipeline.py — REMOVE this function entirely:
# def _builder_for_artifact_spec(...) -> DataFrameBuilder:
#     _ = context
#     msg = f"Unsupported artifact spec output: {spec.name!r}."
#     raise ValueError(msg)

# Also remove its registration in _CONSOLIDATED_BUILDER_HANDLERS if present.

# src/semantics/ir_pipeline.py — fix docstring
def emit_semantics(ir: SemanticIR) -> SemanticIR:
    """Emit final semantic IR with dataset rows and model fingerprint.

    Compute dataset row metadata for all views in the IR and attach the
    semantic model fingerprint for reproducibility tracking.
    """
```

### Files to Edit
- `src/semantics/pipeline.py` — delete function + remove from dispatch table
- `src/semantics/ir_pipeline.py` — update docstring

### New Files to Create
None.

### Legacy Decommission/Delete Scope
- Delete `_builder_for_artifact_spec` function from `src/semantics/pipeline.py`.
- Delete its entry from `_CONSOLIDATED_BUILDER_HANDLERS` (or equivalent dispatch table).

---

## S4. Add `__post_init__` to `JoinStrategy`

### Goal
Enforce non-empty keys and valid confidence range at construction time instead of runtime dispatch.

### Representative Code Snippets

```python
# src/semantics/joins/strategies.py
@dataclass(frozen=True)
class JoinStrategy:
    strategy_type: JoinStrategyType
    left_keys: tuple[str, ...]
    right_keys: tuple[str, ...]
    filter_expr: str | None = None
    confidence: float = 1.0

    def __post_init__(self) -> None:
        if not self.left_keys:
            msg = "JoinStrategy requires non-empty left_keys."
            raise ValueError(msg)
        if not self.right_keys:
            msg = "JoinStrategy requires non-empty right_keys."
            raise ValueError(msg)
        if not 0.0 <= self.confidence <= 1.0:
            msg = f"JoinStrategy confidence must be in [0.0, 1.0], got {self.confidence}."
            raise ValueError(msg)
```

### Files to Edit
- `src/semantics/joins/strategies.py` — add `__post_init__`

### New Files to Create
None.

### Legacy Decommission/Delete Scope
- Remove the runtime `if not left_keys or not right_keys` guard in `src/semantics/compiler.py:487-489` (now enforced at construction).

---

## S5. Add `__post_init__` to `SemanticIncrementalConfig`

### Goal
Prevent `enabled=True` with `state_dir=None`, an illegal configuration that would fail at runtime.

### Representative Code Snippets

```python
# src/semantics/incremental/config.py
@dataclass(frozen=True)
class SemanticIncrementalConfig:
    enabled: bool = False
    state_dir: Path | None = None
    # ... other fields ...

    def __post_init__(self) -> None:
        if self.enabled and self.state_dir is None:
            msg = "SemanticIncrementalConfig: state_dir is required when enabled=True."
            raise ValueError(msg)
```

### Files to Edit
- `src/semantics/incremental/config.py` — add `__post_init__`

### New Files to Create
None.

### Legacy Decommission/Delete Scope
None.

---

## S6. Add `require_unambiguous_spans()` to `SemanticSchema`

### Goal
Encapsulate span ambiguity detection in the schema, replacing the reimplemented scan in the compiler. Also activates the currently-dead `_has_ambiguous_span`, `_span_start_candidates`, and `_span_end_candidates` methods.

### Representative Code Snippets

```python
# src/semantics/schema.py
def require_unambiguous_spans(self, *, table: str) -> None:
    """Raise if the schema has ambiguous span start/end columns.

    Parameters
    ----------
    table
        Table name for diagnostic messages.

    Raises
    ------
    SemanticSchemaError
        If multiple span start or end candidates exist.
    """
    if self._has_ambiguous_span():
        starts = self._span_start_candidates()
        ends = self._span_end_candidates()
        msg = (
            f"Ambiguous span columns in table {table!r}: "
            f"start candidates={starts}, end candidates={ends}. "
            f"Specify span columns explicitly."
        )
        raise SemanticSchemaError(msg)

# src/semantics/compiler.py — replace lines 724-740 with:
sem.require_unambiguous_spans(table=table_name)
```

### Files to Edit
- `src/semantics/schema.py` — add public method, make `_has_ambiguous_span` etc. used
- `src/semantics/compiler.py` — replace manual span-candidate scan with `sem.require_unambiguous_spans(table=...)`

### New Files to Create
None.

### Legacy Decommission/Delete Scope
- Delete the manual `column_types.items()` scan at `src/semantics/compiler.py:724-740`.

---

## S7. Fix Double-Inference in Compiler Join Fallback

### Goal
Eliminate the redundant second call to `require_join_strategy` when `infer_join_strategy_with_confidence` already returned `None`.

### Representative Code Snippets

```python
# src/semantics/compiler.py — replace lines 898-914
strategy_result = infer_join_strategy_with_confidence(
    left_info.annotated,
    right_info.annotated,
    hint=hint,
)
if strategy_result is None:
    caps = JoinCapabilities.from_schemas(left_info.annotated, right_info.annotated)
    msg = (
        f"Cannot infer join strategy for {left_table!r} x {right_table!r}. "
        f"Capabilities: {caps.describe()}"
    )
    raise JoinInferenceError(msg)
strategy = strategy_result.strategy
join_confidence = strategy_result.confidence
```

### Files to Edit
- `src/semantics/compiler.py` — replace double-inference pattern

### New Files to Create
None.

### Legacy Decommission/Delete Scope
- Remove the `require_join_strategy(...)` call from the fallback path in `compiler.py`.

---

## S8. Extract `_TABLE_TYPE_GRAIN_MAP` Constant in `catalog/tags.py`

### Goal
Eliminate byte-for-byte duplicate mapping dictionaries in two functions.

### Representative Code Snippets

```python
# src/semantics/catalog/tags.py — module-level constant
_TABLE_TYPE_GRAIN_MAP: Final[dict[TableType, tuple[str, str]]] = {
    TableType.RELATION: ("edge", "per_edge"),
    TableType.ENTITY: ("entity", "per_entity"),
    TableType.EVIDENCE: ("evidence", "per_evidence"),
    TableType.SYMBOL_SOURCE: ("symbol", "per_symbol"),
}

def _infer_entity_from_schema(...) -> tuple[str, str] | None:
    ...
    return _TABLE_TYPE_GRAIN_MAP.get(table_type)

def _infer_entity_grain_from_table_type(table_type: TableType) -> tuple[str, str] | None:
    return _TABLE_TYPE_GRAIN_MAP.get(table_type)
```

### Files to Edit
- `src/semantics/catalog/tags.py` — extract constant, update both functions

### New Files to Create
None.

### Legacy Decommission/Delete Scope
- Delete the inline `mapping = { ... }` dicts in both `_infer_entity_from_schema` and `_infer_entity_grain_from_table_type`.

---

## S9. Standardize `_table_exists` on `table_names_snapshot`

### Goal
Replace 2 ad-hoc `_table_exists` implementations (catching 5 exception types) with the canonical `table_names_snapshot` approach.

### Representative Code Snippets

```python
# src/semantics/diagnostics.py — replace _table_exists usage
from datafusion_engine.schema.introspection import table_names_snapshot

# Replace: if _table_exists(ctx, "some_table"):
# With:    if "some_table" in table_names_snapshot(ctx):
available = table_names_snapshot(ctx)
if "some_table" in available:
    df = ctx.table("some_table")
    ...

# Same pattern in src/semantics/signals.py
```

### Files to Edit
- `src/semantics/diagnostics.py` — remove `_table_exists`, use `table_names_snapshot`
- `src/semantics/signals.py` — remove `_table_exists`, use `table_names_snapshot`

### New Files to Create
None.

### Legacy Decommission/Delete Scope
- Delete `_table_exists` function from `src/semantics/diagnostics.py:36-48`.
- Delete `_table_exists` function from `src/semantics/signals.py:53-65`.

---

## S10. Add Logging to Silent Exception Handlers

### Goal
Make graceful degradation events observable by adding structured logging to exception handlers that currently silently swallow errors.

### Representative Code Snippets

```python
# src/semantics/catalog/analysis_builders.py
import logging

logger = logging.getLogger(__name__)

# At line 275 (SCIP fallback):
except (RuntimeError, KeyError, ValueError):
    logger.debug("SCIP table %r unavailable; falling back to empty result.", table_name)

# At line 536 (tree-sitter fallback):
except (RuntimeError, KeyError, ValueError):
    logger.debug("Tree-sitter error table unavailable; falling back to empty result.")

# src/semantics/incremental/cdf_cursors.py — at line 173-176:
except (DecodeError, OSError) as exc:
    logger.warning("Failed to load cursor for %r: %s", dataset_name, exc)
    return None

# src/semantics/incremental/cdf_runtime.py — at CDF fallback transitions:
logger.debug("DataFusion CDF read failed for %r; falling back to direct load_cdf.", dataset_path)
```

### Files to Edit
- `src/semantics/catalog/analysis_builders.py` — add logger + 2 log calls
- `src/semantics/incremental/cdf_cursors.py` — add logger + 1 log call
- `src/semantics/incremental/cdf_runtime.py` — add logger + log calls at fallback transitions

### New Files to Create
None.

### Legacy Decommission/Delete Scope
None.

---

## S11. Add `stage_span` to Signals and Diagnostics Builders

### Goal
Consistent OpenTelemetry instrumentation across all semantic builders, matching the pattern in normalization modules.

### Representative Code Snippets

```python
# src/semantics/signals.py
from obs.otel.scopes import SCOPE_SEMANTICS
from obs.otel.tracing import stage_span

def build_file_quality_view(ctx: SessionContext) -> DataFrame:
    with stage_span(
        "semantics.build_file_quality_view",
        stage="semantics",
        scope_name=SCOPE_SEMANTICS,
        attributes={"codeanatomy.view": "file_quality"},
    ) as span:
        ...

# src/semantics/diagnostics.py — same pattern for each major builder
def build_relationship_quality_metrics(ctx: SessionContext, ...) -> DataFrame:
    with stage_span(
        "semantics.build_relationship_quality_metrics",
        stage="semantics",
        scope_name=SCOPE_SEMANTICS,
    ) as span:
        ...
```

### Files to Edit
- `src/semantics/signals.py` — add `stage_span` to `build_file_quality_view`
- `src/semantics/diagnostics.py` — add `stage_span` to major builders

### New Files to Create
None.

### Legacy Decommission/Delete Scope
None.

---

## S12. Add Structured Debug Logging to Join Inference

### Goal
Enable diagnosis of unexpected join strategy selections by logging which strategies were considered and rejected.

### Representative Code Snippets

```python
# src/semantics/joins/inference.py
import logging

logger = logging.getLogger(__name__)

def _infer_default(
    left: AnnotatedSchema, right: AnnotatedSchema
) -> JoinStrategy | None:
    # Log each strategy check
    if _can_join_by_span_overlap(left, right):
        logger.debug("Join inference: span_overlap viable for %s x %s", left.name, right.name)
        return _build_span_overlap_strategy(left, right)
    logger.debug("Join inference: span_overlap not viable for %s x %s", left.name, right.name)
    # ... continue for each strategy ...
```

### Files to Edit
- `src/semantics/joins/inference.py` — add logger + debug calls in `_infer_default` and `_infer_with_hint`

### New Files to Create
None.

### Legacy Decommission/Delete Scope
None.

---

## S13. Normalize `compiled_cache_policy` at `CpgBuildOptions` Boundary

### Goal
Catch invalid cache policy strings at construction time rather than deep in the pipeline.

### Representative Code Snippets

```python
# src/semantics/pipeline.py
@dataclass(frozen=True)
class CpgBuildOptions:
    ...
    compiled_cache_policy: Mapping[str, CachePolicy] | None = None  # was: Mapping[str, str]
    ...

    def __post_init__(self) -> None:
        # Normalize string cache policies to CachePolicy at construction
        if self.compiled_cache_policy is not None:
            normalized = {
                k: _normalize_cache_policy(v) if isinstance(v, str) else v
                for k, v in self.compiled_cache_policy.items()
            }
            object.__setattr__(self, "compiled_cache_policy", normalized)
```

### Files to Edit
- `src/semantics/pipeline.py` — add `__post_init__` or adjust type + normalization boundary

### New Files to Create
None.

### Legacy Decommission/Delete Scope
- Move the `_normalize_cache_policy_mapping` call from `_view_nodes_for_cpg` (line 1247) to `CpgBuildOptions.__post_init__`.

---

## S14. Audit and Gate Deletion of `metrics.py` and `stats/collector.py`

### Goal
Replace assumption-based deletion with a gated decision:
1. Delete these modules if they are truly dead in production and tests can be removed/migrated.
2. Retain them (and narrow their API) if they are intentional test-support utilities.

### Representative Code Snippets

```python
# Deletion gate checklist (must all pass before remove):
# 1) No non-test imports of semantics.metrics / semantics.stats.collector
# 2) No package-level API contract requiring these symbols
# 3) tests/unit/semantics/stats/test_collector.py removed or migrated
#
# If any gate fails, keep modules and document scope as "retained utility API".
```

### Files to Edit
- `src/semantics/__init__.py` — update exports if modules are deleted or narrowed
- `src/semantics/stats/__init__.py` — remove re-exports or document retained surface
- `tests/unit/semantics/stats/test_collector.py` — remove/migrate tests if deletion path selected

### New Files to Create
None.

### Legacy Decommission/Delete Scope
- If S14 audit gates pass, delete `src/semantics/metrics.py`.
- If S14 audit gates pass, delete `src/semantics/stats/collector.py`.
- If S14 audit gates pass and the module becomes empty, delete `src/semantics/stats/__init__.py`.

---

## S15. Simplify Alias System and Remove Unused `ctx` Parameter in `dataset_specs.py`

### Goal
Remove the identity-mapping alias system that maps every name to itself, and remove the unused `ctx` parameter from spec access functions.

### Representative Code Snippets

```python
# src/semantics/catalog/dataset_specs.py

# REMOVE _get_alias_maps() entirely and replace callers:

def dataset_alias(name: str) -> str:
    """Return the canonical name (identity — aliases are not currently used)."""
    return name

def dataset_name_from_alias(alias: str) -> str:
    """Return the canonical name from an alias (identity)."""
    return alias

# Remove ctx parameter:
def dataset_spec(name: str) -> DatasetSpec:  # was: (name, ctx=None)
    specs = _get_dataset_specs()
    if name not in specs:
        msg = f"Unknown semantic dataset: {name!r}."
        raise KeyError(msg)
    return specs[name]
```

### Files to Edit
- `src/semantics/catalog/dataset_specs.py` — simplify alias functions, remove `ctx` parameter
- All callers of `dataset_spec(name, ctx=...)` — remove `ctx` argument

### New Files to Create
None.

### Legacy Decommission/Delete Scope
- Delete `_get_alias_maps()` function and `_CACHE.dataset_aliases` / `_CACHE.aliases_to_name` fields.
- Delete `ctx` parameter from `dataset_spec()`, `maybe_dataset_spec()`, and `dataset_contract()`.

---

## S16. Simplify `fingerprints.py` Defensive getattr Chains

### Goal
Replace ~80 LOC of defensive polymorphism handling with direct PyArrow schema API calls.

### Representative Code Snippets

```python
# src/semantics/plans/fingerprints.py — replace _extract_field_type, _schema_from_names, etc.

def _schema_to_string(schema: pa.Schema) -> str:
    """Convert a PyArrow schema to a deterministic string representation."""
    parts = []
    for i in range(len(schema)):
        field = schema.field(i)
        parts.append(f"{field.name}:{field.type}")
    return ",".join(parts)
```

### Files to Edit
- `src/semantics/plans/fingerprints.py` — replace `_extract_field_type`, `_schema_from_names`, `_schema_to_string`, `_plan_to_string` with direct PyArrow API usage

### New Files to Create
None.

### Legacy Decommission/Delete Scope
- Delete `_extract_field_type` function (~15 LOC of getattr chains).
- Delete `_schema_from_names` function.
- Simplify `_schema_to_string` to ~5 lines.

---

## S17. Improve `naming.py` Type Safety with Protocols

### Goal
Replace `object` parameter types with narrow Protocols for type-safe access without circular imports.

### Representative Code Snippets

```python
# src/semantics/naming.py
from typing import Protocol

class HasOutputNameMap(Protocol):
    output_name_map: Mapping[str, str]

def canonical_output_name(
    name: str,
    *,
    manifest: HasOutputNameMap | None = None,
) -> str:
    if manifest is not None:
        return manifest.output_name_map.get(name, name)
    return name
```

### Files to Edit
- `src/semantics/naming.py` — replace `object` with Protocol types

### New Files to Create
None.

### Legacy Decommission/Delete Scope
- Delete `getattr(manifest, "output_name_map", None)` pattern — replaced by typed Protocol access.

---

## S18. Add Builder Factory Docstrings in `pipeline.py`

### Goal
Document the implicit schema contracts (required session tables, output columns) for the 11 builder factory functions.

### Representative Code Snippets

```python
# src/semantics/pipeline.py
def _normalize_builder(
    spec: SemanticNormalizationSpec,
    context: _SemanticSpecContext,
) -> DataFrameBuilder:
    """Build a normalization DataFrame for the given spec.

    Requires the following tables registered in the SessionContext:
    - The input table specified by ``spec.input_name``
    - ``py_line_index`` (for line-to-byte normalization)

    Output schema includes: file_id, path, bstart, bend, entity_id, plus
    any columns from the normalization spec's field definitions.
    """
```

### Files to Edit
- `src/semantics/pipeline.py` — add docstrings to all 11 `_*_builder` factory functions

### New Files to Create
None.

### Legacy Decommission/Delete Scope
None.

---

## S19. Thread `model` Parameter Through All 4 IR Pipeline Functions

### Goal
Make `optimize_semantics`, `emit_semantics`, and `infer_semantics` accept `model: SemanticModel` as a parameter, matching the pattern already used by `compile_semantics`. This makes all four phases independently testable.

### Representative Code Snippets

```python
# src/semantics/ir_pipeline.py

def optimize_semantics(ir: SemanticIR, model: SemanticModel) -> SemanticIR:
    """Optimize semantic IR using model relationship specs."""
    rel_specs_by_name = {spec.name: spec for spec in model.relationship_specs}
    dataset_rows = _dataset_rows_for_model(model)
    ...

def emit_semantics(ir: SemanticIR, model: SemanticModel) -> SemanticIR:
    """Emit final semantic IR with dataset rows and model fingerprint."""
    dataset_rows = _dataset_rows_for_model(model)
    model_hash = semantic_model_fingerprint(model)
    ...

def infer_semantics(ir: SemanticIR, model: SemanticModel) -> SemanticIR:
    """Infer semantic properties from IR and model."""
    rel_specs_by_name = {spec.name: spec for spec in model.relationship_specs}
    ...

def build_semantic_ir(model: SemanticModel | None = None) -> SemanticIR:
    """Build the full semantic IR. Composition root for the model global."""
    if model is None:
        model = SEMANTIC_MODEL
    ir = compile_semantics(model)
    ir = infer_semantics(ir, model)
    ir = optimize_semantics(ir, model)
    ir = emit_semantics(ir, model)
    return ir
```

### Files to Edit
- `src/semantics/ir_pipeline.py` — add `model` parameter to 3 functions, update `build_semantic_ir`
- `src/semantics/pipeline.py` — update calls to pass `SEMANTIC_MODEL` explicitly

### New Files to Create
- `tests/unit/semantics/test_ir_pipeline_injection.py` — tests verifying IR phases work with injected model

### Legacy Decommission/Delete Scope
- Remove direct `SEMANTIC_MODEL` access from within `optimize_semantics`, `emit_semantics`, `infer_semantics`.

---

## S20. Add `IncrementalRuntime` Facade Methods

### Goal
Reduce 8+ deep `runtime.profile.policies.*` chain accesses to single-level facade calls.

### Representative Code Snippets

```python
# src/semantics/incremental/runtime.py
@dataclass
class IncrementalRuntime:
    ...

    def delta_service(self) -> DeltaService:
        """Return the Delta service from the runtime profile."""
        return self.profile.delta_ops.delta_service()

    def scan_policy(self) -> ScanPolicy:
        """Return the scan policy from the runtime profile."""
        return self.profile.policies.scan_policy

    def delta_store_policy(self) -> DeltaStorePolicy:
        """Return the Delta store policy."""
        return self.profile.policies.delta_store_policy

    def diagnostics_sink(self) -> DiagnosticsSink | None:
        """Return the diagnostics sink, or None if not configured."""
        return self.profile.diagnostics.diagnostics_sink

    def settings_hash(self) -> str:
        """Return the settings hash for reproducibility."""
        return self.profile.settings_hash()
```

### Files to Edit
- `src/semantics/incremental/runtime.py` — add facade methods
- `src/semantics/incremental/cdf_runtime.py` — replace `runtime.profile.policies.scan_policy` with `runtime.scan_policy()`
- `src/semantics/incremental/delta_context.py` — replace deep chains
- `src/semantics/incremental/metadata.py` — replace deep chains
- `src/semantics/incremental/plan_fingerprints.py` — replace deep chains

### New Files to Create
None.

### Legacy Decommission/Delete Scope
None (facade methods supplement, not replace, the `profile` attribute).

---

## S21. Add Cache Reset Hooks for Testability

### Goal
Enable test isolation for module-level caches in catalog modules.

### Representative Code Snippets

```python
# src/semantics/catalog/dataset_rows.py
def _reset_cache() -> None:
    """Reset the dataset rows cache. For testing only."""
    _SEMANTIC_DATASET_ROWS_CACHE.rows = None

# src/semantics/catalog/dataset_specs.py
def _reset_cache() -> None:
    """Reset the dataset specs cache. For testing only."""
    _CACHE.dataset_specs = None
    _CACHE.dataset_rows = None
    # If alias caches still exist pre-S15, clear them here as well.
```

### Files to Edit
- `src/semantics/catalog/dataset_rows.py` — add `_reset_cache()`
- `src/semantics/catalog/dataset_specs.py` — add `_reset_cache()`
- `tests/unit/semantics/catalog/test_dataset_rows.py` — add cache-reset isolation tests
- `tests/unit/semantics/catalog/test_dataset_specs.py` — add cache-reset isolation tests

### New Files to Create
None (extend existing catalog tests).

### Legacy Decommission/Delete Scope
None.

---

## S22. Consolidate Duplicate `CdfReadResult` Types

### Goal
Designate a single canonical public `CdfReadResult` contract and remove naming collisions in runtime internals. This is the contract-level prerequisite for the broader CDF read-flow consolidation in S41.

### Representative Code Snippets

```python
# src/semantics/incremental/cdf_runtime.py — runtime-local type is explicit
@dataclass(frozen=True)
class RuntimeCdfReadResult:  # was: CdfReadResult
    """Result for a runtime Delta CDF read (pa.Table form)."""
    table: pa.Table
    updated_version: int

# src/semantics/incremental/cdf_reader.py — canonical exported shape
@dataclass(frozen=True)
class CdfReadResult:
    df: DataFrame
    start_version: int
    end_version: int
    has_changes: bool

# src/semantics/incremental/__init__.py — export only canonical CdfReadResult
from semantics.incremental.cdf_reader import CdfReadResult
# RuntimeCdfReadResult is internal to cdf_runtime.py
```

### Files to Edit
- `src/semantics/incremental/cdf_reader.py` — confirm canonical public `CdfReadResult`
- `src/semantics/incremental/cdf_runtime.py` — rename runtime-local type to `RuntimeCdfReadResult`
- `src/semantics/incremental/__init__.py` — ensure only canonical `CdfReadResult` is publicly exported
- All callers of `cdf_runtime.CdfReadResult` — update to `RuntimeCdfReadResult`

### New Files to Create
None.

### Legacy Decommission/Delete Scope
- Delete the `CdfReadResult` name from `src/semantics/incremental/cdf_runtime.py` (replaced by `RuntimeCdfReadResult`).

---

## S23. Eliminate Duplicate Compile-Resolution in `build_cpg_from_inferred_deps`

### Goal
Prevent double `_resolve_cpg_compile_artifacts` computation while preserving the existing public `build_cpg(...)` return type.

### Representative Code Snippets

```python
# src/semantics/pipeline.py

@dataclass(frozen=True)
class _ResolvedBuildInputs:
    """Internal bundle reused by build_cpg entry points."""
    compile_artifacts: _CpgCompileArtifacts
    semantic_ir: SemanticIR
    requested_outputs: tuple[str, ...]
    manifest: SemanticProgramManifest


def _resolve_build_inputs(...) -> _ResolvedBuildInputs:
    artifacts = _resolve_cpg_compile_artifacts(...)
    semantic_ir = _semantic_ir_for_outputs(...)
    return _ResolvedBuildInputs(
        compile_artifacts=artifacts,
        semantic_ir=semantic_ir,
        requested_outputs=requested_outputs,
        manifest=manifest,
    )


def build_cpg(...) -> ...:  # unchanged public contract
    ...
    resolved = _resolve_build_inputs(...)
    ...
    return view_nodes

def build_cpg_from_inferred_deps(...) -> ...:
    resolved = _resolve_build_inputs(...)
    # Reuse resolved.compile_artifacts instead of recomputing
    ...
```

### Files to Edit
- `src/semantics/pipeline.py` — introduce internal shared resolver and remove duplicate artifact-resolution call

### New Files to Create
None.

### Legacy Decommission/Delete Scope
- Delete the second `_resolve_cpg_compile_artifacts` call at `pipeline.py:2114`.

---

## S24. Extract Shared Normalization Helpers

### Goal
Create a single source of truth for the `_byte_offset` computation and line-index join pattern used by both `span_normalize.py` and `scip_normalize.py`.

### Representative Code Snippets

```python
# src/semantics/normalization_helpers.py (NEW)
from __future__ import annotations

from datafusion import col
from datafusion.expr import Expr
import pyarrow as pa
from datafusion_engine.udf.catalog import udf_expr

__all__ = [
    "byte_offset_expr",
    "line_index_join",
]


def byte_offset_expr(
    line_start_col: str,
    line_text_col: str,
    char_col: str,
    unit_expr: Expr,
) -> Expr:
    """Compute a byte offset from line-start + character column using col_to_byte UDF."""
    base = col(line_start_col).cast(pa.int64())
    char_val = col(char_col).cast(pa.int64())
    offset = udf_expr("col_to_byte", col(line_text_col), char_val, unit_expr)
    guard = col(line_start_col).is_null() | col(line_text_col).is_null() | col(char_col).is_null()
    return case(guard).when(True, lit(None).cast(pa.int64())).otherwise(base + offset)


def line_index_join(
    df: DataFrame,
    line_index_table: str,
    *,
    start_line_col: str,
    end_line_col: str,
    ctx: SessionContext,
) -> DataFrame:
    """Join a DataFrame with line index for start and end line resolution."""
    ...
```

### Files to Edit
- `src/semantics/span_normalize.py` — import and use `byte_offset_expr` and `line_index_join`
- `src/semantics/scip_normalize.py` — import and use `byte_offset_expr` and `line_index_join`

### New Files to Create
- `src/semantics/normalization_helpers.py` — shared helpers
- `tests/unit/semantics/test_normalization_helpers.py` — unit tests

### Legacy Decommission/Delete Scope
- Delete the nested `_byte_offset` function from `src/semantics/span_normalize.py:215-222`.
- Delete the nested `_byte_offset` function from `src/semantics/scip_normalize.py:151-161`.
- Delete duplicated line-index alias/join logic from both files.

---

## S25. Extract Code-Unit Join Helper in `analysis_builders.py`

### Goal
Eliminate 40+ lines of identical code between `cfg_blocks_df_builder` and `cfg_edges_df_builder`.

### Representative Code Snippets

```python
# src/semantics/catalog/analysis_builders.py
def _join_with_code_units(
    primary_df: DataFrame,
    ctx: SessionContext,
    *,
    join_col: str = "code_unit_id",
) -> DataFrame:
    """Left-join a DataFrame with code_units and coalesce file_id/path."""
    code_units = ctx.table("py_bc_code_units").select(
        col("code_unit_id"),
        col("file_id").alias("cu_file_id"),
        col("path").alias("cu_path"),
    )
    joined = primary_df.join(code_units, join_keys=([join_col], ["code_unit_id"]), how="left")
    return joined.with_columns(
        coalesce(col("file_id"), col("cu_file_id")).alias("file_id"),
        coalesce(col("path"), col("cu_path")).alias("path"),
    ).drop("cu_file_id", "cu_path")
```

### Files to Edit
- `src/semantics/catalog/analysis_builders.py` — extract helper, update both builders

### New Files to Create
None.

### Legacy Decommission/Delete Scope
- Delete duplicated code-unit join logic from `cfg_blocks_df_builder` and `cfg_edges_df_builder`.

---

## S26. Consolidate `DatasetRegistrySpec` into `SemanticDatasetRow`

### Goal
Eliminate the twin-dataclass problem where 20 fields are duplicated across two types with a mechanical copy function bridging them.

### Representative Code Snippets

```python
# Sub-registries declare SemanticDatasetRow directly:
# src/semantics/catalog/normalize_registry.py
NORMALIZE_DATASETS: Final[tuple[SemanticDatasetRow, ...]] = (
    SemanticDatasetRow(
        name="py_cst_normalize",
        version=1,
        category="normalize",
        ...
    ),
)

# src/semantics/ir_pipeline.py — remove _row_from_registry_spec conversion
# Rows are now directly SemanticDatasetRow, no conversion needed.
```

### Files to Edit
- `src/semantics/catalog/dataset_registry.py` — delete `DatasetRegistrySpec` class
- `src/semantics/catalog/normalize_registry.py` — use `SemanticDatasetRow` directly
- `src/semantics/catalog/diagnostics_registry.py` — use `SemanticDatasetRow` directly
- `src/semantics/catalog/export_registry.py` — use `SemanticDatasetRow` directly
- `src/semantics/catalog/semantic_singletons_registry.py` — use `SemanticDatasetRow` directly
- `src/semantics/ir_pipeline.py` — delete `_row_from_registry_spec` function

### New Files to Create
None.

### Legacy Decommission/Delete Scope
- Delete `DatasetRegistrySpec` class from `src/semantics/catalog/dataset_registry.py`.
- Delete `_row_from_registry_spec()` function from `src/semantics/ir_pipeline.py:319-349`.

---

## S27. Extract Shared Write Helper for `metadata.py`

### Goal
Reduce 80+ LOC of near-identical Delta write orchestration in 3 `_write_*` functions to a single parameterized helper.

### Representative Code Snippets

```python
# src/semantics/incremental/metadata.py
def _write_named_artifact(
    name: str,
    path: Path,
    table: pa.Table,
    context: DeltaAccessContext,
    *,
    mode: WriteMode = WriteMode.OVERWRITE,
) -> None:
    """Write a named artifact as a Delta table."""
    write_delta_table_via_pipeline(
        table=table,
        path=str(path),
        name=name,
        context=context,
        mode=mode,
    )

# Then each specific writer delegates:
def _write_artifact_table(...) -> None:
    table = _build_artifact_pa_table(...)
    _write_named_artifact(name, path, table, context)
```

### Files to Edit
- `src/semantics/incremental/metadata.py` — extract helper, refactor 3 callers

### New Files to Create
None.

### Legacy Decommission/Delete Scope
- Delete duplicated write orchestration from `_write_artifact_rows` and `_write_view_artifact_rows` (bodies replaced by `_write_named_artifact` calls).

---

## S28. Move `CdfCursorStore` Type to Shared Location

### Goal
Fix the dependency direction violation where `datafusion_engine/session/runtime.py` imports from `semantics.incremental`.

### Representative Code Snippets

```python
# Option: define a protocol in a shared location
# src/core_types/cdf_cursor.py (or src/storage/cdf_cursor_protocol.py)
from __future__ import annotations
from typing import Protocol

class CdfCursorStoreLike(Protocol):
    """Protocol for CDF cursor stores."""
    def load_cursor(self, dataset_name: str) -> CdfCursor | None: ...
    def save_cursor(self, dataset_name: str, cursor: CdfCursor) -> None: ...

# src/datafusion_engine/session/runtime.py — import protocol instead
from core_types.cdf_cursor import CdfCursorStoreLike
```

### Files to Edit
- `src/datafusion_engine/session/runtime.py` — use protocol instead of concrete import
- `src/semantics/incremental/cdf_cursors.py` — ensure `CdfCursorStore` satisfies protocol

### New Files to Create
- Location TBD for `CdfCursorStoreLike` protocol (could be `src/storage/cdf_cursor_protocol.py`)
- `tests/unit/test_cdf_cursor_protocol.py` — protocol compliance test

### Legacy Decommission/Delete Scope
- Delete `from semantics.incremental.cdf_cursors import CdfCursorStore` from `src/datafusion_engine/session/runtime.py`.

---

## S29. Move `RELATION_OUTPUT_NAME` to `semantics/` Shared Constants

### Goal
Eliminate reverse dependencies from `semantics.*` to `relspec` for relation-output constants.

### Representative Code Snippets

```python
# src/semantics/output_names.py (NEW)
from __future__ import annotations
from typing import Final

RELATION_OUTPUT_NAME: Final[str] = "relation_output"
RELATION_OUTPUT_ORDERING_KEYS: Final[tuple[tuple[str, str], ...]] = (...)

__all__ = ["RELATION_OUTPUT_NAME", "RELATION_OUTPUT_ORDERING_KEYS"]

# src/semantics/catalog/semantic_singletons_registry.py — update import
from semantics.output_names import RELATION_OUTPUT_NAME, RELATION_OUTPUT_ORDERING_KEYS

# src/relspec/contracts.py (or wherever it was) — update import
from semantics.output_names import RELATION_OUTPUT_ORDERING_KEYS
```

### Files to Edit
- `src/semantics/catalog/semantic_singletons_registry.py` — update imports
- `src/semantics/registry.py` — update local import usage
- `src/semantics/pipeline.py` — update local import usage
- `src/semantics/ir_pipeline.py` — update local import usage
- `src/relspec/view_defs.py` — import/re-export moved constants
- `src/relspec/contracts.py` — import moved constants
- `src/relspec/__init__.py` — keep lazy-export map aligned

### New Files to Create
- `src/semantics/output_names.py` — shared constants

### Legacy Decommission/Delete Scope
- Delete direct constant definitions from `src/relspec/view_defs.py` and `src/relspec/contracts.py` (moved to `src/semantics/output_names.py`).
- Delete `from relspec.view_defs import RELATION_OUTPUT_NAME` imports from semantics modules.

---

## S30. Simplify Join Inference Dispatch via Strategy-Resolver Table

### Goal
Replace duplicated if/elif cascades in `_infer_with_hint` and `_infer_default` with a data-driven dispatch table.

### Representative Code Snippets

```python
# src/semantics/joins/inference.py
_STRATEGY_RESOLVERS: tuple[
    tuple[
        JoinStrategyType,
        Callable[[JoinCapabilities, JoinCapabilities, AnnotatedSchema, AnnotatedSchema], JoinStrategy | None],
    ],
    ...,
] = (
    (
        JoinStrategyType.SPAN_OVERLAP,
        lambda lc, rc, ls, rs: replace(
            _span_strategy(JoinStrategyType.SPAN_OVERLAP, ls, rs),
            confidence=_SPAN_CONFIDENCE,
        )
        if _can_span_join(lc, rc)
        else None,
    ),
    (
        JoinStrategyType.SPAN_CONTAINS,
        lambda lc, rc, ls, rs: replace(
            _span_strategy(JoinStrategyType.SPAN_CONTAINS, ls, rs),
            confidence=_SPAN_CONFIDENCE,
        )
        if _can_span_join(lc, rc)
        else None,
    ),
    (
        JoinStrategyType.FOREIGN_KEY,
        lambda lc, rc, _ls, _rs: (
            replace(make_fk_strategy(match[0], match[1]), confidence=_FK_CONFIDENCE)
            if (match := _find_fk_match(lc, rc))
            else None
        ),
    ),
    (
        JoinStrategyType.SYMBOL_MATCH,
        lambda lc, rc, ls, rs: (
            replace(strategy, confidence=_SYMBOL_CONFIDENCE)
            if (strategy := _resolve_symbol_match(lc, rc, left_schema=ls, right_schema=rs)) is not None
            else None
        ),
    ),
    (
        JoinStrategyType.EQUI_JOIN,
        lambda lc, rc, ls, rs: (
            replace(strategy, confidence=_FILE_EQUI_CONFIDENCE)
            if (strategy := _resolve_equi_join(lc, rc, left_schema=ls, right_schema=rs)) is not None
            else None
        ),
    ),
)

def _infer_default(
    left_caps: JoinCapabilities,
    right_caps: JoinCapabilities,
    *,
    left_schema: AnnotatedSchema,
    right_schema: AnnotatedSchema,
) -> JoinStrategy | None:
    for _strategy_type, resolver in _STRATEGY_RESOLVERS:
        result = resolver(left_caps, right_caps, left_schema, right_schema)
        if result is not None:
            return result
    return None
```

### Files to Edit
- `src/semantics/joins/inference.py` — extract resolver table, simplify both functions

### New Files to Create
None.

### Legacy Decommission/Delete Scope
- Delete the if/elif cascades in `_infer_with_hint` and `_infer_default` (replaced by table iteration).

---

## S31. Extract CDF Resolution from `pipeline.py`

### Goal
Move ~130 LOC of CDF-related functions and protocols to a dedicated module.

### Representative Code Snippets

```python
# src/semantics/cdf_resolution.py (NEW)
from __future__ import annotations

__all__ = [
    "cdf_changed_inputs",
    "resolve_cdf_location",
]

# Move these functions from pipeline.py:
# - _CdfCursorLike, _CdfCursorStoreLike, _DeltaServiceLike (protocols)
# - _cdf_changed_inputs -> cdf_changed_inputs (make public)
# - _resolve_cdf_location -> resolve_cdf_location
# - _input_has_cdf_changes
# - _cdf_enabled_for_location
# - _outputs_from_changed_inputs
# - _views_downstream_of_inputs
```

### Files to Edit
- `src/semantics/pipeline.py` — move functions out, import from new module

### New Files to Create
- `src/semantics/cdf_resolution.py` — CDF functions and protocols
- `tests/unit/semantics/test_cdf_resolution.py` — unit tests

### Legacy Decommission/Delete Scope
- Delete CDF functions (lines 649-776) from `src/semantics/pipeline.py`.

---

## S32. Extract Output Materialization from `pipeline.py`

### Goal
Move ~215 LOC of Delta-write materialization logic to a dedicated module.

### Files to Edit
- `src/semantics/pipeline.py` — move functions out

### New Files to Create
- `src/semantics/output_materialization.py` — materialization functions
- `tests/unit/semantics/test_output_materialization.py` — unit tests

### Legacy Decommission/Delete Scope
- Delete materialization functions (lines 1635-1849) from `src/semantics/pipeline.py`.

---

## S33. Extract Diagnostics Emission from `pipeline.py`

### Goal
Move ~200 LOC of diagnostics context and emission to a dedicated module.

### Files to Edit
- `src/semantics/pipeline.py` — move `_SemanticDiagnosticsContext` and emission functions

### New Files to Create
- `src/semantics/diagnostics_emission.py` — diagnostics context and emission
- `tests/unit/semantics/test_diagnostics_emission.py` — unit tests

### Legacy Decommission/Delete Scope
- Delete diagnostics emission functions (lines 1851-2050) from `src/semantics/pipeline.py`.
- Delete `_SemanticDiagnosticsContext` class from `src/semantics/pipeline.py`.

---

## S34. Split `diagnostics.py` into Package

### Goal
Decompose the 773 LOC monolith into focused modules with an **atomic file-to-package cutover** (avoid intermediate states where both `diagnostics.py` and `diagnostics/` coexist).

### New Files to Create
- `src/semantics/diagnostics/__init__.py` — package entrypoint and re-exports
- `src/semantics/diagnostics/quality_metrics.py` — relationship quality metrics
- `src/semantics/diagnostics/coverage.py` — file coverage report
- `src/semantics/diagnostics/ambiguity.py` — ambiguity analysis
- `src/semantics/diagnostics/issue_batching.py` — issue batch extraction
- `src/semantics/diagnostics/schema_anomalies.py` — schema anomaly detection
- `tests/unit/semantics/diagnostics/test_quality_metrics.py`
- `tests/unit/semantics/diagnostics/test_coverage.py`

### Legacy Decommission/Delete Scope
- Delete `src/semantics/diagnostics.py` in the same change that introduces `src/semantics/diagnostics/` package.
- Ensure imports are switched atomically to package paths (`from semantics.diagnostics import ...` remains valid via package `__init__.py`).

---

## S35. Extract Quality-Relationship Compilation from `compiler.py`

### Goal
Move the ~564 LOC quality-relationship compilation pathway into a dedicated module.

### New Files to Create
- `src/semantics/quality_compiler.py` — quality-relationship compilation
- `tests/unit/semantics/test_quality_compiler.py` — unit tests

### Files to Edit
- `src/semantics/compiler.py` — move lines 1254-1818, import from new module

### Legacy Decommission/Delete Scope
- Delete quality-relationship compilation code (lines 1254-1818) from `src/semantics/compiler.py`.

---

## S36. Unify `ColumnType`/`SemanticType` Classification

### Goal
Designate `SemanticType` as the canonical classification and derive `ColumnType` categories as computed groupings.

### Files to Edit
- `src/semantics/column_types.py` — replace pattern-matching with `SemanticType`-derived groupings
- `src/semantics/types/core.py` — becomes the single source of pattern matching
- `src/semantics/schema.py` — update to use unified classification

### New Files to Create
None.

### Legacy Decommission/Delete Scope
- Delete `TYPE_PATTERNS` from `src/semantics/column_types.py` (replaced by `SemanticType` inference).
- Delete `infer_column_type` pattern-matching logic (replaced by thin wrapper over `SemanticType`).

---

## S37. Extract `SemanticExprBuilder` from `SemanticSchema`

### Goal
Separate DataFusion expression generation (lines 470-567) from the pure schema discovery class.

### New Files to Create
- `src/semantics/expr_builder.py` — `SemanticExprBuilder` class
- `tests/unit/semantics/test_expr_builder.py` — unit tests

### Files to Edit
- `src/semantics/schema.py` — move expression methods, retain delegating wrappers during migration

### Legacy Decommission/Delete Scope
- Expression generation methods on `SemanticSchema` become delegating wrappers (not deleted immediately).

---

## S38. Introduce `DeltaCdfPort` Protocol

### Goal
Abstract Delta Lake operations behind a port for testability and vendor isolation.

### New Files to Create
- `src/semantics/incremental/delta_port.py` — `DeltaCdfPort` protocol
- `tests/unit/semantics/incremental/test_delta_port.py` — protocol compliance test

### Files to Edit
- `src/semantics/incremental/cdf_runtime.py` — depend on port instead of direct `deltalake` calls
- `src/semantics/incremental/cdf_reader.py` — formalize callable injection into port

### Legacy Decommission/Delete Scope
- Delete direct `deltalake.DeltaTable` instantiation from `cdf_runtime.py:152-198`.

---

## S39. Split `analysis_builders.py` by Domain

### Goal
Decompose the 740 LOC file with 14 builders across 6 domains into focused modules.

### New Files to Create
- `src/semantics/catalog/type_builders.py` — type expression builders
- `src/semantics/catalog/cfg_builders.py` — CFG block/edge builders
- `src/semantics/catalog/def_use_builders.py` — def-use event builders
- `src/semantics/catalog/diagnostic_builders.py` — diagnostic/quality builders
- `src/semantics/catalog/builder_exprs.py` — shared expression helpers (`_stable_id_expr`, `_span_expr`, etc.)

### Files to Edit
- `src/semantics/catalog/analysis_builders.py` — becomes thin re-export facade

### Legacy Decommission/Delete Scope
- Delete builder function bodies from `analysis_builders.py` (retained as re-exports only).

---

## S40. Add `stage_span` to `build_file_quality_view` in `signals.py`

*(Merged into S11 scope — retained only for historical tracker continuity; no standalone implementation work.)*

---

## S41. Canonicalize CDF Read Architecture Across `cdf_reader.py` and `cdf_runtime.py`

### Goal
Establish a single canonical CDF read flow and remove duplicated orchestration logic (`read_cdf_changes`, version-range resolution, and fallback handling) across incremental modules.

### Representative Code Snippets

```python
# src/semantics/incremental/cdf_core.py (NEW)
@dataclass(frozen=True)
class CanonicalCdfReadResult:
    table: pa.Table
    start_version: int
    end_version: int
    has_changes: bool


def read_cdf_table(...) -> CanonicalCdfReadResult | None:
    # shared version resolution + provider path + fallback path
    ...


# src/semantics/incremental/cdf_reader.py
# Adapts CanonicalCdfReadResult -> DataFrame-oriented CdfReadResult

# src/semantics/incremental/cdf_runtime.py
# Adapts CanonicalCdfReadResult -> runtime-oriented RuntimeCdfReadResult
```

### Files to Edit
- `src/semantics/incremental/cdf_reader.py` — delegate to canonical CDF core
- `src/semantics/incremental/cdf_runtime.py` — delegate to canonical CDF core
- `src/semantics/incremental/__init__.py` — keep exports consistent with canonical surface

### New Files to Create
- `src/semantics/incremental/cdf_core.py` — shared CDF read orchestration
- `tests/unit/semantics/incremental/test_cdf_core.py` — shared behavior tests

### Legacy Decommission/Delete Scope
- Delete duplicated CDF orchestration helpers from `cdf_reader.py` and `cdf_runtime.py` once delegated to `cdf_core.py`.

---

## S42. Make CDF Reads Object-Store Safe and Push Filter Policies into `load_cdf`

### Goal
Fix CDF runtime behavior for remote/object-store Delta paths and align filtering with Delta/DataFusion pushdown capabilities.

### Representative Code Snippets

```python
# src/semantics/incremental/cdf_runtime.py
def _resolve_cdf_inputs(...) -> CdfReadInputs | None:
    # Remove local-filesystem existence checks:
    # if not Path(dataset_path).exists(): return None
    current_version = runtime.profile.delta_ops.delta_service().table_version(
        path=dataset_path,
        storage_options=resolved_storage,
        log_storage_options=resolved_log_storage,
    )
    if current_version is None:
        return None
    ...


def _prepare_cdf_read_state(...) -> _CdfReadState | None:
    policy = filter_policy or CdfFilterPolicy.include_all()
    cdf_options = DeltaCdfOptions(
        starting_version=starting_version,
        ending_version=current_version,
        predicate=policy.to_sql_predicate(),
        allow_out_of_range=...,
    )
    ...
```

### Files to Edit
- `src/semantics/incremental/cdf_runtime.py` — remove `Path(...).exists()` gating, push predicate into `DeltaCdfOptions`
- `src/semantics/incremental/cdf_reader.py` — keep predicate flow consistent with canonical path
- `src/semantics/incremental/cdf_types.py` — ensure `to_sql_predicate()` contract remains canonical

### New Files to Create
- `tests/unit/semantics/incremental/test_cdf_runtime.py` — object-store path + predicate pushdown tests

### Legacy Decommission/Delete Scope
- Delete local-filesystem existence check for CDF readiness in `cdf_runtime.py`.
- Delete manual post-read `_change_type` filtering branch in fallback path where SQL predicate pushdown already applies.

---

## S43. Remove Remaining `SEMANTIC_MODEL` Global Access from `pipeline.py` Call Paths

### Goal
Finish model injection migration by removing `SEMANTIC_MODEL` global reads in pipeline helpers that still bypass explicit model dependencies.

### Representative Code Snippets

```python
# src/semantics/pipeline.py
def _semantic_output_view_names(
    *,
    model: SemanticModel,
    requested_outputs: Collection[str] | None = None,
    manifest: SemanticProgramManifest,
) -> list[str]:
    if requested_outputs is None:
        view_names = [spec.name for spec in model.outputs]
        ...
    ...
```

### Files to Edit
- `src/semantics/pipeline.py` — thread `model: SemanticModel` through remaining global-lookup helpers
- `src/semantics/registry.py` — keep global model as compatibility entrypoint only (not internal dependency)

### New Files to Create
None.

### Legacy Decommission/Delete Scope
- Delete pipeline-local `from semantics.registry import SEMANTIC_MODEL` dependency once all helpers accept explicit model args.

---

## S44. Add Public API Contract-Lock Tests for Semantics Surface

### Goal
Guard against accidental breaking changes to exported symbols/signatures during refactors (S1, S2, S22, S23, S29, S34).

### Representative Code Snippets

```python
# tests/unit/semantics/test_public_api_contracts.py
def test_semantics_module_exports_expected_symbols() -> None:
    import semantics

    expected = {
        "build_cpg",
        "build_cpg_from_inferred_deps",
        "CpgBuildOptions",
    }
    assert expected.issubset(set(semantics.__all__))


def test_build_cpg_signature_is_stable() -> None:
    sig = inspect.signature(semantics.build_cpg)
    assert "ctx" in sig.parameters
```

### Files to Edit
- `src/semantics/__init__.py` — ensure explicit stable exports

### New Files to Create
- `tests/unit/semantics/test_public_api_contracts.py` — export/signature lock tests

### Legacy Decommission/Delete Scope
None.

---

## S45. Expand Incremental Runtime Unit Test Coverage

### Goal
Add direct tests for incremental modules currently under-covered (`cdf_runtime.py`, `delta_context.py`, `metadata.py`) to reduce regression risk for S20/S22/S27/S41/S42.

### Representative Code Snippets

```python
# tests/unit/semantics/incremental/test_cdf_runtime.py
def test_read_cdf_changes_uses_sql_predicate_pushdown(...) -> None:
    ...


# tests/unit/semantics/incremental/test_delta_context.py
def test_resolve_storage_prefers_profile_location_options(...) -> None:
    ...


# tests/unit/semantics/incremental/test_metadata.py
def test_write_named_artifact_shared_helper_used_by_all_writers(...) -> None:
    ...
```

### Files to Edit
- `src/semantics/incremental/cdf_runtime.py` — add test seams only where required
- `src/semantics/incremental/delta_context.py` — add seam points for deterministic unit tests
- `src/semantics/incremental/metadata.py` — expose helper boundaries for unit-level assertions

### New Files to Create
- `tests/unit/semantics/incremental/test_cdf_runtime.py`
- `tests/unit/semantics/incremental/test_delta_context.py`
- `tests/unit/semantics/incremental/test_metadata.py`

### Legacy Decommission/Delete Scope
None.

---

## Cross-Scope Legacy Decommission and Deletion Plan

### Batch D1 (after S9, S11)
- Delete `_table_exists` from `src/semantics/diagnostics.py` and `src/semantics/signals.py` — replaced by `table_names_snapshot`.

### Batch D2 (after S15, S26)
- Delete `DatasetRegistrySpec` from `src/semantics/catalog/dataset_registry.py` — replaced by direct `SemanticDatasetRow` usage.
- Delete `_row_from_registry_spec()` from `src/semantics/ir_pipeline.py` — no longer needed.
- Delete `_get_alias_maps()` from `src/semantics/catalog/dataset_specs.py` — identity mapping removed.

### Batch D3 (after S31, S32, S33)
- Delete CDF resolution functions (lines 649-776), materialization functions (lines 1635-1849), and diagnostics emission functions (lines 1851-2050) from `src/semantics/pipeline.py` — all moved to dedicated modules.

### Batch D4 (after S34)
- Delete `src/semantics/diagnostics.py` — replaced atomically by `src/semantics/diagnostics/` package.

### Batch D5 (after S35)
- Delete quality-relationship compilation code (lines 1254-1818) from `src/semantics/compiler.py` — moved to `quality_compiler.py`.

### Batch D6 (after S36)
- Delete `TYPE_PATTERNS` and pattern-matching logic from `src/semantics/column_types.py` — replaced by `SemanticType`-derived classification.

### Batch D7 (after S14)
- Conditional delete `src/semantics/metrics.py`, `src/semantics/stats/collector.py`, and `src/semantics/stats/__init__.py` only if S14 audit gates pass.

### Batch D8 (after S22, S41, S42)
- Delete duplicate CDF read orchestration in `cdf_reader.py` / `cdf_runtime.py` (duplicate entry points, duplicate fallback loaders, duplicate post-read filtering).

### Batch D9 (after S29, S43)
- Delete residual `relspec` constant imports from semantics modules and pipeline-local `SEMANTIC_MODEL` imports in favor of injected model + shared `semantics.output_names`.

---

## Implementation Sequence

**Phase 1: Quick Wins (S1-S18)** — Small, independently deployable changes.

1. **S1** — Rename `_cpg_view_specs`, audit `__all__`. Zero-risk naming fix.
2. **S8** — Extract `_TABLE_TYPE_GRAIN_MAP`. Single-file dedup.
3. **S9** — Standardize `_table_exists` on `table_names_snapshot`. Mechanical replacement.
4. **S10** — Add logging to silent handlers. Non-breaking addition.
5. **S2** — Rename `compiler.get()`. Update all callers in one pass.
6. **S3** — Remove dead `_builder_for_artifact_spec`. Dead code cleanup.
7. **S4** — Add `JoinStrategy.__post_init__`. Contract tightening.
8. **S5** — Add `SemanticIncrementalConfig.__post_init__`. Contract tightening.
9. **S6** — Add `require_unambiguous_spans()`. Encapsulation fix.
10. **S7** — Fix double-inference. Correctness fix.
11. **S11** — Add `stage_span` to signals/diagnostics. Observability.
12. **S12** — Add join inference logging. Observability.
13. **S13** — Normalize cache policy at boundary. Parse-don't-validate.
14. **S14** — Audit + gate deletion decision for `metrics.py`/`stats`. Evidence-first cleanup.
15. **S15** — Simplify alias system + remove `ctx`. KISS/YAGNI.
16. **S16** — Simplify `fingerprints.py`. KISS cleanup.
17. **S17** — `naming.py` type safety. Protocol improvement.
18. **S18** — Builder factory docstrings. Documentation.

**Phase 2: Medium Refactors (S19-S30, S41-S43, S45)** — Higher impact, integrate with explicit dependency ordering.

19. **S19** — Thread `model` through IR pipeline. Highest-ROI testability fix. *Do first in Phase 2.*
20. **S22** — Consolidate `CdfReadResult` contracts. API clarity prerequisite for CDF unification.
21. **S41** — Canonicalize CDF read architecture. Single shared flow.
22. **S42** — Object-store-safe CDF + predicate pushdown. Correctness and performance.
23. **S20** — `IncrementalRuntime` facade methods. Law of Demeter fix.
24. **S23** — Eliminate duplicate compile-resolution without public API churn.
25. **S24** — Extract normalization helpers. DRY in normalization.
26. **S25** — Extract code-unit join helper. DRY in catalog.
27. **S21** — Cache reset hooks for testability.
28. **S27** — Metadata write helper. DRY in incremental.
29. **S28** — Move `CdfCursorStore` to shared location. Dependency direction.
30. **S29** — Move `RELATION_OUTPUT_NAME`. Dependency direction.
31. **S30** — Strategy-resolver table. KISS in join inference.
32. **S43** — Remove remaining `SEMANTIC_MODEL` global pipeline reads.
33. **S26** — Consolidate `DatasetRegistrySpec`. Largest DRY fix in catalog. *Do late — depends on S15, S21.*
34. **S45** — Expand incremental runtime unit coverage. Lock behavior before large decompositions.

**Phase 3: Structural Decomposition (S31-S39)** — Large changes, should be done incrementally.

35. **S31** — Extract CDF resolution from `pipeline.py`. Most self-contained extraction.
36. **S32** — Extract output materialization. Second extraction.
37. **S33** — Extract diagnostics emission. Third extraction.
38. **S34** — Split `diagnostics.py` into package with atomic cutover.
39. **S35** — Extract quality-relationship compilation from `compiler.py`.
40. **S36** — Unify type classification.
41. **S37** — Extract `SemanticExprBuilder` after S36.
42. **S38** — Introduce `DeltaCdfPort` protocol.
43. **S39** — Split `analysis_builders.py` by domain (largest decomposition; do last).

**Phase 4: Contract Hardening (S44)** — Final guardrails before broad rollout.

44. **S44** — Add public API contract-lock tests.

**Tracker Note**

45. **S40** — Tracker-only item merged into S11 (no standalone implementation step).

---

## Implementation Checklist

### Phase 1: Quick Wins
- [ ] S1. Rename `_cpg_view_specs`, audit `__all__`
- [ ] S2. Rename `compiler.get()` to `get_or_register()`
- [ ] S3. Remove dead `_builder_for_artifact_spec` + fix docstring
- [ ] S4. Add `JoinStrategy.__post_init__`
- [ ] S5. Add `SemanticIncrementalConfig.__post_init__`
- [ ] S6. Add `require_unambiguous_spans()` to `SemanticSchema`
- [ ] S7. Fix double-inference in compiler join fallback
- [ ] S8. Extract `_TABLE_TYPE_GRAIN_MAP` in `tags.py`
- [ ] S9. Standardize `_table_exists` on `table_names_snapshot`
- [ ] S10. Add logging to silent exception handlers
- [ ] S11. Add `stage_span` to signals/diagnostics builders
- [ ] S12. Add structured logging to join inference
- [ ] S13. Normalize `compiled_cache_policy` at boundary
- [ ] S14. Audit and gate deletion decision for `metrics.py` and `stats/collector.py`
- [ ] S15. Simplify alias system + remove `ctx` parameter
- [ ] S16. Simplify `fingerprints.py` defensive chains
- [ ] S17. Improve `naming.py` type safety
- [ ] S18. Add builder factory docstrings

### Phase 2: Medium Refactors
- [ ] S19. Thread `model` parameter through IR pipeline
- [ ] S22. Consolidate duplicate `CdfReadResult` contracts
- [ ] S41. Canonicalize CDF read architecture
- [ ] S42. Make CDF reads object-store safe + push predicate pushdown
- [ ] S20. Add `IncrementalRuntime` facade methods
- [ ] S21. Add cache reset hooks for testability
- [ ] S23. Eliminate duplicate compile-resolution
- [ ] S24. Extract shared normalization helpers
- [ ] S25. Extract code-unit join helper
- [ ] S27. Extract shared write helper for `metadata.py`
- [ ] S28. Move `CdfCursorStore` to shared location
- [ ] S29. Move `RELATION_OUTPUT_NAME` to shared constants
- [ ] S30. Simplify join inference dispatch
- [ ] S43. Remove remaining `SEMANTIC_MODEL` global access in `pipeline.py`
- [ ] S26. Consolidate `DatasetRegistrySpec` into `SemanticDatasetRow`
- [ ] S45. Expand incremental runtime unit test coverage

### Phase 3: Structural Decomposition
- [ ] S31. Extract CDF resolution from `pipeline.py`
- [ ] S32. Extract output materialization from `pipeline.py`
- [ ] S33. Extract diagnostics emission from `pipeline.py`
- [ ] S34. Split `diagnostics.py` into package
- [ ] S35. Extract quality-relationship compilation from `compiler.py`
- [ ] S36. Unify `ColumnType`/`SemanticType` classification
- [ ] S37. Extract `SemanticExprBuilder` from `SemanticSchema`
- [ ] S38. Introduce `DeltaCdfPort` protocol
- [ ] S39. Split `analysis_builders.py` by domain

### Phase 4: Contract Hardening
- [ ] S44. Add semantics public API contract-lock tests

### Tracker
- [ ] S40. Tracker-only item acknowledged (merged into S11; no implementation)

### Decommission Batches
- [ ] D1. Delete `_table_exists` duplicates (after S9, S11)
- [ ] D2. Delete `DatasetRegistrySpec` + alias system (after S15, S26)
- [ ] D3. Delete extracted pipeline.py functions (after S31, S32, S33)
- [ ] D4. Delete `diagnostics.py` monolith (after S34)
- [ ] D5. Delete quality-compilation from `compiler.py` (after S35)
- [ ] D6. Delete parallel type patterns from `column_types.py` (after S36)
- [ ] D7. Conditionally delete `metrics.py`/`stats/` if S14 gates pass
- [ ] D8. Delete duplicate CDF orchestration paths (after S22, S41, S42)
- [ ] D9. Delete residual semantics->relspec constant imports and pipeline `SEMANTIC_MODEL` globals (after S29, S43)
