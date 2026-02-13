# DataFusion Engine Decomposition Implementation Plan

**Date:** 2026-02-12
**Status:** DRAFT
**Owner:** Engineering Team
**Assessment Source:** `/Users/paulheyse/CodeAnatomy/docs/plans/datafusion_engine_decomposition_assessment_v1_2026-02-12.md`

---

## Executive Summary

This plan operationalizes the decomposition of `src/datafusion_engine/` (73,655 LOC) by removing dead code (~1,670 LOC), pruning dead exports (~530 LOC), simplifying Rust adapter layers (~600 LOC), and restructuring monolithic files for maintainability. Total immediate actionable scope: ~2,800 LOC removal + 12,648 LOC restructuring across 5 waves.

**Execution waves:**
1. **Wave 0** — Dead code deletion (zero production callers): ~1,040 LOC removed
2. **Wave 1** — Dead export pruning from live files: ~530 LOC removed
3. **Wave 2** — UDF bridge simplification: ~300 LOC simplified
4. **Wave 3** — Delta control-plane thinning: ~500 LOC removed
5. **Wave 4** — Monolith decomposition: 12,648 LOC restructured (0 net change)

**Wave 5** (future scope) depends on extraction transitioning to Rust: ~7,567 LOC removal.

---

## Architecture Context

The pipeline has three phases with Python and Rust owning distinct responsibilities:

```mermaid
graph LR
    A[Phase 1: Python Extraction] --> B[Phase 2: Python Semantic IR]
    B --> C[Phase 3: Rust Engine]

    A --> D[Delta tables work_dir]
    B --> E[SemanticExecutionSpec JSON]
    C --> F[CPG outputs output_dir]

    style A fill:#e1f5ff
    style B fill:#e1f5ff
    style C fill:#ffe1e1
```

**Python's legitimate DataFusion usage:**
- **Extraction** — Build and execute DataFusion queries to transform evidence tables
- **Semantic IR** — Assemble view definitions (not executed) for Rust compiler

**Rust owns:**
- Session construction (`SessionStateBuilder`)
- Plan compilation, rule application, scheduling
- Execution and materialization
- UDF implementations (all UDFs are Rust-native)
- Delta control plane

**DataFusion version alignment:** Python bindings are `datafusion` 51.0.0 (PyPI), Rust crate is 51.0 (Cargo.toml). Keep in sync.

---

## Wave 0: Dead Code Deletion

**Scope:** Delete files with zero production callers. No callsite changes required.

### Files to Delete

#### 1. `src/datafusion_engine/workload/` subpackage (307 LOC)

**Evidence:** Zero imports from `src/` or `tests/`. Only internal cross-references between the two files.

**What it was:** Planned feature for dynamic session tuning based on workload classification (BATCH_INGEST, INTERACTIVE_QUERY, etc.). Never wired into the pipeline.

**Files:**
- `src/datafusion_engine/workload/__init__.py` (~10 LOC)
- `src/datafusion_engine/workload/classifier.py` (~170 LOC)
- `src/datafusion_engine/workload/session_profiles.py` (~137 LOC)

#### 2. `src/datafusion_engine/plan/pipeline_runtime.py` (541 LOC)

**Evidence:** Zero imports from `src/`. One test file imports it.

**What it was:** Two-pass planning pipeline for old rustworkx scheduling system. Imports from `relspec.inferred_deps` (now deprecated).

**Code snippet showing dead dependency:**
```python
# src/datafusion_engine/plan/pipeline_runtime.py (line 25)
from relspec.inferred_deps import InferredDeps, infer_deps_from_view_nodes
# ^^^ This is the old rustworkx scheduling system, now replaced by Rust
```

**Associated test file to delete:**
- `tests/unit/datafusion_engine/plan/test_pipeline_scan_policy_inference_wiring.py` (~50 LOC)

#### 3. `src/datafusion_engine/delta/plugin_options.py` (~122 LOC)

**Evidence:** Registered in `delta/__init__.py` lazy-load registry but never imported.

**What it was:** Delta plugin option resolution for session-to-plugin configuration bridging. Superseded by Rust plugin system.

**File edit required:**
- `src/datafusion_engine/delta/__init__.py` — Remove lazy-load entries

### File Edit: `delta/__init__.py`

**Current state (lines to remove):**
```python
# src/datafusion_engine/delta/__init__.py
__all__ = [
    # ... other exports ...
    "delta_plugin_options_from_session",  # REMOVE
    "delta_plugin_options_json",          # REMOVE
]

_LAZY_REGISTRY = {
    # ... other entries ...
    "delta_plugin_options_from_session": (
        "datafusion_engine.delta.plugin_options",
        "delta_plugin_options_from_session",
    ),  # REMOVE
    "delta_plugin_options_json": (
        "datafusion_engine.delta.plugin_options",
        "delta_plugin_options_json",
    ),  # REMOVE
}

if TYPE_CHECKING:
    # ... other imports ...
    from datafusion_engine.delta.plugin_options import (  # REMOVE BLOCK
        delta_plugin_options_from_session,
        delta_plugin_options_json,
    )
```

**After cleanup:**
```python
# src/datafusion_engine/delta/__init__.py
__all__ = [
    # ... other exports (plugin_options entries removed) ...
]

_LAZY_REGISTRY = {
    # ... other entries (plugin_options entries removed) ...
}

if TYPE_CHECKING:
    # ... other imports (plugin_options block removed) ...
```

### File Edit: `plan/__init__.py`

**Check for stale re-exports:**
```bash
# Verify pipeline_runtime is not re-exported
grep -n "pipeline_runtime" src/datafusion_engine/plan/__init__.py
```

If found, remove the re-export.

### Wave 0 Implementation Checklist

- [ ] **Pre-flight checks**
  - [ ] Run `/cq calls workload` to confirm zero src/ callers
  - [ ] Run `/cq calls pipeline_runtime` to confirm zero src/ callers
  - [ ] Run `/cq calls plugin_options` to confirm zero src/ callers
  - [ ] Run full test suite: `uv run pytest tests/ -m "not e2e" -q`

- [ ] **Delete workload subpackage**
  - [ ] Delete `src/datafusion_engine/workload/__init__.py`
  - [ ] Delete `src/datafusion_engine/workload/classifier.py`
  - [ ] Delete `src/datafusion_engine/workload/session_profiles.py`
  - [ ] Remove directory if empty: `rmdir src/datafusion_engine/workload/`

- [ ] **Delete pipeline_runtime**
  - [ ] Delete `src/datafusion_engine/plan/pipeline_runtime.py`
  - [ ] Delete `tests/unit/datafusion_engine/plan/test_pipeline_scan_policy_inference_wiring.py`
  - [ ] Check `src/datafusion_engine/plan/__init__.py` for stale re-exports, remove if present

- [ ] **Delete plugin_options**
  - [ ] Delete `src/datafusion_engine/delta/plugin_options.py`
  - [ ] Edit `src/datafusion_engine/delta/__init__.py`:
    - [ ] Remove `delta_plugin_options_from_session` from `__all__`
    - [ ] Remove `delta_plugin_options_json` from `__all__`
    - [ ] Remove both entries from `_LAZY_REGISTRY`
    - [ ] Remove TYPE_CHECKING import block for plugin_options

- [ ] **Post-flight checks**
  - [ ] Run `uv run ruff format`
  - [ ] Run `uv run ruff check --fix`
  - [ ] Run `uv run pyrefly check`
  - [ ] Run `uv run pyright`
  - [ ] Run full test suite: `uv run pytest tests/ -m "not e2e" -q`
  - [ ] Verify no import errors: `uv run python -c "from datafusion_engine import *"`

**Expected outcome:** ~1,040 LOC removed, 0 callsite changes, all tests pass.

---

## Wave 1: Dead Export Pruning

**Scope:** Remove dead exports from live files. Requires verifying zero callers with `/cq calls`.

### 1A: `schema/registry.py` — 56 dead exports (~300-400 LOC)

This is the second-largest file in the module (4,166 LOC). Approximately 59% of its exports are dead.

#### Dead Schema View-Name Constants (6 constants)

**Code snippet:**
```python
# src/datafusion_engine/schema/registry.py
# DEAD — Zero callers in src/ or tests/
AST_VIEW_NAMES: frozenset[str] = frozenset({...})
BYTECODE_VIEW_NAMES: frozenset[str] = frozenset({...})
SCIP_VIEW_NAMES: frozenset[str] = frozenset({...})
SYMTABLE_VIEW_NAMES: frozenset[str] = frozenset({...})
TREE_SITTER_VIEW_NAMES: frozenset[str] = frozenset({...})
CST_VIEW_NAMES: frozenset[str] = frozenset({...})
```

**Verification command:**
```bash
/cq calls AST_VIEW_NAMES
/cq calls BYTECODE_VIEW_NAMES
/cq calls SCIP_VIEW_NAMES
/cq calls SYMTABLE_VIEW_NAMES
/cq calls TREE_SITTER_VIEW_NAMES
/cq calls CST_VIEW_NAMES
```

**Note:** Keep `AST_CORE_VIEW_NAMES` and `AST_OPTIONAL_VIEW_NAMES` (used by `session/runtime.py`).

#### Dead Pipeline Schema Constants (4 constants)

**Code snippet:**
```python
# src/datafusion_engine/schema/registry.py
# DEAD — Hamilton pipeline event tracking no longer exists
PIPELINE_PLAN_DRIFT_SCHEMA: pa.Schema = pa.schema([...])
PIPELINE_TASK_EXPANSION_SCHEMA: pa.Schema = pa.schema([...])
PIPELINE_TASK_GROUPING_SCHEMA: pa.Schema = pa.schema([...])
PIPELINE_TASK_SUBMISSION_SCHEMA: pa.Schema = pa.schema([...])
```

**Verification command:**
```bash
/cq calls PIPELINE_PLAN_DRIFT_SCHEMA
/cq calls PIPELINE_TASK_EXPANSION_SCHEMA
/cq calls PIPELINE_TASK_GROUPING_SCHEMA
/cq calls PIPELINE_TASK_SUBMISSION_SCHEMA
```

#### Dead DataFusion Schema Constants (3 constants)

**Code snippet:**
```python
# src/datafusion_engine/schema/registry.py
# DEAD — Old schema constants for DataFusion tracking
DATAFUSION_RUNS_SCHEMA: pa.Schema = pa.schema([...])
DATAFUSION_SQL_INGEST_SCHEMA: pa.Schema = pa.schema([...])
DATAFUSION_VIEW_ARTIFACTS_SCHEMA: pa.Schema = pa.schema([...])
```

**Keep these (actively used):**
```python
# KEEP — Used by session/runtime.py, materialize_pipeline.py
DATAFUSION_PLAN_ARTIFACTS_SCHEMA: pa.Schema = pa.schema([...])
DATAFUSION_PIPELINE_EVENTS_V2_SCHEMA: pa.Schema = pa.schema([...])
```

#### Dead Validator Functions (11 functions, ~100 LOC)

**Code snippet:**
```python
# src/datafusion_engine/schema/registry.py
# DEAD — Abandoned validation layer from old extractor validation system

def validate_ast_views(ctx: SessionContext) -> None: ...
def validate_bytecode_views(ctx: SessionContext) -> None: ...
def validate_cst_views(ctx: SessionContext) -> None: ...
def validate_scip_views(ctx: SessionContext) -> None: ...
def validate_symtable_views(ctx: SessionContext) -> None: ...
def validate_ts_views(ctx: SessionContext) -> None: ...

def validate_required_bytecode_functions(ctx: SessionContext) -> None: ...
def validate_required_cst_functions(ctx: SessionContext) -> None: ...
def validate_required_symtable_functions(ctx: SessionContext) -> None: ...

def validate_schema_metadata(schema: pa.Schema, expected: dict) -> None: ...
def validate_udf_info_schema_parity(ctx: SessionContext) -> None: ...
```

**Verification command:**
```bash
/cq calls validate_ast_views
/cq calls validate_bytecode_views
# ... repeat for all 11 functions
```

**Keep these (actively used):**
```python
# KEEP — Used by session/runtime.py, materialize_pipeline.py
def validate_nested_types(ctx: SessionContext, table_name: str) -> None: ...
```

#### Dead Nested Dataset Functions (15+ functions, ~200 LOC)

**Code snippet:**
```python
# src/datafusion_engine/schema/registry.py
# DEAD — Experimental nested dataset extraction system never used

def extract_nested_context_for(path: str) -> str | None: ...
def extract_nested_path_for(dataset_name: str) -> str | None: ...
def extract_nested_role_for(path: str) -> str | None: ...
def extract_nested_spec_for(path: str) -> NestedSpec | None: ...
def extract_datasets_for_path(path: str) -> list[str]: ...

def is_extract_nested_dataset(table_name: str) -> bool: ...
def is_extract_intrinsic_nested_dataset(table_name: str) -> bool: ...
def is_nested_dataset(table_name: str) -> bool: ...
def is_intrinsic_nested_dataset(table_name: str) -> bool: ...

def nested_context_for(table_name: str) -> str | None: ...
def nested_role_for(table_name: str) -> str | None: ...
def datasets_for_path(path: str) -> list[str]: ...
def struct_for_path(path: str) -> pa.StructType | None: ...
def identity_fields_for(table_name: str) -> list[str]: ...
```

**Keep these (actively used):**
```python
# KEEP — Used by session/runtime.py for nested view setup
def nested_view_specs() -> list[NestedViewSpec]: ...
def nested_view_spec(table_name: str) -> NestedViewSpec | None: ...
def nested_path_for(table_name: str) -> str | None: ...
```

#### Dead Relationship Schema Map and Index (2 constants)

**Code snippet:**
```python
# src/datafusion_engine/schema/registry.py
# DEAD — Old relationship schema tracking
RELATIONSHIP_SCHEMA_BY_NAME: Mapping[str, pa.Schema] = {...}
NESTED_DATASET_INDEX: Mapping[str, NestedSpec] = {...}
```

#### Live Exports to KEEP (Critical Path)

**Code snippet showing KEEP pattern:**
```python
# src/datafusion_engine/schema/registry.py
# KEEP — CRITICAL PATH (extraction, semantics)

def extract_schema_for(table_name: str) -> pa.Schema:
    """Return the PyArrow schema for a registered extraction table.

    This is the primary schema lookup API used across extraction and
    semantic compilation. Used by 18+ files.
    """
    ...

def validate_nested_types(ctx: SessionContext, table_name: str) -> None:
    """Validate nested field types in a registered table.

    Used by session/runtime.py and materialize_pipeline.py.
    """
    ...

def nested_view_specs() -> list[NestedViewSpec]:
    """Return all nested view specifications.

    Used by session setup for nested view registration.
    """
    ...

def registered_table_names() -> frozenset[str]:
    """Return all registered table names.

    Used by catalog introspection.
    """
    ...

# Keep these schema constants (actively used)
DATAFUSION_PLAN_ARTIFACTS_SCHEMA: pa.Schema = pa.schema([...])
DATAFUSION_PIPELINE_EVENTS_V2_SCHEMA: pa.Schema = pa.schema([...])
AST_CORE_VIEW_NAMES: frozenset[str] = frozenset({...})
AST_OPTIONAL_VIEW_NAMES: frozenset[str] = frozenset({...})
```

### 1B: `dataset/registration.py` — 6 dead exports (~193 LOC)

**Dead exports:**

**Code snippet:**
```python
# src/datafusion_engine/dataset/registration.py
# DEAD — Zero external importers

@dataclass(frozen=True)
class DataFusionCacheSettings:
    """DEAD — Old cache settings."""
    ...  # ~9 lines

@dataclass(frozen=True)
class DatasetInputSource:
    """DEAD — Old input plugin system."""
    ...  # ~71 lines

@dataclass(frozen=True)
class DatasetRegistrationOptions:
    """DEAD — Old registration options."""
    ...  # ~8 lines

@dataclass(frozen=True)
class DatasetRegistration:
    """DEAD — Old registration result."""
    ...  # ~9 lines

def register_dataset_spec(
    ctx: SessionContext,
    spec: DatasetSpec,
    options: DatasetRegistrationOptions | None = None,
) -> DatasetRegistration:
    """DEAD — Spec-based registration path."""
    ...  # ~52 lines

def resolve_registry_options(
    ctx: SessionContext,
    options: DatasetRegistrationOptions | None,
) -> ResolvedOptions:
    """DEAD — Options resolver."""
    ...  # ~44 lines
```

**Live exports to KEEP:**
```python
# src/datafusion_engine/dataset/registration.py
# KEEP — Actively used

@dataclass(frozen=True)
class DataFusionCachePolicy:
    """KEEP — 18 callers."""
    ...

def register_dataset_df(
    ctx: SessionContext,
    name: str,
    df: DataFrame,
) -> None:
    """KEEP — 3 callers (extraction core)."""
    ...

@dataclass(frozen=True)
class DataFusionRegistryOptions:
    """KEEP — 2 callers."""
    ...

def dataset_input_plugin(plugin_name: str) -> InputPlugin:
    """KEEP — 2 callers."""
    ...

def input_plugin_prefixes() -> frozenset[str]:
    """KEEP — 2 callers."""
    ...
```

### 1C: `extract/templates.py` — 5 dead exports (~20-30 LOC)

**Dead exports:**

**Code snippet:**
```python
# src/datafusion_engine/extract/templates.py
# DEAD — Zero importers

CONFIGS: Mapping[str, ExtractorConfigSpec] = {...}  # ~5 lines
TEMPLATES: Mapping[str, DatasetTemplateSpec] = {...}  # ~5 lines

DatasetTemplateSpec: TypeAlias = ...  # ~3 lines
ExtractorConfigSpec: TypeAlias = ...  # ~3 lines
ExtractorTemplate: TypeAlias = ...  # ~3 lines

def flag_default(flag_name: str) -> Any:
    """Return default value for a feature flag."""
    ...  # ~10 lines
```

**Live exports to KEEP:**
```python
# src/datafusion_engine/extract/templates.py
# KEEP — Heavily used

def config(extractor_name: str) -> ExtractorConfig:
    """KEEP — 450+ call sites."""
    ...

def template(template_name: str) -> DatasetTemplate:
    """KEEP — 116 call sites."""
    ...

def dataset_template_specs() -> list[DatasetTemplateSpec]:
    """KEEP — Used by extraction orchestrator."""
    ...

def expand_dataset_templates(specs: list[DatasetTemplateSpec]) -> list[Dataset]:
    """KEEP — Used by extraction orchestrator."""
    ...
```

### 1D: `datafusion_engine/__init__.py` — Stale re-exports

**Action:** After removing dead exports from submodules, audit `src/datafusion_engine/__init__.py` to remove stale re-exports.

**Code snippet:**
```python
# src/datafusion_engine/__init__.py
# Check for stale lazy-load entries pointing to deleted code

_LAZY_REGISTRY = {
    # ... remove any entries for deleted exports ...
}

if TYPE_CHECKING:
    # ... remove any type-only imports for deleted exports ...
}

__all__ = [
    # ... remove any deleted export names ...
]
```

### Wave 1 Implementation Checklist

- [ ] **Pre-flight checks**
  - [ ] Run `/cq calls <export>` for each identified dead export to confirm zero callers
  - [ ] Document any unexpected callers found (re-assess if not actually dead)
  - [ ] Run full test suite: `uv run pytest tests/ -m "not e2e" -q`

- [ ] **Prune schema/registry.py**
  - [ ] Remove 6 dead schema view-name constants
  - [ ] Remove 4 dead pipeline schema constants
  - [ ] Remove 3 dead DataFusion schema constants
  - [ ] Remove 11 dead validator functions
  - [ ] Remove 15+ dead nested dataset functions
  - [ ] Remove 2 dead relationship constants
  - [ ] Verify `extract_schema_for()` and other KEEP exports remain intact

- [ ] **Prune dataset/registration.py**
  - [ ] Remove `DataFusionCacheSettings` class
  - [ ] Remove `DatasetInputSource` class
  - [ ] Remove `DatasetRegistrationOptions` class
  - [ ] Remove `DatasetRegistration` class
  - [ ] Remove `register_dataset_spec()` function
  - [ ] Remove `resolve_registry_options()` function
  - [ ] Verify `register_dataset_df()` and other KEEP exports remain intact

- [ ] **Prune extract/templates.py**
  - [ ] Remove `CONFIGS` constant
  - [ ] Remove `TEMPLATES` constant
  - [ ] Remove `DatasetTemplateSpec` type alias
  - [ ] Remove `ExtractorConfigSpec` type alias
  - [ ] Remove `ExtractorTemplate` type alias
  - [ ] Remove `flag_default()` function
  - [ ] Verify `config()`, `template()` and other KEEP exports remain intact

- [ ] **Update datafusion_engine/__init__.py**
  - [ ] Remove stale entries from `_LAZY_REGISTRY`
  - [ ] Remove stale TYPE_CHECKING imports
  - [ ] Remove stale names from `__all__`

- [ ] **Post-flight checks**
  - [ ] Run `uv run ruff format`
  - [ ] Run `uv run ruff check --fix`
  - [ ] Run `uv run pyrefly check`
  - [ ] Run `uv run pyright`
  - [ ] Run full test suite: `uv run pytest tests/ -m "not e2e" -q`
  - [ ] Verify no import errors: `uv run python -c "from datafusion_engine import *"`

**Expected outcome:** ~530-640 LOC removed, minimal callsite changes, all tests pass.

---

## Wave 2: UDF Bridge Simplification

**Scope:** Simplify the UDF adapter layer by removing fallback paths. All UDFs are now Rust-native via `datafusion._internal` — no Python fallback exists.

### Architecture Context: DataFusion UDF Best Practices

**Current Rust implementation already follows best-in-class patterns:**

```rust
// rust/datafusion_ext/src/udf/ (EXISTING)
// All UDFs use ScalarUDFImpl (Layer 3 - full power)

impl ScalarUDFImpl for MyUDF {
    fn return_type_from_exprs(
        &self,
        args: &[Expr],
        schema: &DFSchema,
        arg_types: &[DataType],
    ) -> Result<Arc<Field>> {
        // Layer 3: Dynamic return types with metadata
        Ok(Arc::new(Field::new("result", DataType::Utf8, false)
            .with_metadata(HashMap::from([
                ("semantic_type".to_string(), "span_id".to_string()),
            ]))
        ))
    }

    fn signature(&self) -> &Signature {
        // Named parameters for better error messages
        Signature::exact(
            vec![DataType::Int64, DataType::Utf8],
            Volatility::Immutable,  // Foldable by optimizer
        ).with_parameter_names(vec!["offset", "text"])
    }

    fn simplify(&self) -> Option<ExprSimplifier> {
        // Optional: Expression rewriting for optimization
        Some(Box::new(|args, info| {
            // Constant folding, null propagation, etc.
        }))
    }
}
```

**Key patterns:**
- `return_type_from_exprs()` (Layer 3) enables metadata-aware return types
- `Volatility::Immutable` allows constant folding and caching
- Named parameters via `signature().with_parameter_names()`
- Optional `simplify()` for expression rewriting
- `FunctionFactory` support for `CREATE FUNCTION` (already implemented)

### Current Problem: Python Fallback Paths

The Python UDF platform installer has fallback detection and retry logic that was needed during the Python→Rust transition but is no longer necessary.

**Code snippet showing current fallback logic:**
```python
# src/datafusion_engine/udf/platform.py (CURRENT - 425 LOC)
# This file has extensive fallback paths that are no longer needed

def install_rust_udf_platform(
    ctx: SessionContext,
    *,
    options: RustUdfPlatformOptions | None = None,
) -> RustUdfPlatform:
    """Install Rust UDF platform with fallback and retry logic."""
    options = options or RustUdfPlatformOptions()

    # REMOVE: Fallback detection
    if not _is_native_available():
        _LOGGER.warning("Native UDF platform not available, using fallback")
        return _build_fallback_platform(ctx, options)

    # REMOVE: Soft-fail logging for retry loops
    if _SOFT_FAIL_LOGGED.get(ctx):
        return _cached_platform(ctx)

    # REMOVE: Complex retry + validation
    max_retries = options.max_retries or 3
    for attempt in range(max_retries):
        try:
            snapshot = register_rust_udfs(ctx)
            if snapshot is None:
                _LOGGER.warning(f"UDF registration attempt {attempt+1} returned None")
                continue
            # ... validation logic ...
            break
        except Exception as e:
            if attempt == max_retries - 1:
                _LOGGER.error(f"UDF registration failed after {max_retries} attempts")
                return _build_fallback_platform(ctx, options)
            _LOGGER.warning(f"Retry {attempt+1}: {e}")
            time.sleep(0.1 * (attempt + 1))

    # ... 200+ more lines of fallback handling ...
```

### Target Architecture: Simplified UDF Platform

**Code snippet for simplified installation:**
```python
# src/datafusion_engine/udf/platform.py (SIMPLIFIED - ~200 LOC)

def install_rust_udf_platform(
    ctx: SessionContext,
    *,
    options: RustUdfPlatformOptions | None = None,
) -> RustUdfPlatform:
    """Install Rust UDF platform into a DataFusion session.

    All UDFs are Rust-native via datafusion._internal. No Python
    fallback paths exist — Rust UDF availability is a hard requirement.

    DataFusion best practice: UDFs use ScalarUDFImpl Layer 3 with
    return_type_from_exprs for metadata-aware return types, named
    parameters via Signature.with_parameter_names(), and Volatility.Immutable
    for foldable functions.

    Parameters
    ----------
    ctx : SessionContext
        DataFusion session to install UDFs into.
    options : RustUdfPlatformOptions | None
        Platform configuration (function factory, expr planners).

    Returns
    -------
    RustUdfPlatform
        Installed platform with UDF snapshot, rewrite tags, and docs.

    Raises
    ------
    ImportError
        If datafusion._internal is unavailable (hard requirement).
    RuntimeError
        If UDF registration fails.
    """
    options = options or RustUdfPlatformOptions()

    # Direct registration (no fallback)
    snapshot = register_rust_udfs(ctx)
    snapshot_hash = rust_udf_snapshot_hash(snapshot)

    # Optional: Function factory for CREATE FUNCTION support
    factory_status = None
    factory_policy = None
    if options.enable_function_factory:
        factory_status, factory_policy = _install_function_factory(
            ctx,
            enabled=True,
            hook=options.function_factory_hook,
            policy=options.function_factory_policy,
        )

    # Optional: Expression planners for custom syntax
    planner_status = None
    planner_policy = None
    if options.enable_expr_planners:
        planner_status, planner_policy = _install_expr_planners(
            ctx,
            names=options.expr_planner_names,
        )

    return RustUdfPlatform(
        snapshot=snapshot,
        snapshot_hash=snapshot_hash,
        rewrite_tags=rewrite_tag_index(ctx),
        domain_planner_names=domain_planner_names_from_snapshot(snapshot),
        docs=rust_udf_docs(ctx) if snapshot else None,
        function_factory=factory_status,
        expr_planners=planner_status,
        function_factory_policy=factory_policy,
        expr_planner_policy=planner_policy,
    )
```

### Target: Simplified UDF Runtime

**Code snippet for simplified runtime:**
```python
# src/datafusion_engine/udf/runtime.py (SIMPLIFIED)

def register_rust_udfs(ctx: SessionContext) -> Mapping[str, object]:
    """Register all Rust-native UDFs into the session.

    This is now a hard requirement — no Python fallback exists.

    DataFusion best practice: UDFs use ScalarUDFImpl Layer 3 with
    return_type_from_exprs for metadata-aware return types, Volatility.Immutable
    for foldable functions, and named parameter support via Signature.

    Parameters
    ----------
    ctx : SessionContext
        DataFusion session to register UDFs into.

    Returns
    -------
    Mapping[str, object]
        UDF snapshot mapping function names to metadata.

    Raises
    ------
    ImportError
        If datafusion._internal is unavailable.
    RuntimeError
        If UDF registration fails.
    """
    module = importlib.import_module("datafusion._internal")
    snapshot = module.register_codeanatomy_udfs(ctx)

    # Track registration for cleanup
    _RUST_UDF_CONTEXTS.add(ctx)
    _RUST_UDF_SNAPSHOTS[ctx] = snapshot

    return snapshot
```

### Files to Edit

#### 1. `src/datafusion_engine/udf/platform.py`

**Changes:**
- Remove `_is_native_available()` fallback detection
- Remove `_build_fallback_platform()` fallback constructor
- Remove `_SOFT_FAIL_LOGGED` tracking
- Remove retry loops in `install_rust_udf_platform()`
- Simplify to direct `register_rust_udfs()` call
- Keep function factory and expr planner installation (optional features)

**Estimated reduction:** ~200 LOC (425 → ~225)

#### 2. `src/datafusion_engine/udf/runtime.py`

**Changes:**
- Remove `try: ... except ImportError: return None` patterns
- Change `register_rust_udfs()` to raise ImportError directly (no swallowing)
- Remove Python UDF fallback branches (none exist anymore)
- Simplify snapshot validation (no retry logic)

**Estimated reduction:** ~100 LOC (1,677 → ~1,577)

#### 3. `src/datafusion_engine/udf/factory.py`

**Changes:**
- Remove retry loops in function factory installation
- Use direct Rust registration
- Keep `FunctionFactory` protocol implementation (still needed)

**Estimated reduction:** Minor (~20-30 LOC)

### Wave 2 Implementation Checklist

- [ ] **Pre-flight checks**
  - [ ] Verify all UDFs are Rust-native: `rg "def.*udf" src/datafusion_engine/udf/` should return zero Python UDF implementations
  - [ ] Verify `datafusion._internal.register_codeanatomy_udfs` exists: `uv run python -c "from datafusion._internal import register_codeanatomy_udfs"`
  - [ ] Run full test suite: `uv run pytest tests/ -m "not e2e" -q`

- [ ] **Simplify udf/platform.py**
  - [ ] Remove `_is_native_available()` function
  - [ ] Remove `native_udf_platform_available()` function
  - [ ] Remove `_build_fallback_platform()` function
  - [ ] Remove `_SOFT_FAIL_LOGGED` module-level tracking dict
  - [ ] Simplify `install_rust_udf_platform()`: remove fallback paths, retry loops
  - [ ] Keep function factory installation logic
  - [ ] Keep expr planner installation logic
  - [ ] Update docstring to document hard requirement

- [ ] **Simplify udf/runtime.py**
  - [ ] Change `register_rust_udfs()` to raise ImportError instead of returning None
  - [ ] Remove any `try: ... except ImportError: return None` patterns
  - [ ] Remove Python UDF fallback detection
  - [ ] Simplify snapshot validation (remove retry logic)
  - [ ] Update docstring to document hard requirement

- [ ] **Simplify udf/factory.py**
  - [ ] Remove retry loops if present
  - [ ] Use direct Rust registration
  - [ ] Keep `FunctionFactory` protocol (still needed for CREATE FUNCTION)

- [ ] **Update callers if needed**
  - [ ] Check if any caller swallows ImportError from `install_rust_udf_platform()`
  - [ ] Update caller to propagate error (Rust UDF is hard requirement)

- [ ] **Post-flight checks**
  - [ ] Run `uv run ruff format`
  - [ ] Run `uv run ruff check --fix`
  - [ ] Run `uv run pyrefly check`
  - [ ] Run `uv run pyright`
  - [ ] Run full test suite: `uv run pytest tests/ -m "not e2e" -q`
  - [ ] Run extraction pipeline smoke test: `uv run python -m extraction.orchestrator --repo-root . --work-dir /tmp/test`

**Expected outcome:** ~300 LOC simplified, clearer error messages, all tests pass.

---

## Wave 3: Delta Control-Plane Thinning

**Scope:** Thin the Python Delta control plane by moving protocol validation to Rust and evaluating scan policy inference for removal.

### Architecture Context: DataFusion Delta Integration Best Practices

**DataFusion provides `DeltaTableProvider` implementing `TableProvider` trait:**

```rust
// rust/datafusion_ext/src/delta_control_plane.rs (EXISTING)
// Already uses best-in-class Delta integration

impl TableProvider for DeltaTableProvider {
    fn schema(&self) -> SchemaRef {
        // Delta schema from transaction log
        self.snapshot.schema()
    }

    fn scan(&self, ...) -> Result<Arc<dyn ExecutionPlan>> {
        // File-level pruning from Delta log
        // + row-group level pruning from Parquet metadata
        self.create_physical_plan(...)
    }

    fn supports_filters_pushdown(&self, filters: &[Expr]) -> Result<Vec<FilterPushdown>> {
        // Predicate pushdown support
        filters.iter()
            .map(|_| FilterPushdown::Inexact)
            .collect()
    }

    fn statistics(&self) -> Result<Statistics> {
        // Delta transaction log statistics
        self.snapshot.statistics()
    }

    fn insert_into(&self, ...) -> Result<Arc<dyn ExecutionPlan>> {
        // ACID write support
        self.create_insert_plan(...)
    }
}
```

**Key patterns:**
- Delta provides file-level pruning from transaction log
- DataFusion provides row-group level pruning from Parquet metadata
- `collect_statistics = true` (DataFusion 48+) enables cost-based join planning
- `parquet.pushdown_filters = true` + `parquet.enable_page_index = true` for late materialization
- Schema is authoritative from Delta transaction log (no Python inference needed)

### 3A: Move Protocol Gate Validation to Rust

**Current state:** `src/datafusion_engine/delta/protocol.py` implements Delta protocol gate validation in Python. The Rust side already has the validation logic in `rust/datafusion_ext/src/delta_protocol.rs` but Python re-implements it.

**Code snippet showing Python validation (CURRENT):**
```python
# src/datafusion_engine/delta/protocol.py (CURRENT)

@dataclass(frozen=True)
class DeltaFeatureGate:
    """Delta protocol feature gate requirements."""
    min_reader_version: int
    min_writer_version: int
    required_reader_features: frozenset[str] = field(default_factory=frozenset)
    required_writer_features: frozenset[str] = field(default_factory=frozenset)

def validate_delta_protocol(
    snapshot: DeltaSnapshotInfo,
    gate: DeltaFeatureGate,
) -> str | None:
    """Validate Delta protocol version against feature gate.

    Returns
    -------
    str | None
        Error message if validation fails, None if passes.
    """
    if snapshot.min_reader_version < gate.min_reader_version:
        return (
            f"Reader version {snapshot.min_reader_version} < "
            f"required {gate.min_reader_version}"
        )

    if snapshot.min_writer_version < gate.min_writer_version:
        return (
            f"Writer version {snapshot.min_writer_version} < "
            f"required {gate.min_writer_version}"
        )

    # Feature checks...
    missing_reader = gate.required_reader_features - snapshot.reader_features
    if missing_reader:
        return f"Missing reader features: {missing_reader}"

    missing_writer = gate.required_writer_features - snapshot.writer_features
    if missing_writer:
        return f"Missing writer features: {missing_writer}"

    return None
```

**Rust side already has the implementation:**
```rust
// rust/datafusion_ext/src/delta_protocol.rs (EXISTING)

pub fn protocol_gate(
    snapshot: &DeltaSnapshotInfo,
    gate: &DeltaFeatureGate,
) -> Result<(), String> {
    if snapshot.min_reader_version < gate.min_reader_version {
        return Err(format!(
            "Reader version {} < required {}",
            snapshot.min_reader_version, gate.min_reader_version
        ));
    }

    if snapshot.min_writer_version < gate.min_writer_version {
        return Err(format!(
            "Writer version {} < required {}",
            snapshot.min_writer_version, gate.min_writer_version
        ));
    }

    // Feature checks (same logic as Python)
    let missing_reader: Vec<_> = gate.required_reader_features
        .difference(&snapshot.reader_features)
        .collect();
    if !missing_reader.is_empty() {
        return Err(format!("Missing reader features: {:?}", missing_reader));
    }

    let missing_writer: Vec<_> = gate.required_writer_features
        .difference(&snapshot.writer_features)
        .collect();
    if !missing_writer.is_empty() {
        return Err(format!("Missing writer features: {:?}", missing_writer));
    }

    Ok(())
}
```

**Target: Expose Rust validation via PyO3 (NEW):**
```rust
// rust/datafusion_ext/src/delta_protocol.rs (ADD PyO3 binding)

use pyo3::prelude::*;

#[pyfunction]
fn validate_delta_protocol_py(
    snapshot_json: &str,
    gate_json: &str,
) -> PyResult<Option<String>> {
    let snapshot: DeltaSnapshotInfo = serde_json::from_str(snapshot_json)
        .map_err(|e| PyValueError::new_err(e.to_string()))?;
    let gate: DeltaFeatureGate = serde_json::from_str(gate_json)
        .map_err(|e| PyValueError::new_err(e.to_string()))?;

    match protocol_gate(&snapshot, &gate) {
        Ok(()) => Ok(None),
        Err(msg) => Ok(Some(msg)),
    }
}

#[pymodule]
fn datafusion_ext(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(validate_delta_protocol_py, m)?)?;
    Ok(())
}
```

**Python side becomes a thin wrapper:**
```python
# src/datafusion_engine/delta/protocol.py (SIMPLIFIED)

from datafusion._internal import validate_delta_protocol_py
import msgspec

@dataclass(frozen=True)
class DeltaFeatureGate:
    """Delta protocol feature gate requirements."""
    min_reader_version: int
    min_writer_version: int
    required_reader_features: frozenset[str] = field(default_factory=frozenset)
    required_writer_features: frozenset[str] = field(default_factory=frozenset)

def validate_delta_protocol(
    snapshot: DeltaSnapshotInfo,
    gate: DeltaFeatureGate,
) -> str | None:
    """Validate Delta protocol version against feature gate.

    Validation is performed by Rust implementation in datafusion_ext.

    Returns
    -------
    str | None
        Error message if validation fails, None if passes.
    """
    encoder = msgspec.json.Encoder()
    snapshot_json = encoder.encode(snapshot).decode("utf-8")
    gate_json = encoder.encode(gate).decode("utf-8")

    return validate_delta_protocol_py(snapshot_json, gate_json)
```

### 3B: Evaluate `scan_policy_inference.py` for Deletion

**Current callers:**
1. `src/datafusion_engine/plan/pipeline_runtime.py` (line 27) — DELETED in Wave 0
2. `src/relspec/policy_compiler.py` (line 20) — TYPE_CHECKING import only

**Code snippet showing relspec usage:**
```python
# src/relspec/policy_compiler.py (line 20)

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datafusion_engine.delta.scan_policy_inference import ScanPolicyOverride
    # ^^^ Only used for type hints, not runtime

def compile_runtime_profile(...) -> RuntimeProfile:
    # Check if ScanPolicyOverride is actually used at runtime here
    ...
```

**Verification steps:**
1. Delete `pipeline_runtime.py` in Wave 0
2. Check if `relspec/policy_compiler.py` uses `ScanPolicyOverride` at runtime (not just TYPE_CHECKING)
3. Use `/cq calls ScanPolicyOverride` to find all usages
4. If only TYPE_CHECKING imports remain, delete `scan_policy_inference.py`

**If dead, estimated LOC removed:** ~499

### 3C: Thin `delta/control_plane.py` Facade

**Current state:** 2,337 LOC Python facade that marshals parameters to Rust. After moving protocol validation to Rust, evaluate if more functionality can be absorbed by Rust.

**Code snippet showing typical facade pattern:**
```python
# src/datafusion_engine/delta/control_plane.py (CURRENT)

def create_delta_provider(
    ctx: SessionContext,
    table_uri: str,
    options: DeltaProviderOptions | None = None,
) -> DeltaProvider:
    """Create a Delta table provider.

    This is a Python facade that marshals to Rust implementation.
    """
    options = options or DeltaProviderOptions()

    # Marshal options to Rust-compatible format
    rust_options = {
        "storage_options": dict(options.storage_options) if options.storage_options else {},
        "scan_overrides": _marshal_scan_overrides(options.scan_overrides),
        "predicate_str": options.predicate_str,
    }

    # Call Rust implementation
    provider = datafusion._internal.create_delta_table_provider(
        ctx,
        table_uri,
        rust_options,
    )

    return DeltaProvider(
        table_uri=table_uri,
        provider=provider,
        options=options,
    )
```

**Target:** Evaluate if `_marshal_scan_overrides()` and other helper functions can be moved to Rust, further thinning the Python facade.

**Estimated reduction:** ~200-300 LOC (2,337 → ~2,050)

### Files to Edit

#### 1. `rust/datafusion_ext/src/delta_protocol.rs`

**Add PyO3 binding for protocol validation:**
- Add `validate_delta_protocol_py()` function
- Export in module initialization

#### 2. `src/datafusion_engine/delta/protocol.py`

**Simplify to thin wrapper:**
- Keep `DeltaFeatureGate` dataclass
- Replace `validate_delta_protocol()` body with Rust call
- Update docstring to note Rust implementation

#### 3. `src/datafusion_engine/delta/scan_policy_inference.py`

**Evaluate for deletion:**
- After Wave 0, check remaining callers
- If only TYPE_CHECKING imports, delete file
- If runtime usage exists, document why it's needed

#### 4. `src/datafusion_engine/delta/control_plane.py`

**Thin facade (optional):**
- Evaluate `_marshal_scan_overrides()` and similar helpers
- Move to Rust if feasible
- Document facade responsibilities in docstring

### Wave 3 Implementation Checklist

- [ ] **Pre-flight checks**
  - [ ] Verify Wave 0 complete (pipeline_runtime.py deleted)
  - [ ] Run `/cq calls ScanPolicyOverride` to find all usages
  - [ ] Run `/cq calls validate_delta_protocol` to find all callers
  - [ ] Run full test suite: `uv run pytest tests/ -m "not e2e" -q`

- [ ] **Add Rust PyO3 binding**
  - [ ] Edit `rust/datafusion_ext/src/delta_protocol.rs`:
    - [ ] Add `use pyo3::prelude::*;`
    - [ ] Add `validate_delta_protocol_py()` function with PyO3 annotations
    - [ ] Export in module initialization
  - [ ] Rebuild Rust: `bash scripts/rebuild_rust_artifacts.sh`
  - [ ] Verify binding available: `uv run python -c "from datafusion._internal import validate_delta_protocol_py"`

- [ ] **Simplify delta/protocol.py**
  - [ ] Keep `DeltaFeatureGate` dataclass (used by callers)
  - [ ] Replace `validate_delta_protocol()` body with Rust call via `datafusion._internal.validate_delta_protocol_py()`
  - [ ] Update docstring to document Rust implementation
  - [ ] Run tests: `uv run pytest tests/unit/datafusion_engine/delta/test_protocol.py -v`

- [ ] **Evaluate scan_policy_inference.py**
  - [ ] Check `src/relspec/policy_compiler.py` line 20 for runtime usage
  - [ ] Run `/cq calls ScanPolicyOverride` — if only TYPE_CHECKING, proceed with deletion
  - [ ] If safe to delete:
    - [ ] Delete `src/datafusion_engine/delta/scan_policy_inference.py`
    - [ ] Remove import from `src/datafusion_engine/delta/__init__.py`
    - [ ] Remove TYPE_CHECKING import from `src/relspec/policy_compiler.py`
  - [ ] If not safe, document why it's still needed

- [ ] **Optional: Thin control_plane.py**
  - [ ] Identify helper functions that could move to Rust
  - [ ] Document facade responsibilities in module docstring
  - [ ] Defer deeper refactoring to future work (not blocking)

- [ ] **Post-flight checks**
  - [ ] Run `uv run ruff format`
  - [ ] Run `uv run ruff check --fix`
  - [ ] Run `uv run pyrefly check`
  - [ ] Run `uv run pyright`
  - [ ] Run full test suite: `uv run pytest tests/ -m "not e2e" -q`
  - [ ] Run Delta integration test: `uv run pytest tests/integration/delta/ -v`

**Expected outcome:** ~500-800 LOC removed, protocol validation moved to Rust, all tests pass.

---

## Wave 4: Monolith Decomposition

**Scope:** Restructure the two largest files for maintainability. Zero net LOC change — this is code organization only.

### 4A: Split `session/runtime.py` (8,482 LOC → 4 files)

This is the largest file in the entire codebase. It conflates extraction-phase session construction with feature gating, introspection, and configuration — concerns that span multiple responsibility domains.

**Current structure:**
```
session/runtime.py (8,482 LOC, 194 top-level definitions)
├── Runtime profile management (~1,500 LOC)
├── Session construction and configuration (~2,000 LOC)
├── Feature gate resolution (~1,000 LOC)
├── UDF registration orchestration (~500 LOC)
├── Dataset introspection (~1,500 LOC)
├── Schema validation helpers (~500 LOC)
├── Delta session integration (~500 LOC)
├── Cache policy resolution (~300 LOC)
└── Encoding/normalization helpers (~200 LOC)
```

**Target structure:**
```
session/
├── __init__.py          # Public re-exports (unchanged API surface)
├── config.py            # NEW: Configuration classes and profile types (~1,500 LOC)
├── features.py          # NEW: Feature gates and capability resolution (~1,000 LOC)
├── introspection.py     # NEW: Dataset/schema/UDF introspection helpers (~2,000 LOC)
├── runtime.py           # REDUCED: Core session construction (~4,000 LOC)
├── facade.py            # Existing (unchanged)
├── factory.py           # Existing (unchanged)
├── delta_session_builder.py  # Existing (unchanged)
├── helpers.py           # Existing (unchanged)
├── streaming.py         # Existing (unchanged)
├── cache_policy.py      # Existing (unchanged)
```

#### Extract `session/config.py` (~1,500 LOC)

**Code snippet for new file:**
```python
"""Session configuration types and runtime profile definitions.

Extracted from session/runtime.py to isolate configuration concerns
from session construction logic.

DataFusion best practice: Use SessionConfig.with_*() builder methods
for deterministic session construction. Key settings:
- target_partitions: Controls parallelism (CPU-bound)
- batch_size: Controls memory granularity (8192 default)
- collect_statistics: true (DataFusion 48+) for cost-based planning
- parquet.pushdown_filters: true for late materialization
- parquet.enable_page_index: true for page-level pruning
- optimizer.max_passes: 3 for deterministic rule application
- optimizer.skip_failed_rules: false for strict plan validation
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal

import msgspec
from core.config_base import FingerprintableConfig, config_fingerprint
from core_types import DeterminismTier

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence


@dataclass(frozen=True)
class DataFusionRuntimeProfile:
    """Immutable runtime profile for DataFusion session construction.

    Controls session-level configuration: batch sizes, memory limits,
    parallelism, feature gates, and Delta store policies.

    Attributes
    ----------
    target_partitions : int
        Number of parallel partitions (default: CPU count).
    batch_size : int
        Row batch size for processing (default: 8192).
    memory_limit_mb : int | None
        Memory limit in MB (None = unlimited).
    collect_statistics : bool
        Enable statistics collection for cost-based planning (default: True).
    parquet_pushdown_filters : bool
        Enable Parquet filter pushdown (default: True).
    parquet_enable_page_index : bool
        Enable Parquet page-level pruning (default: True).
    optimizer_max_passes : int
        Maximum optimizer passes (default: 3).
    optimizer_skip_failed_rules : bool
        Skip failed optimizer rules (default: False for strict validation).
    """
    target_partitions: int = field(default_factory=lambda: os.cpu_count() or 4)
    batch_size: int = 8192
    memory_limit_mb: int | None = None
    collect_statistics: bool = True  # DataFusion 48+ default
    parquet_pushdown_filters: bool = True
    parquet_enable_page_index: bool = True
    optimizer_max_passes: int = 3
    optimizer_skip_failed_rules: bool = False

    # ... more profile fields extracted from runtime.py ...


@dataclass(frozen=True)
class SessionConfigOptions:
    """Session configuration options for extraction phase.

    These options control DataFusion session behavior during extraction
    query execution. They are separate from CPG execution configuration
    (which is owned by Rust).
    """
    profile: DataFusionRuntimeProfile = field(default_factory=DataFusionRuntimeProfile)
    enable_function_factory: bool = False
    enable_expr_planners: bool = False

    # ... more config options extracted from runtime.py ...


def default_extraction_profile() -> DataFusionRuntimeProfile:
    """Return default runtime profile for extraction phase.

    Extraction queries are typically CPU-bound with moderate memory usage.
    """
    return DataFusionRuntimeProfile(
        target_partitions=os.cpu_count() or 4,
        batch_size=8192,
        memory_limit_mb=4096,  # 4GB default for extraction
        collect_statistics=True,
        parquet_pushdown_filters=True,
        parquet_enable_page_index=True,
    )


# ... more functions extracted from runtime.py ...
```

#### Extract `session/features.py` (~1,000 LOC)

**Code snippet for new file:**
```python
"""Feature gate resolution and capability detection.

Extracted from session/runtime.py to isolate feature-gate logic
from session construction.

Feature gates control optional DataFusion capabilities:
- Schema evolution adapters (Rust-native via SessionStateBuilder)
- CDF (Change Data Feed) input registration
- Parquet metadata caching (list_files_cache_limit, metadata_cache_limit)
- Statistics collection (collect_statistics)
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datafusion import SessionContext


@dataclass(frozen=True)
class FeatureGateSnapshot:
    """Snapshot of resolved feature gates for a session.

    Feature gates control optional DataFusion capabilities. They are
    resolved at session construction time and remain immutable for the
    session's lifetime.

    Attributes
    ----------
    schema_evolution_adapter : bool
        Enable Rust-native schema evolution adapters.
    cdf_enabled : bool
        Enable Change Data Feed (CDF) input registration.
    metadata_cache_enabled : bool
        Enable Parquet metadata caching.
    statistics_collection : bool
        Enable statistics collection (DataFusion 48+ default).
    """
    schema_evolution_adapter: bool = False
    cdf_enabled: bool = False
    metadata_cache_enabled: bool = False
    statistics_collection: bool = True  # DataFusion 48+ default


def resolve_feature_gates(
    ctx: SessionContext,
    options: SessionConfigOptions,
) -> FeatureGateSnapshot:
    """Resolve feature gates for a session.

    Feature gates are resolved based on session configuration and
    runtime environment. They control optional capabilities.

    Parameters
    ----------
    ctx : SessionContext
        DataFusion session to resolve gates for.
    options : SessionConfigOptions
        Session configuration options.

    Returns
    -------
    FeatureGateSnapshot
        Resolved feature gate snapshot.
    """
    # ... feature gate resolution logic extracted from runtime.py ...


# ... more functions extracted from runtime.py ...
```

#### Extract `session/introspection.py` (~2,000 LOC)

**Code snippet for new file:**
```python
"""Dataset, schema, and UDF introspection helpers.

Extracted from session/runtime.py to isolate introspection concerns
from session construction logic.

These helpers query registered tables, schemas, and UDFs from DataFusion
sessions for extraction-phase workflows.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

import pyarrow as pa

if TYPE_CHECKING:
    from datafusion import SessionContext
    from collections.abc import Sequence


def registered_tables(ctx: SessionContext) -> frozenset[str]:
    """Return all registered table names in the session.

    Parameters
    ----------
    ctx : SessionContext
        DataFusion session to introspect.

    Returns
    -------
    frozenset[str]
        Registered table names.
    """
    # ... introspection logic extracted from runtime.py ...


def table_schema(ctx: SessionContext, table_name: str) -> pa.Schema:
    """Return the PyArrow schema for a registered table.

    Parameters
    ----------
    ctx : SessionContext
        DataFusion session to query.
    table_name : str
        Table name to get schema for.

    Returns
    -------
    pa.Schema
        Table schema.

    Raises
    ------
    KeyError
        If table is not registered.
    """
    # ... schema lookup logic extracted from runtime.py ...


def registered_udfs(ctx: SessionContext) -> frozenset[str]:
    """Return all registered UDF names in the session.

    Parameters
    ----------
    ctx : SessionContext
        DataFusion session to introspect.

    Returns
    -------
    frozenset[str]
        Registered UDF names.
    """
    # ... UDF introspection logic extracted from runtime.py ...


# ... more functions extracted from runtime.py ...
```

#### Update `session/__init__.py`

**Code snippet for updated public API:**
```python
"""DataFusion session construction and runtime for extraction phase.

This module provides session construction, configuration, feature gates,
and introspection for the extraction phase. The Rust engine owns session
construction for CPG execution.

Public API (unchanged after decomposition):
- DataFusionRuntimeProfile (now in config.py)
- SessionConfigOptions (now in config.py)
- FeatureGateSnapshot (now in features.py)
- registered_tables() (now in introspection.py)
- table_schema() (now in introspection.py)
- ... (all existing public exports maintained)
"""
from __future__ import annotations

# Re-export from decomposed modules (maintain existing API)
from datafusion_engine.session.config import (
    DataFusionRuntimeProfile,
    SessionConfigOptions,
    default_extraction_profile,
)
from datafusion_engine.session.features import (
    FeatureGateSnapshot,
    resolve_feature_gates,
)
from datafusion_engine.session.introspection import (
    registered_tables,
    table_schema,
    registered_udfs,
)
from datafusion_engine.session.runtime import (
    # Core session construction remains in runtime.py
    build_extraction_session,
    # ... other runtime exports ...
)

__all__ = [
    # Config
    "DataFusionRuntimeProfile",
    "SessionConfigOptions",
    "default_extraction_profile",
    # Features
    "FeatureGateSnapshot",
    "resolve_feature_gates",
    # Introspection
    "registered_tables",
    "table_schema",
    "registered_udfs",
    # Runtime
    "build_extraction_session",
    # ... (maintain all existing public exports)
]
```

### 4B: Split `schema/registry.py` (4,166 LOC → 3 files after Wave 1 pruning)

After Wave 1 removes ~56 dead exports, the remaining live code (~3,850 LOC) should be split by responsibility domain.

**Target structure:**
```
schema/
├── __init__.py              # Public re-exports
├── extraction_schemas.py    # NEW: Schemas for extraction table outputs (~1,500 LOC)
├── nested_views.py          # NEW: Nested view spec helpers (~500 LOC)
├── observability_schemas.py # NEW: Pipeline event and artifact schemas (~300 LOC)
├── registry.py              # REMOVED (split into above three)
├── alignment.py             # Existing (unchanged)
├── catalog_contracts.py     # Existing (unchanged)
├── contracts.py             # Existing (unchanged)
├── derivation.py            # Existing (unchanged)
├── field_types.py           # Existing (unchanged)
├── finalize.py              # Existing (unchanged)
├── introspection.py         # Existing (unchanged)
├── policy.py                # Existing (unchanged)
├── spec_protocol.py         # Existing (unchanged)
├── validation.py            # Existing (unchanged)
```

#### Extract `schema/extraction_schemas.py` (~1,500 LOC)

**Code snippet for new file:**
```python
"""Schema definitions for extraction evidence tables.

These schemas define the Arrow schema contracts for extraction outputs.
DataFusion best practice: Use explicit schemas rather than inference
for stability. Schemas should declare NOT NULL constraints via Arrow
Field.nullable=False and use schema metadata for semantic annotations.

All extraction schemas use byte span types (bstart/bend) as canonical
coordinates. These are defined in datafusion_engine.arrow.semantic.
"""
from __future__ import annotations

import pyarrow as pa

from datafusion_engine.arrow.semantic import byte_span_type, span_type

# Core extraction schemas used by extract_schema_for()

# AST extraction schemas
AST_NODES_SCHEMA: pa.Schema = pa.schema([
    pa.field("file_id", pa.utf8(), nullable=False, metadata={"semantic_type": "file_id"}),
    pa.field("node_id", pa.utf8(), nullable=False, metadata={"semantic_type": "node_id"}),
    pa.field("node_type", pa.utf8(), nullable=False),
    pa.field("bstart", pa.int64(), nullable=False, metadata={"semantic_type": "byte_offset"}),
    pa.field("bend", pa.int64(), nullable=False, metadata={"semantic_type": "byte_offset"}),
    # ... more fields extracted from registry.py ...
])

AST_EDGES_SCHEMA: pa.Schema = pa.schema([
    pa.field("source_id", pa.utf8(), nullable=False, metadata={"semantic_type": "node_id"}),
    pa.field("target_id", pa.utf8(), nullable=False, metadata={"semantic_type": "node_id"}),
    pa.field("edge_type", pa.utf8(), nullable=False),
    # ... more fields extracted from registry.py ...
])

# LibCST extraction schemas
CST_NODES_SCHEMA: pa.Schema = pa.schema([
    # ... extracted from registry.py ...
])

# Bytecode extraction schemas
BYTECODE_INSTRUCTIONS_SCHEMA: pa.Schema = pa.schema([
    # ... extracted from registry.py ...
])

# SCIP extraction schemas
SCIP_OCCURRENCES_SCHEMA: pa.Schema = pa.schema([
    # ... extracted from registry.py ...
])

# Symtable extraction schemas
SYMTABLE_SYMBOLS_SCHEMA: pa.Schema = pa.schema([
    # ... extracted from registry.py ...
])

# Tree-sitter extraction schemas
TS_NODES_SCHEMA: pa.Schema = pa.schema([
    # ... extracted from registry.py ...
])

# Schema registry mapping table names to schemas
_EXTRACTION_SCHEMA_REGISTRY: dict[str, pa.Schema] = {
    "ast_nodes_v1": AST_NODES_SCHEMA,
    "ast_edges_v1": AST_EDGES_SCHEMA,
    "cst_nodes_v1": CST_NODES_SCHEMA,
    "bytecode_instructions_v1": BYTECODE_INSTRUCTIONS_SCHEMA,
    "scip_occurrences_v1": SCIP_OCCURRENCES_SCHEMA,
    "symtable_symbols_v1": SYMTABLE_SYMBOLS_SCHEMA,
    "ts_nodes_v1": TS_NODES_SCHEMA,
    # ... more mappings extracted from registry.py ...
}


def extract_schema_for(table_name: str) -> pa.Schema:
    """Return the PyArrow schema for a registered extraction table.

    This is the primary schema lookup API used across extraction and
    semantic compilation. Used by 18+ files.

    Parameters
    ----------
    table_name : str
        Table name to get schema for.

    Returns
    -------
    pa.Schema
        Table schema with semantic metadata.

    Raises
    ------
    KeyError
        If table is not registered.
    """
    if table_name not in _EXTRACTION_SCHEMA_REGISTRY:
        raise KeyError(f"No extraction schema for table: {table_name}")
    return _EXTRACTION_SCHEMA_REGISTRY[table_name]


# ... more functions extracted from registry.py ...
```

#### Extract `schema/nested_views.py` (~500 LOC)

**Code snippet for new file:**
```python
"""Nested view specification and registration helpers.

Extracted from schema/registry.py to isolate nested view concerns.

Nested views are DataFusion views that project struct fields from
extraction tables. They are registered during session setup to provide
flat access to nested data.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyarrow as pa

if TYPE_CHECKING:
    from datafusion import SessionContext


@dataclass(frozen=True)
class NestedViewSpec:
    """Specification for a nested view registration.

    Attributes
    ----------
    view_name : str
        Name of the view to create.
    source_table : str
        Source table containing nested struct.
    nested_path : str
        Dot-separated path to nested field (e.g., "metadata.spans").
    """
    view_name: str
    source_table: str
    nested_path: str


def nested_view_specs() -> list[NestedViewSpec]:
    """Return all nested view specifications.

    Used by session setup for nested view registration.

    Returns
    -------
    list[NestedViewSpec]
        All nested view specifications.
    """
    # ... logic extracted from registry.py ...


def nested_view_spec(table_name: str) -> NestedViewSpec | None:
    """Return the nested view spec for a table name, if it exists.

    Parameters
    ----------
    table_name : str
        Table name to lookup.

    Returns
    -------
    NestedViewSpec | None
        Nested view spec if table is a nested view, None otherwise.
    """
    # ... logic extracted from registry.py ...


def nested_path_for(table_name: str) -> str | None:
    """Return the nested path for a table name, if it is a nested view.

    Parameters
    ----------
    table_name : str
        Table name to lookup.

    Returns
    -------
    str | None
        Nested path if table is a nested view, None otherwise.
    """
    # ... logic extracted from registry.py ...


# ... more functions extracted from registry.py ...
```

#### Extract `schema/observability_schemas.py` (~300 LOC)

**Code snippet for new file:**
```python
"""Schema definitions for pipeline observability events and artifacts.

These schemas define the Arrow schema contracts for pipeline events,
plan artifacts, and execution metrics. They are used by the observability
layer (src/obs/) for telemetry collection.

DataFusion best practice: Use explicit schemas with semantic metadata
for observability data to enable stable querying and aggregation.
"""
from __future__ import annotations

import pyarrow as pa

# Pipeline event schemas (V2)
DATAFUSION_PIPELINE_EVENTS_V2_SCHEMA: pa.Schema = pa.schema([
    pa.field("event_id", pa.utf8(), nullable=False, metadata={"semantic_type": "event_id"}),
    pa.field("timestamp_ns", pa.int64(), nullable=False, metadata={"semantic_type": "timestamp"}),
    pa.field("event_type", pa.utf8(), nullable=False),
    pa.field("task_name", pa.utf8(), nullable=True),
    # ... more fields extracted from registry.py ...
])

# Plan artifact schemas
DATAFUSION_PLAN_ARTIFACTS_SCHEMA: pa.Schema = pa.schema([
    pa.field("artifact_id", pa.utf8(), nullable=False, metadata={"semantic_type": "artifact_id"}),
    pa.field("plan_type", pa.utf8(), nullable=False),  # "logical", "optimized", "physical"
    pa.field("plan_repr", pa.utf8(), nullable=False),
    # ... more fields extracted from registry.py ...
])

# ... more schemas extracted from registry.py ...
```

#### Update `schema/__init__.py`

**Code snippet for updated public API:**
```python
"""DataFusion schema definitions and registry.

This module provides schema definitions for extraction tables, nested
views, and observability artifacts.

Public API (unchanged after decomposition):
- extract_schema_for() (now in extraction_schemas.py)
- nested_view_specs() (now in nested_views.py)
- DATAFUSION_PLAN_ARTIFACTS_SCHEMA (now in observability_schemas.py)
- ... (all existing public exports maintained)
"""
from __future__ import annotations

# Re-export from decomposed modules (maintain existing API)
from datafusion_engine.schema.extraction_schemas import (
    extract_schema_for,
    AST_NODES_SCHEMA,
    AST_EDGES_SCHEMA,
    # ... other extraction schemas ...
)
from datafusion_engine.schema.nested_views import (
    NestedViewSpec,
    nested_view_specs,
    nested_view_spec,
    nested_path_for,
)
from datafusion_engine.schema.observability_schemas import (
    DATAFUSION_PIPELINE_EVENTS_V2_SCHEMA,
    DATAFUSION_PLAN_ARTIFACTS_SCHEMA,
)

__all__ = [
    # Extraction schemas
    "extract_schema_for",
    "AST_NODES_SCHEMA",
    "AST_EDGES_SCHEMA",
    # Nested views
    "NestedViewSpec",
    "nested_view_specs",
    "nested_view_spec",
    "nested_path_for",
    # Observability schemas
    "DATAFUSION_PIPELINE_EVENTS_V2_SCHEMA",
    "DATAFUSION_PLAN_ARTIFACTS_SCHEMA",
    # ... (maintain all existing public exports)
]
```

### Wave 4 Implementation Checklist

- [ ] **Pre-flight checks**
  - [ ] Verify Wave 1 complete (dead exports removed from registry.py)
  - [ ] Run full test suite: `uv run pytest tests/ -m "not e2e" -q`
  - [ ] Document current import patterns: `rg "from datafusion_engine.session.runtime import" src/ tests/`
  - [ ] Document current import patterns: `rg "from datafusion_engine.schema.registry import" src/ tests/`

- [ ] **Split session/runtime.py**
  - [ ] Create `src/datafusion_engine/session/config.py`
    - [ ] Extract `DataFusionRuntimeProfile` and related classes
    - [ ] Extract profile factory functions
    - [ ] Extract ~1,500 LOC of config logic
  - [ ] Create `src/datafusion_engine/session/features.py`
    - [ ] Extract `FeatureGateSnapshot` and related classes
    - [ ] Extract feature gate resolution logic
    - [ ] Extract ~1,000 LOC of feature logic
  - [ ] Create `src/datafusion_engine/session/introspection.py`
    - [ ] Extract introspection functions (`registered_tables`, `table_schema`, etc.)
    - [ ] Extract ~2,000 LOC of introspection logic
  - [ ] Update `src/datafusion_engine/session/runtime.py`
    - [ ] Remove extracted code
    - [ ] Add imports from new modules
    - [ ] Retain ~4,000 LOC of core session construction
  - [ ] Update `src/datafusion_engine/session/__init__.py`
    - [ ] Add re-exports from new modules
    - [ ] Maintain existing public API surface

- [ ] **Split schema/registry.py**
  - [ ] Create `src/datafusion_engine/schema/extraction_schemas.py`
    - [ ] Extract all extraction schema constants
    - [ ] Extract `extract_schema_for()` function
    - [ ] Extract ~1,500 LOC of extraction schema logic
  - [ ] Create `src/datafusion_engine/schema/nested_views.py`
    - [ ] Extract `NestedViewSpec` and related classes
    - [ ] Extract nested view registration logic
    - [ ] Extract ~500 LOC of nested view logic
  - [ ] Create `src/datafusion_engine/schema/observability_schemas.py`
    - [ ] Extract observability schema constants
    - [ ] Extract ~300 LOC of observability schema logic
  - [ ] Delete `src/datafusion_engine/schema/registry.py` (fully decomposed)
  - [ ] Update `src/datafusion_engine/schema/__init__.py`
    - [ ] Add re-exports from new modules
    - [ ] Maintain existing public API surface

- [ ] **Update imports across codebase**
  - [ ] Run `uv run ruff check --fix` to auto-fix some imports
  - [ ] Manually fix remaining imports if needed
  - [ ] Verify no broken imports: `uv run python -c "from datafusion_engine.session import *"`
  - [ ] Verify no broken imports: `uv run python -c "from datafusion_engine.schema import *"`

- [ ] **Post-flight checks**
  - [ ] Run `uv run ruff format`
  - [ ] Run `uv run ruff check --fix`
  - [ ] Run `uv run pyrefly check`
  - [ ] Run `uv run pyright`
  - [ ] Run full test suite: `uv run pytest tests/ -m "not e2e" -q`
  - [ ] Run extraction pipeline smoke test: `uv run python -m extraction.orchestrator --repo-root . --work-dir /tmp/test`
  - [ ] Verify public API unchanged: Document any breaking changes (should be none)

**Expected outcome:** 12,648 LOC restructured (0 net change), improved maintainability, all tests pass, public API unchanged.

---

## Wave 5: Rust Transition Preparation (Future Scope)

**Scope:** Prepare the codebase for extraction phase transition to Rust. This wave does NOT implement the transition but marks code boundaries and creates Rust-side stubs.

### 5A: Session Factory Transition Preparation

**Rust side already implements best-in-class session construction:**

```rust
// rust/codeanatomy_engine/src/session/factory.rs (EXISTING)
// Already implements DataFusion best practices

pub fn build_execution_session(
    config: &ExecutionConfig,
) -> Result<SessionContext> {
    // SessionStateBuilder (builder-first, no post-build mutation)
    let mut builder = SessionStateBuilder::new()
        .with_default_features()
        .with_config(session_config_from_execution_config(config));

    // FairSpillPool (bounded memory, spill-to-disk)
    let runtime_env = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::new(FairSpillPool::new(
            config.memory_limit_bytes,
        )))
        .build()?;
    builder = builder.with_runtime_env(Arc::new(runtime_env));

    // Register UDFs (Rust-native)
    for udf in codeanatomy_udfs() {
        builder = builder.with_scalar_udf(udf);
    }

    // Build session
    let state = builder.build();
    Ok(SessionContext::new_with_state(state))
}

fn session_config_from_execution_config(config: &ExecutionConfig) -> SessionConfig {
    SessionConfig::new()
        .with_target_partitions(config.target_partitions)
        .with_batch_size(config.batch_size)
        .with_collect_statistics(true)  // DataFusion 48+ cost-based planning
        .with_parquet_pushdown_filters(true)  // Late materialization
        .with_parquet_enable_page_index(true)  // Page-level pruning
        .set_bool("optimizer.skip_failed_rules", false)  // Strict validation
        .set_usize("optimizer.max_passes", 3)  // Deterministic, bounded
        .set_bool("execution.enable_dynamic_filter_pushdown", true)  // DF 51+
        .set_bool("execution.filter_null_join_keys", true)  // Join optimization
        .set_bool("catalog.information_schema", true)  // Catalog introspection
}
```

**Key patterns already implemented:**
1. SessionStateBuilder (builder-first construction)
2. FairSpillPool (bounded memory management)
3. collect_statistics = true (cost-based optimizer)
4. parquet.pushdown_filters = true (late materialization)
5. parquet.enable_page_index = true (page-level pruning)
6. optimizer.skip_failed_rules = false (strict validation)
7. optimizer.max_passes = 3 (deterministic, bounded)
8. enable_dynamic_filter_pushdown = true (DF 51+)
9. filter_null_join_keys = true (join optimization)
10. information_schema = true (catalog introspection)

**Rust extraction session stub (future):**

```rust
// rust/codeanatomy_engine/src/session/extraction.rs (FUTURE STUB)
// When extraction transitions to Rust, this module will provide
// extraction-specific session construction.

pub fn build_extraction_session(
    config: &ExtractionConfig,
) -> Result<SessionContext> {
    // Extraction-specific SessionConfig (different from execution)
    let session_config = SessionConfig::new()
        .with_target_partitions(config.parallelism)
        .with_batch_size(8192)  // Extraction default
        .with_collect_statistics(true)
        .with_parquet_pushdown_filters(true)
        .with_parquet_enable_page_index(true);

    // Extraction-specific memory pool (different limits)
    let runtime_env = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::new(FairSpillPool::new(
            config.memory_limit_bytes.unwrap_or(4 * 1024 * 1024 * 1024),  // 4GB default
        )))
        .build()?;

    let mut builder = SessionStateBuilder::new()
        .with_config(session_config)
        .with_runtime_env(Arc::new(runtime_env));

    // Register extraction-specific UDFs (col_to_byte, span_id, etc.)
    for udf in extraction_udfs() {
        builder = builder.with_scalar_udf(udf);
    }

    // Register Delta providers for extraction inputs
    for input_spec in &config.input_tables {
        let provider = create_delta_table_provider(&input_spec.uri, &input_spec.options)?;
        builder = builder.with_table_provider(
            input_spec.table_name.clone(),
            Arc::new(provider),
        );
    }

    let state = builder.build();
    Ok(SessionContext::new_with_state(state))
}
```

**Mark Python session factory for transition:**

```python
# src/datafusion_engine/session/factory.py (ADD MARKER COMMENT)

# RUST_TRANSITION: This module will become dead when extraction transitions
# to Rust. The Rust side already has best-in-class session construction
# in rust/codeanatomy_engine/src/session/factory.rs. The extraction-specific
# session builder will be added to rust/codeanatomy_engine/src/session/extraction.rs.
#
# Key DataFusion APIs to use in Rust:
# - SessionStateBuilder::new() for deterministic construction
# - RuntimeEnvBuilder with extraction-specific memory pool
# - register_table() for Delta extraction inputs
# - DataFrame.cache() for hot extraction intermediates
#
# Estimated transition: Wave 5 (future scope, requires extraction → Rust)
```

### 5B: Lineage Transition Preparation

**Rust lineage extraction stub (future):**

```rust
// rust/codeanatomy_engine/src/compiler/lineage.rs (FUTURE STUB)
// When lineage transitions to Rust, this module will provide
// Rust-native lineage extraction from LogicalPlan.

use datafusion::logical_expr::{LogicalPlan, Expr};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use std::collections::HashSet;

/// Extract referenced table names from a LogicalPlan.
///
/// DataFusion provides plan introspection via:
/// - LogicalPlan::inputs() — child plan references
/// - LogicalPlan.apply() / TreeNode trait — recursive plan walking
/// - LogicalPlan::TableScan variant — extract table references
/// - Expr::Column — extract column references
pub fn referenced_tables(plan: &LogicalPlan) -> Result<Vec<String>> {
    let mut tables = HashSet::new();

    plan.apply(|node| {
        if let LogicalPlan::TableScan(scan) = node {
            tables.insert(scan.table_name.to_string());
        }
        Ok(TreeNodeRecursion::Continue)
    })?;

    Ok(tables.into_iter().collect())
}

/// Extract referenced columns from a LogicalPlan.
pub fn referenced_columns(plan: &LogicalPlan) -> Result<Vec<(String, String)>> {
    let mut columns = HashSet::new();

    plan.apply(|node| {
        if let LogicalPlan::TableScan(scan) = node {
            // Extract projection columns
            if let Some(projection) = &scan.projection {
                for idx in projection {
                    if let Some(field) = scan.source.schema().field(*idx).ok() {
                        columns.insert((
                            scan.table_name.to_string(),
                            field.name().to_string(),
                        ));
                    }
                }
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })?;

    Ok(columns.into_iter().collect())
}
```

**Mark Python lineage for transition:**

```python
# src/datafusion_engine/lineage/datafusion.py (ADD MARKER COMMENT)

# RUST_TRANSITION: This module's core lineage extraction logic will move
# to Rust when scheduling fully transitions. The Rust side will use
# DataFusion's TreeNode trait for plan walking and LogicalPlan introspection.
#
# Key DataFusion APIs to use in Rust:
# - LogicalPlan::inputs() — child plan references
# - LogicalPlan.apply() / TreeNode trait — recursive plan walking
# - LogicalPlan::TableScan variant — extract table references
# - Expr::Column — extract column references
#
# Python's lineage/diagnostics.py will stay (observability concern).
#
# Estimated transition: Wave 5 (future scope, requires scheduling → Rust)
```

### Wave 5 Implementation Checklist (FUTURE)

This checklist is for future reference when extraction transitions to Rust.

- [ ] **Extraction session transition**
  - [ ] Create `rust/codeanatomy_engine/src/session/extraction.rs`
  - [ ] Implement `build_extraction_session()` with extraction-specific config
  - [ ] Register extraction-specific UDFs (col_to_byte, span_id, etc.)
  - [ ] Register Delta providers for extraction inputs
  - [ ] Expose via PyO3 for Python extraction orchestrator
  - [ ] Update Python extraction to call Rust session builder
  - [ ] Delete `src/datafusion_engine/session/factory.py` (~433 LOC)

- [ ] **Plan execution transition**
  - [ ] Move extraction plan execution to Rust
  - [ ] Delete `src/datafusion_engine/plan/execution_runtime.py` (~348 LOC)

- [ ] **Lineage transition**
  - [ ] Create `rust/codeanatomy_engine/src/compiler/lineage.rs`
  - [ ] Implement `referenced_tables()` using TreeNode trait
  - [ ] Implement `referenced_columns()` using plan introspection
  - [ ] Expose via PyO3 if Python needs lineage data
  - [ ] Delete `src/datafusion_engine/lineage/datafusion.py` (~570 LOC)
  - [ ] Evaluate `src/datafusion_engine/lineage/scan.py` for deletion (~824 LOC)
  - [ ] Keep `src/datafusion_engine/lineage/diagnostics.py` (observability)

- [ ] **Dataset registration transition**
  - [ ] Move extraction input registration to Rust
  - [ ] Delete remaining Python registration code (~2,500 LOC)

- [ ] **UDF metadata transition**
  - [ ] Remove Python UDF metadata layer (no longer needed)
  - [ ] Delete `src/datafusion_engine/udf/catalog.py` (~1,215 LOC)
  - [ ] Delete `src/datafusion_engine/udf/runtime.py` (~1,577 LOC post-Wave 2)

**Expected outcome (when complete):** ~7,567 LOC removed, extraction phase fully in Rust.

---

## Cross-Cutting Concerns

### DataFusion Version Alignment

**Current versions:**
- Python: `datafusion` 51.0.0 (PyPI)
- Rust: `datafusion` 51.0 (Cargo.toml)

**Policy:** Keep Python and Rust DataFusion versions in sync. When upgrading DataFusion:
1. Update `pyproject.toml` → `datafusion = "X.Y.Z"`
2. Update `rust/Cargo.toml` → `datafusion = "X.Y"`
3. Run `bash scripts/rebuild_rust_artifacts.sh`
4. Run full test suite
5. Update any code for breaking changes

**Breaking change tracking:**
- DataFusion 48: `collect_statistics` default changed to true
- DataFusion 50: `FairSpillPool` API updated
- DataFusion 51: `enable_dynamic_filter_pushdown` added

### Test Impact Analysis

#### Wave 0: Dead Code Deletion

**Test files to delete:**
- `tests/unit/datafusion_engine/plan/test_pipeline_scan_policy_inference_wiring.py`

**Test files potentially affected:**
- None (zero production callers)

#### Wave 1: Dead Export Pruning

**Test files potentially affected:**
- `tests/unit/datafusion_engine/schema/test_registry.py` — May have tests for dead exports
- `tests/unit/datafusion_engine/dataset/test_registration.py` — May have tests for dead exports

**Action:** Review test files, remove tests for deleted exports.

#### Wave 2: UDF Bridge Simplification

**Test files affected:**
- `tests/unit/datafusion_engine/udf/test_platform.py` — Remove fallback path tests
- `tests/unit/datafusion_engine/udf/test_runtime.py` — Update tests for hard requirement
- `tests/integration/udf/` — Update integration tests if needed

#### Wave 3: Delta Control-Plane Thinning

**Test files affected:**
- `tests/unit/datafusion_engine/delta/test_protocol.py` — Update for Rust-backed validation
- `tests/integration/delta/` — Verify Delta integration still works

#### Wave 4: Monolith Decomposition

**Test files affected:**
- `tests/unit/datafusion_engine/session/test_runtime.py` — Update imports
- `tests/unit/datafusion_engine/schema/test_registry.py` — Update imports
- All test files that import from `session.runtime` or `schema.registry`

**Action:** Run `uv run ruff check --fix` to auto-fix imports, manually fix remaining.

### Import Cleanup Strategy

After each wave, run import cleanup:

```bash
# Auto-fix imports
uv run ruff check --fix

# Check for unused imports
uv run ruff check --select F401

# Check for missing imports
uv run pyrefly check

# Verify no import errors
uv run python -c "from datafusion_engine import *"
```

### Quality Gate (Run After Each Wave)

```bash
# Single command for all checks
uv run ruff format && \
uv run ruff check --fix && \
uv run pyrefly check && \
uv run pyright && \
uv run pytest tests/ -m "not e2e" -q
```

**Timing:** Run quality gate AFTER completing each wave, not mid-task.

---

## Execution Dependencies

```mermaid
graph TD
    W0[Wave 0: Dead Code Deletion]
    W1[Wave 1: Dead Export Pruning]
    W2[Wave 2: UDF Bridge Simplification]
    W3[Wave 3: Delta Control-Plane Thinning]
    W4[Wave 4: Monolith Decomposition]
    W5[Wave 5: Rust Transition Prep]

    W0 --> W3
    W1 --> W4

    W0 -.->|Independent| W1
    W0 -.->|Independent| W2
    W2 -.->|Independent| W3
    W3 -.->|Independent| W4
    W4 -.->|Independent| W5

    style W0 fill:#d4edda
    style W1 fill:#d4edda
    style W2 fill:#fff3cd
    style W3 fill:#fff3cd
    style W4 fill:#f8d7da
    style W5 fill:#d1ecf1
```

**Legend:**
- Solid arrows: Hard dependency (must complete before dependent wave)
- Dashed arrows: Independent (can run in parallel)
- Green: Zero risk (deletions only)
- Yellow: Low risk (simplifications)
- Red: Medium risk (structural changes)
- Blue: Future scope (preparation only)

**Execution order:**
1. **Wave 0** must complete before **Wave 3** (scan_policy_inference.py evaluation depends on pipeline_runtime.py deletion)
2. **Wave 1** must complete before **Wave 4** (schema/registry.py split depends on dead export removal)
3. All other waves can proceed independently

**Recommended sequence for minimal risk:**
1. Wave 0 (dead code deletion) — Safest, highest impact
2. Wave 1 (dead export pruning) — Low risk, enables Wave 4
3. Wave 2 (UDF simplification) — Independent, medium impact
4. Wave 3 (Delta thinning) — Depends on Wave 0, medium impact
5. Wave 4 (monolith decomposition) — Depends on Wave 1, highest effort
6. Wave 5 (Rust transition prep) — Future scope, documentation only

---

## Success Criteria

### Wave 0 Success

- [ ] All 5 dead files deleted
- [ ] All associated test files deleted
- [ ] `delta/__init__.py` cleaned up
- [ ] ~1,040 LOC removed
- [ ] All tests pass
- [ ] No import errors

### Wave 1 Success

- [ ] ~56 dead exports removed from schema/registry.py
- [ ] ~6 dead exports removed from dataset/registration.py
- [ ] ~5 dead exports removed from extract/templates.py
- [ ] `datafusion_engine/__init__.py` cleaned up
- [ ] ~530-640 LOC removed
- [ ] All tests pass
- [ ] No import errors

### Wave 2 Success

- [ ] UDF platform simplified (no fallback paths)
- [ ] UDF runtime simplified (hard requirement)
- [ ] UDF factory simplified (no retry loops)
- [ ] ~300 LOC simplified
- [ ] All tests pass
- [ ] Extraction pipeline smoke test passes

### Wave 3 Success

- [ ] Protocol validation moved to Rust
- [ ] `scan_policy_inference.py` evaluated for deletion
- [ ] Control plane facade thinned
- [ ] ~500-800 LOC removed
- [ ] All tests pass
- [ ] Delta integration tests pass

### Wave 4 Success

- [ ] `session/runtime.py` split into 4 files (8,482 → 4,000 + 1,500 + 1,000 + 2,000)
- [ ] `schema/registry.py` split into 3 files (~3,850 → 1,500 + 500 + 300)
- [ ] Public API unchanged (all existing imports still work)
- [ ] 12,648 LOC restructured (0 net change)
- [ ] All tests pass
- [ ] No breaking changes

### Wave 5 Success (Future)

- [ ] Rust session extraction stub created
- [ ] Rust lineage stub created
- [ ] Python code marked with RUST_TRANSITION comments
- [ ] Documentation updated with transition plan
- [ ] No implementation work (preparation only)

### Overall Success

- [ ] ~2,800 LOC removed (Waves 0-3)
- [ ] 12,648 LOC restructured (Wave 4)
- [ ] All quality gates pass
- [ ] No regressions
- [ ] Improved maintainability
- [ ] Clear path for Rust transition (Wave 5)

---

## Document Metadata

**Version:** 1.0
**Date:** 2026-02-12
**Authors:** Engineering Team
**Status:** DRAFT
**Related Documents:**
- `/Users/paulheyse/CodeAnatomy/docs/plans/datafusion_engine_decomposition_assessment_v1_2026-02-12.md`
- `/Users/paulheyse/CodeAnatomy/AGENTS.md`
- `/Users/paulheyse/CodeAnatomy/CLAUDE.md`

**Next Steps:**
1. Review this plan with the team
2. Approve execution order
3. Assign waves to engineers
4. Begin Wave 0 execution
