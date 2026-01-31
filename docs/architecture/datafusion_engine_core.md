# Part III: DataFusion Engine - Core Planning and Execution

## Overview

The DataFusion Engine provides the primary query planning and execution infrastructure for CodeAnatomy's inference pipeline. Built on Apache Arrow DataFusion (v50.1+), it serves as the bridge between high-level relational specifications and efficient, deterministic query execution over Arrow-native data.

**Why DataFusion?** DataFusion is an Apache Arrow-native SQL query engine written in Rust that provides Volcano-style query planning, rule-based optimization, and vectorized execution over Arrow record batches. CodeAnatomy leverages DataFusion for several critical capabilities:

1. **Plan Stability**: DataFusion's logical/optimized/physical plan separation enables deterministic fingerprinting via Substrait serialization
2. **Extension Points**: Rust UDF registration, custom ExprPlanners, and FunctionFactory hooks enable domain-specific query transformations
3. **Schema Evolution**: Native support for schema drift through TableProvider adapters eliminates downstream projection complexity
4. **Delta Lake Integration**: First-class support for versioned Delta tables with CDF (Change Data Feed) and time-travel capabilities

**Integration Architecture**: The DataFusion engine sits at Layer 3 of the four-stage pipeline (Extraction → Normalization → Task Catalog → CPG Build). Task specifications from `src/relspec/` compile to DataFusion DataFrames via builder functions. The engine captures plan bundles containing logical, optimized, and physical plans plus Substrait bytes for reproducibility. These bundles feed into Hamilton DAG orchestration and drive downstream CPG materialization.

> **Module Consolidation Note:** The DataFusion engine consolidates functionality that was previously spread across separate modules. Lineage extraction, SQL analysis, and expression building are now DataFusion-native operations. The engine provides unified query planning, UDF registration, schema validation, and plan fingerprinting through a single cohesive interface.

## Module Map

The DataFusion engine is organized into the following subdirectories (120 Python files total):

- **`plan/`**: Plan bundling, execution, caching, profiling, artifact storage (11 files, ~6000 lines)
- **`session/`**: Session runtime, facade, factory, helpers, streaming (5 files, ~1000 lines)
- **`views/`**: View graph, registry, specs, DSL, artifacts (11 files, ~7500 lines)
- **`lineage/`**: Lineage extraction, scan planning, diagnostics (3 files, ~2100 lines)
- **`schema/`**: Contracts, introspection, registry, inference, validation, finalization (8 files, ~8500 lines)
- **`udf/`**: UDF catalog, runtime, platform, factory, signature, parity, shims (7 files, ~1500 lines)
- **`dataset/`**: Registry, resolution (2 files, ~500 lines)
- **`catalog/`**: Introspection, provider, provider registry (3 files, ~1000 lines)
- **`delta/`**: Protocol, control plane, store policy, maintenance, scan config, contracts, payload, observability (9 files, ~1500 lines)
- **`arrow/`**: Schema, build, semantic, interop, coercion, encoding, metadata, ABI, nested, chunking, dictionary, field builders (16 files, ~3000 lines)
- **`compile/`**: Compilation options (1 file, ~200 lines)
- **`sql/`**: SQL options, guard (2 files, ~300 lines)
- **`expr/`**: Expression planner, domain planner, query spec, span, spec (5 files, ~800 lines)
- **`io/`**: IO adapter, write pipeline, ingest (3 files, ~600 lines)
- **`encoding/`**: Encoding policy (1 file, ~100 lines)
- **`tables/`**: Table spec, metadata (2 files, ~200 lines)
- **`extract/`**: Extract templates, metadata, bundles, extractors (4 files, ~600 lines)
- **`symtable/`**: Symtable views (1 file, ~200 lines)
- **Root-level**: Hashing, identity, kernels, errors, `__init__.py` (5 files, ~44,000 lines)

**Key Design Principles**:
1. **Plan Bundles**: All planning and scheduling flows through `DataFusionPlanBundle` artifacts
2. **Facade Pattern**: `DataFusionExecutionFacade` provides unified API for compilation, execution, registration, writes
3. **Two-Pass Planning**: Delta pin planning separates lineage extraction from scan resolution
4. **Substrait-First**: Execution prioritizes Substrait replay for determinism, falls back to proto cache and original DataFrame
5. **View Graph**: Dependency-aware view registration with topological sorting and schema validation
6. **UDF Platform**: Unified installation of Rust UDFs, ExprPlanners, FunctionFactory before planning
7. **Schema Contracts**: Declarative schema validation with evolution policies
8. **Lineage Extraction**: Column-level lineage from optimized logical plans drives dependency inference

## Core Architecture Components

### Plan Bundle - The Central Artifact

**File**: `/home/paul/CodeAnatomy/src/datafusion_engine/plan/bundle.py` (2123 lines)

The `DataFusionPlanBundle` dataclass is the canonical plan artifact that all scheduling and execution paths consume. It encapsulates the complete planning lifecycle:

```python
@dataclass(frozen=True)
class DataFusionPlanBundle:
    df: DataFrame                           # DataFusion DataFrame
    logical_plan: object                    # Unoptimized logical plan
    optimized_logical_plan: object          # Post-optimization logical plan
    execution_plan: object | None           # Physical execution plan
    substrait_bytes: bytes | None           # Substrait serialization
    plan_fingerprint: str                   # SHA-256 digest for caching
    artifacts: PlanArtifacts                # Serializable metadata
    delta_inputs: tuple[DeltaInputPin, ...] # Pinned Delta versions
    required_udfs: tuple[str, ...]          # UDF dependencies
    required_rewrite_tags: tuple[str, ...]  # Rewrite rule tags
    plan_details: Mapping[str, object]      # Diagnostics payload
```

**Design Invariant**: The `plan_fingerprint` (line 138) is a stable hash computed from:
- Substrait bytes (serialized optimized plan)
- Session configuration settings
- Planning environment hash (DataFusion version, optimizer rules)
- UDF snapshot hash (registered functions and their signatures)
- Required UDFs and rewrite tags
- Delta input pins (dataset versions)
- Delta store policy hash
- Information schema hash

This fingerprint enables deterministic caching and cross-run comparison. The computation happens in `_hash_plan()` (lines 1488-1543), which validates Substrait bytes availability before hashing.

**Fingerprinting Detail** (`PlanFingerprintInputs` dataclass, lines 1472-1486):

The fingerprinting system uses a structured input dataclass to ensure all determinism-critical factors are captured:

```python
@dataclass(frozen=True)
class PlanFingerprintInputs:
    substrait_bytes: bytes | None
    df_settings: Mapping[str, str]
    planning_env_hash: str | None
    rulepack_hash: str | None
    information_schema_hash: str | None
    udf_snapshot_hash: str
    required_udfs: Sequence[str]
    required_rewrite_tags: Sequence[str]
    delta_inputs: Sequence[DeltaInputPin]
    delta_store_policy_hash: str | None
```

**Substrait Fingerprinting for Determinism** (lines 1309-1335):

Substrait serialization provides a stable, cross-platform plan representation that is independent of DataFusion's internal plan node structure. The conversion flow:

1. **Plan Extraction**: Optimized logical plan is obtained via `df.optimized_logical_plan()`
2. **Substrait Conversion**: `SubstraitProducer.to_substrait_plan(optimized, ctx)` converts to Substrait Plan object
3. **Serialization**: `substrait_plan.encode()` produces portable bytes
4. **Hashing**: SHA-256 digest of Substrait bytes becomes `substrait_hash` component

**Why Substrait**: DataFusion's internal plan representation may change across versions or depend on compilation flags. Substrait provides a stable wire format that can be:
- Validated across different DataFusion versions
- Replayed for execution (`replay_substrait_bytes()` in execution facade)
- Stored in artifact stores for cross-session plan reuse
- Used for plan diff analysis in debugging

**Plan Reproducibility Guarantees**:

When `plan_fingerprint` matches across two executions:
- Identical logical plan structure (via Substrait bytes)
- Identical session settings (via `df_settings_hash`)
- Identical optimizer configuration (via `planning_env_hash` + `rulepack_hash`)
- Identical UDF registry (via `udf_snapshot_hash`)
- Identical Delta input versions (via `delta_inputs` payload)
- Identical table schemas (via `information_schema_hash`)

This enables deterministic caching: if fingerprints match, execution results are guaranteed to be equivalent (modulo non-deterministic UDFs or Delta writes between runs).

**Key Mechanism - Plan Fingerprinting**:

1. **Substrait Serialization** (lines 1084-1110): The optimized logical plan is converted to Substrait bytes via `SubstraitProducer.to_substrait_plan()`. Substrait is a cross-platform IR (intermediate representation) that provides stable serialization independent of DataFusion's internal plan node representation.

2. **Environment Capture** (lines 488-521): The planning environment includes DataFusion version, session config, SQL policies, execution settings (target partitions, batch size), async UDF configuration, and Delta protocol support. This snapshot ensures the fingerprint reflects all factors that influence plan generation.

3. **Delta Input Pinning** (lines 78-93, 220-280): The `DeltaInputPin` dataclass captures dataset version, timestamp, feature gates, protocol compatibility, and storage options. This enables time-travel queries and reproducible scans across Delta table versions.

4. **UDF Dependency Tracking** (lines 1883-1896): The `_required_udf_artifacts()` function extracts UDF references from the optimized plan via lineage analysis. It validates that required UDFs exist in the snapshot and captures rewrite tags for schema transformation rules.

**Plan Artifacts** (lines 47-76): The `PlanArtifacts` dataclass contains all serializable metadata for reproducibility:
- Explain outputs (tree, verbose, analyze)
- Environment snapshots (settings, planning env, rulepack, information_schema)
- Substrait validation results
- Proto-serialized plans (logical, optimized, execution)
- UDF snapshots and function registry
- Rewrite tags and domain planner names

This separation enables artifact persistence to Delta tables (see Plan Artifact Store below) independent of the ephemeral DataFrame/plan objects.

### Session Runtime

**File**: `/home/paul/CodeAnatomy/src/datafusion_engine/session/runtime.py`

The `SessionRuntime` class manages the lifecycle of a DataFusion `SessionContext` with profile-driven configuration. It encapsulates:

- **DataFusion SessionContext**: The core query engine instance
- **Runtime Profile**: Configuration object containing dataset locations, optimizer settings, UDF policies, Delta protocol support
- **UDF Snapshot**: Cached registry of Rust UDFs with signatures and metadata
- **Settings Hash**: Stable digest of session configuration for fingerprinting
- **Rewrite Tags**: Metadata tags for schema transformation rules

> **Cross-reference**: For comprehensive documentation of runtime profiles, configuration options, and environment variables, see [Configuration Reference - DataFusionRuntimeProfile](configuration_reference.md#datafusionruntimeprofile) and [Configuration Reference - Runtime Profiles](configuration_reference.md#runtime-profiles).

**Configuration Options** (referenced in `plan_bundle.py:544-611`):
- `target_partitions`: Parallelism for execution
- `batch_size`: Arrow record batch size
- `repartition_aggregations/windows/file_scans`: Repartitioning policies
- `enable_async_udfs`: Async UDF support with timeout and batch size
- `delta_protocol_mode`: Delta protocol compatibility level
- `explain_verbose/analyze`: Explain capture settings

The runtime profile is initialized during facade construction and remains immutable throughout the session lifecycle.

**Profile Presets** (see [Configuration Reference](configuration_reference.md#key-profile-presets)):
- `default`: General-purpose profile (8 GiB memory, moderate concurrency)
- `dev`: Development profile (4 GiB memory, reduced concurrency)
- `prod`: Production profile (16 GiB memory, high concurrency)
- `cst_autoload`: LibCST extraction with catalog auto-loading
- `symtable`: Symtable extraction with statistics disabled

### Execution Facade

**File**: `/home/paul/CodeAnatomy/src/datafusion_engine/session/facade.py` (843 lines)

The `DataFusionExecutionFacade` provides the unified API for compilation, execution, registration, and writes. It coordinates all DataFusion operations through a consistent interface.

**Key Methods**:

1. **`compile_to_bundle()`** (lines 225-310): Compiles a DataFrame builder function to a `DataFusionPlanBundle`. This is the canonical compilation path:
   - Invokes builder with SessionContext to generate DataFrame
   - Calls `build_plan_bundle()` with session runtime and scan units
   - Returns plan bundle ready for execution or scheduling

2. **`execute_plan_bundle()`** (lines 312-414): Executes a plan bundle with Substrait-first replay:
   - Validates UDF compatibility between planning and execution contexts
   - Attempts Substrait replay via `replay_substrait_bytes()`
   - Falls back to original DataFrame on replay failure
   - Records execution artifacts (duration, status, errors)
   - Returns `ExecutionResult` wrapper

3. **`register_dataset()`** (lines 735-774): Registers a dataset location via the registry bridge:
   - Validates runtime profile availability
   - Delegates to `register_dataset_df()` from `dataset.registration` module
   - Returns DataFusion DataFrame representing the registered dataset

4. **`write()` / `write_view()`** (lines 600-708): Delegates to `WritePipeline` for write operations:
   - Constructs pipeline with session context and SQL options
   - Executes write request (table, view, streaming)
   - Returns `ExecutionResult` wrapping write metadata

5. **`ensure_view_graph()`** (lines 710-733): Ensures view graph is registered:
   - Optionally includes registry views before pipeline views
   - Returns Rust UDF snapshot used for view registration

6. **`build_plan_bundle()`** (lines 791-834): Builds plan bundle from DataFrame:
   - Creates canonical plan artifact for scheduling and lineage
   - Returns `DataFusionPlanBundle` with fingerprinting

**Error Handling Pattern** (lines 416-452): The facade captures exceptions during execution and records them as artifacts via `_record_execution_artifact()`. This enables post-mortem analysis of planning vs execution failures.

**Substrait-First Execution** (lines 454-496): The `_substrait_first_df()` method prioritizes Substrait replay for determinism:
```python
if not substrait_bytes:
    msg = "Plan bundle is missing Substrait bytes."
    raise ValueError(msg)
cached_entry = self._plan_cache_entry(bundle)
try:
    df = replay_substrait_bytes(self.ctx, substrait_bytes)
except (RuntimeError, TypeError, ValueError):
    cached_df = self._rehydrate_from_proto(bundle)
    if cached_df is not None:
        return cached_df, True
    return bundle.df, True  # Fallback on replay error
else:
    return df, False  # Successful Substrait replay
```

This pattern ensures that plans are executed from their serialized Substrait representation rather than the ephemeral DataFrame object, eliminating in-memory plan drift. The facade also supports plan proto caching for fallback when Substrait replay fails.

### Planning Pipeline - Delta Pin Two-Pass Planner

**File**: `/home/paul/CodeAnatomy/src/datafusion_engine/plan/pipeline.py`

The `plan_with_delta_pins()` function implements a two-pass planning strategy that pins Delta input versions before final plan compilation:

**Pass 1 - Baseline Planning**:
1. Register UDF platform and view graph via `ensure_view_graph()`
2. Plan view nodes without scan units to extract lineage
3. Infer dependencies from view nodes via `infer_deps_from_view_nodes()`
4. Extract scan lineage (dataset references, projected columns, filters)

**Pass 2 - Pinned Planning**:
1. Plan scan units with Delta version resolution via `plan_scan_units()`
2. Apply scan unit overrides (Delta version pins, file pruning)
3. Re-register view graph with pinned scan providers
4. Re-plan view nodes with scan units included in `PlanBundleOptions`

**Why Two Passes?** Delta scan planning requires lineage information (which tables, which columns) to determine file pruning and version selection. The first pass extracts this lineage from unresolved plans. The second pass re-plans with Delta providers that enforce version pins and file-level pruning, ensuring deterministic scan inputs.

**Scan Planning**: The `_scan_planning()` function:
- Groups scans by task name from inferred dependencies
- Calls `plan_scan_units()` to resolve Delta versions and prune files
- Builds scan task name mappings (stable digest-based names)
- Creates scan task units for scheduling (scan units become schedulable tasks)

**Lineage Extraction**: After pinned planning, `_lineage_by_view()` extracts column-level lineage from optimized logical plans via `extract_lineage()`. This feeds into scheduling edge validation.

### Lineage Extraction

**File**: `/home/paul/CodeAnatomy/src/datafusion_engine/lineage/datafusion.py` (534 lines)

The `extract_lineage()` function walks the DataFusion logical plan tree and extracts structured lineage information:

**LineageReport Components**:
- **scans**: `ScanLineage` entries with dataset name, projected columns, pushed filters
- **joins**: `JoinLineage` entries with join type, left/right keys
- **exprs**: `ExprInfo` entries with expression kind, referenced columns, referenced UDFs
- **required_udfs**: Tuple of UDF names referenced in the plan
- **required_rewrite_tags**: Tuple of rewrite rule tags required for execution
- **required_columns_by_dataset**: Mapping of dataset → columns for dependency analysis
- **filters/aggregations/window_functions**: Extracted from expression metadata
- **referenced_tables**: Property that returns sorted unique table names from scans

**Extraction Mechanism**:
```python
for node in walk_logical_complete(plan):
    variant = _plan_variant(node)
    tag = _variant_name(node=node, variant=variant)
    scans.extend(_extract_scan_lineage(tag=tag, variant=variant))
    joins.extend(_extract_join_lineage(tag=tag, variant=variant))
    exprs.extend(_extract_expr_infos(tag=tag, variant=variant, udf_name_map=udf_name_map))
```

The plan walk (via `walk_logical_complete()` from `plan.walk` module) visits every node in the logical plan tree. Each node is converted to its variant (e.g., `TableScan`, `Join`, `Projection`) for pattern matching.

**Scan Lineage**: For `TableScan` variants:
- Extract `table_name` or `fqn` (fully qualified name)
- Extract `projection` as column names
- Extract `filters` as pushed-down predicates
- Return `ScanLineage(dataset_name, projected_columns, pushed_filters)`

**Join Lineage**: For `Join` variants:
- Extract `join_type` (inner, left, right, full, cross)
- Extract `on` pairs as (left_expr, right_expr) tuples
- Extract qualified column names from expressions
- Return `JoinLineage(join_type, left_keys, right_keys)`

**Expression Info**: For plan nodes with expression attributes:
- Extract expressions from node attributes (e.g., `Projection.projections`, `Filter.predicate`)
- Recursively walk expression trees to find column references and UDF calls
- Return `ExprInfo(kind, referenced_columns, referenced_udfs, text)`

**UDF Dependency Resolution**: The lineage report includes `required_udfs` extracted from expression UDF references, and `required_rewrite_tags` resolved from the UDF snapshot's rewrite tag index. This enables scheduling to validate UDF availability before execution.

### Schema Contracts

**File**: `/home/paul/CodeAnatomy/src/datafusion_engine/schema/contracts.py` (1005 lines)

Schema contracts provide declarative schema validation and evolution policies. They enable compile-time detection of schema drift and DDL generation.

**SchemaContract**: Core contract abstraction:
```python
@dataclass(frozen=True)
class SchemaContract:
    table_name: str
    columns: tuple[ColumnContract, ...]
    partition_cols: tuple[str, ...]
    ordering: tuple[str, ...]
    evolution_policy: EvolutionPolicy
    schema_metadata: dict[bytes, bytes]
    enforce_columns: bool
```

**Evolution Policies**:
- `STRICT`: No schema changes allowed (fails on missing/extra columns)
- `ADDITIVE`: New columns allowed, removals fail
- `RELAXED`: Any compatible change allowed

**Validation Mechanism**: The `validate_against_introspection()` method:
1. Checks table existence in introspection snapshot
2. Retrieves actual column definitions from catalog
3. Compares expected vs actual for each column:
   - Missing columns → `MISSING_COLUMN` violation
   - Type mismatches → `TYPE_MISMATCH` violation (via `_types_compatible()`)
   - Nullability mismatches → `NULLABILITY_MISMATCH` violation
4. Under `STRICT` policy, extra columns → `EXTRA_COLUMN` violation

**Arrow Schema Interop**:
- `from_arrow_schema()`: Creates contract from PyArrow schema, stamping ABI fingerprint in metadata
- `to_arrow_schema()`: Converts contract to PyArrow schema with preserved metadata
- `schema_from_catalog()`: Retrieves Arrow schema from DataFusion catalog for comparison

**Schema ABI Fingerprinting**: The `SCHEMA_ABI_FINGERPRINT_META` metadata key stores a stable digest of the schema structure via `schema_fingerprint()`. This enables change detection across schema versions and validates that execution-time schemas match planning-time expectations.

### UDF System

**Files**:
- `/home/paul/CodeAnatomy/src/datafusion_engine/udf/catalog.py`
- `/home/paul/CodeAnatomy/src/datafusion_engine/udf/runtime.py`
- `/home/paul/CodeAnatomy/src/datafusion_engine/udf/platform.py`

The UDF catalog manages Rust UDF registration and metadata extraction via the `datafusion_ext` Rust extension module.

**DataFusionUdfSpec**: Specification for UDF registration:
```python
@dataclass(frozen=True)
class DataFusionUdfSpec:
    func_id: str                    # Unique function identifier
    engine_name: str                # DataFusion registration name
    kind: Literal["scalar", "aggregate", "window", "table"]
    input_types: tuple[pa.DataType, ...]
    return_type: pa.DataType
    state_type: pa.DataType | None  # For aggregate functions
    volatility: str                 # immutable/stable/volatile
    arg_names: tuple[str, ...] | None
    rewrite_tags: tuple[str, ...]   # Schema transformation tags
```

**Function Catalog**: Runtime function catalog built from `information_schema`:
- `from_information_schema()`: Constructs catalog from routines/parameters snapshots
- `function_names`: Frozenset of all registered function names
- `functions_by_category`: Mapping of category → function names
- `function_signatures`: Mapping of name → `FunctionSignature` with input/return types

**UDF Runtime** manages snapshot caching and validation.

**Snapshot Structure**: Required snapshot keys:
```python
_REQUIRED_SNAPSHOT_KEYS = (
    "scalar", "aggregate", "window", "table",  # UDF categories
    "aliases", "parameter_names",               # Metadata
    "signature_inputs", "return_types",         # Type info
)
```

**Snapshot Validation**: The `validate_rust_udf_snapshot()` function:
1. Checks for required keys in snapshot mapping
2. Validates sequence fields (scalar, aggregate, window, table)
3. Validates mapping fields (aliases, parameter_names, signature_inputs, return_types)
4. Ensures signature metadata exists for all registered UDFs

**Required UDF Validation**: The `validate_required_udfs()` function:
1. Extracts UDF names from snapshot via `udf_names_from_snapshot()`
2. Checks that all required UDFs exist in the snapshot
3. Resolves aliases to canonical names via alias index
4. Validates signature metadata presence for required UDFs
5. Validates return type metadata presence for required UDFs

**Snapshot Hash**: The `rust_udf_snapshot_hash()` function computes a stable SHA-256 digest of the normalized snapshot. This hash is used in plan fingerprinting to detect UDF registry changes.

**Caching Strategy**: The runtime maintains weak references to SessionContext instances and caches snapshots per context:
```python
_RUST_UDF_CONTEXTS: WeakSet[SessionContext] = WeakSet()
_RUST_UDF_SNAPSHOTS: WeakKeyDictionary[SessionContext, Mapping[str, object]] = WeakKeyDictionary()
_RUST_UDF_VALIDATED: WeakSet[SessionContext] = WeakSet()
```

This ensures validation runs once per context and snapshots persist for the session lifecycle without preventing garbage collection.

### Rust UDF Platform

**File**: `/home/paul/CodeAnatomy/src/datafusion_engine/udf/platform.py`

The Rust UDF Platform provides a unified installation mechanism for planning-critical extensions. Planner extensions (Rust UDFs, ExprPlanners, FunctionFactory, and RelationPlanners) are installed before any plan-bundle construction to ensure deterministic query planning.

> **Cross-reference**: For comprehensive documentation of the Rust UDF implementation, architecture, and 40+ custom functions, see [Part VIII: Rust Architecture](part_viii_rust_architecture.md), specifically the "Custom UDF System" section.

**RustUdfPlatformOptions**:
```python
@dataclass(frozen=True)
class RustUdfPlatformOptions:
    """Configuration for installing the Rust UDF platform."""

    enable_udfs: bool = True
    enable_async_udfs: bool = False
    async_udf_timeout_ms: int | None = None
    async_udf_batch_size: int | None = None
    enable_function_factory: bool = True
    enable_expr_planners: bool = True
    function_factory_policy: FunctionFactoryPolicy | None = None
    function_factory_hook: Callable[[SessionContext], None] | None = None
    expr_planner_hook: Callable[[SessionContext], None] | None = None
    expr_planner_names: Sequence[str] = ()
    strict: bool = True
```

**Platform Installation Pattern**:
```python
from datafusion_engine.runtime import DataFusionRuntimeProfile
from datafusion_engine.udf_platform import (
    RustUdfPlatformOptions,
    install_rust_udf_platform,
)

ctx = DataFusionRuntimeProfile().session_context()
options = RustUdfPlatformOptions(
    enable_udfs=True,
    enable_function_factory=True,
    enable_expr_planners=True,
    expr_planner_names=("codeanatomy_domain",),
    strict=True,
)
install_rust_udf_platform(ctx, options=options)
```

**Platform Components**:
- **Rust UDFs**: Scalar, aggregate, window, and table functions implemented in `datafusion_ext`
- **FunctionFactory**: Dynamic function resolution for unknown function calls during planning
- **ExprPlanners**: Custom expression rewrite rules (e.g., domain-specific transformations)
- **TableProviderCapsule**: PyCapsule-based integration for custom table providers

**RustUdfPlatform Snapshot**:
```python
@dataclass(frozen=True)
class RustUdfPlatform:
    """Snapshot of a Rust UDF platform installation."""

    snapshot: Mapping[str, object] | None
    snapshot_hash: str | None
    rewrite_tags: tuple[str, ...]
    domain_planner_names: tuple[str, ...]
    docs: Mapping[str, object] | None
    function_factory: ExtensionInstallStatus | None
    expr_planners: ExtensionInstallStatus | None
    function_factory_policy: Mapping[str, object] | None
    expr_planner_policy: Mapping[str, object] | None
```

All DataFusion execution facades automatically install the platform in `__post_init__` to ensure extensions are available before plan operations. The platform snapshot is captured in plan bundles for reproducibility.

### Views Subsystem

The views subsystem provides declarative view management with dependency-aware registration, schema validation, and artifact recording.

#### View Graph Architecture

**File**: `/home/paul/CodeAnatomy/src/datafusion_engine/views/graph.py` (645 lines)

The view graph system enables dependency-driven view registration with topological sorting and cache materialization.

**ViewNode Abstraction**:

```python
@dataclass(frozen=True)
class ViewNode:
    name: str
    deps: tuple[str, ...]  # Explicit dependency names
    builder: Callable[[SessionContext], DataFrame]
    contract_builder: Callable[[pa.Schema], SchemaContract] | None = None
    required_udfs: tuple[str, ...] = ()
    plan_bundle: DataFusionPlanBundle | None = None
    cache_policy: Literal["none", "memory", "delta_staging", "delta_output"] = "none"
```

**Key Features**:
- **Explicit Dependencies**: Dependencies are declared explicitly in `deps` tuple (can be auto-inferred from plan bundles)
- **Schema Contracts**: Optional contract builders for schema validation
- **UDF Requirements**: Required UDFs declared for validation
- **Plan Bundles**: Optional plan bundle for lineage extraction
- **Cache Policies**: Materialization strategies (none, memory, delta_staging, delta_output)

**Registration Flow** (`register_view_graph()`):

1. **Validation**: Validate Rust UDF snapshot structure
2. **Materialization**: Materialize nodes with dependencies from plan bundles via `_materialize_nodes()`
3. **Topological Sort**: Order views via rustworkx or Kahn's algorithm via `_topo_sort_nodes()`
4. **Per-View Registration**:
   - Validate dependencies exist (in context or earlier views)
   - Validate required UDFs exist in snapshot
   - Build DataFrame via `builder(ctx)`
   - Apply cache policy (memory, delta_staging, delta_output)
   - Register view with SessionContext
   - Validate schema contract (if provided)
   - Record view artifact to runtime profile

**Cache Policies**:
- **`"none"`**: Register as ephemeral view (no caching)
- **`"memory"`**: Call `df.cache()` before registration (DataFusion in-memory cache)
- **`"delta_staging"`**: Write to temp Delta table, register as Delta scan
- **`"delta_output"`**: Write to configured dataset location, register as Delta scan

Delta cache policies enable view materialization with versioned storage and CDF support.

**Topological Sorting**:

The system uses rustworkx when available for deterministic lexicographical topological sort, falling back to Kahn's algorithm:

```python
def _topo_sort_nodes(nodes: Sequence[ViewNode]) -> tuple[ViewNode, ...]:
    node_map = {node.name: node for node in nodes}
    ordered = _topo_sort_nodes_rx(node_map, nodes)  # Try rustworkx
    if ordered is not None:
        return ordered
    return _topo_sort_nodes_kahn(node_map, nodes)  # Fallback to Kahn
```

**Cycle Detection**: If Kahn's algorithm doesn't visit all nodes, a dependency cycle exists, raising `ValueError` with cycle participants.

**Schema Contract Validation**:

Views can declare schema contracts that are validated post-registration:

```python
contract = SchemaContract(
    table_name="cpg_nodes",
    columns=(
        ColumnContract(name="node_id", dtype=pa.utf8(), nullable=False),
        ColumnContract(name="node_type", dtype=pa.utf8(), nullable=False),
    ),
    evolution_policy=EvolutionPolicy.STRICT,
)
```

Validation detects:
- Missing columns
- Type mismatches
- Nullability violations
- Extra columns (under `STRICT` policy)
- Schema metadata mismatches (including ABI fingerprints)

Violations raise `SchemaContractViolationError` with detailed diagnostic information.

#### View Specification DSL

**File**: `/home/paul/CodeAnatomy/src/datafusion_engine/views/view_spec.py` (672 lines)

The view specification DSL provides a declarative approach to defining view projections, replacing hand-coded expression tuples with concise specifications.

**ViewProjectionSpec**:

```python
from datafusion_engine.views.view_spec import ViewProjectionSpec, ColumnTransform

spec = ViewProjectionSpec(
    name="ast_calls",
    base_table="ast_files_v1",
    include_file_identity=True,
    passthrough_cols=("ast_id", "parent_ast_id"),
    include_span_struct=True,
)

# Generate expressions from spec
exprs = spec.to_exprs(base_schema)
```

**Column Transform Types** (`ColumnTransformKind`):
- `PASSTHROUGH`: Direct column pass-through with alias
- `RENAME`: Rename column
- `CAST`: Type casting via `arrow_cast()`
- `STRUCT_FIELD`: Extract struct field
- `NESTED_STRUCT_FIELD`: Extract nested struct field
- `MAP_EXTRACT`: Extract map value by key
- `METADATA_EXTRACT`: Extract Arrow metadata by key
- `COALESCE`: Coalesce multiple columns
- `LITERAL`: Literal value
- `EXPRESSION`: Custom expression

**Benefits of DSL**:
- Uniform interface for view projection definitions
- Composable with view registry for dependency management
- Reduces boilerplate for common transformation patterns
- Type-safe transformation specifications
- Enables generic builders for expression generation

#### Registry Views

**File**: `/home/paul/CodeAnatomy/src/datafusion_engine/views/registry.py` (3256 lines)

The registry views module provides pre-defined view builders for AST, CST, symtable, and tree-sitter evidence layers.

**Registry View Categories**:
- **AST Core Views**: Core AST views (required)
- **AST Optional Views**: Optional AST enrichment views
- **CST Views**: LibCST view definitions
- **Symtable Views**: Symtable analysis views (class methods, function partitions, scope edges, namespace edges)
- **Tree-sitter Views**: Tree-sitter AST views and CST alignment checks
- **Python Imports**: Python import extraction views
- **Map Utilities**: Map entries, keys, values views for nested structures
- **Span Unnesting**: CST span unnesting views

**View Builder Patterns**:

```python
def _symtable_class_methods_df(ctx: SessionContext) -> DataFrame:
    """Build symtable_class_methods view."""
    return ctx.sql("""
        SELECT
            file_id,
            stable_id(file_id, class_name, method_name) AS class_method_id,
            class_name,
            method_name
        FROM symtable_classes_v1
        CROSS JOIN UNNEST(methods) AS t(method_name)
    """)
```

**Registry Integration** (`ensure_view_graph()`):

```python
def ensure_view_graph(
    ctx: SessionContext,
    *,
    runtime_profile: DataFusionRuntimeProfile | None = None,
    include_registry_views: bool = True,
) -> Mapping[str, object]:
    """Ensure the view graph is registered for the current context."""
    # Install UDF platform
    # Register registry views if requested
    # Return UDF snapshot for downstream use
```

**View Spec Catalog**:

**File**: `/home/paul/CodeAnatomy/src/datafusion_engine/views/view_specs_catalog.py` (1207 lines)

Centralized catalog of view projection specifications for all registry views. Provides:
- Comprehensive view spec definitions
- Spec validation and composition
- Metadata for view dependencies and contracts

#### Python ↔ Rust UDF Boundary

The Rust UDF system integrates with DataFusion through a multi-layer architecture:

**Installation Flow**:
1. Python calls `install_function_factory()` from `datafusion_ext` module
2. Rust `install_function_factory_native()` receives Arrow IPC policy payload
3. Rust registers UDF primitives with DataFusion SessionContext
4. Python captures UDF snapshot via `rust_udf_snapshot()` for fingerprinting

**Key Integration Points**:
- **datafusion_ext crate** (`rust/datafusion_ext/src/udf_custom.rs`): Implements 40+ custom UDFs with `ScalarUDFImpl` trait
- **datafusion_ext_py wrapper** (`rust/datafusion_ext_py/`): Thin PyO3 cdylib exposing Rust functions to Python
- **UDF catalog** (`src/datafusion_engine/udf_catalog.py`): Python-side UDF spec definitions and metadata extraction
- **UDF runtime** (`src/datafusion_engine/udf_runtime.py`): Snapshot caching, validation, and hash computation

**Rust UDF Categories** (from Part VIII):
- Hashing & ID Generation (8 functions): `stable_hash64`, `stable_hash128`, `prefixed_hash64`, `stable_id`, etc.
- Span Arithmetic (5 functions): `span_make`, `span_len`, `span_overlaps`, `span_contains`, `interval_align_score`
- String Normalization (3 functions): `utf8_normalize`, `utf8_null_if_blank`, `qname_normalize`
- Collection Utilities (4 functions): `list_compact`, `list_unique_sorted`, `map_get_default`, `map_normalize`
- Delta CDF Utilities (3 functions): `cdf_change_rank`, `cdf_is_upsert`, `cdf_is_delete`

See [Part VIII: Rust Architecture - Custom UDF System](part_viii_rust_architecture.md#custom-udf-system) for complete UDF listings and implementation details.

## Key Data Flow Patterns

### Plan Bundle Lifecycle

```mermaid
flowchart TD
    A[DataFrame Builder] -->|Builder Function| B[SessionContext]
    B -->|df = builder ctx| C[DataFrame]
    C -->|logical_plan| D[Logical Plan]
    D -->|optimize| E[Optimized Logical Plan]
    E -->|to_substrait_plan| F[Substrait Bytes]
    E -->|execution_plan| G[Physical Execution Plan]

    H[Session Runtime] -->|Settings| I[Environment Snapshot]
    H -->|UDF Registry| J[UDF Snapshot Hash]
    H -->|Delta Pins| K[Delta Input Pins]

    F -->|hash| L[Plan Fingerprint]
    I -->|hash| L
    J -->|hash| L
    K -->|hash| L

    C --> M[DataFusionPlanBundle]
    D --> M
    E --> M
    G --> M
    F --> M
    L --> M
    N[PlanArtifacts] --> M
    K --> M
```

**Explanation**:
1. A DataFrame builder function receives a SessionContext
2. Builder generates a DataFrame by registering tables and composing operations
3. DataFusion extracts logical plan, optimizes it, and optionally generates physical plan
4. Optimized plan is serialized to Substrait bytes for portable fingerprinting
5. Environment snapshot, UDF snapshot hash, and Delta pins contribute to fingerprint
6. Plan bundle assembles all components for execution and scheduling

### Execution Flow

```mermaid
sequenceDiagram
    participant Client
    participant Facade as DataFusionExecutionFacade
    participant Validator as UDF Validator
    participant Replay as Substrait Replay
    participant Store as Plan Artifact Store

    Client->>Facade: execute_plan_bundle(bundle)
    Facade->>Validator: _ensure_udf_compatibility(ctx, bundle)
    Validator->>Validator: Compare snapshot hashes
    Validator-->>Facade: UDF compatibility OK

    Facade->>Replay: _substrait_first_df(bundle)
    alt Substrait bytes available
        Replay->>Replay: replay_substrait_bytes(ctx, bytes)
        Replay-->>Facade: DataFrame (replay success)
    else Substrait unavailable or replay failed
        Replay-->>Facade: bundle.df (fallback)
    end

    Facade->>Store: _record_execution_artifact(request)
    Store->>Store: Build PlanArtifactRow
    Store->>Store: Append to Delta table
    Store-->>Facade: Artifact persisted

    Facade-->>Client: ExecutionResult
```

**Explanation**:
1. Client invokes `execute_plan_bundle()` with a compiled plan bundle
2. Facade validates UDF compatibility between planning and execution contexts
3. Facade attempts Substrait replay for deterministic execution
4. On replay failure, facade falls back to original DataFrame
5. Facade records execution artifact (duration, status, error) to Delta store
6. Facade returns `ExecutionResult` wrapper with DataFrame

### Two-Pass Delta Pin Planning

```mermaid
flowchart TD
    A[View Nodes] -->|Pass 1| B[Plan without Scan Units]
    B --> C[Extract Lineage]
    C --> D[Infer Dependencies]
    D --> E[Extract Scan Lineage]

    E --> F[Plan Scan Units]
    F --> G[Resolve Delta Versions]
    G --> H[Prune Files]

    H -->|Pass 2| I[Apply Scan Overrides]
    I --> J[Re-register View Graph]
    J --> K[Plan with Scan Units]
    K --> L[Pinned Plan Bundles]

    L --> M[Build Scan Tasks]
    M --> N[Inferred Deps + Scan Deps]
```

**Explanation**:
1. Pass 1 plans views without scan units to extract lineage (dataset references)
2. Lineage drives scan unit planning with Delta version resolution and file pruning
3. Scan units become scan task specifications for scheduling
4. Pass 2 re-plans views with scan units included in bundle options
5. Final plan bundles have pinned Delta versions in `delta_inputs` field
6. Scan tasks are added to inferred dependency graph for scheduling

## Design Patterns

### Determinism Contract Enforcement

Every plan bundle must satisfy the determinism contract:
- **Substrait bytes required**: Fingerprinting fails if Substrait serialization unavailable
- **Environment snapshot captured**: All optimizer rules, settings, and UDF metadata recorded
- **Delta inputs pinned**: Dataset versions locked to specific snapshots
- **UDF snapshot hash validated**: Execution context must match planning context

The `build_plan_bundle()` function (lines 314-372 in `plan_bundle.py`) enforces these requirements:
```python
if not resolved.compute_substrait:
    msg = "Substrait bytes are required for plan bundle construction."
    raise ValueError(msg)
if resolved.session_runtime is None:
    msg = "SessionRuntime is required for plan bundle construction."
    raise ValueError(msg)
```

### Graceful Degradation for Missing Inputs

When optional inputs are unavailable, the system produces correct-schema empty outputs rather than exceptions:
- No scans → empty `scan_units` tuple
- No UDFs referenced → empty `required_udfs` tuple
- Substrait replay failure → fallback to original DataFrame with diagnostics

This pattern prevents cascading failures in the pipeline and enables partial execution.

### Weak Reference Caching

UDF snapshots and session metadata are cached using weak references:
```python
_RUST_UDF_SNAPSHOTS: WeakKeyDictionary[SessionContext, Mapping[str, object]]
```

This prevents memory leaks while maintaining per-session caching. When the SessionContext is garbage collected, the snapshot cache entry is automatically removed.

### Immutable Data Structures

All plan bundle components use frozen dataclasses:
```python
@dataclass(frozen=True)
class DataFusionPlanBundle: ...

@dataclass(frozen=True)
class PlanArtifacts: ...

@dataclass(frozen=True)
class DeltaInputPin: ...
```

This prevents accidental mutation and enables safe sharing across threads and processes.

## Critical Files Reference

### Plan Bundle System
- **`plan/bundle.py`** (2123 lines): Core plan artifact with fingerprinting logic
  - `build_plan_bundle()`: Main entrypoint for plan compilation
  - `DataFusionPlanBundle`: Canonical plan artifact dataclass
  - `PlanBundleOptions`: Options for building plan bundles
  - `_delta_inputs_from_scan_units()`: Derives Delta pins from scan lineage
  - Substrait serialization for plan portability and fingerprinting

- **`plan/execution.py`** (182 lines): Plan execution helpers
  - `execute_plan_bundle()`: Execute plan bundle with scan overrides
  - `replay_substrait_bytes()`: Substrait replay (stub in current build)
  - `validate_substrait_plan()`: Substrait validation (stub in current build)
  - `datafusion_to_async_batches()`: Async batch streaming
  - `datafusion_write_options()`: Write options from policy

- **`plan/result_types.py`**: Execution result types
  - `ExecutionResult`: Unified execution result wrapper
  - `ExecutionResultKind`: Result kind enumeration
  - `PlanExecutionResult`: Detailed execution result with artifacts
  - `PlanExecutionOptions`: Execution options with scan overrides
  - `PlanEmissionOptions`: Artifact emission control

- **`plan/cache.py`**: Plan proto caching
  - `PlanCacheEntry`: Cache entry for plan protos
  - `PlanProtoCache`: Proto cache protocol

- **`plan/profiler.py`**: Plan profiling and explain capture
  - `ExplainCapture`: Explain output capture
  - `capture_explain()`: Capture explain output from DataFrame

- **`plan/walk.py`**: Plan tree walking utilities
  - `walk_logical_complete()`: Walk logical plan tree
  - Plan and expression tree traversal helpers

- **`plan/artifact_store.py`** (1392 lines): Delta-backed artifact store
  - `PlanArtifactRow`: Serializable artifact row
  - `persist_execution_artifact()`: Persist execution artifacts
  - Event-time partitioned Delta tables for artifact persistence

### Execution Infrastructure
- **`session/facade.py`** (843 lines): Unified API for compilation and execution
  - `DataFusionExecutionFacade`: Main facade class
  - `compile_to_bundle()`: DataFrame builder → plan bundle
  - `execute_plan_bundle()`: Substrait-first execution with fallback
  - `register_dataset()`: Dataset registration via registry bridge
  - `write()` / `write_view()`: Write operations via WritePipeline
  - `ensure_view_graph()`: View graph registration
  - `build_plan_bundle()`: Build plan bundle from DataFrame
  - `_ensure_udf_compatibility()`: Pre-execution UDF validation
  - `_substrait_first_df()`: Substrait-first execution with proto cache fallback
  - `_record_execution_artifact()`: Execution artifact persistence

- **`session/runtime.py`**: Session lifecycle management
  - `DataFusionRuntimeProfile`: Configuration container with profile presets
  - `SessionRuntime`: Session + profile + cached snapshots
  - Profile presets: `default`, `dev`, `prod`, `cst_autoload`, `symtable`
  - Session factory and configuration management
  - UDF platform hooks and diagnostics sinks

- **`session/factory.py`**: Session context factory
  - `SessionFactory`: Factory for creating session contexts
  - Configuration-driven session construction

- **`session/helpers.py`**: Session helper utilities
  - `register_temp_table()`: Temporary table registration
  - `deregister_table()`: Table deregistration

- **`session/streaming.py`**: Streaming execution support

### Planning Pipeline
- **`plan/pipeline.py`**: Two-pass Delta pin planning
  - `plan_with_delta_pins()`: Main orchestration function
  - `PlanningPipelineResult`: Pipeline result dataclass
  - `_scan_planning()`: Scan unit resolution and task generation
  - `_plan_view_nodes()`: View-level planning with scan units
  - `_lineage_by_view()`: Extract lineage from pinned plans

### Lineage and Dependency Analysis
- **`lineage/datafusion.py`** (534 lines): Plan tree lineage extraction
  - `extract_lineage()`: Main extraction function
  - `LineageReport`: Complete lineage report dataclass
  - `ScanLineage`: Scan lineage information
  - `JoinLineage`: Join lineage information
  - `ExprInfo`: Expression lineage information
  - `_extract_scan_lineage()`: Table scan → dataset references
  - `_extract_join_lineage()`: Join → key pairs
  - `_required_columns_by_dataset()`: Column-level dependency map

- **`lineage/scan.py`** (730 lines): Scan planning and Delta integration
  - `ScanUnit`: Deterministic scan unit with Delta pins
  - `plan_scan_unit()`: Single scan unit resolution
  - `plan_scan_units()`: Batch scan unit planning
  - Delta file pruning and candidate selection

- **`lineage/diagnostics.py`** (870 lines): Diagnostics collection and recording
  - `DiagnosticsRecorder`: Diagnostics recording interface
  - `record_artifact()`: Record diagnostic artifact
  - `record_events()`: Record diagnostic events
  - `recorder_for_profile()`: Create recorder from profile

### Schema and Contracts
- **`schema/contracts.py`** (1005 lines): Declarative schema validation
  - `SchemaContract`: Contract with evolution policies
  - `ColumnContract`: Column-level contract
  - `EvolutionPolicy`: Schema evolution policies (STRICT, ADDITIVE, RELAXED)
  - `validate_against_introspection()`: Snapshot validation
  - `schema_contract_from_dataset_spec()`: Spec → contract bridge

- **`schema/introspection.py`** (1256 lines): Schema introspection
  - `SchemaIntrospector`: Information schema query interface
  - `catalogs_snapshot()`: Catalog snapshot
  - `constraint_rows()`: Constraint introspection
  - `table_constraint_rows()`: Table constraint introspection

- **`schema/registry.py`** (3904 lines): Schema registry and validation
  - View name constants (AST, CST, Tree-sitter)
  - `nested_view_specs()`: Nested view specifications
  - `validate_nested_types()`: Nested type validation
  - `validate_semantic_types()`: Semantic type validation
  - `validate_required_engine_functions()`: Engine function validation

- **`schema/inference.py`** (708 lines): Schema inference
  - `infer_schema_from_dataframe()`: Schema inference from DataFrame
  - Schema inference utilities

- **`schema/validation.py`** (729 lines): Schema validation
  - Schema validation helpers and utilities

- **`schema/finalize.py`** (935 lines): Schema finalization
  - Schema finalization and canonicalization

- **`schema/contract_population.py`** (619 lines): Contract population
  - Contract population from evidence tables

### UDF Platform
- **`udf/catalog.py`**: UDF metadata and builtin resolution
  - `DataFusionUdfSpec`: UDF registration spec
  - `FunctionCatalog`: Runtime function catalog
  - `get_default_udf_catalog()`: Default UDF catalog
  - `get_strict_udf_catalog()`: Strict UDF catalog
  - `rewrite_tag_index()`: Rewrite tag index from snapshot

- **`udf/runtime.py`**: Snapshot caching and validation
  - `rust_udf_snapshot()`: Cached snapshot retrieval
  - `validate_rust_udf_snapshot()`: Structural validation
  - `validate_required_udfs()`: Dependency validation
  - `rust_udf_snapshot_hash()`: Stable digest computation
  - `udf_names_from_snapshot()`: Extract UDF names from snapshot

- **`udf/platform.py`**: Rust UDF platform installation
  - `RustUdfPlatformOptions`: Platform configuration options
  - `RustUdfPlatform`: Platform snapshot
  - `install_rust_udf_platform()`: Unified UDF/ExprPlanner/FunctionFactory installation
  - See [Part VIII: Rust Architecture](part_viii_rust_architecture.md) for Rust implementation details

- **`udf/factory.py`**: Function factory integration
  - `install_function_factory()`: Function factory installation
  - `function_factory_payloads()`: Factory payload extraction

- **`udf/signature.py`**: UDF signature handling
  - Signature metadata and type conversion

- **`udf/parity.py`**: UDF parity validation
  - Information schema parity checks

- **`udf/shims.py`**: Python shims for Rust UDFs
  - Python-side UDF wrappers for testing

### View Registration and Graph Management
- **`views/graph.py`** (645 lines): Dependency-aware view registration
  - `ViewNode`: Declarative view definition with explicit dependencies
  - `register_view_graph()`: Topologically-sorted view registration
  - `ViewGraphOptions`: Registration configuration
  - `ViewGraphRuntimeOptions`: Runtime options for registration
  - `SchemaContractViolationError`: Contract violation error
  - `_materialize_nodes()`: Materialize view nodes with dependencies from bundles
  - `_topo_sort_nodes()`: rustworkx/Kahn topological sort
  - Cache policies: `none`, `memory`, `delta_staging`, `delta_output`

- **`views/registry.py`** (3256 lines): Registry view specifications and registration
  - `registry_view_specs()`: View specifications for registry views
  - `register_all_views()`: Register all registry views
  - `registry_view_nodes()`: Registry view nodes for graph registration
  - `ensure_view_graph()`: Ensure view graph is registered
  - View builders for AST, CST, symtable, and tree-sitter views
  - Map entries/keys/values view builders
  - Python imports view builders

- **`views/view_spec.py`** (672 lines): Declarative view projection specifications
  - `ViewProjectionSpec`: DSL for view projections
  - `ColumnTransform`: Column transformation specification
  - `ColumnTransformKind`: Transformation type enumeration
  - Generic builder for expression generation from specs

- **`views/view_specs_catalog.py`** (1207 lines): View specification catalog
  - Centralized catalog of view projection specifications
  - View spec definitions for all registry views

- **`views/registry_specs.py`** (566 lines): Registry spec builders
  - View spec builders for registry views
  - Spec composition and validation

- **`views/dsl.py`** (625 lines): View DSL implementation
  - DSL for building views from specifications
  - Expression builders and helpers

- **`views/dsl_views.py`**: DSL-driven view definitions
  - Views built using the DSL

- **`views/bundle_extraction.py`**: Bundle extraction utilities
  - `arrow_schema_from_df()`: Extract Arrow schema from DataFrame
  - `extract_lineage_from_bundle()`: Extract lineage from plan bundle
  - `resolve_required_udfs_from_bundle()`: Resolve required UDFs from bundle

- **`views/artifacts.py`**: View artifact recording
  - `ViewArtifactRequest`: View artifact request
  - `ViewArtifactLineage`: View lineage artifact
  - `build_view_artifact_from_bundle()`: Build view artifact from bundle

### Dataset Management
- **`dataset/registry.py`**: Dataset catalog and location management
  - `DatasetCatalog`: Dataset catalog registry
  - `DatasetLocation`: Dataset location specification

- **`dataset/resolution.py`**: Scan configuration and dataset resolution
  - `apply_scan_unit_overrides()`: Re-register table providers with pinned scans
  - Dataset resolution utilities

### Catalog and Introspection
- **`catalog/introspection.py`** (683 lines): Catalog introspection
  - `IntrospectionCache`: Introspection cache protocol
  - Catalog metadata extraction and caching

- **`catalog/provider.py`**: Table provider integration
  - Custom table provider implementations

- **`catalog/provider_registry.py`**: Provider registry
  - Registry for table providers

### Delta Lake Integration
- **`delta/protocol.py`**: Delta protocol support
  - `DeltaProtocolSupport`: Protocol compatibility levels
  - `DeltaProtocolSnapshot`: Protocol snapshot

- **`delta/control_plane.py`**: Delta control plane
  - Delta table operations and management

- **`delta/store_policy.py`**: Delta store policy
  - `DeltaStorePolicy`: Store policy configuration
  - `apply_delta_store_policy()`: Apply policy to session
  - `delta_store_policy_hash()`: Policy fingerprinting

- **`delta/maintenance.py`**: Delta maintenance operations
  - Vacuum, optimize, and compaction operations

- **`delta/scan_config.py`**: Delta scan configuration
  - Scan configuration for Delta tables

- **`delta/contracts.py`**: Delta contracts
  - Contract validation for Delta tables

- **`delta/payload.py`**: Delta payload utilities
  - Payload serialization and deserialization

- **`delta/observability.py`**: Delta observability
  - Diagnostics and metrics for Delta operations

### Arrow Utilities
- **`arrow/schema.py`**: Arrow schema utilities
  - Schema construction and manipulation
  - `map_entry_type()`: Map entry type construction
  - `version_field()`: Version field construction
  - `versioned_entries_schema()`: Versioned entries schema

- **`arrow/build.py`**: Arrow builders
  - Record batch and table builders

- **`arrow/semantic.py`**: Semantic type utilities
  - Semantic type handling and validation

- **`arrow/interop.py`**: Arrow interop
  - `RecordBatchReaderLike`: Reader protocol
  - `SchemaLike`: Schema protocol
  - `TableLike`: Table protocol

- **`arrow/coercion.py`**: Type coercion
  - `to_arrow_table()`: Coerce to Arrow table

- **`arrow/encoding.py`**: Encoding utilities
  - Arrow encoding helpers

- **`arrow/metadata.py`**: Metadata handling
  - `schema_constraints_from_metadata()`: Extract constraints from metadata

- **`arrow/abi.py`**: Arrow ABI utilities
  - ABI compatibility helpers

- **`arrow/nested.py`**: Nested type utilities
  - Nested struct and list handling

- **`arrow/chunking.py`**: Chunking utilities
  - Record batch chunking

- **`arrow/dictionary.py`**: Dictionary encoding
  - Dictionary encoding utilities

- **`arrow/encoding_metadata.py`**: Encoding metadata
  - Metadata for Arrow encoding

- **`arrow/metadata_codec.py`**: Metadata codec
  - Metadata encoding and decoding

- **`arrow/field_builders.py`**: Field builders
  - Field construction utilities

### Compilation and Execution Options
- **`compile/options.py`**: Compilation options
  - `DataFusionCompileOptions`: Compilation configuration
  - `DataFusionSqlPolicy`: SQL policy enumeration
  - `DataFusionCacheEvent`: Cache event tracking
  - `DataFusionSubstraitFallbackEvent`: Substrait fallback tracking
  - `resolve_sql_policy()`: Resolve SQL policy from environment

### SQL and Expressions
- **`sql/options.py`**: SQL options
  - `sql_options_for_profile()`: SQL options from profile
  - `statement_sql_options_for_profile()`: Statement SQL options

- **`sql/guard.py`**: SQL guard
  - SQL injection prevention and validation

- **`expr/planner.py`**: Expression planner
  - Custom expression rewrite rules
  - `install_expr_planners()`: Install expression planners
  - `expr_planner_payloads()`: Extract planner payloads

- **`expr/domain_planner.py`**: Domain planner
  - Domain-specific query transformations

- **`expr/query_spec.py`**: Query specification
  - Query specification builders

- **`expr/span.py`**: Span expressions
  - Byte span expression utilities

- **`expr/spec.py`**: Expression specifications
  - Expression spec builders

### IO and Encoding
- **`io/adapter.py`**: IO adapter
  - `DataFusionIOAdapter`: IO operations adapter

- **`io/write.py`**: Write pipeline
  - `WritePipeline`: Write pipeline orchestration
  - `WriteRequest`: Write request specification
  - `WriteViewRequest`: View write request

- **`io/ingest.py`**: Ingest utilities
  - Data ingestion helpers

- **`encoding/policy.py`**: Encoding policy
  - Encoding policy configuration

### Table Specifications
- **`tables/spec.py`**: Table specifications
  - Table spec definitions

- **`tables/metadata.py`**: Table metadata
  - `table_provider_metadata()`: Extract table provider metadata

### Hashing and Identity
- **`hashing.py`** (12441 lines): Hashing utilities
  - DataFusion-specific hashing helpers
  - Thin re-export wrapper for DataFusion callers

- **`identity.py`** (1973 lines): Identity hashing
  - `schema_identity_hash()`: Schema identity fingerprinting

- **`kernels.py`** (31171 lines): DataFusion kernels
  - Custom compute kernels

### Extract Integration
- **`extract/templates.py`**: Extract templates
  - DataFrame builder patterns for extraction artifacts

- **`extract/metadata.py`**: Extract metadata
  - Metadata extraction helpers

- **`extract/bundles.py`**: Extract bundles
  - Bundle construction for extraction

- **`extract/extractors.py`**: Extractors
  - Extractor implementations

### Symtable Views
- **`symtable/views.py`**: Symtable view builders
  - Symtable-specific view definitions

### Error Handling
- **`errors.py`** (662 lines): Error types
  - DataFusion-specific error types and handling

### Scan Overrides

**File**: `/home/paul/CodeAnatomy/src/datafusion_engine/dataset_resolution.py`

Scan overrides enable fine-grained control over table scan behavior after Delta version pinning. The system applies custom scan configurations to registered table providers.

**Key Use Case**: When a view references a Delta table, the two-pass planning pipeline:
1. **Pass 1**: Plans view without scan units to extract lineage (which tables, which columns, which filters)
2. **Pass 2**: Resolves Delta versions and file pruning → produces `ScanUnit` objects with candidate files
3. **Scan Override Application**: `apply_scan_unit_overrides()` re-registers table providers with:
   - Pinned Delta versions
   - Pruned file lists (only candidate files from predicate pushdown)
   - Custom scan configuration (file column injection, schema overrides)

**Override Mechanism** (from `view_registry.py` usage):
```python
from datafusion_engine.dataset_resolution import apply_scan_unit_overrides

# After scan units are planned
apply_scan_unit_overrides(
    ctx,
    scan_units=scan_units,
    runtime_profile=runtime_profile,
)
# SessionContext now has updated table providers with pinned scans
```

**ScanUnit Configuration** (`scan_planner.py`, lines 103-125):

Each `ScanUnit` captures:
- **Delta version pin**: `delta_version` / `delta_timestamp` / `snapshot_timestamp`
- **Protocol compatibility**: `delta_protocol`, `protocol_compatible`, `protocol_compatibility`
- **File pruning results**: `total_files`, `candidate_file_count`, `pruned_file_count`, `candidate_files`
- **Scan config hash**: `delta_scan_config_hash` for deterministic provider comparison
- **Provider marker**: `datafusion_provider` identifier (e.g., `"delta_table_provider"`)

**File Pruning Integration** (`scan_planner.py`, lines 339-392):

The scan planner uses `storage.deltalake.file_pruning` to evaluate partition and stats filters:

```python
from storage.deltalake.file_pruning import (
    FilePruningPolicy,
    PartitionFilter,
    StatsFilter,
    evaluate_and_select_files,
)

# Build pruning policy from lineage filters
policy = FilePruningPolicy(
    partition_filters=[PartitionFilter(column="year", op="=", value="2024")],
    stats_filters=[StatsFilter(column="event_ts", op=">=", value=start_ts)],
)

# Apply to Delta file index
pruning_result = evaluate_and_select_files(index, policy, ctx=ctx)
# Returns: total_files, candidate_count, pruned_count, candidate_paths
```

This enables **predicate pushdown to Delta file selection**: only files matching partition/stats filters are registered with the table provider, minimizing scan I/O.

### View Registry

**File**: `/home/paul/CodeAnatomy/src/datafusion_engine/view_registry.py` (704 lines)

The view registry provides dependency-aware view registration with automatic topological sorting, schema validation, and artifact recording.

**ViewNode Abstraction** (lines 46-74):

```python
@dataclass(frozen=True)
class ViewNode:
    name: str
    deps: tuple[str, ...]  # Auto-inferred from plan lineage
    builder: Callable[[SessionContext], DataFrame]
    contract_builder: Callable[[pa.Schema], SchemaContract] | None = None
    required_udfs: tuple[str, ...]  # Auto-inferred from plan lineage
    plan_bundle: DataFusionPlanBundle | None = None
    cache_policy: Literal["none", "memory", "delta_staging", "delta_output"] = "none"
```

**Dependency Inference** (lines 417-431):

View dependencies are automatically extracted from DataFusion plan bundles via lineage analysis:

```python
def _deps_from_plan_bundle(bundle: DataFusionPlanBundle) -> tuple[str, ...]:
    lineage = extract_lineage(bundle.optimized_logical_plan)
    return lineage.referenced_tables  # Auto-extracted from TableScan nodes
```

This eliminates manual dependency declarations. The system walks the optimized logical plan, identifies all `TableScan` nodes, and extracts referenced table names.

**Registration Flow** (`register_view_graph()`, lines 121-200):

1. **UDF Validation**: Validate Rust UDF snapshot structure
2. **Dependency Materialization**: Extract deps + required UDFs from plan bundles
3. **Topological Sort**: Order views via rustworkx or Kahn's algorithm
4. **Per-View Registration**:
   - Validate dependencies exist (in context or earlier views)
   - Validate required UDFs exist in snapshot
   - Apply scan unit overrides (if plan bundle provided)
   - Build DataFrame via `builder(ctx)`
   - Apply cache policy (memory, delta_staging, delta_output)
   - Register view with SessionContext
   - Validate schema contract (if provided)
   - Record view artifact to runtime profile

**Topological Sorting** (lines 639-695):

The system uses rustworkx when available for deterministic lexicographical topological sort, falling back to Kahn's algorithm:

```python
def _topo_sort_nodes(nodes: Sequence[ViewNode]) -> tuple[ViewNode, ...]:
    node_map = {node.name: node for node in nodes}
    ordered = _topo_sort_nodes_rx(node_map, nodes)  # Try rustworkx
    if ordered is not None:
        return ordered
    return _topo_sort_nodes_kahn(node_map, nodes)  # Fallback to Kahn
```

**Cycle Detection**: If Kahn's algorithm doesn't visit all nodes, a dependency cycle exists, raising `ValueError` with cycle participants.

**Cache Policies** (lines 236-346):

- **`"none"`**: Register as ephemeral view (no caching)
- **`"memory"`**: Call `df.cache()` before registration (DataFusion in-memory cache)
- **`"delta_staging"`**: Write to temp Delta table, register as Delta scan
- **`"delta_output"`**: Write to configured dataset location, register as Delta scan

Delta cache policies enable view materialization with versioned storage and CDF support.

**Schema Contract Validation** (lines 503-566):

Views can declare schema contracts that are validated post-registration:

```python
contract = SchemaContract(
    table_name="cpg_nodes",
    columns=(
        ColumnContract(name="node_id", dtype=pa.utf8(), nullable=False),
        ColumnContract(name="node_type", dtype=pa.utf8(), nullable=False),
    ),
    evolution_policy=EvolutionPolicy.STRICT,
)
```

Validation detects:
- Missing columns
- Type mismatches
- Nullability violations
- Extra columns (under `STRICT` policy)
- Schema metadata mismatches (including ABI fingerprints)

Violations raise `SchemaContractViolationError` with detailed diagnostic information.

### View Graph Registry

**File**: `/home/paul/CodeAnatomy/src/datafusion_engine/view_graph_registry.py` (referenced but not shown in extracts)

The view graph registry extends the basic view registry with graph-level orchestration capabilities for complex view pipelines.

**Expected Functionality** (inferred from naming and usage patterns):

1. **Graph Construction**: Build dependency graphs from view node collections
2. **Parallel Registration**: Register independent views concurrently when topologically safe
3. **Incremental Updates**: Re-register only views affected by upstream changes
4. **Artifact Correlation**: Track view→task→plan relationships for debugging

This is used in planning pipeline (`planning_pipeline.py`) for view-driven task scheduling.

## Extension Points

The DataFusion engine exposes several extension mechanisms:

1. **Custom UDFs**: Register Rust UDFs via `datafusion_ext` module with signature metadata
2. **ExprPlanners**: Custom expression rewrite rules via `expr_planner_hook` in runtime profile
3. **FunctionFactory**: Dynamic function resolution via `function_factory_hook`
4. **Domain Planners**: Domain-specific query transformations with rewrite tags
5. **TableProviders**: Custom scan implementations via `TableProviderCapsule`
6. **Schema Adapters**: Schema evolution adapters attached at registration time
7. **Scan Overrides**: Fine-grained scan configuration via `apply_scan_unit_overrides()`

These extension points enable domain-specific optimizations without modifying core DataFusion logic.

## Performance Characteristics

**Plan Compilation**: O(N) in plan node count for logical → optimized → physical transformation. Substrait serialization adds ~10-50ms overhead depending on plan complexity.

**Fingerprint Computation**: O(1) for hash computation given Substrait bytes. Environment snapshot hashing is amortized O(1) via caching.

**Lineage Extraction**: O(N) in plan node count for tree walk. Expression recursion depth is bounded by query complexity.

**Substrait Replay**: O(N) in plan node count for deserialization. Replay overhead is ~5-20ms compared to native DataFrame execution.

**Artifact Persistence**: O(M) in artifact size for Delta write. Batch writes minimize overhead. Partitioned by event time for efficient pruning.

**Bottlenecks**:
- Large plan trees (>1000 nodes) can slow Substrait serialization
- Deep expression nesting (>50 levels) can impact lineage extraction
- High UDF counts (>100) can increase snapshot hash computation time

### Extract Templates

**Context**: Extract templates are DataFrame builder patterns used in the extraction pipeline to standardize data loading and transformation from extraction artifacts.

**Architecture Pattern**:

Extract templates follow a consistent builder function signature:

```python
from typing import Callable
from datafusion import SessionContext
from datafusion.dataframe import DataFrame

ExtractTemplateBuilder = Callable[[SessionContext], DataFrame]
```

**Common Extract Template Patterns**:

1. **Delta Table Load**:
```python
def load_extract_table(ctx: SessionContext) -> DataFrame:
    """Load extraction artifact from Delta table."""
    return ctx.table("extract_artifacts")
```

2. **Parquet Scan with Schema**:
```python
def scan_libcst_nodes(ctx: SessionContext) -> DataFrame:
    """Scan LibCST node extraction with explicit schema."""
    schema = pa.schema([
        ("file_id", pa.utf8()),
        ("node_type", pa.utf8()),
        ("start_byte", pa.int64()),
        ("end_byte", pa.int64()),
    ])
    return ctx.read_parquet("libcst_nodes/*.parquet", schema=schema)
```

3. **View-Driven Transformation**:
```python
def normalized_spans(ctx: SessionContext) -> DataFrame:
    """Build normalized spans via registered view."""
    return ctx.sql("""
        SELECT
            stable_id(file_id, start_byte, end_byte) AS span_id,
            span_make(start_byte, end_byte) AS span,
            span_len(start_byte, end_byte) AS span_len
        FROM extract_artifacts
        WHERE start_byte IS NOT NULL
    """)
```

**Integration with View Registry**:

Extract templates are wrapped in `ViewNode` objects for dependency-aware registration:

```python
from datafusion_engine.view_registry import ViewNode

node = ViewNode(
    name="libcst_nodes_normalized",
    builder=normalized_spans,  # Extract template function
    deps=("extract_artifacts",),  # Auto-inferred from plan bundle
    required_udfs=("stable_id", "span_make", "span_len"),  # Auto-inferred
    cache_policy="delta_staging",  # Materialize to Delta for reuse
)
```

**Benefits of Template Pattern**:
- Uniform interface for all extraction data sources
- Composable with view registry for dependency management
- Enables plan bundle fingerprinting for caching
- Supports schema evolution via contracts
- Integrates with scan planning for Delta version pinning

See extraction pipeline documentation (not yet created) for comprehensive extract template catalog.

## Cross-References

### Internal Documentation

- **[Part VIII: Rust Architecture](part_viii_rust_architecture.md)**: Comprehensive Rust UDF system documentation
  - Custom UDF System (40+ functions)
  - Delta Lake Integration (control plane, mutations, maintenance)
  - PyO3 Bindings and Arrow IPC bridge
  - Plugin Architecture

- **[Configuration Reference](configuration_reference.md)**: Complete configuration documentation
  - DataFusion Runtime Profiles (`default`, `dev`, `prod`, `cst_autoload`, `symtable`)
  - Environment Variables (OpenTelemetry, Hamilton, Delta Lake, Cache)
  - Determinism Tiers (CANONICAL, STABLE_SET, BEST_EFFORT)
  - Configuration Naming Conventions (Policy, Settings, Config, Spec, Options, Profile)

- **Observability Documentation** (referenced but not yet created):
  - OpenTelemetry instrumentation
  - Hamilton tracker integration
  - Runtime profile metrics

- **Storage and Incremental Processing** (referenced but not yet created):
  - Delta Lake integration architecture
  - Change Data Feed (CDF) runtime
  - Incremental impact analysis

### Related Modules

- **Extraction Pipeline** (`src/extract/`): Multi-source extractors (LibCST, AST, symtable, bytecode, SCIP)
- **Normalization** (`src/normalize/`): Byte-span canonicalization, stable ID generation
- **Task Catalog** (`src/relspec/`): TaskSpec builders, PlanCatalog, inferred dependency graph
- **CPG Build** (`src/cpg/`): CPG schema definitions, node/edge/property contracts
- **Hamilton Pipeline** (`src/hamilton_pipeline/`): Hamilton DAG orchestration

## Future Enhancements

The DataFusion engine architecture enables several planned improvements:

1. **Incremental View Maintenance**: Leverage Delta CDF for efficient view updates
2. **Plan Caching**: Store Substrait bytes in artifact store for cross-session reuse
3. **Adaptive Execution**: Use explain analyze metrics to guide runtime re-optimization
4. **Schema Versioning**: Extend contracts with migration rules for automated schema evolution
5. **UDF Versioning**: Track UDF implementation versions via snapshot metadata
6. **Cross-Engine Replay**: Use Substrait for execution on alternative Arrow engines (e.g., Velox, Acero)
7. **Extract Template Catalog**: Centralized registry of standardized extract templates
