# Part V: Storage and Incremental Processing

## Conceptual Overview

CodeAnatomy employs Delta Lake as its primary persistent storage layer, combined with a sophisticated incremental processing architecture to minimize recomputation. This system provides ACID-guaranteed versioned storage, efficient Change Data Feed (CDF) tracking for detecting modifications, and intelligent invalidation mechanisms that determine when cached artifacts must be rebuilt.

**Semantic incremental protocol:** The canonical CDF join/merge configuration now lives in `src/semantics/incremental/` (cursor store, CDF reader, merge strategies, config). The legacy `src/incremental/` package remains as a facade and for broader invalidation/runtime tooling, but pipeline-facing configuration should use `semantics.incremental`.

The architecture separates storage concerns (Delta protocol compliance, file pruning, scan optimization) from computation concerns (invalidation detection, state management, incremental updates), creating a layered approach where storage optimizations feed into scheduling decisions while invalidation signals drive selective recomputation.

At its core, Delta Lake provides time-travel capabilities through versioned snapshots and transaction logs, enabling incremental pipelines to track "what changed since last run" with precision. The CDF subsystem transforms Delta's transaction log into queryable change streams, while cursor-based tracking ensures each incremental run processes exactly the delta between the previous state and current state.

---

## Delta Lake Integration Architecture

### Delta Protocol and Feature Detection

The Delta protocol layer (`src/datafusion_engine/delta/protocol.py`) provides version compatibility checking and feature gate validation:

```python
# src/datafusion_engine/delta/protocol.py:18-37
class DeltaProtocolSupport(StructBaseStrict, frozen=True):
    """Runtime support bounds for Delta protocol and features."""
    max_reader_version: NonNegInt | None = None
    max_writer_version: NonNegInt | None = None
    supported_reader_features: tuple[str, ...] = ()
    supported_writer_features: tuple[str, ...] = ()

class DeltaProtocolSnapshot(StructBaseCompat, frozen=True):
    """Snapshot of Delta protocol versions and feature flags."""
    min_reader_version: NonNegInt | None = None
    min_writer_version: NonNegInt | None = None
    reader_features: tuple[str, ...] = ()
    writer_features: tuple[str, ...] = ()
```

**Protocol Compatibility Evaluation** (`src/datafusion_engine/delta/protocol.py:68-132`):

The `delta_protocol_compatibility()` function compares table requirements against runtime capabilities:

1. **Version Checking**: Validates `min_reader_version` and `min_writer_version` against runtime support bounds
2. **Feature Detection**: Compares required reader/writer features against supported feature sets
3. **Mismatch Identification**: Returns `DeltaProtocolCompatibility` dataclass with detailed compatibility status, missing features, and version gaps
4. **Compatibility Flag**: Produces boolean `compatible` flag only when all constraints are satisfied

**Feature Gate Validation** (`src/datafusion_engine/delta/protocol.py:135-174`):

The `validate_delta_gate()` function enforces minimum protocol requirements by raising `DataFusionEngineError` when snapshot metadata fails to satisfy gate constraints. Uses Rust extension (`datafusion_ext.validate_protocol_gate`) for validation.

### Delta Store Policy and Configuration

Storage policies (`src/datafusion_engine/delta/store_policy.py`) centralize object-store and log-store configuration:

```python
# src/datafusion_engine/delta/store_policy.py:15-21
@dataclass(frozen=True)
class DeltaStorePolicy(FingerprintableConfig):
    """Runtime-level object store and log store configuration."""
    storage_options: Mapping[str, str] = field(default_factory=dict)
    log_storage_options: Mapping[str, str] = field(default_factory=dict)
    require_local_paths: bool = False
```

**Policy Resolution** (`src/datafusion_engine/delta/store_policy.py:50-78`):

The `resolve_delta_store_policy()` function merges runtime-level policy with per-table overrides using three-layer precedence:

1. Base policy from runtime configuration
2. Table-specific storage options (override base)
3. Table-specific log-storage options (override base log config)

Returns tuple of effective storage options and log-storage options. Raises `ValueError` when remote Delta tables are disallowed by policy.

**Policy Hashing for Determinism** (`src/datafusion_engine/delta/store_policy.py:128-138`):

The `delta_store_policy_hash()` function produces stable fingerprint hashes of policy configuration via `policy.fingerprint()`, ensuring that storage option changes trigger plan invalidations.

---

## Scan Planning and File Pruning

### ScanUnit Abstraction

The `ScanUnit` dataclass (`src/datafusion_engine/lineage/scan.py:100-121`) represents a deterministic, pruned scan description:

```python
@dataclass(frozen=True)
class ScanUnit:
    """Deterministic, pruned scan description for scheduling."""
    key: str  # Stable hash for deduplication
    dataset_name: str
    delta_version: int | None
    delta_timestamp: str | None
    snapshot_timestamp: int | None
    delta_protocol: DeltaProtocolSnapshot | None
    delta_scan_config: DeltaScanConfigSnapshot | None
    delta_scan_config_hash: str | None
    datafusion_provider: str | None
    protocol_compatible: bool | None
    protocol_compatibility: DeltaProtocolCompatibility | None
    total_files: int
    candidate_file_count: int
    pruned_file_count: int
    candidate_files: tuple[Path, ...]
    pushed_filters: tuple[str, ...]
    projected_columns: tuple[str, ...]
```

**Scan Unit Key Generation** (`src/datafusion_engine/lineage/scan.py:79-97`):

Keys incorporate all inputs affecting scan results:
- Dataset name and version (or timestamp)
- Delta protocol snapshot
- Scan config hash (schema, pushdown settings)
- DataFusion provider type
- Projected columns and pushed filters

Keys use SHA256 hashes (16-char prefix) of msgpack-serialized payloads, ensuring identical scans produce identical keys for deduplication.

### File Pruning Pipeline

**Index Construction** (`src/storage/deltalake/file_index.py`):

The `build_delta_file_index_from_add_actions()` function transforms Rust control-plane add actions into normalized PyArrow tables:

1. Extracts `path`, `size_bytes`, `modification_time`, `partition_values`, `stats` from each add action
2. Parses JSON stats payloads to extract `minValues` and `maxValues`
3. Normalizes to canonical schema with `stats_min` and `stats_max` as map columns
4. Returns PyArrow table suitable for DataFusion predicate evaluation

The `FileIndexEntry` dataclass provides typed representation of index entries with fields: `path`, `size_bytes`, `modification_time`, `partition_values`, `stats_min`, `stats_max`, `num_records`.

**Pruning Policy** (`src/storage/deltalake/file_pruning.py:22-54`):

```python
@dataclass(frozen=True)
class PartitionFilter:
    """Partition-value filter for file pruning."""
    column: str
    op: Literal["=", "!=", "in", "not in"]
    value: str | Sequence[str]

@dataclass(frozen=True)
class StatsFilter:
    """Statistics range filter for file pruning."""
    column: str
    op: Literal["=", "!=", ">", ">=", "<", "<="]
    value: Any
    cast_type: str | None = None

@dataclass(frozen=True)
class FilePruningPolicy(FingerprintableConfig):
    """File pruning policy with partition and statistics filters."""
    partition_filters: list[PartitionFilter]
    stats_filters: list[StatsFilter]
```

Partition filters apply equality/in-list predicates to `partition_values` maps. Stats filters apply range predicates to `stats_min`/`stats_max` maps, leveraging Parquet column statistics for file-level pruning.

**Predicate-Based Filtering** (`src/storage/deltalake/file_pruning.py`):

The `evaluate_and_select_files()` function:

1. Registers file index as temporary DataFusion table via `temp_table()` context manager
2. Converts `FilePruningPolicy` to DataFusion predicate expressions
3. Applies predicate via `df.filter(predicate)`
4. Materializes filtered table and extracts file paths
5. Cleanup handled automatically by context manager

### Scan Planning Workflow

**Plan Scan Unit** (`src/datafusion_engine/lineage/scan.py`):

1. **Resolve Configuration**: Extract Delta protocol snapshot, scan config, provider type from dataset location
2. **Delta Scan Resolution**: Call `_resolve_delta_scan_resolution()` to fetch add actions via control plane, apply pruning policy, check protocol compatibility
3. **Scan Unit Key Generation**: Build deterministic key from dataset name, version, protocol, config, filters, columns
4. **Return ScanUnit**: Package pruned files, statistics, protocol compatibility status, and metadata

**Bulk Planning** (`src/datafusion_engine/lineage/scan.py`):

The `plan_scan_units()` function plans scans for all tasks:

1. Iterates over tasks and their scan lineage entries extracted from plan bundles
2. Plans `ScanUnit` for each unique dataset/lineage combination
3. Deduplicates scan units by key
4. Returns sorted scan units plus per-task scan key mappings
5. Records scan artifacts for observability via `_record_scan_plan_artifact()`

---

## Incremental Processing Runtime

### State Store Layout

The `StateStore` dataclass (`src/incremental/state_store.py:10-180`) defines filesystem layout:

```
<root>/
  snapshots/
    latest/
      repo_snapshot/         # Current repo state (Delta)
      incremental_diff/      # Detected changes (Delta)
      scip_snapshot/         # SCIP index state (Delta)
      scip_diff/             # SCIP changes (Delta)
  datasets/
    <dataset_name>/          # Per-dataset Delta tables
  metadata/
    invalidation_snapshot/   # Plan fingerprints (Delta)
    cdf_cursors/             # CDF version cursors (JSON)
    incremental_metadata/    # Runtime metadata (Delta)
    cdf_cursor_snapshot/     # CDF cursor snapshots (Delta)
    incremental_pruning_metrics/  # Pruning statistics (Delta)
    incremental_view_artifacts/   # View artifact cache (Delta)
```

### Incremental Runtime

The `IncrementalRuntime` dataclass (`src/incremental/runtime.py:21-80`) bundles DataFusion runtime with determinism tracking:

```python
@dataclass
class IncrementalRuntime:
    """Runtime container for DataFusion incremental execution."""
    profile: DataFusionRuntimeProfile
    _session_runtime: SessionRuntime
    determinism_tier: DeterminismTier = DeterminismTier.BEST_EFFORT
```

Factory methods:
- `build()`: Create runtime with default or custom DataFusion profile
- `session_runtime()`: Access cached SessionRuntime
- `session_context()`: Access DataFusion SessionContext
- `io_adapter()`: Create DataFusionIOAdapter bound to runtime session

**Temp Table Registry** (`src/incremental/runtime.py:82-143`):

The `TempTableRegistry` context manager tracks temporary DataFusion tables for cleanup:

1. `register_table()`: Registers PyArrow table via IO adapter, assigns UUID-based name with `__incremental_` prefix
2. `track()`: Tracks externally registered table name
3. `deregister()`: Safely deregisters table and invalidates introspection cache
4. `close()`: Deregisters all tracked tables on context exit
5. Context manager support via `__enter__`/`__exit__` for automatic cleanup

---

## Invalidation and Change Detection

### Invalidation Snapshot Schema

Invalidation snapshots (`src/incremental/invalidations.py:30-42`) use strict Arrow schemas:

```python
_INVALIDATION_SCHEMA = pa.schema([
    pa.field("version", pa.int32(), nullable=False),
    pa.field("incremental_plan_fingerprints", pa.list_(_PLAN_FINGERPRINT_ENTRY), nullable=False),
    pa.field("incremental_metadata_hash", pa.string(), nullable=True),
    pa.field("runtime_profile_hash", pa.string(), nullable=True),
])
```

Each snapshot row captures:
- **Version**: Schema version constant (`INVALIDATION_SNAPSHOT_VERSION = 2`)
- **Plan fingerprints**: List of `(plan_name, plan_fingerprint, plan_task_signature)` structs
- **Metadata hash**: Hash of DataFusion settings and runtime profile
- **Runtime profile hash**: Standalone runtime profile hash

The `InvalidationSnapshot` dataclass (`src/incremental/invalidations.py:65-71`) wraps these fields with `incremental_plan_fingerprints` as `dict[str, PlanFingerprint]`.

### Invalidation Diffing

**Diff Snapshots** (`src/incremental/invalidations.py:195-218`):

The `diff_invalidation_snapshots()` function produces reason codes:

1. **Runtime Profile Change**: `runtime_profile_hash` mismatch → `"runtime_profile_hash"` reason
2. **Metadata Change**: `incremental_metadata_hash` mismatch → `"incremental_metadata"` reason
3. **Plan Changes**: Calls `_diff_mapping()` to detect missing/changed plan fingerprints by comparing `PlanFingerprint` objects:
   - `"incremental_plan_fingerprint_missing:plan_name"` when plan exists in only one snapshot
   - `"incremental_plan_fingerprint_changed:plan_name"` when `plan_task_signature` differs

**Check State Store Invalidation** (`src/incremental/invalidations.py:221-243`):

1. Build current snapshot via `build_invalidation_snapshot()` which extracts plan fingerprints from view registry or view artifacts table
2. Read previous snapshot via `read_invalidation_snapshot()` from Delta table
3. If no previous snapshot exists, write current and return `InvalidationResult(invalidated=False, reasons=("missing_snapshot",))`
4. Diff snapshots; if reasons exist, write updated snapshot and return `InvalidationResult(invalidated=True, reasons=...)`
5. Otherwise return `InvalidationResult(invalidated=False, reasons=())`

---

## Change Data Feed (CDF) Processing

### CDF Cursor Management

CDF cursors (`src/incremental/cdf_cursors.py:15-27`) track the last processed Delta version:

```python
class CdfCursor(StructBaseCompat, frozen=True):
    """Cursor tracking the last processed Delta table version for CDF reads."""
    dataset_name: str
    last_version: NonNegInt  # Annotated int with >=0 constraint
```

**Cursor Store** (`src/incremental/cdf_cursors.py:30-147`):

The `CdfCursorStore` dataclass manages cursor persistence:

- **Storage**: JSON files in `cursors_path/<safe_dataset_name>.cursor.json` (sanitized for filesystem)
- **Save**: `save_cursor()` writes msgspec JSON via `dumps_json()` with pretty formatting
- **Load**: `load_cursor()` reads and deserializes via `loads_json()`, returns `None` if missing or decode fails
- **Has**: `has_cursor()` checks cursor file existence
- **Delete**: `delete_cursor()` removes cursor file
- **List**: `list_cursors()` glob-matches `*.cursor.json` files and deserializes all valid cursors

### CDF Filter Policies

**Change Types** (`src/incremental/cdf_filters.py:16-30`):

```python
class CdfChangeType(Enum):
    """Delta CDF change operation types."""
    INSERT = "insert"
    UPDATE_POSTIMAGE = "update_postimage"  # Final state after update
    DELETE = "delete"
```

Helper methods:
- `from_cdf_column(value)`: Parse CDF `_change_type` column value to enum
- `to_cdf_column_value()`: Convert enum to CDF column value string

**Filter Policy** (`src/incremental/cdf_filters.py:66-85`):

```python
@dataclass(frozen=True)
class CdfFilterPolicy(FingerprintableConfig):
    """Policy for filtering Delta CDF changes by operation type."""
    include_insert: bool = True
    include_update_postimage: bool = True
    include_delete: bool = True
```

Factory methods:
- `include_all()`: All change types
- `inserts_and_updates_only()`: Excludes deletes
- `inserts_only()`: Only inserts
- `to_datafusion_predicate()`: Converts policy to DataFusion filter expression on `_change_type` column

### CDF Read Workflow

**Read CDF Changes** (`src/incremental/cdf_runtime.py:164-272`):

1. **Resolve CDF inputs** via `_resolve_cdf_inputs()`: Extract current Delta version, storage options, scan options, CDF policy from dataset location
2. **Validate CDF availability**: If inputs unavailable and CDF is required by policy, raise `ValueError`; otherwise return `None`
3. **Prepare read state** via `_prepare_cdf_read_state()`: Load cursor, check if current version > cursor version, build `DeltaCdfOptions` with version window `[last_version + 1, current_version]`
4. **Handle no-op case**: If no state (no new changes), record artifact and return `None`
5. **Register Delta CDF provider**: Create `DatasetLocation` with `datafusion_provider="delta_cdf"` and `delta_cdf_options`, register via facade
6. **Read and filter CDF table**: Load table via `_read_cdf_table()`, apply `CdfFilterPolicy` predicate
7. **Update cursor**: Save cursor at `current_version` via `cursor_store.save_cursor()`
8. **Return result**: Package as `CdfReadResult(table=table, updated_version=current_version)`
9. **Record artifact**: Log CDF read operation for observability

All operations use `TempTableRegistry` context manager for automatic cleanup of temporary DataFusion tables.

---

## Key Design Patterns

### Determinism Through Hashing

**Policy Hashes**:
- `delta_store_policy_hash()` returns `policy.fingerprint()` from `FingerprintableConfig` protocol
- `delta_scan_identity_hash()` hashes scan configuration snapshot
- Storage options merged and hashed as part of scan config fingerprinting

These hashes become inputs to scan unit keys and plan fingerprints.

**Plan Fingerprints** (`src/incremental/plan_fingerprints.py`):
- `plan_fingerprint`: SHA256 hash of Substrait bytes or optimized plan display
- `plan_task_signature`: Runtime-aware task signature including session identity
- Stored in `PlanFingerprintSnapshot` dataclass with both fields
- Persisted to Delta tables in state store metadata directory

### File-Level Pruning for Performance

1. **Partition Pruning**: Equality/in-list filters on partition columns eliminate entire partitions
2. **Stats Pruning**: Range filters on `stats_min`/`stats_max` eliminate files where values cannot satisfy predicate
3. **DataFusion Evaluation**: Leverages Acero for predicate evaluation

### Cursor-Based Incremental Reads

1. **Atomic Cursor Updates**: Cursor advances only after successful table materialization
2. **Version Windows**: CDF reads process `[cursor.last_version + 1, current_version]`
3. **Idempotent Reads**: Re-running without new data produces `None`

### Graceful Degradation

- **Missing Cursors**: First run initializes cursor at current version
- **Unavailable CDF**: Returns `None` unless `cdf_policy.required=True`
- **Protocol Incompatibility**: Warns or errors based on `delta_protocol_mode` setting
- **Missing Snapshots**: Returns `invalidated=False` and initializes

---

## Critical Implementation Files

| File | Lines | Purpose |
|------|-------|---------|
| `src/datafusion_engine/delta/control_plane.py` | ~1989 | Delta control plane facade |
| `src/storage/deltalake/file_pruning.py` | ~460 | File-level pruning policies |
| `src/incremental/invalidations.py` | ~397 | Invalidation detection |
| `src/storage/deltalake/file_index.py` | ~382 | File index construction |
| `src/incremental/cdf_runtime.py` | ~361 | CDF read orchestration |
| `src/datafusion_engine/delta/protocol.py` | ~280 | Protocol compatibility |
| `src/incremental/plan_fingerprints.py` | ~245 | Plan fingerprint persistence |
| `src/incremental/state_store.py` | ~172 | State layout paths |
| `src/incremental/cdf_cursors.py` | ~149 | CDF cursor persistence |
| `src/datafusion_engine/delta/store_policy.py` | ~148 | Storage policy resolution |
| `src/incremental/runtime.py` | ~145 | Incremental runtime and temp table registry |
| `src/datafusion_engine/lineage/scan.py` | ~730 | Scan planning with pruning |

---

## Data Flow Diagram

```
Delta Table URI
       │
       ▼
┌──────────────────────────────────────────────────────────────┐
│                    DELTA CONTROL PLANE                        │
│  delta_add_actions() → Add Actions Payload                   │
└────────────────────────┬─────────────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────────────────┐
│                     FILE INDEX BUILD                          │
│  build_delta_file_index() → PyArrow Table                    │
│  (path, size, partition_values, stats_min, stats_max)        │
└────────────────────────┬─────────────────────────────────────┘
                         │
    Query Lineage ───────┼───────▶ Pushed Filters
                         │                │
                         │                ▼
                         │         FilePruningPolicy
                         │                │
                         ▼                ▼
┌──────────────────────────────────────────────────────────────┐
│                    FILE PRUNING                               │
│  evaluate_and_select_files() → Pruned File List              │
│  - Partition filters → eliminate partitions                   │
│  - Stats filters → eliminate files by min/max                 │
└────────────────────────┬─────────────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────────────────┐
│                      SCAN UNIT                                │
│  (key, dataset, version, pruned_files, filters, columns)     │
└────────────────────────┬─────────────────────────────────────┘
                         │
                         ▼
                   Task Scheduling


Previous Snapshot ────┬──── Current Snapshot
                      │
                      ▼
┌──────────────────────────────────────────────────────────────┐
│                 INVALIDATION DIFF                             │
│  diff_invalidation_snapshots() → Reason Codes                │
│  - Plan fingerprint changes                                   │
│  - Runtime profile changes                                    │
│  - Metadata hash changes                                      │
└────────────────────────┬─────────────────────────────────────┘
                         │
                         ▼
              Invalidated? ──▶ Yes: Rebuild Subgraph
                    │
                    └────────▶ No: Use Cached Outputs


CDF Cursor (last_version) ────┬──── Current Delta Version
                              │
                              ▼
┌──────────────────────────────────────────────────────────────┐
│                     CDF READ                                  │
│  Version Window: [last_version + 1, current_version]         │
│  → Register Delta CDF Provider                               │
│  → Apply CdfFilterPolicy                                     │
│  → Save Updated Cursor                                       │
└──────────────────────────────────────────────────────────────┘
```

---

*This ensures that storage optimizations (file pruning, protocol compatibility) feed deterministic scan units into scheduling, while invalidation detection and CDF tracking enable efficient incremental computation with exactly-once semantics.*
