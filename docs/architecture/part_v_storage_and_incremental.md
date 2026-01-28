# Part V: Storage and Incremental Processing

## Conceptual Overview

CodeAnatomy employs Delta Lake as its primary persistent storage layer, combined with a sophisticated incremental processing architecture to minimize recomputation. This system provides ACID-guaranteed versioned storage, efficient Change Data Feed (CDF) tracking for detecting modifications, and intelligent invalidation mechanisms that determine when cached artifacts must be rebuilt.

The architecture separates storage concerns (Delta protocol compliance, file pruning, scan optimization) from computation concerns (invalidation detection, state management, incremental updates), creating a layered approach where storage optimizations feed into scheduling decisions while invalidation signals drive selective recomputation.

At its core, Delta Lake provides time-travel capabilities through versioned snapshots and transaction logs, enabling incremental pipelines to track "what changed since last run" with precision. The CDF subsystem transforms Delta's transaction log into queryable change streams, while cursor-based tracking ensures each incremental run processes exactly the delta between the previous state and current state.

---

## Delta Lake Integration Architecture

### Delta Protocol and Feature Detection

The Delta protocol layer (`src/datafusion_engine/delta_protocol.py`) provides version compatibility checking and feature gate validation:

```python
# src/datafusion_engine/delta_protocol.py:10-28
@dataclass(frozen=True)
class DeltaFeatureGate:
    """Protocol and feature-gating requirements for Delta tables."""
    min_reader_version: int | None = None
    min_writer_version: int | None = None
    required_reader_features: tuple[str, ...] = ()
    required_writer_features: tuple[str, ...] = ()

@dataclass(frozen=True)
class DeltaProtocolSupport:
    """Runtime support bounds for Delta protocol and features."""
    max_reader_version: int | None = None
    max_writer_version: int | None = None
    supported_reader_features: tuple[str, ...] = ()
    supported_writer_features: tuple[str, ...] = ()
```

**Protocol Compatibility Evaluation** (`src/datafusion_engine/delta_protocol.py:53-110`):

The `delta_protocol_compatibility()` function compares table requirements against runtime capabilities:

1. **Version Checking**: Validates `min_reader_version` and `min_writer_version` against runtime support bounds
2. **Feature Detection**: Compares required reader/writer features against supported feature sets
3. **Mismatch Identification**: Returns detailed payload describing compatibility status, missing features, and version gaps
4. **Compatibility Flag**: Produces boolean `compatible` flag only when all constraints are satisfied

**Feature Gate Validation** (`src/datafusion_engine/delta_protocol.py:113-151`):

The `validate_delta_gate()` function enforces minimum protocol requirements by raising `ValueError` when snapshot metadata fails to satisfy gate constraints.

### Delta Store Policy and Configuration

Storage policies (`src/datafusion_engine/delta_store_policy.py`) centralize object-store and log-store configuration:

```python
# src/datafusion_engine/delta_store_policy.py:14-20
@dataclass(frozen=True)
class DeltaStorePolicy:
    """Runtime-level object store and log store configuration."""
    storage_options: Mapping[str, str] = field(default_factory=dict)
    log_storage_options: Mapping[str, str] = field(default_factory=dict)
    require_local_paths: bool = False
```

**Policy Resolution** (`src/datafusion_engine/delta_store_policy.py:22-50`):

The `resolve_delta_store_policy()` function merges runtime-level policy with per-table overrides using three-layer precedence:

1. Base policy from runtime configuration
2. Table-specific storage options (override base)
3. Table-specific log-storage options (override base log config)

**Policy Hashing for Determinism** (`src/datafusion_engine/delta_store_policy.py:98-116`):

The `delta_store_policy_hash()` function produces stable SHA256 hashes of policy configuration, ensuring that storage option changes trigger plan invalidations.

---

## Scan Planning and File Pruning

### ScanUnit Abstraction

The `ScanUnit` dataclass (`src/datafusion_engine/scan_planner.py:96-118`) represents a deterministic, pruned scan description:

```python
@dataclass(frozen=True)
class ScanUnit:
    """Deterministic, pruned scan description for scheduling."""
    key: str  # Stable hash for deduplication
    dataset_name: str
    delta_version: int | None
    delta_timestamp: str | None
    snapshot_timestamp: int | None
    delta_feature_gate: DeltaFeatureGate | None
    delta_protocol: Mapping[str, object] | None
    storage_options_hash: str | None
    delta_scan_config_hash: str | None
    datafusion_provider: str | None
    protocol_compatible: bool | None
    total_files: int
    candidate_file_count: int
    pruned_file_count: int
    candidate_files: tuple[Path, ...]
    pushed_filters: tuple[str, ...]
    projected_columns: tuple[str, ...]
```

**Scan Unit Key Generation** (`src/datafusion_engine/scan_planner.py:72-92`):

Keys incorporate all inputs affecting scan results:
- Dataset name and version (or timestamp)
- Delta protocol requirements and feature gates
- Storage options hash
- Scan config hash (schema, pushdown settings)
- DataFusion provider type
- Projected columns and pushed filters

Keys are SHA256 hashes (16-char prefix) of msgpack-serialized payloads, ensuring identical scans produce identical keys for deduplication.

### File Pruning Pipeline

**Index Construction** (`src/storage/deltalake/file_index.py:105-161`):

The `build_delta_file_index_from_add_actions()` function transforms Rust control-plane add actions into normalized PyArrow tables:

1. Extracts `path`, `size`, `modification_time`, `partition_values`, `stats` from each add action
2. Parses JSON stats payloads to extract `minValues` and `maxValues`
3. Normalizes to canonical schema with `stats_min` and `stats_max` as map columns
4. Returns PyArrow table suitable for DataFusion predicate evaluation

**Pruning Policy** (`src/storage/deltalake/file_pruning.py:42-100`):

```python
@dataclass(frozen=True)
class FilePruningPolicy:
    """File pruning policy with partition and statistics filters."""
    partition_filters: list[PartitionFilter]  # Equality on partition cols
    stats_filters: list[StatsFilter]  # Range filters on stats_min/max
```

Partition filters apply equality/in-list predicates to `partition_values` maps. Stats filters apply range predicates (`>`, `<`, `=`) to `stats_min`/`stats_max` maps, leveraging Parquet column statistics for file-level pruning.

**Predicate-Based Filtering** (`src/storage/deltalake/file_pruning.py:148-192`):

The `evaluate_filters_against_index()` function:

1. Registers file index as temporary DataFusion table
2. Converts `FilePruningPolicy` to DataFusion predicate expressions
3. Applies predicate via `df.filter(predicate)`
4. Materializes filtered table and deregisters temp table

### Scan Planning Workflow

**Plan Scan Unit** (`src/datafusion_engine/scan_planner.py:148-209`):

1. **Resolve Configuration**: Extract feature gate, storage options hash, scan config, provider type
2. **Delta Scan Resolution**: Call `_resolve_delta_scan_resolution()` to fetch add actions, apply pruning, check protocol
3. **Scan Unit Key Generation**: Build deterministic key from all resolved inputs
4. **Return ScanUnit**: Package pruned files, statistics, and metadata

**Bulk Planning** (`src/datafusion_engine/scan_planner.py:285-316`):

The `plan_scan_units()` function plans scans for all tasks:

1. Iterates over tasks and their scan lineage entries
2. Plans `ScanUnit` for each unique dataset/lineage combination
3. Deduplicates scan units by key
4. Returns sorted scan units plus per-task scan key mappings

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

The `IncrementalRuntime` dataclass (`src/incremental/runtime.py:22-80`) bundles DataFusion runtime with determinism tracking:

```python
@dataclass
class IncrementalRuntime:
    """Runtime container for DataFusion incremental execution."""
    profile: DataFusionRuntimeProfile
    _session_runtime: SessionRuntime
    determinism_tier: DeterminismTier = DeterminismTier.BEST_EFFORT
```

**Temp Table Registry** (`src/incremental/runtime.py:82-156`):

The `TempTableRegistry` context manager tracks temporary DataFusion tables for cleanup:

1. `register_table()`: Registers PyArrow table, assigns UUID-based name
2. `register_batches()`: Registers record batches
3. `track()`: Tracks externally registered table name
4. `deregister()`: Safely deregisters table
5. `close()`: Deregisters all tracked tables on context exit

---

## Invalidation and Change Detection

### Invalidation Snapshot Schema

Invalidation snapshots (`src/incremental/invalidations.py:42-47`) use strict Arrow schemas:

```python
_INVALIDATION_SCHEMA = pa.schema([
    pa.field("version", pa.int32(), nullable=False),
    pa.field("incremental_plan_fingerprints", pa.list_(_PLAN_FINGERPRINT_ENTRY), nullable=False),
    pa.field("incremental_metadata_hash", pa.string(), nullable=True),
    pa.field("runtime_profile_hash", pa.string(), nullable=True),
])
```

Each snapshot row captures:
- **Plan fingerprints**: List of `(plan_name, plan_fingerprint, plan_task_signature)` structs
- **Metadata hash**: Hash of DataFusion settings and runtime profile
- **Runtime profile hash**: Standalone runtime profile hash

### Invalidation Diffing

**Diff Snapshots** (`src/incremental/invalidations.py:194-217`):

The `diff_invalidation_snapshots()` function produces reason codes:

1. **Runtime Profile Change**: `runtime_profile_hash` mismatch → `"runtime_profile_hash"` reason
2. **Metadata Change**: `incremental_metadata_hash` mismatch → `"incremental_metadata"` reason
3. **Plan Changes**: Calls `_diff_mapping()` to detect missing/changed plan fingerprints:
   - `"incremental_plan_fingerprint_missing:plan_name"`
   - `"incremental_plan_fingerprint_changed:plan_name"`

**Check State Store Invalidation** (`src/incremental/invalidations.py:220-241`):

1. Build current snapshot via `build_invalidation_snapshot()`
2. Read previous snapshot via `read_invalidation_snapshot()`
3. If no previous snapshot exists, write current and return `InvalidationResult(invalidated=False)`
4. Diff snapshots; if reasons exist, write updated snapshot and return `InvalidationResult(invalidated=True)`
5. Otherwise return `InvalidationResult(invalidated=False)`

---

## Change Data Feed (CDF) Processing

### CDF Cursor Management

CDF cursors (`src/incremental/cdf_cursors.py:15-28`) track the last processed Delta version:

```python
class CdfCursor(StructBase, frozen=True):
    """Cursor tracking the last processed Delta table version for CDF reads."""
    dataset_name: str
    last_version: NonNegInt  # Annotated int with >=0 constraint
```

**Cursor Store** (`src/incremental/cdf_cursors.py:30-147`):

- **Storage**: JSON files in `cursors_path/<dataset_name>.cursor.json`
- **Save**: `save_cursor()` writes msgspec JSON
- **Load**: `load_cursor()` reads and deserializes, returns `None` if missing
- **List**: `list_cursors()` glob-matches `*.cursor.json` files

### CDF Filter Policies

**Change Types** (`src/incremental/cdf_filters.py:13-61`):

```python
class CdfChangeType(Enum):
    INSERT = "insert"
    UPDATE_POSTIMAGE = "update_postimage"  # Final state after update
    DELETE = "delete"
```

**Filter Policy** (`src/incremental/cdf_filters.py:64-191`):

```python
@dataclass(frozen=True)
class CdfFilterPolicy:
    """Policy for filtering Delta CDF changes by operation type."""
    include_insert: bool = True
    include_update_postimage: bool = True
    include_delete: bool = True
```

Factory methods:
- `include_all()`: All change types
- `inserts_and_updates_only()`: Excludes deletes
- `inserts_only()`: Only inserts

### CDF Read Workflow

**Read CDF Changes** (`src/incremental/cdf_runtime.py:148-212`):

1. Resolve CDF inputs via `_resolve_cdf_inputs()`
2. Prepare read state via `_prepare_cdf_read_state()`
3. If no state (no changes), return `None`
4. Register Delta CDF provider with version window
5. Read CDF table, apply filter policy
6. Save updated cursor at `current_version`
7. Return `CdfReadResult(table=table, updated_version=current_version)`

---

## Key Design Patterns

### Determinism Through Hashing

**Policy Hashes**:
- `delta_store_policy_hash()` hashes sorted storage options
- `delta_scan_config_hash()` hashes msgpack-serialized scan config
- `_storage_options_hash()` hashes sorted storage + log storage options

These hashes become inputs to scan unit keys and plan fingerprints.

**Plan Fingerprints**:
- `plan_fingerprint`: Hash of compiled plan structure
- `plan_task_signature`: Hash of plan + task scheduling metadata

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
| `src/datafusion_engine/scan_planner.py` | ~810 | Scan planning with pruning |
| `src/storage/deltalake/file_pruning.py` | ~450 | File-level pruning |
| `src/incremental/invalidations.py` | ~400 | Change detection |
| `src/incremental/cdf_runtime.py` | ~220 | CDF read orchestration |
| `src/incremental/cdf_cursors.py` | ~150 | CDF version tracking |
| `src/incremental/state_store.py` | ~180 | State layout |
| `src/datafusion_engine/delta_protocol.py` | ~150 | Protocol compatibility |
| `src/datafusion_engine/delta_control_plane.py` | ~2300 | Control plane facade |

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
