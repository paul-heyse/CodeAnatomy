# Configuration Reference

## Overview

CodeAnatomy's runtime behavior is controlled through environment variables, configuration dataclasses, and policy objects. This document provides a comprehensive reference for all configuration surfaces across the system.

**Configuration Philosophy**: CodeAnatomy uses a hierarchical configuration model where:
- Environment variables provide runtime overrides and deployment-specific settings
- Policy objects (`*Policy`) control runtime behavior (e.g., `DeltaWritePolicy`)
- Settings objects (`*Settings`) define initialization parameters (e.g., `DiskCacheSettings`)
- Config objects (`*Config`) encapsulate request/command parameters (e.g., `OtelConfig`)
- Spec objects (`*Spec`) provide declarative schema definitions (e.g., `TableSpec`)
- Options objects (`*Options`) bundle optional parameters (e.g., `CompileOptions`)

## Runtime Profiles

### DataFusionRuntimeProfile

**File**: `src/datafusion_engine/session/runtime.py`

The `DataFusionRuntimeProfile` is the central configuration class for DataFusion execution sessions. It encapsulates DataFusion SessionConfig settings, catalog configuration, external table registrations, optimizer behavior, and UDF policies. Unlike a simple dataclass, it is a rich configuration class with extensive fields for controlling session behavior.

**Key Configuration Categories**:

- **Execution Settings**: `target_partitions`, `batch_size`, `memory_limit_bytes`, `spill_dir`
- **Catalog Configuration**: `default_catalog`, `default_schema`, `registry_catalogs`
- **External Tables**: `ast_catalog_location`, `bytecode_catalog_location`, `extract_dataset_locations`
- **Feature Toggles**: `enable_information_schema`, `enable_url_table`, `cache_enabled`
- **Hook Points**: `function_factory_hook`, `expr_planner_hook`

**Key Profile Presets**:

- **`default`**: General-purpose profile (8 GiB memory, moderate concurrency)
- **`dev`**: Development profile (4 GiB memory, reduced concurrency)
- **`prod`**: Production profile (16 GiB memory, high concurrency)
- **`cst_autoload`**: LibCST extraction with catalog auto-loading
- **`symtable`**: Symtable extraction with statistics disabled

### RuntimeProfileSpec

**File**: `src/engine/runtime_profile.py`

The `RuntimeProfileSpec` combines DataFusion runtime configuration with determinism tier and Hamilton telemetry settings.

```python
@dataclass(frozen=True)
class RuntimeProfileSpec:
    name: str                                         # Profile name
    datafusion: DataFusionRuntimeProfile              # DataFusion runtime config
    determinism_tier: DeterminismTier                 # Determinism level
    tracker_config: HamiltonTrackerConfig | None      # engine.telemetry.hamilton.HamiltonTrackerConfig
    hamilton_telemetry: HamiltonTelemetryProfile | None  # engine.telemetry.hamilton.HamiltonTelemetryProfile
```

**Properties**:
- `datafusion_settings_hash`: Stable hash of DataFusion settings
- `runtime_profile_hash`: Hash combining profile name, tier, and telemetry

### DataFusionConfigPolicy

**File**: `src/datafusion_engine/session/runtime.py`

Policy object containing DataFusion SessionConfig settings as key-value mappings.

**Fields**:
- `settings: Mapping[str, str]`: DataFusion config key-value pairs

**Common Settings** (from `DEFAULT_DF_POLICY`):

| Setting Key | Default Value | Description |
|-------------|---------------|-------------|
| `datafusion.execution.collect_statistics` | `"true"` | Enable statistics collection |
| `datafusion.execution.meta_fetch_concurrency` | `"8"` | Metadata fetch parallelism |
| `datafusion.execution.planning_concurrency` | `"8"` | Planning parallelism |
| `datafusion.execution.parquet.pushdown_filters` | `"true"` | Enable filter pushdown |
| `datafusion.execution.parquet.max_predicate_cache_size` | `"67108864"` | 64 MiB predicate cache |
| `datafusion.execution.parquet.enable_page_index` | `"true"` | Use Parquet page index |
| `datafusion.runtime.list_files_cache_limit` | `"134217728"` | 128 MiB file list cache |
| `datafusion.runtime.metadata_cache_limit` | `"268435456"` | 256 MiB metadata cache |
| `datafusion.runtime.memory_limit` | `"8589934592"` | 8 GiB memory limit |
| `datafusion.runtime.temp_directory` | `"/tmp/datafusion"` | Temp directory path |

**Environment Overrides**:
- `CODEANATOMY_DATAFUSION_CATALOG_LOCATION`: Catalog auto-load location
- `CODEANATOMY_DATAFUSION_CATALOG_FORMAT`: Catalog file format

### SchemaHardeningProfile

**File**: `src/datafusion_engine/session/runtime.py`

Schema-stability settings for deterministic schema evolution and explain outputs.

**Fields**:

```python
@dataclass(frozen=True)
class SchemaHardeningProfile:
    enable_view_types: bool = False                   # Use Utf8View/BinaryView types
    expand_views_at_output: bool = False              # Expand view types at output
    timezone: str = "UTC"                             # Execution timezone
    parser_dialect: str | None = None                 # SQL parser dialect
    show_schema_in_explain: bool = True               # Include schema in EXPLAIN
    explain_format: str = "tree"                      # EXPLAIN format (tree/verbose)
    show_types_in_format: bool = True                 # Show types in formatted output
    strict_aggregate_schema_check: bool = True        # Validate aggregate schemas
```

**Preset Profiles**:
- `schema_hardening`: Standard hardening (default)
- `arrow_performance`: Enable Utf8View/BinaryView for performance

### DataFusionFeatureGates

**File**: `src/datafusion_engine/session/runtime.py`

Optimizer feature toggles for DataFusion dynamic filter pushdown.

**Fields**:

```python
@dataclass(frozen=True)
class DataFusionFeatureGates:
    enable_dynamic_filter_pushdown: bool = True
    enable_join_dynamic_filter_pushdown: bool = True
    enable_aggregate_dynamic_filter_pushdown: bool = True
    enable_topk_dynamic_filter_pushdown: bool = True
```

**Note**: `enable_aggregate_dynamic_filter_pushdown` is automatically disabled for DataFusion v51+.

### DataFusionJoinPolicy

**File**: `src/datafusion_engine/session/runtime.py`

Join algorithm preferences for DataFusion execution.

**Fields**:

```python
@dataclass(frozen=True)
class DataFusionJoinPolicy:
    enable_hash_join: bool = True
    enable_sort_merge_join: bool = True
    enable_nested_loop_join: bool = True
    repartition_joins: bool = True
    enable_round_robin_repartition: bool = True
    perfect_hash_join_small_build_threshold: int | None = None
    perfect_hash_join_min_key_density: float | None = None
```

## Environment Variables

### OpenTelemetry Configuration

**File**: `src/obs/otel/config.py`, `src/obs/otel/bootstrap.py`

OpenTelemetry observability settings control tracing, metrics, and logging exporters.

#### Core OTEL Settings

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `OTEL_SDK_DISABLED` | bool | `false` | Disable OpenTelemetry SDK |
| `OTEL_TRACES_EXPORTER` | string | (enabled) | Trace exporter type (set to "none" to disable) |
| `OTEL_METRICS_EXPORTER` | string | (enabled) | Metrics exporter type (set to "none" to disable) |
| `OTEL_LOGS_EXPORTER` | string | (enabled) | Logs exporter type (set to "none" to disable) |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | string | `"grpc"` | OTLP protocol (grpc/http) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | string | (none) | OTLP endpoint (e.g., `http://localhost:4317`) |
| `OTEL_EXPORTER_OTLP_HEADERS` | string | (none) | OTLP headers (comma-separated `key=value`) |
| `OTEL_EXPORTER_OTLP_TIMEOUT` | int | (none) | OTLP export timeout (ms) |
| `OTEL_EXPORTER_OTLP_TRACES_PROTOCOL` | string | (inherits) | Trace-specific OTLP protocol |
| `OTEL_EXPORTER_OTLP_METRICS_PROTOCOL` | string | (inherits) | Metrics-specific OTLP protocol |
| `OTEL_EXPORTER_OTLP_LOGS_PROTOCOL` | string | (inherits) | Logs-specific OTLP protocol |

#### Trace Configuration

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `OTEL_TRACES_SAMPLER` | string | `"parentbased_traceidratio"` | Sampler type (always_on, always_off, traceidratio, parentbased_*) |
| `OTEL_TRACES_SAMPLER_ARG` | float | `0.1` | Sampler ratio for traceidratio (0.0-1.0) |
| `OTEL_BSP_SCHEDULE_DELAY` | int | `5000` | BatchSpanProcessor schedule delay (ms) |
| `OTEL_BSP_EXPORT_TIMEOUT` | int | `30000` | BatchSpanProcessor export timeout (ms) |
| `OTEL_BSP_MAX_QUEUE_SIZE` | int | `2048` | BatchSpanProcessor max queue size |
| `OTEL_BSP_MAX_EXPORT_BATCH_SIZE` | int | `512` | BatchSpanProcessor max batch size |

#### Metrics Configuration

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `OTEL_METRIC_EXPORT_INTERVAL` | int | `60000` | Metric export interval (ms) |
| `OTEL_METRIC_EXPORT_TIMEOUT` | int | `30000` | Metric export timeout (ms) |
| `OTEL_METRICS_EXEMPLAR_FILTER` | string | (none) | Exemplar filter (always_on, always_off, trace_based) |
| `OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE` | string | (none) | Temporality preference (cumulative, delta, lowmemory) |
| `OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION` | string | (none) | Histogram aggregation (explicit_bucket_histogram, exponential_bucket_histogram) |

#### Logs Configuration

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `OTEL_PYTHON_LOG_CORRELATION` | bool | `true` | Enable log-trace correlation |
| `OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED` | bool | `false` | Auto-instrument logging |
| `OTEL_BLRP_SCHEDULE_DELAY` | int | `5000` | BatchLogRecordProcessor schedule delay (ms) |
| `OTEL_BLRP_EXPORT_TIMEOUT` | int | `30000` | BatchLogRecordProcessor export timeout (ms) |
| `OTEL_BLRP_MAX_QUEUE_SIZE` | int | `2048` | BatchLogRecordProcessor max queue size |
| `OTEL_BLRP_MAX_EXPORT_BATCH_SIZE` | int | `512` | BatchLogRecordProcessor max batch size |

#### Resource Attributes

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `CODEANATOMY_SERVICE_VERSION` | string | (auto) | Service version for resource attributes |
| `CODEANATOMY_SERVICE_NAMESPACE` | string | (none) | Service namespace |
| `CODEANATOMY_ENVIRONMENT` | string | (none) | Deployment environment (dev/prod/ci) |
| `DEPLOYMENT_ENVIRONMENT` | string | (none) | Alternative deployment environment variable |
| `OTEL_SCHEMA_URL` | string | (none) | OpenTelemetry schema URL |
| `CODEANATOMY_OTEL_SCHEMA_URL` | string | (none) | Alternative schema URL |

#### Span/Log Limits

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `OTEL_ATTRIBUTE_COUNT_LIMIT` | int | (none) | Global attribute count limit |
| `OTEL_ATTRIBUTE_VALUE_LENGTH_LIMIT` | int | (none) | Global attribute value length limit |
| `OTEL_SPAN_ATTRIBUTE_COUNT_LIMIT` | int | (none) | Span attribute count limit |
| `OTEL_SPAN_ATTRIBUTE_VALUE_LENGTH_LIMIT` | int | (none) | Span attribute value length limit |
| `OTEL_SPAN_EVENT_COUNT_LIMIT` | int | (none) | Span event count limit |
| `OTEL_SPAN_LINK_COUNT_LIMIT` | int | (none) | Span link count limit |
| `OTEL_EVENT_ATTRIBUTE_COUNT_LIMIT` | int | (none) | Event attribute count limit |
| `OTEL_LINK_ATTRIBUTE_COUNT_LIMIT` | int | (none) | Link attribute count limit |
| `OTEL_LOGRECORD_ATTRIBUTE_COUNT_LIMIT` | int | (none) | Log record attribute count limit |
| `OTEL_LOGRECORD_ATTRIBUTE_VALUE_LENGTH_LIMIT` | int | (none) | Log record attribute value length limit |

#### Advanced OTEL Settings

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `OTEL_LOG_LEVEL` | string | (none) | OTEL internal log level (DEBUG, INFO, WARNING, ERROR) |
| `OTEL_PYTHON_CONTEXT` | string | (none) | Python context implementation |
| `OTEL_PYTHON_ID_GENERATOR` | string | (none) | Trace ID generator (random, xray) |
| `OTEL_EXPERIMENTAL_CONFIG_FILE` | string | (none) | OTEL experimental config file path |
| `CODEANATOMY_OTEL_TEST_MODE` | bool | `false` | Use in-memory exporters for testing |
| `CODEANATOMY_OTEL_AUTO_INSTRUMENTATION` | bool | `false` | Enable auto-instrumentation |
| `CODEANATOMY_OTEL_SYSTEM_METRICS` | bool | `false` | Enable system metrics instrumentation |
| `CODEANATOMY_OTEL_SAMPLING_RULE` | string | `"codeanatomy.default"` | Sampling rule identifier |

### Hamilton Configuration

**File**: `src/engine/runtime_profile.py`, `src/hamilton_pipeline/driver_factory.py`

Hamilton tracker and telemetry settings for pipeline observability.

#### Tracker Configuration

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `CODEANATOMY_HAMILTON_PROJECT_ID` | int | (none) | Hamilton project ID |
| `HAMILTON_PROJECT_ID` | int | (none) | Alternative project ID variable |
| `CODEANATOMY_HAMILTON_USERNAME` | string | (none) | Hamilton username |
| `HAMILTON_USERNAME` | string | (none) | Alternative username variable |
| `CODEANATOMY_HAMILTON_DAG_NAME` | string | (none) | DAG name for tracker |
| `HAMILTON_DAG_NAME` | string | (none) | Alternative DAG name variable |
| `CODEANATOMY_HAMILTON_API_URL` | string | (none) | Hamilton tracker API URL |
| `HAMILTON_API_URL` | string | (none) | Alternative API URL variable |
| `CODEANATOMY_HAMILTON_UI_URL` | string | (none) | Hamilton UI URL |
| `HAMILTON_UI_URL` | string | (none) | Alternative UI URL variable |

#### Telemetry Profile

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `CODEANATOMY_HAMILTON_TELEMETRY_PROFILE` | string | (from env) | Telemetry profile name (dev/prod/ci/test) |
| `HAMILTON_TELEMETRY_PROFILE` | string | (from env) | Alternative telemetry profile variable |
| `CODEANATOMY_HAMILTON_TRACKER_ENABLED` | bool | (profile) | Override tracker enable flag |
| `HAMILTON_TRACKER_ENABLED` | bool | (profile) | Alternative tracker enable variable |
| `CODEANATOMY_HAMILTON_CAPTURE_DATA_STATISTICS` | bool | (profile) | Capture data statistics |
| `HAMILTON_CAPTURE_DATA_STATISTICS` | bool | (profile) | Alternative statistics capture variable |
| `CODEANATOMY_HAMILTON_MAX_LIST_LENGTH_CAPTURE` | int | (profile) | Max list length for capture |
| `HAMILTON_MAX_LIST_LENGTH_CAPTURE` | int | (profile) | Alternative max list length variable |
| `CODEANATOMY_HAMILTON_MAX_DICT_LENGTH_CAPTURE` | int | (profile) | Max dict length for capture |
| `HAMILTON_MAX_DICT_LENGTH_CAPTURE` | int | (profile) | Alternative max dict length variable |
| `CODEANATOMY_HAMILTON_CACHE_PATH` | string | (none) | Hamilton cache directory path |
| `HAMILTON_CACHE_PATH` | string | (none) | Alternative cache path variable |

**Telemetry Profile Defaults**:

| Profile | Tracker Enabled | Capture Stats | Max List | Max Dict |
|---------|----------------|---------------|----------|----------|
| `prod` / `production` | `true` | `false` | 20 | 50 |
| `ci` / `test` | `false` | `false` | 5 | 10 |
| `dev` (default) | `true` | `true` | 200 | 200 |

### Delta Lake Settings

**File**: `src/datafusion_engine/delta_protocol.py`, `src/storage/deltalake/config.py`

Delta Lake protocol support and write policies.

#### Delta Protocol Support

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| (No env vars) | N/A | N/A | Protocol support configured via `DeltaProtocolSupport` dataclass |

**DeltaProtocolSupport Fields**:
- `max_reader_version: int | None`: Maximum reader protocol version supported
- `max_writer_version: int | None`: Maximum writer protocol version supported
- `supported_reader_features: tuple[str, ...]`: Reader features supported
- `supported_writer_features: tuple[str, ...]`: Writer features supported

#### Delta Write Policy

**File**: `src/storage/deltalake/config.py`

```python
@dataclass(frozen=True)
class DeltaWritePolicy:
    target_file_size: int | None = 96 * 1024 * 1024  # 96 MiB target file size
    partition_by: tuple[str, ...] = ()                # Partition columns
    zorder_by: tuple[str, ...] = ()                   # Z-order columns
    stats_policy: Literal["off", "explicit", "auto"] = "auto"  # Stats collection mode
    stats_columns: tuple[str, ...] | None = None      # Explicit stats columns
    stats_max_columns: int = 32                       # Max auto-selected stats columns
    parquet_writer_policy: ParquetWriterPolicy | None = None  # Parquet-specific settings
    enable_features: tuple[...] = ()                  # Delta features to enable
```

**Delta Features** (in `enable_features`):
- `"change_data_feed"`: Enable Change Data Feed (CDF)
- `"deletion_vectors"`: Enable deletion vectors
- `"row_tracking"`: Enable row-level tracking
- `"in_commit_timestamps"`: Enable in-commit timestamps
- `"column_mapping"`: Enable column mapping
- `"v2_checkpoints"`: Enable v2 checkpoint format

#### Parquet Writer Policy

```python
@dataclass(frozen=True)
class ParquetWriterPolicy:
    statistics_enabled: tuple[str, ...] = ()          # Columns with statistics
    statistics_level: Literal["none", "chunk", "page"] = "page"
    bloom_filter_enabled: tuple[str, ...] = ()        # Columns with Bloom filters
    bloom_filter_fpp: float | None = None             # False positive probability
    bloom_filter_ndv: int | None = None               # Number of distinct values
    dictionary_enabled: tuple[str, ...] = ()          # Columns with dictionary encoding
```

### Cache Settings

**File**: `src/cache/diskcache_factory.py`

DiskCache configuration for plan caching, extraction caching, and runtime caching.

#### Cache Root Directory

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `CODEANATOMY_DISKCACHE_DIR` | string | `~/.cache/codeanatomy/diskcache` | Root directory for all DiskCache instances |

#### Cache Kinds

CodeAnatomy uses separate cache instances for different subsystems:

| Cache Kind | Default Size Limit | TTL | Shards | Description |
|------------|-------------------|-----|--------|-------------|
| `plan` | 512 MiB | None | 1 | DataFusion plan bundles and Substrait bytes |
| `extract` | 8 GiB | 24 hours | 8 | Extraction artifacts (AST, CST, symtable, bytecode) |
| `schema` | 256 MiB | 5 minutes | 1 | Schema introspection results |
| `repo_scan` | 512 MiB | 30 minutes | 1 | Repository scan metadata |
| `runtime` | 256 MiB | 24 hours | 1 | Runtime profile and UDF snapshots |
| `queue` | 128 MiB | None | 1 | Persistent queue storage |
| `index` | 128 MiB | None | 1 | Persistent index storage |
| `coordination` | 64 MiB | None | 1 | Coordination metadata |

### DiskCacheSettings

**File**: `src/cache/diskcache_factory.py`

```python
@dataclass(frozen=True)
class DiskCacheSettings:
    size_limit_bytes: int                             # Cache size limit
    cull_limit: int = 10                              # Items to cull per eviction pass
    eviction_policy: str = "least-recently-used"      # Eviction policy
    statistics: bool = True                           # Enable statistics tracking
    tag_index: bool = True                            # Enable tag-based indexing
    shards: int | None = None                         # Number of shards (None = single cache)
    timeout_seconds: float = 60.0                     # Lock timeout
    disk_min_file_size: int | None = None             # Minimum file size for disk storage
    sqlite_journal_mode: str | None = "wal"           # SQLite journal mode
    sqlite_mmap_size: int | None = None               # SQLite memory-mapped size
    sqlite_synchronous: str | None = None             # SQLite synchronous mode
```

**Eviction Policies**:
- `"least-recently-used"`: Evict least recently used entries
- `"least-recently-stored"`: Evict oldest stored entries
- `"least-frequently-used"`: Evict least frequently accessed entries
- `"none"`: No automatic eviction

### Storage Options

**File**: `src/utils/storage_options.py`

Storage options for Delta Lake and object store access.

**Normalization Function**:
```python
def normalize_storage_options(
    storage_options: Mapping[str, object] | None,
    log_storage_options: Mapping[str, object] | None = None,
    *,
    fallback_log_to_storage: bool = False,
) -> tuple[dict[str, str] | None, dict[str, str] | None]:
    """Normalize storage and log storage option mappings."""
```

**Common Storage Options** (for S3/Azure/GCS):
- `"AWS_ACCESS_KEY_ID"`: AWS access key
- `"AWS_SECRET_ACCESS_KEY"`: AWS secret key
- `"AWS_REGION"`: AWS region
- `"AZURE_STORAGE_ACCOUNT_NAME"`: Azure storage account
- `"AZURE_STORAGE_ACCOUNT_KEY"`: Azure storage key
- `"GOOGLE_SERVICE_ACCOUNT"`: GCS service account JSON

### Runtime Configuration

**File**: `src/hamilton_pipeline/driver_factory.py`, `src/hamilton_pipeline/modules/inputs.py`

Pipeline runtime and determinism tier configuration.

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `CODEANATOMY_RUNTIME_PROFILE` | string | `"default"` | Runtime profile name (default/dev/prod/cst_autoload/symtable) |
| `CODEANATOMY_DETERMINISM_TIER` | string | `"best_effort"` | Determinism tier (canonical/stable_set/best_effort) |
| `CODEANATOMY_FORCE_TIER2` | bool | `false` | Force CANONICAL tier regardless of other settings |
| `CODEANATOMY_PIPELINE_MODE` | string | (none) | Pipeline execution mode |
| `CODEANATOMY_STATE_DIR` | string | (none) | Incremental state directory |
| `CODEANATOMY_ENV` | string | (none) | Environment identifier (dev/prod/ci) |
| `CODEANATOMY_TEAM` | string | (none) | Team identifier for telemetry |

### Git Configuration

**File**: `src/extract/git_settings.py`, `src/extract/git_remotes.py`

Git remote access and repository scanning settings.

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `CODEANATOMY_GIT_USERNAME` | string | (none) | Git remote username |
| `CODEANATOMY_GIT_PASSWORD` | string | (none) | Git remote password |
| `CODEANATOMY_GIT_SSH_PUBKEY` | string | (none) | SSH public key path |
| `CODEANATOMY_GIT_SSH_KEY` | string | (none) | SSH private key path |
| `CODEANATOMY_GIT_SSH_PASSPHRASE` | string | (none) | SSH key passphrase |
| `CODEANATOMY_GIT_ALLOW_INVALID_CERTS` | bool | `false` | Allow invalid SSL certificates |
| `CODEANATOMY_GIT_OWNER_VALIDATION` | bool | (none) | Validate repository ownership |
| `CODEANATOMY_GIT_SERVER_TIMEOUT_MS` | int | (none) | Git server timeout (ms) |
| `CODEANATOMY_GIT_SSL_CERT_FILE` | string | (none) | SSL certificate file path |

### Incremental Processing

**File**: `src/hamilton_pipeline/modules/inputs.py`

Incremental impact analysis and change detection settings.

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `CODEANATOMY_REPO_ID` | string | (none) | Repository identifier |
| `CODEANATOMY_GIT_HEAD_REF` | string | (none) | Git HEAD ref (current commit) |
| `CODEANATOMY_GIT_BASE_REF` | string | (none) | Git base ref (comparison commit) |
| `CODEANATOMY_GIT_CHANGED_ONLY` | bool | (none) | Process only changed files |
| `CODEANATOMY_INCREMENTAL_IMPACT_STRATEGY` | string | (none) | Impact analysis strategy |
| `CODEANATOMY_ENABLE_STREAMING_TABLES` | bool | `false` | Enable streaming table processing |

### DataFusion Cache Controls

**File**: `src/engine/runtime_profile.py`

Delta-backed cache controls for view caches, dataset caches, runtime artifacts,
and metadata snapshots.

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `CODEANATOMY_CACHE_OUTPUT_ROOT` | string | (none) | Root directory for Delta-backed caches (defaults to `/tmp/datafusion_cache`) |
| `CODEANATOMY_RUNTIME_ARTIFACT_CACHE_ENABLED` | bool | `false` | Persist runtime artifact tables to Delta |
| `CODEANATOMY_RUNTIME_ARTIFACT_CACHE_ROOT` | string | (none) | Override runtime artifact cache root (defaults to `<cache_output_root>/runtime_artifacts`) |
| `CODEANATOMY_METADATA_CACHE_SNAPSHOT_ENABLED` | bool | `false` | Snapshot DataFusion metadata caches to Delta during session setup |

### Diagnostics Sink Controls

**File**: `src/engine/runtime_profile.py`, `src/datafusion_engine/lineage/diagnostics.py`

Choose the diagnostics sink used for DataFusion runtime artifacts and events.

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `CODEANATOMY_DIAGNOSTICS_SINK` | string | (none) | Diagnostics sink selection (`otel`, `memory`, `none`) |

### DataFusion Plugin

**File**: `src/datafusion_engine/runtime.py`

Rust DataFusion plugin library configuration.

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `CODEANATOMY_DF_PLUGIN_PATH` | string | (auto-detected) | Path to df_plugin_codeanatomy shared library |
| `CODEANATOMY_DF_PLUGIN_MANIFEST_PATH` | string | `build/datafusion_plugin_manifest.json` | Optional manifest file with plugin metadata and path |
| `CODEANATOMY_PLUGIN_STUB` | bool | `false` | Explicitly enable datafusion_ext stub injection (tests only) |

## Determinism Tiers

**File**: `src/core_types.py`

CodeAnatomy defines three determinism tiers that control reproducibility guarantees:

```python
class DeterminismTier(StrEnum):
    CANONICAL = "canonical"      # Tier 2: Strict determinism
    STABLE_SET = "stable_set"    # Tier 1: Stable ordering within sets
    BEST_EFFORT = "best_effort"  # Tier 0: Fast execution, minimal guarantees
```

### Tier Aliases

- `FAST` → `BEST_EFFORT`
- `STABLE` → `STABLE_SET`

### Tier Selection

**Parsing Function** (`parse_determinism_tier`):

```python
def parse_determinism_tier(value: DeterminismTier | str | None) -> DeterminismTier | None:
    """Parse a determinism tier from string or enum input."""
```

**String Mappings**:
- `"tier2"` / `"canonical"` → `CANONICAL`
- `"tier1"` / `"stable"` / `"stable_set"` → `STABLE_SET`
- `"tier0"` / `"fast"` / `"best_effort"` → `BEST_EFFORT`

### Tier Semantics

#### CANONICAL (Tier 2)

**Guarantees**:
- Deterministic plan fingerprints (Substrait-based)
- Stable output ordering (explicit sort keys)
- Reproducible aggregation order
- Fixed random seeds for sampling

**Use Cases**:
- Regression testing
- Audit trails
- Cross-version comparison

#### STABLE_SET (Tier 1)

**Guarantees**:
- Deterministic record sets (order may vary)
- Stable aggregation results
- Consistent join outputs (unordered)

**Use Cases**:
- Development workflows
- Integration testing

#### BEST_EFFORT (Tier 0)

**Guarantees**:
- Correct results (no data loss)
- No ordering guarantees
- Parallel execution allowed

**Use Cases**:
- Interactive queries
- Exploratory analysis
- Performance testing

### Environment Configuration

**Override Precedence**:
1. `CODEANATOMY_FORCE_TIER2=true` → Always use `CANONICAL`
2. `CODEANATOMY_DETERMINISM_TIER=<tier>` → Explicit tier selection
3. Default to `BEST_EFFORT`

## Configuration Examples

### Production DataFusion Profile

```python
from datafusion_engine.runtime import DataFusionRuntimeProfile
from core_types import DeterminismTier
from engine.runtime_profile import RuntimeProfileSpec

# Use production preset with canonical determinism
profile_spec = RuntimeProfileSpec(
    name="prod",
    datafusion=PROD_DATAFUSION_PROFILE,
    determinism_tier=DeterminismTier.CANONICAL,
)
```

**Environment Variables**:
```bash
export CODEANATOMY_RUNTIME_PROFILE=prod
export CODEANATOMY_DETERMINISM_TIER=canonical
export CODEANATOMY_DATAFUSION_CATALOG_LOCATION=/data/catalogs
export CODEANATOMY_DISKCACHE_DIR=/data/cache
```

### Delta Lake Write Policy

```python
from storage.deltalake.config import DeltaWritePolicy, ParquetWriterPolicy

# Configure Delta writes with partitioning and CDF
write_policy = DeltaWritePolicy(
    target_file_size=128 * 1024 * 1024,  # 128 MiB
    partition_by=("repo_id", "file_path"),
    zorder_by=("node_id",),
    stats_policy="auto",
    stats_max_columns=64,
    enable_features=("change_data_feed", "column_mapping"),
    parquet_writer_policy=ParquetWriterPolicy(
        statistics_level="page",
        bloom_filter_enabled=("node_id", "symbol_name"),
        bloom_filter_fpp=0.01,
    ),
)
```

### Hamilton Tracker Configuration

```bash
# Production tracker with reduced capture limits
export CODEANATOMY_HAMILTON_PROJECT_ID=42
export CODEANATOMY_HAMILTON_USERNAME=data-team
export CODEANATOMY_HAMILTON_DAG_NAME=cpg_builder
export CODEANATOMY_HAMILTON_API_URL=https://tracker.example.com/api
export CODEANATOMY_HAMILTON_TELEMETRY_PROFILE=prod
export CODEANATOMY_HAMILTON_TRACKER_ENABLED=true
export CODEANATOMY_HAMILTON_CAPTURE_DATA_STATISTICS=false
export CODEANATOMY_HAMILTON_MAX_LIST_LENGTH_CAPTURE=20
export CODEANATOMY_HAMILTON_MAX_DICT_LENGTH_CAPTURE=50
```

### OpenTelemetry Observability

```bash
# Enable full observability with OTLP export
export OTEL_SDK_DISABLED=false
export OTEL_EXPORTER_OTLP_PROTOCOL=grpc
export OTEL_EXPORTER_OTLP_ENDPOINT=https://otel-collector.example.com:4317

# Trace configuration
export OTEL_TRACES_SAMPLER=parentbased_traceidratio
export OTEL_TRACES_SAMPLER_ARG=0.1
export OTEL_BSP_MAX_QUEUE_SIZE=4096
export OTEL_BSP_MAX_EXPORT_BATCH_SIZE=1024

# Metrics configuration
export OTEL_METRIC_EXPORT_INTERVAL=30000
export OTEL_METRICS_EXEMPLAR_FILTER=trace_based
export OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE=delta

# Resource attributes
export CODEANATOMY_SERVICE_VERSION=2.0.0
export CODEANATOMY_SERVICE_NAMESPACE=data-platform
export CODEANATOMY_ENVIRONMENT=production

# Advanced features
export CODEANATOMY_OTEL_AUTO_INSTRUMENTATION=true
export CODEANATOMY_OTEL_SYSTEM_METRICS=true
```

### DiskCache Customization

```python
from cache.diskcache_factory import DiskCacheProfile, DiskCacheSettings

# Custom cache profile with larger extract cache
profile = DiskCacheProfile(
    root=Path("/data/cache"),
    base_settings=DiskCacheSettings(size_limit_bytes=2 * 1024**3),
    overrides={
        "extract": DiskCacheSettings(
            size_limit_bytes=16 * 1024**3,  # 16 GiB
            shards=16,
            disk_min_file_size=1024 * 1024,  # 1 MiB
            sqlite_journal_mode="wal",
        ),
        "plan": DiskCacheSettings(
            size_limit_bytes=1 * 1024**3,  # 1 GiB
            eviction_policy="least-recently-used",
        ),
    },
    ttl_seconds={
        "extract": 7 * 24 * 60 * 60,  # 7 days
        "plan": None,  # No TTL
    },
)
```

**Environment Variable**:
```bash
export CODEANATOMY_DISKCACHE_DIR=/data/cache
```

## Configuration Validation

### Environment Variable Parsing

CodeAnatomy provides strict parsing utilities in `src/utils/env_utils.py`:

**Boolean Parsing**:
```python
from utils.env_utils import env_bool, env_bool_strict

# Flexible boolean parsing (accepts 1/true/yes/y)
enabled = env_bool("FEATURE_ENABLED", default=False)

# Strict boolean parsing (only true/false)
strict_flag = env_bool_strict("STRICT_FLAG", default=True)
```

**Integer Parsing**:
```python
from utils.env_utils import env_int

# Parse integer with default fallback
timeout = env_int("TIMEOUT_MS", default=5000)
```

**Float Parsing**:
```python
from utils.env_utils import env_float

# Parse float with default fallback
ratio = env_float("SAMPLING_RATIO", default=0.1)
```

**String Parsing**:
```python
from utils.env_utils import env_value, env_text

# Get raw environment variable (None if unset/empty)
api_key = env_value("API_KEY")

# Get with default and normalization
endpoint = env_text("ENDPOINT", default="http://localhost", strip=True)
```

### Storage Options Normalization

```python
from utils.storage_options import normalize_storage_options, merged_storage_options

# Normalize storage options for Delta Lake
storage_opts = {"AWS_REGION": "us-west-2", "AWS_ACCESS_KEY_ID": "..."}
log_storage_opts = {"AWS_REGION": "us-east-1"}

# Get normalized tuple (storage, log_storage)
storage, log_storage = normalize_storage_options(
    storage_opts,
    log_storage_opts,
    fallback_log_to_storage=True,  # Mirror storage to log_storage if absent
)

# Get merged storage options dict
merged = merged_storage_options(storage_opts, log_storage_opts)
```

## Configuration Naming Conventions

**File**: `CLAUDE.md` (lines 119-127)

CodeAnatomy follows strict naming conventions for configuration classes:

### Policy

**Suffix**: `*Policy`
**Purpose**: Runtime behavior control
**Examples**: `DeltaWritePolicy`, `DataFusionConfigPolicy`, `DeltaStorePolicy`
**Immutability**: Typically frozen dataclasses

### Settings

**Suffix**: `*Settings`
**Purpose**: Initialization parameters
**Examples**: `DiskCacheSettings`
**Immutability**: Typically frozen dataclasses

### Config

**Suffix**: `*Config`
**Purpose**: Request/command parameters
**Examples**: `OtelConfig`, `engine.telemetry.hamilton.HamiltonTrackerConfig`
**Immutability**: Typically frozen dataclasses

### Spec

**Suffix**: `*Spec`
**Purpose**: Declarative schema definitions
**Examples**: `TableSpec`, `ViewSpec`, `RuntimeProfileSpec`
**Immutability**: Always frozen

### Options

**Suffix**: `*Options`
**Purpose**: Optional parameter bundles
**Examples**: `CompileOptions`, `OtelBootstrapOptions`, `ResourceOptions`
**Immutability**: Typically frozen dataclasses

### Profile

**Suffix**: `*Profile`
**Purpose**: Preset configurations
**Examples**: `DataFusionRuntimeProfile`, `DiskCacheProfile`, `SchemaHardeningProfile`
**Immutability**: May be mutable for runtime state

## See Also

- **DataFusion Engine Core** (`docs/architecture/datafusion_engine_core.md`): DataFusion integration architecture
- **Observability** (`docs/architecture/observability.md`): OpenTelemetry instrumentation and metrics
- **Storage and Incremental Processing** (`docs/architecture/part_v_storage_and_incremental.md`): Delta Lake integration
- **CLAUDE.md**: Project conventions and development guidelines
