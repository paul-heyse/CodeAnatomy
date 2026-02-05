# Configuration & Utilities

## Purpose

This document provides a comprehensive reference for CodeAnatomy's configuration system and cross-cutting utilities. It covers runtime profiles, environment variables, determinism tiers, hashing, registry protocols, and utility modules.

## Key Concepts

- **Hierarchical Configuration** - Environment variables → Policy objects → Settings → Specs
- **Naming Conventions** - Policy, Settings, Config, Spec, Options suffixes have specific meanings
- **Determinism Tiers** - CANONICAL, STABLE_SET, BEST_EFFORT control reproducibility
- **Explicit Utilities** - Type-safe helpers with no implicit behavior

---

## Configuration Philosophy

CodeAnatomy uses a hierarchical configuration model:

| Type | Suffix | Purpose | Example |
|------|--------|---------|---------|
| **Policy** | `*Policy` | Runtime behavior control | `DeltaWritePolicy` |
| **Settings** | `*Settings` | Initialization parameters | `DiskCacheSettings` |
| **Config** | `*Config` | Request/command parameters | `OtelConfig` |
| **Spec** | `*Spec` | Declarative schema definitions | `TableSpec` |
| **Options** | `*Options` | Optional parameter bundles | `CompileOptions` |
| **Profile** | `*Profile` | Preset configurations | `DataFusionRuntimeProfile` |

---

## Runtime Profiles

### DataFusionRuntimeProfile

**File:** `src/datafusion_engine/session/runtime.py`

Central configuration for DataFusion execution sessions:

| Category | Fields |
|----------|--------|
| **Execution** | `target_partitions`, `batch_size`, `memory_limit_bytes`, `spill_dir` |
| **Catalog** | `default_catalog`, `default_schema`, `registry_catalogs` |
| **External Tables** | `ast_catalog_location`, `extract_dataset_locations` |
| **Feature Toggles** | `enable_information_schema`, `cache_enabled` |
| **Hooks** | `function_factory_hook`, `expr_planner_hook` |

**Profile Presets:**

| Preset | Memory | Concurrency | Use Case |
|--------|--------|-------------|----------|
| `default` | 8 GiB | 8 | General-purpose |
| `dev` | 4 GiB | 4 | Development |
| `prod` | 16 GiB | 16 | Production |
| `cst_autoload` | 8 GiB | 8 | LibCST extraction |
| `symtable` | 4 GiB | 4 | Symtable extraction |

### RuntimeProfileSpec

Combines DataFusion runtime with determinism and telemetry:

```python
@dataclass(frozen=True)
class RuntimeProfileSpec:
    name: str
    datafusion: DataFusionRuntimeProfile
    determinism_tier: DeterminismTier
    tracker_config: HamiltonTrackerConfig | None
    hamilton_telemetry: HamiltonTelemetryProfile | None
```

---

## Determinism Tiers

**File:** `src/core_types.py`

Three tiers controlling reproducibility guarantees:

| Tier | Value | Guarantees |
|------|-------|------------|
| **CANONICAL** | `"canonical"` | Deterministic fingerprints, stable ordering, reproducible aggregation |
| **STABLE_SET** | `"stable_set"` | Deterministic record sets, stable aggregation |
| **BEST_EFFORT** | `"best_effort"` | Correct results, no ordering guarantees |

**Aliases:**
- `FAST` → `BEST_EFFORT`
- `STABLE` → `STABLE_SET`

**Environment Override Precedence:**
1. `CODEANATOMY_FORCE_TIER2=true` → Always use `CANONICAL`
2. `CODEANATOMY_DETERMINISM_TIER=<tier>` → Explicit selection
3. Default: `BEST_EFFORT`

---

## Environment Variables

### Core Runtime

| Variable | Default | Description |
|----------|---------|-------------|
| `CODEANATOMY_RUNTIME_PROFILE` | `"default"` | Runtime profile name |
| `CODEANATOMY_DETERMINISM_TIER` | `"best_effort"` | Determinism tier |
| `CODEANATOMY_ENV` | - | Environment identifier (dev/prod/ci) |
| `CODEANATOMY_STATE_DIR` | - | Incremental state directory |

### DataFusion

| Variable | Default | Description |
|----------|---------|-------------|
| `CODEANATOMY_DATAFUSION_CATALOG_LOCATION` | - | Catalog auto-load location |
| `CODEANATOMY_DF_PLUGIN_PATH` | auto-detected | Path to plugin library |
| `CODEANATOMY_CACHE_OUTPUT_ROOT` | `/tmp/datafusion_cache` | Delta-backed cache root |

### Cache Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `CODEANATOMY_DISKCACHE_DIR` | `~/.cache/codeanatomy/diskcache` | DiskCache root |

**Cache Kinds:**

| Kind | Size Limit | TTL | Shards | Purpose |
|------|------------|-----|--------|---------|
| `plan` | 512 MiB | None | 1 | Plan bundles, Substrait |
| `extract` | 8 GiB | 24h | 8 | Extraction artifacts |
| `schema` | 256 MiB | 5min | 1 | Schema introspection |
| `repo_scan` | 512 MiB | 30min | 1 | Repository scan metadata |
| `runtime` | 256 MiB | 24h | 1 | Runtime artifacts |

### Hamilton

| Variable | Default | Description |
|----------|---------|-------------|
| `CODEANATOMY_HAMILTON_PROJECT_ID` | - | Hamilton project ID |
| `CODEANATOMY_HAMILTON_TELEMETRY_PROFILE` | env-based | Profile (dev/prod/ci) |
| `CODEANATOMY_HAMILTON_TRACKER_ENABLED` | profile | Enable Hamilton UI tracker |
| `CODEANATOMY_HAMILTON_CAPTURE_DATA_STATISTICS` | profile | Capture data statistics |
| `CODEANATOMY_HAMILTON_CACHE_PATH` | - | Hamilton cache directory |

**Telemetry Profile Defaults:**

| Profile | Tracker | Statistics | Max List | Max Dict |
|---------|---------|------------|----------|----------|
| `prod` | `true` | `false` | 20 | 50 |
| `ci` | `false` | `false` | 5 | 10 |
| `dev` | `true` | `true` | 200 | 200 |

### Git Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `CODEANATOMY_GIT_USERNAME` | - | Git remote username |
| `CODEANATOMY_GIT_PASSWORD` | - | Git remote password |
| `CODEANATOMY_GIT_SSH_KEY` | - | SSH private key path |

### Incremental Processing

| Variable | Default | Description |
|----------|---------|-------------|
| `CODEANATOMY_GIT_HEAD_REF` | - | Git HEAD ref |
| `CODEANATOMY_GIT_BASE_REF` | - | Git base ref |
| `CODEANATOMY_GIT_CHANGED_ONLY` | - | Process only changed files |

---

## Delta Lake Configuration

### DeltaWritePolicy

```python
@dataclass(frozen=True)
class DeltaWritePolicy:
    target_file_size: int | None = 96 * 1024 * 1024  # 96 MiB
    partition_by: tuple[str, ...] = ()
    zorder_by: tuple[str, ...] = ()
    stats_policy: Literal["off", "explicit", "auto"] = "auto"
    stats_max_columns: int = 32
    enable_features: tuple[str, ...] = ()
```

**Delta Features:**
- `"change_data_feed"` - Enable CDF
- `"deletion_vectors"` - Enable deletion vectors
- `"column_mapping"` - Enable column mapping

### Dataset Resolution

**File:** `src/datafusion_engine/dataset/registry.py`

Override precedence for each field:
1. `DatasetLocation.overrides.<field>`
2. `DatasetSpec.<field>` (when present)
3. Derived defaults

**Override-capable fields:** `datafusion_scan`, `delta_scan`, `delta_cdf_policy`, `delta_write_policy`, `delta_schema_policy`, `table_spec`

---

## Fingerprinting

### Principles

- Include explicit `version` in payloads
- Compose hashes from stable sub-components
- Keep payloads JSON-compatible with sorted keys
- Use 32-hex-character (128-bit) truncation

### Composite Fingerprints

```python
@dataclass(frozen=True)
class CompositeFingerprint:
    version: int
    components: tuple[tuple[str, str], ...]  # (name, hash) pairs
```

### Cache Key Compatibility

When changing cache key construction:
1. Add new `version` value
2. Keep old `version` stable
3. Provide dual-read compatibility
4. Update documentation and tests

---

## Cross-Cutting Utilities

### Module Map

```
src/utils/
├── hashing.py           # Deterministic hashing
├── registry_protocol.py # Registry abstractions
├── env_utils.py         # Environment parsing
├── storage_options.py   # Storage config normalization
├── validation.py        # Type validation helpers
├── value_coercion.py    # Type coercion
├── uuid_factory.py      # UUIDv7 generation
└── file_io.py           # File reading utilities
```

### Hashing Utilities

**File:** `src/utils/hashing.py`

| Function | Algorithm | Use Case |
|----------|-----------|----------|
| `hash64_from_text(value)` | BLAKE2b-64 | Arrow Int64 columns, DB keys |
| `hash128_from_text(value)` | BLAKE2b-128 | Compact identifiers |
| `hash_sha256_hex(payload)` | SHA-256 | Fingerprints, verification |
| `hash_msgpack_canonical(payload)` | msgpack + SHA-256 | Configuration hashing |
| `hash_json_canonical(payload)` | JSON + SHA-256 | External compatibility |
| `hash_storage_options(storage, log)` | JSON + SHA-256 | Delta Lake credentials |
| `hash_file_sha256(path)` | SHA-256 | File content hashing |

**Example:**

```python
from utils.hashing import hash_msgpack_canonical, config_fingerprint

# Plan fingerprinting
plan_hash = hash_msgpack_canonical({
    "substrait": substrait_bytes,
    "env_hash": environment_hash,
    "udfs": tuple(required_udfs),
})

# Configuration fingerprint
settings_hash = config_fingerprint({
    "target_partitions": 4,
    "batch_size": 8192,
})
```

### Registry Protocol

**File:** `src/utils/registry_protocol.py`

**Protocol:**

```python
@runtime_checkable
class Registry(Protocol[K, V]):
    def register(self, key: K, value: V) -> None: ...
    def get(self, key: K) -> V | None: ...
    def __contains__(self, key: K) -> bool: ...
    def __iter__(self) -> Iterator[K]: ...
    def __len__(self) -> int: ...
```

**Implementations:**

| Class | Storage | Lookup | Use Case |
|-------|---------|--------|----------|
| `MutableRegistry[K, V]` | dict | O(1) | Session-scoped caches |
| `ImmutableRegistry[K, V]` | tuple | O(n) | Module-level singletons |

**Usage:**

```python
from utils.registry_protocol import ImmutableRegistry, MutableRegistry

# Module-level singleton
BUILTIN_UDFS = ImmutableRegistry.from_dict({
    "hash64": Hash64UDF(),
    "hash128": Hash128UDF(),
})

# Session-scoped cache
cache = MutableRegistry[str, pa.Schema]()
cache.register("nodes", schema)
```

### Environment Parsing

**File:** `src/utils/env_utils.py`

| Function | Returns | Description |
|----------|---------|-------------|
| `env_value(name)` | `str | None` | Raw value or None |
| `env_text(name, default)` | `str` | With default and normalization |
| `env_bool(name, default)` | `bool | None` | Flexible boolean (1/true/yes) |
| `env_bool_strict(name, default)` | `bool` | Strict (true/false only) |
| `env_int(name, default)` | `int | None` | Integer with error logging |
| `env_float(name, default)` | `float | None` | Float with error logging |

**Example:**

```python
from utils.env_utils import env_bool, env_int

ENABLE_CACHE = env_bool("CODEANATOMY_ENABLE_CACHE", default=True)
TARGET_PARTITIONS = env_int("DATAFUSION_TARGET_PARTITIONS", default=4)
```

### Storage Options

**File:** `src/utils/storage_options.py`

```python
from utils.storage_options import normalize_storage_options, merged_storage_options

# Normalize with fallback
storage, log_storage = normalize_storage_options(
    storage_options={"aws_access_key_id": "..."},
    log_storage_options=None,
    fallback_log_to_storage=True,
)

# Merge for unified operations
combined = merged_storage_options(storage, log_storage)
```

### Validation

**File:** `src/utils/validation.py`

| Function | Purpose |
|----------|---------|
| `ensure_mapping(value, label)` | Validate Mapping protocol |
| `ensure_sequence(value, label, item_type)` | Validate Sequence with items |
| `ensure_callable(value, label)` | Validate callable |
| `ensure_table(value, label)` | Convert to PyArrow Table |
| `validate_required_items(required, available)` | Check all items present |

**Example:**

```python
from utils.validation import ensure_mapping, ensure_callable

def register_view(name: str, builder: object, options: object) -> None:
    validated_builder = ensure_callable(builder, label="builder")
    validated_options = ensure_mapping(options or {}, label="options")
```

### UUID Factory

**File:** `src/utils/uuid_factory.py`

Time-ordered UUIDv7 generation:

| Function | Returns | Description |
|----------|---------|-------------|
| `uuid7()` | `uuid.UUID` | UUIDv7 instance |
| `uuid7_str()` | `str` | With hyphens (36 chars) |
| `uuid7_hex()` | `str` | Without hyphens (32 chars) |
| `uuid7_suffix(length)` | `str` | Short random suffix |
| `secure_token_hex(nbytes)` | `str` | Cryptographic token |

**UUIDv7 Benefits:**
- Time-ordered (sortable)
- Efficient database indexing
- Embedded timestamp for debugging

**Example:**

```python
from utils import uuid7, uuid7_hex, secure_token_hex

run_id = uuid7()          # Sortable UUID
cache_key = uuid7_hex()   # 32-char hex
api_key = secure_token_hex(32)  # Cryptographic token
```

### Value Coercion

**File:** `src/utils/value_coercion.py`

**Tolerant (returns None on failure):**

| Function | Accepts |
|----------|---------|
| `coerce_int(value)` | str, float, int |
| `coerce_float(value)` | str, int, float |
| `coerce_bool(value)` | str (true/1/yes/on/false/0/no/off) |
| `coerce_str(value)` | Any (except None) |
| `coerce_str_list(value)` | str, Sequence |

**Strict (raises CoercionError):**

| Function | Purpose |
|----------|---------|
| `raise_for_int(value, context)` | Strict integer |
| `raise_for_float(value, context)` | Strict float |
| `raise_for_bool(value, context)` | Strict boolean |
| `raise_for_str(value, context)` | Strict string |

### File I/O

**File:** `src/utils/file_io.py`

| Function | Purpose |
|----------|---------|
| `read_text(path)` | UTF-8 text file |
| `read_toml(path)` | Parse TOML file |
| `read_json(path)` | Parse JSON file |
| `read_pyproject_toml(path)` | Read pyproject.toml |

---

## Configuration Examples

### Production Profile

```bash
export CODEANATOMY_RUNTIME_PROFILE=prod
export CODEANATOMY_DETERMINISM_TIER=canonical
export CODEANATOMY_DISKCACHE_DIR=/data/cache
export CODEANATOMY_DATAFUSION_CATALOG_LOCATION=/data/catalogs
```

### Delta Lake Write

```python
from storage.deltalake.config import DeltaWritePolicy

write_policy = DeltaWritePolicy(
    target_file_size=128 * 1024 * 1024,
    partition_by=("repo_id", "file_path"),
    zorder_by=("node_id",),
    stats_policy="auto",
    enable_features=("change_data_feed", "column_mapping"),
)
```

### Hamilton Tracker

```bash
export CODEANATOMY_HAMILTON_PROJECT_ID=42
export CODEANATOMY_HAMILTON_DAG_NAME=cpg_builder
export CODEANATOMY_HAMILTON_TELEMETRY_PROFILE=prod
export CODEANATOMY_HAMILTON_TRACKER_ENABLED=true
```

### DiskCache Customization

```python
from cache.diskcache_factory import DiskCacheProfile, DiskCacheSettings

profile = DiskCacheProfile(
    root=Path("/data/cache"),
    overrides={
        "extract": DiskCacheSettings(
            size_limit_bytes=16 * 1024**3,
            shards=16,
        ),
    },
)
```

---

## Cross-References

- **[08_observability.md](08_observability.md)** - OpenTelemetry configuration
- **[07_storage_and_incremental.md](07_storage_and_incremental.md)** - Delta Lake settings
- **[04_datafusion_integration.md](04_datafusion_integration.md)** - DataFusion profiles

**Source Files:**
- `src/utils/` - Cross-cutting utilities
- `src/datafusion_engine/session/runtime.py` - Runtime profiles
- `src/core_types.py` - Determinism tiers
- `src/cache/diskcache_factory.py` - Cache settings
