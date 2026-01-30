# Cross-Cutting Utilities Reference

## Overview

The `src/utils/` module provides foundational utilities used throughout CodeAnatomy. These utilities consolidate common patterns for hashing, environment configuration, registry management, validation, UUID generation, and file I/O. By centralizing these mechanisms, the codebase avoids duplication and ensures consistent behavior across subsystems.

**Design Philosophy**: Each utility module serves a single purpose and exposes explicit, semantics-preserving interfaces. Utilities avoid implicit state, magic defaults, or coupling to domain logic. They provide building blocks rather than frameworks.

**Core Modules**:
- `hashing.py` - Deterministic hashing with stable serialization
- `registry_protocol.py` - Registry abstractions and base implementations
- `env_utils.py` - Environment variable parsing with type coercion
- `storage_options.py` - Storage configuration normalization
- `validation.py` - Type and collection validation helpers
- `uuid_factory.py` - Time-ordered UUID generation (UUIDv7)
- `file_io.py` - Consistent file reading with encoding handling

**Usage Pattern**: Import utilities directly from `src/utils/` submodules. The top-level `__init__.py` only exports UUID factory functions for convenience. All other utilities require explicit imports to maintain clarity.

---

## Hashing Utilities

**File**: `/home/paul/CodeAnatomy/src/utils/hashing.py` (lines 1-289)

### Purpose

Provide deterministic, semantics-preserving hash functions for configuration fingerprinting, cache keys, and stable identifiers. All hash functions guarantee consistent output for equivalent inputs across Python versions and platforms.

### Design Principles

**Explicit Serialization**: Every hash function explicitly chooses its serialization format (msgpack, JSON, or raw bytes). Callers know exactly how their data will be encoded before hashing.

**Canonical Ordering**: Hash functions with `_canonical` suffixes enforce deterministic key ordering for dictionaries and mappings. This ensures `{"a": 1, "b": 2}` and `{"b": 2, "a": 1}` produce identical hashes.

**Algorithm Selection**: SHA-256 is used for fingerprints requiring cryptographic collision resistance (plan fingerprints, storage option hashes). BLAKE2b is used for shorter identifiers where performance matters (hash64/hash128 from text).

**No Implicit Conversion**: Functions accept typed inputs (bytes, str, Mapping) and fail fast on type mismatches rather than attempting automatic conversion. This prevents silent serialization bugs.

### Core Hash Functions

#### Text-Based Hashing

**`hash64_from_text(value: str) -> int`** (lines 37-52)
- Returns a deterministic signed 64-bit integer from a string
- Uses BLAKE2b with 8-byte digest, masked to positive range
- Suitable for Arrow Int64 columns and database keys
- Example: `hash64_from_text("module.function")` → `7234567890123456`

**`hash128_from_text(value: str) -> str`** (lines 55-68)
- Returns a 128-bit hex string from a string (32 characters)
- Uses BLAKE2b with 16-byte digest
- Suitable for compact identifiers with low collision probability
- Example: `hash128_from_text("file_path.py")` → `"a1b2c3d4e5f6..."`

**`hash_sha256_hex(payload: bytes, *, length: int | None = None) -> str`** (lines 18-34)
- Base SHA-256 hashing function used by all higher-level hashes
- Optional truncation via `length` parameter
- Returns full 64-character hex digest when `length` is None
- Example: `hash_sha256_hex(b"data", length=16)` → `"3a6eb079f66f4..."`

#### Msgpack-Based Hashing

**`hash_msgpack_default(payload: object) -> str`** (lines 76-89)
- Encodes payload using `msgspec.msgpack.encode()` (insertion-order preserving)
- Returns SHA-256 hex digest
- Fast for structured data where key ordering is already stable
- Suitable for: tuples, lists, dataclasses with frozen=True

**`hash_msgpack_canonical(payload: object) -> str`** (lines 92-105)
- Encodes payload using `MSGPACK_ENCODER` with deterministic key sorting
- Returns SHA-256 hex digest
- Required for dictionaries and arbitrary mappings where key order varies
- Used by `hash_settings()` for configuration hashing

**Design Note**: Msgpack hashing uses the `MSGPACK_ENCODER` from `serde_msgspec.py`, which configures msgspec with `enc_hook` for custom type encoding and deterministic float serialization.

#### JSON-Based Hashing

**`hash_json_default(payload: object, *, str_keys: bool = False) -> str`** (lines 128-143)
- Encodes via `JSON_ENCODER` with `to_builtins()` normalization
- Optional key stringification via `str_keys=True`
- Preserves insertion order (Python 3.7+ dict ordering)
- Used when JSON compatibility is required for external systems

**`hash_json_canonical(payload: object, *, str_keys: bool = False) -> str`** (lines 146-161)
- Encodes via `JSON_ENCODER_SORTED` for deterministic key ordering
- Used by `config_fingerprint()` for stable configuration hashes
- Guarantees identical output regardless of dictionary construction order

**`hash_json_stdlib(payload: object, *, sort_keys: bool = False) -> str`** (lines 164-180)
- Uses standard library `json.dumps()` instead of msgspec
- Provided for compatibility with legacy code expecting stdlib behavior
- Slower than msgspec-based functions but more conservative

**Internal Mechanism** (lines 113-125): The `_json_hash()` function consolidates JSON encoding logic:
1. Convert payload to builtins via `to_builtins()` (handles Path, datetime, etc.)
2. Encode into a `bytearray` buffer using msgspec encoder
3. Hash the buffer contents with SHA-256
4. Return hex digest

#### Configuration Hashing

**`hash_settings(settings: Mapping[str, str]) -> str`** (lines 188-202)
- Converts mapping to sorted tuple of items
- Uses `hash_msgpack_canonical()` for deterministic output
- Primary use: hashing session configuration for plan fingerprints
- Example: `hash_settings({"batch_size": "8192", "target_partitions": "4"})`

**`hash_storage_options(storage_options, log_storage_options) -> str | None`** (lines 205-229)
- Combines storage and log storage options into a single fingerprint
- Returns `None` when both inputs are None/empty (no credentials)
- Encodes as JSON dict with `storage` and `log_storage` keys
- Uses stdlib `json.dumps(sort_keys=True)` for external compatibility
- Critical for Delta Lake write idempotency and cache invalidation

**Design Rationale**: Storage options hashing uses stdlib JSON instead of msgspec to match Delta Lake's internal credential hashing. This ensures cache hits when storage options are semantically identical.

#### File Content Hashing

**`hash_file_sha256(path: Path, chunk_size: int = 1024 * 1024) -> str`** (lines 237-256)
- Streams file contents in chunks to support large files
- Returns SHA-256 hex digest of complete file
- Default chunk size: 1MB (optimal for SSD read patterns)
- Used for DDL file fingerprinting and artifact versioning

**`config_fingerprint(payload: Mapping[str, object]) -> str`** (lines 259-272)
- High-level configuration fingerprinting function
- Wraps `hash_json_canonical()` with `str_keys=True`
- Used by runtime profiles and execution facades
- Ensures consistent fingerprints across configuration types

### Usage Patterns

**Plan Fingerprinting**:
```python
from utils.hashing import hash_msgpack_canonical, hash_storage_options

# Hash Substrait bytes + environment
plan_hash = hash_msgpack_canonical({
    "substrait": substrait_bytes,
    "env_hash": environment_hash,
    "udfs": tuple(required_udfs),
})

# Hash storage credentials
storage_hash = hash_storage_options(
    storage_options={"aws_access_key_id": "..."},
    log_storage_options={"aws_region": "us-west-2"},
)
```

**Cache Key Generation**:
```python
from utils.hashing import hash128_from_text, hash64_from_text

# Compact cache keys
cache_key = hash128_from_text(f"{dataset_name}:{version}")

# Int64 foreign keys for Arrow tables
entity_id = hash64_from_text(qualified_name)
```

**Configuration Snapshots**:
```python
from utils.hashing import config_fingerprint

# Session configuration fingerprinting
settings_hash = config_fingerprint({
    "target_partitions": 4,
    "batch_size": 8192,
    "delta_protocol_mode": "v2",
})
```

### Extension Points

**Custom Type Encoding**: To hash custom types, extend the `enc_hook` in `serde_msgspec.py`:
```python
# In serde_msgspec.py
def enc_hook(obj: object) -> object:
    if isinstance(obj, MyCustomType):
        return {"__type__": "MyCustomType", "value": obj.serialize()}
    # ... existing hooks
```

**Hash Length Optimization**: For identifier columns with cardinality constraints, truncate hashes:
```python
# 16-character prefix provides 64-bit uniqueness
short_hash = hash_sha256_hex(payload, length=16)
```

---

## Registry Protocol

**File**: `/home/paul/CodeAnatomy/src/utils/registry_protocol.py` (lines 1-224)

### Purpose

Provide standardized registry abstractions for key-value storage with minimal coupling. The protocol enables polymorphism across registry implementations while base classes (`MutableRegistry`, `ImmutableRegistry`) handle common patterns.

### Design Principles

**Protocol Over Inheritance**: The `Registry[K, V]` protocol (lines 20-42) defines the interface without forcing inheritance. Rich domain registries (e.g., `ProviderRegistry`, `SchemaRegistry`) can implement the protocol without inheriting base classes.

**Immutability for Safety**: `ImmutableRegistry` (lines 143-217) uses frozen dataclasses and tuple storage to prevent accidental mutation. This is critical for module-level registries that serve as singletons.

**Composition Over Base Classes**: Registries with complex behavior (validation, caching, computed lookups) should use composition rather than extending `MutableRegistry`. The base class is intentionally minimal to avoid method pollution.

### Registry Protocol

**`Registry[K, V]`** (lines 20-42)

Runtime-checkable protocol defining the core registry interface:

```python
@runtime_checkable
class Registry(Protocol[K, V]):
    def register(self, key: K, value: V) -> None: ...
    def get(self, key: K) -> V | None: ...
    def __contains__(self, key: K) -> bool: ...
    def __iter__(self) -> Iterator[K]: ...
    def __len__(self) -> int: ...
```

**Interface Contract**:
- `register()` - Add a key/value pair (behavior on duplicate keys is implementation-defined)
- `get()` - Retrieve value by key, return None when missing
- `__contains__()` - Check key existence (enables `key in registry`)
- `__iter__()` - Iterate over keys (enables `for key in registry`)
- `__len__()` - Return count of registered entries

**Protocol Advantages**:
1. Duck typing - any class implementing these methods satisfies the protocol
2. Runtime checking - `isinstance(obj, Registry)` works with `runtime_checkable`
3. Type safety - `Registry[str, TableSpec]` provides full generic type checking
4. No inheritance tax - implementations avoid method resolution order complexity

### MutableRegistry Implementation

**`MutableRegistry[K, V]`** (lines 44-141)

Standard mutable registry using dictionary storage:

```python
@dataclass
class MutableRegistry[K, V]:
    _entries: dict[K, V] = field(default_factory=dict)
```

**Key Methods**:

**`register(key, value, *, overwrite: bool = False)`** (lines 50-70)
- Registers a key/value pair
- Default behavior: raises `ValueError` on duplicate keys
- Set `overwrite=True` to replace existing entries
- Example:
  ```python
  registry = MutableRegistry[str, int]()
  registry.register("count", 42)
  registry.register("count", 99, overwrite=True)  # Replaces 42
  ```

**`get(key) -> V | None`** (lines 72-85)
- Retrieves value by key, returns None when missing
- Type-safe return value (V | None)
- Example:
  ```python
  value = registry.get("count")  # Returns 42 or None
  ```

**`snapshot() -> Mapping[K, V]`** (lines 132-140)
- Returns an immutable snapshot of current state
- Creates a dict copy, preventing external mutation
- Useful for checkpointing registry state during execution

**`items() -> Iterator[tuple[K, V]]`** (lines 122-130)
- Iterates over key/value pairs
- Enables dict-like iteration patterns

**Design Notes**:
- Internal storage (`_entries`) uses leading underscore to discourage direct access
- No public methods for removal - registries are append-only by default
- Iteration order matches dict insertion order (Python 3.7+ guarantee)

### ImmutableRegistry Implementation

**`ImmutableRegistry[K, V]`** (lines 143-217)

Frozen registry built from a sequence of entries:

```python
@dataclass(frozen=True)
class ImmutableRegistry[K, V]:
    _entries: tuple[tuple[K, V], ...]
```

**Construction**:

**`from_dict(data: Mapping[K, V]) -> ImmutableRegistry[K, V]`** (lines 202-216)
- Class method for creating immutable registries from mappings
- Converts mapping to tuple of tuples for immutable storage
- Example:
  ```python
  registry = ImmutableRegistry.from_dict({
      "builtin.sum": SumUDF,
      "builtin.count": CountUDF,
  })
  ```

**Key Methods**:

**`get(key) -> V | None`** (lines 149-165)
- Linear search through tuple storage
- Returns None when key not found
- Trade-off: Immutability vs O(n) lookup performance
- Acceptable for small registries (< 100 entries)

**`__contains__(key) -> bool`** (lines 167-180)
- Linear search using generator expression
- Optimized with `any()` for early termination
- Example: `"builtin.sum" in registry`

**Performance Characteristics**:
- Construction: O(n) conversion from dict to tuple
- Lookup: O(n) linear search through entries
- Memory: Compact storage (tuple vs dict overhead)
- Thread-safety: Guaranteed by immutability (no locks needed)

**Design Rationale**: Immutable registries prioritize safety over lookup performance. Use when:
1. Registry is constructed once at module level
2. Mutation would cause undefined behavior
3. Registry size is small (< 100 entries)
4. Thread-safe access is required without locking

### Usage Patterns

**Module-Level Builtin Registry**:
```python
# src/datafusion_engine/udf_builtin.py
BUILTIN_UDFS = ImmutableRegistry.from_dict({
    "hash64": Hash64UDF(),
    "hash128": Hash128UDF(),
    # ... additional builtins
})

def get_builtin_udf(name: str) -> UDFSpec | None:
    return BUILTIN_UDFS.get(name)
```

**Dynamic Registry with Validation**:
```python
# src/datafusion_engine/schema_registry.py
class SchemaRegistry:
    def __init__(self) -> None:
        self._schemas = MutableRegistry[str, pa.Schema]()

    def register_schema(self, name: str, schema: pa.Schema) -> None:
        # Custom validation before registration
        if not isinstance(schema, pa.Schema):
            raise TypeError(f"Expected Schema, got {type(schema)}")
        self._schemas.register(name, schema, overwrite=False)

    def get_schema(self, name: str) -> pa.Schema | None:
        return self._schemas.get(name)
```

**Registry Composition (Preferred for Rich Behavior)**:
```python
# Avoid inheriting from MutableRegistry
class ProviderRegistry:
    def __init__(self) -> None:
        self._providers = MutableRegistry[str, TableProvider]()
        self._aliases: dict[str, str] = {}

    def register_provider(self, name: str, provider: TableProvider) -> None:
        # Rich registration logic with alias support
        self._providers.register(name, provider)
        if hasattr(provider, "aliases"):
            for alias in provider.aliases:
                self._aliases[alias] = name

    def resolve(self, name: str) -> TableProvider | None:
        # Computed lookup with alias resolution
        canonical = self._aliases.get(name, name)
        return self._providers.get(canonical)
```

### When to Use Each Pattern

**`Registry` Protocol**: Use for type hints and polymorphic functions
```python
def validate_registry(registry: Registry[str, int]) -> None:
    assert "required_key" in registry
```

**`MutableRegistry`**: Use for simple key/value storage without complex logic
- Session-scoped caches
- Temporary name mappings
- Builder state accumulation

**`ImmutableRegistry`**: Use for static, module-level registries
- Builtin UDF catalogs
- Default configuration mappings
- Constant lookup tables

**Custom Implementation**: Use when registry needs rich behavior
- Provider registries with caching and lazy loading
- Schema registries with validation and compatibility checking
- Function registries with signature matching and overload resolution

---

## Environment Parsing Utilities

**File**: `/home/paul/CodeAnatomy/src/utils/env_utils.py` (lines 1-287)

### Purpose

Provide type-safe environment variable parsing with explicit defaults and error handling. All functions follow consistent conventions for missing values, invalid inputs, and logging.

### Design Principles

**Explicit Defaults**: Every function requires a `default` parameter (or returns None). No implicit "zero" or "false" defaults.

**Type Preservation**: Return types match the requested type (bool, int, float, str). No automatic string coercion.

**Fail-Safe Parsing**: Invalid values log warnings and return defaults rather than raising exceptions. This prevents deployment failures from configuration typos.

**Overload Precision**: Functions use `@overload` decorators to provide exact return types based on default parameter presence.

### Core Parsing Functions

#### String Parsing

**`env_value(name: str) -> str | None`** (lines 22-39)
- Returns stripped environment variable value or None
- Treats empty strings as None (normalization)
- Base function used by other parsers
- Example:
  ```python
  token = env_value("API_TOKEN")  # None or non-empty string
  ```

**`env_text(name, *, default, strip, allow_empty) -> str | None`** (lines 42-73)
- Returns environment variable with optional normalization
- Parameters:
  - `default`: Value when variable is unset or empty
  - `strip`: Remove leading/trailing whitespace (default: True)
  - `allow_empty`: Return empty strings instead of default (default: False)
- Example:
  ```python
  region = env_text("AWS_REGION", default="us-west-2")
  mode = env_text("EXEC_MODE", default="local", strip=True)
  ```

#### Boolean Parsing

**`env_bool(name, *, default, on_invalid, log_invalid) -> bool | None`** (lines 109-148)

Flexible boolean parser with overloaded signatures:

**Overload 1**: No default → returns `bool | None`
```python
enabled = env_bool("FEATURE_FLAG")  # None, True, or False
```

**Overload 2**: Non-None default → returns `bool`
```python
debug = env_bool("DEBUG", default=False)  # Always bool
```

**Overload 3**: Explicit `on_invalid` behavior → returns `bool` or `bool | None`
```python
flag = env_bool("FLAG", default=False, on_invalid="false", log_invalid=True)
```

**Recognized Values**:
- True: `"1"`, `"true"`, `"yes"`, `"y"` (case-insensitive)
- False: `"0"`, `"false"`, `"no"`, `"n"` (case-insensitive)

**Invalid Value Handling** (controlled by `on_invalid` parameter):
- `"default"`: Return the default value (default behavior)
- `"none"`: Return None
- `"false"`: Return False

**Example Usage**:
```python
# Feature flags with safe defaults
enable_cache = env_bool("ENABLE_CACHE", default=True)
debug_mode = env_bool("DEBUG", default=False, log_invalid=True)

# Optional configuration (None when unset)
override = env_bool("OVERRIDE_SETTING")
if override is not None:
    apply_override(override)
```

**`env_bool_strict(name, *, default, log_invalid) -> bool`** (lines 156-185)
- Strict boolean parser accepting only `"true"` or `"false"`
- Rejects numeric values (`"1"`, `"0"`) and shortcuts (`"yes"`, `"no"`)
- Returns default for invalid or missing values
- Use when environment variable must be explicitly set by humans
- Example:
  ```python
  production = env_bool_strict("PRODUCTION", default=False)
  # Only accepts: "true" or "false" (case-insensitive)
  ```

#### Integer Parsing

**`env_int(name, *, default) -> int | None`** (lines 205-227)

Overloaded integer parser with error logging:

**Overload 1**: No default → returns `int | None`
```python
port = env_int("PORT")  # None or int
```

**Overload 2**: Non-None default → returns `int`
```python
workers = env_int("WORKERS", default=4)  # Always int
```

**Error Handling**:
- Invalid integers log warning and return default
- Whitespace is stripped before parsing
- Empty strings return default

**Example Usage**:
```python
# Server configuration
port = env_int("PORT", default=8080)
max_connections = env_int("MAX_CONN", default=100)

# Optional limits
row_limit = env_int("ROW_LIMIT")  # None or int
if row_limit is not None:
    apply_limit(row_limit)
```

#### Float Parsing

**`env_float(name, *, default) -> float | None`** (lines 243-265)

Overloaded float parser mirroring `env_int()` behavior:

**Overload 1**: No default → returns `float | None`
```python
threshold = env_float("THRESHOLD")
```

**Overload 2**: Non-None default → returns `float`
```python
timeout = env_float("TIMEOUT", default=30.0)
```

**Example Usage**:
```python
# Performance tuning
spill_threshold = env_float("SPILL_THRESHOLD", default=0.75)
sample_rate = env_float("SAMPLE_RATE", default=0.1)
```

#### Legacy Boolean Helper

**`env_truthy(value: str | None) -> bool`** (lines 268-276)
- Returns True only when value is literally `"true"` (case-insensitive)
- Accepts None as input (returns False)
- Legacy compatibility function - prefer `env_bool()` for new code
- Example:
  ```python
  if env_truthy(os.environ.get("LEGACY_FLAG")):
      enable_legacy_mode()
  ```

### Usage Patterns

**Feature Flag Management**:
```python
from utils.env_utils import env_bool

# Default-enabled features
ENABLE_CACHE = env_bool("CODEANATOMY_ENABLE_CACHE", default=True)
ENABLE_DELTA_CDF = env_bool("CODEANATOMY_ENABLE_CDF", default=True)

# Default-disabled features (experimental)
ENABLE_INCREMENTAL = env_bool("CODEANATOMY_INCREMENTAL", default=False)
```

**Configuration with Validation**:
```python
from utils.env_utils import env_int, env_float

# Execution settings
TARGET_PARTITIONS = env_int("DATAFUSION_TARGET_PARTITIONS", default=4)
BATCH_SIZE = env_int("DATAFUSION_BATCH_SIZE", default=8192)
TIMEOUT_SECONDS = env_float("QUERY_TIMEOUT", default=300.0)

# Validate parsed values
if TARGET_PARTITIONS < 1:
    raise ValueError("TARGET_PARTITIONS must be positive")
```

**Optional Override Pattern**:
```python
from utils.env_utils import env_text, env_int

# Start with default config
config = DEFAULT_CONFIG.copy()

# Apply environment overrides when present
if override_mode := env_text("EXECUTION_MODE"):
    config["mode"] = override_mode

if override_workers := env_int("WORKERS"):
    config["workers"] = override_workers
```

**Strict Production Guards**:
```python
from utils.env_utils import env_bool_strict

# Production flag must be explicitly set to "true"
IS_PRODUCTION = env_bool_strict("PRODUCTION", default=False)

if IS_PRODUCTION:
    # Strict validation - no shortcuts like "1" or "yes"
    enable_production_mode()
```

### Extension Points

**Custom Parsers**: Build domain-specific parsers using base functions:
```python
from utils.env_utils import env_text

def env_log_level(name: str, *, default: str = "INFO") -> str:
    level = env_text(name, default=default).upper()
    if level not in {"DEBUG", "INFO", "WARNING", "ERROR"}:
        logger.warning("Invalid log level: %s, using %s", level, default)
        return default
    return level

LOG_LEVEL = env_log_level("LOG_LEVEL", default="INFO")
```

---

## Storage Options Utilities

**File**: `/home/paul/CodeAnatomy/src/utils/storage_options.py` (lines 1-73)

### Purpose

Normalize and merge storage option mappings for Delta Lake and object storage integration. Ensures consistent credential handling across data/log storage boundaries.

### Design Principles

**Separation of Concerns**: Data storage and log storage (Delta transaction log) may use different credentials. Functions preserve this separation while enabling fallback behavior.

**Type Normalization**: All values are stringified to match Delta Lake's storage option contract (`dict[str, str]`).

**None Propagation**: Empty or None inputs produce None outputs rather than empty dicts. This enables `if storage_options:` checks to work correctly.

### Core Functions

#### Storage Option Normalization

**`normalize_storage_options(storage_options, log_storage_options, *, fallback_log_to_storage) -> tuple[dict[str, str] | None, dict[str, str] | None]`** (lines 14-40)

Normalize storage and log storage options with optional fallback:

**Parameters**:
- `storage_options`: Data storage credentials (S3, Azure, GCS, etc.)
- `log_storage_options`: Transaction log storage credentials
- `fallback_log_to_storage`: Mirror data storage credentials to log storage when absent

**Returns**:
- Tuple of `(storage, log_storage)` where each is `dict[str, str] | None`
- None when mapping is empty (not `{}`)

**Normalization Steps** (internal `_stringify_mapping()`, lines 8-11):
1. Check if input is a non-empty Mapping
2. Convert all keys to strings via `str(key)`
3. Convert all values to strings via `str(value)`
4. Return empty dict `{}` for None/empty inputs

**Fallback Behavior**:
```python
storage, log_storage = normalize_storage_options(
    storage_options={"aws_access_key_id": "AKIA..."},
    log_storage_options=None,
    fallback_log_to_storage=True,
)
# Result: log_storage = {"aws_access_key_id": "AKIA..."}
```

**Example Usage**:
```python
from utils.storage_options import normalize_storage_options

# Separate data/log credentials
storage, log_storage = normalize_storage_options(
    storage_options={
        "aws_access_key_id": "AKIA_DATA",
        "aws_secret_access_key": "secret_data",
    },
    log_storage_options={
        "aws_access_key_id": "AKIA_LOG",
        "aws_secret_access_key": "secret_log",
    },
)

# Fallback to data credentials for log
storage, log_storage = normalize_storage_options(
    storage_options={"azure_storage_account_name": "myaccount"},
    log_storage_options=None,
    fallback_log_to_storage=True,
)
```

#### Storage Option Merging

**`merged_storage_options(storage_options, log_storage_options, *, fallback_log_to_storage) -> dict[str, str] | None`** (lines 43-66)

Merge storage and log storage options into a single mapping:

**Purpose**: Some Delta Lake operations require unified credentials (e.g., table discovery, metadata reads).

**Merge Behavior**:
1. Normalize both option sets via `normalize_storage_options()`
2. Create empty dict, update with storage options
3. Update with log storage options (log overrides data on conflicts)
4. Return None if result is empty

**Example Usage**:
```python
from utils.storage_options import merged_storage_options

# Combine credentials for Delta table registration
combined = merged_storage_options(
    storage_options={"aws_region": "us-west-2"},
    log_storage_options={"aws_access_key_id": "AKIA..."},
)
# Result: {"aws_region": "us-west-2", "aws_access_key_id": "AKIA..."}

# Register with DataFusion
ctx.register_delta_table("dataset", table_uri, storage_options=combined)
```

### Usage Patterns

**Delta Lake Write Configuration**:
```python
from utils.storage_options import normalize_storage_options

# Configure separate credentials for data/log
storage, log_storage = normalize_storage_options(
    storage_options=get_data_credentials(),
    log_storage_options=get_log_credentials(),
)

write_deltalake(
    table_path,
    data,
    storage_options=storage,
    log_storage_options=log_storage,
)
```

**Credential Fallback (Development/Testing)**:
```python
from utils.storage_options import normalize_storage_options

# Use same credentials for both in dev environments
storage, log_storage = normalize_storage_options(
    storage_options=dev_credentials,
    log_storage_options=None,
    fallback_log_to_storage=True,
)
```

**Unified Credentials for Read Operations**:
```python
from utils.storage_options import merged_storage_options

# Merge credentials for metadata operations
options = merged_storage_options(
    storage_options=s3_credentials,
    log_storage_options=s3_log_credentials,
)

# Load Delta table for schema inspection
dt = DeltaTable(table_uri, storage_options=options)
schema = dt.schema().to_pyarrow()
```

### Integration with Hashing

Storage options are hashed as part of plan fingerprints to detect credential changes:

```python
from utils.hashing import hash_storage_options
from utils.storage_options import normalize_storage_options

# Normalize before hashing
storage, log_storage = normalize_storage_options(
    storage_options=raw_storage_options,
    log_storage_options=raw_log_storage_options,
)

# Generate fingerprint for cache invalidation
storage_hash = hash_storage_options(storage, log_storage)
plan_fingerprint = hash_msgpack_canonical({
    "substrait": substrait_bytes,
    "storage_hash": storage_hash,
})
```

---

## Validation Utilities

**File**: `/home/paul/CodeAnatomy/src/utils/validation.py` (lines 1-197)

### Purpose

Provide type-safe validation helpers for runtime type checking, collection validation, and PyArrow table coercion. These utilities complement static type checking by enforcing contracts at API boundaries.

### Design Principles

**Fail-Fast Validation**: Raise exceptions immediately when validation fails. No silent coercion or partial validation.

**Descriptive Errors**: All validators accept a `label` parameter for contextual error messages. `TypeError("value must be Mapping")` becomes `TypeError("config.storage_options must be Mapping")`.

**Minimal Coercion**: Validators preserve types when possible. `ensure_table()` coerces to PyArrow Table, but `ensure_mapping()` does not convert to dict.

**Generic Type Support**: Validators use TypeVars to preserve input types in return signatures.

### Core Validators

#### Type Validators

**`ensure_mapping(value, *, label, error_type) -> Mapping[str, object]`** (lines 15-40)

Validate that value implements the Mapping protocol:

**Parameters**:
- `value`: Object to validate
- `label`: Descriptive name for error messages
- `error_type`: Exception class to raise (default: TypeError)

**Returns**: The original value, typed as Mapping

**Example Usage**:
```python
from utils.validation import ensure_mapping

def process_config(config: object) -> None:
    validated = ensure_mapping(config, label="config")
    # Type checker knows validated is Mapping[str, object]
    for key, value in validated.items():
        apply_setting(key, value)
```

**`ensure_sequence(value, *, label, item_type) -> Sequence[object]`** (lines 43-83)

Validate that value is a Sequence with optional item type checking:

**Parameters**:
- `value`: Object to validate
- `label`: Descriptive name for error messages
- `item_type`: Optional type or tuple of types for item validation

**Rejects**: Strings and bytes (even though they implement Sequence protocol)

**Item Validation**: When `item_type` is provided, validates each item:
```python
validated = ensure_sequence(
    value,
    label="column_names",
    item_type=str,
)
# Raises TypeError if any item is not a str
```

**Example Usage**:
```python
from utils.validation import ensure_sequence

def register_columns(columns: object) -> None:
    validated = ensure_sequence(
        columns,
        label="columns",
        item_type=str,
    )
    for col in validated:
        register_column(col)  # col is guaranteed to be str
```

**`ensure_callable(value, *, label) -> Callable[..., object]`** (lines 86-113)

Validate that value is callable:

**Example Usage**:
```python
from utils.validation import ensure_callable

def register_builder(builder: object) -> None:
    validated = ensure_callable(builder, label="view_builder")
    result = validated(ctx)  # Safe to call
```

#### PyArrow Validators

**`ensure_table(value, *, label) -> pa.Table`** (lines 116-142)

Convert table-like input to PyArrow Table:

**Accepted Types**:
- `pyarrow.Table`
- `pyarrow.RecordBatch`
- `pyarrow.RecordBatchReader`

**Conversion Mechanism**: Delegates to `to_arrow_table()` from `datafusion_engine.arrow_schema.coercion`:
1. Tables return as-is
2. RecordBatch converts to single-batch Table
3. RecordBatchReader consumes stream and concatenates batches

**Example Usage**:
```python
from utils.validation import ensure_table

def write_dataset(data: object) -> None:
    table = ensure_table(data, label="input_data")
    # table is guaranteed to be pa.Table
    write_parquet(table)
```

#### Collection Validators

**`find_missing(required: Iterable[T], available: Container[T]) -> list[T]`** (lines 145-160)

Find items in required that are not in available:

**Returns**: List of missing items (empty when all present)

**Example Usage**:
```python
from utils.validation import find_missing

required_columns = ["id", "name", "timestamp"]
available_columns = table.column_names

missing = find_missing(required_columns, available_columns)
if missing:
    raise ValueError(f"Missing columns: {missing}")
```

**`validate_required_items(required, available, *, item_label, error_type) -> None`** (lines 163-186)

Validate that all required items are present:

**Parameters**:
- `required`: Items that must be present
- `available`: Container to check against
- `item_label`: Label for error messages (default: "items")
- `error_type`: Exception to raise (default: ValueError)

**Example Usage**:
```python
from utils.validation import validate_required_items

# Validate UDF dependencies
validate_required_items(
    required=plan.required_udfs,
    available=registered_udfs,
    item_label="UDFs",
    error_type=RuntimeError,
)
# Raises: RuntimeError("Missing required UDFs: ['hash128', 'json_extract']")
```

### Usage Patterns

**API Boundary Validation**:
```python
from utils.validation import ensure_mapping, ensure_callable

class ViewRegistry:
    def register_view(
        self,
        name: str,
        builder: object,
        options: object | None = None,
    ) -> None:
        # Validate at entry point
        validated_builder = ensure_callable(builder, label="builder")
        validated_options = ensure_mapping(
            options or {},
            label="options",
        )

        self._builders[name] = validated_builder
        self._options[name] = dict(validated_options)
```

**Schema Contract Validation**:
```python
from utils.validation import validate_required_items

def validate_schema_compatibility(
    source: pa.Schema,
    target: pa.Schema,
) -> None:
    # Ensure target has all source columns
    validate_required_items(
        required=source.names,
        available=target.names,
        item_label="columns",
    )
```

**DataFrame Coercion**:
```python
from utils.validation import ensure_table

def materialize_artifact(data: object, path: Path) -> None:
    # Coerce various inputs to Arrow Table
    table = ensure_table(data, label="artifact_data")

    # Now safe to call Arrow I/O
    pq.write_table(table, path)
```

---

## UUID Factory

**File**: `/home/paul/CodeAnatomy/src/utils/uuid_factory.py` (lines 1-127)

### Purpose

Provide time-ordered UUID generation using UUIDv7 for sortable identifiers. Falls back to `uuid6` package when running on Python < 3.14.

### Design Principles

**Time-Ordered by Default**: UUIDv7 embeds Unix timestamp in high-order bits, enabling chronological sorting and efficient database indexing.

**Thread-Safe Monotonicity**: All generation functions use a lock to ensure unique, monotonically-increasing identifiers even under concurrent access.

**Security Separation**: Separate `secure_token_hex()` function for cryptographic-quality randomness (authentication tokens, API keys).

**Version Detection**: Automatically uses native `uuid.uuid7()` on Python 3.14+ or falls back to `uuid6` package.

### UUIDv7 Background

**UUIDv7 Structure** (RFC 9562):
- Bits 0-47: Unix timestamp (milliseconds)
- Bits 48-63: Random data + counter
- Bits 64-127: Random data

**Advantages Over UUIDv4**:
1. **Sortability**: Chronological ordering without separate timestamp column
2. **Index Efficiency**: Sequential inserts avoid B-tree fragmentation
3. **Debugging**: Timestamp visible in hex representation
4. **Compatibility**: Still a valid RFC 4122 UUID

### Core Functions

#### UUIDv7 Generation

**`uuid7() -> uuid.UUID`** (lines 32-52)

Generate a time-ordered UUIDv7 instance:

**Thread Safety**: Uses module-level lock (`_UUID_LOCK`) to ensure monotonicity:
```python
with _UUID_LOCK:
    uuid7_func = getattr(uuid, "uuid7", None)
    if callable(uuid7_func):
        return uuid7_func()  # Python 3.14+
    if UUID6_MODULE is None:
        raise RuntimeError("uuid7 requires Python 3.14+ or uuid6")
    return UUID6_MODULE.uuid7()  # Fallback
```

**Example Usage**:
```python
from utils import uuid7

# Generate sortable identifier
run_id = uuid7()
# UUID('0191a8f0-1234-7abc-9def-0123456789ab')

# Extract timestamp (first 48 bits)
timestamp_ms = (run_id.int >> 80) & ((1 << 48) - 1)
```

**`uuid7_str() -> str`** (lines 55-63)

Return UUIDv7 as string with hyphens:

```python
run_id = uuid7_str()
# "0191a8f0-1234-7abc-9def-0123456789ab"
```

**`uuid7_hex() -> str`** (lines 66-74)

Return UUIDv7 as 32-character hex string (no hyphens):

```python
cache_key = uuid7_hex()
# "0191a8f012347abc9def0123456789ab"
```

**`uuid7_suffix(length: int = 12) -> str`** (lines 77-96)

Extract short suffix from UUIDv7 random tail:

**Parameters**:
- `length`: Number of hex characters to extract (1-32)

**Validation**:
- Raises `ValueError` if length ≤ 0 or > 32

**Example Usage**:
```python
from utils import uuid7_suffix

# Generate short, sortable identifiers
short_id = uuid7_suffix(12)  # Last 12 hex chars
# "56789abcdef0"

# Use for human-readable task IDs
task_id = f"task_{uuid7_suffix(8)}"
# "task_abcdef01"
```

#### Secure Token Generation

**`secure_token_hex(nbytes: int = 16) -> str`** (lines 99-115)

Generate cryptographic-quality random token:

**Purpose**: Use for security-sensitive identifiers where sortability is not required:
- API keys
- Session tokens
- Authentication secrets
- External/public identifiers

**Difference from UUIDv7**: Uses `secrets.token_hex()` (CSPRNG) instead of time-based generation.

**Example Usage**:
```python
from utils import secure_token_hex

# Generate API key
api_key = secure_token_hex(32)  # 64 hex characters
# "a1b2c3d4e5f6...789" (cryptographically random)

# Generate session token
session_token = secure_token_hex(16)
```

### Usage Patterns

**Run Identifiers**:
```python
from utils import uuid7

class ExecutionContext:
    def __init__(self) -> None:
        self.run_id = uuid7()  # Sortable across runs
        self.started_at = datetime.now(UTC)

    def start_task(self, name: str) -> TaskContext:
        return TaskContext(
            task_id=uuid7(),  # Chronologically after run_id
            run_id=self.run_id,
            name=name,
        )
```

**Database Primary Keys**:
```python
import pyarrow as pa
from utils import uuid7_hex

# Use UUIDv7 hex as primary key
records = [
    {"id": uuid7_hex(), "name": "task_1"},
    {"id": uuid7_hex(), "name": "task_2"},
]

table = pa.table({
    "id": pa.array([r["id"] for r in records]),
    "name": pa.array([r["name"] for r in records]),
})
# IDs are sortable: earlier records have lexicographically smaller IDs
```

**Short Identifiers for Logs**:
```python
from utils import uuid7_suffix

logger.info(
    "Task started",
    extra={
        "task_id": uuid7_suffix(8),  # Compact for log readability
        "run_id": current_run_id,
    },
)
```

**API Authentication**:
```python
from utils import secure_token_hex

def create_api_key(user_id: str) -> str:
    # Use secure token, not UUID
    return secure_token_hex(32)
```

### Migration from UUIDv4

When migrating from UUIDv4 to UUIDv7:

**Benefits**:
- Improved database performance (sequential inserts)
- Natural chronological ordering
- Embedded timestamp for debugging

**Compatibility**:
- UUIDv7 is still a valid UUID (RFC 4122 compliant)
- Existing UUID columns accept UUIDv7 values
- String representation remains 36 characters with hyphens

**Migration Example**:
```python
# Before
import uuid
run_id = uuid.uuid4()  # Random, non-sortable

# After
from utils import uuid7
run_id = uuid7()  # Time-ordered, sortable
```

---

## File I/O Utilities

**File**: `/home/paul/CodeAnatomy/src/utils/file_io.py` (lines 1-87)

### Purpose

Provide consistent file reading utilities with explicit encoding handling. Centralizes common file I/O patterns to ensure UTF-8 encoding and error-free parsing.

### Core Functions

**`read_text(path: Path, *, encoding: str = "utf-8") -> str`** (lines 12-27)

Read text file with consistent encoding:

```python
from pathlib import Path
from utils.file_io import read_text

content = read_text(Path("config.txt"))
```

**`read_toml(path: Path) -> Mapping[str, object]`** (lines 30-43)

Read and parse TOML file using Python 3.11+ `tomllib`:

```python
from utils.file_io import read_toml

config = read_toml(Path("settings.toml"))
database_url = config["database"]["url"]
```

**`read_json(path: Path) -> Any`** (lines 46-59)

Read and parse JSON file:

```python
from utils.file_io import read_json

manifest = read_json(Path("manifest.json"))
```

**`read_pyproject_toml(path: Path) -> Mapping[str, object]`** (lines 62-78)

Read `pyproject.toml` file with directory path support:

**Path Resolution**:
- If `path` is a file: read directly
- If `path` is a directory: append `"pyproject.toml"` and read

```python
from pathlib import Path
from utils.file_io import read_pyproject_toml

# Read from directory
config = read_pyproject_toml(Path("/home/user/project"))
# Reads: /home/user/project/pyproject.toml

# Read from file
config = read_pyproject_toml(Path("pyproject.toml"))
```

### Usage Patterns

**Configuration Loading**:
```python
from pathlib import Path
from utils.file_io import read_toml

def load_config(config_path: Path) -> dict[str, object]:
    raw_config = read_toml(config_path)
    return validate_config(raw_config)
```

**Project Metadata Parsing**:
```python
from utils.file_io import read_pyproject_toml

def get_project_version(repo_root: Path) -> str:
    pyproject = read_pyproject_toml(repo_root)
    return pyproject["project"]["version"]
```

---

## Cross-Module Integration

### Hashing + Storage Options

Storage options are normalized before hashing for cache keys:

```python
from utils.hashing import hash_storage_options
from utils.storage_options import normalize_storage_options

# Normalize credentials
storage, log_storage = normalize_storage_options(
    storage_options=raw_storage,
    log_storage_options=raw_log_storage,
)

# Hash for fingerprint
storage_hash = hash_storage_options(storage, log_storage)
```

### Environment + Validation

Environment parsing combined with validation for robust configuration:

```python
from utils.env_utils import env_int, env_text
from utils.validation import ensure_mapping

# Parse with defaults
workers = env_int("WORKERS", default=4)
config_path = env_text("CONFIG_PATH", default="config.toml")

# Validate range
if workers < 1 or workers > 64:
    raise ValueError(f"WORKERS must be 1-64, got {workers}")

# Load and validate config
raw_config = read_toml(Path(config_path))
validated_config = ensure_mapping(raw_config, label="config")
```

### UUID + Hashing

UUID-based identifiers combined with hashing for derived keys:

```python
from utils import uuid7_hex
from utils.hashing import hash128_from_text

# Generate base identifier
run_id = uuid7_hex()

# Derive cache key
cache_key = hash128_from_text(f"run:{run_id}:artifact:schema")
```

### Registry + Validation

Registries with validation at registration time:

```python
from utils.registry_protocol import MutableRegistry
from utils.validation import ensure_callable

class BuilderRegistry:
    def __init__(self) -> None:
        self._registry = MutableRegistry[str, Callable]()

    def register(self, name: str, builder: object) -> None:
        validated = ensure_callable(builder, label=f"builder[{name}]")
        self._registry.register(name, validated)
```

---

## Testing Utilities

All utility modules include comprehensive test coverage:

**Hash Determinism Tests** (`tests/unit/utils/test_hashing.py`):
- Verify consistent output across runs
- Test canonical ordering for dictionaries
- Validate storage option hashing behavior

**Registry Tests** (`tests/unit/utils/test_registry_protocol.py`):
- Verify protocol compliance
- Test immutability guarantees
- Validate error handling for duplicate registrations

**Environment Parsing Tests** (`tests/unit/utils/test_env_utils.py`):
- Test all coercion rules (bool, int, float, str)
- Verify default value handling
- Test invalid input logging

**UUID Tests** (`tests/unit/utils/test_uuid_factory.py`):
- Verify UUIDv7 sortability
- Test thread-safe generation under concurrency
- Validate suffix extraction

**Validation Tests** (`tests/unit/utils/test_validation.py`):
- Test type enforcement
- Verify error message formatting
- Test PyArrow coercion paths

---

## Extension Guidelines

### Adding New Hash Functions

When adding domain-specific hash functions:

1. **Choose Serialization Format**: Prefer msgpack for structured data, JSON for external compatibility
2. **Use Canonical Encoders**: Use `MSGPACK_ENCODER` or `JSON_ENCODER_SORTED` for determinism
3. **Document Semantics**: Explain what aspects of the input affect the hash
4. **Add Tests**: Verify determinism and collision resistance

Example:
```python
def hash_arrow_schema(schema: pa.Schema) -> str:
    """Return SHA-256 digest of Arrow schema DDL."""
    ddl = schema.to_string(truncate_metadata=False)
    return hash128_from_text(ddl)
```

### Custom Registry Implementations

For registries with rich behavior, prefer composition:

```python
class CachingRegistry[K, V]:
    def __init__(self) -> None:
        self._backing = MutableRegistry[K, V]()
        self._cache: dict[K, V] = {}

    def register(self, key: K, value: V) -> None:
        self._backing.register(key, value)
        self._cache[key] = value

    def get(self, key: K) -> V | None:
        if key in self._cache:
            return self._cache[key]
        return self._backing.get(key)
```

### Environment Variable Conventions

When adding new environment variables:

1. **Prefix with Module**: `CODEANATOMY_*`, `DATAFUSION_*`, `OTEL_*`
2. **Use Consistent Casing**: UPPER_SNAKE_CASE for environment variable names
3. **Document Defaults**: Always specify default value in code comments
4. **Add to Documentation**: Document in project README or config reference

Example:
```python
# Environment: CODEANATOMY_CACHE_DIR
# Default: .cache/codeanatomy
CACHE_DIR = Path(env_text(
    "CODEANATOMY_CACHE_DIR",
    default=".cache/codeanatomy",
))
```

---

## Performance Considerations

### Hashing Performance

**Msgpack vs JSON**: Msgpack is 2-3x faster than JSON for structured data:
- Use msgpack for internal fingerprints (plan bundles, cache keys)
- Use JSON for external interfaces (storage options, config files)

**Hash Truncation**: Use `length` parameter for shorter identifiers:
```python
# Full 64-character hash
full_hash = hash_sha256_hex(data)

# Truncated 16-character hash (64-bit uniqueness)
short_hash = hash_sha256_hex(data, length=16)
```

### Registry Lookup Performance

**MutableRegistry**: O(1) lookup via dict backing
**ImmutableRegistry**: O(n) lookup via tuple scan

Use `ImmutableRegistry` only when:
- Registry has < 100 entries
- Immutability is required for correctness
- Thread-safe access without locks is needed

For larger registries, use `MutableRegistry` and protect with locks if needed.

### Environment Variable Caching

Environment variables are read once per call. For frequently accessed values, cache at module level:

```python
# Read once at module import
ENABLE_CACHE = env_bool("ENABLE_CACHE", default=True)

# Use cached value
def get_value(key: str) -> object | None:
    if ENABLE_CACHE:
        return cache.get(key)
    return None
```

---

## Summary

The `src/utils/` module provides essential cross-cutting utilities that ensure consistency, determinism, and type safety across CodeAnatomy. Key design principles:

1. **Explicitness**: All functions have clear contracts with no hidden defaults
2. **Type Safety**: Validation helpers enforce contracts at runtime
3. **Determinism**: Hash functions guarantee stable output for equivalent inputs
4. **Minimal Coupling**: Utilities avoid domain logic and framework dependencies
5. **Composability**: Simple primitives combine to build complex behavior

When extending utilities, prioritize clarity and predictability over convenience. Utilities should be boring, reliable building blocks that enable innovation in domain code.
