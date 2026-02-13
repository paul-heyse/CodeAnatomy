# Runtime Services Architecture

## Overview

The CQ runtime services tier provides process-global infrastructure for execution policy management, persistent caching, worker scheduling, and hexagonal service composition. This layer sits between the CLI/command layer and the domain-specific subsystems (search, query, calls, neighborhood), providing reusable concurrency primitives, cache backends, LSP runtime infrastructure, and service-oriented interfaces.

**Key Responsibilities:**
- Runtime execution policy management with environment variable overrides
- Dual-pool worker scheduling (CPU/process-based, I/O/thread-based)
- Persistent disk-backed caching with fail-open semantics
- Hexagonal service layer for search, entity, and calls commands
- LSP runtime infrastructure (contract state, request budgets, session management)
- Diagnostic artifact contracts and performance benchmarking

**Design Philosophy:**
- **Fail-open**: All caching and LSP operations degrade gracefully on errors
- **Singleton management**: Workspace-keyed singletons with thread-safe initialization
- **Composition root**: Central `CqRuntimeServices` bundle wires all services
- **Contract-first**: All payloads and requests use msgspec structs
- **Environment overrides**: All policies support `CQ_RUNTIME_*` environment variables

## Architecture Topology

```mermaid
flowchart TB
    subgraph CLI["CLI Layer"]
        Search[search command]
        Query[query command]
        Calls[calls command]
        Run[run command]
    end

    subgraph Bootstrap["Composition Root"]
        Bootstrap[bootstrap.py]
        Services[CqRuntimeServices]
    end

    subgraph ServiceLayer["Service Layer"]
        SearchSvc[SearchService]
        EntitySvc[EntityService]
        CallsSvc[CallsService]
    end

    subgraph Runtime["Runtime Infrastructure"]
        Policy[RuntimeExecutionPolicy]
        Scheduler[WorkerScheduler]
        Cache[CqCacheBackend]
    end

    subgraph Workers["Worker Pools"]
        CPUPool[ProcessPoolExecutor<br/>spawn context]
        IOPool[ThreadPoolExecutor]
    end

    subgraph LSP["LSP Runtime"]
        LspAdapter[lsp_front_door_adapter]
        LspBudget[lsp_request_budget]
        LspSession[lsp/session_manager]
        LspQueue[lsp/request_queue]
    end

    CLI --> Bootstrap
    Bootstrap --> Services
    Services --> ServiceLayer
    Services --> Runtime
    Runtime --> Scheduler
    Runtime --> Cache
    Scheduler --> Workers
    ServiceLayer --> LSP
    LSP --> Workers

    style Bootstrap fill:#e1f5ff
    style Runtime fill:#fff4e6
    style Workers fill:#f3e5f5
    style LSP fill:#e8f5e9
```

---

## Runtime Execution Policy

### Three-Level Policy Hierarchy

Runtime behavior is controlled by a three-level policy hierarchy defined in `tools/cq/core/runtime/execution_policy.py`:

```python
class ParallelismPolicy(CqSettingsStruct, frozen=True):
    """Worker policy for CPU and I/O execution."""
    cpu_workers: PositiveInt
    io_workers: PositiveInt
    lsp_request_workers: PositiveInt
    query_partition_workers: PositiveInt = 2
    calls_file_workers: PositiveInt = 4
    run_step_workers: PositiveInt = 4
    enable_process_pool: bool = True

class LspRuntimePolicy(CqSettingsStruct, frozen=True):
    """LSP timing and budgeting policy."""
    timeout_ms: PositiveInt = 2000
    startup_timeout_ms: PositiveInt = 2000
    max_targets_search: PositiveInt = 1
    max_targets_calls: PositiveInt = 1
    max_targets_entity: PositiveInt = 3

class CacheRuntimePolicy(CqSettingsStruct, frozen=True):
    """Cache policy for CQ runtime adapters."""
    enabled: bool = True
    ttl_seconds: PositiveInt = 900
    shards: PositiveInt = 8
    timeout_seconds: PositiveFloat = 0.05

class RuntimeExecutionPolicy(CqSettingsStruct, frozen=True):
    """Top-level runtime policy envelope."""
    parallelism: ParallelismPolicy
    lsp: LspRuntimePolicy = LspRuntimePolicy()
    cache: CacheRuntimePolicy = CacheRuntimePolicy()
```

**Policy Levels:**
1. **ParallelismPolicy** - Worker pool sizing for CPU/I/O/LSP tasks
2. **LspRuntimePolicy** - LSP timeout budgets and target limits
3. **CacheRuntimePolicy** - Disk cache behavior and TTL

### Environment Variable Override System

All policy fields support `CQ_RUNTIME_*` environment variable overrides:

```python
def default_runtime_execution_policy() -> RuntimeExecutionPolicy:
    """Build runtime policy from host defaults and optional env overrides."""
    cpu_count = max(1, os.cpu_count() or 1)
    cpu_workers = _env_int("CPU_WORKERS", max(1, cpu_count - 1))
    io_workers = _env_int("IO_WORKERS", max(8, cpu_count))
    lsp_workers = _env_int("LSP_REQUEST_WORKERS", 4)
    enable_process_pool = _env_bool("ENABLE_PROCESS_POOL", default=True)
    # ... (lines 103-149)
```

**Supported Environment Variables:**

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `CQ_RUNTIME_CPU_WORKERS` | int | `cpu_count - 1` | ProcessPool workers |
| `CQ_RUNTIME_IO_WORKERS` | int | `max(8, cpu_count)` | ThreadPool workers |
| `CQ_RUNTIME_LSP_REQUEST_WORKERS` | int | `4` | LSP request concurrency |
| `CQ_RUNTIME_QUERY_PARTITION_WORKERS` | int | `2` | Query partition workers |
| `CQ_RUNTIME_CALLS_FILE_WORKERS` | int | `4` | Calls file workers |
| `CQ_RUNTIME_RUN_STEP_WORKERS` | int | `4` | Run step workers |
| `CQ_RUNTIME_ENABLE_PROCESS_POOL` | bool | `true` | Enable ProcessPool (else fallback to ThreadPool) |
| `CQ_RUNTIME_LSP_TIMEOUT_MS` | int | `2000` | LSP request timeout (milliseconds) |
| `CQ_RUNTIME_LSP_STARTUP_TIMEOUT_MS` | int | `2000` | LSP startup timeout (milliseconds) |
| `CQ_RUNTIME_LSP_TARGETS_SEARCH` | int | `1` | Max LSP targets for search |
| `CQ_RUNTIME_LSP_TARGETS_CALLS` | int | `1` | Max LSP targets for calls |
| `CQ_RUNTIME_LSP_TARGETS_ENTITY` | int | `3` | Max LSP targets for entity queries |
| `CQ_RUNTIME_CACHE_ENABLED` | bool | `true` | Enable disk cache |
| `CQ_RUNTIME_CACHE_TTL_SECONDS` | int | `900` | Cache TTL (15 minutes) |
| `CQ_RUNTIME_CACHE_SHARDS` | int | `8` | FanoutCache shards |
| `CQ_RUNTIME_CACHE_TIMEOUT_SECONDS` | float | `0.05` | Cache operation timeout |

**Environment Parsing Helpers:**

```python
def _env_int(name: str, default: int, *, minimum: int = 1) -> int:
    raw = os.getenv(f"{_ENV_PREFIX}{name}")
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return max(minimum, value)

def _env_bool(name: str, *, default: bool) -> bool:
    raw = os.getenv(f"{_ENV_PREFIX}{name}")
    if raw is None:
        return default
    value = raw.strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    return default
```

All helpers apply safe parsing with fallback to defaults on parse failure.

---

## Worker Scheduler

### Dual-Pool Architecture

The `WorkerScheduler` in `tools/cq/core/runtime/worker_scheduler.py` manages two executor pools:

1. **CPU Pool** - `ProcessPoolExecutor` with `spawn` multiprocessing context
2. **I/O Pool** - `ThreadPoolExecutor` for I/O-bound tasks

```python
class WorkerScheduler:
    """Lazily-initialized shared worker pools with bounded collection helpers."""

    def __init__(self, policy: ParallelismPolicy) -> None:
        self._policy = policy
        self._lock = threading.Lock()
        self._cpu_pool: ProcessPoolExecutor | None = None
        self._io_pool: ThreadPoolExecutor | None = None

    def io_pool(self) -> ThreadPoolExecutor:
        """Return shared IO pool."""
        with self._lock:
            if self._io_pool is None:
                self._io_pool = ThreadPoolExecutor(
                    max_workers=self._policy.io_workers
                )
            return self._io_pool

    def cpu_pool(self) -> ProcessPoolExecutor:
        """Return shared CPU pool configured with spawn context."""
        with self._lock:
            if self._cpu_pool is None:
                ctx = multiprocessing.get_context("spawn")
                self._cpu_pool = ProcessPoolExecutor(
                    max_workers=self._policy.cpu_workers,
                    mp_context=ctx,
                )
            return self._cpu_pool
```

**Why `spawn` Context:**
- Avoids fork-related issues on macOS (especially with LSP client state)
- Prevents accidental state sharing between workers
- Safer for complex runtime state (LSP sessions, cache handles)

**Design Note:** CPU pool is conditionally used based on `ParallelismPolicy.enable_process_pool`. If disabled, all tasks fall back to the I/O ThreadPool.

### Task Submission API

```python
def submit_io(
    self,
    fn: Callable[P, R],
    /,
    *args: P.args,
    **kwargs: P.kwargs,
) -> Future[R]:
    """Submit an I/O task."""
    callable_fn = cast("Callable[..., R]", fn)
    return self.io_pool().submit(callable_fn, *args, **kwargs)

def submit_cpu(
    self,
    fn: Callable[P, R],
    /,
    *args: P.args,
    **kwargs: P.kwargs,
) -> Future[R]:
    """Submit a CPU task."""
    callable_fn = cast("Callable[..., R]", fn)
    if self._policy.enable_process_pool:
        return self.cpu_pool().submit(callable_fn, *args, **kwargs)
    return self.io_pool().submit(callable_fn, *args, **kwargs)
```

**Usage Pattern:**
- LSP requests → `submit_io()`
- AST classification tasks → `submit_cpu()` (when enabled)
- Search partition enrichment → `submit_cpu()`

### Bounded Collection with Deterministic Timeouts

The scheduler provides `collect_bounded()` for timeout-based future collection:

```python
@staticmethod
def collect_bounded(
    futures: Iterable[Future[T]],
    *,
    timeout_seconds: float,
) -> WorkerBatchResult[T]:
    """Collect futures up to timeout, preserving deterministic completion order.

    Returns:
        Completed values and timeout count.
    """
    future_list = list(futures)
    if not future_list:
        return WorkerBatchResult(done=[], timed_out=0)
    done, pending = wait(future_list, timeout=timeout_seconds)
    values: list[T] = [
        future.result() for future in future_list if future in done
    ]
    for future in pending:
        future.cancel()
    return WorkerBatchResult(done=values, timed_out=len(pending))
```

**WorkerBatchResult Contract:**
```python
@dataclass(slots=True)
class WorkerBatchResult[T]:
    """Result bundle for bounded worker collection."""
    done: list[T]
    timed_out: int
```

**Key Properties:**
- Cancels pending futures on timeout
- Preserves submission order for `done` results
- Returns timed-out count for telemetry

### Process-Global Singleton

Worker scheduler uses lazy singleton pattern with atexit cleanup:

```python
class _SchedulerState:
    """Mutable holder for process-global scheduler singleton."""
    def __init__(self) -> None:
        self.scheduler: WorkerScheduler | None = None

_SCHEDULER_STATE = _SchedulerState()
_SCHEDULER_LOCK = threading.Lock()

def get_worker_scheduler() -> WorkerScheduler:
    """Return process-global CQ worker scheduler."""
    with _SCHEDULER_LOCK:
        if _SCHEDULER_STATE.scheduler is None:
            policy = default_runtime_execution_policy().parallelism
            _SCHEDULER_STATE.scheduler = WorkerScheduler(policy)
        return _SCHEDULER_STATE.scheduler

def close_worker_scheduler() -> None:
    """Close and clear process-global worker scheduler."""
    with _SCHEDULER_LOCK:
        scheduler = _SCHEDULER_STATE.scheduler
        _SCHEDULER_STATE.scheduler = None
    if scheduler is not None:
        scheduler.close()

atexit.register(close_worker_scheduler)
```

**Lifecycle:**
1. First call to `get_worker_scheduler()` builds policy and scheduler
2. Scheduler pools are lazily initialized on first task submission
3. `atexit.register()` ensures cleanup on process exit
4. `close()` shuts down pools with `cancel_futures=True`

---

## Cache Infrastructure

### Cache Architecture Overview

The cache subsystem (`tools/cq/core/cache/`) provides fail-open persistent caching backed by `diskcache.FanoutCache`:

```
cache/
├── interface.py          # CqCacheBackend protocol, NoopCacheBackend
├── policy.py             # CqCachePolicyV1, default_cache_policy()
├── key_builder.py        # build_cache_key() with SHA256 digest
├── contracts.py          # Typed cache payload contracts
└── diskcache_backend.py  # DiskcacheBackend, get_cq_cache_backend()
```

### CqCacheBackend Protocol

Defined in `tools/cq/core/cache/interface.py`:

```python
class CqCacheBackend(Protocol):
    """Cache adapter contract used by CQ runtime services."""

    def get(self, key: str) -> object | None:
        """Fetch cached value for key. Returns None when absent."""

    def set(
        self,
        key: str,
        value: object,
        *,
        expire: int | None = None,
        tag: str | None = None,
    ) -> None:
        """Store value for key."""

    def delete(self, key: str) -> None:
        """Delete cached key."""

    def evict_tag(self, tag: str) -> None:
        """Evict entries associated with tag when supported."""

    def close(self) -> None:
        """Close backend resources."""
```

**NoopCacheBackend:**
Used when caching is disabled via `CQ_CACHE_ENABLED=false`:

```python
class NoopCacheBackend:
    """No-op cache backend used when caching is disabled."""
    def get(self, key: str) -> object | None:
        return None
    # ... all methods are no-ops
```

### Cache Policy

Defined in `tools/cq/core/cache/policy.py`:

```python
class CqCachePolicyV1(CqSettingsStruct, frozen=True):
    """Policy controlling disk-backed CQ cache behavior."""

    enabled: bool = True
    directory: str = ".cq_cache"
    shards: PositiveInt = 8
    timeout_seconds: PositiveFloat = 0.05
    ttl_seconds: PositiveInt = 900  # 15 minutes

def default_cache_policy(*, root: Path) -> CqCachePolicyV1:
    """Build cache policy from runtime defaults and optional env overrides."""
    runtime = default_runtime_execution_policy().cache
    raw_enabled = os.getenv("CQ_CACHE_ENABLED")
    enabled = runtime.enabled
    if raw_enabled is not None:
        enabled = raw_enabled.strip().lower() not in {
            "0", "false", "no", "off"
        }

    raw_dir = os.getenv("CQ_CACHE_DIR")
    directory = raw_dir.strip() if raw_dir else str(root / ".cq_cache")

    raw_ttl = os.getenv("CQ_CACHE_TTL_SECONDS")
    ttl_seconds = runtime.ttl_seconds
    if raw_ttl:
        try:
            ttl_seconds = max(1, int(raw_ttl))
        except ValueError:
            ttl_seconds = runtime.ttl_seconds

    return CqCachePolicyV1(
        enabled=enabled,
        directory=directory,
        shards=runtime.shards,
        timeout_seconds=runtime.timeout_seconds,
        ttl_seconds=ttl_seconds,
    )
```

**Environment Variable Support:**
- `CQ_CACHE_ENABLED` - Enable/disable caching
- `CQ_CACHE_DIR` - Override default `.cq_cache` directory
- `CQ_CACHE_TTL_SECONDS` - Override default 900s TTL

### Cache Key Builder

Defined in `tools/cq/core/cache/key_builder.py`:

```python
def build_cache_key(
    namespace: str,
    *,
    version: str,
    workspace: str,
    language: str,
    target: str,
    extras: dict[str, object] | None = None,
) -> str:
    """Build a deterministic cache key string for CQ runtime data.

    Returns:
        Namespaced cache key that includes a stable payload digest.
    """
    payload = {
        "namespace": namespace,
        "version": version,
        "workspace": workspace,
        "language": language,
        "target": target,
        "extras": extras or {},
    }
    digest = hashlib.sha256(msgspec.json.encode(payload)).hexdigest()
    return f"cq:{namespace}:{version}:{digest}"
```

**Key Structure:** `cq:<namespace>:<version>:<sha256_digest>`

**Cache Tags for Invalidation:**
```python
def build_cache_tag(*, workspace: str, language: str) -> str:
    """Build tag used for bulk invalidation."""
    return f"{workspace}:{language}"

def build_run_cache_tag(*, workspace: str, language: str, run_id: str) -> str:
    """Build run-scoped cache invalidation tag."""
    return f"{workspace}:{language}:run:{run_id}"
```

### Typed Cache Payload Contracts

Defined in `tools/cq/core/cache/contracts.py`:

```python
class SgRecordCacheV1(CqCacheStruct, frozen=True):
    """Cache-safe serialization contract for one ast-grep record."""
    record: RecordType = "def"
    kind: str = ""
    file: str = ""
    start_line: NonNegativeInt = 0
    start_col: NonNegativeInt = 0
    end_line: NonNegativeInt = 0
    end_col: NonNegativeInt = 0
    text: str = ""
    rule_id: str = ""

class SearchPartitionCacheV1(CqCacheStruct, frozen=True):
    """Cached search partition payload."""
    pattern: str
    raw_matches: list[dict[str, object]]
    stats: dict[str, object]
    enriched_matches: list[dict[str, object]]

class QueryEntityScanCacheV1(CqCacheStruct, frozen=True):
    """Cached entity-query scan payload."""
    records: list[SgRecordCacheV1]

class CallsTargetCacheV1(CqCacheStruct, frozen=True):
    """Cached calls-target metadata payload."""
    target_location: tuple[str, int] | None = None
    target_callees: dict[str, int] = msgspec.field(default_factory=dict)
```

**Cache Namespace Mapping:**
- `search_partition` → `SearchPartitionCacheV1`
- `query_entity_scan` → `QueryEntityScanCacheV1`
- `calls_target` → `CallsTargetCacheV1`
- `lsp_front_door` → `dict[str, object]` (untyped LSP payloads)

### DiskcacheBackend Implementation

Defined in `tools/cq/core/cache/diskcache_backend.py`:

```python
class DiskcacheBackend:
    """Fail-open diskcache backend for CQ runtime."""

    def __init__(self, cache: FanoutCache, *, default_ttl_seconds: int) -> None:
        self._cache = cache
        self._default_ttl_seconds = default_ttl_seconds

    def get(self, key: str) -> object | None:
        """Fetch key from cache."""
        try:
            return self._cache.get(key, default=None)
        except (OSError, RuntimeError, ValueError, TypeError):
            return None

    def set(
        self,
        key: str,
        value: object,
        *,
        expire: int | None = None,
        tag: str | None = None,
    ) -> None:
        """Write value to cache."""
        ttl = expire if expire is not None else self._default_ttl_seconds
        try:
            self._cache.set(key, value, expire=ttl, tag=tag, retry=True)
        except (OSError, RuntimeError, ValueError, TypeError):
            return

    def delete(self, key: str) -> None:
        """Delete key from cache."""
        try:
            self._cache.delete(key, retry=True)
        except (OSError, RuntimeError, ValueError, TypeError):
            return

    def evict_tag(self, tag: str) -> None:
        """Evict tagged items."""
        try:
            self._cache.evict(tag, retry=True)
        except (OSError, RuntimeError, ValueError, TypeError):
            return

    def close(self) -> None:
        """Close cache resources."""
        self._cache.close()
```

**Fail-Open Semantics:**
All methods catch common exceptions and fail gracefully:
- `OSError` - Disk I/O failures
- `RuntimeError` - Internal cache state errors
- `ValueError` - Malformed cache data
- `TypeError` - Type contract violations

### Workspace-Keyed Singleton

```python
class _BackendState:
    """Mutable holder for process-global cache backend singleton."""
    def __init__(self) -> None:
        self.backends: dict[str, CqCacheBackend] = {}

_BACKEND_STATE = _BackendState()
_BACKEND_LOCK = threading.Lock()

def get_cq_cache_backend(*, root: Path) -> CqCacheBackend:
    """Return workspace-keyed CQ cache backend."""
    workspace = str(root.resolve())
    with _BACKEND_LOCK:
        existing = _BACKEND_STATE.backends.get(workspace)
        if existing is not None:
            return existing
        policy = default_cache_policy(root=root)
        if not policy.enabled:
            backend: CqCacheBackend = NoopCacheBackend()
        else:
            backend = _build_diskcache_backend(policy)
        _BACKEND_STATE.backends[workspace] = backend
        return backend

def close_cq_cache_backend(*, root: Path | None = None) -> None:
    """Close and clear workspace-backed cache backend(s)."""
    backends: list[CqCacheBackend]
    with _BACKEND_LOCK:
        if root is None:
            backends = list(_BACKEND_STATE.backends.values())
            _BACKEND_STATE.backends.clear()
        else:
            workspace = str(root.resolve())
            backend = _BACKEND_STATE.backends.pop(workspace, None)
            backends = [backend] if backend is not None else []
    for backend in backends:
        backend.close()

atexit.register(close_cq_cache_backend)
```

**Backend Construction:**
```python
def _build_diskcache_backend(policy: CqCachePolicyV1) -> CqCacheBackend:
    cache = FanoutCache(
        directory=str(Path(policy.directory).expanduser()),
        shards=max(1, int(policy.shards)),
        timeout=float(policy.timeout_seconds),
        tag_index=True,
    )
    return DiskcacheBackend(cache, default_ttl_seconds=policy.ttl_seconds)
```

**FanoutCache Configuration:**
- `directory` - Expanded cache directory path
- `shards` - Number of SQLite database shards (8 by default)
- `timeout` - SQLite lock timeout (0.05s default)
- `tag_index=True` - Enables tag-based eviction

---

## Service Layer

### Hexagonal Ports

Defined in `tools/cq/core/ports.py`:

```python
class SearchServicePort(Protocol):
    """Port for search command execution."""
    def execute(self, request: SearchServiceRequest) -> CqResult:
        ...

class EntityServicePort(Protocol):
    """Port for entity query execution."""
    def attach_front_door(self, request: EntityFrontDoorRequest) -> None:
        ...

class CallsServicePort(Protocol):
    """Port for calls macro execution."""
    def execute(self, request: CallsServiceRequest) -> CqResult:
        ...

class CachePort(Protocol):
    """Port for cache get/set operations."""
    def get(self, key: str) -> object | None:
        ...
    def set(
        self,
        key: str,
        value: object,
        *,
        expire: int | None = None,
        tag: str | None = None,
    ) -> None:
        ...
```

**Design:** Ports define abstract interfaces for service contracts, decoupling CLI/command layer from concrete implementations.

### Service Implementations

#### SearchService

Defined in `tools/cq/core/services/search_service.py`:

```python
class SearchServiceRequest(CqStruct, frozen=True):
    """Typed request contract for smart-search service execution."""
    root: Path
    query: str
    mode: QueryMode | None = None
    lang_scope: QueryLanguageScope = "auto"
    include_globs: list[str] | None = None
    exclude_globs: list[str] | None = None
    include_strings: bool = False
    limits: SearchLimits | None = None
    tc: Toolchain | None = None
    argv: list[str] | None = None

class SearchService:
    """Application-layer service for CQ search."""

    @staticmethod
    def execute(request: SearchServiceRequest) -> CqResult:
        """Execute CQ smart search."""
        return smart_search(
            root=request.root,
            query=request.query,
            mode=request.mode,
            lang_scope=request.lang_scope,
            include_globs=request.include_globs,
            exclude_globs=request.exclude_globs,
            include_strings=request.include_strings,
            limits=request.limits,
            tc=request.tc,
            argv=request.argv,
        )
```

**Delegation:** Service directly delegates to `smart_search()` from `tools/cq/search/smart_search.py`.

#### EntityService

Defined in `tools/cq/core/services/entity_service.py`:

```python
class EntityFrontDoorRequest(CqStruct, frozen=True):
    """Typed request contract for entity front-door attachment."""
    result: CqResult
    relationship_detail_max_matches: int = 50

class EntityService:
    """Application-layer service for CQ entity flow."""

    @staticmethod
    def attach_front_door(request: EntityFrontDoorRequest) -> None:
        """Attach entity front-door insight to a CQ result."""
        attach_entity_front_door_insight(
            request.result,
            relationship_detail_max_matches=(
                request.relationship_detail_max_matches
            ),
        )
```

**Delegation:** Service delegates to `attach_entity_front_door_insight()` from `tools/cq/query/entity_front_door.py`.

#### CallsService

Defined in `tools/cq/core/services/calls_service.py`:

```python
class CallsServiceRequest(CqStruct, frozen=True):
    """Typed request contract for calls macro service execution."""
    root: Path
    function_name: str
    tc: Toolchain
    argv: list[str]

class CallsService:
    """Application-layer service for CQ calls macro."""

    @staticmethod
    def execute(request: CallsServiceRequest) -> CqResult:
        """Execute CQ calls macro."""
        return cmd_calls(
            tc=request.tc,
            root=request.root,
            argv=request.argv,
            function_name=request.function_name,
        )
```

**Delegation:** Service delegates to `cmd_calls()` from `tools/cq/macros/calls.py`.

---

## Composition Root

### CqRuntimeServices Bundle

Defined in `tools/cq/core/bootstrap.py`:

```python
@dataclass(frozen=True)
class CqRuntimeServices:
    """Runtime service bundle."""
    search: SearchService
    entity: EntityService
    calls: CallsService
    cache: CqCacheBackend
    policy: RuntimeExecutionPolicy

def build_runtime_services(*, root: Path) -> CqRuntimeServices:
    """Construct CQ runtime service bundle for a workspace."""
    return CqRuntimeServices(
        search=SearchService(),
        entity=EntityService(),
        calls=CallsService(),
        cache=get_cq_cache_backend(root=root),
        policy=default_runtime_execution_policy(),
    )
```

**Bundle Contents:**
- `search` - SearchService instance
- `entity` - EntityService instance
- `calls` - CallsService instance
- `cache` - Workspace-keyed cache backend
- `policy` - Runtime execution policy with env overrides

### Workspace-Scoped Singleton

```python
_RUNTIME_SERVICES_LOCK = threading.Lock()
_RUNTIME_SERVICES: dict[str, CqRuntimeServices] = {}

def resolve_runtime_services(root: Path) -> CqRuntimeServices:
    """Resolve workspace-scoped CQ runtime services."""
    workspace = str(root.resolve())
    with _RUNTIME_SERVICES_LOCK:
        services = _RUNTIME_SERVICES.get(workspace)
        if services is not None:
            return services
        services = build_runtime_services(root=root)
        _RUNTIME_SERVICES[workspace] = services
        return services

def clear_runtime_services() -> None:
    """Clear cached runtime service bundles."""
    with _RUNTIME_SERVICES_LOCK:
        _RUNTIME_SERVICES.clear()
    close_cq_cache_backend()

atexit.register(clear_runtime_services)
```

**Lifecycle:**
1. `resolve_runtime_services(root)` called by CLI commands
2. First call builds bundle and caches by resolved workspace path
3. Subsequent calls for same workspace return cached bundle
4. `atexit.register()` ensures cleanup on process exit

**Integration Points:**
- Search command → `services.search.execute(SearchServiceRequest(...))`
- Query command → `services.entity.attach_front_door(EntityFrontDoorRequest(...))`
- Calls command → `services.calls.execute(CallsServiceRequest(...))`
- Run command → Uses services for step dispatch

---

## LSP Runtime Infrastructure

### LSP Contract State

Defined in `tools/cq/search/lsp_contract_state.py`:

```python
LspProvider = Literal["pyrefly", "rust_analyzer", "none"]
LspStatus = Literal["unavailable", "skipped", "failed", "partial", "ok"]

class LspContractStateV1(CqStruct, frozen=True):
    """Deterministic LSP state for front-door degradation semantics."""
    provider: LspProvider = "none"
    available: bool = False
    attempted: int = 0
    applied: int = 0
    failed: int = 0
    timed_out: int = 0
    status: LspStatus = "unavailable"
    reasons: tuple[str, ...] = ()

class LspContractStateInputV1(CqStruct, frozen=True):
    """Input envelope for deterministic LSP contract-state derivation."""
    provider: LspProvider
    available: bool
    attempted: int = 0
    applied: int = 0
    failed: int = 0
    timed_out: int = 0
    reasons: tuple[str, ...] = ()

def derive_lsp_contract_state(
    input_state: LspContractStateInputV1
) -> LspContractStateV1:
    """Derive canonical LSP state from capability + attempt telemetry."""
    # ... (lines 38-89)
```

**Status Derivation Logic:**
1. `unavailable` - LSP provider not available (`available=False`)
2. `skipped` - LSP available but not attempted (`attempted=0`)
3. `failed` - LSP attempted but no successful applications (`applied=0`)
4. `partial` - LSP applied to some targets but some failed
5. `ok` - LSP applied to all targets successfully

**Example:**
```python
input_state = LspContractStateInputV1(
    provider="pyrefly",
    available=True,
    attempted=3,
    applied=2,
    failed=1,
    timed_out=0,
    reasons=("timeout on file.py",),
)
state = derive_lsp_contract_state(input_state)
# → LspContractStateV1(
#     provider="pyrefly",
#     available=True,
#     attempted=3,
#     applied=2,
#     failed=1,
#     status="partial",
#     reasons=("timeout on file.py",),
# )
```

### LSP Front Door Adapter

Defined in `tools/cq/search/lsp_front_door_adapter.py`:

```python
class LanguageLspEnrichmentRequest(CqStruct, frozen=True):
    """Request envelope for language-aware front-door LSP enrichment."""
    language: QueryLanguage
    mode: str
    root: Path
    file_path: Path
    line: int
    col: int
    symbol_hint: str | None = None

def lsp_runtime_enabled() -> bool:
    """Return whether LSP enrichment is enabled for the current process."""
    raw = os.getenv("CQ_ENABLE_LSP")
    if raw is None:
        return True
    return raw.strip().lower() not in {"0", "false", "no", "off"}

def infer_language_for_path(file_path: Path) -> QueryLanguage | None:
    """Infer CQ language from file suffix."""
    if file_path.suffix in {".py", ".pyi"}:
        return "python"
    if file_path.suffix == ".rs":
        return "rust"
    return None

def provider_for_language(language: QueryLanguage | str) -> LspProvider:
    """Map CQ language to canonical LSP provider id."""
    if language == "python":
        return "pyrefly"
    if language == "rust":
        return "rust_analyzer"
    return "none"

def enrich_with_language_lsp(
    request: LanguageLspEnrichmentRequest,
) -> tuple[dict[str, object] | None, bool]:
    """Return language-appropriate LSP payload and timeout marker."""
    if not lsp_runtime_enabled():
        return None, False
    budget = budget_for_mode(request.mode)
    cache = get_cq_cache_backend(root=request.root)
    cache_key = _cache_key_for_request(request)
    cached = cache.get(cache_key)
    if isinstance(cached, dict):
        return dict(cached), False

    if request.language == "python":
        py_request = PyreflyLspRequest(...)
        payload, timed_out = call_with_retry(
            lambda: enrich_with_pyrefly_lsp(py_request),
            max_attempts=budget.max_attempts,
            retry_backoff_ms=budget.retry_backoff_ms,
        )
        if isinstance(payload, dict):
            cache.set(cache_key, payload, expire=..., tag=...)
            return payload, timed_out
        return None, timed_out

    # Rust LSP path...
    rust_request = RustLspRequest(...)
    payload, timed_out = call_with_retry(
        lambda: enrich_with_rust_lsp(rust_request, ...),
        max_attempts=budget.max_attempts,
        retry_backoff_ms=budget.retry_backoff_ms,
    )
    # ... (lines 151-179)
```

**Cache Key Construction:**
```python
def _cache_key_for_request(request: LanguageLspEnrichmentRequest) -> str:
    return build_cache_key(
        "lsp_front_door",
        version="v2",
        workspace=str(request.root.resolve()),
        language=request.language,
        target=str(request.file_path),
        extras={
            "mode": request.mode,
            "line": max(1, int(request.line)),
            "col": max(0, int(request.col)),
            "symbol_hint": request.symbol_hint or "",
            "mtime_ns": _safe_file_mtime_ns(request.file_path),
        },
    )
```

**Design:** Cache key includes file mtime to invalidate on file changes.

### LSP Request Budget

Defined in `tools/cq/search/lsp_request_budget.py`:

```python
class LspRequestBudgetV1(CqStruct, frozen=True):
    """Timeout and retry budget for one LSP request envelope."""
    startup_timeout_seconds: float = 3.0
    probe_timeout_seconds: float = 1.0
    max_attempts: int = 2
    retry_backoff_ms: int = 100

def budget_for_mode(mode: str) -> LspRequestBudgetV1:
    """Return standard budget profile by CQ command mode."""
    if mode == "calls":
        return LspRequestBudgetV1(
            startup_timeout_seconds=2.5,
            probe_timeout_seconds=1.25,
            max_attempts=2,
            retry_backoff_ms=120,
        )
    if mode == "entity":
        return LspRequestBudgetV1(
            startup_timeout_seconds=3.0,
            probe_timeout_seconds=1.25,
            max_attempts=2,
            retry_backoff_ms=120,
        )
    return LspRequestBudgetV1(
        startup_timeout_seconds=3.0,
        probe_timeout_seconds=1.0,
        max_attempts=2,
        retry_backoff_ms=100,
    )

def call_with_retry(
    fn: Callable[[], object],
    *,
    max_attempts: int,
    retry_backoff_ms: int,
) -> tuple[object | None, bool]:
    """Call a function with timeout-only retry semantics."""
    timed_out = False
    attempts = max(1, int(max_attempts))
    backoff_ms = max(0, int(retry_backoff_ms))
    for attempt in range(attempts):
        try:
            return fn(), timed_out
        except TimeoutError:
            timed_out = True
            if attempt + 1 >= attempts:
                return None, timed_out
            if backoff_ms > 0:
                time.sleep((backoff_ms / 1000.0) * (attempt + 1))
        except (OSError, RuntimeError, ValueError, TypeError):
            return None, timed_out
    return None, timed_out
```

**Retry Policy:**
- Only retries on `TimeoutError`
- Non-timeout exceptions fail immediately (fail-open)
- Linear backoff: `backoff_ms * (attempt + 1)`
- Returns `(result, timed_out)` tuple

**Mode-Specific Budgets:**

| Mode | Startup Timeout | Probe Timeout | Max Attempts | Retry Backoff |
|------|----------------|---------------|--------------|---------------|
| `search` | 3.0s | 1.0s | 2 | 100ms |
| `calls` | 2.5s | 1.25s | 2 | 120ms |
| `entity` | 3.0s | 1.25s | 2 | 120ms |

### LSP Session Manager

Defined in `tools/cq/search/lsp/session_manager.py`:

```python
class LspSessionManager[SessionT]:
    """Thread-safe root-keyed session cache with restart-on-failure helper."""

    def __init__(
        self,
        *,
        make_session: Callable[[Path], SessionT],
        close_session: Callable[[SessionT], None],
        ensure_started: Callable[[SessionT, float], None],
    ) -> None:
        self._make_session = make_session
        self._close_session = close_session
        self._ensure_started = ensure_started
        self._lock = threading.Lock()
        self._sessions: dict[str, SessionT] = {}

    def for_root(self, root: Path, *, startup_timeout_seconds: float) -> SessionT:
        """Get or start session for workspace root."""
        root_key = str(root.resolve())
        with self._lock:
            session = self._sessions.get(root_key)
            if session is None:
                session = self._make_session(root)
                self._sessions[root_key] = session
            try:
                self._ensure_started(session, startup_timeout_seconds)
            except (OSError, RuntimeError, TimeoutError, ValueError, TypeError):
                self._close_session(session)
                session = self._make_session(root)
                self._sessions[root_key] = session
                self._ensure_started(session, startup_timeout_seconds)
            return session

    def close_all(self) -> None:
        """Close all sessions and clear cache."""
        with self._lock:
            sessions = list(self._sessions.values())
            self._sessions.clear()
        for session in sessions:
            self._close_session(session)
```

**Restart-on-Failure Logic:**
1. Attempt to ensure session started
2. On exception, close failed session
3. Create fresh session and retry `ensure_started()`
4. Second failure propagates to caller

**Usage:**
- Pyrefly LSP sessions keyed by workspace root
- Rust-analyzer LSP sessions keyed by workspace root

### LSP Capabilities

Defined in `tools/cq/search/lsp/capabilities.py`:

```python
_METHOD_TO_CAPABILITY: dict[str, str] = {
    "textDocument/inlayHint": "inlayHintProvider",
    "textDocument/diagnostic": "diagnosticProvider",
    "workspace/diagnostic": "workspaceDiagnosticProvider",
    "textDocument/codeAction": "codeActionProvider",
    "textDocument/definition": "definitionProvider",
    "textDocument/typeDefinition": "typeDefinitionProvider",
    "textDocument/implementation": "implementationProvider",
    "textDocument/references": "referencesProvider",
    "textDocument/documentSymbol": "documentSymbolProvider",
    "textDocument/hover": "hoverProvider",
    "textDocument/prepareCallHierarchy": "callHierarchyProvider",
    "typeHierarchy/supertypes": "typeHierarchyProvider",
    "workspace/symbol": "workspaceSymbolProvider",
}

def supports_method(capabilities: Mapping[str, object], method: str) -> bool:
    """Return whether server capabilities indicate method support."""
    if method == "textDocument/semanticTokens/range":
        tokens = capabilities.get("semanticTokensProvider")
        return isinstance(tokens, Mapping)
    capability_field = _METHOD_TO_CAPABILITY.get(method)
    if capability_field is None:
        return False
    # ... special cases for diagnostic providers
    return bool(capabilities.get(capability_field))

def coerce_capabilities(payload: object) -> dict[str, object]:
    """Normalize capability payload to dictionary form."""
    if isinstance(payload, Mapping):
        return {str(key): value for key, value in payload.items()}
    return {}
```

**Capability Gating:**
- Advanced LSP planes check capabilities before requesting methods
- Prevents protocol errors when server doesn't support a method
- Used by semantic overlays, diagnostics, code actions, etc.

### LSP Status Derivation

Defined in `tools/cq/search/lsp/status.py`:

```python
class LspStatus(StrEnum):
    """Canonical LSP status values for front-door degradation."""
    unavailable = "unavailable"
    skipped = "skipped"
    failed = "failed"
    partial = "partial"
    ok = "ok"

class LspStatusTelemetry(CqStruct, frozen=True):
    """Typed telemetry for deterministic LSP state derivation."""
    available: bool = False
    attempted: int = 0
    applied: int = 0
    failed: int = 0
    timed_out: int = 0

def derive_lsp_status(telemetry: LspStatusTelemetry) -> LspStatus:
    """Derive canonical LSP status from attempt/apply counters."""
    if not telemetry.available:
        return LspStatus.unavailable
    if telemetry.attempted <= 0:
        return LspStatus.skipped
    if telemetry.applied <= 0:
        return LspStatus.failed
    if telemetry.failed > 0 or telemetry.applied < telemetry.attempted:
        return LspStatus.partial
    return LspStatus.ok
```

**Status Classification:**
- Used by front-door insight rendering
- Determines degradation messaging in output
- Feeds into confidence scoring

### LSP Request Queue

Defined in `tools/cq/search/lsp/request_queue.py`:

```python
def run_lsp_requests(
    callables: Iterable[Callable[[], object]],
    *,
    timeout_seconds: float,
) -> tuple[list[object], int]:
    """Execute request callables in shared IO pool with bounded timeout."""
    scheduler = get_worker_scheduler()
    futures: list[Future[object]] = [
        scheduler.submit_io(callable_item) for callable_item in callables
    ]
    batch = scheduler.collect_bounded(futures, timeout_seconds=timeout_seconds)
    return batch.done, batch.timed_out
```

**Integration:**
- Used by semantic overlays, diagnostics, code actions
- Leverages shared `WorkerScheduler` I/O pool
- Returns completed payloads + timeout count

---

## Diagnostic Contracts

Defined in `tools/cq/core/diagnostics_contracts.py`:

```python
class DiagnosticsArtifactRunMetaV1(CqStruct, frozen=True):
    """Run metadata persisted with offloaded diagnostics payloads."""
    macro: str
    root: str
    run_id: str | None = None

class DiagnosticsArtifactPayloadV1(CqStruct, frozen=True):
    """Typed diagnostics payload for artifact-first rendering."""
    run_meta: DiagnosticsArtifactRunMetaV1
    enrichment_telemetry: dict[str, object] = msgspec.field(default_factory=dict)
    pyrefly_telemetry: dict[str, object] = msgspec.field(default_factory=dict)
    rust_lsp_telemetry: dict[str, object] = msgspec.field(default_factory=dict)
    lsp_advanced_planes: dict[str, object] = msgspec.field(default_factory=dict)
    pyrefly_diagnostics: list[dict[str, object]] = msgspec.field(default_factory=list)
    language_capabilities: dict[str, object] = msgspec.field(default_factory=dict)
    cross_language_diagnostics: list[dict[str, object]] = msgspec.field(
        default_factory=list
    )

def build_diagnostics_artifact_payload(
    result: CqResult
) -> DiagnosticsArtifactPayloadV1 | None:
    """Build typed diagnostics artifact payload from result summary."""
    summary = result.summary
    if not isinstance(summary, dict):
        return None
    payload = DiagnosticsArtifactPayloadV1(
        run_meta=DiagnosticsArtifactRunMetaV1(
            macro=result.run.macro,
            root=result.run.root,
            run_id=result.run.run_id,
        ),
        enrichment_telemetry=_coerce_dict(summary.get("enrichment_telemetry")),
        pyrefly_telemetry=_coerce_dict(summary.get("pyrefly_telemetry")),
        rust_lsp_telemetry=_coerce_dict(summary.get("rust_lsp_telemetry")),
        lsp_advanced_planes=_coerce_dict(summary.get("lsp_advanced_planes")),
        pyrefly_diagnostics=_coerce_list_of_dict(
            summary.get("pyrefly_diagnostics")
        ),
        language_capabilities=_coerce_dict(summary.get("language_capabilities")),
        cross_language_diagnostics=_coerce_list_of_dict(
            summary.get("cross_language_diagnostics")
        ),
    )
    has_data = any(
        (
            payload.enrichment_telemetry,
            payload.pyrefly_telemetry,
            payload.rust_lsp_telemetry,
            payload.lsp_advanced_planes,
            payload.pyrefly_diagnostics,
            payload.language_capabilities,
            payload.cross_language_diagnostics,
        )
    )
    if not has_data:
        return None
    return payload
```

**Payload Fields:**
- `enrichment_telemetry` - Enrichment pipeline stats (5 stages)
- `pyrefly_telemetry` - Pyrefly LSP session telemetry
- `rust_lsp_telemetry` - Rust-analyzer LSP session telemetry
- `lsp_advanced_planes` - Advanced LSP plane stats (semantic tokens, inlay hints, diagnostics, code actions)
- `pyrefly_diagnostics` - Pyrefly-reported diagnostics
- `language_capabilities` - LSP server capability snapshots
- `cross_language_diagnostics` - Cross-language diagnostic aggregation

**Usage:**
- Extracted from `CqResult.summary` dictionary
- Persisted as JSON artifact for large result sets
- Rendered separately from main output to reduce noise

---

## Performance Smoke Tests

Defined in `tools/cq/perf/smoke_report.py`:

```python
class PerfMeasurement(msgspec.Struct, frozen=True):
    """One benchmark timing sample."""
    first_run_ms: float
    second_run_ms: float
    speedup_ratio: float

class PerfSmokeReport(msgspec.Struct, frozen=True):
    """Serialized performance smoke report payload."""
    generated_at_epoch_s: float
    workspace: str
    search_python: PerfMeasurement
    calls_python: PerfMeasurement
    query_entity_auto: PerfMeasurement

def build_perf_smoke_report(*, workspace: Path) -> PerfSmokeReport:
    """Run representative CQ commands twice and return timing report."""
    tc = Toolchain.detect()

    search_measurement = _measure_pair(
        lambda: smart_search(
            root=workspace,
            query="build_graph",
            lang_scope="python",
            tc=tc,
            argv=[],
        )
    )
    calls_measurement = _measure_pair(
        lambda: cmd_calls(
            tc=tc,
            root=workspace,
            argv=[],
            function_name="build_graph",
        )
    )
    query_obj = parse_query("entity=function name=build_graph lang=auto")
    query_plan = compile_query(query_obj)
    query_measurement = _measure_pair(
        lambda: execute_plan(
            query_plan,
            query_obj,
            tc=tc,
            root=workspace,
            argv=[],
        )
    )

    return PerfSmokeReport(
        generated_at_epoch_s=time.time(),
        workspace=str(workspace),
        search_python=search_measurement,
        calls_python=calls_measurement,
        query_entity_auto=query_measurement,
    )
```

**Measurement Logic:**
```python
def _time_call(fn: Callable[[], object], /) -> float:
    start = time.perf_counter()
    fn()
    return (time.perf_counter() - start) * 1000.0

def _measure_pair(fn: Callable[[], object], /) -> PerfMeasurement:
    first = _time_call(fn)
    second = _time_call(fn)
    ratio = (first / second) if second > 0 else 0.0
    return PerfMeasurement(
        first_run_ms=first,
        second_run_ms=second,
        speedup_ratio=ratio,
    )
```

**Speedup Ratio:**
- `first_run_ms / second_run_ms`
- Measures cache effectiveness (LSP sessions, disk cache, def index)
- Expected ratio > 2.0 for cache-heavy operations

**CLI Usage:**
```bash
uv run python -m tools.cq.perf.smoke_report \
    --workspace tests/e2e/cq/_golden_workspace/python_project \
    --output build/perf/cq_smoke_report.json
```

**Output Example:**
```json
{
  "generated_at_epoch_s": 1707766800.0,
  "workspace": "/path/to/workspace",
  "search_python": {
    "first_run_ms": 1250.5,
    "second_run_ms": 450.2,
    "speedup_ratio": 2.78
  },
  "calls_python": {
    "first_run_ms": 980.3,
    "second_run_ms": 320.1,
    "speedup_ratio": 3.06
  },
  "query_entity_auto": {
    "first_run_ms": 1100.7,
    "second_run_ms": 410.4,
    "speedup_ratio": 2.68
  }
}
```

---

## Integration Points

### Search Pipeline Integration

```python
# tools/cq/search/smart_search.py
from tools.cq.core.cache import get_cq_cache_backend, build_cache_key, build_cache_tag
from tools.cq.core.runtime.worker_scheduler import get_worker_scheduler
from tools.cq.search.lsp_front_door_adapter import enrich_with_language_lsp

def smart_search(...) -> CqResult:
    # 1. Get workspace-keyed cache backend
    cache = get_cq_cache_backend(root=root)

    # 2. Get worker scheduler for parallel classification
    scheduler = get_worker_scheduler()

    # 3. Submit classification tasks to CPU pool
    futures = [
        scheduler.submit_cpu(classify_match, match)
        for match in raw_matches
    ]

    # 4. Collect with timeout
    batch = scheduler.collect_bounded(
        futures,
        timeout_seconds=classification_timeout,
    )

    # 5. LSP enrichment via front-door adapter
    for target in top_targets:
        lsp_request = LanguageLspEnrichmentRequest(
            language="python",
            mode="search",
            root=root,
            file_path=target.file_path,
            line=target.line,
            col=target.col,
        )
        payload, timed_out = enrich_with_language_lsp(lsp_request)
        # ... merge LSP data into result
```

### Query/Entity Integration

```python
# tools/cq/query/entity_front_door.py
from tools.cq.core.cache import get_cq_cache_backend, build_cache_key
from tools.cq.search.lsp_front_door_adapter import enrich_with_language_lsp

def attach_entity_front_door_insight(result: CqResult, ...) -> None:
    cache = get_cq_cache_backend(root=Path(result.run.root))

    # Cache entity scan results
    for partition in partitions:
        cache_key = build_cache_key(
            "query_entity_scan",
            version="v1",
            workspace=result.run.root,
            language=partition.language,
            target=partition.pattern,
        )
        cached = cache.get(cache_key)
        if cached is not None:
            partition.records = cached.records
        else:
            # ... run entity scan
            cache.set(
                cache_key,
                QueryEntityScanCacheV1(records=partition.records),
                expire=900,
            )

    # LSP enrichment for top entities
    for entity in top_entities:
        lsp_request = LanguageLspEnrichmentRequest(...)
        payload, timed_out = enrich_with_language_lsp(lsp_request)
        # ... attach LSP data to front-door insight
```

### Calls Macro Integration

```python
# tools/cq/macros/calls.py
from tools.cq.core.cache import get_cq_cache_backend, build_cache_key
from tools.cq.search.lsp_front_door_adapter import enrich_with_language_lsp

def cmd_calls(...) -> CqResult:
    cache = get_cq_cache_backend(root=root)

    # Cache target resolution
    target_cache_key = build_cache_key(
        "calls_target",
        version="v1",
        workspace=str(root),
        language="python",
        target=function_name,
    )
    cached_target = cache.get(target_cache_key)
    if isinstance(cached_target, CallsTargetCacheV1):
        target_location = cached_target.target_location
        target_callees = cached_target.target_callees
    else:
        # ... resolve target
        cache.set(
            target_cache_key,
            CallsTargetCacheV1(
                target_location=target_location,
                target_callees=target_callees,
            ),
            expire=900,
        )

    # LSP enrichment for target definition
    if target_location:
        lsp_request = LanguageLspEnrichmentRequest(
            language="python",
            mode="calls",
            root=root,
            file_path=Path(target_location[0]),
            line=target_location[1],
            col=0,
            symbol_hint=function_name,
        )
        payload, timed_out = enrich_with_language_lsp(lsp_request)
        # ... merge LSP data into result
```

### Run/Chain Integration

```python
# tools/cq/run/runner.py
from tools.cq.core.bootstrap import resolve_runtime_services

def execute_run_plan(...) -> CqResult:
    services = resolve_runtime_services(root=root)

    for step in plan.steps:
        if step.type == "search":
            step_result = services.search.execute(
                SearchServiceRequest(
                    root=root,
                    query=step.query,
                    lang_scope=step.lang_scope,
                    tc=tc,
                    argv=argv,
                )
            )
        elif step.type == "calls":
            step_result = services.calls.execute(
                CallsServiceRequest(
                    root=root,
                    function_name=step.function_name,
                    tc=tc,
                    argv=argv,
                )
            )
        elif step.type == "entity":
            # Execute entity query, then attach front door
            step_result = execute_entity_query(...)
            services.entity.attach_front_door(
                EntityFrontDoorRequest(
                    result=step_result,
                    relationship_detail_max_matches=50,
                )
            )
        # ... merge step results
```

---

## Design Tensions and Improvement Vectors

### Current Limitations

#### 1. Cache Invalidation Granularity

**Issue:** Tag-based eviction is coarse-grained (workspace + language level).

**Current State:**
```python
tag = build_cache_tag(workspace=str(root), language="python")
cache.evict_tag(tag)  # Evicts ALL Python cache entries for workspace
```

**Improvement Vector:**
- Add file-level tags: `{workspace}:{language}:{file_hash}`
- Add function-level tags: `{workspace}:{language}:{function_signature_hash}`
- Implement cache versioning with automatic migration

#### 2. LSP Session Lifecycle Management

**Issue:** LSP sessions persist for process lifetime, even when idle.

**Current State:**
- Sessions created on first use
- Kept alive until `atexit` cleanup
- No idle timeout or health monitoring

**Improvement Vector:**
- Add idle timeout (e.g., 5 minutes of no requests)
- Add periodic health checks with automatic restart
- Add session pool size limits per workspace

#### 3. Worker Pool Sizing Strategy

**Issue:** Static pool sizes based on CPU count don't adapt to workload.

**Current State:**
```python
cpu_workers = max(1, cpu_count - 1)
io_workers = max(8, cpu_count)
```

**Improvement Vector:**
- Add adaptive pool sizing based on queue depth
- Add per-command pool size profiles (search vs calls vs query)
- Add pool saturation metrics for tuning

#### 4. Cache Hit Rate Observability

**Issue:** No visibility into cache effectiveness.

**Current State:**
- Cache hits/misses not tracked
- No cache size monitoring
- No eviction rate metrics

**Improvement Vector:**
- Add `CacheMetrics` struct with hit/miss/evict counters
- Expose metrics via `--verbose` or diagnostic artifacts
- Add cache size limits with LRU eviction policy

#### 5. LSP Retry Logic Limitations

**Issue:** Only retries on timeout, not on transient errors.

**Current State:**
```python
except TimeoutError:
    timed_out = True
    # ... retry
except (OSError, RuntimeError, ValueError, TypeError):
    return None, timed_out  # Fail immediately
```

**Improvement Vector:**
- Add retry classification (transient vs permanent errors)
- Add exponential backoff option
- Add circuit breaker for repeatedly failing sessions

#### 6. Service Layer Abstractions

**Issue:** Services are thin wrappers with minimal abstraction value.

**Current State:**
```python
class SearchService:
    @staticmethod
    def execute(request: SearchServiceRequest) -> CqResult:
        return smart_search(...)  # Direct delegation
```

**Improvement Vector:**
- Add service middleware for cross-cutting concerns (logging, metrics, tracing)
- Add service composition helpers for multi-step workflows
- Add request/response interceptors for diagnostics

#### 7. Diagnostic Artifact Size Management

**Issue:** Diagnostic payloads can grow unbounded for large result sets.

**Current State:**
```python
pyrefly_diagnostics: list[dict[str, object]] = msgspec.field(default_factory=list)
```

**Improvement Vector:**
- Add diagnostic sampling (top N errors by file/severity)
- Add diagnostic deduplication by error signature
- Add diagnostic compression for artifact storage

#### 8. Performance Benchmarking Coverage

**Issue:** Smoke tests only measure 3 commands on small workspace.

**Current State:**
- Tests: search, calls, entity query
- Workspace: Small golden workspace fixture
- Metrics: First/second run times only

**Improvement Vector:**
- Add benchmark suite for all commands (neighborhood, run, chain, ldmd)
- Add large workspace benchmarks (10k+ files)
- Add memory profiling and GC metrics
- Add percentile latency tracking (p50, p90, p99)

---

## Summary

The CQ runtime services tier provides a unified infrastructure layer for execution policy management, persistent caching, worker scheduling, and service composition. Key design principles:

1. **Fail-Open Semantics** - All caching and LSP operations degrade gracefully on errors
2. **Workspace-Keyed Singletons** - Cache backends, service bundles, and LSP sessions are keyed by resolved workspace path
3. **Environment Override System** - All policies support `CQ_RUNTIME_*` environment variables for tuning
4. **Dual-Pool Worker Architecture** - Separate CPU (ProcessPool/spawn) and I/O (ThreadPool) pools with bounded collection
5. **Contract-First Design** - All requests, responses, and cache payloads use msgspec structs
6. **LSP Runtime Infrastructure** - Comprehensive LSP lifecycle management with contract state, budgets, sessions, and capabilities
7. **Composition Root Pattern** - Central `CqRuntimeServices` bundle wires all dependencies

**File Locations:**
- Runtime policy: `tools/cq/core/runtime/execution_policy.py`
- Worker scheduler: `tools/cq/core/runtime/worker_scheduler.py`
- Cache backend: `tools/cq/core/cache/diskcache_backend.py`
- Service layer: `tools/cq/core/services/`, `tools/cq/core/ports.py`
- Composition root: `tools/cq/core/bootstrap.py`
- LSP infrastructure: `tools/cq/search/lsp_*.py`, `tools/cq/search/lsp/`
- Diagnostics: `tools/cq/core/diagnostics_contracts.py`
- Performance tests: `tools/cq/perf/smoke_report.py`

This layer enables CQ commands to focus on domain logic while delegating concurrency, caching, and LSP lifecycle management to reusable infrastructure.
