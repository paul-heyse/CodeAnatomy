# Delta-Backed Cache Alignment Plan

This plan extends the existing Delta-backed caching patterns (view caches + extract outputs)
to the remaining in-memory and key-value cache surfaces so caches are:
1) persisted as Parquet/Delta tables,
2) registered for DataFusion introspection, and
3) included in planning/diagnostics workflows.

The goal is to keep fast local caching **and** make caches observable and reproducible via Delta.

---

## Scope A — Delta-Backed Dataset Registration Cache

**Goal:** Replace the in-memory `df.cache()` registration cache with a Delta-backed cache
that is introspectable and stable across sessions when desired.

### Representative implementation

```python
# src/datafusion_engine/dataset/registration.py (UPDATE)
@dataclass(frozen=True)
class DataFusionCachePolicy(FingerprintableConfig):
    enabled: bool | None = None
    max_columns: int | None = None
    storage: Literal["memory", "delta_staging"] = "memory"


def _maybe_cache(context: DataFusionRegistrationContext, df: DataFrame) -> DataFrame:
    if not context.cache.enabled or not _should_cache_df(df, cache_max_columns=context.cache.max_columns):
        return df
    if context.cache.storage == "delta_staging":
        return _register_delta_cache_for_dataset(context, df)
    return _register_memory_cache(context, df)
```

```python
# src/datafusion_engine/dataset/registration.py (NEW helper)
def _register_delta_cache_for_dataset(
    context: DataFusionRegistrationContext,
    df: DataFrame,
) -> DataFrame:
    cache_root = _cache_root_from_profile(context.runtime_profile)
    cache_key = table_spec_from_location(context.name, context.location).cache_key()
    cache_path = str(Path(cache_root) / f"{context.name}__{cache_key}")
    pipeline = WritePipeline(context.ctx, runtime_profile=context.runtime_profile)
    pipeline.write(
        WriteRequest(
            source=df,
            destination=cache_path,
            format=WriteFormat.DELTA,
            mode=WriteMode.OVERWRITE,
        )
    )
    register_cached_delta_table(
        context.ctx,
        context.runtime_profile,
        name=context.name,
        location=DatasetLocation(path=cache_path, format="delta"),
        snapshot_version=None,
    )
    return context.ctx.table(context.name)
```

### Target files
- `src/datafusion_engine/dataset/registration.py` (cache policy + delta cache path)
- `src/datafusion_engine/session/runtime.py` (cache root configuration; diagnostics)
- `src/datafusion_engine/cache/registry.py` (reuse inventory for dataset caches)

### Deprecate/delete after completion
- None (replace in-memory cache path with a Delta-backed mode)

### Implementation checklist
- [ ] Add `storage` to `DataFusionCachePolicy` (default `memory`)
- [ ] Add runtime profile cache root setting (e.g., `cache_output_root`)
- [ ] Implement Delta-backed dataset cache registration path
- [ ] Record cache inventory entries for dataset caches
- [ ] Add tests covering delta cache registration + lookup

---

## Scope B — Optional Delta Persistence for RuntimeArtifacts

**Goal:** Persist large intermediate tables held in `RuntimeArtifacts.materialized_tables`
to Delta when a cache policy is enabled, keeping only references in memory.

### Representative implementation

```python
# src/relspec/runtime_artifacts.py (UPDATE)
def register_execution(
    self,
    name: str,
    result: ExecutionResult,
    *,
    spec: ExecutionArtifactSpec,
) -> None:
    if self.execution is not None and self.execution.profile.cache_enabled:
        location = _runtime_artifact_location(self.execution.profile, name, spec.plan_task_signature)
        _write_execution_result_to_delta(self.execution.profile, name, result, location)
        spec = replace(spec, storage_path=str(location.path))
    self.execution_artifacts[name] = ExecutionArtifact(name=name, result=result, spec=spec)
```

### Target files
- `src/relspec/runtime_artifacts.py` (persist + store storage_path)
- `src/datafusion_engine/session/runtime.py` (artifact cache root + policy)
- `src/engine/materialize_pipeline.py` (shared Delta write helpers)

### Deprecate/delete after completion
- None (keep in-memory path as fallback for small tables)

### Implementation checklist
- [ ] Add runtime artifact cache root and policy toggle
- [ ] Write ExecutionResult tables/readers to Delta when enabled
- [ ] Store Delta path in `ExecutionArtifactSpec.storage_path`
- [ ] Register cached artifact table with DataFusion session
- [ ] Add tests for delta persistence toggle

---

## Scope C — Snapshot DataFusion Metadata Caches into Delta

**Goal:** Provide an introspectable Delta snapshot of DataFusion’s in-memory caches
(`metadata_cache`, `statistics_cache`, `list_files_cache`) for planning diagnostics.

### Representative implementation

```python
# src/datafusion_engine/cache/metadata_snapshots.py (NEW)
def snapshot_datafusion_caches(
    ctx: SessionContext,
    *,
    runtime_profile: DataFusionRuntimeProfile,
) -> None:
    for name, sql in {
        "df_metadata_cache": "SELECT * FROM metadata_cache()",
        "df_statistics_cache": "SELECT * FROM statistics_cache()",
        "df_list_files_cache": "SELECT * FROM list_files_cache()",
    }.items():
        df = ctx.sql(sql)
        location = _cache_snapshot_location(runtime_profile, name)
        WritePipeline(ctx, runtime_profile=runtime_profile).write(
            WriteRequest(source=df, destination=str(location.path), format=WriteFormat.DELTA)
        )
        register_dataset_df(ctx, name=name, location=location, runtime_profile=runtime_profile)
```

### Target files
- `src/datafusion_engine/cache/metadata_snapshots.py` (new snapshot helper)
- `src/datafusion_engine/session/runtime.py` (hook on diagnostics capture)
- `src/datafusion_engine/cache/registry.py` (optional inventory of snapshots)

### Deprecate/delete after completion
- None (additive diagnostic layer)

### Implementation checklist
- [ ] Create Delta snapshot helper for metadata/list/stats caches
- [ ] Add runtime profile toggle to enable/disable snapshots
- [ ] Register snapshot tables in DataFusion for introspection
- [ ] Record snapshot inventory + version in diagnostics artifacts

---

## Scope D — Unified Cache Root Configuration

**Goal:** Centralize all Delta-backed cache paths (view caches, dataset caches,
runtime artifact caches, metadata snapshots) under a single runtime profile root.

### Representative implementation

```python
# src/datafusion_engine/session/runtime.py (UPDATE)
cache_output_root: str | None = None

def cache_root(self) -> str:
    if self.cache_output_root is not None:
        return self.cache_output_root
    return str(Path(tempfile.gettempdir()) / "datafusion_cache")
```

### Target files
- `src/datafusion_engine/session/runtime.py` (cache root config + helpers)
- `src/datafusion_engine/views/graph.py` (use shared cache root for delta_staging)
- `src/datafusion_engine/dataset/registration.py` (cache root for dataset caches)
- `src/hamilton_pipeline/modules/inputs.py` (wire config from run settings)

### Deprecate/delete after completion
- Temporary per-module cache roots (e.g., `datafusion_view_cache`) once unified.

### Implementation checklist
- [ ] Add `cache_output_root` to runtime profile + config plumbing
- [ ] Route delta_staging cache path through cache root
- [ ] Route dataset cache path through cache root
- [ ] Add one-liner diagnostic payload for cache root in run artifacts

---

## Implementation Order

1. **Scope D** — Unified cache root configuration  
2. **Scope A** — Delta-backed dataset registration cache  
3. **Scope B** — Runtime artifact persistence  
4. **Scope C** — Metadata cache snapshots  

---

## Success Criteria

1. No large in-memory caches remain when Delta caching is enabled.
2. All cache tables are discoverable via DataFusion catalogs.
3. Cache inventory includes Delta versions + schema hashes.
4. Planning diagnostics can query cache tables via SQL.
5. Cache root can be configured deterministically per run.
