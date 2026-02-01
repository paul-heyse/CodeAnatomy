# Engine Semantic Alignment Plan

> **Goal**: Align `src/engine` with the semantic-first architecture in `src/semantics`, consolidating overlapping patterns, clarifying the execution/definition boundary, and removing legacy code paths.

---

## Executive Summary

| Module | Files | Lines | Current Role | Migration Target |
|--------|-------|-------|--------------|------------------|
| `runtime_profile.py` | 1 | ~489 | Profile resolution + Hamilton telemetry | **Simplify** - Extract telemetry, consume SemanticRuntimeConfig |
| `plan_policy.py` | 1 | ~49 | ExecutionSurfacePolicy | **Relocate** - Move to `datafusion_engine.materialize_policy` |
| `materialize_pipeline.py` | 1 | ~546 | View materialization + CachePolicy | **Refactor** - Remove CachePolicy, use semantic patterns |
| `plan_cache.py` | 1 | ~206 | Substrait plan cache | **Relocate** - Move to datafusion_engine |
| `session.py` | 1 | ~72 | EngineSession bundle | **Keep** - Execution authority |
| `session_factory.py` | 1 | ~105 | build_engine_session() | **Enhance** - Integrate SemanticRuntimeConfig |
| `runtime.py` | 1 | ~97 | EngineRuntime + builder | **Keep** - Clean execution wrapper |
| `plan_product.py` | 1 | ~74 | PlanProduct wrapper | **Keep** - Clean output wrapper |
| `delta_tools.py` | 1 | ~356 | Delta maintenance ops | **Keep** - Utility functions |

**Key Insights:**
1. **Engine is execution-only authority** - Engine orchestrates DataFusion execution; semantics owns definitions
2. **Policy fragmentation exists** - Multiple `CachePolicy` types and policy classes need consolidation
3. **Plan cache is mislocated** - Substrait-specific caching belongs in `datafusion_engine`
4. **Hamilton telemetry is engine-specific** - Can be extracted into dedicated module
5. **Session factory should leverage SemanticRuntimeConfig** - Reduce DataFusion profile coupling
6. **No raw SQL in engine integration** - Use DataFusion DataFrame/Expr APIs exclusively
7. **Design-phase migration** - Remove compatibility shims and legacy fallbacks immediately

---

## Architecture Overview

### Legacy State (Pre-Migration)

```
src/engine/
├── runtime_profile.py      ← RuntimeProfileSpec, Hamilton telemetry (MIXED CONCERNS)
├── plan_policy.py          ← ExecutionSurfacePolicy (OVERLAPS with semantic CachePolicy)
├── materialize_pipeline.py ← build_view_product(), CachePolicy class (TYPE COLLISION)
├── plan_cache.py           ← PlanCache, Substrait caching (MISLOCATED)
├── session.py              ← EngineSession (CLEAN)
├── session_factory.py      ← build_engine_session() (NEEDS SemanticRuntimeConfig)
├── runtime.py              ← EngineRuntime, build_engine_runtime() (CLEAN)
├── plan_product.py         ← PlanProduct (CLEAN)
└── delta_tools.py          ← Delta maintenance (CLEAN)

src/semantics/
├── runtime.py              ← SemanticRuntimeConfig, CachePolicy type alias (DEFINITION AUTHORITY)
└── pipeline.py             ← build_cpg(), _default_semantic_cache_policy() (SEMANTIC LOGIC)

src/relspec/
└── pipeline_policy.py      ← DiagnosticsPolicy, PipelinePolicy (BRIDGE)
```

**Problems:**
1. `engine/materialize_pipeline.py` defines `CachePolicy` class while `semantics/runtime.py` defines `CachePolicy` type alias - name collision
2. `RuntimeProfileSpec` handles both DataFusion profiles AND Hamilton telemetry - mixed concerns
3. `PlanCache` in engine/ but operates on Substrait (DataFusion-specific artifacts)
4. No direct integration between engine and `SemanticRuntimeConfig`

### Current State (Post-Implementation)

```
src/engine/
├── runtime_profile.py      ← RuntimeProfileSpec (DataFusion only) + telemetry link
├── materialize_pipeline.py ← view materialization + cache decisions
├── delta_tools.py          ← Delta maintenance + query helpers (DataFusion builder, no SQL)
├── diagnostics.py          ← EngineEventRecorder (wired in materialize_pipeline + delta_tools)
├── semantic_boundary.py    ← execution boundary guardrails (enforced for semantic views)
├── session.py              ← EngineSession
├── session_factory.py      ← build_engine_session() + semantic bridge
├── runtime.py              ← EngineRuntime wrapper
├── plan_product.py         ← PlanProduct wrapper
└── telemetry/
    └── hamilton.py         ← Hamilton telemetry config

src/datafusion_engine/
├── plan/
│   └── cache.py            ← PlanCache relocated
├── materialize_policy.py   ← MaterializationPolicy
└── semantics_runtime.py    ← semantic runtime bridge
```

### Target State (Clean Separation)

```
src/engine/
├── session.py              ← EngineSession (EXECUTION BUNDLE)
├── session_factory.py      ← build_engine_session() + SemanticRuntimeConfig adapter
├── runtime.py              ← EngineRuntime wrapper
├── runtime_profile.py      ← RuntimeProfileSpec (DataFusion only)
├── plan_product.py         ← PlanProduct wrapper
├── materialize_pipeline.py ← build_view_product() (no CachePolicy class)
├── delta_tools.py          ← Delta maintenance
├── diagnostics.py          ← EngineEventRecorder (shared diagnostics utilities)
└── telemetry/
    └── hamilton.py         ← HamiltonTrackerConfig, HamiltonTelemetryProfile

src/datafusion_engine/
├── plan/
│   └── cache.py            ← PlanCache, PlanCacheKey, PlanCacheEntry (RELOCATED)
├── materialize_policy.py   ← MaterializationPolicy (execution policy)
└── semantics_runtime.py    ← semantic_runtime_from_profile() (BRIDGE)

src/semantics/
├── runtime.py              ← SemanticRuntimeConfig, CachePolicy (DEFINITION AUTHORITY)
└── pipeline.py             ← build_cpg() (SEMANTIC LOGIC)
```

### Architecture Invariants (Enforced)

1. **Semantics defines views; engine executes** - Engine must not re-register semantic views.
2. **No raw SQL** - Engine integration uses DataFusion DataFrame/Expr APIs only.
3. **Semantic runtime is authoritative** - Engine consumes `SemanticRuntimeConfig` and resolved cache policy rather than re-deriving.
4. **No backward-compat shims** - Remove legacy aliases and deprecated stubs in design phase.

**Invariant Status Notes (Post-Implementation):**
- Invariant 1 enforced: semantic view materialization requires pre-registered views.
- Invariant 2 enforced: engine no longer uses `ctx.sql*` helpers for execution paths.
- Invariant 3 enforced: semantic cache policy inputs are threaded into cache decisions.
- Invariant 4 enforced: no `ExecutionSurfacePolicy` alias remains.

---

## Scope Index

1. **Scope 1** - Relocate PlanCache to DataFusion Engine
2. **Scope 2** - Extract Hamilton Telemetry Configuration
3. **Scope 3** - Remove CachePolicy Class Collision
4. **Scope 4** - Relocate and Rename ExecutionSurfacePolicy
5. **Scope 5** - Integrate SemanticRuntimeConfig in Session Factory
6. **Scope 6** - Consolidate Diagnostics Recording Helpers
7. **Scope 7** - Remove Deprecated build_plan_product Stub
8. **Scope 8** - Align Cache Policy Resolution with Semantic Runtime
9. **Scope 9** - Centralize Semantic Runtime Bridge in datafusion_engine
10. **Scope 10** - Enforce Engine/Semantics Execution Boundary

---

## Scope 1 - Relocate PlanCache to DataFusion Engine

### Status
**Completed (implementation; verification pending)**

### Goal
Move `PlanCache`, `PlanCacheKey`, and `PlanCacheEntry` from `engine/plan_cache.py` to `datafusion_engine/plan/cache.py` since these are Substrait-specific artifacts that belong with DataFusion infrastructure.

### Representative Pattern (Target)

```python
# src/datafusion_engine/plan/cache.py (RELOCATED)
"""Substrait plan cache for DataFusion replay."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from cache.diskcache_factory import (
    DiskCacheProfile,
    cache_for_kind,
    default_diskcache_profile,
)

if TYPE_CHECKING:
    from diskcache import Cache, FanoutCache


@dataclass(frozen=True)
class PlanCacheKey:
    """Cache key for Substrait plan bytes."""

    profile_hash: str
    substrait_hash: str
    plan_fingerprint: str
    udf_snapshot_hash: str
    function_registry_hash: str
    information_schema_hash: str
    required_udfs_hash: str
    required_rewrite_tags_hash: str
    settings_hash: str
    delta_inputs_hash: str

    def as_key(self) -> str:
        parts = (
            self.profile_hash,
            self.substrait_hash,
            self.plan_fingerprint,
            # ... remaining parts
        )
        return "plan:" + ":".join(parts)


@dataclass
class PlanCache:
    """DiskCache-backed cache of Substrait plan bytes."""

    cache_profile: DiskCacheProfile | None = field(default_factory=default_diskcache_profile)
    _cache: Cache | FanoutCache | None = field(default=None, init=False, repr=False)

    def get(self, key: PlanCacheKey) -> bytes | None:
        # ... existing implementation
        pass
```

### Target Files
- Create: `src/datafusion_engine/plan/cache.py`
- Delete: `src/engine/plan_cache.py`
- Modify: `src/datafusion_engine/session/runtime.py` (update import)
- Modify: `src/datafusion_engine/compile/options.py` (update import)

### Deletions
- Delete `src/engine/plan_cache.py` entirely (no compatibility shim in design phase)

### Implementation Checklist
- [x] Create `src/datafusion_engine/plan/cache.py` with relocated classes
- [x] Delete `src/engine/plan_cache.py` (no shim)
- [x] Update `datafusion_engine/session/runtime.py` import path
- [x] Update `datafusion_engine/compile/options.py` import path
- [ ] Run full test suite to verify no regressions

---

## Scope 2 - Extract Hamilton Telemetry Configuration

### Status
**Completed (implementation; verification pending)**

### Goal
Extract Hamilton-specific telemetry configuration from `runtime_profile.py` into a dedicated `telemetry/hamilton.py` module, separating orchestration concerns from runtime profile resolution. Align telemetry with OTel scopes and ensure telemetry modules do **not** import semantics.

### Representative Pattern (Current)

```python
# src/engine/runtime_profile.py (CURRENT - MIXED CONCERNS)

@dataclass(frozen=True)
class HamiltonTrackerConfig:
    """Tracker configuration sourced from runtime profile or environment."""
    project_id: int | None = None
    username: str | None = None
    dag_name: str | None = None
    api_url: str | None = None
    ui_url: str | None = None

@dataclass(frozen=True)
class HamiltonTelemetryProfile:
    """Telemetry profile for Hamilton tracker capture controls."""
    name: str
    enable_tracker: bool
    capture_data_statistics: bool
    max_list_length_capture: int
    max_dict_length_capture: int

def _tracker_config_from_env() -> HamiltonTrackerConfig | None:
    # ~25 lines of env var parsing
    pass

def _resolve_hamilton_telemetry_profile() -> HamiltonTelemetryProfile:
    # ~20 lines of profile resolution
    pass
```

### Representative Pattern (Target)

```python
# src/engine/telemetry/hamilton.py (NEW - DEDICATED MODULE)
"""Hamilton orchestration telemetry configuration."""

from __future__ import annotations

from dataclasses import dataclass

from utils.env_utils import env_bool, env_int, env_value


@dataclass(frozen=True)
class HamiltonTrackerConfig:
    """Tracker configuration sourced from runtime profile or environment."""

    project_id: int | None = None
    username: str | None = None
    dag_name: str | None = None
    api_url: str | None = None
    ui_url: str | None = None

    @property
    def enabled(self) -> bool:
        """Return True when tracker configuration is complete."""
        return self.project_id is not None and self.username is not None

    @classmethod
    def from_env(cls) -> HamiltonTrackerConfig | None:
        """Build tracker config from environment variables.

        Returns
        -------
        HamiltonTrackerConfig | None
            Tracker config when at least one env var is set.
        """
        project_id = env_int("CODEANATOMY_HAMILTON_PROJECT_ID")
        if project_id is None:
            project_id = env_int("HAMILTON_PROJECT_ID")
        username = env_value("CODEANATOMY_HAMILTON_USERNAME")
        if username is None:
            username = env_value("HAMILTON_USERNAME")
        # ... remaining env resolution
        if all(v is None for v in (project_id, username, dag_name, api_url, ui_url)):
            return None
        return cls(
            project_id=project_id,
            username=username,
            dag_name=dag_name,
            api_url=api_url,
            ui_url=ui_url,
        )


@dataclass(frozen=True)
class HamiltonTelemetryProfile:
    """Telemetry profile for Hamilton tracker capture controls."""

    name: str
    enable_tracker: bool
    capture_data_statistics: bool
    max_list_length_capture: int
    max_dict_length_capture: int

    @classmethod
    def resolve(cls) -> HamiltonTelemetryProfile:
        """Resolve telemetry profile from environment.

        Returns
        -------
        HamiltonTelemetryProfile
            Resolved telemetry profile with overrides applied.
        """
        profile_name = _resolve_profile_name()
        defaults = _profile_defaults(profile_name)
        overrides = _resolve_overrides()
        return cls(
            name=profile_name,
            enable_tracker=overrides.enable_tracker or defaults.enable_tracker,
            capture_data_statistics=overrides.capture_stats or defaults.capture_stats,
            max_list_length_capture=overrides.max_list or defaults.max_list,
            max_dict_length_capture=overrides.max_dict or defaults.max_dict,
        )


__all__ = [
    "HamiltonTelemetryProfile",
    "HamiltonTrackerConfig",
]
```

```python
# src/engine/runtime_profile.py (SIMPLIFIED)
"""Runtime profile presets and helpers."""

from __future__ import annotations

from dataclasses import dataclass

from datafusion_engine.session.runtime import DataFusionRuntimeProfile
from engine.telemetry.hamilton import HamiltonTelemetryProfile, HamiltonTrackerConfig


@dataclass(frozen=True)
class RuntimeProfileSpec:
    """Resolved runtime profile and determinism tier."""

    name: str
    datafusion: DataFusionRuntimeProfile
    determinism_tier: DeterminismTier = DeterminismTier.BEST_EFFORT
    tracker_config: HamiltonTrackerConfig | None = None
    hamilton_telemetry: HamiltonTelemetryProfile | None = None

    # ... remaining methods unchanged


def resolve_runtime_profile(
    profile: str,
    *,
    determinism: DeterminismTier | None = None,
) -> RuntimeProfileSpec:
    """Return a runtime profile spec for the requested profile name."""
    df_profile = DataFusionRuntimeProfile(config_policy_name=profile)
    df_profile = _apply_named_profile_overrides(profile, df_profile)
    df_profile = _apply_memory_overrides(profile, df_profile, df_profile.settings_payload())
    df_profile = _apply_env_overrides(df_profile)
    tracker_config = HamiltonTrackerConfig.from_env()
    telemetry_profile = HamiltonTelemetryProfile.resolve()
    return RuntimeProfileSpec(
        name=profile,
        datafusion=df_profile,
        determinism_tier=determinism or DeterminismTier.BEST_EFFORT,
        tracker_config=tracker_config,
        hamilton_telemetry=telemetry_profile,
    )
```

### Target Files
- Create: `src/engine/telemetry/__init__.py`
- Create: `src/engine/telemetry/hamilton.py`
- Modify: `src/engine/runtime_profile.py` (simplify, import from telemetry)
- Modify: `src/engine/__init__.py` (update exports)

### Deletions
- Remove from `runtime_profile.py`: `HamiltonTrackerConfig`, `HamiltonTelemetryProfile`, all `_*hamilton*` helper functions (~150 lines)

### Implementation Checklist
- [x] Create `src/engine/telemetry/hamilton.py` with extracted classes
- [x] Add class methods `from_env()` and `resolve()` for cleaner API
- [x] Update `runtime_profile.py` to import from telemetry module
- [x] Update `__init__.py` exports
- [ ] Verify Hamilton pipeline integration tests pass
- [x] Update documentation references
- [x] Ensure telemetry modules have no semantics imports (OTel-only concerns)

**Documentation note:** `docs/architecture/configuration_reference.md` updated to reference
`engine.telemetry.hamilton`.

---

## Scope 3 - Remove CachePolicy Class Collision

### Status
**Completed**

### Goal
Remove the `CachePolicy` class from `engine/materialize_pipeline.py` which collides with the `CachePolicy` type alias in `semantics/runtime.py`. Replace with a renamed class that clearly indicates its role.

### Representative Pattern (Current - Collision)

```python
# src/engine/materialize_pipeline.py (CURRENT)
@dataclass(frozen=True)
class CachePolicy(FingerprintableConfig):
    """Cache policy for DataFusion materialization surfaces."""
    enabled: bool
    reason: str
    writer_strategy: str
    param_mode: str
    param_signature: str | None

# src/semantics/runtime.py (AUTHORITATIVE)
CachePolicy = Literal["none", "delta_staging", "delta_output"]
```

### Representative Pattern (Target)

```python
# src/engine/materialize_pipeline.py (RENAMED)
@dataclass(frozen=True)
class MaterializationCacheDecision(FingerprintableConfig):
    """Cache eligibility decision for view materialization.

    This class captures the decision of whether to cache a materialized view,
    including the reasoning and relevant configuration. Not to be confused with
    the semantic CachePolicy type alias which controls Delta staging behavior.
    """

    enabled: bool
    reason: str
    writer_strategy: str
    param_mode: str
    param_signature: str | None

    def fingerprint_payload(self) -> Mapping[str, object]:
        return {
            "enabled": self.enabled,
            "reason": self.reason,
            "writer_strategy": self.writer_strategy,
            "param_mode": self.param_mode,
            "param_signature": self.param_signature,
        }


def resolve_materialization_cache_decision(
    *,
    policy: MaterializationPolicy,
    prefer_reader: bool,
    params: Mapping[str, object] | None,
) -> MaterializationCacheDecision:
    """Return the cache eligibility decision for plan materialization.

    Returns
    -------
    MaterializationCacheDecision
        Decision about whether to cache the materialization.
    """
    # ... existing _resolve_cache_policy logic renamed
    pass


from datafusion_engine.materialize_policy import MaterializationPolicy
```

### Target Files
- Modify: `src/engine/materialize_pipeline.py`
- Modify: `src/engine/__init__.py` (export new name)
- Modify: Callers to use `MaterializationPolicy` from `datafusion_engine.materialize_policy`

### Deletions
- Remove: `CachePolicy` class (renamed to `MaterializationCacheDecision`)
- Remove: `resolve_cache_policy` function (no compatibility shim in design phase)

### Implementation Checklist
- [x] Rename `CachePolicy` to `MaterializationCacheDecision`
- [x] Rename `resolve_cache_policy` to `resolve_materialization_cache_decision`
- [x] Update all internal callers
- [x] Update `__init__.py` exports

---

## Scope 4 - Relocate and Rename ExecutionSurfacePolicy

### Status
**Completed (implementation; verification pending)**

### Goal
Relocate `ExecutionSurfacePolicy` into `datafusion_engine` and rename it to `MaterializationPolicy` to clarify its role in view materialization and keep execution policies out of engine-specific modules. Remove the legacy alias to avoid dual naming.

### Representative Pattern (Target)

```python
# src/datafusion_engine/materialize_policy.py (RELOCATED + RENAMED)
"""Materialization policies for view output handling."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal

from core.config_base import FingerprintableConfig, config_fingerprint
from core_types import DeterminismTier

WriterStrategy = Literal["arrow", "datafusion"]


@dataclass(frozen=True)
class MaterializationPolicy(FingerprintableConfig):
    """Policy controlling how view outputs are materialized.

    Controls streaming preferences, determinism requirements, and writer
    strategy selection. Distinct from semantic CachePolicy which controls
    Delta staging behavior.

    Attributes
    ----------
    prefer_streaming
        When True, prefer streaming readers over materialized tables.
    determinism_tier
        Required determinism level for outputs.
    writer_strategy
        Arrow or DataFusion writer selection.
    """

    prefer_streaming: bool = True
    determinism_tier: DeterminismTier = DeterminismTier.BEST_EFFORT
    writer_strategy: WriterStrategy = "arrow"

    def fingerprint_payload(self) -> Mapping[str, object]:
        return {
            "prefer_streaming": self.prefer_streaming,
            "determinism_tier": self.determinism_tier.value,
            "writer_strategy": self.writer_strategy,
        }

    def fingerprint(self) -> str:
        return config_fingerprint(self.fingerprint_payload())


__all__ = ["MaterializationPolicy", "WriterStrategy"]
```

### Target Files
- Create: `src/datafusion_engine/materialize_policy.py`
- Delete: `src/engine/plan_policy.py`
- Modify: `src/engine/__init__.py` (update imports)
- Modify: `src/engine/session.py` (update import)
- Modify: `src/engine/session_factory.py` (update import)
- Modify: `src/engine/materialize_pipeline.py` (update import)

### Deletions
- Delete: `src/engine/plan_policy.py` (policy relocated)
- Remove: `ExecutionSurfacePolicy` alias (no backward-compat)

### Implementation Checklist
- [x] Create `src/datafusion_engine/materialize_policy.py`
- [x] Rename `ExecutionSurfacePolicy` to `MaterializationPolicy`
- [x] Update all internal imports
- [x] Remove `ExecutionSurfacePolicy` alias and exports (no backward-compat)
- [x] Update documentation

**Documentation note:** `ExecutionSurfacePolicy` references updated in
`docs/architecture/part_vi_cpg_build_and_orchestration.md` and
`docs/architecture/part_4_hamilton_pipeline.md`.

---

## Scope 5 - Integrate SemanticRuntimeConfig in Session Factory

### Status
**Completed**

### Goal
Enhance `build_engine_session()` to optionally accept and thread `SemanticRuntimeConfig`, reducing the need for callers to manually adapt from DataFusion profiles.

### Representative Pattern (Target)

```python
# src/engine/session_factory.py (ENHANCED)
"""Engine session factory helpers."""

from __future__ import annotations

from typing import TYPE_CHECKING

from datafusion_engine.dataset.registration import dataset_input_plugin, input_plugin_prefixes
from datafusion_engine.dataset.registry import DatasetCatalog, registry_snapshot
from datafusion_engine.session.runtime import feature_state_snapshot
from datafusion_engine.materialize_policy import MaterializationPolicy
from datafusion_engine.semantics_runtime import (
    apply_semantic_runtime_config,
    semantic_runtime_from_profile,
)
from engine.runtime import build_engine_runtime
from engine.runtime_profile import (
    RuntimeProfileSpec,
    engine_runtime_artifact,
    runtime_profile_snapshot,
)
from engine.session import EngineSession
from obs.diagnostics import DiagnosticsCollector
from obs.otel import OtelBootstrapOptions, configure_otel
from relspec.pipeline_policy import DiagnosticsPolicy

if TYPE_CHECKING:
    from semantics.runtime import SemanticBuildOptions, SemanticRuntimeConfig


def build_engine_session(
    *,
    runtime_spec: RuntimeProfileSpec,
    diagnostics: DiagnosticsCollector | None = None,
    surface_policy: MaterializationPolicy | None = None,
    diagnostics_policy: DiagnosticsPolicy | None = None,
    semantic_config: SemanticRuntimeConfig | None = None,
    build_options: SemanticBuildOptions | None = None,
) -> EngineSession:
    """Build an EngineSession bound to the provided runtime spec.

    Parameters
    ----------
    runtime_spec
        Resolved runtime profile specification.
    diagnostics
        Optional diagnostics collector for recording execution events.
    surface_policy
        Optional materialization policy overrides.
    diagnostics_policy
        Optional diagnostics capture policy.
    semantic_config
        Optional semantic runtime configuration. When provided, cache policy
        overrides and output locations are applied to the session.
    build_options
        Optional semantic build options to thread into semantic execution.

    Returns
    -------
    EngineSession
        Engine session wired to the runtime surfaces.
    """
    configure_otel(
        service_name="codeanatomy",
        options=OtelBootstrapOptions(
            resource_overrides={"codeanatomy.runtime_profile": runtime_spec.name},
        ),
    )
    engine_runtime = build_engine_runtime(
        runtime_profile=runtime_spec.datafusion,
        diagnostics=diagnostics,
        diagnostics_policy=diagnostics_policy,
    )
    df_profile = engine_runtime.datafusion_profile

    # Resolve + apply semantic config via the datafusion_engine bridge
    resolved_semantic = semantic_config or semantic_runtime_from_profile(df_profile)
    df_profile = apply_semantic_runtime_config(df_profile, resolved_semantic)
    engine_runtime = engine_runtime.with_datafusion_profile(df_profile)

    profile_name = runtime_spec.name
    if diagnostics is not None:
        snapshot = feature_state_snapshot(
            profile_name=profile_name,
            determinism_tier=runtime_spec.determinism_tier,
            runtime_profile=df_profile,
        )
        diagnostics.record_events("feature_state_v1", [snapshot.to_row()])

    datasets = DatasetCatalog()
    input_plugin_names: list[str] = []
    if df_profile is not None:
        plugin = dataset_input_plugin(datasets, runtime_profile=df_profile)
        registry_catalogs = dict(df_profile.registry_catalogs)
        registry_catalogs.setdefault(df_profile.default_schema, datasets)
        df_profile = replace(
            df_profile,
            input_plugins=(*df_profile.input_plugins, plugin),
            registry_catalogs=registry_catalogs,
        )
        engine_runtime = engine_runtime.with_datafusion_profile(df_profile)
        input_plugin_names = [plugin.__name__]

    settings_hash = df_profile.settings_hash()
    runtime_snapshot = runtime_profile_snapshot(
        df_profile,
        name=profile_name,
        determinism_tier=runtime_spec.determinism_tier,
    )
    if diagnostics is not None:
        if not diagnostics.artifacts_snapshot().get("engine_runtime_v2"):
            diagnostics.record_artifact(
                "engine_runtime_v2",
                engine_runtime_artifact(
                    df_profile,
                    name=profile_name,
                    determinism_tier=runtime_spec.determinism_tier,
                ),
            )
        diagnostics.record_artifact(
            "datafusion_input_plugins_v1",
            {
                "plugins": input_plugin_names,
                "prefixes": list(input_plugin_prefixes()),
                "dataset_registry": registry_snapshot(datasets),
            },
        )
    return EngineSession(
        engine_runtime=engine_runtime,
        datasets=datasets,
        diagnostics=diagnostics,
        surface_policy=surface_policy or MaterializationPolicy(),
        settings_hash=settings_hash,
        runtime_profile_hash=runtime_snapshot.profile_hash,
    )


__all__ = ["build_engine_session"]
```

### Target Files
- Modify: `src/engine/session_factory.py`
- Modify: `src/engine/__init__.py` (ensure new parameter is documented)
- Modify: `src/datafusion_engine/semantics_runtime.py` (bridge helpers)

### Deletions
None (enhancement only)

### Implementation Checklist
- [x] Add `semantic_config` parameter to `build_engine_session()`
- [x] Add `build_options` parameter to `build_engine_session()`
- [x] Route semantic config through `datafusion_engine.semantics_runtime`
- [x] Update docstrings with semantic config usage
- [x] Add integration test for semantic config flow
- [x] Update Hamilton pipeline modules to use semantic config path

---

## Scope 6 - Consolidate Diagnostics Recording Helpers

### Status
**Completed (implementation; verification pending)**

### Goal
Extract the duplicated diagnostics recording patterns from `materialize_pipeline.py` and `delta_tools.py` into a single `EngineEventRecorder` helper class, with typed payloads aligned to semantic diagnostics artifacts and deterministic schema.

### Representative Pattern (Current - Duplicated)

```python
# src/engine/materialize_pipeline.py
def _record_plan_execution(runtime_profile, *, plan_id, result, view_artifact):
    # ... 15 lines of diagnostics recording

def _record_extract_write(runtime_profile, *, record):
    # ... 12 lines of diagnostics recording

def _record_diskcache_stats(runtime_profile):
    # ... 20 lines of diagnostics recording

# src/engine/delta_tools.py
def _record_maintenance(runtime_profile, *, payload):
    # ... 8 lines of diagnostics recording

def _record_delta_query(runtime_profile, *, payload):
    # ... 8 lines of diagnostics recording

def _record_delta_snapshot_table(profile, *, table_uri, snapshot, dataset_name):
    # ... 10 lines of diagnostics recording
```

**Note:** Delta snapshot/maintenance observability artifacts still use
`datafusion_engine.delta.observability` and remain in `delta_tools.py`; they are
not part of EngineEventRecorder because they target the Delta observability pipeline.

### Representative Pattern (Target)

```python
# src/engine/diagnostics.py (NEW)
"""Consolidated diagnostics recording for engine operations."""

from __future__ import annotations

import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from datafusion_engine.session.facade import ExecutionResult
    from datafusion_engine.views.artifacts import DataFusionViewArtifact
    from storage.deltalake import DeltaWriteResult


@dataclass(frozen=True)
class PlanExecutionEvent:
    """Typed payload for plan execution diagnostics."""

    plan_id: str
    result_kind: str
    rows: int | None
    plan_fingerprint: str | None

    def to_payload(self) -> Mapping[str, object]:
        return {
            "plan_id": self.plan_id,
            "result_kind": self.result_kind,
            "rows": self.rows,
            "plan_fingerprint": self.plan_fingerprint,
        }


@dataclass
class EngineEventRecorder:
    """Unified diagnostics recorder for engine operations.

    Provides a consistent interface for recording execution events,
    artifacts, and telemetry across engine modules.
    """

    runtime_profile: DataFusionRuntimeProfile | None

    @property
    def _sink(self):
        if self.runtime_profile is None:
            return None
        return self.runtime_profile.diagnostics_sink

    def record_artifact(self, name: str, payload: Mapping[str, object]) -> None:
        """Record a diagnostics artifact."""
        if self._sink is None:
            return
        from datafusion_engine.lineage.diagnostics import record_artifact
        record_artifact(self.runtime_profile, name, payload)

    def record_events(self, name: str, events: list[Mapping[str, object]]) -> None:
        """Record diagnostics events."""
        if self._sink is None:
            return
        from datafusion_engine.lineage.diagnostics import record_events
        record_events(self.runtime_profile, name, events)

    def record_plan_execution(
        self,
        *,
        plan_id: str,
        result: ExecutionResult,
        view_artifact: DataFusionViewArtifact | None = None,
    ) -> None:
        """Record plan execution diagnostics."""
        rows: int | None = None
        if result.table is not None:
            rows = result.table.num_rows
        event = PlanExecutionEvent(
            plan_id=plan_id,
            result_kind=result.kind.value,
            rows=rows,
            plan_fingerprint=view_artifact.plan_fingerprint if view_artifact else None,
        )
        self.record_artifact("plan_execute_v1", event.to_payload())

    def record_extract_write(
        self,
        *,
        dataset: str,
        mode: str,
        path: str,
        file_format: str,
        rows: int | None,
        copy_sql: str | None,
        copy_options: Mapping[str, object] | None,
        delta_result: DeltaWriteResult | None,
    ) -> None:
        """Record extract output write diagnostics."""
        payload = {
            "event_time_unix_ms": int(time.time() * 1000),
            "dataset": dataset,
            "mode": mode,
            "path": path,
            "format": file_format,
            "rows": rows,
            "copy_sql": copy_sql,
            "copy_options": dict(copy_options) if copy_options is not None else None,
            "delta_version": delta_result.version if delta_result is not None else None,
        }
        self.record_artifact("datafusion_extract_output_writes_v1", payload)

    def record_delta_maintenance(
        self,
        *,
        dataset: str | None,
        path: str,
        operation: str,
        extra: Mapping[str, object] | None = None,
    ) -> None:
        """Record Delta maintenance operation diagnostics."""
        payload = {
            "event_time_unix_ms": int(time.time() * 1000),
            "dataset": dataset,
            "path": path,
            "operation": operation,
        }
        if extra:
            payload.update(extra)
        self.record_artifact("delta_maintenance_v1", payload)

    def record_delta_query(
        self,
        *,
        path: str,
        sql: str | None,
        table_name: str,
        engine: str,
    ) -> None:
        """Record Delta query execution diagnostics."""
        payload = {
            "event_time_unix_ms": int(time.time() * 1000),
            "path": path,
            "sql": sql,
            "table_name": table_name,
            "engine": engine,
        }
        self.record_artifact("delta_query_v1", payload)


__all__ = ["EngineEventRecorder"]
```

### Target Files
- Create: `src/engine/diagnostics.py`
- Modify: `src/engine/materialize_pipeline.py` (use EngineEventRecorder)
- Modify: `src/engine/delta_tools.py` (use EngineEventRecorder)
- Modify: `src/engine/__init__.py` (export EngineEventRecorder)

### Deletions
- Remove from `materialize_pipeline.py`: `_record_plan_execution`, `_record_extract_write`, `_record_diskcache_stats` (replaced by EngineEventRecorder methods)
- Remove from `delta_tools.py`: `_record_maintenance`, `_record_delta_query` (replaced by EngineEventRecorder methods)

### Implementation Checklist
- [x] Create `src/engine/diagnostics.py` with `EngineEventRecorder`
- [x] Add typed payload dataclasses to keep diagnostics schemas stable
- [x] Migrate `materialize_pipeline.py` to use `EngineEventRecorder`
- [x] Migrate `delta_tools.py` to use `EngineEventRecorder`
- [x] Update `__init__.py` exports
- [x] Verify diagnostics output parity with previous implementation
- [ ] Run integration tests to verify no regressions

**Parity notes:** `plan_execute_v1` payloads now match prior key behavior (plan_fingerprint omitted when absent).
`delta_query_v1` uses a nullable `sql` field to avoid raw SQL in execution paths.

---

## Scope 7 - Remove Deprecated build_plan_product Stub

### Status
**Completed (implementation; verification pending)**

### Goal
Remove the deprecated `build_plan_product` function that only raises `ValueError`, along with any related dead code.

### Representative Pattern (Current - Dead Code)

```python
# src/engine/materialize_pipeline.py (CURRENT)
def build_plan_product(
    plan: DataFusionPlanBundle,
    *,
    runtime_profile: DataFusionRuntimeProfile,
    policy: MaterializationPolicy,
    plan_id: str | None = None,
) -> PlanProduct:
    """Raise because plan materialization is deprecated.

    Raises
    ------
    ValueError
        Always raised to enforce view-only materialization.
    """
    _ = (plan, runtime_profile, policy, plan_id)
    msg = "Plan materialization is deprecated; use build_view_product instead."
    raise ValueError(msg)
```

### Representative Pattern (Target)

```python
# src/engine/materialize_pipeline.py (AFTER REMOVAL)
# Function removed entirely - only build_view_product remains

__all__ = [
    "build_view_product",
    "resolve_materialization_cache_decision",
    "resolve_prefer_reader",
    "write_extract_outputs",
]
```

### Target Files
- Modify: `src/engine/materialize_pipeline.py` (remove function)
- Modify: `src/engine/__init__.py` (remove export if present)

### Deletions
- Remove: `build_plan_product` function
- Remove: Any imports only used by `build_plan_product`

### Implementation Checklist
- [x] Search for any callers of `build_plan_product` (should be none)
- [x] Remove function from `materialize_pipeline.py`
- [x] Remove from `__init__.py` exports if present
- [x] Remove any unused imports left behind
- [ ] Run test suite to verify no breakage

---

## Scope 8 - Align Cache Policy Resolution with Semantic Runtime

### Status
**Completed (implementation; verification pending)**

### Goal
Align engine cache decisions with semantics by **consuming the resolved semantic cache policy** (from `SemanticRuntimeConfig` or semantic view nodes). Engine must not re-derive cache policy from semantic metadata.

### Representative Pattern (Target)

```python
# src/engine/materialize_pipeline.py (ENHANCED)
"""Unified materialization and output writing helpers."""

from __future__ import annotations

from semantics.runtime import CachePolicy


def resolve_materialization_cache_decision(
    *,
    policy: MaterializationPolicy,
    prefer_reader: bool,
    params: Mapping[str, object] | None,
    semantic_cache_policy: CachePolicy | None = None,
) -> MaterializationCacheDecision:
    """Return the cache eligibility decision for plan materialization.

    When semantic_cache_policy is provided, it is authoritative and originates
    from semantics (definition authority). Engine only adapts execution.

    Parameters
    ----------
    policy
        Materialization policy controlling streaming and determinism.
    prefer_reader
        Whether streaming readers are preferred.
    params
        Optional parameter bindings.
    semantic_cache_policy
        Optional resolved semantic cache policy ("none", "delta_staging", "delta_output").

    Returns
    -------
    MaterializationCacheDecision
        Decision about whether to cache the materialization.
    """
    writer_strategy = policy.writer_strategy
    param_mode, param_signature = _param_binding_state(params)

    if param_mode != "none":
        return MaterializationCacheDecision(
            enabled=False,
            reason="params",
            writer_strategy=writer_strategy,
            param_mode=param_mode,
            param_signature=param_signature,
        )

    # Semantic policy (authoritative) overrides engine heuristics
    if semantic_cache_policy is not None:
        return MaterializationCacheDecision(
            enabled=semantic_cache_policy != "none",
            reason=f"semantic_policy_{semantic_cache_policy}",
            writer_strategy=writer_strategy,
            param_mode=param_mode,
            param_signature=param_signature,
        )

    # Fallback to policy-based decision
    if writer_strategy != "arrow":
        return MaterializationCacheDecision(
            enabled=False,
            reason=f"writer_strategy_{writer_strategy}",
            writer_strategy=writer_strategy,
            param_mode=param_mode,
            param_signature=param_signature,
        )

    if prefer_reader:
        return MaterializationCacheDecision(
            enabled=False,
            reason="prefer_streaming",
            writer_strategy=writer_strategy,
            param_mode=param_mode,
            param_signature=param_signature,
        )

    if policy.determinism_tier == DeterminismTier.BEST_EFFORT:
        return MaterializationCacheDecision(
            enabled=False,
            reason="policy_best_effort",
            writer_strategy=writer_strategy,
            param_mode=param_mode,
            param_signature=param_signature,
        )

    return MaterializationCacheDecision(
        enabled=True,
        reason="materialize",
        writer_strategy=writer_strategy,
        param_mode=param_mode,
        param_signature=param_signature,
    )
```

### Target Files
- Modify: `src/engine/materialize_pipeline.py`
- Modify: Integration points that call `resolve_materialization_cache_decision`

### Deletions
None (enhancement only)

### Implementation Checklist
- [x] Add `semantic_cache_policy` parameter to `resolve_materialization_cache_decision`
- [x] Thread resolved semantic cache policy from semantic view graph into `build_view_product`
- [x] Add tests for semantic policy-driven cache decisions
- [x] Verify alignment with semantic cache policy resolution in `datafusion_engine/views/registry_specs.py`

---

## Scope 9 - Centralize Semantic Runtime Bridge in datafusion_engine

### Status
**Completed**

### Goal
Centralize the translation between `DataFusionRuntimeProfile` and `SemanticRuntimeConfig` in a single datafusion_engine module. Engine and semantics must both use this bridge to avoid duplicated mappings.

### Representative Pattern (Target)

```python
# src/datafusion_engine/semantics_runtime.py (NEW)
"""Bridge between DataFusion runtime profiles and semantic runtime config."""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

from datafusion_engine.dataset.registry import DatasetLocation

if TYPE_CHECKING:
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from semantics.runtime import SemanticRuntimeConfig


def semantic_runtime_from_profile(
    profile: DataFusionRuntimeProfile,
) -> SemanticRuntimeConfig:
    """Build SemanticRuntimeConfig from a DataFusion runtime profile."""
    from semantics.runtime import SemanticRuntimeConfig

    return SemanticRuntimeConfig(
        output_locations={name: loc.path for name, loc in profile.dataset_locations.items()},
        cache_policy_overrides=dict(getattr(profile, "semantic_cache_overrides", {}) or {}),
        cdf_enabled=profile.cdf_enabled,
        cdf_cursor_store=profile.cdf_cursor_store,
        storage_options=profile.storage_options,
        schema_evolution_enabled=profile.enable_schema_evolution_adapter,
    )


def apply_semantic_runtime_config(
    profile: DataFusionRuntimeProfile,
    semantic_config: SemanticRuntimeConfig,
) -> DataFusionRuntimeProfile:
    """Apply SemanticRuntimeConfig to a DataFusion runtime profile."""
    dataset_locations = dict(profile.dataset_locations)
    for name, path in semantic_config.output_locations.items():
        if name not in dataset_locations:
            dataset_locations[name] = DatasetLocation(
                path=path,
                format="delta",
                storage_options=dict(semantic_config.storage_options or {}),
            )

    semantic_cache_overrides = dict(getattr(profile, "semantic_cache_overrides", {}) or {})
    semantic_cache_overrides.update(semantic_config.cache_policy_overrides)

    return replace(
        profile,
        dataset_locations=dataset_locations,
        semantic_cache_overrides=semantic_cache_overrides,
        cdf_enabled=semantic_config.cdf_enabled,
        cdf_cursor_store=semantic_config.cdf_cursor_store,
        enable_schema_evolution_adapter=semantic_config.schema_evolution_enabled,
    )
```

### Target Files
- Create: `src/datafusion_engine/semantics_runtime.py`
- Modify: `src/engine/session_factory.py` (use the bridge)
- Modify: `src/semantics/pipeline.py` (use the bridge where appropriate)

### Deletions
- Remove any local semantic runtime mapping helpers from engine modules

### Implementation Checklist
- [x] Create `datafusion_engine/semantics_runtime.py` with `semantic_runtime_from_profile`
- [x] Implement `apply_semantic_runtime_config` for profile updates
- [x] Update engine session factory to use the bridge
- [x] Confirm semantic pipeline does not derive runtime config directly (no change required)
- [x] Add integration tests for round-trip semantic runtime mapping

---

## Scope 10 - Enforce Engine/Semantics Execution Boundary

### Status
**Completed (implementation; verification pending)**

### Goal
Ensure engine never re-registers semantic views and only executes or materializes views defined by semantics.

### Representative Pattern (Target)

```python
# src/engine/semantic_boundary.py (NEW)
"""Guardrails to keep engine execution-only."""

from __future__ import annotations

from collections.abc import Sequence

from datafusion import SessionContext
from semantics.spec_registry import SEMANTIC_VIEW_NAMES


def ensure_semantic_views_registered(
    ctx: SessionContext,
    view_names: Sequence[str] | None = None,
) -> None:
    """Raise if required semantic views are not registered."""
    names = tuple(view_names) if view_names is not None else SEMANTIC_VIEW_NAMES
    missing: list[str] = []
    for name in names:
        try:
            _ = ctx.table(name)
        except Exception:
            missing.append(name)
    if missing:
        msg = f"Semantic views missing from session: {sorted(missing)}"
        raise ValueError(msg)
```

```python
# src/engine/materialize_pipeline.py (ENHANCED)
from engine.semantic_boundary import ensure_semantic_views_registered

def build_view_product(...):
    ensure_semantic_views_registered(ctx)
    # proceed with execution-only flow
```

### Target Files
- Create: `src/engine/semantic_boundary.py`
- Modify: `src/engine/materialize_pipeline.py` (enforce boundary before materialization)
- Add: `tests/unit/engine/test_semantic_boundary.py`

### Deletions
- Remove any engine helpers that register or define semantic views

### Implementation Checklist
- [x] Add `ensure_semantic_views_registered` guard
- [x] Enforce guard in `materialize_pipeline.py` before materialization
- [x] Add boundary test to prevent engine from importing/defining semantic views
- [ ] Validate that semantics pipeline is the only view registration authority

---

## Decommission and Deletion Summary

### Files to Relocate/Delete (After All Scopes Complete)

**Scope 1 - Plan Cache Relocation:**
- Relocate: `src/engine/plan_cache.py` → `src/datafusion_engine/plan/cache.py`
- Delete: `src/engine/plan_cache.py` (no shim)

**Scope 2 - Hamilton Telemetry Extraction:**
- Create: `src/engine/telemetry/hamilton.py`
- Modify: `src/engine/runtime_profile.py` (remove ~150 lines)

**Scope 3 - CachePolicy Rename:**
- Rename: `CachePolicy` → `MaterializationCacheDecision`
- Rename: `resolve_cache_policy` → `resolve_materialization_cache_decision`

**Scope 4 - Policy Rename:**
- Relocate: `plan_policy.py` → `datafusion_engine/materialize_policy.py`
- Rename: `ExecutionSurfacePolicy` → `MaterializationPolicy`

**Scope 7 - Dead Code Removal:**
- Delete: `build_plan_product` function

**Scope 9 - Semantic Runtime Bridge:**
- Create: `src/datafusion_engine/semantics_runtime.py`
- Delete: any engine-local semantic runtime mapping helpers

**Scope 10 - Execution Boundary:**
- Create: `src/engine/semantic_boundary.py`
- Add: boundary enforcement in `materialize_pipeline.py`

### Classes/Functions to Remove

**From `materialize_pipeline.py`:**
- `CachePolicy` class (renamed)
- `resolve_cache_policy` function (renamed)
- `build_plan_product` function (deleted)
- `_record_plan_execution` (moved to EngineEventRecorder)
- `_record_extract_write` (moved to EngineEventRecorder)
- `_record_diskcache_stats` (moved to EngineEventRecorder)

**From `delta_tools.py`:**
- `_record_maintenance` (moved to EngineEventRecorder)
- `_record_delta_query` (moved to EngineEventRecorder)

**From `runtime_profile.py`:**
- `HamiltonTrackerConfig` (moved to telemetry/hamilton.py)
- `HamiltonTelemetryProfile` (moved to telemetry/hamilton.py)
- All `_*hamilton*` helper functions (moved to telemetry/hamilton.py)

**From `engine/plan_policy.py`:**
- `ExecutionSurfacePolicy` (moved to `datafusion_engine/materialize_policy.py`)

---

## Execution Order (Recommended)

```
Phase 1: Relocate and Extract
1) Scope 1 - Relocate PlanCache to datafusion_engine
2) Scope 2 - Extract Hamilton telemetry configuration
3) Scope 9 - Centralize semantic runtime bridge

Phase 2: Rename and Consolidate
4) Scope 3 - Remove CachePolicy class collision
5) Scope 4 - Relocate and rename ExecutionSurfacePolicy
6) Scope 6 - Consolidate diagnostics recording

Phase 3: Enhance and Clean Up
7) Scope 5 - Integrate SemanticRuntimeConfig
8) Scope 7 - Remove deprecated build_plan_product
9) Scope 8 - Align cache policy with semantic runtime
10) Scope 10 - Enforce engine/semantics execution boundary
```

---

## Completion Criteria

- [x] `PlanCache` relocated to `datafusion_engine/plan/cache.py`
- [x] Hamilton telemetry configuration extracted to `engine/telemetry/hamilton.py`
- [x] No `CachePolicy` class collision between engine and semantics
- [x] `MaterializationPolicy` lives in `datafusion_engine/materialize_policy.py`
- [x] `build_engine_session` accepts `SemanticRuntimeConfig` and routes via semantic bridge
- [x] Diagnostics recording consolidated into `EngineEventRecorder`
- [x] Deprecated `build_plan_product` removed
- [x] Cache policy resolution accepts semantic cache policy inputs when provided
- [x] Resolved semantic cache policy from semantic view graph is threaded into engine materialization
- [x] Semantic runtime bridge centralized in `datafusion_engine`
- [x] Engine/semantics execution boundary enforced (no re-registration)
- [x] No backward compatibility shims remain
- [x] Documentation references updated for telemetry + materialization policy rename
- [x] Diagnostics payload parity validated against previous implementation
- [ ] Full test suite passes with no regressions

---

## Verification Commands

```bash
# After each scope, verify no regressions
uv run pytest tests/unit/engine/ -v
uv run pytest tests/integration/test_engine_session_smoke.py -v

# Verify no dangling imports
uv run ruff check src/engine/

# Verify no SQL strings in engine integration
rg "ctx\\.sql|SELECT|FROM" src/engine/

# Type check
uv run pyright src/engine/ --warnings
uv run pyrefly check

# Full test suite
uv run pytest tests/ -m "not e2e"
```

---

## Cross-Reference

This plan builds on the patterns established in:
- `docs/plans/datafusion_engine_semantic_alignment_plan.md`
- `docs/plans/datafusion_engine_semantic_alignment_opportunities_plan.md`

Key architectural invariants preserved:
- **Engine is execution authority** - Orchestrates DataFusion execution
- **Semantics is definition authority** - Owns view logic and specifications
- **Clean boundary** - Engine adapts to semantic needs via SemanticRuntimeConfig
