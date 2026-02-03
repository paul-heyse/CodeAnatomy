# Diagnostics Build Troubleshooting Plan v1

Status: Proposed  
Owner: Codex  
Scope: Diagnostics improvements for first full CLI build + troubleshooting

## Objectives
- Expose high-leverage OpenTelemetry controls at the CLI/config level for rapid troubleshooting.
- Add a diagnostics profile system to make "debug-ready" configurations trivial to enable.
- Surface Hamilton tracker and capture controls so node-level telemetry is available on demand.
- Emit DataFusion cache/stats diagnostics artifacts for fast performance/root-cause analysis.
- Correlate run IDs to run bundles and outputs for deterministic post-run investigation.

---

## Scope Item 1 — OTel Troubleshooting Controls in CLI + Config

### Why
OTel setup issues are the most common cause of missing diagnostics. We need easy, explicit overrides for endpoint, protocol, sampling, batching, and resource attributes.

### Design (best-in-class choice)
Add CLI flags that map to `OtelBootstrapOptions` and/or env override helpers and ensure they are incorporated into `obs.otel.config.resolve_otel_config()` / `obs.otel.bootstrap.configure_otel()`.

### Representative snippet
```python
# src/cli/app.py (or src/cli/commands/build.py)
@dataclass(frozen=True)
class ObservabilityOptions:
    otel_endpoint: Annotated[
        str | None,
        Parameter(name="--otel-endpoint", help="OTLP endpoint override.")
    ] = None
    otel_protocol: Annotated[
        Literal["grpc", "http/protobuf"] | None,
        Parameter(name="--otel-protocol", help="OTLP protocol override.")
    ] = None

# src/obs/otel/config.py
endpoint = env_text("OTEL_EXPORTER_OTLP_ENDPOINT") or overrides.otel_endpoint
protocol = env_text("OTEL_EXPORTER_OTLP_PROTOCOL") or overrides.otel_protocol
```

### Target files
- `src/cli/app.py`
- `src/cli/context.py`
- `src/obs/otel/config.py`
- `src/obs/otel/bootstrap.py`

### Deprecations / deletions
- None.

### Implementation checklist
- Add CLI flags for endpoint, protocol, sampler, sampler arg, log correlation, export intervals, BSP/BLRP tuning.
- Thread options into `OtelBootstrapOptions`.
- Extend `resolve_otel_config()` to honor overrides with clear precedence.
- Update `config show --with-sources` to surface OTel overrides where applicable.

---

## Scope Item 2 — Diagnostics Profile Presets

### Why
We need one-liner diagnostics enablement for troubleshooting builds without editing multiple config keys.

### Design
Introduce `diagnostics_profile` in config (e.g., `debug`, `dev`, `prod`) that expands to a preset map of diagnostics settings.

### Representative snippet
```python
# src/cli/config_loader.py
PROFILE_PRESETS = {
    "debug": {
        "enable_plan_diagnostics": True,
        "enable_hamilton_node_diagnostics": True,
        "enable_structured_run_logs": True,
        "enable_otel_node_tracing": True,
    },
}

if profile := config.get("diagnostics_profile"):
    config = {**config, **PROFILE_PRESETS.get(profile, {})}
```

### Target files
- `src/cli/config_loader.py`
- `src/cli/config_models.py`
- `docs/python_library_reference/opentelemetry.md` (optional note)

### Deprecations / deletions
- None.

### Implementation checklist
- Add `diagnostics_profile` to config models.
- Implement preset expansion in config loader normalization.
- Document available profiles and their effects.

---

## Scope Item 3 — Hamilton Tracker + Capture Controls in CLI/Config

### Why
Hamilton UI and node telemetry drastically improve debugging but are not first-class in CLI config surfaces.

### Design
Expose Hamilton tracker settings and capture limits (data statistics, max list/dict capture) as config keys and CLI flags.

### Representative snippet
```python
# src/cli/config_models.py
hamilton_project_id: int | None = None
hamilton_username: str | None = None
hamilton_dag_name: str | None = None
hamilton_api_url: str | None = None
hamilton_ui_url: str | None = None
hamilton_capture_data_statistics: bool | None = None
hamilton_max_list_length_capture: int | None = None
hamilton_max_dict_length_capture: int | None = None
```

### Target files
- `src/cli/config_models.py`
- `src/cli/commands/build.py`
- `src/hamilton_pipeline/driver_factory.py`
- `src/obs/otel/bootstrap.py`

### Deprecations / deletions
- None.

### Implementation checklist
- Add CLI flags and config support for Hamilton tracker fields.
- Ensure driver builder passes tracker config + capture controls.
- Confirm defaults remain safe for production (capture stats opt-out).

---

## Scope Item 4 — DataFusion Cache/Stats Diagnostics Artifacts

### Why
Cache and stats settings are the primary contributors to DataFusion performance and plan behavior.

### Design
Emit explicit diagnostics artifacts showing DataFusion cache limits, TTLs, stats collection, and concurrency settings.

### Representative snippet
```python
# src/obs/diagnostics.py
collector.record_artifact(
    "datafusion_cache_config_v1",
    {
        "metadata_cache_limit": config.metadata_cache_limit,
        "list_files_cache_limit": config.list_files_cache_limit,
        "list_files_cache_ttl": config.list_files_cache_ttl,
        "collect_statistics": config.collect_statistics,
        "meta_fetch_concurrency": config.meta_fetch_concurrency,
    },
)
```

### Target files
- `src/obs/diagnostics.py`
- `src/datafusion_engine/session/runtime.py`
- `src/obs/datafusion_runs.py`

### Deprecations / deletions
- None.

### Implementation checklist
- Read cache + stats settings from DataFusion runtime config.
- Emit a `datafusion_cache_config_v1` artifact on session init.
- Include `collect_statistics` / `meta_fetch_concurrency` in diagnostics payload.

---

## Scope Item 5 — Run Bundle + Output Correlation Artifact

### Why
We need a deterministic mapping from run IDs to run bundles and outputs for post‑mortem debugging.

### Design
Emit a diagnostics artifact at build completion with run_id, output_dir, and run_bundle_dir.

### Representative snippet
```python
# src/graph/product_build.py
record_semantic_quality_artifact(
    sink,
    artifact=SemanticQualityArtifact(
        name="build_output_locations_v1",
        row_count=0,
        schema_hash=None,
        artifact_uri=str(result.run_bundle_dir),
        run_id=run_id,
    ),
)
```

### Target files
- `src/graph/product_build.py`
- `src/obs/diagnostics.py`
- `src/hamilton_pipeline/io_contracts.py` (if needed for manifest linkage)

### Deprecations / deletions
- None.

### Implementation checklist
- Emit a `build_output_locations_v1` artifact after build.
- Include run_id, output_dir, run_bundle_dir.
- Ensure it appears in the diagnostics report.

---

## Cross-Cutting Acceptance Gates
- CLI can override OTel endpoints/protocol/sampling/batching without code changes.
- `diagnostics_profile` reliably expands to a known debug configuration.
- Hamilton tracker settings are configurable and reflected in runs.
- DataFusion cache/stats config is visible as diagnostics artifacts.
- Run bundle/output correlation artifact is emitted for each build.
