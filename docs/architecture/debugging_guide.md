# Debugging and Troubleshooting Guide

## Overview

This guide provides practical debugging workflows for CodeAnatomy's inference-driven Code Property Graph pipeline. It covers common failure modes, diagnostic tools, and step-by-step resolution procedures.

## Plan Bundle Inspection and Fingerprint Debugging

### Plan Bundle Structure

The `DataFusionPlanBundle` is the canonical plan artifact for all DataFusion operations. It contains:

- **Logical Plan**: Unoptimized logical plan from the DataFrame
- **Optimized Plan**: Optimized logical plan used for lineage extraction
- **Execution Plan**: Physical execution plan (may be None for lazy evaluation)
- **Substrait Bytes**: Portable serialization for fingerprinting and caching
- **Plan Fingerprint**: Stable hash incorporating substrait, settings, UDFs, and Delta inputs

### Inspecting Plan Bundle Contents

#### Display Plan Text Representations

```python
from datafusion_engine.plan_bundle import DataFusionPlanBundle

# Assuming you have a bundle
bundle: DataFusionPlanBundle = ...

# Display logical plan with schema
logical_text = bundle.display_logical_plan()
print(logical_text)

# Display optimized plan with schema
optimized_text = bundle.display_optimized_plan()
print(optimized_text)

# Display physical execution plan
execution_text = bundle.display_execution_plan()
print(execution_text)

# Get GraphViz DOT representation for visualization
dot_graph = bundle.graphviz()
if dot_graph:
    with open("plan.dot", "w") as f:
        f.write(dot_graph)
```

#### Access Plan Metadata

```python
# Examine plan details dictionary
details = bundle.plan_details

# Check partition count
partition_count = details.get("partition_count")

# Check repartition operations count
repartition_count = details.get("repartition_count")

# View schema information
schema_names = details.get("schema_names")
schema_describe = details.get("schema_describe")

# Check plan statistics
stats = details.get("statistics")
```

### Fingerprint Mismatch Diagnosis

Plan fingerprints are critical for caching and determinism. A fingerprint incorporates:

1. **Substrait hash**: SHA-256 of serialized Substrait plan
2. **DataFusion settings**: Session configuration snapshot
3. **Planning environment**: Runtime profile settings
4. **Rulepack hash**: Planner rules (analyzer, optimizer, physical optimizer)
5. **Information schema hash**: Catalog metadata snapshot
6. **UDF snapshot hash**: Registered UDF metadata
7. **Required UDFs**: UDFs referenced in the plan
8. **Delta inputs**: Version pins for Delta Lake tables
9. **Delta store policy hash**: Storage policy configuration

#### Diagnosing Fingerprint Changes

```python
# Access determinism audit from plan details
audit = bundle.plan_details.get("determinism_audit")

# Inspect individual hash components
plan_fingerprint = audit["plan_fingerprint"]
planning_env_hash = audit["planning_env_hash"]
rulepack_hash = audit["rulepack_hash"]
information_schema_hash = audit["information_schema_hash"]
df_settings_hash = audit["df_settings_hash"]
udf_snapshot_hash = audit["udf_snapshot_hash"]
function_registry_hash = audit["function_registry_hash"]
schema_contract_hash = audit["schema_contract_hash"]

# Compare artifacts between two runs
artifacts_1 = bundle_1.artifacts
artifacts_2 = bundle_2.artifacts

# Check planning environment differences
env_1 = artifacts_1.planning_env_snapshot
env_2 = artifacts_2.planning_env_snapshot

# Identify changed settings
changed_settings = {
    key: (env_1.get(key), env_2.get(key))
    for key in set(env_1) | set(env_2)
    if env_1.get(key) != env_2.get(key)
}
```

#### Common Fingerprint Mismatch Causes

| Cause | Symptom | Resolution |
|-------|---------|------------|
| Different DataFusion versions | `datafusion_version` differs in planning env | Pin DataFusion version in dependencies |
| Session config drift | `df_settings_hash` mismatch | Normalize session settings via `DataFusionRuntimeProfile` |
| UDF registration order | `function_registry_hash` mismatch | Use deterministic UDF registration order |
| Delta version changes | `delta_inputs` differ | Pin Delta table versions explicitly |
| Optimizer rule changes | `rulepack_hash` mismatch | Lock DataFusion version or disable dynamic rules |
| Catalog metadata changes | `information_schema_hash` mismatch | Ensure catalog snapshots are stable |

### Substrait Plan Debugging

#### Enable Substrait Validation

```python
from datafusion_engine.runtime import DataFusionRuntimeProfile

profile = DataFusionRuntimeProfile(substrait_validation=True)
# Plan bundle will include substrait validation payload
```

#### Inspect Substrait Validation Results

```python
substrait_validation = bundle.plan_details.get("substrait_validation")

if substrait_validation:
    match = substrait_validation.get("match")
    if not match:
        # Substrait round-trip failed
        error_details = substrait_validation
        print(f"Substrait validation failed: {error_details}")
```

#### Manual Substrait Inspection

```python
from datafusion.substrait import Producer

# Access raw Substrait bytes
substrait_bytes = bundle.substrait_bytes

# Decode for inspection (requires protobuf)
if substrait_bytes:
    # Write to file for external tools
    with open("plan.substrait", "wb") as f:
        f.write(substrait_bytes)

    # Compute hash for comparison
    from utils.hashing import hash_sha256_hex
    substrait_hash = hash_sha256_hex(substrait_bytes)
    print(f"Substrait hash: {substrait_hash}")
```

### Policy Hash Investigation

The Delta store policy affects how Delta tables are accessed and impacts plan fingerprints.

#### Inspect Delta Store Policy

```python
from datafusion_engine.delta_store_policy import delta_store_policy_hash

# Get policy hash from runtime profile
profile = session_runtime.profile
policy = profile.delta_store_policy
policy_hash = delta_store_policy_hash(policy)

# Compare policy hashes between sessions
print(f"Policy hash: {policy_hash}")

# Check specific policy settings
if policy:
    print(f"Log store: {policy.log_store_backend}")
    print(f"AWS region: {policy.aws_region}")
    print(f"Storage options: {policy.storage_options}")
```

#### Policy Mismatch Resolution

```python
# Ensure consistent policy across sessions
from datafusion_engine.delta_store_policy import DeltaStorePolicy

policy = DeltaStorePolicy(
    log_store_backend="default",
    aws_region=None,
    storage_options={},
)

profile = DataFusionRuntimeProfile(delta_store_policy=policy)
```

## Hamilton Execution Tracing

### Using Hamilton Tracker for Debugging

The `CodeAnatomyHamiltonTracker` provides execution telemetry and lineage tracking.

#### Enable Hamilton Tracker

```python
from hamilton_pipeline.hamilton_tracker import CodeAnatomyHamiltonTracker
from hamilton_sdk import adapters

# Initialize tracker with run-scoped tags
tracker = CodeAnatomyHamiltonTracker(
    project_id=1,
    username="debug_user",
    dag_name="debug_dag",
    run_tag_provider=lambda: {"debug_run": "true", "session_id": "abc123"},
)

# Use with Hamilton driver
from hamilton import driver

dr = driver.Builder().with_adapters(tracker).build()
```

#### Inspect Execution Metadata

```python
# Tracker automatically captures:
# - Node execution times
# - Input/output schemas
# - Errors and exceptions
# - Task dependencies

# Access tracker state (before stop)
base_tags = tracker.base_tags  # Run-scoped tags

# Post-execution analysis via Hamilton UI or local logs
```

### OpenTelemetry Trace Analysis

CodeAnatomy emits OpenTelemetry spans for all major operations.

#### Configure OpenTelemetry Export

```python
from obs.otel.bootstrap import bootstrap_otel
from obs.otel.config import OtelConfig

config = OtelConfig(
    enable_tracing=True,
    enable_metrics=True,
    enable_logging=True,
    exporter_endpoint="http://localhost:4317",  # OTLP endpoint
)

bootstrap_otel(config)
```

#### Key Span Names for Debugging

| Span Name | Operation | Useful For |
|-----------|-----------|------------|
| `planning.plan_bundle` | Plan bundle construction | Fingerprint debugging |
| `planning.compilation` | SQL compilation | Query translation issues |
| `execution.materialize` | DataFrame materialization | Runtime errors |
| `datafusion.write` | Table write operations | Delta write failures |
| `lineage.extract` | Lineage extraction | Dependency inference debugging |
| `hamilton.execute` | Hamilton DAG execution | Pipeline orchestration issues |

#### Analyze Traces

```python
# Spans automatically include attributes:
# - codeanatomy.plan_fingerprint
# - codeanatomy.task_name
# - codeanatomy.view_name
# - codeanatomy.delta_version

# Use trace analysis tools (Jaeger, Zipkin, Honeycomb) to:
# - Identify slow operations
# - Trace error propagation
# - Inspect resource utilization
```

### Execution Flow Debugging

#### Enable Verbose Logging

```python
import logging

# Set logger for DataFusion operations
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("datafusion_engine")
logger.setLevel(logging.DEBUG)
```

#### Trace Execution Path

```python
from obs.otel.tracing import stage_span

# Manually instrument code sections
with stage_span("custom_debug", stage="debug", attributes={"context": "value"}):
    # Perform operation
    pass
```

### Node Failure Diagnosis

#### Capture Hamilton Node Errors

```python
# Hamilton tracker automatically captures node failures
# Access via post_graph_execute callback

def on_error(run_id, graph, error, **kwargs):
    print(f"Run {run_id} failed: {error}")
    # Inspect failed node
    # error.node_name if available

tracker.post_graph_execute = on_error
```

#### Inspect Task Graph Failures

```python
from relspec.rustworkx_graph import TaskGraph

# Access failed task from graph
graph: TaskGraph = ...
failed_tasks = [
    task for task in graph.nodes()
    if task.status == "failed"
]

for task in failed_tasks:
    print(f"Failed task: {task.name}")
    print(f"Error: {task.error}")
    print(f"Dependencies: {task.dependencies}")
```

## Delta Lake Version Pinning Issues

### Version Conflict Diagnosis

Version conflicts occur when multiple scan units reference the same Delta table at different versions.

#### Error Pattern

```
ValueError: Conflicting Delta pins for dataset 'my_table':
(123, None, ...) vs (124, None, ...)
```

#### Inspect Delta Input Pins

```python
# Access Delta inputs from plan bundle
delta_inputs = bundle.delta_inputs

for pin in delta_inputs:
    print(f"Dataset: {pin.dataset_name}")
    print(f"Version: {pin.version}")
    print(f"Timestamp: {pin.timestamp}")
    print(f"Feature gate: {pin.feature_gate}")
    print(f"Protocol: {pin.protocol}")
    print(f"Storage options hash: {pin.storage_options_hash}")
```

#### Resolution Strategies

**1. Explicit Version Pinning**

```python
from serde_artifacts import DeltaInputPin

# Pin to specific version
pins = [
    DeltaInputPin(
        dataset_name="my_table",
        version=123,
        timestamp=None,
        feature_gate=None,
        protocol=None,
        storage_options_hash=None,
    )
]

# Pass to plan bundle options
from datafusion_engine.plan_bundle import PlanBundleOptions

options = PlanBundleOptions(delta_inputs=pins)
```

**2. Timestamp-Based Pinning**

```python
# Pin to timestamp instead of version
pin = DeltaInputPin(
    dataset_name="my_table",
    version=None,
    timestamp="2024-01-15T10:30:00Z",
)
```

**3. Remove Version Pins**

```python
# Allow latest version (no pins)
options = PlanBundleOptions(delta_inputs=[])
```

### Protocol Compatibility Issues

Delta protocol compatibility checks prevent reading tables with unsupported features.

#### Check Protocol Compatibility

```python
from datafusion_engine.delta_protocol import DeltaProtocolSupport

# Define supported protocol
support = DeltaProtocolSupport(
    max_reader_version=2,
    max_writer_version=5,
    supported_reader_features=["deletionVectors"],
    supported_writer_features=["deletionVectors", "changeDataFeed"],
)

# Set in runtime profile
profile = DataFusionRuntimeProfile(delta_protocol_support=support)
```

#### Protocol Violation Error

```
DataFusionError: Delta table requires reader version 3, but only version 2 is supported
```

#### Resolution

```python
# Option 1: Upgrade supported protocol
support = DeltaProtocolSupport(
    max_reader_version=3,  # Increased
    supported_reader_features=["deletionVectors", "columnMapping"],
)

# Option 2: Disable protocol checks (unsafe)
profile = DataFusionRuntimeProfile(delta_protocol_mode="permissive")
```

### CDF Cursor Problems

Change Data Feed (CDF) cursors track incremental changes between Delta table versions.

#### Inspect CDF Windows

```python
# Access CDF windows from plan details
cdf_windows = bundle.plan_details.get("cdf_windows")

for window in cdf_windows:
    print(f"Dataset: {window['dataset_name']}")
    print(f"Table URI: {window['table_uri']}")
    print(f"Starting version: {window['starting_version']}")
    print(f"Ending version: {window['ending_version']}")
    print(f"Allow out of range: {window['allow_out_of_range']}")
```

#### CDF Cursor Error Patterns

**Out of Range Error**

```
DataFusionError: CDF starting version 100 is out of range [0, 50]
```

**Resolution:**

```python
from incremental.cdf_runtime import DeltaCDFOptions

# Allow out-of-range versions
cdf_options = DeltaCDFOptions(
    starting_version=100,
    ending_version=None,
    allow_out_of_range=True,  # Enable fallback
)
```

**Version Regression**

```
ValueError: CDF ending version 100 is before starting version 150
```

**Resolution:**

```python
# Reset cursor to latest
from incremental.changes import reset_cdf_cursor

reset_cdf_cursor(table_uri, dataset_name="my_table")
```

### Checkpoint Debugging

Delta checkpoints consolidate transaction logs for performance.

#### Inspect Checkpoint Status

```python
from deltalake import DeltaTable

dt = DeltaTable(table_uri)
version = dt.version()
print(f"Current version: {version}")

# Check for checkpoints
checkpoint_files = list(dt.table_uri().glob("_delta_log/*checkpoint*"))
print(f"Checkpoint files: {len(checkpoint_files)}")
```

#### Checkpoint-Related Errors

```
OSError: No checkpoint found at expected location
```

**Resolution:**

```python
# Force checkpoint creation
dt.create_checkpoint()

# Or repair checkpoint metadata
from deltalake import write_deltalake

write_deltalake(
    table_uri,
    data=...,
    mode="overwrite",
    overwrite_schema=True,
)
```

## UDF Compatibility Validation

### Rust UDF Registration Failures

#### Error Pattern

```
DataFusionError: UDF 'stable_hash64' not found in registry
```

#### Diagnose Registration

```python
from datafusion_engine.udf_runtime import rust_udf_snapshot

# Get registered UDFs
snapshot = rust_udf_snapshot(ctx)

# Check if UDF is registered
scalar_udfs = snapshot.get("scalar", [])
print(f"Registered scalar UDFs: {scalar_udfs}")

# Check aliases
aliases = snapshot.get("aliases", {})
print(f"UDF aliases: {aliases}")
```

#### Verify UDF Availability

```python
from datafusion_engine.udf_runtime import udf_names_from_snapshot

# Get all UDF names (including aliases)
available_udfs = udf_names_from_snapshot(snapshot)
print(f"Available UDFs: {sorted(available_udfs)}")

# Check required UDFs from plan
required_udfs = bundle.required_udfs
missing_udfs = set(required_udfs) - available_udfs
if missing_udfs:
    print(f"Missing UDFs: {missing_udfs}")
```

#### Resolution

```python
# Ensure UDFs are registered
from datafusion_ext import register_all_with_policy

register_all_with_policy(
    ctx,
    enable_async=True,
    async_udf_timeout_ms=5000,
    async_udf_batch_size=1024,
)
```

### Type Coercion Issues

#### Error Pattern

```
DataFusionError: Type mismatch: expected Int64, got Utf8
```

#### Inspect UDF Signatures

```python
# Access signature information from snapshot
signature_inputs = snapshot.get("signature_inputs", {})
return_types = snapshot.get("return_types", {})

udf_name = "stable_hash64"
input_signature = signature_inputs.get(udf_name)
return_type = return_types.get(udf_name)

print(f"{udf_name} signature:")
print(f"  Inputs: {input_signature}")
print(f"  Returns: {return_type}")
```

#### Use Explicit Casts

```python
from datafusion import functions as f, col

# Cast inputs to expected types
df = df.select(
    f.arrow_cast(col("input_col"), "Int64").alias("input_col"),
)

# Then apply UDF
result = df.select(f.call_udf("stable_hash64", [col("input_col")]))
```

### UDF Test Patterns

#### Conformance Testing

```rust
// See rust/datafusion_ext/tests/udf_conformance.rs

#[test]
fn test_stable_hash64_conformance() {
    let ctx = SessionContext::new();
    register_all(&ctx).unwrap();

    // Test with known inputs/outputs
    let result = ctx
        .sql("SELECT stable_hash64('test_value')")
        .unwrap()
        .collect()
        .unwrap();

    // Verify output
    assert_eq!(result[0].column(0).len(), 1);
}
```

#### Python-Rust Parity Testing

```python
# Test that Python and Rust implementations match
from datafusion import SessionContext, functions as f, col

ctx = SessionContext()

# Register Rust UDFs
from datafusion_ext import register_all
register_all(ctx)

# Test parity
df = ctx.sql("SELECT stable_hash64('test') as rust_hash")
result = df.to_pylist()[0]

# Compare with Python implementation (if exists)
from utils.hashing import stable_hash64_py
expected = stable_hash64_py("test")
assert result["rust_hash"] == expected
```

### PyO3 Boundary Debugging

#### Common PyO3 Errors

**Borrow Checker Violations**

```
pyo3::PyErr: RuntimeError: Already borrowed
```

**Resolution:** Ensure Python objects are not held across await points or shared mutably.

**Type Conversion Failures**

```
pyo3::PyErr: TypeError: Cannot convert type
```

**Resolution:** Use explicit conversions and check type compatibility.

#### Enable PyO3 Debug Logging

```bash
# Set environment variable
export RUST_LOG=pyo3=debug

# Run tests
uv run pytest tests/unit/
```

#### Inspect PyO3 Boundary

```python
# Check if function is Rust-based
import inspect

from datafusion_ext import stable_hash64_udf

# Rust UDFs won't have Python source
try:
    source = inspect.getsource(stable_hash64_udf)
except TypeError:
    print("This is a Rust UDF (no Python source)")
```

## Schema Contract Violations

### Schema Validation Error Interpretation

Schema validation errors are emitted when Arrow tables don't conform to expected schemas.

#### Run Schema Validation

```python
from datafusion_engine.schema_validation import (
    validate_table,
    ArrowValidationOptions,
)
from schema_spec.system import SystemTableSpec

# Define validation options
options = ArrowValidationOptions(
    strict="filter",  # Remove invalid rows
    coerce=False,     # Don't coerce types
    max_errors=10,    # Limit error count
    emit_invalid_rows=True,
    emit_error_table=True,
)

# Run validation
spec = SystemTableSpec.my_table  # Your table spec
report = validate_table(
    table,
    spec=spec,
    options=options,
    runtime_profile=profile,
)

# Inspect results
print(f"Valid: {report.valid}")
print(f"Validated rows: {report.validated.num_rows}")
print(f"Invalid rows: {report.invalid_rows.num_rows if report.invalid_rows else 0}")
```

#### Interpret Error Table

```python
# Access error table
errors = report.errors.to_pylist()

for error in errors:
    error_code = error["error_code"]
    error_column = error["error_column"]
    error_count = error["error_count"]

    print(f"{error_code} in column '{error_column}': {error_count} rows")
```

#### Error Code Reference

| Error Code | Meaning | Resolution |
|------------|---------|------------|
| `missing_column` | Required column not present | Add column or make optional |
| `extra_column` | Unexpected column present | Set `strict=False` or remove column |
| `type_mismatch` | Column type doesn't match | Cast column or update spec |
| `null_violation` | Non-nullable column has nulls | Fill nulls or make nullable |
| `missing_key_field` | Key field missing | Add key column |
| `duplicate_keys` | Primary key duplicates | Deduplicate rows |

### Contract Mismatch Resolution

#### Missing Column

```python
# Option 1: Add column with default value
import pyarrow.compute as pc

table = table.append_column(
    "missing_col",
    pa.array([None] * table.num_rows, type=pa.int64()),
)

# Option 2: Make column optional in spec
# Modify TableSchemaSpec to make field nullable
```

#### Type Mismatch

```python
# Option 1: Cast column to expected type
import pyarrow.compute as pc

corrected_col = pc.cast(table["wrong_type_col"], pa.string())
table = table.set_column(
    table.schema.get_field_index("wrong_type_col"),
    "wrong_type_col",
    corrected_col,
)

# Option 2: Enable coercion in validation
options = ArrowValidationOptions(coerce=True)
```

#### Null Violations

```python
# Fill nulls with default values
import pyarrow.compute as pc

filled_col = pc.fill_null(table["nullable_col"], default_value)
table = table.set_column(
    table.schema.get_field_index("nullable_col"),
    "nullable_col",
    filled_col,
)
```

### Column-Level Debugging

#### Inspect Column Statistics

```python
import pyarrow.compute as pc

col_data = table["my_column"]

# Basic stats
print(f"Type: {col_data.type}")
print(f"Length: {len(col_data)}")
print(f"Null count: {col_data.null_count}")

# Value distribution
value_counts = pc.value_counts(col_data)
print(f"Unique values: {len(value_counts)}")

# Min/max (for numeric/temporal types)
if pa.types.is_integer(col_data.type) or pa.types.is_floating(col_data.type):
    print(f"Min: {pc.min(col_data)}")
    print(f"Max: {pc.max(col_data)}")
```

#### Identify Invalid Rows

```python
# Filter to invalid rows using validation report
if report.invalid_rows is not None:
    invalid_df = report.invalid_rows.to_pandas()
    print(invalid_df.head())

    # Inspect specific invalid values
    print(invalid_df["problematic_column"].value_counts())
```

### Arrow Schema Issues

#### Schema Fingerprint Mismatch

```python
from datafusion_engine.arrow_schema.abi import schema_fingerprint

# Compare schema fingerprints
fp1 = schema_fingerprint(schema1)
fp2 = schema_fingerprint(schema2)

if fp1 != fp2:
    print("Schema mismatch detected")

    # Field-by-field comparison
    for field1, field2 in zip(schema1, schema2):
        if field1 != field2:
            print(f"Field mismatch: {field1.name}")
            print(f"  Expected: {field1.type}, nullable={field1.nullable}")
            print(f"  Actual: {field2.type}, nullable={field2.nullable}")
```

#### Schema Metadata Issues

```python
# Inspect schema metadata
metadata = table.schema.metadata

if metadata:
    for key, value in metadata.items():
        print(f"{key}: {value}")

# Check for ABI fingerprint
from datafusion_engine.schema_contracts import SCHEMA_ABI_FINGERPRINT_META

abi_key = SCHEMA_ABI_FINGERPRINT_META.decode("utf-8")
abi_value = metadata.get(abi_key)

if abi_value:
    print(f"Explicit schema ABI fingerprint: {abi_value}")
```

## Cache Invalidation Debugging

### Cache Key Investigation

CodeAnatomy uses plan fingerprints as cache keys for deterministic caching.

#### Inspect Cache Key Construction

```python
# Cache key is derived from plan fingerprint
cache_key = bundle.plan_fingerprint

# Examine components
from datafusion_engine.plan_bundle import PlanFingerprintInputs

inputs = PlanFingerprintInputs(
    substrait_bytes=bundle.substrait_bytes,
    df_settings=bundle.artifacts.df_settings,
    planning_env_hash=bundle.artifacts.planning_env_hash,
    rulepack_hash=bundle.artifacts.rulepack_hash,
    information_schema_hash=bundle.artifacts.information_schema_hash,
    udf_snapshot_hash=bundle.artifacts.udf_snapshot_hash,
    required_udfs=bundle.required_udfs,
    required_rewrite_tags=bundle.required_rewrite_tags,
    delta_inputs=bundle.delta_inputs,
    delta_store_policy_hash=...,
)

# Recompute fingerprint
from datafusion_engine.plan_bundle import _hash_plan
recomputed = _hash_plan(inputs)
assert recomputed == cache_key
```

#### Cache Hit/Miss Debugging

```python
# Enable cache diagnostics
from obs.diagnostics import DiagnosticsCollector

sink = DiagnosticsCollector()
profile = DataFusionRuntimeProfile(diagnostics_sink=sink)

# After execution, inspect cache events
cache_events = sink.events.get("cache_access", [])

for event in cache_events:
    print(f"Cache key: {event['cache_key']}")
    print(f"Hit: {event['hit']}")
    print(f"Source: {event['source']}")
```

### Lineage-Based Invalidation Issues

Hamilton nodes are invalidated based on upstream changes detected via cache lineage.

#### Inspect Cache Lineage

```python
# Access cache lineage from diagnostics
lineage_artifacts = sink.artifacts.get("hamilton_cache_lineage_v2", [])

if lineage_artifacts:
    summary = lineage_artifacts[-1]

    print(f"Run ID: {summary['run_id']}")
    print(f"Total nodes: {summary['total_nodes']}")
    print(f"Cache hits: {summary['cache_hits']}")
    print(f"Cache misses: {summary['cache_misses']}")
    print(f"Invalidated: {summary['invalidated_nodes']}")

# Per-node lineage
node_events = sink.events.get("hamilton_cache_lineage_nodes_v1", [])

for node in node_events:
    print(f"Node: {node['node_name']}")
    print(f"  Status: {node['cache_status']}")
    print(f"  Hash: {node['content_hash']}")
    print(f"  Upstream hashes: {node['upstream_hashes']}")
```

#### Debug Unexpected Invalidation

```python
# Identify nodes that were invalidated unexpectedly
invalidated = [
    node for node in node_events
    if node["cache_status"] == "invalidated"
]

for node in invalidated:
    print(f"Invalidated: {node['node_name']}")

    # Check which upstream changed
    upstream_changed = node.get("upstream_changed", [])
    if upstream_changed:
        print(f"  Caused by: {upstream_changed}")
```

### DiskCache Inspection

#### Access DiskCache Directory

```python
from cache.diskcache_factory import DiskCacheSettings

settings = DiskCacheSettings(
    cache_dir="/path/to/cache",
    size_limit=10 * 1024**3,  # 10 GB
)

# Inspect cache contents
import os

cache_files = list(os.listdir(settings.cache_dir))
print(f"Cache files: {len(cache_files)}")

# Check cache size
import shutil
cache_size = shutil.disk_usage(settings.cache_dir).used
print(f"Cache size: {cache_size / 1024**3:.2f} GB")
```

#### Clear Cache Manually

```python
import shutil

# Remove entire cache
shutil.rmtree(settings.cache_dir)
os.makedirs(settings.cache_dir)

# Or remove specific entries
import glob

# Remove all plan cache entries
plan_caches = glob.glob(f"{settings.cache_dir}/**/plan_*.cache", recursive=True)
for path in plan_caches:
    os.remove(path)
```

### Cache Corruption Recovery

#### Detect Corruption

```python
# Corruption symptoms:
# - Deserialization errors
# - Type mismatches
# - Missing fields

try:
    cached_value = cache.get(key)
except Exception as e:
    print(f"Cache corruption detected: {e}")
    # Remove corrupted entry
    cache.delete(key)
```

#### Rebuild Cache

```python
# Force cache rebuild by clearing and re-running pipeline
from graph import GraphProductBuildRequest, build_graph_product

# Clear cache
shutil.rmtree(cache_dir)

# Rebuild
result = build_graph_product(
    GraphProductBuildRequest(
        repo_root=".",
        force_rebuild=True,
    )
)
```

## Common Error Patterns and Resolutions

### Error Reference Table

| Error Pattern | Root Cause | Resolution |
|---------------|------------|------------|
| `ValueError: Conflicting Delta pins` | Multiple scan units reference same table at different versions | Use consistent version pins or remove pins |
| `DataFusionError: UDF 'X' not found` | UDF not registered in session context | Call `register_all_with_policy(ctx)` |
| `TypeError: Cannot convert type` | PyO3 type conversion failure | Use explicit type annotations and conversions |
| `RuntimeError: Already borrowed` | PyO3 borrow checker violation | Avoid holding Python objects across await points |
| `ArrowError: Schema mismatch` | Column types don't match expected schema | Use `align_to_schema` or `validate_table` |
| `DataFusionError: Column 'X' not found` | Missing required column | Add column or update view definition |
| `DeltaTableError: Version X not found` | Requested version doesn't exist | Use valid version or timestamp |
| `OSError: No checkpoint found` | Checkpoint metadata corrupted | Recreate checkpoint with `dt.create_checkpoint()` |
| `Substrait validation failed` | Plan round-trip mismatch | Check DataFusion version compatibility |
| `Cache key mismatch` | Fingerprint components changed | Inspect determinism audit to identify changed component |

### Environment Setup Issues

#### Missing Rust Extension

```
ImportError: No module named 'datafusion_ext'
```

**Resolution:**

```bash
# Rebuild Rust extension
uv run maturin develop --release
```

#### DataFusion Version Mismatch

```
AttributeError: 'SessionContext' has no attribute 'register_udtf'
```

**Resolution:**

```bash
# Check DataFusion version
uv run python -c "import datafusion; print(datafusion.__version__)"

# Reinstall with correct version
uv sync --reinstall-package datafusion
```

### Dependency Resolution Problems

#### Conflicting Dependencies

```
ERROR: Cannot install conflicting dependencies
```

**Resolution:**

```bash
# Clear UV cache
uv cache clean

# Reinstall from scratch
rm -rf .venv uv.lock
uv sync
```

#### Missing Optional Dependencies

```
ImportError: No module named 'deltalake'
```

**Resolution:**

```bash
# Install optional dependencies
uv sync --all-extras
```

### Memory and Resource Issues

#### Out of Memory During Execution

```
DataFusionError: Memory limit exceeded
```

**Resolution:**

```python
# Increase memory limit
profile = DataFusionRuntimeProfile(
    memory_limit_bytes=16 * 1024**3,  # 16 GB
)

# Enable spilling to disk
profile = DataFusionRuntimeProfile(
    spill_dir="/tmp/datafusion_spill",
)
```

#### Too Many Partitions

```
Warning: Execution plan has 1000+ partitions
```

**Resolution:**

```python
# Reduce target partitions
profile = DataFusionRuntimeProfile(
    target_partitions=8,  # Match CPU cores
    repartition_file_scans=False,  # Disable automatic repartitioning
)
```

## Diagnostic Commands

### Useful Debugging Commands

#### Inspect DataFusion Version and Features

```bash
uv run python -c "
import datafusion
import datafusion_ext

print(f'DataFusion: {datafusion.__version__}')
print(f'datafusion_ext features: {datafusion_ext.__dict__.keys()}')
"
```

#### Dump Plan Bundle to JSON

```python
import json
from serde_msgspec import to_builtins

# Serialize plan artifacts
artifacts_dict = to_builtins(bundle.artifacts, str_keys=True)

with open("plan_artifacts.json", "w") as f:
    json.dump(artifacts_dict, f, indent=2)
```

#### Query information_schema

```python
from datafusion import SessionContext

ctx = SessionContext()

# List registered tables
tables = ctx.sql("SELECT * FROM information_schema.tables").to_pylist()
for row in tables:
    print(f"{row['table_schema']}.{row['table_name']}")

# List registered functions
functions = ctx.sql("SELECT routine_name FROM information_schema.routines").to_pylist()
print(f"Registered UDFs: {sorted(r['routine_name'] for r in functions)}")
```

#### Inspect Delta Table Metadata

```bash
# Using delta-inspect CLI
uv run delta-inspect info /path/to/delta/table

# Get table version
uv run python -c "
from deltalake import DeltaTable
dt = DeltaTable('/path/to/delta/table')
print(f'Version: {dt.version()}')
print(f'Files: {len(dt.files())}')
"
```

### Log Analysis Tips

#### Enable Structured Logging

```python
import logging
import json

class StructuredFormatter(logging.Formatter):
    def format(self, record):
        log_obj = {
            "timestamp": self.formatTime(record),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if hasattr(record, "plan_fingerprint"):
            log_obj["plan_fingerprint"] = record.plan_fingerprint
        return json.dumps(log_obj)

handler = logging.StreamHandler()
handler.setFormatter(StructuredFormatter())

logger = logging.getLogger("datafusion_engine")
logger.addHandler(handler)
logger.setLevel(logging.INFO)
```

#### Filter Logs by Span

```bash
# If using OpenTelemetry exporter with file backend
grep "planning.plan_bundle" otel_traces.jsonl | jq '.duration_ms'
```

### Environment Inspection

#### Check Environment Variables

```python
from utils.env_utils import (
    bool_from_env,
    int_from_env,
    float_from_env,
    str_from_env,
)

# Check feature flags
use_inferred_deps = bool_from_env("USE_INFERRED_DEPS", default=True)
target_partitions = int_from_env("DATAFUSION_TARGET_PARTITIONS", default=8)

print(f"USE_INFERRED_DEPS: {use_inferred_deps}")
print(f"DATAFUSION_TARGET_PARTITIONS: {target_partitions}")
```

#### Validate Runtime Configuration

```python
# Dump full runtime profile settings
profile = DataFusionRuntimeProfile()
settings_payload = profile.settings_payload()

import json
print(json.dumps(settings_payload, indent=2))
```

---

## Quick Reference

### Essential Debugging Workflow

1. **Enable diagnostics**: Create `DiagnosticsCollector` and attach to `DataFusionRuntimeProfile`
2. **Capture plan bundle**: Inspect `plan_details`, `plan_fingerprint`, and `artifacts`
3. **Check fingerprint audit**: Review `determinism_audit` for changed components
4. **Inspect UDF snapshot**: Verify required UDFs are registered
5. **Validate schema**: Use `validate_table` to check contract compliance
6. **Review traces**: Analyze OpenTelemetry spans for performance bottlenecks
7. **Check cache lineage**: Inspect Hamilton cache events for unexpected invalidation

### Key Files for Debugging

- `/home/paul/CodeAnatomy/src/datafusion_engine/plan_bundle.py` - Plan bundle construction and fingerprinting
- `/home/paul/CodeAnatomy/src/datafusion_engine/diagnostics.py` - Diagnostics recording infrastructure
- `/home/paul/CodeAnatomy/src/datafusion_engine/schema_validation.py` - Schema contract validation
- `/home/paul/CodeAnatomy/src/obs/diagnostics.py` - Observability sink for telemetry
- `/home/paul/CodeAnatomy/rust/datafusion_ext/src/udf_registry.rs` - Rust UDF registration
- `/home/paul/CodeAnatomy/rust/datafusion_ext/src/errors.rs` - Error type definitions
