# Scope 3: Unbounded External Tables - Data Flow

## Complete Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│ 1. Dataset Specification                                             │
│                                                                       │
│   DatasetSpec(                                                        │
│     table_spec=TableSchemaSpec(...),                                  │
│     datafusion_scan=DataFusionScanOptions(unbounded=True) ◄─────┐    │
│   )                                                             │    │
└────────────────────────────────┬────────────────────────────────┘    │
                                 │                                     │
                                 ▼                                     │
┌─────────────────────────────────────────────────────────────────────┤
│ 2. Streaming Detection (NEW)                                    │    │
│                                                                  │    │
│   spec.is_streaming  ──────────────────────────────────────────┘    │
│   Returns: True if datafusion_scan.unbounded is True                 │
└────────────────────────────────┬────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│ 3. Configuration Generation (NEW)                                    │
│                                                                       │
│   config = spec.external_table_config_with_streaming(                │
│       location='/path/to/streaming',                                 │
│       file_format='PARQUET'                                           │
│   )                                                                   │
│                                                                       │
│   Creates: ExternalTableConfig(unbounded=True, ...)                  │
└────────────────────────────────┬────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│ 4. DDL Generation (EXISTING - line 618 in specs.py)                 │
│                                                                       │
│   ddl = spec.external_table_sql(config)                              │
│                                                                       │
│   Generates:                                                          │
│   ┌───────────────────────────────────────────────────────────────┐ │
│   │ CREATE UNBOUNDED EXTERNAL TABLE streaming_events (           │ │
│   │   event_id VARCHAR NOT NULL,                                 │ │
│   │   timestamp TIMESTAMP,                                       │ │
│   │   payload VARCHAR                                            │ │
│   │ )                                                            │ │
│   │ STORED AS PARQUET                                            │ │
│   │ LOCATION '/path/to/streaming'                                │ │
│   └───────────────────────────────────────────────────────────────┘ │
└────────────────────────────────┬────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│ 5. DDL-Based Registration (NEW)                                      │
│                                                                       │
│   register_dataset_ddl(ctx, "streaming_events", spec, location)      │
│                                                                       │
│   Actions:                                                            │
│   a) Execute DDL: ctx.sql_with_options(ddl, options).collect()       │
│   b) Record Metadata:                                                 │
│      TableProviderMetadata(                                           │
│          table_name="streaming_events",                               │
│          ddl=ddl,                                                     │
│          unbounded=True,  ◄────────── NEW FIELD                      │
│          ddl_fingerprint=hash(ddl),  ◄─ NEW FIELD                    │
│          ...                                                          │
│      )                                                                │
└────────────────────────────────┬────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│ 6. DataFusion Internal Processing                                    │
│                                                                       │
│   DataFusion's DDL parser recognizes "UNBOUNDED EXTERNAL" and        │
│   sets up streaming semantics internally:                            │
│                                                                       │
│   - Marks table as unbounded in catalog                              │
│   - Applies appropriate execution strategies                         │
│   - Enables streaming-specific optimizations                         │
└─────────────────────────────────────────────────────────────────────┘
```

## Alternative Path: Existing register_dataset_df()

```
┌─────────────────────────────────────────────────────────────────────┐
│ DatasetLocation with scan options                                    │
│                                                                       │
│   location = DatasetLocation(                                         │
│       path='/path/to/data',                                           │
│       format='parquet',                                               │
│       scan_options={'unbounded': True}  ◄────────────────────────┐   │
│   )                                                               │   │
└────────────────────────────────┬──────────────────────────────────┘   │
                                 │                                      │
                                 ▼                                      │
┌─────────────────────────────────────────────────────────────────────┤
│ register_dataset_df(ctx, name=..., location=location)           │   │
│                                                                  │   │
│ Internally:                                                      │   │
│ 1. Resolves scan options (line 694)                             │   │
│ 2. Extracts unbounded flag (line 701) ─────────────────────────┘   │
│ 3. Creates ExternalTableConfigOverrides(unbounded=True)             │
│ 4. Generates DDL via datafusion_external_table_sql()                │
│ 5. Routes to _register_external_table() (line 932 check)            │
│ 6. Executes DDL (line 1074)                                         │
│ 7. Records metadata with unbounded flag (line 923)                  │
└─────────────────────────────────────────────────────────────────────┘
```

## Key Integration Points

### 1. DDL Generation (`specs.py:618`)
```python
kind = "UNBOUNDED EXTERNAL" if config.unbounded else "EXTERNAL"
```
- Already implemented
- Triggered by `config.unbounded=True`

### 2. Registration Routing (`registry_bridge.py:932`)
```python
if (scan is not None and scan.unbounded) or _should_register_external_table(context):
    return _register_external_table(context)
```
- Already implemented
- Ensures unbounded sources use DDL path

### 3. Metadata Recording (`registry_bridge.py:923`)
```python
unbounded=options.scan.unbounded if options.scan else False
```
- Enhanced in this implementation
- Now includes `ddl_fingerprint` as well

## New vs Existing Code

### NEW Components (This Implementation)
- ✨ `DatasetSpec.is_streaming` property
- ✨ `DatasetSpec.external_table_config_with_streaming()` method
- ✨ `register_dataset_ddl()` public API function
- ✨ `TableProviderMetadata.unbounded` field
- ✨ `TableProviderMetadata.ddl_fingerprint` field

### EXISTING Components (Already Working)
- ✓ `ExternalTableConfig.unbounded` field
- ✓ `ExternalTableConfigOverrides.unbounded` field
- ✓ `DataFusionScanOptions.unbounded` field
- ✓ DDL generation with "UNBOUNDED EXTERNAL" keyword
- ✓ Registration routing based on unbounded flag
- ✓ Metadata extraction from scan options

## Benefits of New Implementation

1. **Explicit API**: `register_dataset_ddl()` makes streaming intent clear
2. **Type Safety**: `is_streaming` property provides clean boolean access
3. **Helper Method**: `external_table_config_with_streaming()` encapsulates logic
4. **Enhanced Diagnostics**: `unbounded` and `ddl_fingerprint` in metadata
5. **Documentation**: Clear examples and flow for streaming sources
6. **Zero Breaking Changes**: All existing code continues to work

## Usage Patterns

### Pattern 1: Direct DDL Registration (New - Recommended for Streaming)
```python
spec = DatasetSpec(
    table_spec=table_spec,
    datafusion_scan=DataFusionScanOptions(unbounded=True)
)
register_dataset_ddl(ctx, "events", spec, "/path/to/streaming")
```

### Pattern 2: Dataset Location (Existing - Still Works)
```python
location = DatasetLocation(
    path='/path/to/streaming',
    format='parquet',
    dataset_spec=DatasetSpec(
        table_spec=table_spec,
        datafusion_scan=DataFusionScanOptions(unbounded=True)
    )
)
register_dataset_df(ctx, name="events", location=location)
```

### Pattern 3: Manual DDL Generation (Existing - Advanced)
```python
ddl = datafusion_external_table_sql(
    name="events",
    location=location,
    runtime_profile=profile
)
ctx.sql_with_options(ddl, options).collect()
```
