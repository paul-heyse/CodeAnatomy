# Scope 3 Implementation Summary: Unbounded External Tables for Streaming Sources

## Overview

Implemented support for `CREATE UNBOUNDED EXTERNAL TABLE` DDL for streaming sources, enabling proper streaming semantics in DataFusion through DDL-based registration.

## Key Changes

### 1. DatasetSpec Enhancements (`src/schema_spec/system.py`)

Added two new methods to `DatasetSpec`:

#### `is_streaming` Property
```python
@property
def is_streaming(self) -> bool:
    """Return True if this dataset represents a streaming source."""
    if self.datafusion_scan is not None:
        return self.datafusion_scan.unbounded
    return False
```

#### `external_table_config_with_streaming()` Method
```python
def external_table_config_with_streaming(
    self,
    *,
    location: str,
    file_format: str,
    overrides: ExternalTableConfigOverrides | None = None,
) -> ExternalTableConfig:
    """Return ExternalTableConfig with streaming flag from scan options."""
```

This method creates an `ExternalTableConfig` that properly propagates the `unbounded` flag from `DataFusionScanOptions` to the DDL generation.

### 2. TableProviderMetadata Extensions (`src/datafusion_engine/table_provider_metadata.py`)

Added two new fields to track streaming and DDL provenance:

```python
@dataclass(frozen=True)
class TableProviderMetadata:
    # ... existing fields ...
    unbounded: bool = False
    ddl_fingerprint: str | None = None
```

Updated all `with_*` methods to preserve these new fields.

### 3. Registry Bridge Enhancements (`src/datafusion_engine/registry_bridge.py`)

#### New Public API: `register_dataset_ddl()`
Added a dedicated function for DDL-based registration:

```python
def register_dataset_ddl(
    ctx: SessionContext,
    name: str,
    spec: DatasetSpec,
    location: str,
    *,
    file_format: str = "PARQUET",
    sql_options: SQLOptions | None = None,
) -> None:
    """Register dataset using DDL (preferred path for streaming sources)."""
```

This function:
- Uses the new `external_table_config_with_streaming()` method
- Executes the DDL with proper SQL options
- Records comprehensive metadata including `unbounded` and `ddl_fingerprint`

#### Enhanced Metadata Recording
Updated metadata recording in `_build_registration_context()` to capture:
- `unbounded` flag from scan options
- `ddl_fingerprint` for DDL provenance tracking

## How It Works

### Flow for Streaming Sources

1. **Define DatasetSpec with unbounded flag**:
   ```python
   spec = DatasetSpec(
       table_spec=table_spec,
       datafusion_scan=DataFusionScanOptions(unbounded=True),
   )
   ```

2. **Generate DDL with streaming semantics**:
   ```python
   config = spec.external_table_config_with_streaming(
       location='/path/to/streaming',
       file_format='PARQUET',
   )
   ddl = spec.external_table_sql(config)
   ```

3. **Register using DDL**:
   ```python
   register_dataset_ddl(ctx, "streaming_events", spec, "/path/to/streaming")
   ```

### Generated DDL Example

For a streaming source, the generated DDL will be:

```sql
CREATE UNBOUNDED EXTERNAL TABLE streaming_events (
  event_id VARCHAR NOT NULL,
  timestamp TIMESTAMP,
  payload VARCHAR
)
STORED AS PARQUET
LOCATION '/path/to/streaming'
```

Note the `UNBOUNDED EXTERNAL` keywords instead of just `EXTERNAL`.

## Integration Points

### Existing Code Compatibility

The implementation integrates seamlessly with existing code:

1. **Existing DDL generation** already supported `unbounded` flag (line 618 in `specs.py`)
2. **Existing registration routing** already checks `scan.unbounded` (line 932 in `registry_bridge.py`)
3. **Existing metadata generation** already extracts `unbounded` from scan options (line 701 in `registry_bridge.py`)

### New Capabilities

The new code provides:
1. Clean property access via `spec.is_streaming`
2. Helper method for DDL config generation that respects streaming flag
3. Dedicated public API for DDL-based registration
4. Enhanced diagnostics metadata tracking

## Files Modified

1. **`src/schema_spec/system.py`**
   - Added `is_streaming` property to `DatasetSpec`
   - Added `external_table_config_with_streaming()` method
   - Added `ExternalTableConfigOverrides` import

2. **`src/datafusion_engine/table_provider_metadata.py`**
   - Added `unbounded` field
   - Added `ddl_fingerprint` field
   - Updated all `with_*` methods to preserve new fields
   - Updated docstring

3. **`src/datafusion_engine/registry_bridge.py`**
   - Added `register_dataset_ddl()` function
   - Enhanced metadata recording with `unbounded` and `ddl_fingerprint`
   - Added to `__all__` exports

## Testing Recommendations

To test the implementation:

```python
from datafusion import SessionContext
from schema_spec.system import DatasetSpec, DataFusionScanOptions
from schema_spec.specs import TableSchemaSpec, ArrowFieldSpec
from datafusion_engine.registry_bridge import register_dataset_ddl
import pyarrow as pa

# Create streaming dataset spec
table_spec = TableSchemaSpec(
    name='streaming_events',
    fields=[
        ArrowFieldSpec(name='event_id', dtype=pa.string(), nullable=False),
        ArrowFieldSpec(name='timestamp', dtype=pa.timestamp('us')),
        ArrowFieldSpec(name='payload', dtype=pa.string()),
    ]
)

spec = DatasetSpec(
    table_spec=table_spec,
    datafusion_scan=DataFusionScanOptions(unbounded=True),
)

# Register as streaming source
ctx = SessionContext()
register_dataset_ddl(ctx, "streaming_events", spec, "s3://bucket/streaming/")

# Verify registration
df = ctx.sql("SELECT * FROM streaming_events")
```

## Benefits

1. **Type Safety**: Streaming sources are explicitly marked in the type system
2. **DDL-First**: Uses DataFusion's native DDL parsing for better compatibility
3. **Diagnostics**: Full metadata tracking for debugging and introspection
4. **Clean API**: Simple, focused function for streaming registration
5. **Backward Compatible**: No breaking changes to existing code
