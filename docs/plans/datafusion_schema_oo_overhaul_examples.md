# DataFusion Schema-Driven Overhaul Examples

These examples show how to use the new schema-hardening, view specs, and
registry-driven catalog helpers introduced in the overhaul.

## Schema hardening + runtime profile

```python
from datafusion_engine.runtime import DataFusionRuntimeProfile, SchemaHardeningProfile

profile = DataFusionRuntimeProfile(
    schema_hardening_name="schema_hardening",
    schema_hardening=SchemaHardeningProfile(enable_view_types=True),
)
ctx = profile.session_context()
```

## Dataset handle + external table DDL

```python
from schema_spec.system import dataset_spec_from_schema
from schema_spec.dataset_handle import DatasetHandle

spec = dataset_spec_from_schema("my_table", schema)
handle = spec.to_handle()
ddl = handle.ddl(
    location="/data/my_table",
    file_format="parquet",
)
```

## View specs and registration

```python
import pyarrow as pa

from datafusion_engine.runtime import DataFusionRuntimeProfile, register_view_specs
from schema_spec.view_specs import ViewSpec

profile = DataFusionRuntimeProfile()
ctx = profile.session_context()

view = ViewSpec(
    name="example_view",
    sql="SELECT col_a, col_b FROM my_table",
    schema=pa.schema(
        [
            pa.field("col_a", pa.string(), nullable=False),
            pa.field("col_b", pa.int64(), nullable=True),
        ]
    ),
)
register_view_specs(ctx, views=(view,), runtime_profile=profile)
```

## Schema introspection

```python
from datafusion_engine.schema_introspection import SchemaIntrospector

introspector = SchemaIntrospector(ctx)
columns = introspector.table_columns("my_table")
settings = introspector.settings_snapshot()
```

## Schema inference harness

```python
from schema_spec.schema_inference import SchemaInferenceHarness

harness = SchemaInferenceHarness()
result = harness.infer("inferred_table", table)
spec = result.spec
ddl_fingerprint = result.ddl_fingerprint
```

## Registry-backed catalog provider

```python
from datafusion_engine.catalog_provider import register_registry_catalog

register_registry_catalog(
    ctx,
    registry=dataset_registry,
    catalog_name="registry",
    schema_name="public",
)
```
