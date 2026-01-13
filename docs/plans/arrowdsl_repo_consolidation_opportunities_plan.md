## ArrowDSL Repo Consolidation Opportunities Plan

### Goals
- Centralize repeated Arrow patterns in `src/arrowdsl` to reduce bespoke implementations.
- Make schema alignment, hashing, joins, and serialization consistent across pipelines.
- Minimize duplicate type/metadata definitions and IO entrypoints.
- Keep plan-lane and kernel-lane APIs uniform and predictable.

### Constraints
- Preserve existing behavior, schema metadata, and ordering semantics.
- Keep all modules fully typed (pyright strict + pyrefly).
- Avoid relative imports; keep public APIs explicit and stable.
- Prefer Arrow-native helpers over ad hoc Python loops.

---

### Scope 1: Schema Inference + Alignment Wrappers
**Description**
Replace local alignment and schema inference wrappers with ArrowDSL-native helpers.

**Code patterns**
```python
# src/arrowdsl/schema/infer.py
schema = unify_schemas(schemas, promote_options="permissive", prefer_nested=True)

# src/arrowdsl/plan_helpers.py
exprs, names = projection_for_schema(schema, available=plan.schema(ctx=ctx).names)
plan = plan.project(exprs, names, ctx=ctx)
```

**Target files**
- Update: `src/arrowdsl/schema/infer.py`
- Update: `src/arrowdsl/plan_helpers.py`
- Update: `src/normalize/schema_infer.py`
- Update: `src/extract/tables.py`
- Update: `src/normalize/plan_helpers.py`
- Update: `src/relspec/compiler.py`

**Implementation checklist**
- [ ] Add ArrowDSL-first inference + alignment helpers with consistent options.
- [ ] Replace local wrappers with ArrowDSL imports.
- [ ] Ensure schema metadata propagation remains intact.

**Status**
Planned.

---

### Scope 2: Encoding Metadata + Dictionary Field Consolidation
**Description**
Centralize dictionary encoding metadata constants, parsing, and field factories.

**Code patterns**
```python
# src/arrowdsl/schema/encoding.py
field = dict_field("kind", ordered=True, metadata={"encoding": "dictionary"})
policy = encoding_policy_from_schema(schema)
```

**Target files**
- Update: `src/arrowdsl/schema/encoding.py`
- Update: `src/schema_spec/specs.py`
- Update: `src/normalize/encoding.py`
- Update: `src/normalize/schemas.py`

**Implementation checklist**
- [ ] Move dict field factory + metadata parsing into ArrowDSL encoding helpers.
- [ ] Reuse ArrowDSL encoding constants from schema_spec and normalize.
- [ ] Remove duplicate metadata parsing logic.

**Status**
Planned.

---

### Scope 3: Hash + ID Helper Centralization
**Description**
Unify hash column creation, plan-lane hash projections, and stable ID helpers.

**Code patterns**
```python
# src/arrowdsl/compute/ids.py
expr, out_col = hash_projection(spec, available=names, required=req)
plan = plan.project(exprs + [expr], names + [out_col], ctx=ctx)
```

**Target files**
- Update: `src/arrowdsl/compute/ids.py`
- Update: `src/arrowdsl/plan_helpers.py`
- Update: `src/extract/hashing.py`
- Update: `src/normalize/ids.py`
- Update: `src/extract/hash_specs.py`
- Update: `src/normalize/hash_specs.py`
- Update: `src/cpg/hash_specs.py`

**Implementation checklist**
- [ ] Move hash column helpers into ArrowDSL (kernel + plan lanes).
- [ ] Centralize HashSpec registries or factories where possible.
- [ ] Replace local ID helpers with ArrowDSL APIs.

**Status**
Planned.

---

### Scope 4: Join Config + Projection Defaults
**Description**
Provide standardized join helpers that handle key selection, output columns, and defaults.

**Code patterns**
```python
# src/arrowdsl/plan/joins.py
config = JoinConfig.from_sequences(
    left_keys=("object_key",),
    right_keys=("object_key",),
    left_output=left_cols,
    right_output=("rt_id",),
)
joined = left_join(left, right, config=config, ctx=ctx)
```

**Target files**
- Update: `src/arrowdsl/plan/joins.py`
- Update: `src/arrowdsl/plan_helpers.py`
- Update: `src/normalize/arrow_utils.py`
- Update: `src/extract/runtime_inspect_extract.py`
- Update: `src/extract/symtable_extract.py`
- Update: `src/hamilton_pipeline/modules/normalization.py`

**Implementation checklist**
- [ ] Add join config factories for common left-join patterns.
- [ ] Replace repeated `JoinConfig.from_sequences` blocks.
- [ ] Ensure output projections stay deterministic.

**Status**
Planned.

---

### Scope 5: Plan Finalization + Alignment Helpers
**Description**
Consolidate plan finalization and schema alignment logic under ArrowDSL.

**Code patterns**
```python
# src/arrowdsl/plan_helpers.py
result = finalize_plan(plan, ctx=ctx, schema=target_schema)
```

**Target files**
- Update: `src/arrowdsl/plan_helpers.py`
- Update: `src/normalize/plan_helpers.py`
- Update: `src/extract/tables.py`
- Update: `src/relspec/compiler.py`

**Implementation checklist**
- [ ] Centralize `finalize_plan` and `align_plan_to_schema` helpers.
- [ ] Remove duplicate alignment wrappers.
- [ ] Keep ordering metadata and pipeline-breaker behavior unchanged.

**Status**
Planned.

---

### Scope 6: JSON Serialization Utilities
**Description**
Reuse ArrowDSL JSON factories across obs, cpg, and pipeline modules.

**Code patterns**
```python
# src/arrowdsl/json_factory.py
payload = dumps_text(value, policy=JsonPolicy(sort_keys=True))
```

**Target files**
- Update: `src/arrowdsl/json_factory.py`
- Update: `src/obs/repro.py`
- Update: `src/obs/manifest.py`
- Update: `src/cpg/emit_props.py`
- Update: `src/cpg/builders.py`

**Implementation checklist**
- [ ] Expose a shared JSON default/serializer in ArrowDSL.
- [ ] Replace ad hoc JSON serialization in obs/cpg.
- [ ] Ensure ASCII policies remain consistent where required.

**Status**
Planned.

---

### Scope 7: IO Surface Consolidation (IPC/Parquet)
**Description**
Make ArrowDSL IO helpers the single entrypoint for IPC/Parquet operations.

**Code patterns**
```python
# src/arrowdsl/io/parquet.py
write_table_parquet(table, path, opts=ParquetWriteOptions())
```

**Target files**
- Update: `src/arrowdsl/io/ipc.py`
- Update: `src/arrowdsl/io/parquet.py`
- Update: `src/storage/ipc.py`
- Update: `src/storage/parquet.py`
- Update: `src/hamilton_pipeline/arrow_adapters.py`

**Implementation checklist**
- [ ] Ensure IO options are centralized in ArrowDSL.
- [ ] Update storage wrappers to delegate or remove them.
- [ ] Keep metadata sidecar behavior intact.

**Status**
Planned.

---

### Scope 8: Spec Registration + Metadata Factories
**Description**
Unify dataset registration and metadata bundling via ArrowDSL spec factories.

**Code patterns**
```python
# src/arrowdsl/spec/factories.py
spec = register_dataset(name=name, version=version, fields=fields, bundles=bundles)
```

**Target files**
- Add: `src/arrowdsl/spec/factories.py`
- Update: `src/extract/spec_helpers.py`
- Update: `src/normalize/schemas.py`
- Update: `src/schema_spec/system.py`

**Implementation checklist**
- [ ] Add a shared dataset registration helper in ArrowDSL.
- [ ] Route extract/normalize schema registration through it.
- [ ] Preserve metadata and ordering policies.

**Status**
Planned.

---

### Scope 9: Type/Struct Factory Consolidation
**Description**
Centralize small Arrow type constructors used across modules.

**Code patterns**
```python
# src/arrowdsl/schema/arrays.py
list_type = list_view_type(pa.string(), large=True)
struct = struct_type({"kind": pa.string(), "value": pa.int64()})
```

**Target files**
- Update: `src/arrowdsl/schema/arrays.py`
- Update: `src/schema_spec/specs.py`
- Update: `src/normalize/schemas.py`

**Implementation checklist**
- [ ] Move list/struct/map helpers to ArrowDSL arrays.
- [ ] Replace local constructors in schema_spec/normalize.
- [ ] Keep type factories ASCII-only and fully typed.

**Status**
Planned.
