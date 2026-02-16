# Design Review: src/datafusion_engine/schema/

**Date:** 2026-02-16
**Scope:** `src/datafusion_engine/schema/`
**Focus:** All principles (1-24)
**Depth:** deep (all 14 files in scope)
**Files reviewed:** 14

## Executive Summary

The `schema/` subpackage provides a comprehensive schema contract, introspection, and validation layer for the DataFusion engine. The package demonstrates strong separation between schema definition (`extraction_schemas.py`), contract validation (`contracts.py`, `validation.py`), and introspection (`introspection.py`). However, there are notable issues with DRY violations (duplicated `_ENGINE_FUNCTION_REQUIREMENTS`, duplicated `_mapping_from_registry`, near-identical validation functions), an overloaded `observability_schemas.py` that conflates schema declaration with runtime validation logic, and tight coupling between the introspection layer and DataFusion session internals. The highest-priority improvements involve extracting repeated validation patterns into a shared framework and splitting `observability_schemas.py` into its schema declaration and validation concerns.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 2 | small | low | `__init__.py` re-exports private symbols like `_resolve_nested_row_schema_authority` and `_semantic_validation_tables` |
| 2 | Separation of concerns | 1 | medium | medium | `observability_schemas.py` mixes schema declarations with 900+ lines of validation/introspection logic |
| 3 | SRP | 1 | medium | medium | `extraction_schemas.py` owns schema definitions AND schema resolution/registry construction AND nested schema derivation |
| 4 | High cohesion, low coupling | 2 | medium | low | Validation functions in `observability_schemas.py` depend on `SchemaIntrospector`, `SessionContext`, `udf_expr`, and UDF runtime |
| 5 | Dependency direction | 2 | small | low | `observability_schemas.py` imports private symbols from `extraction_schemas.py` (`_function_requirements`, `_schema_with_metadata`) |
| 6 | Ports & Adapters | 2 | medium | low | DataFusion `SessionContext` is passed directly through all layers; no abstraction boundary |
| 7 | DRY | 1 | small | medium | `_ENGINE_FUNCTION_REQUIREMENTS` defined identically in both `extraction_schemas.py:1546` and `observability_schemas.py:406`; `_mapping_from_registry` duplicated across two files |
| 8 | Design by contract | 2 | small | low | `SchemaContract` and `TableConstraints` provide explicit contract validation but some functions lack precondition docs |
| 9 | Parse, don't validate | 2 | small | low | Schema definitions are parsed once at module level; nested resolution converts messy paths to struct types at boundary |
| 10 | Make illegal states unrepresentable | 2 | small | low | `ConstraintType` uses `Literal` union; `EvolutionPolicy` is a proper enum; but `NestedDatasetSpec` uses TypedDict with string role |
| 11 | CQS | 2 | small | low | `validate_*` functions raise on failure (command) but some also return state; generally well-separated |
| 12 | DI + explicit composition | 2 | small | low | `SchemaPolicy` and `FinalizeOptions` allow injection; `SchemaIntrospector` accepts `sql_options` |
| 13 | Composition over inheritance | 3 | - | - | No inheritance hierarchies; everything composed via dataclasses and protocols |
| 14 | Law of Demeter | 1 | medium | medium | `observability_schemas.py:527-530` chains `ctx.table().select().limit().to_arrow_table().to_pylist()` |
| 15 | Tell, don't ask | 2 | small | low | `SchemaContract` encapsulates validation logic; but raw `snapshot.columns.to_pylist()` patterns leak structure |
| 16 | Functional core, imperative shell | 2 | small | low | Schema definitions are pure; validation functions are side-effectful; boundary is mostly clean |
| 17 | Idempotency | 3 | - | - | Schema definitions are module-level constants; introspection is read-only; finalize is deterministic |
| 18 | Determinism / reproducibility | 3 | - | - | Schema fingerprinting, canonical hashing, and ordering metadata throughout |
| 19 | KISS | 2 | small | low | `extraction_schemas.py` is 2000 lines of straightforward schema declarations; complexity is inherent to the domain |
| 20 | YAGNI | 2 | small | low | Backward-compatible aliases in `nested_views.py` (lines 584-681) double the API surface for no clear second consumer |
| 21 | Least astonishment | 1 | small | medium | `_schema_with_metadata` mutates schemas by reassigning module-level names (e.g., `AST_FILES_SCHEMA = _schema_with_metadata("ast_files_v1", AST_FILES_SCHEMA)` at line 1180) |
| 22 | Public contracts | 2 | small | low | `__all__` declared in most files; schema versions embedded in names; but `__init__.py` exports private symbols |
| 23 | Design for testability | 2 | small | low | Pure schema definitions easily testable; validation functions require full `SessionContext` |
| 24 | Observability | 2 | small | low | Structured logging with extras in `extraction_schemas.py`; warning-level logging for schema conflicts |

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information hiding -- Alignment: 2/3

**Current state:**
The `__init__.py` module uses a lazy-loading `__getattr__` pattern to re-export symbols from submodules, which is well-designed for avoiding import cycles. However, it publicly exports private symbols.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/schema/__init__.py:33` exports `_resolve_nested_row_schema_authority` (underscore-prefixed internal function)
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/schema/__init__.py:34` exports `_semantic_validation_tables` (underscore-prefixed internal function)
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/schema/extraction_schemas.py:1542-1563` defines `_sorted_tokens` and `_ENGINE_FUNCTION_REQUIREMENTS` as module-level private symbols that are nonetheless imported by `observability_schemas.py:43-44` via `_function_requirements` and `_schema_with_metadata`

**Suggested improvement:**
Rename `_resolve_nested_row_schema_authority` and `_semantic_validation_tables` to public names if they are part of the stable surface, or remove them from `__init__.py.__all__` and the `_EXPORT_MAP`. For cross-module private dependencies, extract the shared `_schema_with_metadata` and `_function_requirements` helpers into a dedicated internal module (e.g., `schema/_shared.py`) with a public interface.

**Effort:** small
**Risk if unaddressed:** low

---

#### P2. Separation of concerns -- Alignment: 1/3

**Current state:**
`observability_schemas.py` is the most problematic file in the scope. It starts with ~400 lines of schema declarations (its stated purpose) but then contains ~930 lines of runtime validation logic that operates on live `SessionContext` objects, introspects DataFusion catalogs, validates UDF signatures, probes semantic types via row sampling, and validates AST/tree-sitter span metadata.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/schema/observability_schemas.py:437-1330`: The second half of this file is entirely validation logic that has nothing to do with "schema definitions for pipeline observability events." Functions like `validate_semantic_types` (line 543), `_validate_ast_span_metadata` (line 937), `validate_required_cst_functions` (line 1144), and `validate_udf_info_schema_parity` (line 1275) are runtime validation concerns.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/schema/extraction_schemas.py:1594-2000`: Similarly, the bottom 400 lines of `extraction_schemas.py` contain schema resolution logic (`extract_schema_for`, `extract_base_schema_for`, `_derived_extract_nested_schema_for`) and registry construction (`base_extract_schema_registry`, `relationship_schema_registry`) that go beyond "schema definitions."

**Suggested improvement:**
Split `observability_schemas.py` into two files: `observability_schemas.py` (schema declarations only, ~400 lines) and a new `observability_validation.py` (all `validate_*` functions and `FunctionCatalog`). Similarly, extract schema resolution functions from `extraction_schemas.py` into a dedicated `schema_resolution.py` module.

**Effort:** medium
**Risk if unaddressed:** medium -- The monolithic file makes it hard to reason about what changes when the schema changes vs. when validation logic changes. New contributors must wade through validation code to find schema definitions.

---

#### P3. SRP -- Alignment: 1/3

**Current state:**
`extraction_schemas.py` changes for three distinct reasons: (1) extraction schema shapes change, (2) schema resolution/authority logic changes, (3) registry construction patterns change. `observability_schemas.py` changes for: (1) observability schema shapes, (2) validation logic, (3) function catalog introspection.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/schema/extraction_schemas.py` at 2000 lines owns schema definitions (lines 1-1536), schema metadata application (lines 1158-1285), schema resolution (lines 1594-1870), and registry adapters (lines 1956-1996)
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/schema/observability_schemas.py` at 1330 lines owns observability schema definitions (lines 1-430), DataFusion session validation (lines 437-1327), and function catalog construction (lines 622-684)

**Suggested improvement:**
Same as P2: extract schema resolution into `schema_resolution.py` and validation into `observability_validation.py`. The `FunctionCatalog` class and its construction logic could also be extracted to a `function_catalog.py` module.

**Effort:** medium
**Risk if unaddressed:** medium

---

#### P4. High cohesion, low coupling -- Alignment: 2/3

**Current state:**
Schema definition modules are highly cohesive. The coupling issue is that `observability_schemas.py` imports from at least 10 distinct modules including `datafusion_engine.udf.expr`, `datafusion_engine.sql.options`, `datafusion_engine.schema.introspection`, and `utils.validation`, creating a wide dependency fan-out for what should be a schema declaration file.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/schema/observability_schemas.py:17-51`: 14 import statements, spanning schema definitions, DataFusion sessions, SQL options, UDF expressions, introspection, and utilities
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/schema/finalize.py:5-45`: 18 import statements from 16 distinct modules

**Suggested improvement:**
After splitting validation out of `observability_schemas.py`, the schema declaration file would have minimal imports (just `pyarrow`, field builders, and metadata helpers). The validation module would naturally own the heavier dependencies.

**Effort:** medium
**Risk if unaddressed:** low

---

#### P5. Dependency direction -- Alignment: 2/3

**Current state:**
Schema definitions are core knowledge that should have minimal dependencies. The inward-dependency rule is mostly respected: `contracts.py` depends on `introspection.py` (which wraps DataFusion), and `validation.py` depends on `contracts.py`. However, `observability_schemas.py` (a schema definition module) depends on `introspection.py` and session-level UDF utilities, inverting the expected direction.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/schema/observability_schemas.py:47-49`: Schema definition module imports `SchemaIntrospector` and `table_names_snapshot` from `introspection.py`, plus `sql_options_for_profile` and `udf_expr` -- these are runtime/adapter concerns
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/schema/observability_schemas.py:36-46`: Imports private symbols `_function_requirements` and `_schema_with_metadata` from `extraction_schemas.py`

**Suggested improvement:**
Move validation functions out of the schema definition module so that schema definitions depend only on `pyarrow` and field builder utilities. Validation modules can then depend on both schemas and introspection.

**Effort:** small
**Risk if unaddressed:** low

---

#### P6. Ports & Adapters -- Alignment: 2/3

**Current state:**
`SessionContext` from DataFusion is passed through as a concrete dependency everywhere. The `SchemaIntrospector` acts as a partial adapter (wrapping information_schema queries), but it still exposes `SessionContext` in its constructor and many standalone functions accept `ctx: SessionContext` directly.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/schema/introspection.py:740-749`: `SchemaIntrospector` takes `ctx: SessionContext` as a required field. It acts as a facade but is still concretely coupled.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/schema/observability_schemas.py:455-465`: `schema_contract_for_table` delegates directly through `ctx` with no abstraction.

**Suggested improvement:**
This is acceptable given the architecture -- DataFusion is a foundational dependency, not a swappable adapter. The `SchemaIntrospector` already provides a reasonable facade. No action needed unless DataFusion needs to be swapped, which is not a near-term change vector. Score reflects minor coupling.

**Effort:** medium
**Risk if unaddressed:** low

---

### Category: Knowledge (7-11)

#### P7. DRY -- Alignment: 1/3

**Current state:**
Several knowledge duplications exist across the schema subpackage.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/schema/extraction_schemas.py:1546-1563` and `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/schema/observability_schemas.py:406-423`: Identical `_ENGINE_FUNCTION_REQUIREMENTS` definition with the same 14 required functions. Both are the `function_requirements_metadata_spec(required=(...))` call with exactly the same tuple of function names.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/schema/extraction_schemas.py:1994-1995` and `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/schema/nested_views.py:459-460`: Identical `_mapping_from_registry` function with the same body.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/schema/observability_schemas.py:1144-1245`: `validate_required_cst_functions`, `validate_required_symtable_functions`, and `validate_required_bytecode_functions` follow the same pattern (create error dict, get function catalog, get requirements from schema, validate required functions, optionally validate signatures, raise if errors). They differ only in the schema they reference and which sub-checks they run.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/schema/observability_schemas.py:937-1003`: `_validate_ast_span_metadata` and `_validate_ts_span_metadata` are structurally identical functions differing only in the expected metadata dict and the prefix tuples.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/schema/contracts.py:602-685` and `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/schema/catalog_contracts.py:80-139`: Both implement schema-vs-contract violation detection with overlapping logic (missing columns, type mismatches, extra columns) but different interfaces.

**Suggested improvement:**
1. Define `_ENGINE_FUNCTION_REQUIREMENTS` once in a shared location (e.g., a `_constants.py` or within `extraction_schemas.py`) and import it in `observability_schemas.py`.
2. Move `_mapping_from_registry` to a shared utility or into `utils/registry_protocol.py`.
3. Extract a parameterized `_validate_required_functions_for_schema(ctx, schema, label)` that handles the common pattern of: get requirements -> validate functions -> validate signatures -> raise.
4. Extract a parameterized `_validate_span_metadata(ctx, errors, expected_meta, prefixes, view_name)` for the span validation pattern.

**Effort:** small
**Risk if unaddressed:** medium -- When function requirements change, two files must be updated in lockstep. The duplicated validation pattern means bugs fixed in one variant may not be fixed in others.

---

#### P8. Design by contract -- Alignment: 2/3

**Current state:**
The `SchemaContract` class provides explicit contract validation with typed violations. `TableConstraints` and `ConstraintSpec` model constraints declaratively. The `finalize.py` module implements a structured invariant pipeline.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/schema/contracts.py:471-500`: `SchemaContract` explicitly declares `columns`, `partition_cols`, `ordering`, `evolution_policy`, and `enforce_columns` -- a well-structured contract
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/schema/finalize.py:87-132`: `Contract` dataclass bundles schema, invariants, key fields, deduplication spec, and canonical sort -- explicit postconditions
- Some public functions have placeholder docstrings (`Args: name: Description.`) at `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/schema/extraction_schemas.py:1615-1619` and elsewhere

**Suggested improvement:**
Fill in placeholder docstring parameter descriptions for public functions. The incomplete `Args: name: Description.` pattern appears in multiple functions.

**Effort:** small
**Risk if unaddressed:** low

---

#### P9. Parse, don't validate -- Alignment: 2/3

**Current state:**
Schema definitions are parsed once at module load time into `pa.Schema` objects. The `struct_for_path` function in `nested_views.py` converts dot-delimited paths to typed `pa.StructType` at the boundary. The `_schema_with_metadata` function enriches schemas with version and name metadata at definition time.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/schema/nested_views.py:692-722`: `struct_for_path` is a proper parse-don't-validate implementation -- paths are resolved to concrete struct types at the boundary
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/schema/extraction_schemas.py:1135-1155`: `_schema_with_metadata` enriches schemas once at definition time, not at every usage

**Suggested improvement:**
No significant gaps. The approach is sound.

**Effort:** small
**Risk if unaddressed:** low

---

#### P10. Make illegal states unrepresentable -- Alignment: 2/3

**Current state:**
`EvolutionPolicy` is a proper enum with three variants. `ConstraintType` is a `Literal` union restricting to four valid values. `ConstraintSpec` uses `msgspec.Struct` with frozen=True for immutability. However, `NestedDatasetSpec` uses a `TypedDict` with a string `role` field rather than an enum.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/schema/contracts.py:419-424`: `EvolutionPolicy` enum prevents invalid policy states
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/schema/nested_views.py:33-39`: `NestedDatasetSpec` uses `Literal["intrinsic", "derived"]` for `role` -- this is acceptable but the `context: dict[str, str]` field allows arbitrary context mappings with no structural constraint
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/schema/contracts.py:141-159`: `ConstraintSpec` allows `column`, `columns`, and `expression` to all be `None` simultaneously, which may be a valid but meaningless state for some constraint types

**Suggested improvement:**
Consider adding a `__post_init__` check to `ConstraintSpec` that validates consistency (e.g., a `pk` constraint should have at least `column` or `columns`). The `Literal` type for role is fine.

**Effort:** small
**Risk if unaddressed:** low

---

#### P11. CQS -- Alignment: 2/3

**Current state:**
Most functions follow CQS. Schema definition functions are pure queries. Validation functions like `validate_required_cst_functions` raise exceptions (commands) without returning state. The `finalize` function returns a `FinalizeResult` and also mutates the session context via temporary table registration/deregistration.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/schema/finalize.py:736-824`: `finalize()` both transforms data (query) and registers/deregisters temp tables (command), though the temp table side effects are properly scoped
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/schema/introspection.py:770-784`: `invalidate_cache` returns `int` (eviction count) while also evicting cache entries -- mixed query/command but the return value is informational about the side effect

**Suggested improvement:**
The `finalize` function's temporary table registration is implementation detail that does not leak. The `invalidate_cache` return is a common pattern for cache operations. No action needed.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Composition (12-15)

#### P12. Dependency inversion + explicit composition -- Alignment: 2/3

**Current state:**
`SchemaPolicy`, `FinalizeOptions`, and `FinalizeContext` allow dependency injection of encoding policy, validation options, and schema policies. The `spec_protocol.py` defines `ArrowFieldSpec` and `TableSchemaSpec` as protocols, enabling structural typing.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/schema/spec_protocol.py:13-68`: `ArrowFieldSpec` and `TableSchemaSpec` protocols provide proper abstractions for field and schema specs
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/schema/finalize.py:296-308`: `FinalizeOptions` allows injecting `error_spec`, `schema_policy`, `chunk_policy`, `runtime_profile`
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/schema/observability_schemas.py:552`: `SchemaIntrospector` is created inline inside `validate_semantic_types` rather than injected

**Suggested improvement:**
Make `validate_semantic_types` and similar validation functions accept a pre-built `SchemaIntrospector` to improve testability and avoid hidden construction.

**Effort:** small
**Risk if unaddressed:** low

---

#### P13. Composition over inheritance -- Alignment: 3/3

**Current state:**
No class inheritance hierarchies exist in this subpackage. All types use dataclasses, msgspec structs, or protocols. Behavior is composed through function parameters and dataclass fields.

**Findings:**
- No inheritance chains detected across all 14 files.

**Suggested improvement:**
None needed.

**Effort:** -
**Risk if unaddressed:** -

---

#### P14. Law of Demeter -- Alignment: 1/3

**Current state:**
Several functions chain through multiple object boundaries to retrieve data.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/schema/observability_schemas.py:527-530`: `ctx.table(table_name).select(udf_expr(...)).limit(1).to_arrow_table().to_pylist()` -- 5 levels of method chaining through DataFusion internals
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/schema/observability_schemas.py:583-595`: `ctx.table("ast_files_v1")` followed by `.select(*exprs).limit(1).collect()` -- 4 levels
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/schema/introspection.py:323-326`: `snapshot.tables["table_name"].to_pylist()` -- accessing columns by name on a snapshot table, then converting to Python lists
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/schema/introspection.py:836-850`: `snapshot.columns.to_pylist()` followed by iterating through rows and extracting values by key -- exposing raw PyArrow table structure

**Suggested improvement:**
Encapsulate common DataFusion query patterns into helper methods on `SchemaIntrospector`. For example, a `probe_field_metadata(table_name, column_name, metadata_key)` method would eliminate the 5-level chain in `_semantic_type_via_row_probe`. Similarly, `snapshot.table_names()` could wrap the column extraction pattern.

**Effort:** medium
**Risk if unaddressed:** medium -- These chains are fragile when DataFusion API changes. The `to_pylist()` pattern forces materialization and couples to PyArrow table internals.

---

#### P15. Tell, don't ask -- Alignment: 2/3

**Current state:**
`SchemaContract.validate_against_introspection` and `SchemaContract.validate_against_schema` properly encapsulate validation logic. However, the `SchemaIntrospector` methods expose raw data structures (lists of dicts) that callers then sift through.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/schema/introspection.py:824-851`: `table_columns` returns `list[dict[str, object]]` -- callers must interpret the dict structure themselves
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/schema/observability_schemas.py:556-567`: Caller loops through `column_names`, checks `semantic_type_for_field_name`, and calls `_require_semantic_type` -- this "ask the introspector, then decide" pattern could be encapsulated

**Suggested improvement:**
Add semantic-level methods to `SchemaIntrospector` (e.g., `validate_semantic_types_for_table(table_name)`) rather than having callers orchestrate low-level queries.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Correctness (16-18)

#### P16. Functional core, imperative shell -- Alignment: 2/3

**Current state:**
Schema definitions (`extraction_schemas.py`, most of `observability_schemas.py`) are pure module-level constants. The `contracts.py` types are immutable frozen dataclasses/structs. Validation functions are side-effectful (they query DataFusion sessions). The `finalize.py` module is a clear imperative shell that orchestrates pure invariant checks with I/O.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/schema/finalize.py:736-824`: `finalize()` is the imperative shell: it registers temp tables, runs validation, collects invariant results, filters rows, aggregates errors, and deregisters tables
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/schema/finalize.py:310-346`: `_required_non_null_results` is pure (operates on in-memory arrays) -- good functional core example

**Suggested improvement:**
No significant gaps. The functional core / imperative shell boundary is mostly clean.

**Effort:** small
**Risk if unaddressed:** low

---

#### P17. Idempotency -- Alignment: 3/3

**Current state:**
Schema definitions are immutable module-level constants. Introspection queries are read-only. The `finalize` function produces deterministic outputs from the same inputs. Temporary tables are registered and deregistered within scoped blocks.

**Findings:**
- All schema constants are assigned at module load time and never mutated at runtime.
- `SchemaContract`, `TableConstraints`, `ConstraintSpec` are frozen.

**Suggested improvement:**
None needed.

**Effort:** -
**Risk if unaddressed:** -

---

#### P18. Determinism / reproducibility -- Alignment: 3/3

**Current state:**
The package extensively supports determinism: schema fingerprints, `policy_hash` metadata, `schema_identity_hash`, ordering metadata, and canonical sort in `finalize`. The `_schema_with_metadata` function attaches version metadata to all schemas.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/schema/extraction_schemas.py:1142-1155`: `_schema_with_metadata` attaches `schema_name` and `schema_version` to all extraction schemas
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/schema/contracts.py:528-531`: `SchemaContract.from_arrow_schema` attaches `SCHEMA_ABI_FINGERPRINT_META` for deterministic identity
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/schema/finalize.py:796-801`: Canonical sort applied with `canonical_sort_if_canonical`

**Suggested improvement:**
None needed. Determinism is a strength of this package.

**Effort:** -
**Risk if unaddressed:** -

---

### Category: Simplicity (19-22)

#### P19. KISS -- Alignment: 2/3

**Current state:**
Schema declarations are straightforward and domain-appropriate. The complexity in `extraction_schemas.py` (2000 lines) is largely inherent -- each extraction source needs its own schema. However, the schema resolution chain (`extract_schema_for` -> `_derived_extract_schema_for` -> `_derived_extract_base_schema_for` / `_derived_extract_nested_schema_for` -> multiple sub-resolution functions) involves 6+ levels of indirection.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/schema/extraction_schemas.py:1682-1724`: `_derived_extract_nested_schema_for` calls `_resolve_nested_dataset_context` which calls `_nested_dataset_root_schemas` and `_resolve_nested_row_struct`, then calls `_resolve_nested_row_schema_authority` and `_collect_nested_schema_fields` -- 5 helper functions for a single resolution path
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/schema/finalize.py:87-132`: The `Contract` dataclass has 13 fields, which is at the upper boundary of comprehensibility but each field is meaningful

**Suggested improvement:**
Consider consolidating the nested schema resolution chain into fewer, more self-documenting functions. The current decomposition is fine for correctness but makes the resolution flow hard to trace.

**Effort:** small
**Risk if unaddressed:** low

---

#### P20. YAGNI -- Alignment: 2/3

**Current state:**
`nested_views.py` defines both `extract_nested_dataset_names` and a backward-compatible alias `nested_dataset_names` that simply delegates. There are 7 such alias pairs (lines 584-681). If these aliases have no external consumers, they represent speculative generality.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/schema/nested_views.py:584-681`: Seven backward-compatible alias functions: `nested_dataset_names`, `nested_schema_names`, `nested_spec_for`, `datasets_for_path`, `nested_path_for`, `nested_context_for`, `nested_role_for`, `is_nested_dataset`, `is_intrinsic_nested_dataset`. Each is a one-line delegation to the `extract_*` variant.

**Suggested improvement:**
If these aliases are not used externally (outside this package), remove them. If they are used, add a deprecation warning to guide callers toward the canonical names.

**Effort:** small
**Risk if unaddressed:** low

---

#### P21. Least astonishment -- Alignment: 1/3

**Current state:**
Several module-level schema names are reassigned in-place using `_schema_with_metadata`, which can surprise readers.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/schema/extraction_schemas.py:1180-1284`: Module-level constants like `AST_FILES_SCHEMA`, `BYTECODE_FILES_SCHEMA`, `LIBCST_FILES_SCHEMA` etc. are defined once (e.g., line 688) and then reassigned at the bottom of the file (e.g., line 1180: `AST_FILES_SCHEMA = _schema_with_metadata("ast_files_v1", apply_semantic_types(AST_FILES_SCHEMA), ...)`). A reader encountering `AST_FILES_SCHEMA` at line 688 would not expect it to be overwritten 500 lines later.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/schema/observability_schemas.py:365-379` and `425-429`: `ENGINE_RUNTIME_SCHEMA` is defined at line 365, then reassigned at line 425 with function requirements metadata. Same surprising reassignment pattern.
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/schema/extraction_schemas.py:1566`: `from dataclasses import dataclass` import appears at line 1566, far from the top of the file -- violates standard import ordering expectations.

**Suggested improvement:**
Define schemas in a two-phase approach: (1) define the "raw" schema with a distinct name like `_AST_FILES_SCHEMA_RAW`, (2) define the final `AST_FILES_SCHEMA` by calling `_schema_with_metadata` on the raw version. This makes the relationship explicit and avoids shadowing. Move the stray `dataclasses` import to the top of the file.

**Effort:** small
**Risk if unaddressed:** medium -- The reassignment pattern can cause bugs if someone adds code between the initial definition and the reassignment, or if they reference the pre-metadata version expecting the final version.

---

#### P22. Public contracts -- Alignment: 2/3

**Current state:**
Most files declare `__all__`. Schema versions are embedded in names (e.g., `"ast_files_v1"`, `"datafusion_plan_artifacts_v10"`). The `__init__.py` provides a comprehensive lazy-export map.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/schema/contracts.py:1241-1263`: Comprehensive `__all__` list
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/schema/__init__.py:8-68`: `__all__` includes private symbols (noted in P1)
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/schema/observability_schemas.py`: No `__all__` declared

**Suggested improvement:**
Add `__all__` to `observability_schemas.py`. Remove private symbols from `__init__.py.__all__`.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Quality (23-24)

#### P23. Design for testability -- Alignment: 2/3

**Current state:**
Pure schema definitions are trivially testable. The `SchemaContract` class can be tested by constructing instances directly. However, validation functions in `observability_schemas.py` require a live `SessionContext` with registered tables and UDFs, making them heavy to test.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/schema/observability_schemas.py:543-567`: `validate_semantic_types` creates its own `SchemaIntrospector` internally -- not injectable for testing
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/schema/validation.py:621-670`: `validate_table` creates a `SessionContext` via `_session_context(runtime_profile)` internally, registers temp tables, and runs DataFusion queries -- requires full runtime infrastructure
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/schema/finalize.py:648-654`: `_datafusion_context` silently returns `None` on failure, making error path testing ambiguous

**Suggested improvement:**
Accept `SchemaIntrospector` as a parameter in validation functions to enable testing with mock introspectors. For `validate_table`, consider extracting the pure validation logic (column checks, type checks) from the DataFusion-dependent row filtering.

**Effort:** small
**Risk if unaddressed:** low

---

#### P24. Observability -- Alignment: 2/3

**Current state:**
Structured logging is used throughout with meaningful `extra` dicts. Schema authority decisions are logged with `_LOGGER.debug("extract_schema_authority", extra={...})`. Warnings are emitted for schema conflicts and validation skips.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/schema/extraction_schemas.py:1826-1836`: Schema authority decisions logged with structured extras including `dataset_name` and `authority`
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/schema/extraction_schemas.py:1899-1910`: Schema authority conflicts logged at WARNING level with both `derived_fields` and `struct_fields`
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/schema/nested_views.py:990-1001`: Validation mismatches logged with `missing` and `mismatched` details

**Suggested improvement:**
Consider adding structured logging to the `finalize` pipeline (alignment stats, invariant results, error counts) since it is a critical data path where observability of schema transformations is valuable.

**Effort:** small
**Risk if unaddressed:** low

---

## Cross-Cutting Themes

### Theme 1: Observability schemas as a "God module"

`observability_schemas.py` has grown to 1330 lines and serves as the primary dumping ground for any validation logic that needs schema awareness. It conflates schema declarations (its stated purpose) with:
- Session-level validation (semantic types, span metadata, view outputs)
- Function catalog introspection and validation
- Information schema parity checks
- UDF validation
- Table registration checks

This creates a root cause for violations of P2 (separation of concerns), P3 (SRP), P4 (coupling), P5 (dependency direction), and P23 (testability). Splitting this file would improve alignment across 5 principles simultaneously.

### Theme 2: Duplicated knowledge across schema modules

The `_ENGINE_FUNCTION_REQUIREMENTS` duplication (P7), `_mapping_from_registry` duplication (P7), and near-identical `validate_required_*_functions` family (P7) share a root cause: the lack of a shared internal helpers module within the schema subpackage. Creating a `schema/_internal.py` or promoting shared helpers to utility modules would address all three duplications.

### Theme 3: Module-level name reassignment pattern

The pattern of defining a schema constant and then reassigning it later in the same file (P21) is used for all 7 extraction schemas and `ENGINE_RUNTIME_SCHEMA`. This pattern is fragile, surprising, and difficult to reason about during code review. Adopting a two-phase naming convention would eliminate this issue.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7 (DRY) | Deduplicate `_ENGINE_FUNCTION_REQUIREMENTS` across `extraction_schemas.py:1546` and `observability_schemas.py:406` | small | Eliminates silent drift risk for 14 required function names |
| 2 | P7 (DRY) | Deduplicate `_mapping_from_registry` across `extraction_schemas.py:1994` and `nested_views.py:459` | small | Single implementation point |
| 3 | P21 (Least astonishment) | Rename raw schemas to `_*_RAW` and define final schemas cleanly in `extraction_schemas.py:1180-1284` | small | Eliminates surprising module-level reassignment |
| 4 | P1 (Info hiding) | Remove `_resolve_nested_row_schema_authority` and `_semantic_validation_tables` from `__init__.py.__all__` | small | Reduces public surface area to stable symbols |
| 5 | P22 (Public contracts) | Add `__all__` to `observability_schemas.py` | small | Explicit public surface declaration |

## Recommended Action Sequence

1. **Deduplicate shared helpers (P7)**: Move `_ENGINE_FUNCTION_REQUIREMENTS` to a single definition in `extraction_schemas.py` and import in `observability_schemas.py`. Move `_mapping_from_registry` to `utils/registry_protocol.py`. This is the lowest-risk, highest-value change.

2. **Fix module-level reassignment pattern (P21)**: Rename the initial schema definitions to `_*_RAW` variants and define the final schemas in a single assignment. This improves readability without changing behavior.

3. **Clean up public surface (P1, P22)**: Remove private symbols from `__init__.py.__all__`; add `__all__` to `observability_schemas.py`.

4. **Split `observability_schemas.py` (P2, P3, P5)**: Extract validation functions (lines 437-1327) into a new `observability_validation.py`. This is the largest change but addresses the most principles simultaneously. Depends on step 1 being complete.

5. **Extract parameterized validation pattern (P7)**: Consolidate `validate_required_cst_functions`, `validate_required_symtable_functions`, and `validate_required_bytecode_functions` into a single generic `validate_required_functions_for_schema(ctx, schema, label)`. Similarly consolidate span metadata validators.

6. **Encapsulate Law of Demeter violations (P14)**: Add helper methods to `SchemaIntrospector` for common query patterns (field metadata probing, table name extraction, column name sets) to eliminate deep method chains in `observability_schemas.py`.

7. **Extract schema resolution from `extraction_schemas.py` (P3)**: Move `extract_schema_for`, `extract_base_schema_for`, and the nested resolution chain into a dedicated `schema_resolution.py`. This reduces `extraction_schemas.py` from 2000 to ~1600 lines of pure schema definitions.
