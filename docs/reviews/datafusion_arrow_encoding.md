# Design Review: src/datafusion_engine/arrow/ + src/datafusion_engine/encoding/

**Date:** 2026-02-16
**Scope:** `src/datafusion_engine/arrow/`, `src/datafusion_engine/encoding/`
**Focus:** All principles (1-24)
**Depth:** deep
**Files reviewed:** 16

## Executive Summary

The Arrow and encoding modules provide a well-structured abstraction layer over PyArrow with strong protocol-based typing and clean separation of nested type builders from metadata operations. However, `metadata.py` (1,262 lines, 47 `__all__` exports) has become a gravity well that conflates six distinct concerns -- metadata specs, encoding policy derivation, ordering metadata, evidence metadata, extractor defaults, and function requirements. There is meaningful knowledge duplication across the package (`_resolve_schema` in 2 files, `_storage_type`/`storage_type` in 2 files, `DEFAULT_DICTIONARY_INDEX_TYPE` in 2 files, `_ensure_arrow_dtype`/`ensure_arrow_dtype` in 2 files), and the absence of any logging or tracing makes operational diagnosis of schema coercion failures difficult. The interop module's protocol + alias pattern is an exemplary information-hiding boundary.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 2 | small | low | interop.py protocols excellent; metadata.py leaks encoding internals to all callers |
| 2 | Separation of concerns | 1 | medium | medium | metadata.py mixes 6 concerns in one module |
| 3 | SRP (one reason to change) | 1 | medium | medium | metadata.py changes for ordering, evidence, encoding, extractor defaults, function reqs |
| 4 | High cohesion, low coupling | 2 | small | low | Most modules cohesive; metadata.py is the exception |
| 5 | Dependency direction | 2 | small | low | Core types in interop/types; encoding.policy pulls DataFusion SessionContext |
| 6 | Ports & Adapters | 2 | small | low | Protocol types serve as ports; some adapter leakage |
| 7 | DRY (knowledge) | 1 | medium | medium | 4 pairs of duplicated knowledge across the package |
| 8 | Design by contract | 2 | small | low | Preconditions checked; postconditions mostly implicit |
| 9 | Parse, don't validate | 2 | small | low | metadata_codec parses to typed structs; some raw dict paths remain |
| 10 | Make illegal states unrepresentable | 2 | small | low | EncodingPolicy __post_init__ normalizes; redundant fields allow drift |
| 11 | CQS | 2 | small | low | register_semantic_extension_types is side-effectful query; otherwise clean |
| 12 | Dependency inversion | 2 | small | low | Protocol-based; encoding/policy.py creates SessionContext internally |
| 13 | Prefer composition over inheritance | 3 | - | - | Dataclass composition throughout; no inheritance hierarchies |
| 14 | Law of Demeter | 2 | small | low | Some chained casts through protocol types |
| 15 | Tell, don't ask | 2 | small | low | SchemaMetadataSpec.apply is good tell pattern; many raw dict extractors |
| 16 | Functional core, imperative shell | 2 | small | low | Pure transforms dominate; encoding/policy.py creates session internally |
| 17 | Idempotency | 3 | - | - | All operations are safe to re-run |
| 18 | Determinism / reproducibility | 3 | - | - | Sorted keys, canonical hashing, versioned payloads |
| 19 | KISS | 2 | small | low | nested.py union logic is inherently complex but well-decomposed |
| 20 | YAGNI | 2 | small | low | field_builders.py is thin wrappers over pa.field; marginal value |
| 21 | Least astonishment | 2 | small | low | list_view_type returns list_ or large_list, not list_view; naming misleads |
| 22 | Declare and version public contracts | 2 | small | low | __all__ declared; metadata payloads versioned; no stability markers |
| 23 | Design for testability | 1 | medium | medium | encoding/policy.py creates SessionContext internally; metadata.py hard to test in isolation |
| 24 | Observability | 0 | medium | high | Zero logging, tracing, or metrics across 5,685 lines |

## Detailed Findings

### Category: Boundaries

#### P1. Information hiding -- Alignment: 2/3

**Current state:**
`interop.py` (868 lines) is an exemplary information-hiding boundary. It defines protocol types (`TableLike`, `SchemaLike`, `ArrayLike`, etc.) and re-exports PyArrow concrete types through protocol-typed aliases, allowing the rest of the codebase to program against stable interfaces without depending on PyArrow internals directly.

**Findings:**
- `interop.py:741-792` re-exports PyArrow types behind protocol aliases (e.g., `Table: type[TableLike] = cast("type[TableLike]", pa.Table)`) -- this is a strong boundary.
- `metadata.py:28-29` imports `EncodingPolicy` and `EncodingSpec` from `arrow.encoding` and then re-exports them in `__all__` (line 1216-1262), making metadata.py a facade that leaks encoding internals to 20+ consumers who import from metadata.py.
- `metadata.py:1204-1213` uses `__getattr__`/`__dir__` to lazily re-export `TableSchemaSpec` from `schema_spec.specs`, creating a hidden dynamic import that surprises callers.

**Suggested improvement:**
Remove `EncodingPolicy`/`EncodingSpec` re-exports from `metadata.py` `__all__` and update the 7 consumers of `from datafusion_engine.arrow.encoding import EncodingPolicy` to import directly from `arrow.encoding`. Remove the `__getattr__` lazy import in favor of an explicit `TYPE_CHECKING` import or documentation pointer.

**Effort:** small
**Risk if unaddressed:** low

---

#### P2. Separation of concerns -- Alignment: 1/3

**Current state:**
`metadata.py` (1,262 lines, 47 `__all__` exports) conflates at least six distinct concerns: (1) `SchemaMetadataSpec` and its nested metadata application logic, (2) encoding policy derivation (`encoding_policy_from_spec`, `encoding_policy_from_schema`, `encoding_policy_from_fields`), (3) ordering metadata (`infer_ordering_keys`, `ordering_metadata_spec`, `ordering_from_schema`), (4) evidence metadata (`EvidenceMetadata`, `EvidenceMetadataSpec`, `evidence_metadata_spec`), (5) extractor defaults (`extractor_option_defaults_spec`, `extractor_option_defaults_from_metadata`), (6) function requirements (`required_functions_from_metadata`, `function_requirements_metadata_spec`).

**Findings:**
- Lines 100-275: `SchemaMetadataSpec` class and its nested metadata traversal -- core concern.
- Lines 277-311: Options hashing and run-specific provenance -- separate concern.
- Lines 313-451: Extractor defaults encode/decode -- a self-contained serialization concern that uses its own versioned payload format.
- Lines 454-523: Evidence metadata types and builders -- domain-specific metadata.
- Lines 656-761: Ordering inference, metadata spec, and parsing -- a self-contained ordering concern.
- Lines 878-939: Three `encoding_policy_from_*` factory functions -- encoding policy construction concern.
- Lines 1036-1131: Function requirements metadata -- another self-contained metadata sub-concern.

**Suggested improvement:**
Split `metadata.py` into focused modules: `arrow/metadata_spec.py` (SchemaMetadataSpec + merge/apply), `arrow/ordering_metadata.py` (ordering keys/levels), `arrow/evidence_metadata.py` (evidence types + builders), `arrow/extractor_metadata.py` (extractor defaults encode/decode), `arrow/function_requirements.py` (function requirements metadata). Keep a thin `metadata.py` facade for backward compatibility if needed.

**Effort:** medium
**Risk if unaddressed:** medium -- every change to any metadata sub-concern touches 1,262 lines and risks merge conflicts.

---

#### P3. SRP (one reason to change) -- Alignment: 1/3

**Current state:**
`metadata.py` changes for at least six independent reasons (see P2). Changes to ordering metadata format, evidence metadata format, extractor default encoding, function requirement encoding, encoding policy derivation, or core schema metadata application all modify the same file.

**Findings:**
- `metadata.py` has 47 exports in `__all__`, confirming multiple responsibilities in a single module.
- `build.py` (823 lines, 56 exports) also combines multiple concerns: table construction, column expression types, iteration helpers, and schema resolution. While less severe than metadata.py, it mixes builder patterns with utility iterators.

**Suggested improvement:**
For `metadata.py`, apply the decomposition in P2. For `build.py`, consider extracting the `ColumnExpr` family (`ConstExpr`, `FieldExpr`, `ColumnOrNullExpr`, `CoalesceExpr`, `ColumnDefaultsSpec`) into a separate `arrow/column_expr.py` module, as these are a cohesive set of types with a distinct change reason (column default/expression logic).

**Effort:** medium
**Risk if unaddressed:** medium

---

#### P4. High cohesion, low coupling -- Alignment: 2/3

**Current state:**
Most smaller modules are highly cohesive: `types.py` (type protocols), `encoding.py` (encoding policy dataclass), `encoding_metadata.py` (encoding metadata constants), `metadata_codec.py` (MessagePack codec), `chunking.py` (chunk normalization), `dictionary.py` (dictionary normalization), `coercion.py` (type coercion), `schema.py` (field builders), `semantic.py` (extension types). The coupling graph shows `metadata.py` as a central hub importing from 8 sibling modules.

**Findings:**
- `metadata.py` imports from: `dictionary.py`, `encoding.py`, `encoding_metadata.py`, `interop.py`, `metadata_codec.py`, `nested.py`, `types.py`, and external `arrow_utils.core`, `schema_spec`, `serde_msgspec`, `utils.hashing` -- 11 total import sources.
- `build.py` imports from `interop.py`, `nested.py`, `types.py`, `arrow_utils.core` -- reasonably scoped.
- `nested.py` imports from `interop.py`, `types.py`, `utils.validation` -- tight and cohesive.

**Suggested improvement:**
The decomposition suggested in P2 would reduce `metadata.py`'s import fan-in and fan-out. Each sub-module would import only the dependencies it needs.

**Effort:** small (this is addressed by P2 decomposition)
**Risk if unaddressed:** low

---

#### P5. Dependency direction -- Alignment: 2/3

**Current state:**
The dependency direction is mostly correct: `interop.py` and `types.py` are leaf modules with no sibling dependencies. `encoding.py` depends only on `interop.py`. `nested.py` depends on `interop.py` and `types.py`. The primary violation is `encoding/policy.py`, which creates a `SessionContext` internally.

**Findings:**
- `encoding/policy.py:132-136`: `_datafusion_context()` imports and instantiates `DataFusionRuntimeProfile` from `datafusion_engine.session.runtime`, pulling the DataFusion session runtime (a "detail") into what should be a pure encoding logic module.
- `interop.py` correctly has zero sibling imports -- it is the true leaf of the dependency tree.
- `types.py:10` depends only on `interop.py` -- correct inward direction.

**Suggested improvement:**
Accept a `SessionContext` parameter in `apply_encoding()` and `encode_table()` instead of creating one internally via `_datafusion_context()`. This makes the dependency explicit and injectable.

**Effort:** small
**Risk if unaddressed:** low

---

#### P6. Ports & Adapters -- Alignment: 2/3

**Current state:**
The protocol types in `interop.py` (`TableLike`, `SchemaLike`, `ArrayLike`, etc.) and `types.py` (`ListTypeProtocol`, `StructTypeProtocol`, `MapTypeProtocol`, `UnionTypeProtocol`) serve as ports. Concrete PyArrow implementations are the adapters. The pattern is well-applied.

**Findings:**
- `interop.py:16-165`: 12 protocol types define the port surface cleanly.
- `ComputeModule` protocol at `interop.py:672-738` mirrors the pyarrow.compute surface -- this is a large protocol but serves as a comprehensive port.
- The `_SemanticExtensionType` at `semantic.py:93` directly extends `pa.ExtensionType`, tightly coupling to PyArrow's concrete class. This is acceptable since extension types are inherently PyArrow-specific.

**Suggested improvement:**
No significant changes needed. The protocol-based approach is well-executed.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Knowledge

#### P7. DRY (knowledge, not lines) -- Alignment: 1/3

**Current state:**
Four instances of duplicated knowledge exist within this scope:

**Findings:**
- **`_resolve_schema`**: Defined in both `build.py:44-53` and `abi.py:19-29`. Both convert `SchemaLike` to `pa.Schema` with identical logic (check isinstance, try `to_pyarrow()`, raise TypeError). The `abi.py` version additionally calls `register_semantic_extension_types()`.
- **`_storage_type` / `storage_type`**: `build.py:70-93` defines `_storage_type` (private) and `coercion.py:10-64` defines `storage_type` (public). Both recursively unwrap ExtensionTypes, structs, lists, and maps. The `coercion.py` version additionally handles `string_view` and `binary_view` types, making it a superset.
- **`DEFAULT_DICTIONARY_INDEX_TYPE`**: Defined as `pa.int32()` in both `types.py:94` and `encoding/policy.py:28`. Same constant, two locations.
- **`ensure_arrow_dtype` / `_ensure_arrow_dtype`**: `interop.py:20-40` defines a public version with `object` parameter type and lazy import fallback. `encoding/policy.py:139-145` defines a private version with `ArrowTypeSpec` parameter type and direct import. The core logic is identical: check `pa.DataType`, try `ArrowTypeBase` conversion, raise `TypeError`.

**Suggested improvement:**
1. Extract `_resolve_schema` into a shared private helper in `interop.py` or a new `arrow/_internal.py`, parameterized by whether to register semantic types.
2. Delete `build.py:_storage_type` and `build.py:_storage_schema` and import from `coercion.py` instead.
3. Remove `DEFAULT_DICTIONARY_INDEX_TYPE` from `encoding/policy.py` and import from `types.py`.
4. Have `encoding/policy.py` import `ensure_arrow_dtype` from `interop.py` instead of defining its own `_ensure_arrow_dtype`.

**Effort:** medium
**Risk if unaddressed:** medium -- divergent evolution of duplicated logic leads to subtle schema handling inconsistencies (e.g., `coercion.py` handles `string_view` but `build.py` does not).

---

#### P8. Design by contract -- Alignment: 2/3

**Current state:**
Preconditions are generally well-checked with `TypeError`/`ValueError` raises. The metadata codec validates versions explicitly. However, postconditions are largely implicit.

**Findings:**
- `metadata_codec.py:92-95`: Version check on `decode_metadata_map` -- explicit precondition.
- `nested.py:612-614`: `union_array_from_values` validates `patypes.is_union(union_type)` before proceeding -- good.
- `encoding.py:32-48`: `EncodingPolicy.__post_init__` normalizes derived fields, which acts as an invariant enforcer.
- `metadata.py:398-426`: `_extractor_defaults_from_row` validates version, entries list type, and entry mapping type -- thorough precondition checking.
- Missing: No postcondition assertions on output schema shape after `SchemaMetadataSpec.apply()` at `metadata.py:246-274`. A caller cannot easily verify that metadata was correctly applied.

**Suggested improvement:**
Add debug-mode postcondition assertions (e.g., `assert len(result) == len(schema)`) to `SchemaMetadataSpec.apply()` and `apply_encoding()` to catch silent schema corruption during development.

**Effort:** small
**Risk if unaddressed:** low

---

#### P9. Parse, don't validate -- Alignment: 2/3

**Current state:**
`metadata_codec.py` follows the parse-don't-validate pattern well: `decode_metadata_map`, `decode_metadata_list`, and `decode_metadata_scalar_map` all parse raw bytes into typed `StructBaseCompat` objects (`MetadataMapPayload`, `MetadataListPayload`, `MetadataScalarPayload`) via the generic `_decode_payload` helper at line 234. The `_SemanticExtensionType` deserialization at `semantic.py:70-90` also parses into structured `SemanticTypeInfo`.

**Findings:**
- `metadata_codec.py:234-256`: Generic `_decode_payload[T]` parses bytes into typed structs -- exemplary.
- `metadata.py:329-348`: `extractor_option_defaults_from_metadata` parses then re-validates with isinstance checks -- the parsing boundary is clean but the downstream `_extractor_defaults_from_row` at line 398 re-validates individual fields. This is a symptom of the payload being decoded to `object` via `loads_msgpack(..., target_type=object, strict=False)` rather than a typed struct.
- `metadata.py:1170-1195`: `_ordering_keys_from_payload` similarly uses `loads_msgpack(..., target_type=object)` and then manually validates, rather than parsing into a typed struct.

**Suggested improvement:**
Define typed msgspec structs for `_ordering_keys_payload` and `_extractor_defaults_payload` (similar to `MetadataListPayload`) and parse directly into those types instead of using `target_type=object` with manual validation.

**Effort:** small
**Risk if unaddressed:** low

---

#### P10. Make illegal states unrepresentable -- Alignment: 2/3

**Current state:**
`EncodingPolicy` at `encoding.py:22-88` allows potentially inconsistent state: `dictionary_cols`, `specs`, `dictionary_index_types`, and `dictionary_ordered_flags` can all be specified independently, and `__post_init__` only normalizes when the derived fields are empty. A caller could provide `specs` with different columns than `dictionary_cols`.

**Findings:**
- `encoding.py:25-30`: `EncodingPolicy` has 6 fields, of which 3 are derived from `specs`. The `__post_init__` at line 32-48 only populates empty fields, meaning if both `specs` and `dictionary_cols` are provided with conflicting data, the conflict goes undetected.
- `metadata.py:103-108`: `SchemaMetadataSpec` correctly uses a simple, hard-to-misuse structure (`schema_metadata` dict + `field_metadata` dict).
- `metadata_codec.py:16-48`: Payload structs are frozen and narrow -- good.

**Suggested improvement:**
Make `EncodingPolicy` constructible only from `specs` (the primary source), deriving `dictionary_cols`, `dictionary_index_types`, and `dictionary_ordered_flags` as computed properties or make them private. Alternatively, validate consistency in `__post_init__` when both `specs` and `dictionary_cols` are provided.

**Effort:** small
**Risk if unaddressed:** low

---

#### P11. CQS -- Alignment: 2/3

**Current state:**
Most functions are clean queries (return data) or commands (mutate state). The main violation is `_extension_type` at `semantic.py:128-130`.

**Findings:**
- `semantic.py:128-130`: `_extension_type(info)` calls `register_semantic_extension_types()` (a side effect: registering types with PyArrow's global registry) before returning the extension type. This mixes query (create type object) with command (register globally). Every call to `span_type()`, `byte_span_type()`, `node_id_type()`, etc. triggers this global side effect.
- `abi.py:20`: `_resolve_schema` calls `register_semantic_extension_types()` -- same pattern, side-effectful query.
- All other functions are clean queries or commands.

**Suggested improvement:**
Call `register_semantic_extension_types()` once during module initialization or application bootstrap rather than on every type access. The `_extension_type` function should assume registration has occurred.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Composition

#### P12. Dependency inversion + explicit composition -- Alignment: 2/3

**Current state:**
Protocol-based typing provides good dependency inversion. The main violation is `encoding/policy.py:132-136` where `_datafusion_context()` internally creates a `DataFusionRuntimeProfile` and extracts a session context.

**Findings:**
- `encoding/policy.py:132-136`: Hidden creation of `DataFusionRuntimeProfile` and `SessionContext`. This is the only place in scope that creates an external resource internally.
- All other modules receive their dependencies as parameters.

**Suggested improvement:**
Add a `ctx: SessionContext | None = None` parameter to `apply_encoding()` and `encode_table()`. When `None`, fall back to the internal creation (preserving backward compatibility), but allow callers to inject their own context.

**Effort:** small
**Risk if unaddressed:** low

---

#### P13. Prefer composition over inheritance -- Alignment: 3/3

**Current state:**
The entire scope uses dataclass composition. `_SemanticExtensionType` extends `pa.ExtensionType` at `semantic.py:93`, which is required by the PyArrow extension type API and not avoidable. `NormalizePolicy` composes `EncodingPolicy` + `ChunkPolicy`. `ColumnDefaultsSpec` composes `ColumnExpr` protocol implementations.

No action needed.

---

#### P14. Law of Demeter -- Alignment: 2/3

**Current state:**
Most code talks to direct collaborators. Some violations exist where functions reach through multiple levels of cast objects.

**Findings:**
- `metadata.py:129`: `cast("StructTypeProtocol", field.type)` followed by iteration -- acceptable for type narrowing.
- `build.py:663-672`: `value_field()` chains `cast("pa.MapType", dtype).item_type`, `cast("pa.MapType", dtype).key_type`, and `getattr(map_type_value, "keys_sorted", False)` -- three levels of attribute access through a cast.
- `nested.py:764-767`: `nested_array_factory` casts `dtype` to `pa.MapType` and then accesses `.key_type`, `.item_type`, `.key_field.type`, `.item_field.type` -- four-level chain through the map type.

**Suggested improvement:**
Extract map-type field access into a helper function in `types.py` (e.g., `map_type_fields(dtype) -> tuple[DataTypeLike, DataTypeLike, bool]`) to encapsulate the chained access.

**Effort:** small
**Risk if unaddressed:** low

---

#### P15. Tell, don't ask -- Alignment: 2/3

**Current state:**
`SchemaMetadataSpec.apply()` at `metadata.py:246-274` is a good "tell" pattern: it takes a schema and returns an updated schema. The `ChunkPolicy.apply()` and `NormalizePolicy.apply()` follow the same pattern. However, many metadata extraction functions are "ask" patterns that return raw dictionaries.

**Findings:**
- `metadata.py:656-670`: `schema_constraints_from_metadata` extracts raw tuples from metadata -- "ask" pattern.
- `metadata.py:673-697`: `schema_identity_from_metadata` returns a raw dict -- "ask" pattern.
- These are acceptable for boundary parsing where the caller needs the raw data. The concern is minor.

**Suggested improvement:**
No significant changes needed. The "ask" patterns at the metadata parsing boundary are idiomatic for this type of module.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Correctness

#### P16. Functional core, imperative shell -- Alignment: 2/3

**Current state:**
The vast majority of functions are pure transforms: metadata encoding/decoding, schema manipulation, array building. The only impure operations are `register_semantic_extension_types()` (global state mutation) and `encoding/policy.py:apply_encoding()` (creates SessionContext, registers temp table).

**Findings:**
- `encoding/policy.py:73-88`: `apply_encoding` creates a `SessionContext`, registers a temporary table, runs a DataFusion query, and returns the result. This is the "shell" but it lives inside what otherwise appears to be a pure module.
- `semantic.py:115-125`: `register_semantic_extension_types` mutates global PyArrow state.
- All other functions (metadata.py, metadata_codec.py, nested.py, build.py, types.py, coercion.py, dictionary.py) are pure transforms.

**Suggested improvement:**
Move `apply_encoding` to the `encoding/` package (which it already partially lives in) and document that it is the imperative shell for encoding operations. The `arrow/` package should remain purely functional.

**Effort:** small
**Risk if unaddressed:** low

---

#### P17. Idempotency -- Alignment: 3/3

**Current state:**
All operations in scope are safe to re-run. `SchemaMetadataSpec.apply()` produces the same output for the same input. `register_semantic_extension_types` uses `contextlib.suppress(ValueError, pa.ArrowKeyError)` to safely handle re-registration. `normalize_dictionaries` is idempotent. `apply_encoding` is idempotent (dictionary-encoding already-encoded columns is a no-op via the `patypes.is_dictionary` check at `encoding/policy.py:116-117`).

No action needed.

---

#### P18. Determinism / reproducibility -- Alignment: 3/3

**Current state:**
The codebase is intentionally deterministic. Metadata payloads use sorted keys (`metadata_codec.py:71`, `metadata.py:360`). Options hashing uses `hash_msgpack_canonical` with a version field (`metadata.py:286-287`). Ordering keys payloads are versioned (`ORDERING_KEYS_VERSION`). Encoding policy fingerprints sort their specs (`encoding.py:64`).

No action needed.

---

### Category: Simplicity

#### P19. KISS -- Alignment: 2/3

**Current state:**
`nested.py` handles inherent complexity (union, struct, list, map, dictionary array construction) and decomposes it well into small functions. The `_tagged_union_values` / `_tagged_sparse_union_values` split could be unified but the current approach is clear. `metadata.py`'s complexity is artificial -- it is complex because it combines too many concerns, not because any single concern is complex.

**Findings:**
- `nested.py:128-143`: `_union_child_index` dispatches through predicates and castability -- complex but inherently so.
- `metadata.py:109-244`: Nested metadata traversal through struct/list/map types -- complex recursive logic that handles all Arrow container types.
- `build.py:811-823`: `array_from_lists` is defined after `__all__` at line 754 -- surprising placement.

**Suggested improvement:**
Move `array_from_lists` above the `__all__` declaration in `build.py` and add it to `__all__`. The decomposition of `metadata.py` (P2) would reduce per-module complexity significantly.

**Effort:** small
**Risk if unaddressed:** low

---

#### P20. YAGNI -- Alignment: 2/3

**Current state:**
`field_builders.py` (135 lines) provides thin wrappers like `string_field(name)` that call `pa.field(name, pa.string(), nullable=nullable)`. These save approximately 10 characters per call and add no logic. While not actively harmful, they represent speculative convenience abstractions.

**Findings:**
- `field_builders.py:10-123`: 9 field builder functions that each delegate to `pa.field(name, type, nullable=nullable)` with zero additional logic.
- `schema.py:8-234`: Similarly provides field builders (`id_field`, `version_field`, `timestamp_field`, etc.) that overlap with `field_builders.py` in concept but serve specific semantic purposes (named fields with domain defaults).

**Suggested improvement:**
Consider consolidating `field_builders.py` into `schema.py` or deprecating `field_builders.py` if `schema.py`'s domain-specific builders are sufficient. Alternatively, if `field_builders.py` is used by external callers, keep it but acknowledge its minimal value.

**Effort:** small
**Risk if unaddressed:** low

---

#### P21. Least astonishment -- Alignment: 2/3

**Current state:**
Most APIs behave as expected. Two naming issues stand out.

**Findings:**
- `types.py:48-56`: `list_view_type(value_type, *, large=False)` returns `pa.list_(value_type)` when `large=False` and `pa.large_list(value_type)` when `large=True`. Despite its name suggesting it creates a `list_view` type, it creates regular `list_` or `large_list` types. This is actively misleading.
- `build.py:435-443`: `table_from_row_dicts` is a thin alias for `table_from_rows` with no additional logic or documentation about why it exists. A caller cannot easily distinguish when to use which.
- `metadata.py:1150-1167`: `_metadata_payload_for_schema` calls `metadata_spec_from_schema(schema)` twice, creating redundant work. `metadata_payload` at line 1157 is a public wrapper around this private function with no additional logic.

**Suggested improvement:**
Rename `list_view_type` to `list_type` or `flexible_list_type` to reflect that it creates regular list types (not list_view types). Remove `table_from_row_dicts` alias or document its purpose. Fix the double call in `_metadata_payload_for_schema`.

**Effort:** small
**Risk if unaddressed:** low

---

#### P22. Declare and version public contracts -- Alignment: 2/3

**Current state:**
All modules declare `__all__` exports explicitly. Metadata payload formats are versioned (`METADATA_PAYLOAD_VERSION`, `ORDERING_KEYS_VERSION`, `OPTIONS_HASH_VERSION`, `EXTRACTOR_DEFAULTS_VERSION`, `SCHEMA_ABI_VERSION`). However, there are no stability markers distinguishing stable vs experimental APIs.

**Findings:**
- `metadata.py:1216-1262`: 47 `__all__` exports -- all public, no stability markers.
- `interop.py:794-867`: 74 `__all__` exports -- all public.
- `metadata_codec.py:272-282`: 8 `__all__` exports -- tightly scoped.
- Version constants: `METADATA_PAYLOAD_VERSION = 1`, `ORDERING_KEYS_VERSION = 1`, `OPTIONS_HASH_VERSION = 1`, `EXTRACTOR_DEFAULTS_VERSION = 1`, `SCHEMA_ABI_VERSION = 1` -- all at version 1, suggesting stable initial state.

**Suggested improvement:**
No immediate action needed. The versioning scheme is appropriate for the current maturity level. Consider adding `@deprecated` markers if any of the 47 metadata.py exports are candidates for removal during the P2 decomposition.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Quality

#### P23. Design for testability -- Alignment: 1/3

**Current state:**
`encoding/policy.py` is the primary testability concern: `apply_encoding` creates a `SessionContext` internally, making it impossible to test without standing up a DataFusion session. `metadata.py`'s 1,262-line surface area makes isolated testing difficult -- testing ordering metadata behavior requires importing all evidence metadata, extractor defaults, and function requirements code.

**Findings:**
- `encoding/policy.py:73-88`: `apply_encoding` cannot be tested with a mock `SessionContext` because `_datafusion_context()` is called internally at line 83.
- `metadata.py`: No dedicated test file exists for the metadata module. `tests/unit/test_schema_adapter_metadata.py` tests some behavior but metadata.py's coverage is likely incomplete given its size and complexity.
- `metadata_codec.py`: Has a dedicated test at `tests/unit/datafusion_engine/test_metadata_codec.py` -- good.
- `nested.py`: Indirectly tested via `tests/unit/test_datafusion_nested_registry.py`.
- `interop.py`: Tested via `tests/unit/test_arrow_interop_capsules.py` and `tests/unit/test_arrow_coercion.py`.

**Suggested improvement:**
1. Accept `SessionContext` as a parameter in `apply_encoding()` (see P12).
2. After decomposing `metadata.py` (P2), add focused test files for each sub-module.
3. The pure-functional nature of most code in scope is inherently testable -- the main barrier is module size making it hard to identify what needs testing.

**Effort:** medium
**Risk if unaddressed:** medium

---

#### P24. Observability -- Alignment: 0/3

**Current state:**
Zero logging, zero structured tracing, zero metrics across the entire 5,685-line scope. Schema coercion failures, metadata encoding errors, and dictionary normalization issues produce exceptions but provide no structured telemetry for operational diagnosis.

**Findings:**
- No `import logging` in any file in `src/datafusion_engine/arrow/` or `src/datafusion_engine/encoding/`.
- `coercion.py:101-103`: `coerce_table_to_storage` silently succeeds or raises -- no logging of what was coerced.
- `dictionary.py:116-131`: `normalize_dictionaries` catches `TypeError` and re-raises with a generic message -- no logging of which column failed.
- `encoding/policy.py:73-88`: `apply_encoding` registers a temp table, runs a query, and returns -- no telemetry on encoding duration or column count.
- `metadata.py:246-274`: `SchemaMetadataSpec.apply()` silently applies metadata -- no logging when fields are updated.
- `semantic.py:115-125`: `register_semantic_extension_types` suppresses errors silently -- no logging of what was registered or what failed.

**Suggested improvement:**
1. Add a module-level `logger = logging.getLogger(__name__)` to `encoding/policy.py`, `dictionary.py`, `coercion.py`, and `semantic.py`.
2. Add `logger.debug` calls for: encoding policy application (columns encoded, duration), dictionary normalization (columns normalized), semantic type registration (types registered).
3. Add `logger.warning` for suppressed exceptions in `register_semantic_extension_types`.
4. Consider adding OpenTelemetry spans to `apply_encoding` to align with the project's existing observability patterns in `src/obs/`.

**Effort:** medium
**Risk if unaddressed:** high -- schema-related failures in production are extremely difficult to diagnose without telemetry, and these modules are used by 20+ downstream consumers.

---

## Cross-Cutting Themes

### 1. metadata.py as a gravity well

**Root cause:** `metadata.py` grew organically by accretion -- each new metadata concern was added to the existing file because it dealt with "metadata." Over time, six distinct sub-concerns accumulated into a 1,262-line module with 47 exports. This affects P2, P3, P4, P19, and P23.

**Affected principles:** P2 (separation of concerns), P3 (SRP), P4 (cohesion), P19 (KISS), P23 (testability).

**Suggested approach:** Decompose into 5 focused sub-modules as described in P2. Use a thin `metadata.py` facade initially for backward compatibility, then migrate consumers to direct imports over time.

### 2. Duplicated schema resolution and type conversion knowledge

**Root cause:** Functions like `_resolve_schema`, `_storage_type`, `_ensure_arrow_dtype`, and `DEFAULT_DICTIONARY_INDEX_TYPE` were independently developed in multiple modules rather than being centralized. This is a classic DRY violation at the knowledge level.

**Affected principles:** P7 (DRY), P10 (illegal states).

**Suggested approach:** Centralize `_resolve_schema` and `_ensure_arrow_dtype` in `interop.py` (or a shared `arrow/_conversions.py`). Use `coercion.py:storage_type` as the canonical storage type resolver. Consolidate `DEFAULT_DICTIONARY_INDEX_TYPE` to a single location in `types.py`.

### 3. Complete absence of observability

**Root cause:** These modules were built as pure utility layers where observability was not considered necessary. However, their position as the Arrow abstraction boundary for the entire codebase means that failures here cascade widely.

**Affected principles:** P24 (observability).

**Suggested approach:** Add structured logging to the 4 modules with IO or global state effects (`encoding/policy.py`, `dictionary.py`, `coercion.py`, `semantic.py`). The pure-transform modules (`metadata_codec.py`, `nested.py`, `build.py`) need minimal observability.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7 (DRY) | Consolidate `DEFAULT_DICTIONARY_INDEX_TYPE` from `types.py:94` and `encoding/policy.py:28` to single location | small | Eliminates divergence risk |
| 2 | P7 (DRY) | Replace `encoding/policy.py:_ensure_arrow_dtype` with import of `interop.py:ensure_arrow_dtype` | small | Removes duplicated conversion logic |
| 3 | P21 (Least astonishment) | Rename `types.py:list_view_type` to `list_type` since it returns `pa.list_`/`pa.large_list`, not list_view | small | Prevents caller confusion |
| 4 | P24 (Observability) | Add `logger.warning` to `semantic.py:register_semantic_extension_types` for suppressed errors | small | Makes silent failures visible |
| 5 | P7 (DRY) | Delete `build.py:_storage_type`/`_storage_schema` and import from `coercion.py:storage_type`/`storage_schema` | small | Eliminates superset divergence |

## Recommended Action Sequence

1. **Consolidate duplicated knowledge (P7)** -- Merge `DEFAULT_DICTIONARY_INDEX_TYPE`, `_ensure_arrow_dtype`, `_resolve_schema`, and `_storage_type`/`_storage_schema` to their canonical locations. This is risk-free and immediately reduces maintenance burden. Touches `types.py`, `encoding/policy.py`, `build.py`, `abi.py`, `interop.py`, `coercion.py`.

2. **Add observability to IO-adjacent modules (P24)** -- Add structured logging to `encoding/policy.py`, `dictionary.py`, `coercion.py`, and `semantic.py`. This is independent of all other changes and immediately improves diagnosability.

3. **Fix naming surprises (P21)** -- Rename `list_view_type` to `list_type` in `types.py` and update the 3 callers (`build.py`, `nested.py`, and external). Small, focused change with clear benefit.

4. **Make `apply_encoding` injectable (P12, P23)** -- Add `ctx: SessionContext | None = None` parameter to `apply_encoding()`. This unblocks testability and respects dependency direction without breaking existing callers.

5. **Decompose metadata.py (P2, P3, P4)** -- Split into 5 focused sub-modules. This is the largest change and should be done after steps 1-4 have stabilized the foundation. Use a facade `metadata.py` initially to avoid a big-bang migration.

6. **Extract ColumnExpr types from build.py (P3)** -- Move `ConstExpr`, `FieldExpr`, `ColumnOrNullExpr`, `CoalesceExpr`, `ColumnDefaultsSpec` to `arrow/column_expr.py`. This depends on step 5 being complete so the package structure is stable.
