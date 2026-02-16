# Design Review: Semantic Catalog, Entity Model & Registry Subsystem

**Date:** 2026-02-16
**Scope:** `src/semantics/catalog/`, `src/semantics/registry.py`, `src/semantics/entity_registry.py`, `src/semantics/entity_model.py`, `src/semantics/resolver_identity.py`
**Focus:** All principles (1-24), with emphasis on registry protocol adherence, DRY, information hiding, SRP, and dependency direction
**Depth:** deep (all files in scope)
**Files reviewed:** 16

## Executive Summary

The Catalog, Entity Model & Registry subsystem is generally well-structured, with a clean entity model (`entity_model.py`) that achieves excellent information hiding, and a resolver identity tracker (`resolver_identity.py`) that demonstrates strong design-by-contract. The primary weaknesses concentrate on two themes: (1) a near-identical twin data class problem between `DatasetRegistrySpec` and `SemanticDatasetRow` (all 20 fields duplicated), and (2) multiple module-level mutable caches without thread safety or invalidation contracts, creating a latent correctness risk. The `analysis_builders.py` file is the largest in scope and contains significant code duplication across its DataFrame builders.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 2 | small | low | Module-level `_CACHE` internals are well-hidden, but `DatasetRegistrySpec` exposes 20 fields identical to `SemanticDatasetRow` |
| 2 | Separation of concerns | 2 | medium | medium | `analysis_builders.py` mixes DataFusion SQL plumbing with domain normalization logic |
| 3 | SRP | 1 | medium | medium | `analysis_builders.py` has 14 builder functions covering 6+ distinct domains |
| 4 | High cohesion, low coupling | 2 | small | low | Catalog subpackage is cohesive; cross-module coupling is narrow |
| 5 | Dependency direction | 2 | medium | medium | `semantic_singletons_registry.py` reaches into `relspec.contracts` and `relspec.view_defs` |
| 6 | Ports & Adapters | 2 | small | low | `MappingRegistryAdapter` properly adapts registries; no pure port abstraction needed |
| 7 | DRY | 1 | medium | high | `DatasetRegistrySpec` duplicates all fields of `SemanticDatasetRow`; `TableType` mapping duplicated in `tags.py` |
| 8 | Design by contract | 2 | small | low | `resolver_identity.py` exemplary; caches lack postcondition guarantees |
| 9 | Parse, don't validate | 2 | small | low | `entity_model.py` uses `StructBaseStrict` for parsing; scattered `KeyError` checks in `dataset_specs.py` |
| 10 | Make illegal states unrepresentable | 2 | small | medium | `SemanticDatasetRow` allows many impossible combinations (e.g., `role="input"` with `register_view=True`) |
| 11 | CQS | 2 | small | low | Mostly pure queries; `_get_*` cache functions mix mutation with return |
| 12 | DI + explicit composition | 2 | small | low | `MappingRegistryAdapter.from_mapping()` with `read_only=True` is good composition |
| 13 | Composition over inheritance | 3 | -- | -- | No inheritance hierarchies; all composition via frozen dataclasses |
| 14 | Law of Demeter | 2 | small | low | `spec.spec.primary_span.canonical_start` in `ir_pipeline.py:364` is a 3-deep chain |
| 15 | Tell, don't ask | 2 | small | low | `SemanticDatasetRow` is a data carrier; inference logic in `tags.py` asks raw fields |
| 16 | Functional core, imperative shell | 2 | small | low | Entity model is purely functional; caches are imperative at module scope |
| 17 | Idempotency | 3 | -- | -- | All builders produce same output for same inputs; caches are idempotent |
| 18 | Determinism | 3 | -- | -- | Entity declarations and spec generation are fully deterministic |
| 19 | KISS | 2 | small | low | Alias system in `dataset_specs.py` maps names to themselves; unnecessary complexity |
| 20 | YAGNI | 2 | small | low | `ctx` parameter in `dataset_spec()` is unused, reserved for "future runtime enrichment" |
| 21 | Least astonishment | 2 | small | low | `dataset_specs()` returns a generator (Iterable), not a tuple; inconsistent with `dataset_names()` |
| 22 | Public contracts | 2 | small | low | `__all__` declarations are thorough; no versioned contract boundary |
| 23 | Testability | 2 | medium | medium | Entity model well-tested; catalog caches have no reset mechanism for testing |
| 24 | Observability | 1 | medium | medium | No structured logging in any catalog module; silent fallbacks in `analysis_builders.py` |

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information hiding -- Alignment: 2/3

**Current state:**
The catalog subpackage uses `__init__.py` (line 1-92) to curate a public surface, re-exporting only stable accessors. Internal caches (`_DatasetSpecCache`, `_SemanticDatasetRowCache`) use leading underscore convention and are not re-exported. However, `DatasetRegistrySpec` is imported directly by external modules.

**Findings:**
- `src/semantics/catalog/dataset_registry.py:14-39`: `DatasetRegistrySpec` has 20 fields with an exact 1:1 correspondence to `SemanticDatasetRow`, but is a separate public type. Consumers in `ir_pipeline.py` must understand both types and the conversion between them.
- `src/semantics/catalog/dataset_specs.py:23-28`: `_DatasetSpecCache` is properly private with underscore prefix, hiding 5 lazily-initialized mapping caches behind function accessors.
- `src/semantics/catalog/analysis_builders.py:36-38`: Module-level constants `_DEF_USE_OPS`, `_DEF_USE_PREFIXES`, `_USE_PREFIXES` are properly private.

**Suggested improvement:**
Consider whether `DatasetRegistrySpec` should be an internal-only type that is never exported -- it appears to serve solely as input to `_row_from_registry_spec()` in `ir_pipeline.py`. If so, mark it private or merge it with `SemanticDatasetRow` using a factory method pattern.

**Effort:** small
**Risk if unaddressed:** low

---

#### P2. Separation of concerns -- Alignment: 2/3

**Current state:**
The catalog package broadly separates dataset metadata (rows, specs, tags) from DataFrame construction (analysis_builders, projections). However, `analysis_builders.py` is a 740-line file that mixes DataFusion expression construction (SQL plumbing) with domain normalization logic.

**Findings:**
- `src/semantics/catalog/analysis_builders.py:570-580`: `file_quality_df_builder` delegates to `semantics.signals`, which is clean separation. But `type_exprs_df_builder` (line 151-211) embeds column selection policy, hash ID generation, and span construction in a single function.
- `src/semantics/catalog/analysis_builders.py:246-277`: `type_nodes_df_builder` contains a try/except that silently swallows `RuntimeError, KeyError, ValueError` when attempting SCIP table access, mixing error policy with data plumbing.
- `src/semantics/catalog/analysis_builders.py:487-551`: `diagnostics_df_builder` similarly catches `(RuntimeError, KeyError, ValueError)` at line 536 for graceful degradation, but the catch is broad and the comment at line 503 states "simplified version."

**Suggested improvement:**
Extract the common code-unit join pattern (lines 282-377) into a private helper like `_join_with_code_units(df, ctx)`. Extract the stable-ID + span construction pattern into a reusable `NormalizationExprKit` or similar value object that bundles `_stable_id_expr` + `_span_expr` + `_hash_part` together with consistent null handling.

**Effort:** medium
**Risk if unaddressed:** medium -- maintenance cost increases with each new builder

---

#### P3. SRP (one reason to change) -- Alignment: 1/3

**Current state:**
`analysis_builders.py` has 14 exported builder functions spanning at least 6 distinct analytic domains (type expressions, CFG blocks/edges, def-use events, reaching definitions, diagnostics, quality metrics, file coverage, exported defs). These would change for independent reasons.

**Findings:**
- `src/semantics/catalog/analysis_builders.py:1-741`: This file changes whenever any of these domains evolves. The type expression builders (lines 151-279) have no connection to the CFG builders (lines 282-377) or the diagnostic builders (lines 487-707).
- `src/semantics/registry.py:1-400`: This file also mixes multiple concerns: table spec generation (line 23-39), normalization spec definitions (lines 57-107), lookup helpers (lines 110-181), relationship specs (lines 194-225), output spec generation (lines 298-358), and the `SemanticModel` class (lines 252-296). It would change for normalization-related, relationship-related, or output-manifest-related reasons independently.
- `src/semantics/catalog/spec_builder.py:140-268`: The chain of `_*_field_specs` functions (6 of them) implement a dispatch pattern by checking `row.name` or `row.template` with string comparisons, and each handles a different domain.

**Suggested improvement:**
Split `analysis_builders.py` into domain-specific builder modules: `type_builders.py`, `cfg_builders.py`, `def_use_builders.py`, `diagnostic_builders.py`. The shared expression helpers (`_stable_id_expr`, `_span_expr`, `_coalesce_cols`, etc.) should live in a shared `builder_exprs.py` module. For `registry.py`, consider separating `SemanticModel` construction from the static normalization and relationship spec definitions.

**Effort:** medium
**Risk if unaddressed:** medium -- the 740-line `analysis_builders.py` will continue growing as new builder types are added

---

#### P4. High cohesion, low coupling -- Alignment: 2/3

**Current state:**
The catalog subpackage is internally cohesive -- `dataset_rows.py`, `dataset_specs.py`, and `spec_builder.py` form a tight trio. External coupling is narrow (accessed via `__init__.py` re-exports). However, there are some unnecessary coupling points.

**Findings:**
- `src/semantics/catalog/semantic_singletons_registry.py:7-11`: Imports from `relspec.contracts` and `relspec.view_defs` -- the catalog module reaching into the task/plan catalog layer. This inverts the expected dependency direction (semantic catalog should be upstream of relspec).
- `src/semantics/catalog/tags.py:12`: Imports `ColumnType, TableType, infer_column_type, infer_table_type` from `semantics.column_types`, plus `CompatibilityGroup, get_compatibility_groups` from `semantics.types.core`. This coupling is reasonable but pulls in the full type inference system for tag generation.

**Suggested improvement:**
Move the `RELATION_OUTPUT_NAME` constant and `RELATION_OUTPUT_ORDERING_KEYS` references in `semantic_singletons_registry.py` to a shared location in `semantics/` rather than importing from `relspec/`. This would keep the dependency direction clean: `relspec` depends on `semantics`, not the reverse.

**Effort:** small
**Risk if unaddressed:** low

---

#### P5. Dependency direction -- Alignment: 2/3

**Current state:**
The overall direction is correct: `entity_model.py` depends only on `semantics.specs` and `serde_msgspec`. `entity_registry.py` depends on `entity_model` (downstream). `registry.py` depends on `entity_registry` (downstream). The catalog package depends on the semantic core. However, there are reverse dependencies.

**Findings:**
- `src/semantics/catalog/semantic_singletons_registry.py:7-8`: `from relspec.contracts import RELATION_OUTPUT_ORDERING_KEYS` and `from relspec.view_defs import RELATION_OUTPUT_NAME` -- the semantic catalog depends on `relspec`, but `relspec` is a downstream consumer of semantic outputs. This creates a circular dependency direction.
- `src/semantics/catalog/analysis_builders.py:596-597`: Deferred imports from `semantics.diagnostics` and `semantics.spec_registry` -- these are peer modules within `semantics/`, which is acceptable.
- `src/semantics/catalog/spec_builder.py:251,274`: Deferred imports from `datafusion_engine.extract.registry` inside `_semantic_normalize_field_specs` and `_input_field_specs` -- the catalog reaches into the extract layer for schema information.

**Suggested improvement:**
Extract `RELATION_OUTPUT_NAME` and `RELATION_OUTPUT_ORDERING_KEYS` into a shared constants module in `semantics/` (e.g., `semantics/output_names.py`) that both `relspec` and the catalog can depend on. This eliminates the reverse dependency.

**Effort:** medium
**Risk if unaddressed:** medium -- the circular dependency direction between `semantics.catalog` and `relspec` risks becoming entangled as both grow

---

#### P6. Ports & Adapters -- Alignment: 2/3

**Current state:**
`registry.py` uses `MappingRegistryAdapter.from_mapping(..., read_only=True)` from `src/utils/registry_protocol.py` for its three registry factories (`semantic_table_registry`, `semantic_normalization_registry`, `semantic_relationship_registry`). This is proper adapter usage.

**Findings:**
- `src/semantics/registry.py:110-131`: Three factory functions return `MappingRegistryAdapter` instances, conforming to the project's standard registry protocol. Well aligned.
- The catalog registries (`normalize_registry.py`, `diagnostics_registry.py`, `export_registry.py`, `semantic_singletons_registry.py`) are static tuples rather than `Registry` protocol implementors. These are consumed only by `ir_pipeline.py` and do not need the adapter pattern.

**Suggested improvement:**
No changes needed. The static tuple pattern for one-way consumption is simpler and justified (YAGNI). The `MappingRegistryAdapter` usage in `registry.py` for lookup-based consumption is appropriate.

**Effort:** --
**Risk if unaddressed:** --

---

### Category: Knowledge (7-11)

#### P7. DRY (knowledge, not lines) -- Alignment: 1/3

**Current state:**
This is the most significant violation in the subsystem. `DatasetRegistrySpec` and `SemanticDatasetRow` encode the same 20-field schema in two separate places, with a field-by-field copy function bridging them.

**Findings:**
- `src/semantics/catalog/dataset_registry.py:14-39` vs `src/semantics/catalog/dataset_rows.py:22-102`: Both dataclasses have identical field names, identical types, and identical defaults for all 20 fields (`name`, `version`, `bundles`, `fields`, `category`, `supports_cdf`, `partition_cols`, `merge_keys`, `join_keys`, `template`, `view_builder`, `kind`, `semantic_id`, `entity`, `grain`, `stability`, `schema_ref`, `materialization`, `materialized_name`, `metadata_extra`, `register_view`, `source_dataset`, `role`). The only difference is that `DatasetRegistrySpec` uses a `DEFAULT_DATASET_KIND` constant while `SemanticDatasetRow` uses `"table"` inline.
- `src/semantics/ir_pipeline.py:319-349`: `_row_from_registry_spec()` is a 30-line function that copies every field from `DatasetRegistrySpec` to `SemanticDatasetRow` one by one. This is the classic symptom of duplicated knowledge: a mechanical mapping function that must be updated whenever a field is added to either type.
- `src/semantics/catalog/tags.py:171-177` and `src/semantics/catalog/tags.py:183-189`: The `TableType` to `(entity, grain)` mapping is duplicated in two functions: `_infer_entity_from_schema` (line 171) and `_infer_entity_grain_from_table_type` (line 183). Both contain the identical 4-entry dict: `{RELATION: ("edge","per_edge"), ENTITY: ("entity","per_entity"), EVIDENCE: ("evidence","per_evidence"), SYMBOL_SOURCE: ("symbol","per_symbol")}`.
- `src/semantics/catalog/analysis_builders.py:282-331` and `src/semantics/catalog/analysis_builders.py:334-377`: The code-unit join logic is copy-pasted between `cfg_blocks_df_builder` and `cfg_edges_df_builder`. Both perform: (1) select code_units with alias columns, (2) left join on `code_unit_id`, (3) coalesce `file_id` and `path`. The only difference is the primary table name (`py_bc_blocks` vs `py_bc_cfg_edges`).

**Suggested improvement:**
1. Eliminate `DatasetRegistrySpec` entirely. Have the sub-registries (`NORMALIZE_DATASETS`, `DIAGNOSTIC_DATASETS`, etc.) declare `SemanticDatasetRow` instances directly, or introduce a single factory function on `SemanticDatasetRow` that accepts the same kwargs. This eliminates the conversion function and the dual-type maintenance burden.
2. Extract the `TableType -> (entity, grain)` mapping into a module-level constant `_TABLE_TYPE_GRAIN_MAP` and reference it from both `_infer_entity_from_schema` and `_infer_entity_grain_from_table_type`.
3. Extract the code-unit join pattern into `_join_with_code_units(primary_df, ctx, code_units_table)`.

**Effort:** medium
**Risk if unaddressed:** high -- every new dataset field must be added in 3 places (both types + the conversion function), and the duplicated mapping will inevitably drift

---

#### P8. Design by contract -- Alignment: 2/3

**Current state:**
`resolver_identity.py` is exemplary for design by contract: `assert_identity()` (line 91-101) provides an explicit postcondition check, `record_resolver` has a clear precondition (tracker must be active), and the `strict` parameter on the context manager automates verification.

**Findings:**
- `src/semantics/resolver_identity.py:63-89`: `verify_identity()` returns structured violation descriptions, enabling both automated assertion and human-readable diagnostics. Well aligned with DBC.
- `src/semantics/catalog/dataset_specs.py:136-148`: `_get_alias_maps()` checks a uniqueness invariant at line 139-141, raising `ValueError` for duplicate dataset names. This is good contract enforcement but could be made more explicit with a dedicated invariant check.
- `src/semantics/catalog/dataset_rows.py:154-169`: `dataset_row(..., strict=True)` uses an overloaded signature to enforce the contract at the type level -- `strict=True` guarantees a non-None return. Good use of `@overload`.

**Suggested improvement:**
Add a module-level docstring or `assert` in `dataset_rows.py` that verifies the ordering invariant documented in its docstring ("dependency order"). Currently, the ordering is implicitly trusted from `semantic_ir_for_outputs()`.

**Effort:** small
**Risk if unaddressed:** low

---

#### P9. Parse, don't validate -- Alignment: 2/3

**Current state:**
`entity_model.py` uses `StructBaseStrict` (msgspec) for its domain types, which parses and validates at construction time. Once constructed, `EntityDeclaration`, `IdentitySpec`, `LocationSpec`, and `ForeignKeySpec` instances are guaranteed well-formed.

**Findings:**
- `src/semantics/entity_model.py:14-72`: All four entity model structs use `StructBaseStrict` with `frozen=True`, ensuring immutability after construction. Illegal field combinations would raise at construction time.
- `src/semantics/catalog/dataset_specs.py:151-167`: `dataset_spec()` validates by checking key presence in a dict, but then raises `KeyError` with a custom message. This is validate-then-use rather than parse-once.
- `src/semantics/catalog/spec_builder.py:292-314`: `_field_specs_for_row()` uses a chain-of-responsibility pattern (6 builder functions returning `None` to signal "not my type") -- this is effectively late-binding dispatch based on string fields (`row.name`, `row.template`, `row.role`). Structural typing or an enum could parse this dispatch key earlier.

**Suggested improvement:**
Consider making `template` a dedicated enum type rather than `str | None` in `SemanticDatasetRow`. This would let the `_field_specs_for_row` dispatch be a match statement on an enum variant rather than a chain of string checks with sentinel `None` returns.

**Effort:** small
**Risk if unaddressed:** low

---

#### P10. Make illegal states unrepresentable -- Alignment: 2/3

**Current state:**
`SemanticDatasetRow` has 20+ fields, many of which have implicit co-constraints documented only in behavior, not structure.

**Findings:**
- `src/semantics/catalog/dataset_rows.py:80-102`: Several combinations are impossible but representable: `role="input"` with `register_view=True`; `materialization=None` with `materialized_name="something"`; `supports_cdf=False` with `merge_keys` set. These invariants are enforced procedurally in `spec_builder.py` but not at the type level.
- `src/semantics/entity_model.py:74-115`: `EntityDeclaration` is much better: `identity` and `location` are mandatory (not optional), `span_unit` is a `SpanUnit` literal type, and `normalized_name` defaults cleanly. Few impossible states.
- `src/semantics/catalog/dataset_registry.py:14-39`: Same issue as `SemanticDatasetRow` -- same 20 optional fields, same unconstrained combinations.

**Suggested improvement:**
Consider introducing variant types for different dataset roles. For example, an `InputDatasetRow` vs `OutputDatasetRow` vs `DiagnosticDatasetRow` where each variant only carries the fields relevant to its role, making impossible combinations unrepresentable. This could be done via msgspec `Tagged` unions or separate frozen dataclasses with a shared protocol.

**Effort:** medium (structural change)
**Risk if unaddressed:** medium -- current reliance on procedural checks in `spec_builder.py` makes it easy to create malformed rows

---

#### P11. CQS -- Alignment: 2/3

**Current state:**
Most functions are either pure queries (returning data) or commands (registering data). The lazy caching functions are the main CQS tension point.

**Findings:**
- `src/semantics/catalog/dataset_specs.py:83-95`: `_get_dataset_rows_map()` both mutates `_CACHE.dataset_rows` and returns the value. This is a minor CQS tension -- the mutation is a caching side effect, not a business state change.
- `src/semantics/catalog/dataset_rows.py:114-128`: Same pattern for `_get_semantic_dataset_rows()`.
- `src/semantics/resolver_identity.py:39-51`: `record_resolver()` is a pure command with no return value. Well aligned.
- All 14 builder functions in `analysis_builders.py` are pure queries (return DataFrame, no mutation). Well aligned.

**Suggested improvement:**
No action needed. The caching side effects in `_get_*` functions are implementation details hidden behind stable query interfaces. The CQS tension is minimal and pragmatic.

**Effort:** --
**Risk if unaddressed:** low

---

### Category: Composition (12-15)

#### P12. DI + explicit composition -- Alignment: 2/3

**Current state:**
`registry.py` uses `MappingRegistryAdapter.from_mapping()` for explicit composition of read-only registries. Builder functions accept `SessionContext` as a parameter (DI). However, some composition is implicit via module-level singletons.

**Findings:**
- `src/semantics/registry.py:39`: `SEMANTIC_TABLE_SPECS` is computed eagerly at module import time via `_generate_semantic_table_specs()`. This tight-couples import time to entity registry processing.
- `src/semantics/registry.py:378`: `SEMANTIC_MODEL` is also computed eagerly at import time. Any import of `semantics.registry` triggers the full model build.
- `src/semantics/catalog/analysis_builders.py:151,214,282,334,403,425,487`: All builder functions accept `SessionContext` as their only parameter -- clean DI pattern.

**Suggested improvement:**
Consider making `SEMANTIC_TABLE_SPECS` and `SEMANTIC_MODEL` lazy (computed on first access) to avoid import-time side effects. A `functools.cache` or explicit lazy accessor would suffice.

**Effort:** small
**Risk if unaddressed:** low -- current eager evaluation works but increases import time

---

#### P13. Composition over inheritance -- Alignment: 3/3

**Current state:**
No inheritance hierarchies exist anywhere in the reviewed scope. All types are frozen dataclasses or msgspec structs. Behavior is composed through function chains and adapter patterns.

**Findings:**
- All entity model types (`IdentitySpec`, `LocationSpec`, `ForeignKeySpec`, `EntityDeclaration`) use composition: `EntityDeclaration` contains `IdentitySpec`, `LocationSpec`, and `tuple[ForeignKeySpec, ...]`.
- `SemanticModel` composes `SemanticNormalizationSpec` and `QualityRelationshipSpec` tuples.
- No class inherits from another class in the entire scope.

**Suggested improvement:**
None needed. Excellent alignment.

**Effort:** --
**Risk if unaddressed:** --

---

#### P14. Law of Demeter -- Alignment: 2/3

**Current state:**
Most access patterns are direct collaborator access. A few chains exceed 2 levels.

**Findings:**
- Used in `ir_pipeline.py:364-366` (consuming `analysis_builders.py` output): `spec.spec.primary_span.canonical_start`, `spec.spec.primary_span.canonical_end`, `spec.spec.entity_id.out_col` -- 3-deep chained attribute access through `SemanticNormalizationSpec.spec.primary_span.canonical_start`.
- `src/semantics/catalog/tags.py:67-68`: `row = dataset_row(canonical, strict=True)` followed by direct field access on `row`. This is fine (1 level).
- `src/semantics/catalog/projections.py:81`: `annotated.columns_by_compatibility_group(...)` returns column specs, then immediate field access `.name`. 2 levels -- acceptable.

**Suggested improvement:**
Add convenience methods on `SemanticNormalizationSpec` to expose commonly-needed derived values like `entity_id_column`, `span_start_col`, `span_end_col` to avoid the `spec.spec.primary_span.canonical_start` chain.

**Effort:** small
**Risk if unaddressed:** low

---

#### P15. Tell, don't ask -- Alignment: 2/3

**Current state:**
`SemanticDatasetRow` and `DatasetRegistrySpec` are primarily data carriers (anemic models). The inference logic operates on their raw fields externally.

**Findings:**
- `src/semantics/catalog/tags.py:143-157`: `_resolve_entity_grain(row)` asks 6+ fields from `row` (`entity`, `grain`, `category`, `join_keys`, `fields`, `partition_cols`) to infer entity/grain. This logic could be a method on the row itself.
- `src/semantics/catalog/tags.py:251-258`: `_resolve_stability(row)` asks `stability` and `category` from `row` to derive a stability string. Again, this could live on the row.
- `src/semantics/entity_model.py:117-130`: `effective_normalized_name()` is a good counter-example -- it encapsulates the naming convention within the `EntityDeclaration` itself. This is "tell" style.

**Suggested improvement:**
Move `_resolve_stability` and a simplified `_resolve_entity_grain` into methods on `SemanticDatasetRow` (or a companion protocol). This keeps the inference logic close to the data it governs.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Correctness (16-18)

#### P16. Functional core, imperative shell -- Alignment: 2/3

**Current state:**
The entity model (`entity_model.py`) and conversion functions (`entity_registry.py`) are fully functional -- frozen types in, frozen types out, no side effects. The imperative shell is the module-level caches and the `analysis_builders.py` functions that interact with DataFusion SessionContext.

**Findings:**
- `src/semantics/entity_model.py:1-138`: Entirely pure. All types are frozen. `effective_normalized_name()` is a pure derivation.
- `src/semantics/entity_registry.py:31-128`: Pure conversion functions. No side effects.
- `src/semantics/catalog/dataset_rows.py:111-127`: Module-level mutable cache `_SEMANTIC_DATASET_ROWS_CACHE` stores state. The imperative shell is thin but it lacks thread safety (no lock).
- `src/semantics/catalog/dataset_specs.py:31`: Same pattern with `_CACHE`.

**Suggested improvement:**
The functional core / imperative shell boundary is clear. The caches should either gain thread safety (locks) or be documented as single-threaded-only to make the contract explicit.

**Effort:** small
**Risk if unaddressed:** low (in practice, these are only populated during pipeline initialization)

---

#### P17. Idempotency -- Alignment: 3/3

**Current state:**
All spec generation, row building, and registry construction produce identical outputs when called with the same inputs. The caches ensure idempotent access.

**Findings:**
- `_get_semantic_dataset_rows()` returns the same tuple on every call after first initialization.
- `build_dataset_spec(row)` is deterministic for a given row.
- `resolver_identity_tracking()` context manager properly restores previous tracker on exit.

**Suggested improvement:**
None needed.

**Effort:** --
**Risk if unaddressed:** --

---

#### P18. Determinism / reproducibility -- Alignment: 3/3

**Current state:**
Entity declarations are defined as a `Final` tuple with explicit ordering. Spec generation preserves declaration order. No nondeterministic operations (no random, no timestamp, no dict iteration order dependencies for outputs).

**Findings:**
- `src/semantics/entity_registry.py:156-237`: `ENTITY_DECLARATIONS` is a `Final[tuple[...]]` with explicit ordering.
- `src/semantics/registry.py:57-100`: `SEMANTIC_NORMALIZATION_SPECS` is a `Final[tuple[...]]` preserving declaration order.
- `src/semantics/catalog/tags.py:87`: `sorted()` is used in `_resolve_edge_owner` for deterministic column ordering.

**Suggested improvement:**
None needed.

**Effort:** --
**Risk if unaddressed:** --

---

### Category: Simplicity (19-22)

#### P19. KISS -- Alignment: 2/3

**Current state:**
Most modules are straightforward. The alias system in `dataset_specs.py` is unnecessarily complex.

**Findings:**
- `src/semantics/catalog/dataset_specs.py:128-148`: `_get_alias_maps()` builds two dicts (`dataset_aliases` and `aliases_to_name`) that both map names to themselves (`aliases = {name: name for name in names}; reverse = aliases.copy()`). This alias system does nothing useful -- it maps every name to itself. The functions `dataset_alias()` and `dataset_name_from_alias()` then check both dicts with fallback logic, but since all mappings are identity mappings, the entire mechanism is a no-op.
- `src/semantics/catalog/spec_builder.py:110-126`: `_bundle()` has a dict lookup plus two hardcoded fallbacks that produce the exact same values the dict already contains. The fallbacks at lines 120-123 duplicate the entries already in `_SEMANTIC_BUNDLE_CATALOG` at lines 101-107.

**Suggested improvement:**
Simplify `_get_alias_maps()` to either implement actual aliasing or remove the alias indirection entirely and replace `dataset_alias(name)` / `dataset_name_from_alias(alias)` with identity functions or remove them from the public API.

**Effort:** small
**Risk if unaddressed:** low -- the complexity is small but misleading

---

#### P20. YAGNI -- Alignment: 2/3

**Current state:**
Minor speculative generality is present in the `ctx` parameter and alias system.

**Findings:**
- `src/semantics/catalog/dataset_specs.py:161-162`: `dataset_spec(name, ctx=None)` accepts `ctx` but immediately discards it with `_ = ctx` and a comment "ctx reserved for future runtime enrichment." This is speculative.
- `src/semantics/catalog/dataset_specs.py:128-148`: The alias system (see P19) builds infrastructure for name aliasing that currently maps everything to itself. No alias mapping is ever registered.

**Suggested improvement:**
Remove the `ctx` parameter from `dataset_spec()`, `maybe_dataset_spec()`, and `dataset_contract()` until runtime enrichment is actually needed. It can be added back when the use case materializes.

**Effort:** small
**Risk if unaddressed:** low

---

#### P21. Least astonishment -- Alignment: 2/3

**Current state:**
API naming is generally consistent and predictable. Return type inconsistencies are the main surprise.

**Findings:**
- `src/semantics/catalog/dataset_specs.py:184-192`: `dataset_specs()` returns `Iterable[DatasetSpec]` (a generator), while the sibling function `dataset_names()` at line 230-238 returns `tuple[str, ...]`. A reader would expect both collection-returning functions to use the same container type.
- `src/semantics/catalog/dataset_rows.py:146-151`: The `dataset_row()` overloads correctly distinguish `strict=True` (returns `SemanticDatasetRow`) from `strict=False` (returns `SemanticDatasetRow | None`). Good use of overloads for predictable API.
- `src/semantics/registry.py:214-225`: `spec_for_relationship(name)` iterates a tuple linearly instead of using the dict that `semantic_relationship_registry()` builds. A reader might expect the dict-based approach given the registry is available.

**Suggested improvement:**
Make `dataset_specs()` return `tuple[DatasetSpec, ...]` for consistency with `dataset_names()`. Change `spec_for_relationship()` to use a dict lookup matching the pattern of `spec_for_table()` at line 183-191.

**Effort:** small
**Risk if unaddressed:** low

---

#### P22. Public contracts -- Alignment: 2/3

**Current state:**
All modules declare `__all__` exports. The `__init__.py` curates a clean public surface. However, there is no versioning or stability annotation beyond the `SEMANTIC_SCHEMA_VERSION` integer.

**Findings:**
- `src/semantics/catalog/__init__.py:62-92`: Comprehensive `__all__` with 28 exports. Good curation.
- `src/semantics/registry.py:380-400`: `__all__` with 18 exports. Good.
- `src/semantics/catalog/dataset_rows.py:18`: `SEMANTIC_SCHEMA_VERSION: Final[int] = 1` provides a version marker but no mechanism for evolution or compatibility checking.

**Suggested improvement:**
No changes needed at this time. The `__all__` curation is thorough. Version evolution can be addressed when schema migration becomes a requirement.

**Effort:** --
**Risk if unaddressed:** low

---

### Category: Quality (23-24)

#### P23. Design for testability -- Alignment: 2/3

**Current state:**
The entity model is well-tested (301 lines in `test_entity_model.py` covering round-trip parity, foreign keys, content columns, span columns, and defaults). The catalog caches are harder to test.

**Findings:**
- `tests/unit/semantics/test_entity_model.py:1-301`: Excellent test coverage for entity model conversions, with parametrized tests for each source table and detailed foreign key verification.
- `src/semantics/catalog/dataset_rows.py:111`: `_SEMANTIC_DATASET_ROWS_CACHE` is a module-level mutable singleton with no reset mechanism. Test isolation requires either monkeypatching the internal `_SEMANTIC_DATASET_ROWS_CACHE.rows` field or reimporting the module.
- `src/semantics/catalog/dataset_specs.py:31`: Same issue with `_CACHE`. No public `reset()` or `clear()` method.
- `src/semantics/catalog/analysis_builders.py:246-279`: `type_nodes_df_builder` has a try/except that silently catches 3 exception types. This makes it difficult to test the SCIP fallback path -- a test must mock the `ctx.table()` call to raise.
- No tests exist for `tags.py`, `projections.py`, or `analysis_builders.py` within the `tests/unit/semantics/` directory (based on file listings).

**Suggested improvement:**
Add a `_reset_cache()` function (underscore-prefixed for test-only use) to both `dataset_rows.py` and `dataset_specs.py` to enable test isolation. Consider adding unit tests for `tag_spec_for_dataset()` and `relation_output_projection()`.

**Effort:** medium
**Risk if unaddressed:** medium -- cache state leaking between tests can cause subtle, order-dependent failures

---

#### P24. Observability -- Alignment: 1/3

**Current state:**
No structured logging, metrics, or tracing exists in any of the 16 reviewed files. The `analysis_builders.py` module silently swallows exceptions in two places. The `resolver_identity.py` module has good diagnostic output but only when explicitly inspected.

**Findings:**
- `src/semantics/catalog/analysis_builders.py:275`: `except (RuntimeError, KeyError, ValueError): pass` -- SCIP table lookup failure is completely silent. No log, no metric, no diagnostic record.
- `src/semantics/catalog/analysis_builders.py:536`: Same pattern for tree-sitter error table lookup failure. Falls back to SQL empty table with no logging.
- `src/semantics/catalog/dataset_rows.py:124-127`: Lazy cache initialization from `semantic_ir_for_outputs()` has no timing metric. Slow IR compilation would be invisible.
- `src/semantics/resolver_identity.py:63-89`: `verify_identity()` produces structured violation descriptions -- this is the only observability-ready code in the scope.
- `src/semantics/registry.py:39,378`: Eager module-level computation of `SEMANTIC_TABLE_SPECS` and `SEMANTIC_MODEL` has no timing or diagnostic output.

**Suggested improvement:**
Add structured logging to the two silent exception handlers in `analysis_builders.py` using the project's observability patterns (`src/obs/`). Add a debug-level log entry when dataset row cache or spec cache is first populated. This enables "what happened?" diagnosis when pipeline outputs are empty.

**Effort:** medium
**Risk if unaddressed:** medium -- silent fallbacks in `analysis_builders.py` make root-cause analysis difficult when outputs are unexpectedly empty

---

## Cross-Cutting Themes

### Theme 1: The Twin Dataclass Problem

**Root cause:** `DatasetRegistrySpec` and `SemanticDatasetRow` were likely introduced at different times with different intents (declarative spec vs runtime row), but converged to identical field sets.

**Affected principles:** P7 (DRY -- duplicated knowledge), P10 (illegal states -- both types permit invalid combinations), P19 (KISS -- unnecessary indirection), P3 (SRP -- the conversion function is maintenance overhead).

**Suggested approach:** Consolidate into a single type. Either make `DatasetRegistrySpec` a type alias for `SemanticDatasetRow`, or introduce a factory classmethod on `SemanticDatasetRow` that the sub-registries can call directly, eliminating the conversion layer.

### Theme 2: Silent Error Handling in Builders

**Root cause:** The graceful degradation architectural principle ("missing optional inputs produce correct-schema empty outputs, not exceptions") is implemented via broad exception catching without logging.

**Affected principles:** P24 (observability -- failures are invisible), P8 (design by contract -- the degradation is implicit, not documented), P2 (separation of concerns -- error policy is mixed into builder plumbing).

**Suggested approach:** Introduce a structured "degradation event" that is logged when a builder falls back to an empty table. This preserves graceful degradation while making it observable.

### Theme 3: Module-Level Mutable Caches

**Root cause:** Both `dataset_rows.py` and `dataset_specs.py` use module-level mutable dataclass instances as caches. This pattern lacks thread safety and test reset mechanisms.

**Affected principles:** P23 (testability -- no reset), P16 (functional core -- caches are imperative state at module scope), P11 (CQS -- `_get_*` functions mix mutation with query).

**Suggested approach:** Consider `functools.cache` on a module-level function (immutable return, automatic memoization), or add explicit `_reset()` hooks for testing and a threading lock for safety.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7 (DRY) | Extract duplicated `TableType -> (entity, grain)` mapping in `tags.py:171-189` into a module-level constant | small | Eliminates exact-copy duplication within a single file |
| 2 | P7 (DRY) | Extract code-unit join pattern from `analysis_builders.py:282-377` into `_join_with_code_units()` helper | small | Removes 40+ lines of identical code |
| 3 | P24 (Observability) | Add `logger.debug()` calls to the two `except (RuntimeError, KeyError, ValueError)` blocks in `analysis_builders.py:275,536` | small | Makes graceful degradation events visible |
| 4 | P19 (KISS) | Remove the identity-mapping alias system in `dataset_specs.py:128-148` or simplify to passthrough | small | Eliminates misleading complexity |
| 5 | P20 (YAGNI) | Remove unused `ctx` parameter from `dataset_spec()`, `maybe_dataset_spec()`, `dataset_contract()` | small | Simplifies API surface |

## Recommended Action Sequence

1. **Extract `_TABLE_TYPE_GRAIN_MAP` constant in `tags.py`** (P7) -- Zero-risk refactor that eliminates the most obvious duplication. Single file change.

2. **Extract `_join_with_code_units()` helper in `analysis_builders.py`** (P7) -- Eliminates the 40-line copy-paste between `cfg_blocks_df_builder` and `cfg_edges_df_builder`. Single file change.

3. **Add logging to silent exception handlers in `analysis_builders.py`** (P24) -- Requires importing the project logger and adding 4 log lines. Unblocks diagnosis of empty-output issues.

4. **Simplify or remove alias system in `dataset_specs.py`** (P19, P20) -- The alias system maps names to themselves. Either remove it or document why it exists for future use.

5. **Remove unused `ctx` parameter from spec access functions** (P20) -- Simplifies the public API. Check callers first (they all pass `ctx=None` or omit it).

6. **Consolidate `DatasetRegistrySpec` into `SemanticDatasetRow`** (P7, P3) -- Larger change that affects `normalize_registry.py`, `diagnostics_registry.py`, `export_registry.py`, `semantic_singletons_registry.py`, and `ir_pipeline.py`. Should be done after items 1-5 to reduce diff noise.

7. **Add cache reset hooks for testability** (P23) -- Add `_reset_cache()` to `dataset_rows.py` and `dataset_specs.py`. Write unit tests for tag and projection functions.

8. **Move `RELATION_OUTPUT_NAME` to `semantics/` shared constants** (P5) -- Eliminates the reverse dependency from `semantics.catalog` to `relspec`. Requires updating imports in both packages.

9. **Split `analysis_builders.py` by domain** (P3) -- Largest change. Split into `type_builders.py`, `cfg_builders.py`, `def_use_builders.py`, `diagnostic_builders.py`. Should be done last as it touches the most files.
