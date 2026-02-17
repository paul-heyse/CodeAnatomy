# Design Review: src/schema_spec

**Date:** 2026-02-17
**Scope:** `src/schema_spec/` (schema specifications) + `src/core/` (config base, fingerprinting) + `src/core_types.py` + `src/serde_msgspec.py` + `src/serde_msgspec_ext.py` + `src/serde_msgspec_inspect.py`
**Focus:** All principles (1-24), with emphasis on boundaries (1-6), simplicity (19-22), quality (23-24)
**Depth:** moderate
**Files reviewed:** 20

## Executive Summary

The `src/schema_spec/` module is a large inner-ring package (~12 files) that defines the declarative schema specifications for all datasets, tables, fields, and evidence metadata. It is heavily imported across the codebase and provides the structural backbone for schema-driven operations. However, it suffers from a critical dependency direction violation: as an inner-ring module, it imports extensively from `datafusion_engine` (engine ring) and `storage` (middle ring). This makes `schema_spec` not truly an inner-ring module but rather an engine-dependent specification layer. The `serde_msgspec.py` and related files provide clean, well-factored serialization infrastructure with proper extension points. The `core/` module is minimal and well-positioned as true inner ring.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 2 | small | low | Large facade in `__init__.py` (~60+ re-exports); some internals leak |
| 2 | Separation of concerns | 1 | large | high | `dataset_spec.py` mixes spec definition with scan policy and DataFusion types |
| 3 | SRP | 1 | large | high | `dataset_spec.py` (~712 LOC) handles specs, contracts, scan policies, runtime config |
| 4 | High cohesion, low coupling | 1 | large | high | 40+ imports from `datafusion_engine` across schema_spec files |
| 5 | Dependency direction | 0 | large | high | Inner-ring module imports from engine ring in 10+ files |
| 6 | Ports & Adapters | 1 | medium | high | No port abstractions for Arrow/DataFusion types |
| 7 | DRY | 2 | small | low | Field specs well-centralized; some schema constants duplicated |
| 8 | Design by contract | 3 | - | - | `TableSchemaContract` defines explicit postconditions |
| 9 | Parse, don't validate | 2 | small | low | Specs parsed at boundary; runtime validated via contract |
| 10 | Make illegal states unrepresentable | 2 | small | low | Frozen structs; sort key specs; schema version typing |
| 11 | CQS | 3 | - | - | Specs are pure data; no mutation |
| 12 | DI + explicit composition | 2 | small | low | `make_table_spec()` composes via bundles; no implicit creation |
| 13 | Composition over inheritance | 3 | - | - | `FieldBundle` composition; no inheritance chains |
| 14 | Law of Demeter | 2 | small | low | `specs.py` accesses Arrow field metadata chains |
| 15 | Tell, don't ask | 2 | small | low | Schema contracts tell validation results; some raw field exposure |
| 16 | Functional core, imperative shell | 2 | medium | medium | Spec construction is pure; but `load_schema_runtime` has side effects |
| 17 | Idempotency | 3 | - | - | Schema specs are immutable; same inputs produce same spec |
| 18 | Determinism | 3 | - | - | Schema derivation is deterministic |
| 19 | KISS | 1 | medium | medium | `dataset_spec.py` has excessive complexity for a spec module |
| 20 | YAGNI | 2 | small | low | Some scan policy config fields may be speculative |
| 21 | Least astonishment | 2 | small | low | `schema_spec/__init__.py` re-exports 60+ symbols -- hard to discover |
| 22 | Public contracts | 2 | small | low | `__all__` defined; no version markers on contracts |
| 23 | Testability | 2 | small | low | Specs testable as data; contract validation testable in isolation |
| 24 | Observability | 3 | - | - | N/A for spec module; schema identity hashes provide traceability |

## Detailed Findings

### Category: Boundaries

#### P5. Dependency direction -- Alignment: 0/3

**Current state:**
`schema_spec/` is classified as inner ring but imports from `datafusion_engine` (engine ring) in 10+ files and from `storage` (middle ring) in 2 files.

**Findings:**
- `src/schema_spec/table_spec.py:7` -- imports `SchemaLike` from `datafusion_engine.arrow.interop`
- `src/schema_spec/dataset_spec.py:26-29` -- imports `TableLike`, `load_schema_runtime`, `DedupeSpec`, `SortKey`, `ArrowValidationOptions` from `datafusion_engine`
- `src/schema_spec/dataset_spec.py:581` -- imports `DeltaSchemaRequest` from `storage.deltalake` (confirmed known issue)
- `src/schema_spec/specs.py:20-31` -- imports 10 symbols from `datafusion_engine.arrow.*`, `datafusion_engine.expr.*`, `datafusion_engine.schema.*`
- `src/schema_spec/field_spec.py:10-12` -- imports from `datafusion_engine.arrow.interop`, `datafusion_engine.arrow.metadata`
- `src/schema_spec/evidence_metadata.py:15,21` -- imports from `datafusion_engine.arrow.interop`
- `src/schema_spec/validation.py:7-8` -- imports from `datafusion_engine.arrow.interop`, `datafusion_engine.schema.validation`
- `src/schema_spec/dataset_runtime.py:13-17,26` -- imports 6 symbols from `datafusion_engine` sub-modules
- `src/schema_spec/dataset_spec_runtime.py:15-28,47,53` -- imports 14 symbols from `datafusion_engine` and 3 from `storage`
- `src/schema_spec/discovery.py:11-12` -- imports from `datafusion_engine.arrow.interop`, `datafusion_engine.delta.service_protocol`
- `src/schema_spec/arrow_type_registry.py:11` -- imports from `datafusion_engine.arrow.semantic`
- `src/schema_spec/metadata_spec.py:9` -- imports from `datafusion_engine.arrow.metadata`
- `src/schema_spec/span_fields.py:25` -- imports from `datafusion_engine.arrow.interop`

Total: 40+ import statements from `datafusion_engine` across the package, plus 5 from `storage`.

**Suggested improvement:**
The `schema_spec` package should be reclassified. It is NOT an inner-ring module -- it is an engine-dependent specification layer. Two approaches:

Option A: **Split** `schema_spec` into `schema_spec_core/` (truly inner-ring, pure spec types with no engine deps) and `schema_spec/` (engine-ring, Arrow/DataFusion-dependent spec implementations). Core types like `FieldBundle`, `TableSchemaSpec` shape definitions, and sort key specs go into core. Arrow schema construction, validation, and DataFusion integration stay in the current location.

Option B: **Reclassify** `schema_spec` as engine ring and update the dependency map accordingly.

**Effort:** large
**Risk if unaddressed:** high -- The false classification as inner ring creates confusion about what modules can safely import from `schema_spec`, and any attempt to use `schema_spec` without `datafusion_engine` installed will fail.

---

#### P2. Separation of concerns -- Alignment: 1/3

**Current state:**
`dataset_spec.py` conflates multiple concerns: spec type definitions, scan policy configuration, contract validation, and runtime factory methods.

**Findings:**
- `src/schema_spec/dataset_spec.py:1-100` defines `ScanPolicyDefaults`, `DeltaScanPolicyDefaults`, `ScanPolicyConfig` -- runtime policy configuration
- `src/schema_spec/dataset_spec.py:100-300` defines `DataFusionScanOptions` (~25 fields) -- DataFusion-specific runtime options
- `src/schema_spec/dataset_spec.py:300-500` defines `TableSchemaContract`, `ContractRow`, `SortKeySpec` -- contract types
- `src/schema_spec/dataset_spec.py:500-712` defines `apply_scan_policy()`, factory functions, and `DeltaSchemaRequest` import at line 581
- This single module handles 4 distinct concerns: scan policies, runtime options, contracts, and factory methods

**Suggested improvement:**
Split into: `scan_policy.py` (scan policy types and defaults), `scan_options.py` (DataFusion scan options), `contracts.py` (table schema contracts), and keep only the core spec type definitions in `dataset_spec.py`.

**Effort:** large
**Risk if unaddressed:** high -- Changes to scan policies risk breaking contract types; the file is difficult to navigate at 712 LOC.

---

#### P3. SRP -- Alignment: 1/3

**Current state:**
`dataset_spec.py` changes for at least 4 reasons: scan policy changes, scan option changes, contract changes, and factory method changes.

**Findings:**
- Same analysis as P2 above. The module has multiple axes of change.
- `src/schema_spec/specs.py:20-31` also imports heavily from DataFusion but is more focused on schema construction logic

**Suggested improvement:**
See P2 suggestion for splitting.

**Effort:** large
**Risk if unaddressed:** high

---

### Category: Knowledge

#### P7. DRY -- Alignment: 2/3

**Current state:**
Field specs are well-centralized in `field_spec.py`. Schema constants are in `arrow_utils/core/schema_constants.py`. Some metadata key duplication exists between `arrow_utils` and `datafusion_engine.arrow.metadata`.

**Findings:**
- `src/schema_spec/field_spec.py` provides `FieldSpec` as the single authority for field definitions
- `src/schema_spec/specs.py:165` defers to `datafusion_engine.arrow.metadata` for encoding policy resolution -- single authority
- `src/arrow_utils/core/schema_constants.py:1-41` defines metadata key constants (`DEFAULT_VALUE_META`, `KEY_FIELDS_META`, etc.) that may overlap with `datafusion_engine.arrow.metadata.ENCODING_META`

**Suggested improvement:**
Verify that schema metadata key constants are not duplicated between `arrow_utils/core/schema_constants.py` and `datafusion_engine/arrow/metadata.py`. If they are, consolidate to a single authority.

**Effort:** small
**Risk if unaddressed:** low

---

#### P8. Design by contract -- Alignment: 3/3

**Current state:**
`TableSchemaContract` provides explicit postcondition validation for schema outputs.

**Findings:**
- `src/schema_spec/dataset_spec.py` `TableSchemaContract` defines `validate()` method that checks output tables against schema specs
- `ContractRow` captures per-field validation results with violation types
- `src/schema_spec/table_spec.py:81-88` `delta_constraints_from_table_spec()` derives Delta constraints from schema constraints -- contract propagation

**Suggested improvement:**
No action needed.

**Effort:** -
**Risk if unaddressed:** -

---

### Core Types and Serde Infrastructure

#### P10. Make illegal states unrepresentable -- Alignment: 2/3

**Current state:**
`core_types.py` defines annotated types that constrain values. `serde_msgspec.py` defines frozen struct bases that prevent mutation.

**Findings:**
- `src/core_types.py:1-80` defines `IdentifierStr`, `RunIdStr`, `HashStr`, etc. as `Annotated[str, ...]` types with `msgspec.Meta` constraints -- good static contract
- `src/core_types.py:85-100` defines `DeterminismTier` StrEnum with `DETERMINISTIC`, `STABLE`, `VOLATILE` -- prevents invalid tier values
- `src/serde_msgspec.py:27-60` defines `StructBaseStrict`, `StructBaseCompat`, `StructBaseHotPath` with appropriate `frozen=True`, `forbid_unknown_fields` settings
- However, `src/core_types.py` imports from both `msgspec.Meta` and `pydantic.Field` -- mixing two validation frameworks in a core type module

**Suggested improvement:**
Consider removing the `pydantic.Field` import from `core_types.py` if the Pydantic field annotations can be replaced with `msgspec.Meta` equivalents. A core type module should ideally depend on a single validation framework.

**Effort:** small
**Risk if unaddressed:** low

---

#### P19. KISS -- Alignment: 2/3 (serde_msgspec)

**Current state:**
`serde_msgspec.py` is clean and well-organized (~654 LOC) with clear encoder/decoder instances and utility functions.

**Findings:**
- `src/serde_msgspec.py` provides a complete serialization toolkit: JSON and MessagePack encode/decode, custom ext hooks, schema export
- `src/serde_msgspec_ext.py:1-52` cleanly defines extension codes and wrapper types as frozen dataclasses
- `src/serde_msgspec_inspect.py:1-249` handles the complex case of converting msgspec inspection objects to builtins, with proper cycle detection

**Suggested improvement:**
No action needed. The complexity is justified by the domain requirements.

**Effort:** -
**Risk if unaddressed:** -

---

#### P18. Determinism -- Alignment: 3/3

**Current state:**
Serialization uses `order="deterministic"` by default. Fingerprinting uses sorted components.

**Findings:**
- `src/serde_msgspec.py:65` `_DEFAULT_ORDER = "deterministic"` -- all encoders use deterministic ordering
- `src/core/fingerprinting.py:26-44` `CompositeFingerprint.from_components()` sorts components by name for deterministic output
- `src/core/config_base.py:28-40` `config_fingerprint()` uses `hash_json_canonical()` for deterministic hashing

**Suggested improvement:**
No action needed. Determinism is well-maintained.

**Effort:** -
**Risk if unaddressed:** -

---

### Arrow Utils

#### P19. KISS -- Alignment: 3/3

**Current state:**
`arrow_utils/` is minimal and focused.

**Findings:**
- `src/arrow_utils/__init__.py` is empty (`__all__: list[str] = []`)
- `src/arrow_utils/core/` contains 5 focused files: `array_iter.py`, `expr_types.py`, `ordering.py`, `schema_constants.py`, `streaming.py`
- `src/arrow_utils/core/ordering.py:1-82` defines clean `Ordering` dataclass with factory methods -- no unnecessary complexity
- `src/arrow_utils/core/schema_constants.py:1-41` defines schema metadata key constants -- pure constants, no logic

**Suggested improvement:**
No action needed.

**Effort:** -
**Risk if unaddressed:** -

---

### Validation Module

#### P10. Make illegal states unrepresentable -- Alignment: 3/3

**Current state:**
`validation/violations.py` defines a clean violation type system.

**Findings:**
- `src/validation/violations.py` defines `ViolationType` enum (10 values) and `ValidationViolation` frozen dataclass
- Clean, no external dependencies
- `src/validation/__init__.py` provides a clean re-export facade

**Suggested improvement:**
No action needed.

**Effort:** -
**Risk if unaddressed:** -

---

## Cross-Cutting Themes

### Theme 1: `schema_spec` Is Not Inner Ring

**Root cause:** `schema_spec` was conceptually designed as a specification layer that should be importable without runtime dependencies. In practice, it evolved to depend heavily on `datafusion_engine` Arrow types for schema construction, validation, and metadata. This makes it an engine-ring module masquerading as inner ring.

**Affected principles:** P2, P3, P4, P5, P6

**Approach:** Either split into `schema_spec_core/` (truly inner, pure types) and `schema_spec/` (engine-dependent implementations), or reclassify `schema_spec` as engine ring and update the dependency map.

### Theme 2: `dataset_spec.py` Overloaded Module

**Root cause:** Organic growth. Scan policies, scan options, schema contracts, and factory methods accumulated in a single module as the dataset specification became more complex.

**Affected principles:** P2, P3, P19

**Approach:** Split into focused sub-modules: `scan_policy.py`, `scan_options.py`, `contracts.py`, keeping only core spec types in `dataset_spec.py`.

### Theme 3: Clean Core Infrastructure

**Root cause:** `core/`, `core_types.py`, `serde_msgspec*.py`, `arrow_utils/`, and `validation/` are well-designed true inner-ring modules with minimal dependencies.

**Affected principles:** P5, P19, P18 -- positively

**Approach:** Maintain these modules as the gold standard for inner-ring design. Use them as the reference for what `schema_spec` and `obs` should aspire to.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P5 Dependency | Reclassify `schema_spec` in dependency map as engine-ring | small | Corrects documentation; prevents false assumptions |
| 2 | P3 SRP | Split `dataset_spec.py` into scan_policy, scan_options, contracts | medium | Clearer module boundaries |
| 3 | P10 States | Remove `pydantic.Field` import from `core_types.py` | small | Cleaner inner-ring dependency |
| 4 | P7 DRY | Verify no duplication between `arrow_utils/core/schema_constants.py` and `datafusion_engine/arrow/metadata.py` | small | Eliminates potential constant drift |
| 5 | P21 Astonishment | Add docstring to `schema_spec/__init__.py` explaining the 60+ re-exports | small | Easier discovery for new contributors |

## Recommended Action Sequence

1. **Reclassify `schema_spec` ring membership** (P5) -- Update dependency direction map to classify `schema_spec` as engine-ring. This is a documentation change that accurately reflects reality.

2. **Split `dataset_spec.py`** (P2, P3) -- Extract `scan_policy.py`, `scan_options.py`, and `contracts.py` from the 712-LOC monolith. Update `__init__.py` re-exports accordingly.

3. **Clean `core_types.py` imports** (P10) -- Remove `pydantic.Field` import if possible, keeping the module purely `msgspec`-based.

4. **Audit metadata key constants** (P7) -- Compare `arrow_utils/core/schema_constants.py` with `datafusion_engine/arrow/metadata.py` and consolidate any duplicates.

5. **Long-term: Split schema_spec into core + engine** (P5, P6) -- Extract pure spec types (no Arrow/DataFusion deps) into `schema_spec_core/`. This would create a truly importable inner-ring schema specification layer.
