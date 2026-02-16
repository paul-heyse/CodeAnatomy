# Design Review: `src/datafusion_engine/session/runtime.py`

**Date:** 2026-02-16
**Scope:** `src/datafusion_engine/session/runtime.py` (single file, 8152 lines)
**Focus:** Decomposition opportunities — information hiding (P1), SRP (P3), high cohesion / low coupling (P4), separation of concerns (P2), composition over inheritance (P13), KISS (P19)
**Depth:** deep
**Files reviewed:** 1 (+ 50+ consumer files for import analysis)

## Executive Summary

`runtime.py` is a **8152-line mega-module** containing **35 classes** and **132 methods on a single God class** (`DataFusionRuntimeProfile`). It serves at least **9 distinct responsibilities**: config policy structs, telemetry schemas, telemetry payload construction, diagnostics hook chaining, session context lifecycle, dataset registration, UDF installation, schema validation, and dataset readiness probing. Nearly every module in `src/datafusion_engine/`, `src/extraction/`, and `src/storage/` imports from it. The file has become a gravity well — new functionality accretes here because "everything is already imported." Decomposition into 8–10 focused modules (each <1000 LOC) would dramatically improve information hiding, testability, and change locality.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 0 | large | high | 8152 LOC in one module; all internals are structurally public |
| 2 | Separation of concerns | 0 | large | high | Telemetry schemas, hook wiring, session lifecycle, dataset IO all in one file |
| 3 | SRP | 0 | large | high | 9+ reasons to change; God class with 132 methods |
| 4 | High cohesion, low coupling | 1 | large | high | Low internal cohesion; 50+ external consumers create extreme fan-out |
| 5 | Dependency direction | 2 | small | low | Core structs are leaf-level; profile class pulls in too many details |
| 6 | Ports & Adapters | 2 | medium | medium | Delta/tracing/cache are adapter concerns mixed into core profile |
| 13 | Composition over inheritance | 1 | medium | medium | 4 mixin classes used to break up God class; composition delegators (DeltaOps, IO, Catalog) exist but are thin wrappers |
| 19 | KISS | 1 | large | high | File complexity makes simple changes risky; cognitive load is extreme |
| 23 | Design for testability | 1 | medium | high | Testing requires constructing full `DataFusionRuntimeProfile`; no isolated units |

## Detailed Findings

### Category: Boundaries

#### P1. Information Hiding — Alignment: 0/3

**Current state:**
Every class, constant, free function, and PyArrow schema defined in runtime.py is structurally visible to any importer. The file defines 35 classes and ~100 module-level functions. Internal helpers like `_coerce_str` (line 307), `_encode_telemetry_msgpack` (line 373), `_map_entries` (line 1940), and `_rulepack_parameter_counts` (line 7340) sit alongside public API symbols like `DataFusionRuntimeProfile` (line 4544) and `align_table_to_schema` (line 7693).

**Findings:**
- **runtime.py:307-334**: 4 coercion helpers (`_coerce_str`, `_coerce_int`, `_coerce_bool`, `_coerce_str_sequence`) are compile-resolver invariant support; should be private to that concern.
- **runtime.py:391-549**: 15 PyArrow schema constants (`_MAP_ENTRY_SCHEMA`, `_TELEMETRY_SCHEMA`, `_SQL_POLICY_SCHEMA`, etc.) are telemetry-specific but visible to every importer.
- **runtime.py:7340-7548**: ~200 lines of `_rulepack_*` functions are entirely UDF/rulepack validation logic with no external callers.
- **runtime.py:7978-8078**: ~100 lines of `_dataset_readiness_payload` and related helpers are a self-contained dataset-readiness concern.
- **runtime.py:2204-2460**: ~250 lines of `_chain_*_hooks` and `diagnostics_*_hook` factory functions are hook-chaining concern.

**Suggested improvement:**
Extract each cohesion cluster into its own module. The telemetry schemas, hook factories, rulepack validators, and readiness helpers each form a natural information-hiding boundary. See "Recommended Module Split" below.

**Effort:** large
**Risk if unaddressed:** high — Any change to an internal detail risks breaking unrelated consumers; cognitive load makes bugs likely during maintenance.

---

#### P2. Separation of Concerns — Alignment: 0/3

**Current state:**
Policy/domain rules, IO/plumbing, telemetry serialization, and framework glue are thoroughly interleaved. For example, `DataFusionRuntimeProfile._install_tracing` (line 6844) handles tracing extension installation; `_build_telemetry_payload_row` (line 2630) handles telemetry schema construction; `_register_ast_dataset` (line 5754) handles dataset IO — all in the same class.

**Findings:**
- **Concern 1: Config policy structs** (lines 1051-1338): `DataFusionConfigPolicy`, `DataFusionFeatureGates`, `DataFusionJoinPolicy`, `DataFusionSettingsContract`, `SchemaHardeningProfile` — pure value objects with `fingerprint_payload()`.
- **Concern 2: Profile config structs** (lines 3729-4172): `ExecutionConfig`, `CatalogConfig`, `DataSourceConfig`, `ZeroRowBootstrapConfig`, `FeatureGatesConfig`, `DiagnosticsConfig`, `PolicyBundleConfig` — profile sub-configuration structs.
- **Concern 3: Telemetry** (lines 391-549, 2595-2990): PyArrow schemas + `_build_telemetry_payload_row` + `_telemetry_common_payload` + `_RuntimeDiagnosticsMixin.telemetry_payload*`.
- **Concern 4: Diagnostics hooks** (lines 2204-2465): Hook type aliases + `_chain_*_hooks` + `diagnostics_*_hook` factories.
- **Concern 5: Session context lifecycle** (lines 4544-4826, 6685-6700): `DataFusionRuntimeProfile._build_session_context`, `session_context`, `build_ephemeral_context`, `context_pool`.
- **Concern 6: UDF installation + validation** (lines 5217-5570): `_install_udf_platform`, `_refresh_udf_catalog`, `_validate_udf_specs`, `_validate_rule_function_allowlist`, rulepack functions.
- **Concern 7: Dataset registration** (lines 5739-5890): `_register_ast_dataset`, `_register_bytecode_dataset`, `_register_scip_datasets`, readiness probing.
- **Concern 8: Schema registry + validation** (lines 5571-6420, 1881-1910): `_install_schema_registry`, `_record_schema_registry_validation`, `SchemaRegistryValidationResult`.
- **Concern 9: Schema alignment / IO utilities** (lines 7609-7910): `align_table_to_schema`, `read_delta_as_reader`, `dataset_schema_from_context`, `assert_schema_metadata`.

**Suggested improvement:**
Each numbered concern above maps to a candidate module. The boundaries are naturally low-coupling since they communicate through well-typed profile attributes.

**Effort:** large
**Risk if unaddressed:** high — Changes to telemetry format should not risk breaking UDF installation logic, but currently they live in the same unit.

---

#### P3. SRP — Alignment: 0/3

**Current state:**
`DataFusionRuntimeProfile` changes for at least 9 distinct reasons: config policy changes, telemetry format changes, new hook types, new dataset sources, UDF catalog changes, schema registry evolution, context pool changes, delta integration changes, and cache infrastructure changes.

**Findings:**
- **runtime.py:4544**: `DataFusionRuntimeProfile` has **132 methods**. A class with >15 methods is a strong SRP smell; 132 is extreme.
- **runtime.py:2991-3368**: `_RuntimeDiagnosticsMixin` (377 lines) mixes settings payload, fingerprinting, and telemetry — at least 3 responsibilities in a "diagnostic" mixin.
- **runtime.py:5924-6137**: Methods `_record_cst_schema_diagnostics`, `_record_tree_sitter_stats`, `_record_tree_sitter_view_schemas`, `_record_tree_sitter_cross_checks`, `_record_cst_view_plans`, `_record_cst_dfschema_snapshots`, `_record_bytecode_metadata` — these are **source-specific schema recording**, each tightly coupled to a specific extraction source (CST, tree-sitter, bytecode).

**Suggested improvement:**
Extract method groups into focused helper modules that accept `DataFusionRuntimeProfile` as a parameter rather than being methods. The profile struct should be a data carrier; behavioral logic should live in domain-specific modules.

**Effort:** large
**Risk if unaddressed:** high — 132-method class is the primary contributor to the file's size and cognitive load.

---

#### P4. High Cohesion, Low Coupling — Alignment: 1/3

**Current state:**
The file has low internal cohesion: functions at lines 200-370 (compile resolver invariants) have no relationship to functions at lines 7693-7780 (schema alignment). They are co-located only by historical accident. External coupling is extreme: 50+ files import from `runtime.py`.

**Findings:**
- **Fan-out analysis**: `DataFusionRuntimeProfile` is imported by files in `src/extraction/`, `src/extract/`, `src/storage/`, `src/cli/`, `src/schema_spec/`, `src/datafusion_engine/session/`, and `src/datafusion_engine/sql/`. This makes it a coupling bottleneck.
- **Internal clustering**: Methods on `DataFusionRuntimeProfile` naturally cluster into groups that never call each other — e.g., `_record_tree_sitter_*` methods never interact with `_install_function_factory` or `_compile_options`.

**Suggested improvement:**
Move co-dependent function clusters into their own modules. Re-export public symbols from `runtime.py` for backwards compatibility during migration.

**Effort:** large
**Risk if unaddressed:** high — The coupling gravity well will continue to grow; each new feature adds more methods to the God class.

---

### Category: Composition

#### P13. Composition over Inheritance — Alignment: 1/3

**Current state:**
`DataFusionRuntimeProfile` uses 4 mixins (`_RuntimeProfileIOFacadeMixin`, `_RuntimeProfileCatalogFacadeMixin`, `_RuntimeProfileDeltaFacadeMixin`, `_RuntimeDiagnosticsMixin`) and 3 composition delegators (`RuntimeProfileDeltaOps`, `RuntimeProfileIO`, `RuntimeProfileCatalog`). The delegators are a good pattern, but the mixins use `cast("DataFusionRuntimeProfile", self)` (seen at lines 2994, 2999, 3010, 3023, 3051, 3069, 3100, 3265, 3366) to access the host class — a code smell indicating the mixin is tightly coupled to a specific concrete type.

**Findings:**
- **runtime.py:2991-3368**: `_RuntimeDiagnosticsMixin` uses `cast("DataFusionRuntimeProfile", self)` 9 times. This is not real composition or real inheritance — it's a fragile coupling mechanism.
- **runtime.py:4487-4540**: Three facade mixins are thin wrappers that delegate to the composition objects. They exist only to maintain backwards compatibility on method signatures.

**Suggested improvement:**
Replace mixins with free functions that accept `DataFusionRuntimeProfile` as an explicit parameter. The `telemetry_payload()`, `settings_payload()`, `fingerprint_payload()` methods become standalone functions in a telemetry module.

**Effort:** medium
**Risk if unaddressed:** medium — Mixin casts will break silently if the host class changes shape.

---

### Category: Simplicity

#### P19. KISS — Alignment: 1/3

**Current state:**
The 8152-line file defeats simple comprehension. A developer needing to understand "how does session context get built?" must navigate through 200+ function/method definitions to find the relevant 50 lines. The massive import block (lines 1-196) pulls in 40+ modules.

**Findings:**
- **runtime.py:1-196**: 196-line import block importing from 40+ modules. This alone indicates the file has too many concerns.
- **runtime.py:8099-8152**: Deferred import of 40+ artifact spec constants at the end of the file, with a comment explaining circular import avoidance. This is a structural symptom of the file doing too much.
- **runtime.py:1535-1685**: ~150 lines of policy preset constants (`DEFAULT_DF_POLICY`, `CST_AUTOLOAD_DF_POLICY`, `SYMTABLE_DF_POLICY`, etc.) and prepared statement specs. These are pure data declarations that should live in a config/presets module.

**Suggested improvement:**
Decomposition itself is the KISS fix. Each extracted module will have a focused import set and a small, comprehensible surface.

**Effort:** large
**Risk if unaddressed:** high — Developer productivity suffers; onboarding cost is extreme; bugs from wrong-section edits are likely.

---

### Category: Quality

#### P23. Design for Testability — Alignment: 1/3

**Current state:**
Testing any behavior of `DataFusionRuntimeProfile` requires constructing a full profile instance with its 10+ sub-config objects. The 132-method God class cannot be tested in isolation because its methods span 9 concerns.

**Findings:**
- The rulepack validation functions (lines 7340-7548) are pure logic with no profile dependency — they accept `rows` and return errors. These are already testable in isolation but are trapped in the mega-module.
- Hook factories (lines 2237-2465) are pure factory functions that could be tested independently.
- Schema alignment (lines 7693-7780) is a pure data transformation.

**Suggested improvement:**
Extraction naturally improves testability. Each extracted module can be tested with minimal setup, without constructing a full `DataFusionRuntimeProfile`.

**Effort:** medium (as a side-effect of decomposition)
**Risk if unaddressed:** high — Testing coverage will remain thin because test setup is expensive.

---

## Cross-Cutting Themes

### Theme 1: God Class Anti-Pattern

`DataFusionRuntimeProfile` (132 methods) is the primary structural problem. It has accumulated methods from 9+ concerns because it was convenient to add "one more method" to the class that already had access to all config. The fix is to move behavioral methods to standalone functions in domain-specific modules, leaving the profile as a pure data carrier with minimal lifecycle methods.

### Theme 2: Gravity Well Effect

Because `runtime.py` already imports 40+ modules, adding new functionality here avoids creating new import chains. This creates a positive feedback loop where the file grows monotonically. Breaking the file will disrupt this cycle and establish natural boundaries.

### Theme 3: Mixin Anti-Pattern

The 4 mixins use `cast()` to access the host class, which is a sign they should be extracted as free functions. Mixins make sense when multiple classes share behavior; here only one class uses each mixin.

---

## Recommended Module Split

Based on cohesion analysis, the 8152-line file should be split into **9 modules** (plus a thin `runtime.py` re-export layer for backwards compatibility):

### Module 1: `runtime_config_policies.py` (~300 lines)
**Contents:** `DataFusionConfigPolicy`, `DataFusionFeatureGates`, `DataFusionJoinPolicy`, `DataFusionSettingsContract`, `SchemaHardeningProfile`, policy preset constants (`DEFAULT_DF_POLICY`, `SYMTABLE_DF_POLICY`, etc.), `SCHEMA_HARDENING_PRESETS`, `CACHE_PROFILES`.
**Lines:** 1051-1338, 1535-1637
**Reason:** Pure config value objects with `fingerprint_payload()`. Single reason to change: config policy evolution.

### Module 2: `runtime_profile_config.py` (~500 lines)
**Contents:** `ExecutionConfig`, `CatalogConfig`, `ExtractOutputConfig`, `SemanticOutputConfig`, `DataSourceConfig`, `ZeroRowBootstrapConfig`, `FeatureGatesConfig`, `DiagnosticsConfig`, `PolicyBundleConfig`, `PreparedStatementSpec`, `AdapterExecutionPolicy`, `ExecutionLabel`, related constants (`CST_DIAGNOSTIC_STATEMENTS`, `INFO_SCHEMA_STATEMENTS`).
**Lines:** 1494-1534, 3729-4172
**Reason:** Profile sub-configuration structs. Single reason to change: profile shape evolution.

### Module 3: `runtime_telemetry.py` (~600 lines)
**Contents:** PyArrow schema constants (`_TELEMETRY_SCHEMA`, `_MAP_ENTRY_SCHEMA`, etc.), `_build_telemetry_payload_row`, `_telemetry_common_payload`, `_runtime_settings_payload`, `_identifier_normalization_mode`, `_extra_settings_payload`, `_cache_profile_settings`, `_encode_telemetry_msgpack`, `_map_entries`, `_map_entry`, `_value_kind`, `_value_text`, `_stable_repr`, `_settings_by_prefix`, `_enrich_query_telemetry`, `_telemetry_enrichment_policy_for_profile`, telemetry mixin logic (`settings_payload`, `settings_hash`, `fingerprint_payload`, `fingerprint`, `telemetry_payload`, `telemetry_payload_v1`, `telemetry_payload_msgpack`, `telemetry_payload_hash`).
**Lines:** 384-549, 1936-1997, 2595-2990, 3015-3368
**Reason:** Telemetry format is a single, self-contained concern. Changes here (new fields, schema evolution) should not touch session lifecycle.

### Module 4: `runtime_hooks.py` (~300 lines)
**Contents:** Hook type aliases (`ExplainHook`, `PlanArtifactsHook`, `SemanticDiffHook`, etc.), `_chain_*_hooks` functions, `diagnostics_*_hook` factories, `labeled_explain_hook`, `_DataFusionExplainCollector`, `_DataFusionPlanCollector`, `_attach_cache_manager`, `apply_execution_label`, `apply_execution_policy`.
**Lines:** 188-196, 1340-1380, 2192-2465, 7550-7606
**Reason:** Hook chaining and diagnostics hook factories are a pure composition concern.

### Module 5: `runtime_session.py` (~500 lines)
**Contents:** `SessionRuntime`, `_RuntimeDiagnosticsMixin` (the `record_artifact`/`record_events`/`view_registry_snapshot` core only), session build/refresh functions (`build_session_runtime`, `refresh_session_runtime`, `session_runtime_hash`, `session_runtime_for_context`, `_build_session_runtime_from_context`, `_settings_rows_to_mapping`, `_merge_runtime_settings_rows`, `record_runtime_setting_override`, `runtime_setting_overrides`), session config helpers (`_read_only_sql_options`, `_sql_with_options`, `settings_snapshot_for_profile`, `catalog_snapshot_for_profile`, `function_catalog_snapshot_for_profile`, `record_view_definition`), `DataFusionViewRegistry`.
**Lines:** 1383-1462, 1999-2115, 3371-3680
**Reason:** Session runtime construction and introspection is a core lifecycle concern.

### Module 6: `runtime_udf.py` (~400 lines)
**Contents:** UDF installation methods (extracted from profile): `_install_udf_platform`, `_refresh_udf_catalog`, `_validate_udf_specs`, `_validate_rule_function_allowlist`, `_record_udf_snapshot`, `_record_udf_docs`, `udf_catalog`, `function_factory_policy_hash`, `_install_function_factory`, `_install_expr_planners`, `_install_physical_expr_adapter_factory`, `_record_expr_planners`, `_record_function_factory`, `_install_planner_rules`, `_PlannerRuleInstallers`, `_resolve_planner_rule_installers`.
Rulepack functions: `_rulepack_parameter_counts`, `_rulepack_parameter_signatures`, `_rulepack_signature_errors`, `_rulepack_function_errors`, `_rulepack_signature_validation`, etc.
**Lines:** 3683-3727, 5217-5570, 6707-6815, 7340-7548
**Reason:** UDF lifecycle (installation, validation, rulepack checking) is a self-contained concern.

### Module 7: `runtime_schema_ops.py` (~500 lines)
**Contents:** Schema registry/validation methods (extracted from profile): `_install_schema_registry`, `_register_schema_tables`, `_register_schema_registry_tables`, `_prepare_schema_registry_context`, `_record_schema_registry_validation`, `_record_schema_diagnostics`, `_validate_schema_views`, `_schema_registry_issues`, `SchemaRegistryValidationResult`, `_register_schema_table`, `_load_schema_evolution_adapter_factory`, `_install_schema_evolution_adapter_factory`, dataset registration (`_register_ast_dataset`, `_register_bytecode_dataset`, `_register_scip_datasets`), schema recording (`_record_cst_schema_diagnostics`, `_record_tree_sitter_stats`, `_record_tree_sitter_view_schemas`, etc.), `_record_schema_snapshots`.
**Lines:** 1689-1935, 5571-6420, 5739-5930, 5959-6201
**Reason:** Schema operations form a tight cluster used during context setup.

### Module 8: `runtime_dataset_io.py` (~400 lines)
**Contents:** `align_table_to_schema`, `_align_projection_exprs`, `_align_table_with_arrow`, `_apply_table_schema_metadata`, `_schema_has_extension`, `_type_has_extension`, `assert_schema_metadata`, `dataset_schema_from_context`, `dataset_spec_from_context`, `read_delta_as_reader`, `_schema_with_table_metadata`, `_datafusion_type_name`, dataset location functions (`normalize_dataset_locations_for_profile`, `extract_output_locations_for_profile`, `semantic_output_locations_for_profile`, `datasource_config_from_manifest`, `datasource_config_from_profile`), readiness probing (`record_dataset_readiness`, `_dataset_readiness_payload`, etc.).
**Lines:** 790-1000, 7609-7910, 7935-8078
**Reason:** Dataset I/O, schema alignment, and readiness probing form a cohesive data-access concern.

### Module 9: `runtime_compile.py` (~250 lines)
**Contents:** Compile-resolver invariant logic (`_CompileResolverInvariantInputs`, `compile_resolver_invariants_strict_mode`, `compile_resolver_invariant_artifact_payload`, `record_compile_resolver_invariants`, `_parse_compile_resolver_invariant_inputs`, `_coerce_*` helpers), compile hooks resolution (`_ResolvedCompileHooks`, `_CompileOptionResolution`, `_resolve_compile_hooks`, `_compile_options`, `_resolve_sql_policy`, `_resolved_sql_policy`, SQL options methods).
**Lines:** 199-377, 2490-2570, 6896-7132
**Reason:** Compile resolution is a focused concern spanning invariant enforcement and option resolution.

### Residual `runtime.py` (~800 lines)
**Contents:** `DataFusionRuntimeProfile` class (slimmed to lifecycle + delegation), `RuntimeProfileDeltaOps`, `RuntimeProfileIO`, `RuntimeProfileCatalog`, facade mixins, context build/cache, `__post_init__`, property accessors. Plus re-exports of all public symbols for backwards compatibility.
**Reason:** The profile class becomes a thin orchestrator that delegates to extracted modules.

---

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P1, P3 | Extract `runtime_config_policies.py` — pure value objects, zero behavioral coupling | small | Removes ~300 LOC; establishes pattern for further splits |
| 2 | P1, P2 | Extract `runtime_hooks.py` — self-contained hook factories with no profile dependency | small | Removes ~300 LOC; improves testability of hook logic |
| 3 | P1, P3 | Extract `runtime_profile_config.py` — profile sub-config structs | small | Removes ~500 LOC; separates data shape from behavior |
| 4 | P1, P23 | Extract `runtime_dataset_io.py` — dataset I/O + readiness + alignment are pure functions | medium | Removes ~400 LOC; enables isolated testing |
| 5 | P1, P3 | Extract `runtime_telemetry.py` — schemas + payload builders | medium | Removes ~600 LOC; decouples telemetry format evolution |

## Recommended Action Sequence

1. **Phase 1 — Pure data extractions (no behavioral changes):**
   1. Extract `runtime_config_policies.py` (P1, P3): Move config policy structs and presets.
   2. Extract `runtime_profile_config.py` (P1, P3): Move profile sub-config structs.
   3. Extract `runtime_hooks.py` (P1, P2): Move hook types and factories.
   4. Add re-exports from `runtime.py` for backwards compatibility.

2. **Phase 2 — Pure function extractions:**
   5. Extract `runtime_telemetry.py` (P1, P2): Move telemetry schemas and payload builders.
   6. Extract `runtime_dataset_io.py` (P1, P23): Move schema alignment and readiness.
   7. Extract `runtime_compile.py` (P1): Move compile invariant + options logic.
   8. Add re-exports from `runtime.py`.

3. **Phase 3 — Method extractions (profile slimming):**
   9. Extract `runtime_udf.py` (P3): Convert profile UDF methods to functions accepting profile.
   10. Extract `runtime_schema_ops.py` (P3): Convert profile schema methods to functions.
   11. Extract `runtime_session.py` (P3): Move session lifecycle functions.
   12. Slim `DataFusionRuntimeProfile` to lifecycle-only (~30 methods).

4. **Phase 4 — Cleanup:**
   13. Replace mixin `cast()` patterns with explicit function calls.
   14. Remove facade mixins if callers have migrated.
   15. Audit and trim re-exports from `runtime.py` once consumers are updated.
