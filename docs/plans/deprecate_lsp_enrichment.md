# Plan: Deprecate and Delete LSP Enrichment in tools/cq

## Objective

Fully remove all pyrefly LSP and rust-analyzer LSP functionality from `tools/cq/`,
replacing it with structural analysis from ast-grep-py, Python ast/symtable, tree-sitter,
and LibCST. The existing non-LSP enrichment pipeline (5-stage: ast_grep, python_ast,
import_detail, libcst, tree_sitter) becomes the sole enrichment path.

## Motivation

LSP servers (pyrefly, rust-analyzer) introduce operational complexity:
subprocess lifecycle management, timeouts, flaky startup, position encoding conversion,
and capability-gating logic. The existing structural enrichment pipeline already provides
~80% of the same signals without these failure modes.

## Scope

- **~24 files deleted** (pure LSP code, ~6,000+ LOC)
- **~22 files modified** (LSP code removed, non-LSP logic preserved, ~2,000-3,000 LOC removed)
- **~5 files enhanced** (structural enrichment promoted to fill gaps)
- **Net LOC reduction: ~8,000-10,000 lines**

## Reference Documentation

The following library documentation supports replacement strategies:
- `docs/python_library_reference/ast-grep-py.md`
- `docs/python_library_reference/ast-grep-py_rust_deepdive.md`
- `docs/python_library_reference/python_ast_libraries_and_cpg_construction.md`
- `docs/python_library_reference/tree-sitter.md`
- `docs/python_library_reference/tree-sitter_advanced.md`
- `docs/python_library_reference/tree-sitter_outputs_overview.md`
- `docs/python_library_reference/libcst.md`
- `docs/python_library_reference/libcst-advanced.md`
- `docs/python_library_reference/tree-sitter-rust.md`

---

## Phase 1: Delete Pure LSP Files

**Goal:** Remove all files that contain only LSP functionality. No non-LSP code exists
in these files, so deletion is clean.

**Delete order** (leaf imports first to minimize cascade):

### 1a. Delete `tools/cq/search/lsp/` subdirectory (8 files, ~548 LOC)

| File | LOC | Purpose |
|------|-----|---------|
| `lsp/__init__.py` | 16 | Re-exports LspSessionManager, run_lsp_requests, etc. |
| `lsp/capabilities.py` | 82 | LSP capability parsing (`supports_method()`) |
| `lsp/contracts.py` | 65 | LSP contract definitions and resolvers |
| `lsp/position_encoding.py` | 84 | UTF-8/UTF-16 position encoding for LSP |
| `lsp/request_queue.py` | 96 | Queued LSP request execution (`run_lsp_requests()`) |
| `lsp/root_resolution.py` | 74 | LSP provider root directory resolution |
| `lsp/session_manager.py` | 84 | LSP session lifecycle management |
| `lsp/status.py` | 47 | LSP status tracking and telemetry structs |

### 1b. Delete LSP contract/struct files (6 files, ~1,400 LOC)

| File | LOC | Purpose |
|------|-----|---------|
| `search/pyrefly_contracts.py` | ~500 | `PyreflyEnrichmentPayload`, `PyreflySymbolGrounding`, `PyreflyTypeContract`, `PyreflyCallGraph`, etc. |
| `search/rust_lsp_contracts.py` | ~694 | `RustLspEnrichmentPayload`, `RustSymbolGrounding`, `RustCallGraph`, etc. |
| `search/lsp_front_door_contracts.py` | ~46 | `LanguageLspEnrichmentRequest`, `LanguageLspEnrichmentOutcome`, `LspOutcomeCacheV1` |
| `search/lsp_contract_state.py` | ~80 | `LspContractStateV1`, `derive_lsp_contract_state()` |
| `search/lsp_request_budget.py` | ~80 | `LspRequestBudgetV1`, `call_with_retry()` |
| `search/pyrefly_signal.py` | ~80 | `evaluate_pyrefly_signal()`, `evaluate_pyrefly_signal_from_mapping()` |

### 1c. Delete LSP advanced planes and overlays (4 files, ~850 LOC)

| File | LOC | Purpose |
|------|-----|---------|
| `search/semantic_overlays.py` | 342 | `SemanticTokenSpanV1`, `fetch_semantic_tokens_range()`, `fetch_inlay_hints_range()` |
| `search/diagnostics_pull.py` | ~255 | `pull_text_document_diagnostics()`, `pull_workspace_diagnostics()` |
| `search/refactor_actions.py` | ~150 | `PrepareRenameResultV1`, `CodeActionV1`, code-action resolve/execute bridge |
| `search/rust_extensions.py` | ~221 | `RustMacroExpansionV1`, `RustRunnableV1`, `expand_macro()`, `get_runnables()` |

### 1d. Delete LSP session/pipeline files (4 files, ~3,650 LOC)

| File | LOC | Purpose |
|------|-----|---------|
| `search/pyrefly_lsp.py` | 1,354 | Full stdio JSON-RPC client for pyrefly. `enrich_with_pyrefly_lsp()`, session management, 8 LSP method probes |
| `search/rust_lsp.py` | 1,739 | Full stdio JSON-RPC client for rust-analyzer. `enrich_with_rust_lsp()`, session management |
| `search/lsp_front_door_pipeline.py` | 414 | `run_language_lsp_enrichment()`, language dispatch |
| `search/lsp_front_door_adapter.py` | ~140 | `lsp_runtime_enabled()`, `enrich_with_language_lsp()` |

### 1e. Delete LSP advanced plane orchestrator (1 file, ~140 LOC)

| File | LOC | Purpose |
|------|-----|---------|
| `search/lsp_advanced_planes.py` | ~140 | `collect_advanced_lsp_planes()` orchestrator |

### 1f. Delete neighborhood LSP adapter (1 file, ~278 LOC)

| File | LOC | Purpose |
|------|-----|---------|
| `neighborhood/pyrefly_adapter.py` | ~278 | `collect_pyrefly_slices()`, `PyreflySliceRequest` |

**Phase 1 total: 24 files, ~6,940 LOC deleted**

---

## Phase 2: Excise LSP Code from Mixed Files

**Goal:** Remove all LSP imports, fields, functions, telemetry, and summary keys from
files that also contain non-LSP logic. Work from leaf consumers inward to core.

### 2a. Search layer — `smart_search.py` (3,382 LOC, ~1,200 LOC LSP removal)

This is the largest modification. Changes:

1. **Remove imports** (lines 86-88, 99, 122-123): Delete imports of `coerce_pyrefly_*`,
   `lsp_front_door_adapter`, `pyrefly_lsp`, `pyrefly_signal`.

2. **Remove `EnrichedMatch.pyrefly_enrichment` field** (line 346): Delete field and all
   references. The `EnrichedMatch` struct loses its pyrefly payload slot.

3. **Delete pyrefly prefetch pipeline** (~80 LOC): `_prefetch_pyrefly_for_raw_matches()`,
   `_PyreflyPrefetchResult`, `MAX_PYREFLY_ENRICH_FINDINGS`.

4. **Delete pyrefly enrichment attachment** (~150 LOC): `_attach_pyrefly_enrichment()`,
   `_merge_matches_and_pyrefly()`, `_merge_match_with_pyrefly_payload()`,
   `_fetch_pyrefly_payload()`, `_pyrefly_payload_from_prefetch()`,
   `_seed_pyrefly_state()`, `_pyrefly_enrich_match()`.

5. **Delete pyrefly overview building** (~60 LOC): `_build_pyrefly_overview()`,
   `_accumulate_pyrefly_overview()`, `_pyrefly_summary_payload()`.

6. **Delete pyrefly telemetry/diagnostics** (~100 LOC): `_new_pyrefly_telemetry()`,
   `_pyrefly_anchor_key()`, `_pyrefly_anchor_key_from_raw()`,
   `_pyrefly_anchor_key_from_match()`, `_pyrefly_failure_diagnostic()`,
   `_pyrefly_no_signal_diagnostic()`, `_normalize_pyrefly_degradation_reason()`,
   `_pyrefly_coverage_reason()`.

7. **Delete LSP insight functions** (~300 LOC): `_apply_search_lsp_insight()`,
   `_collect_search_lsp_outcome()`, `_update_search_summary_lsp_telemetry()`,
   related provider detection and degradation code.

8. **Remove summary LSP keys**: Delete initialization of `pyrefly_overview`,
   `pyrefly_telemetry`, `rust_lsp_telemetry`, `lsp_advanced_planes`,
   `pyrefly_diagnostics` from summary dict (lines 1459-1475).

9. **Simplify `_build_search_summary()`**: Remove `pyrefly_overview`, `pyrefly_telemetry`,
   `pyrefly_diagnostics` parameters from signature and body (lines 2598-2653).

10. **Update `_finalize_search_matches()`**: Remove pyrefly merge step; enriched matches
    come directly from structural enrichment.

### 2b. Search layer — `partition_pipeline.py`

Remove:
- `pyrefly_prefetch` field and parameter passing
- `_resolve_prefetch_result()` calls
- References to `smart_search_mod._prefetch_pyrefly_for_raw_matches`
- References to `smart_search_mod._new_pyrefly_telemetry()`

### 2c. Search layer — `contracts.py`

Remove:
- `PyreflyOverview` class and `coerce_pyrefly_overview()` function (~35 LOC)
- `PyreflyTelemetry` class and `coerce_pyrefly_telemetry()` function (~22 LOC)
- `coerce_pyrefly_diagnostics()` function (~15 LOC)
- `__all__` entries for the above

### 2d. Calls macro — `macros/calls.py`

Remove:
- `_apply_calls_lsp()` and `_apply_calls_lsp_with_telemetry()` (~100 LOC)
- `_attach_calls_lsp_summary()` and `_finalize_calls_lsp_state()` (~70 LOC)
- `_normalize_pyrefly_calls_reason()` (~40 LOC)
- `_calls_payload_has_signal()` and related support functions
- All `pyrefly_telemetry` / `rust_lsp_telemetry` summary initialization
- LSP imports

### 2e. Query layer — `query/executor.py`

Remove from summary initialization (lines 325-341):
- `"pyrefly_overview": dict[str, object]()`
- `"pyrefly_telemetry": {...}`
- `"rust_lsp_telemetry": {...}`
- `"lsp_advanced_planes": dict[str, object]()`
- `"pyrefly_diagnostics": list[dict[str, object]]()`

### 2f. Query layer — `query/entity_front_door.py`

Remove:
- `EntityLspTelemetry` class
- `_run_entity_lsp()`, `_apply_candidate_lsp()`, `_resolve_lsp_target_context()`
- `_record_lsp_*()` functions (~80 LOC)
- `_apply_lsp_contract_state()` (~40 LOC)
- LSP summary building (lines 112-119)
- All LSP imports

### 2g. Query layer — `query/merge.py`

Remove:
- `_coerce_lsp_telemetry()` (~13 LOC)
- `_merge_lsp_contract_inputs()` (~43 LOC)
- LSP contract state derivation (~38 LOC)
- LSP imports

### 2h. Core layer — `core/enrichment_facts.py`

Remove all `CodeFact` definitions with `path` tuples starting with `("pyrefly", ...)`:
- Symbol grounding facts: `definition_targets`, `declaration_targets`, `type_definition_targets`, `implementation_targets`
- Type contract facts: `resolved_type`, `callable_signature`, `parameters`, `return_type`, `generic_params`, `is_async`, `is_generator`
- Call graph facts: `incoming_callers`, `outgoing_callees`, `incoming_total`, `outgoing_total`
- Class method context facts: `enclosing_class`, `base_classes`, `overridden_methods`, `overriding_methods`
- Local scope context facts: `same_scope_symbols`, `nearest_assignments`, `narrowing_hints`, `reference_locations`
- Import alias resolution facts: `resolved_path`, `alias_chain`
- Anchor diagnostics facts
- LSP health facts

**Important:** Preserve all non-pyrefly fact paths. Many facts have dual paths (pyrefly-first, structural fallback). Update these to use only the structural path.

### 2i. Core layer — `core/front_door_insight.py`

Remove:
- `augment_insight_with_lsp()` (~70 LOC)
- `_node_refs_from_lsp_entries()` (~18 LOC)
- `_lsp_provider_and_availability()` (~33 LOC)
- `_read_lsp_telemetry()` (~18 LOC)
- `InsightDegradationV1.lsp` field
- `InsightBudgetV1.lsp_targets` field
- LSP degradation flags and imports

### 2j. Core layer — `core/report.py`

Remove:
- Summary key references: `pyrefly_overview`, `pyrefly_telemetry`, `rust_lsp_telemetry`, `lsp_advanced_planes`, `pyrefly_diagnostics`
- `_format_pyrefly_overview()` (~30 LOC)
- `_derive_pyrefly_telemetry_status()`, `_derive_rust_lsp_telemetry_status()`,
  `_derive_lsp_advanced_status()`, `_derive_pyrefly_diagnostics_status()` (~66 LOC)
- Telemetry status map entries
- `enable_pyrefly=True` parameter

### 2k. Core layer — `core/diagnostics_contracts.py`

Remove fields:
- `pyrefly_telemetry: dict[str, object]`
- `rust_lsp_telemetry: dict[str, object]`
- `lsp_advanced_planes: dict[str, object]`
- `pyrefly_diagnostics: list[dict[str, object]]`
- Corresponding coercion and summary lines

### 2l. Core layer — `core/multilang_orchestrator.py`

Remove:
- `_zero_lsp_telemetry()`, `_coerce_lsp_telemetry()`, `_sum_lsp_telemetry()`,
  `_aggregate_lsp_telemetry()` (~40 LOC)
- `_aggregate_pyrefly_diagnostics()` (~40 LOC)
- `_select_advanced_planes_payload()` (~15 LOC)
- LSP telemetry aggregation calls

### 2m. Neighborhood layer — `neighborhood/bundle_builder.py`

Remove:
- `enable_lsp` parameter and all LSP-gated code paths
- `_collect_lsp_bundle_result()`, `_lsp_slice_kinds()`, `_collect_lsp_slices()`,
  `_collect_rust_lsp_slices()`, `_lsp_slice_from_rows()`, `_lsp_row_to_node()`,
  `_extract_lsp_env()` (~450 LOC total)
- LSP imports (`pyrefly_adapter`, `LspCapabilitySnapshotV1`)

### 2n. Neighborhood layer — `neighborhood/snb_renderer.py`

Remove:
- `enable_lsp` parameter
- `lsp_env` parameter
- `_populate_lsp_metadata()` function (~43 LOC)
- LSP environment extraction

### 2o. Neighborhood layer — supporting files

- `neighborhood/capability_gates.py`: Remove LSP capability references (~20 LOC)
- `neighborhood/section_layout.py`: Remove `lsp_deep_signals` section (~30 LOC)
- `core/snb_schema.py`: Remove `lsp_servers` field from `SemanticBundleMetaV1`

### 2p. Run layer — `run/runner.py`

Remove:
- `_aggregate_run_lsp_telemetry()` (~59 LOC)
- `_select_run_advanced_planes()` (~16 LOC)
- `_lsp_env_from_bundle()` (~14 LOC)
- LSP telemetry aggregation in `_merge_run_results()`
- `lsp_env=` parameter passing

### 2q. Run/CLI layer — minor fields

- `run/spec.py`: Remove `no_lsp: bool = False` field
- `cli_app/step_types.py`: Remove `no_lsp: bool = False` field
- `cli_app/commands/neighborhood.py`: Remove `--no-lsp` CLI parameter and related code

### 2r. Runtime layer

- `core/runtime/execution_policy.py`: Remove `LspRuntimePolicy` class, `ExecutionPolicy.lsp` field
- `core/runtime/worker_scheduler.py`: Remove `_lsp_semaphore`, `submit_lsp()` method

---

## Phase 3: Enhance Structural Enrichment to Fill Gaps

**Goal:** Promote the existing 5-stage non-LSP enrichment pipeline to be the sole
enrichment authority. Fill the most valuable gaps left by LSP removal.

### 3a. Enhance `libcst_python.py` — Symbol grounding replacement

The existing LibCST enrichment already provides:
- `QualifiedNameProvider` → qualified name candidates
- `ScopeProvider` → binding candidates, scope classification
- `ExpressionContextProvider` → LOAD/STORE/DEL role
- `ByteSpanPositionProvider` → canonical byte spans
- `ParentNodeProvider` → enclosing containers

**Enhancements to add:**

1. **Definition target resolution**: Use `ScopeProvider.assignments` to find where
   a name is assigned/defined within the module. Emit as `structural_definition_targets`.

   ```python
   # In LibCST enrichment, for each access node:
   for assignment in access.referents:
       if isinstance(assignment, cst.metadata.Assignment):
           span = byte_spans[assignment.node]
           targets.append({"bstart": span.start, "bend": span.start + span.length})
   ```

   Reference: `docs/python_library_reference/libcst-advanced.md` — ScopeProvider
   `Assignment.node` gives the definition node; `ByteSpanPositionProvider` gives spans.

2. **Reference locations**: Use `ScopeProvider.accesses` to find all references to a
   name within scope.

   Reference: `docs/python_library_reference/libcst.md` — `Access.referents` traces
   back to assignments.

3. **Import alias chain**: Already partially implemented via `import_alias_chain` field.
   Promote to primary authority.

### 3b. Enhance `python_enrichment.py` — Type annotation extraction

The existing ast-grep + Python ast enrichment already provides:
- `signature` (function parameters and return annotation text)
- `decorators`, `item_role`, `class_context`
- `call_target`, `scope_chain`

**Enhancements to add:**

1. **Annotation-based type info**: Extract return type annotation and parameter type
   annotations from function signatures. This replaces pyrefly's `resolved_type` for
   annotated code (the only case structural tools can handle).

   ```python
   # Using ast module:
   for node in ast.walk(tree):
       if isinstance(node, ast.FunctionDef):
           if node.returns:
               return_annotation = ast.get_source_segment(source, node.returns)
           for arg in node.args.args:
               if arg.annotation:
                   param_type = ast.get_source_segment(source, arg.annotation)
   ```

   Reference: `docs/python_library_reference/python_ast_libraries_and_cpg_construction.md`
   — `ast.FunctionDef.returns`, `ast.arg.annotation`, `ast.get_source_segment()`.

2. **Async/generator detection**: Already implemented via `behavior_summary` in
   python_enrichment. Ensure it covers:
   - `is_async`: presence of `async def` or `async for`/`async with`
   - `is_generator`: presence of `yield`/`yield from`

### 3c. Enhance `tree_sitter_python.py` — Scope and call context

The existing tree-sitter enrichment already provides:
- `call_target` (function being called at a call site)
- `enclosing_callable`, `enclosing_class`
- Parse quality metrics (ERROR/MISSING nodes)

**Enhancements to add:**

1. **Enclosing class base classes**: When inside a class, extract base classes from
   the `argument_list` of the `class_definition` node.

   ```python
   # S-expression query for class with bases:
   (class_definition
     name: (identifier) @class.name
     superclasses: (argument_list) @class.bases)
   ```

   Reference: `docs/python_library_reference/tree-sitter.md` — Query API,
   `QueryCursor.captures()` with scoped byte ranges.

2. **Same-scope symbols**: Use tree-sitter to enumerate all identifier nodes within
   the same function/class body. This partially replaces pyrefly's
   `same_scope_symbols`.

### 3d. Enhance `tree_sitter_rust.py` — Rust structural enrichment

The existing Rust tree-sitter enrichment provides scope classification and
signature extraction.

**Enhancements to add:**

1. **Impl block context**: Extract `impl <Type>` or `impl <Trait> for <Type>` context
   for methods found inside impl blocks.

   ```python
   # S-expression query:
   (impl_item
     trait: (type_identifier)? @impl.trait
     type: (type_identifier) @impl.type
     body: (declaration_list) @impl.body)
   ```

   Reference: `docs/python_library_reference/tree-sitter-rust.md`.

2. **Visibility modifier**: Extract `pub`, `pub(crate)`, `pub(super)` from items.

### 3e. Enhance `rust_enrichment.py` — Rust ast-grep structural patterns

The existing ast-grep Rust enrichment provides scope chain, function signatures,
parameters, return types, and visibility.

**Enhancements to add:**

1. **Trait implementation patterns**: Use ast-grep to find `impl Trait for Type` blocks.

   ```python
   root = SgRoot(source, "rust").root()
   impls = root.find_all(pattern="impl $TRAIT for $TYPE { $$$ }")
   for impl_node in impls:
       trait_name = impl_node["TRAIT"].text()
       type_name = impl_node["TYPE"].text()
   ```

   Reference: `docs/python_library_reference/ast-grep-py_rust_deepdive.md` —
   `SgRoot(code, "rust")`, metavariable captures.

2. **Macro invocation detection**: Detect `macro_name!(...)` patterns structurally.
   This replaces the macro identification (but not expansion) from rust-analyzer.

   ```python
   macros = root.find_all(pattern="$NAME!($$$)")
   ```

### 3f. Enhance `enrichment_facts.py` — Repoint fact paths

After removing all `("pyrefly", ...)` primary paths, update facts that had
pyrefly-first, structural-fallback paths to use only the structural path:

- `definition_targets` → `("libcst", "structural_definition_targets")`
- `resolved_type` → `("ast", "return_annotation")` (annotated only)
- `callable_signature` → `("ast_grep", "signature")`
- `is_async` → `("ast", "behavior_summary", "is_async")`
- `is_generator` → `("ast", "behavior_summary", "is_generator")`
- `enclosing_class` → `("tree_sitter", "enclosing_class")` or `("libcst", "enclosing_class")`
- `reference_locations` → `("libcst", "structural_reference_locations")`
- `import_alias_chain` → `("libcst", "import_alias_chain")`

**Accepted gaps** (no structural replacement possible):
- Inferred types for unannotated code (pyrefly `resolved_type` on unannotated functions)
- Cross-package definition targets (require full repo type-checking)
- Flow-sensitive type narrowing (`narrowing_hints`)
- Rust macro expansion text (`expand_macro()`)
- Rust trait method dispatch resolution (dynamic dispatch targets)
- Inlay hints (type/parameter annotations inferred by type checker)
- Semantic tokens (precise token classification by type checker)

---

## Phase 4: Update Documentation and Configuration

### 4a. Update `CLAUDE.md` and `AGENTS.md`

Remove all references to:
- "LSP-backed enrichment planes"
- "Advanced LSP Evidence Planes (Implemented)"
- `semantic_overlays.py`, `diagnostics_pull.py`, `refactor_actions.py`, `rust_extensions.py`
- pyrefly/rust-analyzer capability gating
- `--no-lsp` CLI flag
- "LSP-rich evidence collection" section
- `lsp_front_door_adapter`, `lsp_front_door_pipeline`
- "pyrefly" and "rust_analyzer" provider references

Update CQ skill descriptions to reflect structural-only enrichment.

### 4b. Update `.cq.toml` (if LSP config exists)

Remove any LSP-related configuration keys (timeouts, binary paths, etc.).

### 4c. Update test files

- Delete any test files that test pure LSP functionality
- Update integration tests that assert on pyrefly/LSP output fields
- Update golden snapshots that contain LSP telemetry data

### 4d. Clean up `__pycache__`

After deletion, remove stale `.pyc` files in `__pycache__/` directories for all
deleted modules.

---

## Phase 5: Validate

### 5a. Import validation

Run `uv run python -c "import tools.cq"` to verify no import errors from deleted modules.

### 5b. Quality gate

```bash
uv run ruff format && uv run ruff check --fix && uv run pyrefly check && uv run pyright
```

### 5c. Test suite

```bash
uv run pytest tests/ -m "not e2e" -q
```

### 5d. Functional smoke test

Run representative CQ commands to verify structural enrichment is working:

```bash
./cq search build_graph --format summary
./cq q "entity=function name=~^build" --format summary
./cq neighborhood src/semantics/pipeline.py:1 --format summary
./cq search register_udf --lang rust --format summary
```

---

## Execution Order Summary

| Phase | Description | Files Changed | LOC Impact |
|-------|-------------|---------------|------------|
| 1a | Delete `lsp/` subdirectory | 8 deleted | -548 |
| 1b | Delete LSP contract files | 6 deleted | -1,400 |
| 1c | Delete advanced planes/overlays | 4 deleted | -850 |
| 1d | Delete LSP session/pipeline files | 4 deleted | -3,650 |
| 1e | Delete plane orchestrator | 1 deleted | -140 |
| 1f | Delete neighborhood LSP adapter | 1 deleted | -278 |
| 2a | Excise LSP from `smart_search.py` | 1 modified | -1,200 |
| 2b-2c | Excise from partition/contracts | 2 modified | -100 |
| 2d | Excise from `calls.py` | 1 modified | -250 |
| 2e-2g | Excise from query layer | 3 modified | -200 |
| 2h-2l | Excise from core layer | 5 modified | -500 |
| 2m-2o | Excise from neighborhood layer | 5 modified | -550 |
| 2p-2r | Excise from run/CLI/runtime | 6 modified | -150 |
| 3a-3f | Enhance structural enrichment | 5 modified | +200 |
| 4 | Update docs/config | ~5 modified | neutral |
| 5 | Validate | — | — |
| **Total** | | **24 deleted, ~27 modified** | **~-9,400** |

## Risk Assessment

| Risk | Mitigation |
|------|------------|
| Missing import at runtime | Phase 5a catches this before tests |
| Test failures from removed summary keys | Phase 4c updates tests; golden updates in Phase 5 |
| Enrichment quality regression | Phase 3 enhancements compensate; accepted gaps are documented |
| Large diff makes review hard | Phases 1-2 are mechanical deletions; Phase 3 is small additive |
| Smart search complexity | 2a is the riskiest step; do it in sub-steps, test between each |

## Dependencies

- No external dependency changes needed (ast-grep-py, tree-sitter, libcst are already
  installed)
- No Rust rebuild needed (rust-analyzer subprocess removal is Python-only)
- No DataFusion changes needed
