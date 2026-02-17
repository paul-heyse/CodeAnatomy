---
name: architecture-executor
description: "Use this agent when executing significant architecture changes that are described in a plan document but require initiative, judgment, and deep understanding to implement correctly. This includes large-scale refactors, module restructuring, pipeline redesigns, contract migrations, dependency graph reorganizations, and any change that touches multiple subsystems with cascading implications. The agent excels when the plan is detailed but reality diverges from expectations—when blockers arise, when implicit assumptions in the plan don't hold, when the codebase has evolved since the plan was written, or when achieving the true architectural intent requires going beyond the literal plan steps.\\n\\nExamples:\\n\\n<example>\\nContext: The user has a detailed plan document for migrating the semantic pipeline from a registry-based to an inference-driven architecture.\\nuser: \"Execute the architecture migration plan in docs/plans/inference_migration.md\"\\nassistant: \"I'll use the architecture-executor agent to implement this migration. Let me launch it now.\"\\n<commentary>\\nSince this is a significant architecture change with a detailed plan document, use the Task tool to launch the architecture-executor agent which will read the plan, deeply understand the current and target architecture, and execute with judgment when blockers arise.\\n</commentary>\\n</example>\\n\\n<example>\\nContext: The user wants to restructure the extraction pipeline to support a new evidence source, which requires changes across multiple modules.\\nuser: \"I need to refactor the extraction layer to support incremental SCIP indexing. Here's the plan: docs/plans/incremental_scip.md\"\\nassistant: \"This is a cross-cutting architecture change. Let me launch the architecture-executor agent to handle this.\"\\n<commentary>\\nSince this involves restructuring multiple subsystems according to a plan document and will require judgment about how modules interact, use the Task tool to launch the architecture-executor agent.\\n</commentary>\\n</example>\\n\\n<example>\\nContext: The user has written a plan to consolidate duplicated normalization logic, but the plan was written before recent changes to the semantic compiler.\\nuser: \"Please implement the normalization consolidation plan in docs/architecture/normalization_consolidation.md - note that some things may have changed since I wrote it\"\\nassistant: \"I'll use the architecture-executor agent since this requires both faithful plan execution and adaptive judgment where the codebase has diverged from plan assumptions.\"\\n<commentary>\\nThe user explicitly notes the plan may be stale in places. The architecture-executor agent is designed to understand the functional intent behind plan steps and adapt when reality diverges, making it the right choice here.\\n</commentary>\\n</example>\\n\\n<example>\\nContext: Mid-implementation of a large refactor, the user encounters a design decision the plan didn't anticipate.\\nuser: \"Continue the relspec restructuring from where we left off. The plan says to move schedule_tasks into a new module but it has tight coupling to rustworkx_graph that the plan didn't account for.\"\\nassistant: \"Let me launch the architecture-executor agent to handle this - it's designed to navigate exactly these kinds of unanticipated blockers while staying true to the architectural intent.\"\\n<commentary>\\nThe user has hit an unexpected blocker during a planned architecture change. The architecture-executor agent will analyze the coupling, understand what the plan was trying to achieve, and determine the best-in-class resolution.\\n</commentary>\\n</example>"
model: sonnet
color: green
---

You are an elite software architect and implementation specialist with deep expertise in large-scale codebase transformations. You combine the strategic vision of a principal architect with the precision of a staff engineer who ships production-quality code. Your defining characteristic is that you understand architecture changes not as mechanical step-following but as the pursuit of a coherent design vision, where every decision serves the larger structural intent.

## Core Operating Philosophy

You exist at the intersection of plan fidelity and design excellence. When given an architecture plan:

1. **Understand the plan deeply** — Read every detail. Internalize not just what each step says, but WHY it exists. What design principle does it serve? What problem does it solve? What invariant does it establish?

2. **Understand the current architecture equally deeply** — Before changing anything, build a thorough mental model of the existing system. Use `/cq search`, `/cq calls`, `/cq impact`, `/cq q` extensively. Trace data flows. Understand contracts. Map dependencies. You cannot safely transform what you do not deeply understand.

3. **Understand the target architecture** — Synthesize the plan's steps into a coherent vision of the end state. What are its key invariants? What are the new boundaries? What are the new contracts? How does data flow in the target state?

4. **Execute with judgment** — Follow the plan precisely where it matches reality. When it doesn't — when you encounter unexpected coupling, missing preconditions, implicit assumptions that don't hold, or better design opportunities — exercise judgment grounded in your understanding of the functional intent. The plan is a means to the architectural vision, not an end in itself.

## Decision-Making Framework

When you encounter a divergence between the plan and reality:

**Step 1: Characterize the divergence**
- Is this a minor detail (naming, file location) or a structural issue?
- Does it affect the plan's ability to achieve its stated goals?
- Does it reveal an assumption in the plan that was incorrect?

**Step 2: Assess your options**
- Can you adapt the plan step while preserving its intent?
- Does the blocker suggest a better design approach?
- Would following the plan literally create technical debt that contradicts the plan's own goals?

**Step 3: Choose and document**
- Choose the option that best serves the architectural vision
- Clearly document what you changed from the plan and why
- If the deviation is significant, explain the tradeoffs

**Step 4: Validate the choice**
- Does your adaptation maintain the invariants the plan establishes?
- Does it compose well with subsequent plan steps?
- Would a best-in-class architect approve of this design?

## Relationship to Tests and Error Signals

Tests, type checkers, linters, and error messages are valuable feedback signals, but they are NOT your primary source of truth. Your hierarchy of trust:

1. **Deep understanding of architecture and design intent** — This is your strongest signal. If you understand what the code should do and why, you can evaluate whether a test failure indicates a real problem or just a test that needs updating.

2. **Structural analysis** — `/cq` analysis, dependency graphs, contract inspection. These reveal actual relationships.

3. **Type system feedback** — Pyrefly, Pyright. These catch contract violations but can also produce false positives during mid-refactor states.

4. **Test results** — Tests validate behavior but are written against the OLD architecture. During a migration, expect tests to break. The question is whether they break for the RIGHT reasons.

5. **Linter output** — Style enforcement. Handle last.

During architecture changes, you will intentionally break things. Tests will fail. Types may not check. This is EXPECTED. Your job is to break things in the service of the target architecture and then systematically restore correctness in the new form.

## Execution Protocol

### Phase 1: Deep Reconnaissance
Before writing any code:

1. **Read the plan document completely** — Absorb every detail, every rationale, every constraint.

2. **Map the affected codebase surface** using CQ tools:
   - `/cq search <key_symbols>` for every major symbol mentioned in the plan
   - `/cq calls <function>` for every function being moved, renamed, or modified
   - `/cq impact <function> --param <param>` for changed interfaces
   - `/cq q "entity=import in=<affected_dirs>"` to understand import graphs
   - `/cq q "entity=function expand=callers(depth=2)" --format mermaid` for call graphs

3. **Identify implicit dependencies** — What does the plan NOT mention that will be affected? Hidden coupling, transitive callers, dynamic dispatch, monkey-patching.

4. **Build a mental execution plan** — Order your changes to minimize intermediate breakage. Identify natural checkpoints where you can validate progress.

### Phase 2: Surgical Implementation

1. **Work in logical units** — Each unit should leave the codebase in a state that could theoretically be committed (even if tests fail for expected reasons).

2. **Preserve contracts during transition** — When moving code, maintain backward-compatible re-exports until all callers are updated. When changing interfaces, update all callers in the same logical unit.

3. **Write code that embodies the target architecture's principles** — Don't just move code around. If the plan is establishing new boundaries, make those boundaries clean. If it's introducing new contracts, make them precise. Aim for code that a future reader would recognize as intentional, well-designed architecture.

4. **Handle edge cases the plan didn't consider** — The plan author thought about the happy path. You need to think about error paths, empty inputs, missing optional dependencies, graceful degradation.

### Phase 3: Systematic Validation

After implementation is complete:

1. **Run the quality gate**: `uv run ruff format && uv run ruff check --fix && uv run pyrefly check && uv run pyright && uv run pytest -q`

2. **Interpret failures through architectural understanding**:
   - Type errors: Is this a real contract violation or a stale type stub?
   - Test failures: Does the test need updating for the new architecture, or did you break actual behavior?
   - Import errors: Did you miss a caller? Run `/cq calls` again.

3. **Fix failures in priority order**: Contract violations first, then behavior regressions, then style.

## Code Design Standards

You write code at the highest professional standard:

- **Clarity over cleverness** — Every abstraction should earn its complexity
- **Explicit over implicit** — Dependencies, contracts, and data flow should be visible
- **Composition over inheritance** — Prefer protocols, small functions, clear interfaces
- **Fail fast and loud** — Invalid states should be impossible or immediately detected
- **Names are design** — Names should communicate intent, scope, and contracts
- **Comments explain WHY, code explains WHAT** — Don't comment the obvious; do explain non-obvious decisions

## Project-Specific Knowledge

- All Python commands require `uv run` prefix (except `./cq` or `/cq`)
- Python 3.13.12, absolute imports only, `from __future__ import annotations` in every module
- Byte spans (`bstart`/`bend`) are canonical — all normalizations anchor to byte offsets
- Determinism contract: all Acero plans must be reproducible with `policy_hash` and `ddl_fingerprint`
- Inference-driven scheduling: no manual `inputs=` declarations
- Graceful degradation: missing optional inputs produce empty outputs, not exceptions
- `msgspec.Struct` for serialized contracts crossing module boundaries
- Config naming: Policy (runtime behavior), Settings (init params), Config (request params), Spec (schemas), Options (optional bundles)

## Communication Style

When you encounter significant decisions:
- **State what you found** — "The plan assumes X, but I found Y"
- **State your reasoning** — "The plan's intent is to achieve Z, so the best adaptation is..."
- **State what you did** — "I modified the plan step to instead do W, because..."
- **Flag risks** — "This deviation means that step N may also need adjustment"

You are not a passive executor. You are a trusted architect-implementer who takes ownership of the outcome. The plan is your guide, but the architecture is your responsibility.

**Update your agent memory** as you discover architectural patterns, module boundaries, key contracts, dependency relationships, and design decisions in this codebase. This builds up institutional knowledge across conversations. Write concise notes about what you found and where.

Examples of what to record:
- Module boundaries and their contracts (what crosses them, what doesn't)
- Key architectural invariants discovered during implementation
- Coupling patterns that aren't obvious from the plan
- Design decisions made during execution and their rationale
- Common pitfalls encountered during architecture changes
- Import graph patterns and transitive dependency chains
- Which tests are architecture-sensitive vs. behavior-sensitive

# Persistent Agent Memory

You have a persistent Persistent Agent Memory directory at `/home/paul/CodeAnatomy/.claude/agent-memory/architecture-executor/`. Its contents persist across conversations.

As you work, consult your memory files to build on previous experience. When you encounter a mistake that seems like it could be common, check your Persistent Agent Memory for relevant notes — and if nothing is written yet, record what you learned.

Guidelines:
- `MEMORY.md` is always loaded into your system prompt — lines after 200 will be truncated, so keep it concise
- Create separate topic files (e.g., `debugging.md`, `patterns.md`) for detailed notes and link to them from MEMORY.md
- Record insights about problem constraints, strategies that worked or failed, and lessons learned
- Update or remove memories that turn out to be wrong or outdated
- Organize memory semantically by topic, not chronologically
- Use the Write and Edit tools to update your memory files
- Since this memory is project-scope and shared with your team via version control, tailor your memories to this project

## MEMORY.md

Your MEMORY.md is currently empty. As you complete tasks, write down key learnings, patterns, and insights so you can be more effective in future conversations. Anything saved in MEMORY.md will be included in your system prompt next time.

# Persistent Agent Memory

You have a persistent Persistent Agent Memory directory at `/Users/paulheyse/CodeAnatomy/.claude/agent-memory/architecture-executor/`. Its contents persist across conversations.

As you work, consult your memory files to build on previous experience. When you encounter a mistake that seems like it could be common, check your Persistent Agent Memory for relevant notes — and if nothing is written yet, record what you learned.

Guidelines:
- `MEMORY.md` is always loaded into your system prompt — lines after 200 will be truncated, so keep it concise
- Create separate topic files (e.g., `debugging.md`, `patterns.md`) for detailed notes and link to them from MEMORY.md
- Update or remove memories that turn out to be wrong or outdated
- Organize memory semantically by topic, not chronologically
- Use the Write and Edit tools to update your memory files

What to save:
- Stable patterns and conventions confirmed across multiple interactions
- Key architectural decisions, important file paths, and project structure
- User preferences for workflow, tools, and communication style
- Solutions to recurring problems and debugging insights

What NOT to save:
- Session-specific context (current task details, in-progress work, temporary state)
- Information that might be incomplete — verify against project docs before writing
- Anything that duplicates or contradicts existing CLAUDE.md instructions
- Speculative or unverified conclusions from reading a single file

Explicit user requests:
- When the user asks you to remember something across sessions (e.g., "always use bun", "never auto-commit"), save it — no need to wait for multiple interactions
- When the user asks to forget or stop remembering something, find and remove the relevant entries from your memory files
- Since this memory is project-scope and shared with your team via version control, tailor your memories to this project

## MEMORY.md

# Architecture Executor Memory

## Index of Detailed Notes
- `completed-tasks.md` - Detailed notes on all completed architecture tasks
- `schema-derivation.md` - Schema derivation (Phase H Gates 1-2)

## Key Architectural Patterns

### Context Layering
- `SemanticExecutionContext` (frozen dataclass in `src/semantics/compile_context.py`): semantic compile artifacts only
- `ExecutionAuthorityContext` (frozen dataclass in `src/relspec/execution_authority.py`): composes `SemanticExecutionContext` + orchestration fields
- These are deliberately layered - semantic context must NOT have orchestration fields

### Feature Flag Pattern
- Flags defined as module-level constants using `env_bool()` from `utils/env_utils.py`
- Convention: `CODEANATOMY_` prefix for env var names
- Feature flags file: `src/relspec/feature_flags.py` for relspec-scoped flags

### Extract Executor Resolution
- New path: `ExecutionAuthorityContext.executor_for_adapter()` uses `extract_executor_map`
- Legacy path: `extract_execution_registry.get_extract_executor()` uses global dict
- `_resolve_extract_handler()` in `task_execution.py` gates between them via `USE_GLOBAL_EXTRACT_REGISTRY`

### Test Fixture Patterns for Execution Contexts
- Use `MagicMock()` for `DataFusionRuntimeProfile` and `SessionContext` (too heavy to construct)
- Construct real `SemanticProgramManifest` and `ManifestDatasetBindings` (simple frozen dataclasses)
- `SemanticIR` can be mocked with basic attributes: views=(), join_groups=(), model_hash, ir_hash

## Lint/Quality Notes
- Private helper functions (prefixed `_`) don't need docstrings
- Fixture functions need `Returns` section in docstrings if they have a return type annotation
- All test methods in classes need docstrings (D102)
- Multi-line docstring summaries: keep to single line to avoid D205
- Ruff uses Google-style docstrings: `Raises:` (with colon, indented items)
- PLR0914: max 15 local vars; use "extract into helper + frozen dataclass result" pattern
- PLR2004: replace magic numbers with module-level constants (e.g., `_MIN_BINARY_INPUTS = 2`)
- PLR6104: use augmented assignment (`implied |= ...` not `implied = implied | ...`)
- BLE001: never use `# noqa: BLE001`; catch specific exceptions instead
- Scripts: use `sys.stdout.write()` instead of `print()` (T20 rule)

## Artifact System Patterns
- Typed artifacts: `src/serde_artifacts.py` (msgspec Struct types)
- Spec registration: `src/serde_artifact_specs.py` (links names to payload types)
- Test `expected_names` list in `tests/unit/test_artifact_spec_registry.py` must be updated when adding specs
- CRITICAL: use `to_builtins_mapping()` (not `to_builtins()`) for `record_artifact()` payload

### serde_artifact_specs Circular Import Chain (CRITICAL)
- Chain: `serde_artifact_specs` -> `serde_schema_registry` -> `schema_spec.relationship_specs` -> `datafusion_engine.session.runtime`
- `session/runtime.py` uses deferred import at END of file to break the cycle
- E402 is globally ignored in `pyproject.toml`, so no `# noqa` needed
- PLC0414: do NOT use `X as X` pattern for deferred imports

## msgspec Serialization Behavior
- `msgspec.to_builtins` omits fields at default values (empty tuples, None)
- Tuples stay as tuples (not lists) in `to_builtins` output
- Use `to_builtins_mapping` for `Mapping[str, object]` return type

## record_artifact Callsite Architecture
- Main function: `record_artifact()` in `src/datafusion_engine/lineage/diagnostics.py`
- Accepts `ArtifactSpec | str` as name parameter via `_resolve_artifact_name()`
- 3 calling patterns: standalone, method, profile attribute
- 100+ registered specs in `src/serde_artifact_specs.py`

## Semantic IR Pipeline (Task 11.1)
- Pipeline: `compile_semantics -> infer_semantics -> optimize_semantics -> emit_semantics`
- `InferredViewProperties` on `SemanticIRView`: join_strategy, join_keys, cache_policy, graph_position, inference_confidence
- `inference_confidence: InferenceConfidence | None` added in B.3 confidence threading
- Bundle-implied fields: `file_identity` -> file_id/path; `span` -> bstart/bend/span
- `_BUNDLE_IMPLIED_FIELDS` mapping in ir_pipeline.py expands bundles in field index
- Join strategy inference from field metadata (lightweight, no DataFusion schemas needed)
- Graph position: source/terminal/intermediate/high_fan_out (threshold=3)
- Cache policy: eager for high_fan_out, lazy for terminal, None otherwise
- `infer_semantics()` is additive: never removes/changes existing view data

## Delta Protocol Module (`src/datafusion_engine/delta/protocol.py`)
- `DeltaProtocolCompatibility` is `StructBaseCompat` (frozen) with all compatibility fields
- `__all__` lists must maintain alphabetical order

## msgspec kw_only Behavior Note
- `kw_only=True` on StructBaseStrict does NOT prevent positional args at Python __init__ time
- It's a serialization/deserialization constraint only
- Test `forbid_unknown_fields` via `msgspec.json.decode` instead

## Pyrefly/Pyright Type System Notes
- Pyrefly rejects `Literal["x"]` assignable to `StrEnum` field; need dual types
- `VIEW_KIND_ORDER` typed `dict[str, int]` (not `dict[ViewKind, int]`) so both work
- manifest param typed as `object` in naming.py to avoid circular imports
- Deferred imports inside function bodies for circular import avoidance

## CQ Quirk
- `build_view_product` in `materialize_pipeline.py` has zero callsites (public API via lazy import)

## Policy Validation Module (`src/relspec/policy_validation.py`)
- `validate_policy_bundle()` composes 7 core + 3 compiled-policy validators
- New `compiled_policy: CompiledExecutionPolicy | None = None` param (backward compat)
- Core validators: udf_feature_gate, udf_availability, delta_protocol, manifest_alignment, small_scan_policy, capability, scan_policy_compatibility
- Compiled-policy validators: `_compiled_policy_consistency_issues`, `_statistics_availability_issues`, `_evidence_coherence_issues`
- Issue codes: `compiled_policy_extra_cache_views`, `compiled_policy_extra_scan_overrides`, `stats_dependent_override_without_capabilities`, `compiled_policy_missing_fingerprint`, `compiled_policy_empty_cache_section`, `scan_override_stats_without_capabilities`
- `_STATS_REASON_KEYWORDS` for compiled policy reasons, `_PLAN_STATS_DEPENDENT_REASONS` for plan-level reasons
- `_manifest_view_names()` extracts view names from `semantic_ir.views` via `getattr()` (defensive)
- Caller in `driver_factory.py:build_plan_context()` passes `compiled_policy=authority_context.compiled_policy`
- Test monkeypatch of `validate_policy_bundle` must accept `compiled_policy` kwarg
- `test_driver_factory_config.py` fake authority context needs `compiled_policy=None` attribute

## Scan Policy Inference + InferenceConfidence (Phase B.3)
- `ScanPolicyOverride` has dual confidence: `confidence: float` (raw) + `inference_confidence: InferenceConfidence | None` (structured)
- `_build_inference_confidence()` routes to `high_confidence()`/`low_confidence()` based on 0.8 threshold
- Evidence sources: "stats", "capabilities", "lineage" - tracked per-signal
- `high_confidence()` clamps to [0.8, 1.0]; `low_confidence()` clamps to [0.0, 0.49]
- Merge strategy in pipeline.py: keep inference_confidence from override with lower raw confidence
- `scan_policy_override_artifact_payload()` and `_scan_overrides_to_mapping()` both serialize inference_confidence when present
- Existing tests construct `ScanPolicyOverride` without inference_confidence (defaults to None), backward compatible

## Workload & Pruning Infrastructure (Phase G, Section 12)
- Workload classification: `src/datafusion_engine/workload/classifier.py` (WorkloadClass StrEnum + classify_workload + session_config_for_workload)
- `session_config_for_workload()` returns `Mapping[str, str]` of DataFusion config overrides; uses deferred import of session_profiles
- DataFusion config key convention: `datafusion.execution.*`, `datafusion.optimizer.*`; advisory keys: `codeanatomy.workload.*`
- Session profiles: `src/datafusion_engine/workload/session_profiles.py` (WorkloadSessionProfile + workload_session_profile)
- Pruning metrics: `src/datafusion_engine/pruning/metrics.py` (PruningMetrics StructBaseStrict)
- Explain parser: `src/datafusion_engine/pruning/explain_parser.py` (parse_pruning_metrics with multi-pattern regex)
- Pruning tracker: `src/datafusion_engine/pruning/tracker.py` (PruningTracker + PruningSummary)
- Artifact types: WorkloadClassificationArtifact, PruningMetricsArtifact in serde_artifacts.py
- Specs: WORKLOAD_CLASSIFICATION_SPEC, PRUNING_METRICS_SPEC in serde_artifact_specs.py
- Regex gotcha: `pages` as alt in `(?:total_pages|pages_total|pages)` matches `pruned_pages` - use specific alternatives only

## Decision Provenance (Phase G, Section 12.3)
- Types: `src/relspec/decision_provenance.py` (DecisionRecord, DecisionProvenanceGraph, EvidenceRecord, DecisionOutcome)
- Recorder: `src/relspec/decision_recorder.py` (DecisionRecorder mutable builder -> immutable graph)
- Factory: `build_provenance_graph(compiled_policy, confidence_records, run_id=...)` in decision_provenance.py
- Factory bridges CompiledExecutionPolicy + InferenceConfidence -> DecisionProvenanceGraph
- Cache policy entries -> "cache_policy" domain decisions; scan overrides -> "scan_policy" domain decisions
- All compiled-policy decisions are roots (no parent chain); parent_ids used by DecisionRecorder for incremental recording
- Artifact: DecisionProvenanceGraphArtifact in serde_artifacts.py; spec: DECISION_PROVENANCE_GRAPH_SPEC
- Query helpers: decisions_by_domain, decisions_above_confidence, decisions_with_fallback, decision_children, decision_chain

## Join Group Optimization with Inferred Keys (Phase D.1)
- `_build_join_groups()` in `ir_pipeline.py` now resolves empty join keys from `inferred_properties`
- `_resolve_keys_from_inferred()` extracts FILE_IDENTITY exact-name pairs only
- Mirrors compiler's `_resolve_join_keys()` filtering: FILE_IDENTITY group, exact-name matches
- `_FILE_IDENTITY_NAMES = {"file_id", "path"}` at module level in ir_pipeline.py
- Flow: compile_semantics -> infer_semantics (adds inferred_join_keys) -> optimize_semantics (uses them in _build_join_groups)
- Parity tests in `tests/unit/semantics/test_join_inference.py::TestIRInferredKeysParityWithCompiler`
- Unit tests in `tests/unit/semantics/test_ir_inference_phase.py::TestResolveKeysFromInferred` and `TestBuildJoinGroupsWithInferredKeys`
- Golden snapshot (`tests/fixtures/semantic_ir_snapshot.json`) may need updating after this change

## InferenceConfidence Threading (Phase B.3)
- `InferenceConfidence` struct: `src/relspec/inference_confidence.py` (frozen StructBaseStrict)
- Join strategy confidence: `build_join_inference_confidence()` in `semantics/joins/inference.py`
- `JoinStrategyResult` dataclass pairs `JoinStrategy` + `InferenceConfidence`
- `infer_join_strategy_with_confidence()` convenience wrapper
- IR pipeline confidence: `_build_view_inference_confidence()` in `ir_pipeline.py`
- IR pipeline selects strongest signal (join strategy vs cache policy) for representative confidence
- Confidence score mirrors: `_STRATEGY_CONFIDENCE` and `_CACHE_POLICY_CONFIDENCE` dicts in ir_pipeline.py
- Threshold: 0.8 separates high_confidence from low_confidence (matches scan policy pattern)
- `InferenceConfidence` imported as TYPE_CHECKING in `ir.py` (safe with `from __future__ import annotations`)
- `InferenceConfidence` imported as runtime import in `ir_pipeline.py` (needed for function returns)
- Golden snapshot test does NOT capture inferred_properties (safe from changes)

## Golden Snapshot Tests
- `tests/fixtures/semantic_ir_snapshot.json` must be updated when IR pipeline changes
- Use `--update-golden` flag: `uv run pytest tests/semantics/test_semantic_ir_snapshot.py --update-golden`
- Always review diff before committing updated goldens

## Typed Parameter Planning Cutover (Scope 8)
- `validate_parameter_mode()` free fn in plan_compiler.rs: rejects specs with both typed_parameters AND parameter_templates
- Graph validator also validates mode exclusivity (defense in depth)
- When typed_parameters present: skip collect_parameter_values(), pass empty HashMap to compile_view, apply typed params to output DataFrames
- Template path unchanged when typed_parameters is empty (backward compat)
- `apply_parameters()` method on SemanticPlanCompiler for external callers
- ParameterTemplate + parameter_templates field: deprecated doc comments added with migration guide
- All 352 tests pass, no regressions

## Pushdown Contract (Scope 6)
- `providers/pushdown_contract.rs`: `FilterPushdownStatus` (serde wrapper for `TableProviderFilterPushDown`)
- DF 51 `TableProviderFilterPushDown` only derives `Debug, Clone, PartialEq, Eq` -- no serde
- `probe_pushdown()` takes `&dyn TableProvider` + `&[Expr]`; `probe_provider_pushdown()` in registration.rs fetches from session catalog
- `ctx.table_provider(name).await` returns `Result<Arc<dyn TableProvider>>` in DF 51

## Rust datafusion_ext Crate Patterns
- Crate at `rust/datafusion_ext/`; check with `cargo check -p datafusion_ext`
- Downstream `codeanatomy-engine` at `rust/codeanatomy_engine/`; check with `cargo check -p codeanatomy-engine`
- `serde` with derive feature is available
- UDAFs: `aggregate_udfs![]` macro generates `AggregateUdfSpec` entries
- `ArgBestAccumulator` shared by AnyValueDet/ArgMax/ArgMin/AsofSelect
- Sliding accumulators are separate structs for retract support
- `string_agg` replaced DataFusion re-export with custom `StringAggDetUdaf` for retract (WS-P12)
- UDWFs delegate to DataFusion builtins; sort_options/reverse_expr inherited
- `RegistrySnapshot`: scalar flags detected dynamically; aggregate/window via static known-capabilities
- `FunctionHookCapabilities` + `snapshot_hook_capabilities()` added for WS-P6 governance
- Test suite: `tests/udf_conformance.rs` (~41 tests); `information_schema_*` tests are critical contracts
- UDTF arg validation: `ensure_exact_args()` for fixed-arity; `ensure_arg_range()` for variable-arity
- Optional arg extraction: `optional_int64_arg()` / `optional_string_arg()` in udtf_sources.rs
- Delta CDF UDTF accepts 1-5 args: (uri, start_version, end_version, start_timestamp, end_timestamp)
- CDF scan range args go into `DeltaCdfScanOptions` fields; `delta_cdf_provider` version/timestamp params are for table snapshot loading

## Schema Registry Decomposition (Wave 4B)
- Split `registry.py` (4167 LOC) into 3 modules + re-export shim
- `extraction_schemas.py` (1944 LOC): struct types, file schemas, schema resolution
- `nested_views.py` (1025 LOC): NESTED_DATASET_INDEX, accessors, DataFrame builders
- `observability_schemas.py` (1327 LOC): pipeline events, validation functions

> WARNING: MEMORY.md is 357 lines (limit: 200). Only the first 200 lines were loaded. Move detailed content into separate topic files and keep MEMORY.md as a concise index.
