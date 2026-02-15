# CQ Search Restructuring Plan v1 Review
Date: 2026-02-15

## Review Context
- Environment setup completed first as requested:
- `scripts/bootstrap_codex.sh` (success, Python 3.13.12 and plugin path validated)
- `uv sync` (success)
- Primary source reviewed: `docs/plans/cq_search_restructuring_implementation_plan_v1_2026-02-15.md`
- Validation method:
- Direct code review across `tools/cq/search/` and cross-package callers
- Targeted local API probes (`tree_sitter`, `ast_grep_py`, `msgspec`) via `uv run`
- CQ command outputs where functional; fallback to direct code inspection where CQ CLI paths were incomplete

## Executive Conclusions
1. The plan direction is strong (deduplication, contract hygiene, package boundaries), but it currently contains multiple correctness and feasibility issues that should be fixed before execution.
2. The largest risk is the hard-cutover/no-shim strategy. It conflicts with real, current dependencies outside `tools/cq/search` and will create a high-regression migration.
3. The plan should be revised into a staged migration with explicit file-accounting, API-compatibility checkpoints, and a stricter runtime-vs-serializable boundary model focused on actual violations in the current code.

## Verified Baseline Snapshot (Current Code)
- `tools/cq/search` currently has `96` Python files and `21,898` LOC (plan states `21,284` at `docs/plans/cq_search_restructuring_implementation_plan_v1_2026-02-15.md:24`).
- Files >750 LOC currently include:
- `tools/cq/search/smart_search.py` (3720)
- `tools/cq/search/python_enrichment.py` (2250)
- `tools/cq/search/tree_sitter_rust.py` (1766)
- `tools/cq/search/classifier.py` (1047)
- `tools/cq/search/python_native_resolution.py` (823)
- `tools/cq/search/tree_sitter_python.py` (810)
- Tree-sitter contract inventory in repo is `17` `tree_sitter*contracts.py` files totaling `672` LOC, not `23` / `879` as stated at `docs/plans/cq_search_restructuring_implementation_plan_v1_2026-02-15.md:26` and `docs/plans/cq_search_restructuring_implementation_plan_v1_2026-02-15.md:28`.
- There are many active non-search-package imports of search APIs, including package-level imports (for example `tools/cq/query/executor.py:100`, `tools/cq/macros/calls.py:45`, `tools/cq/macros/impact.py:38`).

## Critical Findings (Must Fix Before Implementation)

### C1. Hard-Cutover/No-Shim Assumption Is Incorrect for Current Dependency Graph
Evidence:
- Plan states no external consumers and no compatibility shims: `docs/plans/cq_search_restructuring_implementation_plan_v1_2026-02-15.md:7`, `docs/plans/cq_search_restructuring_implementation_plan_v1_2026-02-15.md:20`.
- Current repo has active package-level imports from outside `tools/cq/search`, for example `tools/cq/query/executor.py:100`, `tools/cq/macros/calls.py:45`, `tools/cq/macros/impact.py:38`.
- Plan proposes minimal empty `__init__.py`: `docs/plans/cq_search_restructuring_implementation_plan_v1_2026-02-15.md:2357`.
- Current `__init__.py` is a real import surface: `tools/cq/search/__init__.py:8`.
Impact:
- Immediate breakage across query/macros/CLI/runtime code if old paths are removed atomically.
Recommendation:
- Replace hard-cutover with staged compatibility windows:
- Stage A: create new modules and dual-wire imports
- Stage B: migrate callsites by subsystem
- Stage C: remove compatibility exports after repo-wide verification

### C2. Plan Contains API-Incorrect Code Snippets That Would Not Run
Evidence:
- Plan uses `query.matches(...)` in runtime snippet: `docs/plans/cq_search_restructuring_implementation_plan_v1_2026-02-15.md:697`.
- In current environment, `matches/captures` run on `QueryCursor`, not on `Query`; current implementation already does this at `tools/cq/search/tree_sitter_runtime.py:111`.
- Plan ABI snippet uses `language.version` (deprecated) in `check_abi_compatibility`; local probe emits deprecation warning and supports `abi_version`.
Impact:
- Direct implementation from current draft would introduce runtime regressions in query execution and version diagnostics.
Recommendation:
- Rewrite S2 runtime snippets to use `QueryCursor` contracts and current py-tree-sitter behavior.
- Use `abi_version` over `version` for grammar compatibility checks.

### C3. Internal Contradictions in Serializable Settings/Object Strategy
Evidence:
- Design principle says serializable settings should be formalized (`CqSettingsStruct` direction): `docs/plans/cq_search_restructuring_implementation_plan_v1_2026-02-15.md:14`.
- S5 snippet introduces plain mutable settings class explicitly deferring struct usage: `docs/plans/cq_search_restructuring_implementation_plan_v1_2026-02-15.md:1443`.
- S5 snippet imports `source_hash` from non-planned module path (`_helpers/source_hash.py`): `docs/plans/cq_search_restructuring_implementation_plan_v1_2026-02-15.md:1435`, while S1 only defines it inside `node_text.py` per `docs/plans/cq_search_restructuring_implementation_plan_v1_2026-02-15.md:322`.
Impact:
- Plan currently drifts from its own stated architecture and contains immediate import/path defects.
Recommendation:
- Define an explicit boundary policy:
- Serializable config/options: `CqSettingsStruct`
- Serializable outputs: `CqOutputStruct`
- Runtime state with handles: dataclass/plain class only
- Ensure helper module paths are canonical before migration (single source file map).

### C4. File Accounting Is Incomplete and Includes Stale/Incorrect Filenames
Evidence:
- Stale reference: `tree_sitter_tags.py` appears in delete list but file is not present (`docs/plans/cq_search_restructuring_implementation_plan_v1_2026-02-15.md:340`).
- Nonexistent contract names are listed (`tree_sitter_parse_session_contracts.py`, `tree_sitter_node_schema_contracts.py`) at `docs/plans/cq_search_restructuring_implementation_plan_v1_2026-02-15.md:761` and `docs/plans/cq_search_restructuring_implementation_plan_v1_2026-02-15.md:766`.
- Plan reports `10` root-consolidation files but enumerates `12` entries at `docs/plans/cq_search_restructuring_implementation_plan_v1_2026-02-15.md:2492`.
- Existing top-level modules are not explicitly accounted for in scope, including:
- `tools/cq/search/pipeline.py:1`
- `tools/cq/search/tree_sitter_parallel.py:1`
- `tools/cq/search/tree_sitter_query_planner.py:1`
- `tools/cq/search/tree_sitter_diagnostics.py:1`
- `tools/cq/search/tree_sitter_pack_metadata.py:1`
- Plan creates `tools/cq/search/pipeline/` package (`docs/plans/cq_search_restructuring_implementation_plan_v1_2026-02-15.md:1798`) but does not explicitly resolve existing `tools/cq/search/pipeline.py:1` module/path conflict.
Impact:
- High risk of orphaned modules, import ambiguity, and incomplete migration.
Recommendation:
- Add a full file-action manifest (every existing `.py` file marked `keep`, `move`, `split`, `merge`, `delete`).
- Block implementation start until manifest is complete and conflict-free.

### C5. Data Boundary Issue Is Misidentified; Bigger Violations Exist Elsewhere
Evidence:
- Plan flags `_PythonEnrichmentState` as mixed serializable/runtime handles: `docs/plans/cq_search_restructuring_implementation_plan_v1_2026-02-15.md:29`.
- Current `_PythonEnrichmentState` stores payload/status/timings dicts, not AST/tree handle objects: `tools/cq/search/python_enrichment.py:1865`.
- Actual mixed-boundary risk exists in request structs using `object` runtime handles inside `CqStruct`:
- `tools/cq/search/requests.py:39`
- `tools/cq/search/requests.py:54`
Impact:
- Planned remediation may optimize a lower-priority area while leaving high-value runtime/serialization boundary leakage intact.
Recommendation:
- Prioritize splitting runtime handles out of `PythonNodeEnrichmentRequest` and `PythonByteRangeEnrichmentRequest` into runtime-only companion objects.
- Keep transport-safe structs pure.

## High-Value Improvements to Existing Scope Items

### S1 Improvements
- Keep helper extraction, but separate concerns cleanly:
- text helpers (`node_text`, `sg_node_text`, `truncate`)
- hashing (`source_hash`)
- byte/position utilities
- Avoid enforcing 250-750 LOC on helper modules; small focused helpers are desirable.

### S2 Improvements
- Do not merge all tree-sitter contracts into a single `ts/contracts.py`.
- Prefer `ts/contracts/` package split by concern (`runtime`, `parse`, `query`, `diagnostics`, `injections`).
- Preserve and standardize already-implemented advanced tree-sitter features:
- bounded `QueryCursor` execution (`tools/cq/search/tree_sitter_runtime.py:111`)
- changed-range windowing (`tools/cq/search/tree_sitter_change_windows.py:34`)
- `included_ranges` injection parsing (`tools/cq/search/tree_sitter_injection_runtime.py:56`)
- query introspection for planner/specialization (`tools/cq/search/tree_sitter_query_planner.py:57`)

### S3 Improvements
- Keep existing typed `rg_events` model as a first-class contract layer; avoid regressing to lossy hand-parsed dict-only events.
- If converting `RgProcessResult`, keep strong typing for nested events and command metadata.

### S5 Improvements
- Keep current `SearchConfig`/request modeling strengths; avoid downgrading to plain mutable classes.
- Preserve behavior-first extraction from `python_enrichment.py` before package-splitting. Avoid placeholder stubs in plan text that reduce current capability.

### S6 Improvements
- Replace line-count-driven split with behavior seams:
- candidate collection
- enrichment orchestration
- classification
- summary assembly
- telemetry
- Keep current typed config baseline (`tools/cq/search/models.py:16`) rather than introducing weaker ad hoc config classes.

### S8 Improvements
- Reconsider merging `models.py`, `profiles.py`, and `requests.py` into `contracts.py`.
- Current separation supports runtime config vs output contracts; merge only when semantics align and ownership is clear.

### S11 Improvements
- Do not empty `tools/cq/search/__init__.py` until all package-level consumers are migrated.
- Add an explicit compatibility-deletion milestone with import lint checks.

## Beneficial Scope Additions (CQ-Lib-Ref Aligned)

### A1. Add a Capability Delta Matrix (What Is Already Used vs What Is New)
- The current “unused capabilities” claim at `docs/plans/cq_search_restructuring_implementation_plan_v1_2026-02-15.md:30` is partially incorrect.
- Already in use:
- TreeCursor traversal (`tools/cq/search/tree_sitter_structural_export.py:120`)
- Reusable msgspec decoders (`tools/cq/search/rg_events.py:88`)
- `msgspec.convert` in hot paths (`tools/cq/search/partition_pipeline.py:269`)
- Advanced query introspection (`tools/cq/search/tree_sitter_query_planner.py:57`)
- Add matrix columns:
- capability
- current usage file
- gap
- planned change
- acceptance test

### A2. Add Runtime-Boundary Enforcement Scope Item
- Integrate `contracts_runtime_boundary` checks into serialization boundaries (currently mostly isolated utility code).
- Add tests that fail on runtime-only keys crossing transport boundaries.

### A3. Add Pathspec-Based Include/Exclude Normalization Scope
- Current glob shaping is manual in language utilities.
- Add optional `pathspec` normalization layer for deterministic include/exclude behavior shared across lanes.

### A4. Add Diskcache Coordination Scope for Shared Hot Paths
- Where cross-process coordination is needed, prefer `Lock`/`RLock`/`BoundedSemaphore`/`barrier` primitives via existing cache backend integration, rather than bespoke synchronization.
- Keep this targeted to shared mutable resources only.

### A5. Add API-Compatibility and Artifact-Compatibility Gates
- Include golden checks for:
- CLI result shape
- search summary key set
- object view payload schema
- Add migration gate that compares pre/post outputs on representative fixtures.

## Proposed Revised Delivery Sequence
1. Baseline refresh and file-action manifest (mandatory prework).
2. Shared helper extraction and zero-behavior-change consolidation.
3. Tree-sitter packaging and contract boundary cleanup (no API deletion yet).
4. Python/Rust and pipeline package split behind compatibility imports.
5. Full callsite migration using deterministic codemods plus verification script.
6. Compatibility shim removal and final cleanup once all checks are green.

## Concrete Corrections to Apply to the Plan Document
- Correct baseline numbers and contract inventory.
- Replace incorrect API snippets (`QueryCursor` usage, `abi_version`, helper import paths).
- Fix stale filenames and ensure every existing top-level module has an explicit disposition.
- Resolve `pipeline.py` vs `pipeline/` naming conflict in scope text.
- Re-target runtime/serializable split work to actual mixed-boundary structs in `requests.py`.
- Convert no-shim policy into phased compatibility policy with deletion criteria.

## Final Assessment
The plan is directionally strong but not yet executable as written. With the corrections above, it can become a robust migration program that meets the stated goals: deduplication, stronger shared helpers/data classes, and cleaner serializable-vs-runtime boundaries using msgspec intentionally.
