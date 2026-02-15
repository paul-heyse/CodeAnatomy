# CQ Tools Consolidation Streamlining Implementation Plan v1 (2026-02-15)

## 2. Scope Summary
This plan implements a CQ-only hard cutover to aggressively consolidate `tools/cq` through shared helpers, shared data structures, unified contexts/settings factories, and targeted file merges for small modules with overlapping purpose. Design stance: no long-lived compatibility shims, no coupling to `src/`, and explicit deletion of superseded wrappers once replacement paths are landed and validated.

## 3. Design Principles
1. `tools/cq` remains operationally and structurally independent from `src/`.
2. Consolidation favors fewer, clearer modules over micro-file fragmentation when responsibilities are tightly coupled.
3. Shared request/context/settings factories are the default integration surface for CLI, run engine, bundles, and macros.
4. Msgspec contracts remain canonical at module boundaries; runtime-only objects stay out of serializable payloads.
5. Every deduplication target lands with explicit deletion scope; no permanent dual paths.
6. Large existing files should not grow further unless they are the canonical convergence point; prefer extracting reusable mid-sized modules.
7. All new implementation modules require corresponding unit tests under `tests/unit/cq/...`.

## 4. Current Baseline
- `tools/cq` currently has 305 Python files with 138 files at `<=80` lines, with highest small-file density in `tools/cq/core/services`, `tools/cq/macros/common`, `tools/cq/search/tree_sitter/schema`, `tools/cq/search/tree_sitter/core`, and `tools/cq/search/tree_sitter/structural`.
- Service wrappers are split into tiny modules (`tools/cq/core/services/search_service.py`, `tools/cq/core/services/calls_service.py`, `tools/cq/core/services/entity_service.py`) and re-aggregated by `tools/cq/core/services/__init__.py`.
- Macro common wrappers are near-empty re-export shims (`tools/cq/macros/common/contracts.py`, `tools/cq/macros/common/scoring.py`, `tools/cq/macros/common/targets.py`).
- Rust fallback logic is wrapped per macro via duplicate `_apply_rust_fallback` functions in `tools/cq/macros/calls.py`, `tools/cq/macros/impact.py`, `tools/cq/macros/imports.py`, `tools/cq/macros/exceptions.py`, `tools/cq/macros/sig_impact.py`, `tools/cq/macros/side_effects.py`, `tools/cq/macros/scopes.py`, and `tools/cq/macros/bytecode.py`.
- CLI step modeling is duplicated between `tools/cq/cli_app/contracts.py` and canonical `tools/cq/run/spec.py`.
- CLI text/protocol output helpers are duplicated across `tools/cq/cli_app/commands/ldmd.py` and `tools/cq/cli_app/commands/artifact.py`.
- Tree-sitter schema/query/structural helpers are split into multiple thin files with low fan-out and high coupling (`tools/cq/search/tree_sitter/schema/*`, `tools/cq/search/tree_sitter/query/*`, `tools/cq/search/tree_sitter/structural/*`).
- Python/Rust helper duplication exists for text extraction and AST call parsing (`_node_text`, `_safe_unparse`, `_get_call_name`) across `tools/cq/search/tree_sitter/*`, `tools/cq/macros/*`, and `tools/cq/index/*`.
- Environment-driven settings parsing is fragmented across `tools/cq/core/runtime/execution_policy.py`, `tools/cq/core/cache/policy.py`, `tools/cq/core/cache/cache_runtime_tuning.py`, and `tools/cq/search/tree_sitter/core/parser_controls.py`.

## 5. Per-Scope-Item Plan

## S1. Consolidate Core Service Wrappers into One Module
### Goal
Replace tiny split service modules with a single `tools/cq/core/services.py` that defines service request contracts and service adapters in one cohesive file. This removes unnecessary import indirection and reduces structural overhead.

### Representative Code Snippets
```python
# tools/cq/core/services.py
from __future__ import annotations

from pathlib import Path

from tools.cq.core.schema import CqResult
from tools.cq.core.structs import CqStruct
from tools.cq.core.toolchain import Toolchain
from tools.cq.query.language import QueryLanguageScope
from tools.cq.search.pipeline.classifier import QueryMode
from tools.cq.search.pipeline.profiles import SearchLimits
from tools.cq.search.pipeline.smart_search import smart_search


class SearchServiceRequest(CqStruct, frozen=True):
    root: Path
    query: str
    mode: QueryMode | None = None
    lang_scope: QueryLanguageScope = "auto"
    include_globs: list[str] | None = None
    exclude_globs: list[str] | None = None
    include_strings: bool = False
    with_neighborhood: bool = False
    limits: SearchLimits | None = None
    tc: Toolchain | None = None
    argv: list[str] | None = None
    run_id: str | None = None


class SearchService:
    @staticmethod
    def execute(request: SearchServiceRequest) -> CqResult:
        return smart_search(
            root=request.root,
            query=request.query,
            mode=request.mode,
            lang_scope=request.lang_scope,
            include_globs=request.include_globs,
            exclude_globs=request.exclude_globs,
            include_strings=request.include_strings,
            with_neighborhood=request.with_neighborhood,
            limits=request.limits,
            tc=request.tc,
            argv=request.argv,
            run_id=request.run_id,
        )
```

### Files to Edit
- `tools/cq/core/bootstrap.py`
- `tools/cq/core/ports.py`
- `tools/cq/core/bundles.py`
- `tools/cq/query/merge.py`
- `tools/cq/query/executor.py`
- `tools/cq/run/runner.py`
- `tools/cq/cli_app/commands/analysis.py`
- `tools/cq/cli_app/commands/query.py`
- `tools/cq/cli_app/commands/search.py`

### New Files to Create
- `tools/cq/core/services.py`
- `tests/unit/cq/core/test_services.py`

### Legacy Decommission/Delete Scope
- Delete `tools/cq/core/services/search_service.py` (replaced by unified module).
- Delete `tools/cq/core/services/calls_service.py` (replaced by unified module).
- Delete `tools/cq/core/services/entity_service.py` (replaced by unified module).
- Delete `tools/cq/core/services/__init__.py` package aggregator (no longer needed after flat module migration).

---

## S2. Remove Wrapper-Only Macro Common Package
### Goal
Eliminate `tools/cq/macros/common/*` wrapper modules and import canonical macro contracts/helpers directly from their source modules to reduce empty indirection.

### Representative Code Snippets
```python
# tools/cq/macros/<consumer>.py
# before:
# from tools.cq.macros.common.scoring import macro_score_payload
#
# after:
from tools.cq.macros.scoring_utils import macro_score_payload
from tools.cq.macros.target_resolution import resolve_target_files
from tools.cq.macros.contracts import ScopedMacroRequestBase
```

### Files to Edit
- `tools/cq/macros/__init__.py`
- `tools/cq/macros/calls.py`
- `tools/cq/macros/impact.py`
- `tools/cq/macros/imports.py`
- `tools/cq/macros/exceptions.py`
- `tools/cq/macros/sig_impact.py`
- `tools/cq/macros/side_effects.py`
- `tools/cq/macros/scopes.py`
- `tools/cq/macros/bytecode.py`

### New Files to Create
- None.

### Legacy Decommission/Delete Scope
- Delete `tools/cq/macros/common/contracts.py` (pure re-export shim).
- Delete `tools/cq/macros/common/scoring.py` (pure re-export shim).
- Delete `tools/cq/macros/common/targets.py` (pure re-export shim).
- Delete `tools/cq/macros/common/__init__.py` (package marker no longer needed).

---

## S3. Consolidate Rust Fallback Policy and Per-Macro Wrappers
### Goal
Replace duplicated per-macro `_apply_rust_fallback` wrappers with one policy-driven helper that captures macro name, pattern/query derivation, and fallback match extraction in a canonical struct.

### Representative Code Snippets
```python
# tools/cq/macros/rust_fallback_policy.py
from __future__ import annotations

from tools.cq.core.structs import CqStruct
from tools.cq.core.schema import CqResult
from tools.cq.macros.multilang_fallback import apply_rust_macro_fallback


class RustFallbackPolicyV1(CqStruct, frozen=True):
    macro_name: str
    pattern: str
    query: str | None = None
    fallback_matches_summary_key: str | None = None


def apply_rust_fallback_policy(result: CqResult, *, root: str | object, policy: RustFallbackPolicyV1) -> CqResult:
    summary = result.summary if isinstance(result.summary, dict) else {}
    fallback_matches = (
        int(summary.get(policy.fallback_matches_summary_key, 0))
        if policy.fallback_matches_summary_key
        else 0
    )
    return apply_rust_macro_fallback(
        result=result,
        root=root,  # typed as Path at callsite
        pattern=policy.pattern,
        macro_name=policy.macro_name,
        query=policy.query,
        fallback_matches=fallback_matches,
    )
```

### Files to Edit
- `tools/cq/macros/calls.py`
- `tools/cq/macros/impact.py`
- `tools/cq/macros/imports.py`
- `tools/cq/macros/exceptions.py`
- `tools/cq/macros/sig_impact.py`
- `tools/cq/macros/side_effects.py`
- `tools/cq/macros/scopes.py`
- `tools/cq/macros/bytecode.py`
- `tools/cq/macros/multilang_fallback.py`

### New Files to Create
- `tools/cq/macros/rust_fallback_policy.py`
- `tests/unit/cq/macros/test_rust_fallback_policy.py`

### Legacy Decommission/Delete Scope
- Delete `_apply_rust_fallback` from `tools/cq/macros/calls.py`.
- Delete `_apply_rust_fallback` from `tools/cq/macros/impact.py`.
- Delete `_apply_rust_fallback` from `tools/cq/macros/imports.py`.
- Delete `_apply_rust_fallback` from `tools/cq/macros/exceptions.py`.
- Delete `_apply_rust_fallback` from `tools/cq/macros/sig_impact.py`.
- Delete `_apply_rust_fallback` from `tools/cq/macros/side_effects.py`.
- Delete `_apply_rust_fallback` from `tools/cq/macros/scopes.py`.
- Delete `_apply_rust_fallback` from `tools/cq/macros/bytecode.py`.

---

## S4. Introduce Unified Macro/Service Request Factory
### Goal
Create a single request-construction layer for CLI commands, run-step execution, and report bundles. This removes repeated request wiring and ensures option-to-request mapping remains consistent.

### Representative Code Snippets
```python
# tools/cq/core/request_factory.py
from __future__ import annotations

from pathlib import Path

from tools.cq.core.structs import CqStruct
from tools.cq.core.toolchain import Toolchain
from tools.cq.macros.impact import ImpactRequest
from tools.cq.macros.imports import ImportRequest
from tools.cq.macros.sig_impact import SigImpactRequest
from tools.cq.core.services import CallsServiceRequest, SearchServiceRequest


class RequestContextV1(CqStruct, frozen=True):
    root: Path
    argv: list[str]
    tc: Toolchain


class RequestFactory:
    @staticmethod
    def calls(ctx: RequestContextV1, *, function_name: str) -> CallsServiceRequest:
        return CallsServiceRequest(root=ctx.root, function_name=function_name, tc=ctx.tc, argv=ctx.argv)

    @staticmethod
    def impact(ctx: RequestContextV1, *, function_name: str, param_name: str, depth: int) -> ImpactRequest:
        return ImpactRequest(
            tc=ctx.tc,
            root=ctx.root,
            argv=ctx.argv,
            function_name=function_name,
            param_name=param_name,
            max_depth=depth,
        )
```

### Files to Edit
- `tools/cq/cli_app/commands/analysis.py`
- `tools/cq/cli_app/commands/search.py`
- `tools/cq/run/runner.py`
- `tools/cq/core/bundles.py`
- `tools/cq/cli_app/commands/query.py`

### New Files to Create
- `tools/cq/core/request_factory.py`
- `tests/unit/cq/core/test_request_factory.py`

### Legacy Decommission/Delete Scope
- Delete repeated direct request-construction blocks in `tools/cq/cli_app/commands/analysis.py` once factory calls replace them.
- Delete repeated direct request-construction blocks in `tools/cq/run/runner.py` once factory calls replace them.
- Delete repeated direct request-construction blocks in `tools/cq/core/bundles.py` once factory calls replace them.

---

## S5. Add Canonical Error Result Factory
### Goal
Standardize command error result creation (`RunContext`, `RunMeta`, `mk_result`, summary error payload) into one helper, eliminating repeated ad hoc error response assembly.

### Representative Code Snippets
```python
# tools/cq/core/result_factory.py
from __future__ import annotations

from pathlib import Path

from tools.cq.core.run_context import RunContext
from tools.cq.core.schema import CqResult, mk_result
from tools.cq.core.toolchain import Toolchain


def build_error_result(
    *,
    macro: str,
    root: Path,
    argv: list[str],
    tc: Toolchain | None,
    started_ms: float,
    error: Exception | str,
) -> CqResult:
    run_ctx = RunContext.from_parts(root=root, argv=argv, tc=tc, started_ms=started_ms)
    result = mk_result(run_ctx.to_runmeta(macro))
    result.summary["error"] = str(error)
    return result
```

### Files to Edit
- `tools/cq/cli_app/commands/query.py`
- `tools/cq/cli_app/commands/report.py`
- `tools/cq/cli_app/commands/run.py`
- `tools/cq/run/runner.py`
- `tools/cq/query/executor.py`

### New Files to Create
- `tools/cq/core/result_factory.py`
- `tests/unit/cq/core/test_result_factory.py`

### Legacy Decommission/Delete Scope
- Delete repeated `mk_result(...); result.summary["error"]=...` patterns from:
- `tools/cq/cli_app/commands/query.py`
- `tools/cq/cli_app/commands/report.py`
- `tools/cq/cli_app/commands/run.py`
- `tools/cq/run/runner.py`
- `tools/cq/query/executor.py`

---

## S6. Unify CLI Step Parsing with Canonical RunStep Union
### Goal
Remove duplicated step payload dataclasses in CLI and decode `--step/--steps` directly into canonical `RunStep` from `tools/cq/run/spec.py`.

### Representative Code Snippets
```python
# tools/cq/cli_app/params.py
from tools.cq.run.spec import RunStep
from tools.cq.core.typed_boundary import decode_json_strict


def parse_run_step_json(raw: str) -> RunStep:
    return decode_json_strict(raw, type_=RunStep)
```

```python
# tools/cq/run/loader.py
from tools.cq.run.spec import RunStep, coerce_run_step

def _coerce_step(item: object) -> RunStep:
    return coerce_run_step(item)
```

### Files to Edit
- `tools/cq/cli_app/params.py`
- `tools/cq/cli_app/options.py`
- `tools/cq/run/loader.py`
- `tools/cq/cli_app/commands/run.py`

### New Files to Create
- `tools/cq/run/step_decode.py`
- `tests/unit/cq/run/test_step_decode.py`

### Legacy Decommission/Delete Scope
- Delete `tools/cq/cli_app/contracts.py` (`*StepCli` dataclass duplicates).
- Delete `RunStepCli` usage in `tools/cq/cli_app/params.py`.
- Delete dataclass-to-dict coercion bridging in `tools/cq/run/loader.py` once canonical decode is complete.

---

## S7. Consolidate CLI Protocol Output Helpers
### Goal
Unify `_text_result`, `_wants_json`, and JSON/text rendering behavior used by `ldmd`, `artifact`, and `admin` protocol commands into one shared helper module.

### Representative Code Snippets
```python
# tools/cq/cli_app/protocol_output.py
from __future__ import annotations

import json

from tools.cq.cli_app.context import CliContext, CliResult, CliTextResult
from tools.cq.cli_app.types import OutputFormat


def wants_json(ctx: CliContext) -> bool:
    return ctx.output_format == OutputFormat.json


def text_result(
    ctx: CliContext,
    text: str,
    *,
    media_type: str = "text/plain",
    exit_code: int = 0,
) -> CliResult:
    return CliResult(
        result=CliTextResult(text=text, media_type=media_type),
        context=ctx,
        exit_code=exit_code,
        filters=None,
    )


def json_result(ctx: CliContext, payload: object, *, exit_code: int = 0) -> CliResult:
    return text_result(ctx, json.dumps(payload, indent=2), media_type="application/json", exit_code=exit_code)
```

### Files to Edit
- `tools/cq/cli_app/commands/ldmd.py`
- `tools/cq/cli_app/commands/artifact.py`
- `tools/cq/cli_app/commands/admin.py`

### New Files to Create
- `tools/cq/cli_app/protocol_output.py`
- `tests/unit/cq/cli/test_protocol_output.py`

### Legacy Decommission/Delete Scope
- Delete local `_text_result` and `_wants_json` in `tools/cq/cli_app/commands/ldmd.py`.
- Delete local `_text_result` and `_wants_json` in `tools/cq/cli_app/commands/artifact.py`.
- Delete local `_emit_payload` in `tools/cq/cli_app/commands/admin.py`.

---

## S8. Merge Tree-Sitter Schema Micro-Modules
### Goal
Consolidate schema runtime wrappers and generated re-exports into a larger cohesive module centered on `node_schema` to reduce tiny-file fragmentation.

### Representative Code Snippets
```python
# tools/cq/search/tree_sitter/schema/node_schema.py
from __future__ import annotations

from tools.cq.core.structs import CqStruct


class GrammarNodeTypeV1(CqStruct, frozen=True):
    type: str
    named: bool
    fields: tuple[str, ...] = ()


def build_runtime_ids(language: object) -> dict[str, int]:
    out: dict[str, int] = {}
    for kind in ("identifier", "call", "function_definition"):
        try:
            out[kind] = int(language.id_for_node_kind(kind, True))
        except (RuntimeError, TypeError, ValueError, AttributeError):
            continue
    return out
```

### Files to Edit
- `tools/cq/search/tree_sitter/schema/node_schema.py`
- `tools/cq/search/tree_sitter/core/language_registry.py`
- `tools/cq/search/tree_sitter/query/lint.py`
- `tools/cq/search/tree_sitter/query/grammar_drift.py`

### New Files to Create
- `tests/unit/cq/search/tree_sitter/schema/test_node_schema_runtime.py`

### Legacy Decommission/Delete Scope
- Delete `tools/cq/search/tree_sitter/schema/generated.py` (re-export only).
- Delete `tools/cq/search/tree_sitter/schema/runtime.py` (small runtime helper shard merged).
- Delete `tools/cq/search/tree_sitter/schema/__init__.py` if it remains marker-only and unused after consolidation.

---

## S9. Merge Tree-Sitter Structural Micro-Modules
### Goal
Consolidate structural export functionality (query hits, tokens, diagnostics, match rows) into a single larger structural runtime module with internal sections.

### Representative Code Snippets
```python
# tools/cq/search/tree_sitter/structural/runtime.py
from __future__ import annotations

from tools.cq.search.tree_sitter.contracts.core_models import (
    ObjectEvidenceRowV1,
    TreeSitterDiagnosticV1,
    TreeSitterQueryHitV1,
)
from tools.cq.search.tree_sitter.query.pack_metadata import first_capture, pattern_settings


def build_match_rows_with_hits(
    *,
    query: object,
    matches: list[tuple[int, dict[str, list[object]]]],
    source_bytes: bytes,
    query_name: str,
) -> tuple[tuple[ObjectEvidenceRowV1, ...], tuple[TreeSitterQueryHitV1, ...]]:
    # canonical merged implementation path for rows + hits
    ...


def collect_diagnostic_rows(...) -> tuple[TreeSitterDiagnosticV1, ...]:
    ...
```

### Files to Edit
- `tools/cq/search/tree_sitter/python_lane/facts.py`
- `tools/cq/search/tree_sitter/rust_lane/runtime.py`
- `tools/cq/neighborhood/tree_sitter_collector.py`
- `tools/cq/search/tree_sitter/structural/export.py`

### New Files to Create
- `tools/cq/search/tree_sitter/structural/runtime.py`
- `tests/unit/cq/search/tree_sitter/structural/test_runtime.py`

### Legacy Decommission/Delete Scope
- Delete `tools/cq/search/tree_sitter/structural/diagnostic_export.py`.
- Delete `tools/cq/search/tree_sitter/structural/query_hits.py`.
- Delete `tools/cq/search/tree_sitter/structural/token_export.py`.
- Delete `tools/cq/search/tree_sitter/structural/match_rows.py`.

---

## S10. Merge Query Drift/Contract Metadata Modules
### Goal
Consolidate query-pack drift, schema diff, and metadata helpers into one query contracts runtime module to eliminate low-value sharding.

### Representative Code Snippets
```python
# tools/cq/search/tree_sitter/query/contracts_runtime.py
from __future__ import annotations

import hashlib

from tools.cq.core.structs import CqStruct


class GrammarDiffV1(CqStruct, frozen=True):
    added_node_kinds: tuple[str, ...] = ()
    removed_node_kinds: tuple[str, ...] = ()
    added_fields: tuple[str, ...] = ()
    removed_fields: tuple[str, ...] = ()


def diff_schema(old_index: object, new_index: object) -> GrammarDiffV1:
    ...


def build_contract_snapshot(...) -> object:
    ...


def build_grammar_drift_report(...) -> object:
    ...
```

### Files to Edit
- `tools/cq/search/tree_sitter/query/lint.py`
- `tools/cq/search/tree_sitter/query/registry.py`
- `tools/cq/search/tree_sitter/query/planner.py`
- `tools/cq/search/tree_sitter/query/__init__.py`

### New Files to Create
- `tools/cq/search/tree_sitter/query/contracts_runtime.py`
- `tests/unit/cq/search/tree_sitter/query/test_contracts_runtime.py`

### Legacy Decommission/Delete Scope
- Delete `tools/cq/search/tree_sitter/query/drift_diff.py`.
- Delete `tools/cq/search/tree_sitter/query/contract_snapshot.py`.
- Delete `tools/cq/search/tree_sitter/query/grammar_drift.py`.
- Delete `tools/cq/search/tree_sitter/query/pack_metadata.py`.

---

## S11. Merge Rust Injection Settings/Profiles into Injections Runtime
### Goal
Collapse Rust lane injection settings/profile split into one module so parse plan logic, pattern settings, and profile resolution are co-located.

### Representative Code Snippets
```python
# tools/cq/search/tree_sitter/rust_lane/injections.py
from __future__ import annotations

from tools.cq.core.structs import CqStruct


class InjectionSettingsV1(CqStruct, frozen=True):
    language: str | None = None
    combined: bool = False
    include_children: bool = False
    use_self_language: bool = False
    use_parent_language: bool = False


class RustInjectionProfileV1(CqStruct, frozen=True):
    profile_name: str
    language: str
    combined: bool = False
    macro_names: tuple[str, ...] = ()


def settings_for_pattern(query: object, pattern_idx: int) -> InjectionSettingsV1:
    ...


def resolve_rust_injection_profile(macro_name: str | None) -> RustInjectionProfileV1:
    ...
```

### Files to Edit
- `tools/cq/search/tree_sitter/rust_lane/injections.py`
- `tools/cq/search/tree_sitter/rust_lane/runtime.py`
- `tools/cq/search/tree_sitter/rust_lane/injection_runtime.py`

### New Files to Create
- `tests/unit/cq/search/tree_sitter/rust_lane/test_injections_runtime.py`

### Legacy Decommission/Delete Scope
- Delete `tools/cq/search/tree_sitter/rust_lane/injection_settings.py`.
- Delete `tools/cq/search/tree_sitter/rust_lane/injection_profiles.py`.

---

## S12. Consolidate Python Enrichment Small Support Modules
### Goal
Merge low-line-count Python enrichment helper modules into larger support/contracts modules to reduce surface area and simplify internal ownership.

### Representative Code Snippets
```python
# tools/cq/search/python/support.py
from __future__ import annotations

from collections.abc import Mapping


def build_agreement_summary(
    *,
    ast_grep_fields: Mapping[str, object] | None = None,
    native_fields: Mapping[str, object] | None = None,
    tree_sitter_fields: Mapping[str, object] | None = None,
) -> dict[str, object]:
    ...


def evaluate_python_semantic_signal_from_mapping(payload: Mapping[str, object]) -> tuple[bool, tuple[str, ...]]:
    ...


def extract_signature_stage(node: object) -> dict[str, object]:
    ...
```

```python
# tools/cq/search/python/contracts.py
from __future__ import annotations

import msgspec

from tools.cq.core.structs import CqOutputStruct


class PythonResolutionPayloadV1(CqOutputStruct, frozen=True):
    symbol: str | None = None
    symbol_role: str | None = None
    qualified_name_candidates: tuple[dict[str, object], ...] = msgspec.field(default_factory=tuple)
```

### Files to Edit
- `tools/cq/search/python/extractors.py`
- `tools/cq/search/python/resolution_index.py`
- `tools/cq/search/python/analysis_session.py`
- `tools/cq/search/python/resolution_support.py`
- `tools/cq/search/python/__init__.py`

### New Files to Create
- `tools/cq/search/python/support.py`
- `tools/cq/search/python/contracts.py`
- `tests/unit/cq/search/python/test_support.py`
- `tests/unit/cq/search/python/test_contracts.py`

### Legacy Decommission/Delete Scope
- Delete `tools/cq/search/python/stages.py`.
- Delete `tools/cq/search/python/agreement.py`.
- Delete `tools/cq/search/python/semantic_signal.py`.
- Delete `tools/cq/search/python/orchestrator.py`.
- Delete `tools/cq/search/python/resolution_payload.py`.
- Delete `tools/cq/search/python/ast_grep_rules.py` if helper content is merged into `support.py`.

---

## S13. Consolidate Cache Contract Shards and Settings Factories
### Goal
Unify cache/runtime/parser env-derived settings into factory modules and fold tiny contract shards into parent cache modules for clearer ownership.

### Representative Code Snippets
```python
# tools/cq/core/settings/factory.py
from __future__ import annotations

from pathlib import Path

from tools.cq.core.cache.policy import CqCachePolicyV1, default_cache_policy
from tools.cq.core.runtime.execution_policy import RuntimeExecutionPolicy, default_runtime_execution_policy
from tools.cq.search.tree_sitter.core.parser_controls import ParserControlSettingsV1, parser_controls_from_env


class SettingsFactory:
    @staticmethod
    def runtime_policy() -> RuntimeExecutionPolicy:
        return default_runtime_execution_policy()

    @staticmethod
    def cache_policy(*, root: Path) -> CqCachePolicyV1:
        return default_cache_policy(root=root)

    @staticmethod
    def parser_controls() -> ParserControlSettingsV1:
        return parser_controls_from_env()
```

### Files to Edit
- `tools/cq/core/runtime/execution_policy.py`
- `tools/cq/core/cache/policy.py`
- `tools/cq/core/cache/cache_runtime_tuning.py`
- `tools/cq/core/bootstrap.py`
- `tools/cq/core/cache/__init__.py`
- `tools/cq/search/tree_sitter/core/parse.py`
- `tools/cq/neighborhood/tree_sitter_collector.py`

### New Files to Create
- `tools/cq/core/settings/factory.py`
- `tests/unit/cq/core/test_settings_factory.py`

### Legacy Decommission/Delete Scope
- Delete `tools/cq/core/cache/coordination_contracts.py` after moving `LaneCoordinationPolicyV1` into `tools/cq/core/cache/coordination.py`.
- Delete `tools/cq/core/cache/maintenance_contracts.py` after moving `CacheMaintenanceSnapshotV1` into `tools/cq/core/cache/maintenance.py`.
- Delete `tools/cq/core/cache/cache_runtime_tuning_contracts.py` after moving `CacheRuntimeTuningV1` into `tools/cq/core/cache/cache_runtime_tuning.py`.
- Delete `tools/cq/core/cache/tree_sitter_blob_store_contracts.py` after moving `TreeSitterBlobRefV1` into `tools/cq/core/cache/tree_sitter_blob_store.py`.
- Delete `tools/cq/core/cache/tree_sitter_cache_store_contracts.py` after moving `TreeSitterCacheEnvelopeV1` into `tools/cq/core/cache/tree_sitter_cache_store.py`.

---

## S14. Consolidate Shared Python AST Helper Utilities
### Goal
Create one canonical helper module for Python AST text/call parsing/span extraction and migrate duplicate helpers from macros and index modules.

### Representative Code Snippets
```python
# tools/cq/core/python_ast_utils.py
from __future__ import annotations

import ast


def safe_unparse(node: ast.AST, *, default: str) -> str:
    try:
        return ast.unparse(node)
    except (ValueError, TypeError):
        return default


def get_call_name(func: ast.expr) -> tuple[str, bool, str | None]:
    if isinstance(func, ast.Name):
        return func.id, False, None
    if isinstance(func, ast.Attribute):
        if isinstance(func.value, ast.Name):
            receiver = func.value.id
            return f"{receiver}.{func.attr}", True, receiver
        return func.attr, True, None
    return "", False, None
```

### Files to Edit
- `tools/cq/macros/calls.py`
- `tools/cq/macros/calls_target.py`
- `tools/cq/macros/exceptions.py`
- `tools/cq/macros/side_effects.py`
- `tools/cq/index/call_resolver.py`
- `tools/cq/index/def_index.py`
- `tools/cq/search/python/analysis_session.py`
- `tools/cq/search/python/resolution_support.py`

### New Files to Create
- `tools/cq/core/python_ast_utils.py`
- `tests/unit/cq/core/test_python_ast_utils.py`

### Legacy Decommission/Delete Scope
- Delete duplicate `_safe_unparse` in:
- `tools/cq/macros/calls.py`
- `tools/cq/macros/exceptions.py`
- `tools/cq/macros/side_effects.py`
- `tools/cq/index/call_resolver.py`
- `tools/cq/index/def_index.py`
- Delete duplicate `_get_call_name` in:
- `tools/cq/macros/calls.py`
- `tools/cq/macros/calls_target.py`
- `tools/cq/index/call_resolver.py`
- Delete duplicate `_node_byte_span` / `_iter_nodes_with_parents` overlap between `tools/cq/search/python/analysis_session.py` and `tools/cq/search/python/resolution_support.py` by using shared helpers.

---

## S15. Consolidate Tree-Sitter Node Text/Span Utilities
### Goal
Unify tree-sitter node text/span/point extraction across neighborhood, lane runtime, tags, and structural modules by centralizing in one helper module and deleting local wrappers.

### Representative Code Snippets
```python
# tools/cq/search/tree_sitter/core/node_utils.py
from __future__ import annotations

from typing import Protocol


class NodeLike(Protocol):
    start_byte: int
    end_byte: int
    start_point: tuple[int, int]
    end_point: tuple[int, int]


def node_text(node: NodeLike | None, source_bytes: bytes, *, strip: bool = True, max_len: int | None = None) -> str:
    if node is None:
        return ""
    start = int(getattr(node, "start_byte", 0))
    end = int(getattr(node, "end_byte", start))
    if end <= start:
        return ""
    text = source_bytes[start:end].decode("utf-8", errors="replace")
    out = text.strip() if strip else text
    if max_len is not None and len(out) >= max_len:
        return out[: max(1, max_len - 3)] + "..."
    return out


def node_byte_span(node: NodeLike | None) -> tuple[int, int]:
    if node is None:
        return 0, 0
    start = int(getattr(node, "start_byte", 0))
    end = int(getattr(node, "end_byte", start))
    return start, max(start, end)
```

### Files to Edit
- `tools/cq/search/tree_sitter/core/text_utils.py`
- `tools/cq/search/tree_sitter/python_lane/runtime.py`
- `tools/cq/search/tree_sitter/python_lane/facts.py`
- `tools/cq/search/tree_sitter/python_lane/fallback_support.py`
- `tools/cq/search/tree_sitter/python_lane/locals_index.py`
- `tools/cq/search/tree_sitter/rust_lane/runtime.py`
- `tools/cq/search/tree_sitter/rust_lane/injections.py`
- `tools/cq/search/tree_sitter/tags/runtime.py`
- `tools/cq/neighborhood/tree_sitter_collector.py`
- `tools/cq/neighborhood/tree_sitter_neighborhood_query_engine.py`
- `tools/cq/search/tree_sitter/structural/export.py`

### New Files to Create
- `tools/cq/search/tree_sitter/core/node_utils.py`
- `tests/unit/cq/search/tree_sitter/core/test_node_utils.py`

### Legacy Decommission/Delete Scope
- Delete duplicated local `_node_text` implementations from:
- `tools/cq/search/tree_sitter/python_lane/runtime.py`
- `tools/cq/search/tree_sitter/python_lane/facts.py`
- `tools/cq/search/tree_sitter/python_lane/fallback_support.py`
- `tools/cq/search/tree_sitter/python_lane/locals_index.py`
- `tools/cq/search/tree_sitter/rust_lane/runtime.py`
- `tools/cq/search/tree_sitter/rust_lane/injections.py`
- `tools/cq/search/tree_sitter/tags/runtime.py`
- `tools/cq/neighborhood/tree_sitter_collector.py`
- `tools/cq/neighborhood/tree_sitter_neighborhood_query_engine.py`
- Delete overlapping node-text helper implementations in `tools/cq/search/_shared/core.py` and keep tree-sitter path canonicalized in node utils.

---

## 6. Cross-Scope Legacy Decommission and Deletion Plan

### Batch D1 (after S1, S4, S5, S6, S7)
- Delete split services package files (`tools/cq/core/services/search_service.py`, `tools/cq/core/services/calls_service.py`, `tools/cq/core/services/entity_service.py`, `tools/cq/core/services/__init__.py`) after all callsites adopt unified services and request factory.
- Delete `tools/cq/cli_app/contracts.py` only after CLI run-step parsing fully consumes `RunStep` from `tools/cq/run/spec.py`.
- Delete command-local text/protocol helpers from `tools/cq/cli_app/commands/ldmd.py`, `tools/cq/cli_app/commands/artifact.py`, and `tools/cq/cli_app/commands/admin.py` once shared protocol output module is live.

### Batch D2 (after S8, S9, S10, S11, S15)
- Delete tree-sitter schema wrappers (`tools/cq/search/tree_sitter/schema/generated.py`, `tools/cq/search/tree_sitter/schema/runtime.py`) after consolidated schema runtime usage is complete.
- Delete tree-sitter structural micro-modules (`tools/cq/search/tree_sitter/structural/diagnostic_export.py`, `tools/cq/search/tree_sitter/structural/query_hits.py`, `tools/cq/search/tree_sitter/structural/token_export.py`, `tools/cq/search/tree_sitter/structural/match_rows.py`) once runtime module replaces imports.
- Delete query drift metadata shards (`tools/cq/search/tree_sitter/query/drift_diff.py`, `tools/cq/search/tree_sitter/query/contract_snapshot.py`, `tools/cq/search/tree_sitter/query/grammar_drift.py`, `tools/cq/search/tree_sitter/query/pack_metadata.py`) after merged contracts runtime cutover.
- Delete Rust injection split files (`tools/cq/search/tree_sitter/rust_lane/injection_settings.py`, `tools/cq/search/tree_sitter/rust_lane/injection_profiles.py`) once merged injections runtime is adopted.

### Batch D3 (after S2, S3, S4, S14)
- Delete macro wrapper-only package `tools/cq/macros/common/*` after direct imports and request/fallback factories are active.
- Delete per-macro `_apply_rust_fallback` functions across all macro modules once policy helper is canonical.
- Delete duplicate AST helper implementations (`_safe_unparse`, `_get_call_name`) from macro/index modules once shared AST utilities are wired.

### Batch D4 (after S12, S13, S14, S15)
- Delete Python enrichment helper shards (`tools/cq/search/python/stages.py`, `tools/cq/search/python/agreement.py`, `tools/cq/search/python/semantic_signal.py`, `tools/cq/search/python/orchestrator.py`, `tools/cq/search/python/resolution_payload.py`, optional `tools/cq/search/python/ast_grep_rules.py`) after consolidated support/contracts migration.
- Delete tiny cache contract shard modules (`tools/cq/core/cache/coordination_contracts.py`, `tools/cq/core/cache/maintenance_contracts.py`, `tools/cq/core/cache/cache_runtime_tuning_contracts.py`, `tools/cq/core/cache/tree_sitter_blob_store_contracts.py`, `tools/cq/core/cache/tree_sitter_cache_store_contracts.py`) after parent-module consolidation and import migration.

## 7. Implementation Sequence
1. Land S5 first to standardize error-result generation and reduce repeated failure-path code before broader rewiring.
2. Land S1 next so service import paths stabilize early for subsequent factory and run/CLI work.
3. Land S4 to centralize request construction before touching many command/run callsites.
4. Land S6 to remove run-step contract duplication while S4 paths are still fresh.
5. Land S7 to normalize CLI protocol output and simplify CLI command modules.
6. Land S3 once request/service paths are stable; this removes repeated fallback wrappers with low integration risk.
7. Land S14 before S12/S15 so shared AST primitives are available for consolidation.
8. Land S15 next to remove tree-sitter node helper duplication used across multiple remaining scope items.
9. Land S11, then S8, then S10 to incrementally collapse tree-sitter configuration/query internals without breaking runtime behavior.
10. Land S9 after S15/S10 so structural runtime consolidation can consume canonical helpers and query metadata APIs.
11. Land S13 to unify settings/env parsing and cache contract ownership after core call paths stabilize.
12. Land S12 last in feature modules, since it depends on helper consolidation and has many touch points in Python enrichment internals.
13. Execute D1-D4 decommission batches in order, only after prerequisite scope items are completed and tested.

## 8. Implementation Checklist
- [ ] S1. Consolidate core service wrappers into one module.
- [ ] S2. Remove wrapper-only macro common package.
- [ ] S3. Consolidate Rust fallback policy and per-macro wrappers.
- [ ] S4. Introduce unified macro/service request factory.
- [ ] S5. Add canonical error result factory.
- [ ] S6. Unify CLI step parsing with canonical RunStep union.
- [ ] S7. Consolidate CLI protocol output helpers.
- [ ] S8. Merge tree-sitter schema micro-modules.
- [ ] S9. Merge tree-sitter structural micro-modules.
- [ ] S10. Merge query drift/contract metadata modules.
- [ ] S11. Merge Rust injection settings/profiles into injections runtime.
- [ ] S12. Consolidate Python enrichment small support modules.
- [ ] S13. Consolidate cache contract shards and settings factories.
- [ ] S14. Consolidate shared Python AST helper utilities.
- [ ] S15. Consolidate tree-sitter node text/span utilities.
- [ ] Batch D1 decommission (after S1, S4, S5, S6, S7).
- [ ] Batch D2 decommission (after S8, S9, S10, S11, S15).
- [ ] Batch D3 decommission (after S2, S3, S4, S14).
- [ ] Batch D4 decommission (after S12, S13, S14, S15).
