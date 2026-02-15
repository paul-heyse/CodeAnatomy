# CQ Module Consolidation Implementation Plan v1 (2026-02-15)

## Scope Summary

Comprehensive consolidation of `tools/cq/` (305 files, 69,391 LOC) through small-file merging,
shared utility extraction, package flattening, deduplication of cross-module helper functions,
unified request/result factories, and canonical policy helpers. The goal is to reduce file count
by ~60-65 files while improving discoverability, reducing import complexity, eliminating re-export
facades, and removing duplicated logic (Rust fallback wrappers, AST helpers, node text extractors,
error result assembly, CLI step parsing).

**Design stance:** Hard cutover per scope item — no compatibility shims or re-export stubs. All
importers updated atomically within each scope item. Tests must pass after each scope item lands.

**Out of scope:** Large-file decomposition (smart_search.py 3,829 LOC, executor.py 3,235 LOC,
calls.py 2,310 LOC) — these are coherent single-responsibility modules that don't benefit from
splitting. Also out of scope: search file restructuring (covered in `cq_search_file_consolidation_implementation_plan_v1`).

## Design Principles

1. **Merge small files into their natural home** — files ≤60 LOC that contain a single class or
   function should merge into the module they most tightly couple with.
2. **Flatten single-purpose packages** — packages with 2-3 files totaling ≤150 LOC should become
   single modules.
3. **Eliminate re-export facades** — `__init__.py` files that only re-export should be deleted;
   importers should use direct paths.
4. **Preserve public API** — all exported names remain available; only import paths change.
5. **One scope item = one atomic commit** — each scope item is independently testable.
6. **No speculative abstractions** — only introduce shared helpers/factories/protocols when they
   remove concrete duplication already present in the codebase.
7. **Deduplicate before consolidating** — extract shared helpers (AST, node text, error results,
   Rust fallback) first, then consolidate the modules that consumed the duplicated code.
8. **Factory over ad-hoc construction** — request/result/settings construction flows through
   canonical factories rather than repeated inline wiring.

## Current Baseline

- 305 Python files in `tools/cq/`, 69,391 LOC total
- 109 files (36%) are ≤60 LOC — many are single-class wrappers or re-export facades
- 20+ `__init__.py` files that are pure re-export facades (330+ LOC combined)
- 15 `*_contracts.py` files scattered across 7 directories (1,713 LOC)
- 5 tiny cache contract files (17-22 LOC each) alongside a 143-LOC `contracts.py`
- `macros/common/` package (3 files, 35 LOC) exists solely to re-export from parent
- `core/id/` package (3 files, 84 LOC) wraps 2 small functions + 1 normalizer
- `core/services/` package (4 files, 148 LOC) wraps 3 thin service facades
- `core/codec.py` (72 LOC) and `core/public_serialization.py` (35 LOC) now delegate entirely to `core/contract_codec.py`
- Rust fallback logic is wrapped per macro via duplicate `_apply_rust_fallback` functions in 8 macro files (`calls.py`, `impact.py`, `imports.py`, `exceptions.py`, `sig_impact.py`, `side_effects.py`, `scopes.py`, `bytecode.py`)
- CLI step modeling is duplicated between `tools/cq/cli_app/contracts.py` (`*StepCli` dataclasses) and canonical `tools/cq/run/spec.py` (`RunStep`)
- CLI text/protocol output helpers (`_text_result`, `_wants_json`) are duplicated across `ldmd.py`, `artifact.py`, and `admin.py` command files
- Python AST helpers (`_safe_unparse`, `_get_call_name`) are duplicated across 5+ macro/index modules
- Tree-sitter node text/span helpers (`_node_text`, `_node_byte_span`) are duplicated across 9+ files in tree_sitter, neighborhood, and _shared modules
- Error result assembly (`mk_result(...); result.summary["error"]=...`) is repeated in 5+ CLI/run files
- Environment-driven settings parsing is fragmented across `execution_policy.py`, `cache/policy.py`, `cache_runtime_tuning.py`, and `parser_controls.py`
- Tree-sitter has 8 small utility files in `core/` (25-70 LOC each) that form two natural groups
- Tree-sitter `diagnostics/`, `tags/`, and `schema/` packages each have 2-3 files totaling 90-190 LOC
- Search `pipeline/` has 6 small files (24-76 LOC) that form two natural groups
- Search `python/` has 6 small files (20-60 LOC) that form two natural groups
- Search `rust/` has 4 small files (28-101 LOC) that form two natural groups
- Path normalization and glob matching logic is duplicated across `query/`, `index/`, and batch
  helpers (`_normalize_match_file`, `_matches_globs`, `_is_relative_to`)
- Target-spec parsing is duplicated between `core/bundles.py` and
  `neighborhood/target_resolution.py`
- Query compilation boilerplate is duplicated across Python/Rust lane runtimes and neighborhood
  query engine (`_compile_query` variants with similar validation/specialization)
- UUID v7 timestamp extraction logic is duplicated (`uuid_time_millis` vs `uuid7_time_ms`)
- Re-export-only modules persist in `macros/common/` and
  `search/tree_sitter/schema/generated.py`, adding import indirection without behavior

---

## S1. Core Serialization Authority Completion

### Goal

Complete the consolidation around `contract_codec.py` by absorbing the two remaining delegation
modules (`codec.py`, `public_serialization.py`) and the trivial type-alias file (`json_types.py`).
Result: 3 fewer files, single serialization authority.

### Representative Code Snippets

```python
# tools/cq/core/contract_codec.py — absorbs codec.py and public_serialization.py exports
# Existing contract_codec.py content stays. Add these at the bottom:

# --- Absorbed from codec.py ---

def dumps_json_value(value: object) -> bytes:
    """Encode arbitrary value to JSON bytes."""
    return JSON_ENCODER.encode(value)


def loads_json_value(data: bytes | str) -> object:
    """Decode JSON bytes to Python object."""
    return JSON_DECODER.decode(data)


def loads_json_result(data: bytes | str) -> CqResult:
    """Decode JSON bytes directly to CqResult."""
    return JSON_RESULT_DECODER.decode(data)
```

```python
# tools/cq/core/structs.py — absorbs json_types.py
# Add at the top, after imports:

JsonScalar: TypeAlias = str | int | float | bool | None
JsonValue: TypeAlias = JsonScalar | list["JsonValue"] | dict[str, "JsonValue"]
```

### Files to Edit

- `tools/cq/core/contract_codec.py` — absorb `dumps_json_value`, `loads_json_value`, `loads_json_result` from `codec.py`; absorb (already has) `to_public_dict`, `to_public_list` from `public_serialization.py`
- `tools/cq/core/structs.py` — absorb `JsonScalar`, `JsonValue` type aliases from `json_types.py`
- All importers of `core.codec` → repoint to `core.contract_codec`
- All importers of `core.public_serialization` → repoint to `core.contract_codec`
- All importers of `core.json_types` → repoint to `core.structs`

### New Files to Create

- `tests/unit/cq/core/test_contract_codec_consolidated.py` — verify all absorbed exports work

### Legacy Decommission/Delete Scope

- Delete `tools/cq/core/codec.py` — all exports absorbed into `contract_codec.py`
- Delete `tools/cq/core/public_serialization.py` — `to_public_dict`/`to_public_list` already live in `contract_codec.py`
- Delete `tools/cq/core/json_types.py` — type aliases moved to `structs.py`

---

## S2. Core Contract, Identity & Services Consolidation

### Goal

Merge scattered small contract files in `core/` and flatten two tiny packages (`id/`, `services/`)
into single modules. Result: 7 fewer files, simpler core package.

### Representative Code Snippets

```python
# tools/cq/core/contracts.py — absorbs requests.py and uuid_contracts.py
# Existing ContractEnvelope + helpers stay. Add:

# --- Absorbed from requests.py ---

class SummaryBuildRequest(CqStruct, frozen=True):
    """Input contract for multilang summary assembly."""
    result: CqResult
    language: QueryLanguage
    # ... existing fields ...


class MergeResultsRequest(CqStruct, frozen=True):
    """Input contract for multi-language result merge."""
    results: list[CqResult]
    # ... existing fields ...


# --- Absorbed from uuid_contracts.py ---

class UuidIdentityContractV1(CqStruct, frozen=True):
    """Sortable UUID identity contract."""
    run_id: str
    artifact_id: str
    cache_key_uses_uuid: bool = False
    # ... existing fields ...
```

```python
# tools/cq/core/id.py — flattened from id/ package
from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence

import msgspec


def canonicalize_payload(value: object) -> object:
    """Deterministic normalization for digest inputs."""
    if isinstance(value, Mapping):
        return {str(k): canonicalize_payload(v) for k, v in sorted(value.items())}
    if isinstance(value, (list, tuple, Sequence)) and not isinstance(value, (str, bytes)):
        return [canonicalize_payload(v) for v in value]
    return value


def stable_digest(value: object) -> str:
    """SHA-256 hex digest of canonicalized msgpack payload."""
    canonical = canonicalize_payload(value)
    packed = msgspec.msgpack.encode(canonical)
    return hashlib.sha256(packed).hexdigest()


def stable_digest24(value: object) -> str:
    """24-char prefix of stable_digest."""
    return stable_digest(value)[:24]
```

```python
# tools/cq/core/services.py — flattened from services/ package
from __future__ import annotations

from tools.cq.core.structs import CqOutputStruct


class EntityFrontDoorRequest(CqStruct, frozen=True):
    result: "CqResult"
    relationship_detail_max_matches: int = 50


class CallsServiceRequest(CqStruct, frozen=True):
    root: str
    function_name: str
    # ... existing fields ...


class SearchServiceRequest(CqStruct, frozen=True):
    root: str
    query: str
    # ... existing fields ...


class EntityService:
    @staticmethod
    def attach_front_door(request: EntityFrontDoorRequest) -> None:
        from tools.cq.query.entity_front_door import attach_entity_front_door_insight
        attach_entity_front_door_insight(
            request.result,
            relationship_detail_max_matches=request.relationship_detail_max_matches,
        )


class CallsService:
    @staticmethod
    def execute(request: CallsServiceRequest) -> "CqResult":
        from tools.cq.macros.calls import cmd_calls
        return cmd_calls(root=request.root, function_name=request.function_name, ...)


class SearchService:
    @staticmethod
    def execute(request: SearchServiceRequest) -> "CqResult":
        from tools.cq.search.pipeline import run_smart_search_pipeline
        return run_smart_search_pipeline(...)
```

### Files to Edit

- `tools/cq/core/contracts.py` — absorb `SummaryBuildRequest`, `MergeResultsRequest` from `requests.py`; absorb `UuidIdentityContractV1` from `uuid_contracts.py`
- All importers of `core.requests` → repoint to `core.contracts`
- All importers of `core.uuid_contracts` → repoint to `core.contracts`
- All importers of `core.id` / `core.id.canonical` / `core.id.digests` → repoint to `core.id` (flat module)
- All importers of `core.services.*` → repoint to `core.services` (flat module)

### New Files to Create

- `tools/cq/core/id.py` — flattened module (replaces `id/` package)
- `tools/cq/core/services.py` — flattened module (replaces `services/` package)
- `tests/unit/cq/core/test_id_consolidated.py` — verify canonicalize + digest
- `tests/unit/cq/core/test_services_consolidated.py` — verify service facades

### Legacy Decommission/Delete Scope

- Delete `tools/cq/core/requests.py` — absorbed into `contracts.py`
- Delete `tools/cq/core/uuid_contracts.py` — absorbed into `contracts.py`
- Delete `tools/cq/core/id/__init__.py` — replaced by flat `id.py`
- Delete `tools/cq/core/id/canonical.py` — absorbed into flat `id.py`
- Delete `tools/cq/core/id/digests.py` — absorbed into flat `id.py`
- Delete `tools/cq/core/services/__init__.py` — replaced by flat `services.py`
- Delete `tools/cq/core/services/entity_service.py` — absorbed into flat `services.py`
- Delete `tools/cq/core/services/calls_service.py` — absorbed into flat `services.py`
- Delete `tools/cq/core/services/search_service.py` — absorbed into flat `services.py`

---

## S3. Cache Contract Consolidation + Settings Factory

### Goal

Merge 5 tiny cache contract files (17-22 LOC each, single struct per file) into the existing
`cache/contracts.py` (143 LOC), and introduce a `SettingsFactory` that unifies environment-derived
settings construction for cache policy, runtime execution policy, and parser controls.
Result: 5 fewer files, single cache contract authority, canonical settings construction.

### Representative Code Snippets

```python
# tools/cq/core/cache/contracts.py — absorbs 5 satellite contract files
# Existing 12 cache contract classes stay. Add these:

import msgspec

# --- Absorbed from tree_sitter_blob_store_contracts.py ---

class TreeSitterBlobRefV1(CqStruct, frozen=True):
    """Reference to a cached tree-sitter parse blob."""
    blob_id: str
    storage_key: str
    size_bytes: int
    path: str | None = None


# --- Absorbed from coordination_contracts.py ---

class LaneCoordinationPolicyV1(CqStruct, frozen=True):
    """Coordination policy for parallel lane execution."""
    semaphore_key: str
    lock_key_suffix: str
    reentrant_key_suffix: str
    lane_limit: int = 4
    ttl_seconds: int = 15


# --- Absorbed from maintenance_contracts.py ---

class CacheMaintenanceSnapshotV1(CqStruct, frozen=True):
    """Point-in-time snapshot of cache health metrics."""
    hits: int = 0
    misses: int = 0
    expired_removed: int = 0
    culled_removed: int = 0
    integrity_errors: int = 0


# --- Absorbed from cache_runtime_tuning_contracts.py ---

class CacheRuntimeTuningV1(CqStruct, frozen=True):
    """Runtime tuning parameters for DiskCache backend."""
    cull_limit: int = 16
    eviction_policy: str = "least-recently-stored"
    statistics_enabled: bool = False
    create_tag_index: bool = True
    sqlite_mmap_size: int = 0
    sqlite_cache_size: int = 0
    transaction_batch_size: int = 128


# --- Absorbed from tree_sitter_cache_store_contracts.py ---

class TreeSitterCacheEnvelopeV1(CqCacheStruct, frozen=True):
    """Cache envelope for tree-sitter query results."""
    language: str
    file_hash: str
    grammar_hash: str
    query_pack_hash: str
    scope_hash: str
    payload: dict[str, object] = msgspec.field(default_factory=dict)
```

```python
# tools/cq/core/settings_factory.py — unified env-derived settings construction
from __future__ import annotations

from pathlib import Path

from tools.cq.core.cache.policy import CqCachePolicyV1, default_cache_policy
from tools.cq.core.runtime.execution_policy import RuntimeExecutionPolicy, default_runtime_execution_policy
from tools.cq.search.tree_sitter.core.parser_controls import ParserControlSettingsV1, parser_controls_from_env


class SettingsFactory:
    """Canonical factory for environment-derived settings construction."""

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

- `tools/cq/core/cache/contracts.py` — absorb 5 struct definitions
- `tools/cq/core/cache/__init__.py` — remove re-exports of deleted modules
- All importers of the 5 deleted files → repoint to `cache.contracts`
- `tools/cq/core/bootstrap.py` — use `SettingsFactory` for settings construction
- `tools/cq/search/tree_sitter/core/parse.py` — use `SettingsFactory.parser_controls()`
- `tools/cq/neighborhood/tree_sitter_collector.py` — use `SettingsFactory.parser_controls()`

### New Files to Create

- `tools/cq/core/settings_factory.py` — unified settings construction
- `tests/unit/cq/core/cache/test_cache_contracts_consolidated.py` — roundtrip tests for absorbed structs
- `tests/unit/cq/core/test_settings_factory.py` — tests for factory construction

### Legacy Decommission/Delete Scope

- Delete `tools/cq/core/cache/tree_sitter_blob_store_contracts.py` — `TreeSitterBlobRefV1` moved to `contracts.py`
- Delete `tools/cq/core/cache/coordination_contracts.py` — `LaneCoordinationPolicyV1` moved to `contracts.py`
- Delete `tools/cq/core/cache/maintenance_contracts.py` — `CacheMaintenanceSnapshotV1` moved to `contracts.py`
- Delete `tools/cq/core/cache/cache_runtime_tuning_contracts.py` — `CacheRuntimeTuningV1` moved to `contracts.py`
- Delete `tools/cq/core/cache/tree_sitter_cache_store_contracts.py` — `TreeSitterCacheEnvelopeV1` moved to `contracts.py`
- Delete fragmented settings construction callsites in `bootstrap.py` once `SettingsFactory` is canonical

---

## S4. Tree-Sitter Core Utility Consolidation

### Goal

Merge 7 small utility files in `tree_sitter/core/` (25-70 LOC each) into 2 cohesive modules:
`runtime_support.py` (execution-time helpers) and `infrastructure.py` (setup/plumbing helpers).
`text_utils.py` is handled in S15 via canonical `node_utils.py` and then removed.
Result: 5 fewer files from this scope item (plus `text_utils.py` deletion in S15).

### Representative Code Snippets

```python
# tools/cq/search/tree_sitter/core/runtime_support.py
# Merges: autotune.py (42), windowing.py (70), budgeting.py (43)
from __future__ import annotations

from time import monotonic
from typing import TYPE_CHECKING

from tools.cq.core.structs import CqStruct

if TYPE_CHECKING:
    from tree_sitter import Node
    from tools.cq.search.tree_sitter.contracts.core_models import (
        AdaptiveRuntimeSnapshotV1,
        QueryPointWindowV1,
        QueryWindowV1,
    )


# ── Budgeting ────────────────────────────────────────────────────────────

def budget_ms_per_anchor(
    *,
    timeout_seconds: float,
    max_anchors: int,
    reserve_fraction: float = 0.5,
    min_budget_ms: int = 25,
    max_budget_ms: int = 2_000,
) -> int:
    """Derive bounded per-anchor query budget from request timeout bounds."""
    safe_timeout = max(0.1, float(timeout_seconds))
    safe_anchors = max(1, int(max_anchors))
    usable_seconds = safe_timeout * max(0.05, min(0.95, reserve_fraction))
    budget = int((usable_seconds * 1000.0) / safe_anchors)
    return max(min_budget_ms, min(max_budget_ms, budget))


def deadline_from_budget_ms(budget_ms: int | None) -> float | None:
    """Monotonic deadline from budget milliseconds; None when disabled."""
    if budget_ms is None or budget_ms <= 0:
        return None
    return monotonic() + (float(budget_ms) / 1000.0)


def is_deadline_expired(deadline: float | None) -> bool:
    """Check whether monotonic deadline has passed."""
    if deadline is None:
        return False
    return monotonic() >= deadline


# ── Autotune ─────────────────────────────────────────────────────────────

class QueryAutotunePlanV1(CqStruct, frozen=True):
    budget_ms: float = 500.0
    match_limit: int = 1000
    window_split_target: int = 4


def build_autotune_plan(snapshot: AdaptiveRuntimeSnapshotV1) -> QueryAutotunePlanV1:
    """Build autotune plan from adaptive runtime telemetry."""
    # ... existing logic from autotune.py ...


# ── Windowing ────────────────────────────────────────────────────────────

def apply_point_window(
    window: QueryPointWindowV1, source: bytes, node: Node
) -> tuple[int, int]:
    """Apply point window to constrain query cursor range."""
    # ... existing logic from windowing.py ...


def apply_byte_window(
    window: QueryWindowV1, source: bytes, node: Node
) -> tuple[int, int]:
    """Apply byte-range window to constrain query cursor range."""
    # ... existing logic from windowing.py ...
```

```python
# tools/cq/search/tree_sitter/core/infrastructure.py
# Merges: parallel.py (40), stream_source.py (62), parser_controls.py (60), language_runtime.py (40)
from __future__ import annotations

import os
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import TYPE_CHECKING, TypeVar

from tools.cq.core.structs import CqStruct

if TYPE_CHECKING:
    from tree_sitter import Language, Parser

T = TypeVar("T")
R = TypeVar("R")


# ── Language Runtime ─────────────────────────────────────────────────────

def load_language(name: str) -> Language:
    """Load a tree-sitter language grammar by name."""
    from tools.cq.search.tree_sitter.core.language_registry import load_tree_sitter_language
    return load_tree_sitter_language(name)


def make_parser(language: Language) -> Parser:
    """Create a parser bound to a language."""
    from tree_sitter import Parser
    parser = Parser()
    parser.language = language
    return parser


# ── Parser Controls ──────────────────────────────────────────────────────

class ParserControlSettingsV1(CqStruct, frozen=True):
    reset_before_parse: bool = False
    enable_logger: bool = False
    dot_graph_dir: str | None = None


def parser_controls_from_env() -> ParserControlSettingsV1:
    """Build parser control settings from environment variables."""
    # ... existing logic ...


def apply_parser_controls(parser: Parser, settings: ParserControlSettingsV1) -> None:
    """Apply control-plane settings to a parser instance."""
    # ... existing logic ...


# ── Parallel Execution ───────────────────────────────────────────────────

def run_file_lanes_parallel(
    files: list[T], lane_fn: ..., *, max_workers: int = 4
) -> list[R]:
    """Run independent file-lane workloads in parallel."""
    # ... existing logic from parallel.py ...


# ── Streaming Source ─────────────────────────────────────────────────────

def build_stream_reader(path: Path) -> ...:
    """Build a streaming source callback for large file parsing."""
    # ... existing logic from stream_source.py ...


def parse_streaming_source(parser: Parser, path: Path) -> ...:
    """Parse a file using streaming source with fail-open fallback."""
    # ... existing logic from stream_source.py ...
```

### Files to Edit

- All importers of `tree_sitter.core.autotune` → repoint to `core.runtime_support`
- All importers of `tree_sitter.core.windowing` → repoint to `core.runtime_support`
- All importers of `tree_sitter.core.budgeting` → repoint to `core.runtime_support`
- All importers of `tree_sitter.core.parallel` → repoint to `core.infrastructure`
- All importers of `tree_sitter.core.stream_source` → repoint to `core.infrastructure`
- All importers of `tree_sitter.core.parser_controls` → repoint to `core.infrastructure`
- All importers of `tree_sitter.core.language_runtime` → repoint to `core.infrastructure`

### New Files to Create

- `tools/cq/search/tree_sitter/core/runtime_support.py` — merged runtime helpers
- `tools/cq/search/tree_sitter/core/infrastructure.py` — merged plumbing helpers
- `tests/unit/cq/search/tree_sitter/test_core_runtime_support.py` — tests for merged runtime
- `tests/unit/cq/search/tree_sitter/test_core_infrastructure.py` — tests for merged infrastructure

### Legacy Decommission/Delete Scope

- Delete `tools/cq/search/tree_sitter/core/autotune.py` — absorbed into `runtime_support.py`
- Delete `tools/cq/search/tree_sitter/core/windowing.py` — absorbed into `runtime_support.py`
- Delete `tools/cq/search/tree_sitter/core/budgeting.py` — absorbed into `runtime_support.py`
- Delete `tools/cq/search/tree_sitter/core/parallel.py` — absorbed into `infrastructure.py`
- Delete `tools/cq/search/tree_sitter/core/stream_source.py` — absorbed into `infrastructure.py`
- Delete `tools/cq/search/tree_sitter/core/parser_controls.py` — absorbed into `infrastructure.py`
- Delete `tools/cq/search/tree_sitter/core/language_runtime.py` — absorbed into `infrastructure.py`

---

## S5. Tree-Sitter Query & Peripheral Package Consolidation

### Goal

Merge small query utility files, flatten 3 tiny packages (diagnostics, tags, schema support),
consolidate structural exports, and merge injection config. Result: ~12 fewer files.

### Representative Code Snippets

```python
# tools/cq/search/tree_sitter/query/support.py
# Merges: pack_metadata.py (38), resource_paths.py (36), cache_adapter.py (33)
from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from diskcache import FanoutCache


# ── Resource Paths ───────────────────────────────────────────────────────

_QUERIES_ROOT = Path(__file__).resolve().parent.parent.parent / "queries"


def query_pack_dir(language: str) -> Path:
    """Canonical directory for a language's query pack."""
    return _QUERIES_ROOT / language


def query_pack_path(language: str, pack_name: str) -> Path:
    """Canonical path for a specific query pack file."""
    return query_pack_dir(language) / f"{pack_name}.scm"


def query_contracts_path(language: str) -> Path:
    """Path to the query contracts YAML for a language."""
    return query_pack_dir(language) / "contracts.yaml"


def diagnostics_query_path(language: str) -> Path:
    """Path to the diagnostics query pack for a language."""
    return query_pack_dir(language) / "diagnostics.scm"


# ── Pack Metadata ────────────────────────────────────────────────────────

def pattern_settings(query: object, index: int) -> dict[str, str]:
    """Extract pattern_settings dict for a query pattern."""
    # ... existing logic from pack_metadata.py ...


def first_capture(query: object, index: int) -> str | None:
    """Get first capture name for a query pattern."""
    # ... existing logic from pack_metadata.py ...


# ── Cache Adapter ────────────────────────────────────────────────────────

def query_registry_cache() -> FanoutCache | None:
    """Lazy cache backend for query registry stampede guards."""
    try:
        from tools.cq.core.cache.diskcache_backend import get_cq_cache_backend
        return get_cq_cache_backend()
    except Exception:
        return None
```

```python
# tools/cq/search/tree_sitter/query/drift.py
# Merges: drift_diff.py (57), grammar_drift.py (98)
from __future__ import annotations

from tools.cq.core.structs import CqStruct


# ── Diff Types ───────────────────────────────────────────────────────────

class GrammarDiffV1(CqStruct, frozen=True):
    node_kinds_added: tuple[str, ...] = ()
    node_kinds_removed: tuple[str, ...] = ()
    fields_added: tuple[str, ...] = ()
    fields_removed: tuple[str, ...] = ()


def diff_schema(old_index: object, new_index: object) -> GrammarDiffV1:
    """Compute structural diff between grammar schema snapshots."""
    # ... existing logic from drift_diff.py ...


def has_breaking_changes(diff: GrammarDiffV1) -> bool:
    """Check if a grammar diff contains breaking changes."""
    return bool(diff.node_kinds_removed or diff.fields_removed)


# ── Drift Report ─────────────────────────────────────────────────────────

def build_grammar_drift_report(language: str) -> object:
    """Build grammar drift report comparing current vs snapshot."""
    # ... existing logic from grammar_drift.py ...


def get_last_contract_snapshot(language: str) -> object | None:
    """Get cached contract snapshot for a language."""
    # ... existing logic from grammar_drift.py ...
```

```python
# tools/cq/search/tree_sitter/diagnostics.py — FLATTENED from diagnostics/ package
# Merges: diagnostics/collector.py (113), diagnostics/recovery_hints.py (78)
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tree_sitter import Node


# ── Recovery Hints ───────────────────────────────────────────────────────

def recovery_hints_for_node(node: Node) -> list[str]:
    """Generate parse-state recovery hints for ERROR/MISSING nodes."""
    # ... existing logic from recovery_hints.py ...


# ── Diagnostic Collection ────────────────────────────────────────────────

def collect_tree_sitter_diagnostics(
    tree: object, source: bytes, *, language: str, settings: object | None = None
) -> list[object]:
    """Extract ERROR/MISSING diagnostics from a parsed tree."""
    # ... existing logic from collector.py, calls recovery_hints_for_node ...
```

```python
# tools/cq/search/tree_sitter/tags.py — FLATTENED from tags/ package
# Merges: tags/contracts.py (22), tags/runtime.py (67)
from __future__ import annotations

import msgspec

from typing import TYPE_CHECKING, Protocol, runtime_checkable

from tools.cq.core.structs import CqStruct

if TYPE_CHECKING:
    from tree_sitter import Node


class RustTagEventV1(CqStruct, frozen=True):
    role: str
    kind: str
    name: str
    start_byte: int
    end_byte: int
    metadata: dict[str, str] = msgspec.field(default_factory=dict)


@runtime_checkable
class NodeLike(Protocol):
    @property
    def start_byte(self) -> int: ...
    @property
    def end_byte(self) -> int: ...


def build_tag_events(nodes: list[NodeLike], source: bytes, *, kind: str) -> list[RustTagEventV1]:
    """Build tag events from tree-sitter node matches."""
    # ... existing logic from runtime.py ...
```

**Additional merges in this scope item:**

- `schema/runtime.py` (44 LOC) + `schema/generated.py` (20 LOC) + `schema/node_codegen.py` (38 LOC) → merge into `schema/node_schema.py` (add ~100 LOC to existing 178 LOC)
- `structural/diagnostic_export.py` (43) + `structural/token_export.py` (65) + `structural/query_hits.py` (55) → merge into `structural/exports.py` (new, ~163 LOC)
- `rust_lane/injection_settings.py` (40) + `rust_lane/injection_profiles.py` (53) → merge into `rust_lane/injection_config.py` (new, ~93 LOC)

### Files to Edit

- `tools/cq/search/tree_sitter/schema/node_schema.py` — absorb `runtime.py`, `generated.py`, `node_codegen.py`
- All importers of the merged files → repoint to new locations
- `tools/cq/search/tree_sitter/contracts/__init__.py` — update if it re-exports diagnostics/tags

### New Files to Create

- `tools/cq/search/tree_sitter/query/support.py` — merged query utilities
- `tools/cq/search/tree_sitter/query/drift.py` — merged drift detection
- `tools/cq/search/tree_sitter/diagnostics.py` — flattened diagnostics module
- `tools/cq/search/tree_sitter/tags.py` — flattened tags module
- `tools/cq/search/tree_sitter/structural/exports.py` — merged structural exports
- `tools/cq/search/tree_sitter/rust_lane/injection_config.py` — merged injection config
- `tests/unit/cq/search/tree_sitter/test_query_support.py`
- `tests/unit/cq/search/tree_sitter/test_query_drift.py`
- `tests/unit/cq/search/tree_sitter/test_diagnostics.py`
- `tests/unit/cq/search/tree_sitter/test_tags.py`
- `tests/unit/cq/search/tree_sitter/test_structural_exports.py`
- `tests/unit/cq/search/tree_sitter/test_injection_config.py`

### Legacy Decommission/Delete Scope

- Delete `tools/cq/search/tree_sitter/query/pack_metadata.py` — absorbed into `query/support.py`
- Delete `tools/cq/search/tree_sitter/query/resource_paths.py` — absorbed into `query/support.py`
- Delete `tools/cq/search/tree_sitter/query/cache_adapter.py` — absorbed into `query/support.py`
- Delete `tools/cq/search/tree_sitter/query/drift_diff.py` — absorbed into `query/drift.py`
- Delete `tools/cq/search/tree_sitter/query/grammar_drift.py` — absorbed into `query/drift.py`
- Delete `tools/cq/search/tree_sitter/diagnostics/` directory — replaced by `diagnostics.py` module
- Delete `tools/cq/search/tree_sitter/tags/` directory — replaced by `tags.py` module
- Delete `tools/cq/search/tree_sitter/schema/runtime.py` — absorbed into `node_schema.py`
- Delete `tools/cq/search/tree_sitter/schema/generated.py` — absorbed into `node_schema.py`
- Delete `tools/cq/search/tree_sitter/schema/node_codegen.py` — absorbed into `node_schema.py`
- Delete `tools/cq/search/tree_sitter/structural/diagnostic_export.py` — absorbed into `exports.py`
- Delete `tools/cq/search/tree_sitter/structural/token_export.py` — absorbed into `exports.py`
- Delete `tools/cq/search/tree_sitter/structural/query_hits.py` — absorbed into `exports.py`
- Delete `tools/cq/search/tree_sitter/rust_lane/injection_settings.py` — absorbed into `injection_config.py`
- Delete `tools/cq/search/tree_sitter/rust_lane/injection_profiles.py` — absorbed into `injection_config.py`

---

## S6. Search Pipeline & Lane Module Consolidation

### Goal

Merge small files in search `pipeline/`, `python/`, and `rust/` subdirectories into cohesive
modules. Result: ~10 fewer files.

### Representative Code Snippets

```python
# tools/cq/search/pipeline/contracts.py
# Merges: partition_contracts.py (24), models.py (76)
from __future__ import annotations

import msgspec

from tools.cq.core.structs import CqSettingsStruct
from tools.cq.query.language import QueryLanguage
from tools.cq.search.pipeline.profiles import SearchLimits


# --- From models.py ---

class SearchConfig(CqSettingsStruct, frozen=True):
    root: str
    query: str
    mode: str = "auto"
    mode_chain: tuple[str, ...] = msgspec.field(default_factory=tuple)
    # ... existing fields ...


class SearchRequest(CqSettingsStruct, frozen=True):
    root: str
    query: str
    mode: str | None = None
    limits: SearchLimits | None = None
    # ... existing fields ...


class CandidateSearchRequest(CqSettingsStruct, frozen=True):
    root: str
    query: str
    mode: str
    limits: SearchLimits
    # ... existing fields ...


SmartSearchContext = SearchConfig  # type alias


# --- From partition_contracts.py ---

class SearchPartitionPlanV1(CqSettingsStruct, frozen=True):
    root: str
    language: QueryLanguage
    query: str
    mode: str
    include_strings: bool = False
    include_globs: tuple[str, ...] = ()
    exclude_globs: tuple[str, ...] = ()
    max_total_matches: int = 0
    run_id: str | None = None
```

```python
# tools/cq/search/pipeline/orchestration.py
# Merges: assembly.py (27), section_builder.py (33), legacy.py (59)
from __future__ import annotations

from dataclasses import dataclass


# --- From legacy.py ---

@dataclass
class SearchPipeline:
    """Pipeline facade for search orchestration."""

    def run_partitions(self, context: object) -> list[object]:
        # ... existing logic ...

    def assemble(self, partitions: list[object]) -> object:
        # ... existing logic ...

    def execute(self, context: object) -> object:
        partitions = self.run_partitions(context)
        return self.assemble(partitions)


# --- From assembly.py ---

def assemble_result(context: object) -> object:
    """Bridge: assemble CqResult from smart-search partition output."""
    # ... existing logic ...


# --- From section_builder.py ---

def insert_target_candidates(result: object, candidates: list[object]) -> None:
    """Insert target candidate section into result."""
    # ... existing logic ...


def insert_neighborhood_preview(result: object, preview: object) -> None:
    """Insert neighborhood preview section into result."""
    # ... existing logic ...
```

```python
# tools/cq/search/python/pipeline_support.py
# Merges: stages.py (20), ast_grep_rules.py (22), resolution_payload.py (26), orchestrator.py (35)
from __future__ import annotations

from typing import TYPE_CHECKING

import msgspec

from tools.cq.core.structs import CqStruct

if TYPE_CHECKING:
    from ast_grep_py import SgNode


# --- Enrichment Stages ---

def extract_signature_stage(node: SgNode) -> str:
    """Extract function signature preview, bounded to 200 chars."""
    # ... existing logic from stages.py ...


# --- AST-Grep Rules ---

def find_import_aliases(node: SgNode) -> list[object]:
    """Find import X as Y patterns using ast-grep config rules."""
    # ... existing logic from ast_grep_rules.py ...


# --- Resolution Payload ---

class PythonResolutionPayloadV1(CqOutputStruct, frozen=True):
    """Serializable contract for Python native-resolution payload."""
    symbol: str | None = None
    symbol_role: str | None = None
    enclosing_callable: str | None = None
    enclosing_class: str | None = None
    qualified_name_candidates: tuple[dict[str, object], ...] = msgspec.field(default_factory=tuple)


def coerce_resolution_payload(raw: object) -> PythonResolutionPayloadV1:
    """Coerce raw mapping to typed resolution payload."""
    return msgspec.convert(raw, type=PythonResolutionPayloadV1, strict=False)


# --- Orchestration ---

def run_python_enrichment_pipeline(request: object) -> dict[str, object]:
    """Run Python lane enrichment pipeline with native resolution merge."""
    # ... existing logic from orchestrator.py ...
```

```python
# tools/cq/search/python/evidence.py
# Merges: semantic_signal.py (60), agreement.py (56)
from __future__ import annotations


# --- Semantic Signal ---

def evaluate_python_semantic_signal_from_mapping(
    mapping: dict[str, object],
) -> tuple[bool, tuple[str, ...]]:
    """Evaluate semantic signal from Python enrichment payload."""
    # ... existing logic from semantic_signal.py ...


# --- Cross-Source Agreement ---

def build_agreement_summary(
    enrichment: dict[str, object],
) -> dict[str, object]:
    """Analyze cross-source agreement (ast_grep vs native vs tree_sitter)."""
    # ... existing logic from agreement.py ...
```

```python
# tools/cq/search/rust/contracts.py
# Merges: macro_expansion_contracts.py (28), module_graph_contracts.py (36)
from __future__ import annotations

import msgspec

from tools.cq.core.structs import CqOutputStruct


class RustMacroExpansionRequestV1(CqOutputStruct, frozen=True):
    file_path: str
    line: int
    col: int
    macro_call_id: str


class RustMacroExpansionResultV1(CqOutputStruct, frozen=True):
    macro_call_id: str
    name: str | None = None
    expansion: str | None = None
    source: str = "rust_analyzer"
    applied: bool = False


class RustModuleNodeV1(CqOutputStruct, frozen=True):
    module_id: str
    module_name: str
    file_path: str | None = None


class RustImportEdgeV1(CqOutputStruct, frozen=True):
    source_module_id: str
    target_path: str
    visibility: str = "private"
    is_reexport: bool = False


class RustModuleGraphV1(CqOutputStruct, frozen=True):
    modules: tuple[RustModuleNodeV1, ...] = ()
    edges: tuple[RustImportEdgeV1, ...] = ()
    metadata: dict[str, object] = msgspec.field(default_factory=dict)
```

```python
# tools/cq/search/rust/extensions.py
# Merges: macro_expansion_bridge.py (71), module_graph_builder.py (101)
from __future__ import annotations

from tools.cq.search.rust.contracts import (
    RustMacroExpansionRequestV1,
    RustMacroExpansionResultV1,
    RustModuleGraphV1,
    RustModuleNodeV1,
    RustImportEdgeV1,
)


# --- Macro Expansion Bridge ---

def expand_macro(request: RustMacroExpansionRequestV1) -> RustMacroExpansionResultV1 | None:
    """Expand a Rust macro via rust-analyzer (fail-open)."""
    # ... existing logic from macro_expansion_bridge.py ...


def expand_macros(requests: list[RustMacroExpansionRequestV1]) -> list[RustMacroExpansionResultV1]:
    """Batch macro expansion."""
    # ... existing logic ...


# --- Module Graph Builder ---

def build_module_graph(
    module_rows: list[dict[str, object]], import_rows: list[dict[str, object]]
) -> dict[str, object]:
    """Build structured Rust module graph from tree-sitter fact rows."""
    # ... existing logic from module_graph_builder.py ...
```

### Files to Edit

- All importers of merged files → repoint to new locations
- `tools/cq/search/pipeline/__init__.py` — update lazy imports
- `tools/cq/search/python/__init__.py` — update re-exports
- `tools/cq/search/rust/__init__.py` — update re-exports

### New Files to Create

- `tools/cq/search/pipeline/contracts.py` — merged pipeline contracts
- `tools/cq/search/pipeline/orchestration.py` — merged pipeline orchestration
- `tools/cq/search/python/pipeline_support.py` — merged Python lane utilities
- `tools/cq/search/python/evidence.py` — merged evidence evaluation
- `tools/cq/search/rust/contracts.py` — merged Rust contracts
- `tools/cq/search/rust/extensions.py` — merged Rust extensions
- `tests/unit/cq/search/pipeline/test_pipeline_contracts.py`
- `tests/unit/cq/search/pipeline/test_orchestration.py`
- `tests/unit/cq/search/python/test_pipeline_support.py`
- `tests/unit/cq/search/python/test_evidence.py`
- `tests/unit/cq/search/rust/test_contracts.py`
- `tests/unit/cq/search/rust/test_extensions.py`

### Legacy Decommission/Delete Scope

- Delete `tools/cq/search/pipeline/assembly.py` — absorbed into `orchestration.py`
- Delete `tools/cq/search/pipeline/section_builder.py` — absorbed into `orchestration.py`
- Delete `tools/cq/search/pipeline/legacy.py` — absorbed into `orchestration.py`
- Delete `tools/cq/search/pipeline/partition_contracts.py` — absorbed into `contracts.py`
- Delete `tools/cq/search/pipeline/models.py` — absorbed into `contracts.py`
- Delete `tools/cq/search/python/stages.py` — absorbed into `pipeline_support.py`
- Delete `tools/cq/search/python/ast_grep_rules.py` — absorbed into `pipeline_support.py`
- Delete `tools/cq/search/python/resolution_payload.py` — absorbed into `pipeline_support.py`
- Delete `tools/cq/search/python/orchestrator.py` — absorbed into `pipeline_support.py`
- Delete `tools/cq/search/python/semantic_signal.py` — absorbed into `evidence.py`
- Delete `tools/cq/search/python/agreement.py` — absorbed into `evidence.py`
- Delete `tools/cq/search/rust/macro_expansion_contracts.py` — absorbed into `contracts.py`
- Delete `tools/cq/search/rust/module_graph_contracts.py` — absorbed into `contracts.py`
- Delete `tools/cq/search/rust/macro_expansion_bridge.py` — absorbed into `extensions.py`
- Delete `tools/cq/search/rust/module_graph_builder.py` — absorbed into `extensions.py`

---

## S7. Macro Infrastructure Consolidation

### Goal

Merge 4 macro utility files (`scan_utils.py`, `target_resolution.py`, `scope_filters.py`,
`scoring_utils.py`) into a single `macros/shared.py` module and delete the `macros/common/`
re-export layer. Result: 6 fewer files.

### Representative Code Snippets

```python
# tools/cq/macros/shared.py
# Merges: scan_utils.py (31), target_resolution.py (45), scope_filters.py (51), scoring_utils.py (68)
from __future__ import annotations

from pathlib import Path

from tools.cq.macros.contracts import MacroScorePayloadV1


# ── Scope Filtering ──────────────────────────────────────────────────────

def scope_filter_applied(include: tuple[str, ...], exclude: tuple[str, ...]) -> bool:
    """Check whether any scope filters are active."""
    return bool(include or exclude)


def resolve_macro_files(
    *, root: str, include: tuple[str, ...] = (), exclude: tuple[str, ...] = (),
    extensions: tuple[str, ...] = (".py",),
) -> list[Path]:
    """Resolve files matching scope filters from repo index."""
    from tools.cq.index.files import build_repo_file_index
    from tools.cq.index.repo import resolve_repo_context
    # ... existing logic from scope_filters.py ...


# ── File Scanning ────────────────────────────────────────────────────────

def iter_files(
    *, root: str, include: tuple[str, ...] = (), exclude: tuple[str, ...] = (),
    extensions: tuple[str, ...] = (".py",), max_files: int = 500,
) -> list[Path]:
    """Enumerate files matching scope filters with limit."""
    files = resolve_macro_files(root=root, include=include, exclude=exclude, extensions=extensions)
    return files[:max_files]


# ── Target Resolution ────────────────────────────────────────────────────

def resolve_target_files(
    *, root: str, target: str | None = None, max_files: int = 500,
    include: tuple[str, ...] = (), exclude: tuple[str, ...] = (),
    extensions: tuple[str, ...] = (".py",),
) -> list[Path]:
    """Resolve target files from path hint, symbol hint, or full scan."""
    # ... existing logic from target_resolution.py ...


# ── Scoring Utilities ────────────────────────────────────────────────────

def macro_scoring_details(
    *, sites: int = 0, files: int = 0, depth: int = 0,
    breakages: int = 0, ambiguities: int = 0, evidence_kind: str = "static",
) -> dict[str, object]:
    """Build normalized scoring details for analysis macros."""
    from tools.cq.core.scoring import compute_impact_score, compute_confidence_score
    # ... existing logic from scoring_utils.py ...


def macro_score_payload(*, files: int, findings: int) -> MacroScorePayloadV1:
    """Build macro score payload summary."""
    # ... existing logic from scoring_utils.py ...
```

### Files to Edit

- All importers of `macros.scan_utils` → repoint to `macros.shared`
- All importers of `macros.target_resolution` → repoint to `macros.shared`
- All importers of `macros.scope_filters` → repoint to `macros.shared`
- All importers of `macros.scoring_utils` → repoint to `macros.shared`
- All importers of `macros.common.*` → repoint to `macros.contracts` or `macros.shared`
- `tools/cq/macros/__init__.py` — remove `common` from exports if present

### New Files to Create

- `tools/cq/macros/shared.py` — merged macro utilities
- `tests/unit/cq/macros/test_shared.py` — tests for merged utilities

### Legacy Decommission/Delete Scope

- Delete `tools/cq/macros/scan_utils.py` — absorbed into `shared.py`
- Delete `tools/cq/macros/target_resolution.py` — absorbed into `shared.py`
- Delete `tools/cq/macros/scope_filters.py` — absorbed into `shared.py`
- Delete `tools/cq/macros/scoring_utils.py` — absorbed into `shared.py`
- Delete `tools/cq/macros/common/__init__.py` — re-export layer removed
- Delete `tools/cq/macros/common/contracts.py` — re-export layer removed
- Delete `tools/cq/macros/common/scoring.py` — re-export layer removed
- Delete `tools/cq/macros/common/targets.py` — re-export layer removed

---

## S8. CLI & Peripheral Module Consolidation

### Goal

Merge 4 small CLI support files into `cli_app/infrastructure.py`, merge neighborhood contract
files, and consolidate utils UUID modules. Result: ~7 fewer files.

### Representative Code Snippets

```python
# tools/cq/cli_app/infrastructure.py
# Merges: config.py (36), groups.py (45), decorators.py (43), dispatch.py (43)
from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable
from inspect import BoundArguments
from pathlib import Path
from typing import TYPE_CHECKING, Any

import cyclopts
from cyclopts import Group
from cyclopts.config import Env, Toml

if TYPE_CHECKING:
    from tools.cq.cli_app.context import CliContext


# ── Command Groups ───────────────────────────────────────────────────────

global_group = Group("Global", sort_key=0)
analysis_group = Group("Analysis", sort_key=1)
admin_group = Group("Admin", sort_key=2)
protocol_group = Group("Protocol", sort_key=3)
setup_group = Group("Setup", sort_key=4)


# ── Config Chain ─────────────────────────────────────────────────────────

def build_config_chain(
    config_file: str | None = None,
    *,
    use_config: bool = True,
) -> list[Any]:
    """Build ordered config provider chain (CLI > env > config > defaults)."""
    providers: list[Any] = [Env(prefix="CQ_", command=False)]
    if not use_config:
        return providers
    if config_file:
        providers.append(Toml(Path(config_file), must_exist=True))
    else:
        providers.append(Toml("pyproject.toml", root_keys=("tool", "cq"), must_exist=False))
    return providers


# ── Context Decorators ───────────────────────────────────────────────────

def require_ctx() -> object:
    """Decorator: validate CliContext is available before command execution."""
    # ... existing logic from decorators.py ...


def require_context(ctx: CliContext) -> None:
    """Validator: raise if context is missing required fields."""
    # ... existing logic from decorators.py ...


# ── Dispatch ─────────────────────────────────────────────────────────────

def dispatch_bound_command(command: Callable[..., Any], bound: BoundArguments) -> Any:
    """Execute a bound cyclopts command, handling async/sync boundaries."""
    result = command(*bound.args, **bound.kwargs)
    if not inspect.isawaitable(result):
        return result
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        awaitable: Awaitable[Any] = result
        return asyncio.run(_await_result(awaitable))
    if inspect.iscoroutine(result):
        result.close()
    raise RuntimeError(
        "Awaitable command dispatched from sync path while an event loop is running."
    )


async def _await_result(awaitable: Awaitable[Any]) -> Any:
    return await awaitable


def cyclopts_error_payload(exc: cyclopts.CycloptsError) -> dict[str, object]:
    """Normalize Cyclopts error details for telemetry and protocol output."""
    return {
        "message": str(exc),
        "exit_code": int(getattr(exc, "exit_code", 2)),
        "argument": getattr(exc, "argument", None),
        "command_chain": tuple(getattr(exc, "command_chain", ()) or ()),
    }
```

```python
# tools/cq/neighborhood/contracts.py
# Merges: tree_sitter_contracts.py (46), capability_gates.py (57)
from __future__ import annotations

from tools.cq.core.structs import CqStruct
from tools.cq.core.snb_schema import DegradeEventV1, NeighborhoodSliceKind, NeighborhoodSliceV1


# --- Collection Contracts ---

class TreeSitterNeighborhoodCollectRequest(CqStruct, frozen=True):
    """Request for tree-sitter neighborhood evidence collection."""
    # ... existing fields from tree_sitter_contracts.py ...


class TreeSitterNeighborhoodCollectResult(CqStruct, frozen=True):
    """Result of tree-sitter neighborhood evidence collection."""
    # ... existing fields from tree_sitter_contracts.py ...


# --- Capability Gates ---

def normalize_capability_snapshot(capabilities: dict[str, object]) -> dict[str, bool]:
    """Normalize LSP capability snapshot to boolean flags."""
    # ... existing logic from capability_gates.py ...


def plan_feasible_slices(
    capabilities: dict[str, bool], *, no_lsp: bool = False
) -> list[NeighborhoodSliceKind]:
    """Plan feasible neighborhood slices based on capability gates."""
    # ... existing logic from capability_gates.py ...
```

```python
# tools/cq/utils/uuid.py — consolidates UUID modules
# uuid_temporal_contracts_models.py (30 LOC) merges into uuid_temporal_contracts.py
# The resulting file is renamed to uuid.py alongside uuid_factory.py,
# OR uuid_temporal_contracts_models.py is simply absorbed into uuid_temporal_contracts.py

# Minimal change: just merge the models file into the contracts file
# tools/cq/utils/uuid_temporal_contracts.py — absorbs models
from __future__ import annotations

from tools.cq.core.structs import CqStruct


# --- Absorbed from uuid_temporal_contracts_models.py ---

class TemporalUuidInfoV1(CqStruct, frozen=True):
    version: int
    variant: str
    time_ms: int | None = None


class RunIdentityContractV1(CqStruct, frozen=True):
    run_id: str
    artifact_id: str
    # ... existing fields ...


# --- Existing uuid_temporal_contracts.py functions follow ---
# ... (unchanged) ...
```

### Files to Edit

- All importers of `cli_app.config` → repoint to `cli_app.infrastructure`
- All importers of `cli_app.groups` → repoint to `cli_app.infrastructure`
- All importers of `cli_app.decorators` → repoint to `cli_app.infrastructure`
- All importers of `cli_app.dispatch` → repoint to `cli_app.infrastructure`
- `tools/cq/cli.py` — route Cyclopts error handling through `cyclopts_error_payload()`
- All importers of `neighborhood.tree_sitter_contracts` → repoint to `neighborhood.contracts`
- All importers of `neighborhood.capability_gates` → repoint to `neighborhood.contracts`
- All importers of `utils.uuid_temporal_contracts_models` → repoint to `utils.uuid_temporal_contracts`
- `tools/cq/cli_app/__init__.py` — update re-exports
- `tools/cq/neighborhood/__init__.py` — update re-exports

### New Files to Create

- `tools/cq/cli_app/infrastructure.py` — merged CLI support
- `tools/cq/neighborhood/contracts.py` — merged neighborhood contracts
- `tests/unit/cq/cli/test_infrastructure.py`
- `tests/unit/cq/neighborhood/test_contracts.py`

### Legacy Decommission/Delete Scope

- Delete `tools/cq/cli_app/config.py` — absorbed into `infrastructure.py`
- Delete `tools/cq/cli_app/groups.py` — absorbed into `infrastructure.py`
- Delete `tools/cq/cli_app/decorators.py` — absorbed into `infrastructure.py`
- Delete `tools/cq/cli_app/dispatch.py` — absorbed into `infrastructure.py`
- Delete `tools/cq/neighborhood/tree_sitter_contracts.py` — absorbed into `contracts.py`
- Delete `tools/cq/neighborhood/capability_gates.py` — absorbed into `contracts.py`
- Delete `tools/cq/utils/uuid_temporal_contracts_models.py` — absorbed into `uuid_temporal_contracts.py`

---

## S9. Rust Fallback Policy Consolidation

### Goal

Replace 8 duplicated per-macro `_apply_rust_fallback` wrapper functions with one policy-driven
helper. Each macro currently defines its own copy of `_apply_rust_fallback` that calls
`apply_rust_macro_fallback()` from `multilang_fallback.py` — the only differences are the macro
name, pattern string, and optional summary key. A `RustFallbackPolicyV1` struct captures these
parameters, and a single `apply_rust_fallback_policy()` function replaces all 8 copies.

### Representative Code Snippets

```python
# tools/cq/macros/rust_fallback_policy.py
from __future__ import annotations

from tools.cq.core.structs import CqStruct
from tools.cq.core.schema import CqResult
from tools.cq.macros.multilang_fallback import apply_rust_macro_fallback


class RustFallbackPolicyV1(CqStruct, frozen=True):
    """Declarative Rust fallback parameters for a macro."""
    macro_name: str
    pattern: str
    query: str | None = None
    fallback_matches_summary_key: str | None = None


def apply_rust_fallback_policy(
    result: CqResult,
    *,
    root: str | object,
    policy: RustFallbackPolicyV1,
) -> CqResult:
    """Apply Rust fallback using policy parameters instead of per-macro wrappers."""
    summary = result.summary if isinstance(result.summary, dict) else {}
    fallback_matches = (
        int(summary.get(policy.fallback_matches_summary_key, 0))
        if policy.fallback_matches_summary_key
        else 0
    )
    return apply_rust_macro_fallback(
        result=result,
        root=root,
        pattern=policy.pattern,
        macro_name=policy.macro_name,
        query=policy.query,
        fallback_matches=fallback_matches,
    )
```

```python
# tools/cq/macros/calls.py — example consumer (before/after)
# BEFORE:
# def _apply_rust_fallback(result, *, root, pattern, ...):
#     ...
#     return apply_rust_macro_fallback(result=result, root=root, pattern=pattern, ...)
#
# AFTER:
from tools.cq.macros.rust_fallback_policy import RustFallbackPolicyV1, apply_rust_fallback_policy

_CALLS_RUST_FALLBACK = RustFallbackPolicyV1(
    macro_name="calls",
    pattern="$FUNC",
    fallback_matches_summary_key="rust_matches",
)

# In cmd_calls():
result = apply_rust_fallback_policy(result, root=root, policy=_CALLS_RUST_FALLBACK)
```

### Files to Edit

- `tools/cq/macros/calls.py` — replace `_apply_rust_fallback` with policy call
- `tools/cq/macros/impact.py` — replace `_apply_rust_fallback` with policy call
- `tools/cq/macros/imports.py` — replace `_apply_rust_fallback` with policy call
- `tools/cq/macros/exceptions.py` — replace `_apply_rust_fallback` with policy call
- `tools/cq/macros/sig_impact.py` — replace `_apply_rust_fallback` with policy call
- `tools/cq/macros/side_effects.py` — replace `_apply_rust_fallback` with policy call
- `tools/cq/macros/scopes.py` — replace `_apply_rust_fallback` with policy call
- `tools/cq/macros/bytecode.py` — replace `_apply_rust_fallback` with policy call

### New Files to Create

- `tools/cq/macros/rust_fallback_policy.py`
- `tests/unit/cq/macros/test_rust_fallback_policy.py`

### Legacy Decommission/Delete Scope

- Delete `_apply_rust_fallback` function from `tools/cq/macros/calls.py`
- Delete `_apply_rust_fallback` function from `tools/cq/macros/impact.py`
- Delete `_apply_rust_fallback` function from `tools/cq/macros/imports.py`
- Delete `_apply_rust_fallback` function from `tools/cq/macros/exceptions.py`
- Delete `_apply_rust_fallback` function from `tools/cq/macros/sig_impact.py`
- Delete `_apply_rust_fallback` function from `tools/cq/macros/side_effects.py`
- Delete `_apply_rust_fallback` function from `tools/cq/macros/scopes.py`
- Delete `_apply_rust_fallback` function from `tools/cq/macros/bytecode.py`

---

## S10. Unified Macro/Service Request Factory

### Goal

Create a single request-construction layer (`RequestContextV1` + `RequestFactory`) for CLI
commands, run-step execution, and report bundles. Currently, request structs are wired inline at
each callsite with repeated field mapping — the factory centralizes this so option-to-request
mapping remains consistent and new request types require only one new factory method.

### Representative Code Snippets

```python
# tools/cq/core/request_factory.py
from __future__ import annotations

from pathlib import Path

from tools.cq.core.structs import CqStruct
from tools.cq.core.toolchain import Toolchain


class RequestContextV1(CqStruct, frozen=True):
    """Shared context for request construction."""
    root: Path
    argv: list[str]
    tc: Toolchain


class RequestFactory:
    """Canonical request builder for CLI, run engine, and bundles."""

    @staticmethod
    def calls(ctx: RequestContextV1, *, function_name: str) -> object:
        from tools.cq.core.services import CallsServiceRequest
        return CallsServiceRequest(root=ctx.root, function_name=function_name, tc=ctx.tc, argv=ctx.argv)

    @staticmethod
    def impact(ctx: RequestContextV1, *, function_name: str, param_name: str, depth: int = 3) -> object:
        from tools.cq.macros.impact import ImpactRequest
        return ImpactRequest(
            tc=ctx.tc, root=ctx.root, argv=ctx.argv,
            function_name=function_name, param_name=param_name, max_depth=depth,
        )

    @staticmethod
    def search(ctx: RequestContextV1, *, query: str, lang_scope: str = "auto") -> object:
        from tools.cq.core.services import SearchServiceRequest
        return SearchServiceRequest(root=ctx.root, query=query, lang_scope=lang_scope, tc=ctx.tc, argv=ctx.argv)

    @staticmethod
    def sig_impact(ctx: RequestContextV1, *, function_name: str, new_signature: str) -> object:
        from tools.cq.macros.sig_impact import SigImpactRequest
        return SigImpactRequest(tc=ctx.tc, root=ctx.root, argv=ctx.argv,
                                function_name=function_name, new_signature=new_signature)
```

### Files to Edit

- `tools/cq/cli_app/commands/analysis.py` — replace inline request construction with factory calls
- `tools/cq/cli_app/commands/search.py` — replace inline request construction with factory calls
- `tools/cq/cli_app/commands/query.py` — replace inline request construction with factory calls
- `tools/cq/run/runner.py` — replace inline request construction with factory calls
- `tools/cq/core/bundles.py` — replace inline request construction with factory calls

### New Files to Create

- `tools/cq/core/request_factory.py`
- `tests/unit/cq/core/test_request_factory.py`

### Legacy Decommission/Delete Scope

- Delete repeated inline request-construction blocks in `tools/cq/cli_app/commands/analysis.py` once factory calls replace them
- Delete repeated inline request-construction blocks in `tools/cq/run/runner.py` once factory calls replace them
- Delete repeated inline request-construction blocks in `tools/cq/core/bundles.py` once factory calls replace them

---

## S11. Canonical Error Result Factory

### Goal

Standardize command error result creation (`RunContext`, `RunMeta`, `mk_result`,
`summary["error"]=...`) into one helper, eliminating repeated ad-hoc error response assembly.
Currently 5+ files construct error results with the same pattern; a single `build_error_result()`
replaces them all.

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
    """Build a canonical error CqResult with consistent structure."""
    run_ctx = RunContext.from_parts(root=root, argv=argv, tc=tc, started_ms=started_ms)
    result = mk_result(run_ctx.to_runmeta(macro))
    result.summary["error"] = str(error)
    return result
```

```python
# Consumer example — before/after
# BEFORE (repeated in each file):
#     run_ctx = RunContext.from_parts(root=root, argv=argv, tc=tc, started_ms=t0)
#     result = mk_result(run_ctx.to_runmeta("query"))
#     result.summary["error"] = str(exc)
#     return result
#
# AFTER:
from tools.cq.core.result_factory import build_error_result
return build_error_result(macro="query", root=root, argv=argv, tc=tc, started_ms=t0, error=exc)
```

### Files to Edit

- `tools/cq/cli_app/commands/query.py` — replace error result assembly with `build_error_result()`
- `tools/cq/cli_app/commands/report.py` — replace error result assembly
- `tools/cq/cli_app/commands/run.py` — replace error result assembly
- `tools/cq/run/runner.py` — replace error result assembly
- `tools/cq/query/executor.py` — replace error result assembly

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

## S12. CLI Step Parsing Unification

### Goal

Remove duplicated step payload dataclasses in the CLI layer (`*StepCli` in
`tools/cq/cli_app/contracts.py`) and decode `--step`/`--steps` directly into canonical `RunStep`
from `tools/cq/run/spec.py`. This eliminates the CLI-to-run-spec translation layer and ensures
step schemas are defined in one place.

### Representative Code Snippets

```python
# tools/cq/run/step_decode.py — canonical step decoding
from __future__ import annotations

from tools.cq.run.spec import RunStep, coerce_run_step
from tools.cq.core.typed_boundary import decode_json_strict


def parse_run_step_json(raw: str) -> RunStep:
    """Decode JSON string directly to canonical RunStep."""
    return decode_json_strict(raw, type_=RunStep)


def parse_run_steps_json(raw: str) -> list[RunStep]:
    """Decode JSON array string to list of canonical RunStep."""
    items = decode_json_strict(raw, type_=list)
    return [coerce_run_step(item) for item in items]
```

```python
# tools/cq/cli_app/commands/run.py — consumer (after)
from tools.cq.run.step_decode import parse_run_step_json, parse_run_steps_json

# --step parsing:
step = parse_run_step_json(raw_step_json)

# --steps parsing:
steps = parse_run_steps_json(raw_steps_json)
```

### Files to Edit

- `tools/cq/cli_app/params.py` — remove `RunStepCli` usage, use canonical decode
- `tools/cq/cli_app/options.py` — update step option type hints
- `tools/cq/cli_app/commands/run.py` — use canonical decode
- `tools/cq/run/loader.py` — simplify coercion with canonical decode

### New Files to Create

- `tools/cq/run/step_decode.py`
- `tests/unit/cq/run/test_step_decode.py`

### Legacy Decommission/Delete Scope

- Delete `*StepCli` dataclass duplicates from `tools/cq/cli_app/contracts.py`
- Delete `RunStepCli` usage in `tools/cq/cli_app/params.py`
- Delete dataclass-to-dict coercion bridging in `tools/cq/run/loader.py` once canonical decode is complete

---

## S13. CLI Protocol Output Helpers

### Goal

Unify `_text_result`, `_wants_json`, and JSON/text rendering helpers that are duplicated across
the `ldmd`, `artifact`, and `admin` protocol commands into one shared module.

### Representative Code Snippets

```python
# tools/cq/cli_app/protocol_output.py
from __future__ import annotations

import json

from tools.cq.cli_app.context import CliContext, CliResult, CliTextResult
from tools.cq.cli_app.types import OutputFormat


def wants_json(ctx: CliContext) -> bool:
    """Check if the output format requests JSON."""
    return ctx.output_format == OutputFormat.json


def text_result(
    ctx: CliContext,
    text: str,
    *,
    media_type: str = "text/plain",
    exit_code: int = 0,
) -> CliResult:
    """Build a CliResult wrapping plain text output."""
    return CliResult(
        result=CliTextResult(text=text, media_type=media_type),
        context=ctx,
        exit_code=exit_code,
        filters=None,
    )


def json_result(ctx: CliContext, payload: object, *, exit_code: int = 0) -> CliResult:
    """Build a CliResult wrapping JSON-formatted output."""
    return text_result(ctx, json.dumps(payload, indent=2), media_type="application/json", exit_code=exit_code)
```

### Files to Edit

- `tools/cq/cli_app/commands/ldmd.py` — replace local `_text_result` and `_wants_json`
- `tools/cq/cli_app/commands/artifact.py` — replace local `_text_result` and `_wants_json`
- `tools/cq/cli_app/commands/admin.py` — replace local `_emit_payload`

### New Files to Create

- `tools/cq/cli_app/protocol_output.py`
- `tests/unit/cq/cli/test_protocol_output.py`

### Legacy Decommission/Delete Scope

- Delete local `_text_result` and `_wants_json` in `tools/cq/cli_app/commands/ldmd.py`
- Delete local `_text_result` and `_wants_json` in `tools/cq/cli_app/commands/artifact.py`
- Delete local `_emit_payload` in `tools/cq/cli_app/commands/admin.py`

---

## S14. Shared Python AST Helper Utilities

### Goal

Create one canonical helper module for Python AST text/call-name parsing and migrate duplicated
helpers from macros and index modules. Currently `_safe_unparse` is duplicated in 5 files and
`_get_call_name` in 3 files — each copy is near-identical.

### Representative Code Snippets

```python
# tools/cq/core/python_ast_utils.py
from __future__ import annotations

import ast


def safe_unparse(node: ast.AST, *, default: str = "") -> str:
    """Safely unparse an AST node to source text, returning default on failure."""
    try:
        return ast.unparse(node)
    except (ValueError, TypeError):
        return default


def get_call_name(func: ast.expr) -> tuple[str, bool, str | None]:
    """Extract call name, whether it's an attribute call, and optional receiver.

    Returns (name, is_attribute, receiver_or_none).
    """
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

- `tools/cq/macros/calls.py` — replace local `_safe_unparse` and `_get_call_name`
- `tools/cq/macros/calls_target.py` — replace local `_get_call_name`
- `tools/cq/macros/impact.py` — replace local `_safe_unparse` and `_get_call_name`
- `tools/cq/macros/exceptions.py` — replace local `_safe_unparse`
- `tools/cq/macros/side_effects.py` — replace local `_safe_unparse`
- `tools/cq/index/call_resolver.py` — replace local `_safe_unparse` and `_get_call_name`
- `tools/cq/index/def_index.py` — replace local `_safe_unparse`
- `tools/cq/search/python/analysis_session.py` — replace local node-byte-span overlap
- `tools/cq/search/python/resolution_support.py` — replace local node-byte-span overlap

### New Files to Create

- `tools/cq/core/python_ast_utils.py`
- `tests/unit/cq/core/test_python_ast_utils.py`

### Legacy Decommission/Delete Scope

- Delete duplicate `_safe_unparse` from:
  - `tools/cq/macros/calls.py`
  - `tools/cq/macros/impact.py`
  - `tools/cq/macros/exceptions.py`
  - `tools/cq/macros/side_effects.py`
  - `tools/cq/index/call_resolver.py`
  - `tools/cq/index/def_index.py`
- Delete duplicate `_get_call_name` from:
  - `tools/cq/macros/calls.py`
  - `tools/cq/macros/calls_target.py`
  - `tools/cq/macros/impact.py`
  - `tools/cq/index/call_resolver.py`

---

## S15. Tree-Sitter Node Text/Span Utilities

### Goal

Unify tree-sitter node text/span/point extraction across neighborhood, lane runtime, tags, and
structural modules by centralizing in one helper module with a `NodeLike` protocol and canonical
`node_text()` / `node_byte_span()` functions. Currently 9+ files define their own local copies.

### Representative Code Snippets

```python
# tools/cq/search/tree_sitter/core/node_utils.py
from __future__ import annotations

from typing import Protocol


class NodeLike(Protocol):
    """Structural protocol for tree-sitter-like node objects."""
    start_byte: int
    end_byte: int
    start_point: tuple[int, int]
    end_point: tuple[int, int]


def node_text(
    node: NodeLike | None,
    source_bytes: bytes,
    *,
    strip: bool = True,
    max_len: int | None = None,
) -> str:
    """Extract UTF-8 text from a node's byte span."""
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
    """Extract (start_byte, end_byte) from a node, defaulting to (0, 0)."""
    if node is None:
        return 0, 0
    start = int(getattr(node, "start_byte", 0))
    end = int(getattr(node, "end_byte", start))
    return start, max(start, end)
```

### Files to Edit

- `tools/cq/search/tree_sitter/core/text_utils.py` — migrate importers then delete
- `tools/cq/search/tree_sitter/python_lane/runtime.py` — replace local `_node_text`
- `tools/cq/search/tree_sitter/python_lane/facts.py` — replace local `_node_text`
- `tools/cq/search/tree_sitter/python_lane/fallback_support.py` — replace local `_node_text`
- `tools/cq/search/tree_sitter/python_lane/locals_index.py` — replace local `_node_text`
- `tools/cq/search/tree_sitter/rust_lane/runtime.py` — replace local `_node_text`
- `tools/cq/search/tree_sitter/rust_lane/injections.py` — replace local `_node_text`
- `tools/cq/search/tree_sitter/tags/runtime.py` — replace local node text helpers
- `tools/cq/neighborhood/tree_sitter_collector.py` — replace local `_node_text`
- `tools/cq/neighborhood/tree_sitter_neighborhood_query_engine.py` — replace local `_node_text`
- `tools/cq/search/tree_sitter/structural/match_rows.py` — replace local `_node_text`

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
  - `tools/cq/search/tree_sitter/structural/match_rows.py`
- Delete `tools/cq/search/tree_sitter/core/text_utils.py` after all imports move to `node_utils.py`
- Delete overlapping node-text helper in `tools/cq/search/_shared/core.py` once tree-sitter path is canonical

---

## S16. Shared Path/Glob/Relative Utilities

### Goal

Eliminate duplicated path normalization and glob matching logic across query execution, query
parsing, batch selection, and index file discovery. Consolidate into one helper module and adopt
`pathspec` where it improves correctness/performance for repeated pattern sets.

### Representative Code Snippets

```python
# tools/cq/core/pathing.py
from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

from pathspec import PathSpec


def normalize_repo_relative_path(file_path: str | Path, *, root: Path) -> str:
    """Normalize a file path to repo-relative POSIX form when possible."""
    path = Path(file_path)
    if path.is_absolute():
        try:
            return path.relative_to(root).as_posix()
        except ValueError:
            return path.as_posix()
    return path.as_posix()


def is_relative_to(path: Path, root: Path) -> bool:
    """Return whether `path` is under `root` without raising ValueError."""
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def match_ordered_globs(rel_path: str, globs: Sequence[str]) -> bool:
    """Apply CQ's ordered include/negated-include glob semantics."""
    if not globs:
        return True
    has_includes = any(not glob.startswith("!") for glob in globs)
    include = not has_includes
    for glob in globs:
        negated = glob.startswith("!")
        pattern = glob[1:] if negated else glob
        if Path(rel_path).match(pattern):
            include = not negated
    return include


def compile_gitwildmatch(patterns: Sequence[str]) -> PathSpec:
    """Compile reusable gitwildmatch spec for high-volume match loops."""
    return PathSpec.from_lines("gitwildmatch", patterns)
```

### Files to Edit

- `tools/cq/query/executor.py` — replace local `_normalize_match_file`
- `tools/cq/query/sg_parser.py` — replace local `_normalize_match_file`
- `tools/cq/query/batch.py` — replace local `_matches_globs`, `_is_relative_to`, `_rel_path`
- `tools/cq/index/files.py` — replace local `_matches_globs`, `_is_relative_to`
- `tools/cq/query/batch_spans.py` — consume canonical path normalization helper

### New Files to Create

- `tools/cq/core/pathing.py`
- `tests/unit/cq/core/test_pathing.py`

### Legacy Decommission/Delete Scope

- Delete duplicate `_normalize_match_file` from:
  - `tools/cq/query/executor.py`
  - `tools/cq/query/sg_parser.py`
- Delete duplicate `_matches_globs` from:
  - `tools/cq/query/batch.py`
  - `tools/cq/index/files.py`
- Delete duplicate `_is_relative_to` from:
  - `tools/cq/query/batch.py`
  - `tools/cq/index/files.py`

---

## S17. Unified Target Spec Parsing Contracts

### Goal

Consolidate duplicated target-spec parsing in `core/bundles.py` and
`neighborhood/target_resolution.py` into one canonical parser contract that supports both
`kind:value` bundle targets and `file[:line[:col]]` neighborhood anchors.

### Representative Code Snippets

```python
# tools/cq/core/target_specs.py
from __future__ import annotations

from typing import Literal

from tools.cq.core.structs import CqStruct

BundleTargetKind = Literal["function", "class", "method", "module", "path"]


class TargetSpecV1(CqStruct, frozen=True):
    raw: str
    bundle_kind: BundleTargetKind | None = None
    bundle_value: str | None = None
    target_name: str | None = None
    target_file: str | None = None
    target_line: int | None = None
    target_col: int | None = None


def parse_target_spec(raw: str) -> TargetSpecV1:
    """Parse CQ target syntax shared by bundles and neighborhood commands."""
    # 1) Parse bundle form: `kind:value`
    # 2) Parse anchor form: `file:line[:col]`
    # 3) Fallback to symbol target
    # ... shared parsing logic ...
```

### Files to Edit

- `tools/cq/core/bundles.py` — replace local `TargetSpec` + `parse_target_spec`
- `tools/cq/neighborhood/target_resolution.py` — replace local `TargetSpec` + parser
- `tools/cq/cli_app/commands/neighborhood.py` — consume unified spec contract if needed

### New Files to Create

- `tools/cq/core/target_specs.py`
- `tests/unit/cq/core/test_target_specs.py`

### Legacy Decommission/Delete Scope

- Delete local target spec parser from `tools/cq/core/bundles.py`
- Delete local target spec parser from `tools/cq/neighborhood/target_resolution.py`

---

## S18. Shared Tree-Sitter Query Compilation Helper

### Goal

Collapse duplicated `_compile_query` logic across Python lane runtime/facts, Rust lane runtime,
and neighborhood query engine into one validated query compiler that applies pack rules and query
specialization consistently.

### Representative Code Snippets

```python
# tools/cq/search/tree_sitter/query/compiler.py
from __future__ import annotations

from functools import lru_cache
from typing import TYPE_CHECKING

from tools.cq.search.tree_sitter.query.specialization import specialize_query
from tools.cq.search.tree_sitter.contracts.query_models import load_pack_rules
from tools.cq.search.tree_sitter.core.infrastructure import load_language

if TYPE_CHECKING:
    from tree_sitter import Query


@lru_cache(maxsize=256)
def compile_query(
    *,
    language: str,
    pack_name: str,
    source: str,
    request_surface: str = "artifact",
    validate_rules: bool = True,
) -> Query:
    """Compile and specialize a query with shared rooted/non-local validation."""
    from tree_sitter import Query as TreeSitterQuery

    query = TreeSitterQuery(load_language(language), source)
    if validate_rules:
        rules = load_pack_rules(language)
        pattern_count = int(getattr(query, "pattern_count", 0))
        for pattern_idx in range(pattern_count):
            if rules.require_rooted and not bool(query.is_pattern_rooted(pattern_idx)):
                raise ValueError(f"{language} query pattern not rooted: {pattern_idx}")
            if rules.forbid_non_local and bool(query.is_pattern_non_local(pattern_idx)):
                raise ValueError(f"{language} query pattern non-local: {pattern_idx}")
    return specialize_query(query, request_surface=request_surface)
```

### Files to Edit

- `tools/cq/search/tree_sitter/python_lane/runtime.py` — delete local `_compile_query`
- `tools/cq/search/tree_sitter/python_lane/facts.py` — delete local `_compile_query`
- `tools/cq/search/tree_sitter/rust_lane/runtime.py` — delete local `_compile_query`
- `tools/cq/neighborhood/tree_sitter_neighborhood_query_engine.py` — delete local `_compile_query`

### New Files to Create

- `tools/cq/search/tree_sitter/query/compiler.py`
- `tests/unit/cq/search/tree_sitter/query/test_compiler.py`

### Legacy Decommission/Delete Scope

- Delete duplicated `_compile_query` helpers from:
  - `tools/cq/search/tree_sitter/python_lane/runtime.py`
  - `tools/cq/search/tree_sitter/python_lane/facts.py`
  - `tools/cq/search/tree_sitter/rust_lane/runtime.py`
  - `tools/cq/neighborhood/tree_sitter_neighborhood_query_engine.py`

---

## S19. UUID Temporal Helper Consolidation

### Goal

Consolidate UUID temporal metadata contracts and timestamp helpers into one canonical surface.
Remove the duplicate v7 timestamp functions by making `uuid_temporal_contracts.py` depend on
`uuid_factory.uuid7_time_ms`.

### Representative Code Snippets

```python
# tools/cq/utils/uuid_temporal_contracts.py
from __future__ import annotations

import uuid

from tools.cq.utils.uuid_factory import uuid7_time_ms
# RunIdentityContractV1 and TemporalUuidInfoV1 are declared in this module after S8 merge.


def uuid_time_millis(value: uuid.UUID) -> int | None:
    """Compatibility wrapper delegating to canonical factory helper."""
    return uuid7_time_ms(value)
```

### Files to Edit

- `tools/cq/utils/uuid_temporal_contracts.py` — delegate to `uuid7_time_ms`
- `tools/cq/utils/uuid_factory.py` — keep canonical timestamp helper
- `tools/cq/core/contracts.py` and callers — use one temporal contract import surface

### New Files to Create

- None (scope is consolidation within existing UUID modules)
- `tests/unit/cq/utils/test_uuid_temporal_contracts.py` — add/expand timestamp parity tests

### Legacy Decommission/Delete Scope

- Delete duplicate timestamp extraction logic from `tools/cq/utils/uuid_temporal_contracts.py`
- If `uuid_temporal_contracts_models.py` is fully absorbed (S8), delete that file

---

## S20. Msgspec-First Contract Hardening

### Goal

Expand msgspec utilization for better safety/performance and less ad-hoc schema wiring. This scope
standardizes default factories, introspection, conversion strictness, and high-throughput encoding
in CQ hot paths.

### Representative Code Snippets

```python
# tools/cq/core/msgspec_utils.py
from __future__ import annotations

import msgspec
from msgspec import inspect, structs


def struct_field_names(struct_type: type[msgspec.Struct]) -> tuple[str, ...]:
    """Return declared field names for schema-aware validation/rendering."""
    return tuple(field.name for field in structs.fields(struct_type))


def union_schema_summary(types: tuple[type[msgspec.Struct], ...]) -> dict[str, object]:
    """Build lightweight runtime schema summary for debug/telemetry surfaces."""
    infos = inspect.multi_type_info(list(types))
    return {"variant_count": len(infos), "variants": [type(info).__name__ for info in infos]}


def decode_raw_json_blob(data: bytes) -> msgspec.Raw:
    """Defer full decoding for pass-through payload sections."""
    return msgspec.json.decode(data, type=msgspec.Raw)
```

```python
# High-throughput JSON encoding pattern for artifact/protocol output
import msgspec

ENCODER = msgspec.json.Encoder()

def encode_payload_into(buffer: bytearray, payload: object) -> bytes:
    ENCODER.encode_into(payload, buffer)
    return bytes(buffer)
```

### Files to Edit

- `tools/cq/search/pipeline/models.py` and merged contract modules — enforce `msgspec.field(default_factory=...)`
- `tools/cq/run/step_decode.py` — optionally leverage msgspec introspection for union decode diagnostics
- `tools/cq/cli_app/protocol_output.py` — use encoder reuse for JSON payload paths
- `tools/cq/core/contract_codec.py` — centralize strict/non-strict conversion strategy docs/helpers

### New Files to Create

- `tools/cq/core/msgspec_utils.py`
- `tests/unit/cq/core/test_msgspec_utils.py`

### Legacy Decommission/Delete Scope

- Remove remaining mutable literal defaults in msgspec structs
- Remove duplicated per-module schema-field enumeration helpers once `msgspec_utils.py` is canonical

---

## Cross-Scope Legacy Decommission and Deletion Plan

### Batch D1 (after S1, S2, S10, S11)

Core module cleanup — safe only after serialization, contract consolidation, and factory scope items land:

- Remove stale re-exports from `tools/cq/core/__init__.py` that reference deleted modules
  (`codec`, `public_serialization`, `json_types`, `requests`, `uuid_contracts`)
- Delete empty `tools/cq/core/id/` directory (after flat `id.py` is confirmed working)
- Delete empty `tools/cq/core/services/` directory (after flat `services.py` is confirmed working)
- Verify `RequestFactory` and `build_error_result()` callsites have replaced all inline construction

### Batch D2 (after S4, S5, S15, S18)

Tree-sitter package cleanup — safe only after core, peripheral, and node utility consolidation land:

- Delete empty `tools/cq/search/tree_sitter/diagnostics/` directory
- Delete empty `tools/cq/search/tree_sitter/tags/` directory
- Clean up `tools/cq/search/tree_sitter/core/__init__.py` to remove stale re-exports
- Clean up `tools/cq/search/tree_sitter/query/__init__.py` to remove stale re-exports
- Verify local `_compile_query` helpers are removed from lane/neighborhood modules
- Verify all `_node_text` / `_node_byte_span` local copies are deleted

### Batch D3 (after S7, S9)

Macro package cleanup:

- Delete empty `tools/cq/macros/common/` directory
- Verify all 8 per-macro `_apply_rust_fallback` functions are deleted and `RustFallbackPolicyV1` is canonical

### Batch D4 (after S8, S12, S13)

CLI and peripheral cleanup:

- Verify `*StepCli` dataclass duplicates are deleted from `tools/cq/cli_app/contracts.py`
- Verify local `_text_result` / `_wants_json` / `_emit_payload` are deleted from command files
- Verify Cyclopts exception reporting flows through one normalized payload helper
- Clean up `tools/cq/cli_app/__init__.py` and `tools/cq/neighborhood/__init__.py` re-exports

### Batch D5 (after S14)

AST helper deduplication verification:

- Verify all `_safe_unparse` copies deleted from 6 macro/index files
- Verify all `_get_call_name` copies deleted from 4 macro/index files

### Batch D6 (after S16, S17, S18, S19)

Cross-cutting helper deduplication verification:

- Verify `_normalize_match_file` duplication is removed from `query/executor.py` and `query/sg_parser.py`
- Verify `_matches_globs` duplication is removed from `query/batch.py` and `index/files.py`
- Verify target-spec parsing duplication is removed from `core/bundles.py` and `neighborhood/target_resolution.py`
- Verify UUID v7 timestamp extraction has one canonical implementation

### Batch D7 (after all scope items S1-S20)

Final audit:

- Run `find tools/cq -name '*.py' -size 0` to find any empty files created during merges
- Run `find tools/cq -type d -empty` to find empty directories left behind
- Verify no `__init__.py` files contain stale re-exports of deleted modules
- Update `docs/architecture/cq/` documentation to reflect new module paths
- Validate net file reduction target (~50-55 files)

---

## Implementation Sequence

**Foundation helpers first** — shared utilities should land before the modules that consume them,
so deduplication is available while merging files and deleting legacy helpers.

1. **S11: Error Result Factory** — No dependencies; reduces repeated failure-path code everywhere.
2. **S14: Python AST Helpers** — No dependencies; unblocks macro/index dedup.
3. **S15: Tree-Sitter Node Utils** — No dependencies; unblocks tree-sitter dedup.
4. **S16: Shared Path/Glob Utilities** — No dependencies; unblocks query/index dedup.
5. **S1: Core Serialization Authority** — Foundation; no reverse dependencies. Unblocks S2.
6. **S3: Cache Contract + Settings Factory** — Independent; can run parallel with S1.
7. **S2: Core Contract, Identity & Services** — Depends on S1 (clean codec imports).
8. **S10: Request Factory** — Depends on S2 (service request types are centralized).
9. **S9: Rust Fallback Policy** — Independent of S1-S3; natural companion to S7.
10. **S7: Macro Infrastructure** — Depends on S14 and S9.
11. **S4: Tree-Sitter Core Utilities** — Depends on S15 for node utility migration target.
12. **S18: Shared Query Compiler** — Depends on S4 language/runtime surfaces.
13. **S8: CLI & Peripheral** — Independent of tree-sitter scopes.
14. **S12: CLI Step Parsing Unification** — Depends on S8.
15. **S13: CLI Protocol Output Helpers** — Depends on S8.
16. **S5: Tree-Sitter Query & Peripheral** — Depends on S4 and benefits from S18.
17. **S6: Search Pipeline & Lane** — Depends on S5 and S14.
18. **S17: Unified Target Spec Parsing** — Depends on S16 pathing utility.
19. **S19: UUID Temporal Consolidation** — Depends on S8 UUID module consolidation decisions.
20. **S20: Msgspec Hardening** — Final cross-cutting pass after merged modules settle.

**Parallel groups:**
- Wave 1: {S11, S14, S15, S16} (independent foundational helpers)
- Wave 2: {S1, S3, S9} (independent foundations, parallel with each other)
- Wave 3: {S2, S4, S7, S8} (after Wave 1-2 prerequisites)
- Wave 4: {S10, S12, S13, S18} (after Wave 3 prerequisites)
- Wave 5: {S5, S6, S17, S19} (after scope-specific prerequisites)
- Wave 6: {S20} (cross-cutting hardening after structural moves)
- Wave 7: {D1, D2, D3, D4, D5, D6, D7} (after all scope items)

**Estimated impact:**
- Files deleted: ~66-72
- Files created: ~21 (merged targets + new helpers) + ~28 (tests)
- Net file reduction: ~50-55 files (305 → ~250-255)
- Duplicated helper families removed: `_apply_rust_fallback`, `_safe_unparse`, `_get_call_name`,
  `_node_text`, `_normalize_match_file`, `_matches_globs`, `_compile_query`, UUID v7 timestamp
  extraction duplicates
- LOC change: Net negative with higher reuse and lower import indirection

---

## Implementation Checklist

**File merging & consolidation (S1-S8):**
- [ ] S1: Core Serialization Authority Completion (merge codec.py, public_serialization.py, json_types.py)
- [ ] S2: Core Contract, Identity & Services Consolidation (merge requests.py, uuid_contracts.py; flatten id/, services/)
- [ ] S3: Cache Contract + Settings Factory (merge 5 tiny contract files, add SettingsFactory)
- [ ] S4: Tree-Sitter Core Utility Consolidation (merge 7 files → 2 modules; text_utils moved in S15)
- [ ] S5: Tree-Sitter Query & Peripheral Package Consolidation (merge query utils, flatten diagnostics/tags/schema, structural exports, injection config)
- [ ] S6: Search Pipeline & Lane Module Consolidation (merge pipeline, Python, Rust small files)
- [ ] S7: Macro Infrastructure Consolidation (merge utilities, delete re-export layer)
- [ ] S8: CLI & Peripheral Module Consolidation (CLI infrastructure, neighborhood contracts, UUID model merge)

**Deduplication & shared helpers (S9-S20):**
- [ ] S9: Rust Fallback Policy Consolidation (replace 8 `_apply_rust_fallback` with `RustFallbackPolicyV1`)
- [ ] S10: Unified Macro/Service Request Factory (`RequestContextV1` + `RequestFactory`)
- [ ] S11: Canonical Error Result Factory (`build_error_result()`)
- [ ] S12: CLI Step Parsing Unification (eliminate `*StepCli` duplicates, use canonical `RunStep`)
- [ ] S13: CLI Protocol Output Helpers (`wants_json()`, `text_result()`, `json_result()`)
- [ ] S14: Shared Python AST Helper Utilities (`safe_unparse()`, `get_call_name()`)
- [ ] S15: Tree-Sitter Node Text/Span Utilities (`node_text()`, `node_byte_span()`, `NodeLike`)
- [ ] S16: Shared Path/Glob/Relative Utilities (`normalize_repo_relative_path()`, `match_ordered_globs()`, `is_relative_to()`)
- [ ] S17: Unified Target Spec Parsing Contracts (`TargetSpecV1`, shared `parse_target_spec()`)
- [ ] S18: Shared Tree-Sitter Query Compilation Helper (`compile_query()`)
- [ ] S19: UUID Temporal Helper Consolidation (`uuid_time_millis()` delegates to `uuid7_time_ms()`)
- [ ] S20: Msgspec-First Contract Hardening (`msgspec_utils`, encoder reuse, struct introspection)

**Decommission batches:**
- [ ] D1: Core module cleanup (after S1, S2, S10, S11)
- [ ] D2: Tree-sitter package cleanup (after S4, S5, S15, S18)
- [ ] D3: Macro package cleanup (after S7, S9)
- [ ] D4: CLI and peripheral cleanup (after S8, S12, S13)
- [ ] D5: AST helper deduplication verification (after S14)
- [ ] D6: Cross-cutting helper dedup verification (after S16, S17, S18, S19)
- [ ] D7: Final audit (after all scope items S1-S20)
