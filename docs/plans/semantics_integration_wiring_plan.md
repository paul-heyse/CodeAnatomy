# Semantics Module Integration Wiring Plan

**Created:** 2025-01-31
**Status:** In Progress (updated to reflect implementation in repo)
**Scope:** Wire `src/semantics/` into extraction, relspec, Hamilton, and observability layers

---

## Executive Summary

The `src/semantics/` module provides a centralized, rule-based approach for building CPG relationships from extraction outputs. While architecturally complete, it is **entirely disconnected** from the execution pipeline. This plan details the four-phase integration effort to make semantics the sole relationship-building path.

### Current Architecture (Two Parallel Paths)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        LEGACY PATH (Currently Active)                        │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  cpg/relationship_specs.py ──► cpg/relationship_builder.py                  │
│           │                              │                                  │
│           ▼                              ▼                                  │
│  relspec/relationship_datafusion.py ──► ViewNode registration               │
│           │                              │                                  │
│           ▼                              ▼                                  │
│  Hamilton DAG ──────────────────► relation_output_v1                        │
│           │                              │                                  │
│           ▼                              ▼                                  │
│  cpg/view_builders_df.py ───────► CPG nodes/edges                           │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                      SEMANTICS PATH (Not Wired)                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  semantics/compiler.py ──► SemanticCompiler.normalize/relate                │
│           │                              │                                  │
│           ▼                              ▼                                  │
│  semantics/pipeline.py ──────► build_cpg() creates views                    │
│           │                              │                                  │
│           ▼                              ▼                                  │
│        ??? ◄─────────────────── Views never registered anywhere             │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Target Architecture (Post-Integration)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      UNIFIED SEMANTICS PATH                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Extraction ──► cst_refs, cst_defs, scip_occurrences, file_line_index_v1    │
│       │                                                                     │
│       ▼                                                                     │
│  semantics/input_registry.py ──► Validates & maps extraction outputs        │
│       │                                                                     │
│       ▼                                                                     │
│  semantics/compiler.py ──► normalize() → relate() → union()                 │
│       │                     (instrumented with OpenTelemetry)               │
│       ▼                                                                     │
│  datafusion_engine/views/registry_specs.py ──► Registers semantic views     │
│       │                                                                     │
│       ▼                                                                     │
│  Hamilton DAG ──► consumes registered semantic views (no re‑registration)   │
│       │                                                                     │
│       ▼                                                                     │
│  relation_output_v1, cpg_nodes, cpg_edges                                   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Phase 1: Foundation (Prerequisites)

**Goal:** Establish the foundational components that all subsequent phases depend on.

### Task 1.1: Verify/Create `file_line_index_v1` Extraction  
**Status:** ✅ DONE — extractor exists and is wired into repo scan/materialization, templates, and `extract/__init__.py`.

**Priority:** 🔴 CRITICAL (Blocker)
**Effort:** Medium
**Files:**
- `src/datafusion_engine/extract/extractors.py` (investigate)
- `src/datafusion_engine/extract/templates.py:487` (schema exists)
- `src/semantics/scip_normalize.py:24` (consumer)

**Context:**
The SCIP normalization step converts line/column positions to byte offsets using a line index table:

```python
# semantics/scip_normalize.py:20-25
def scip_to_byte_offsets(
    ctx: SessionContext,
    *,
    occurrences_table: str = "scip_occurrences",
    line_index_table: str = "file_line_index_v1",  # ← REQUIRED
) -> DataFrame:
```

The schema is defined in `templates.py` (current fields):

```python
# datafusion_engine/extract/templates.py:480+
{
    **base,
    "name": "file_line_index_v1",
    "fields": [
        "line_no",
        "line_start_byte",
        "line_end_byte",
        "line_text",
        "newline_kind",
    ],
    "ordering_keys": [
        {"column": "path", "order": "ascending"},
        {"column": "line_no", "order": "ascending"},
    ],
    "join_keys": ["file_id", "line_no"],
}
```

**Implementation Notes (Completed):**
1. Extractor implemented in `src/extract/extractors/file_index/line_index.py`
2. Wired into repo scan (`src/extract/scanning/repo_scan.py`)
3. Exported via `src/extract/__init__.py`
4. Registered in extraction templates (`src/datafusion_engine/extract/templates.py`)

**Implementation (Reference):**

```python
# src/extract/extractors/file_index/line_index.py (NEW)
"""Extract line index from source files for byte offset conversion."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyarrow as pa

if TYPE_CHECKING:
    from pathlib import Path

def extract_file_line_index(
    file_path: Path,
    file_id: str,
    *,
    encoding: str = "utf-8",
) -> pa.Table:
    """Extract line index from a source file.

    Parameters
    ----------
    file_path
        Path to the source file.
    file_id
        Unique file identifier.
    encoding
        File encoding for reading.

    Returns
    -------
    pa.Table
        Line index table with columns:
        - file_id: str
        - path: str
        - line_no: int64 (0-indexed)
        - line_start_byte: int64
        - line_text: str
    """
    content = file_path.read_bytes()

    rows: list[dict] = []
    byte_offset = 0
    line_no = 0
    start = 0
    size = len(content)
    while start < size:
        newline_idx = content.find(b"\n", start)
        if newline_idx == -1:
            line_bytes = content[start:]
            newline_kind = "none"
            end = size
            next_start = size
        else:
            end = newline_idx + 1
            line_bytes = content[start:end]
            if line_bytes.endswith(b"\r\n"):
                newline_kind = "crlf"
                line_text_bytes = line_bytes[:-2]
            else:
                newline_kind = "lf"
                line_text_bytes = line_bytes[:-1]
            next_start = end

        if newline_idx == -1:
            line_text_bytes = line_bytes
        line_text = line_text_bytes.decode(encoding, errors="replace")

        rows.append(
            {
                "file_id": file_id,
                "path": str(file_path),
                "line_no": line_no,
                "line_start_byte": byte_offset,
                "line_end_byte": byte_offset + len(line_bytes),
                "line_text": line_text,
                "newline_kind": newline_kind,
            }
        )
        byte_offset += len(line_bytes)
        line_no += 1
        start = next_start

    return pa.Table.from_pylist(rows, schema=FILE_LINE_INDEX_SCHEMA)

FILE_LINE_INDEX_SCHEMA = pa.schema([
    ("file_id", pa.string()),
    ("path", pa.string()),
    ("line_no", pa.int64()),
    ("line_start_byte", pa.int64()),
    ("line_end_byte", pa.int64()),
    ("line_text", pa.string()),
    ("newline_kind", pa.string()),
])
```

**Acceptance Criteria:**
- [x] `file_line_index_v1` table is produced during extraction with `line_end_byte` + `newline_kind`
- [x] Table is registered in DataFusion context before semantic pipeline runs
- [x] `scip_to_byte_offsets()` succeeds without `ValueError`

---

### Task 1.2: Add `SCOPE_SEMANTICS` to Observability Scopes  
**Status:** ✅ DONE — `ScopeName.SEMANTICS` added and `SCOPE_SEMANTICS` exported.

**Priority:** 🟡 MEDIUM
**Effort:** Low
**Files:**
- `src/obs/otel/constants.py` (modify)
- `src/obs/otel/scopes.py` (modify)

**Current scopes:**

```python
# obs/otel/constants.py (partial)
class ScopeName(StrEnum):
    # ...
    SEMANTICS = "codeanatomy.semantics"

# obs/otel/scopes.py (partial)
SCOPE_EXTRACT = "extract"
SCOPE_NORMALIZE = "normalize"
SCOPE_CPG = "cpg"
SCOPE_HAMILTON = "hamilton"
# ... etc
```

**Implementation:**

```python
# obs/otel/scopes.py (add)
SCOPE_SEMANTICS = ScopeName.SEMANTICS
```

**Acceptance Criteria:**
- [x] `ScopeName.SEMANTICS` added in `obs/otel/constants.py`
- [x] `SCOPE_SEMANTICS` exported from `obs/otel/scopes.py`
- [x] Scope follows existing naming conventions

---

### Task 1.3: Create `semantics/input_registry.py`  
**Status:** ✅ DONE — registry is used by the pipeline and CDF mappings are resolved via runtime profile integration.

**Priority:** 🟡 MEDIUM
**Effort:** Medium
**Files:**
- `src/semantics/input_registry.py` (NEW)
- `src/semantics/pipeline.py` (modify to use registry)

**Purpose:**
Map extraction outputs to semantic inputs with validation, replacing hardcoded table names. This registry must also respect CDF mappings produced by `semantics.pipeline._resolve_cdf_inputs()` so the semantic pipeline stays consistent with runtime profile CDF registration.

**Implementation:**

```python
# src/semantics/input_registry.py (NEW)
"""Input registry for semantic pipeline - maps extraction outputs to semantic inputs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from datafusion import SessionContext


@dataclass(frozen=True)
class SemanticInputSpec:
    """Specification for a semantic pipeline input table."""

    canonical_name: str
    """Canonical name used by semantic compiler."""

    extraction_source: str
    """Name of the extraction output table."""

    required: bool = True
    """Whether this input is required for pipeline execution."""

    fallback_names: tuple[str, ...] = ()
    """Alternative table names to try if primary is missing."""


# Canonical input specifications
SEMANTIC_INPUT_SPECS: Final[tuple[SemanticInputSpec, ...]] = (
    SemanticInputSpec(
        canonical_name="cst_refs",
        extraction_source="cst_refs",
        required=True,
    ),
    SemanticInputSpec(
        canonical_name="cst_defs",
        extraction_source="cst_defs",
        required=True,
    ),
    SemanticInputSpec(
        canonical_name="cst_imports",
        extraction_source="cst_imports",
        required=True,
    ),
    SemanticInputSpec(
        canonical_name="cst_callsites",
        extraction_source="cst_callsites",
        required=True,
    ),
    SemanticInputSpec(
        canonical_name="scip_occurrences",
        extraction_source="scip_occurrences",
        required=True,
    ),
    SemanticInputSpec(
        canonical_name="file_line_index_v1",
        extraction_source="file_line_index_v1",
        required=True,  # Required for SCIP byte offset conversion
    ),
)


@dataclass(frozen=True)
class InputValidationResult:
    """Result of validating semantic pipeline inputs."""

    valid: bool
    missing_required: tuple[str, ...]
    missing_optional: tuple[str, ...]
    resolved_names: dict[str, str]


def validate_semantic_inputs(ctx: SessionContext) -> InputValidationResult:
    """Validate that all required semantic inputs are available.

    Parameters
    ----------
    ctx
        DataFusion session context with registered tables.

    Returns
    -------
    InputValidationResult
        Validation result with missing tables and resolved names.
    """
    from datafusion_engine.schema.introspection import table_names_snapshot

    available = set(table_names_snapshot(ctx))
    missing_required: list[str] = []
    missing_optional: list[str] = []
    resolved: dict[str, str] = {}

    for spec in SEMANTIC_INPUT_SPECS:
        # Try primary name first
        if spec.extraction_source in available:
            resolved[spec.canonical_name] = spec.extraction_source
            continue

        # Try fallback names
        found = False
        for fallback in spec.fallback_names:
            if fallback in available:
                resolved[spec.canonical_name] = fallback
                found = True
                break

        if not found:
            if spec.required:
                missing_required.append(spec.canonical_name)
            else:
                missing_optional.append(spec.canonical_name)

    return InputValidationResult(
        valid=len(missing_required) == 0,
        missing_required=tuple(missing_required),
        missing_optional=tuple(missing_optional),
        resolved_names=resolved,
    )


def require_semantic_inputs(ctx: SessionContext) -> dict[str, str]:
    """Validate inputs and return resolved name mapping.

    Raises
    ------
    ValueError
        If required inputs are missing.
    """
    result = validate_semantic_inputs(ctx)
    if not result.valid:
        msg = f"Missing required semantic inputs: {result.missing_required}"
        raise ValueError(msg)
    return result.resolved_names


__all__ = [
    "InputValidationResult",
    "SEMANTIC_INPUT_SPECS",
    "SemanticInputSpec",
    "require_semantic_inputs",
    "validate_semantic_inputs",
]
```

**Acceptance Criteria:**
- [x] `validate_semantic_inputs()` reports missing tables
- [x] `require_semantic_inputs()` raises clear error on missing inputs
- [x] `pipeline.py` uses registry instead of hardcoded `CPG_INPUT_TABLES`
- [x] Registry honors `cdf_inputs` mapping from runtime profile

---

### Task 1.4: Integrate Semantics with Existing View Graph Registration  
**Status:** ✅ DONE — semantic views are registered via `_semantics_view_nodes()` in `registry_specs.py` and consumed by Hamilton; legacy `_relspec_view_nodes()` has been removed.

**Priority:** 🔴 HIGH  
**Effort:** Medium  
**Files:**
- `src/datafusion_engine/views/registry_specs.py` (modify)
- `src/semantics/pipeline.py` (source of builders)

**Purpose:**  
Reuse the existing view‑graph system (`ViewNode`, plan bundles, schema contracts) instead of creating a parallel `semantics/view_registry.py`.

**Implementation Notes (Completed):**
- `_semantics_view_nodes()` is the sole registration point for semantic views.
- `view_graph_nodes()` includes `_semantics_view_nodes()` for `stage in {"all","pre_cpg"}`.
- Hamilton subDAGs consume semantic views via `source()` only (no re‑registration).

**Acceptance Criteria:**
- [x] Semantics views are registered through `registry_specs.py`
- [x] No new `semantics/view_registry.py` file
- [x] Plan bundles + lineage available for semantic views

---

### Task 1.5: Semantic Type System + Annotated Schema  
**Status:** ✅ DONE — semantic types + annotated schema are implemented and used by join inference in `SemanticCompiler`.

**Priority:** 🔴 HIGH  
**Effort:** Medium  
**Files:**
- `src/semantics/types/core.py` (NEW)
- `src/semantics/types/annotated_schema.py` (NEW)
- `src/semantics/compiler.py` (consume annotations)

**Purpose:**  
Introduce a **semantic type registry** and **annotated schemas** so join inference and validation can be programmatic instead of hardcoded.

**Acceptance Criteria:**
- [x] Semantic type registry for common columns (`file_id`, `path`, `bstart/bend`, `entity_id`, `symbol`)
- [x] AnnotatedSchema can be built from Arrow schema or DataFusion DataFrame
- [x] Compiler can access annotated schema for join/validation decisions

---

### Task 1.6: Semantic Catalog + Builder Protocol  
**Status:** ✅ DONE — catalog/builders are integrated into view registration and used for topo ordering.

**Priority:** 🔴 HIGH  
**Effort:** Medium  
**Files:**
- `src/semantics/catalog/catalog.py` (NEW)
- `src/semantics/builders/protocol.py` (NEW)
- `src/semantics/builders/registry.py` (NEW)

**Purpose:**  
Provide a **programmatic registry** of semantic views with metadata (evidence tier, upstream deps, plan fingerprints) and enforce a **builder protocol** to standardize view construction.

**Acceptance Criteria:**
- [x] Catalog registers views with annotated schemas + plan fingerprints
- [x] Builder protocol enforced at registration time
- [x] Dependency-ordered registration works via topological sort

---

### Task 1.7: Canonical Output Naming Policy  
**Status:** ✅ DONE — `semantics/naming.py` added and used in pipeline/spec registry; aliasing handled in view graph.

**Priority:** 🟡 MEDIUM  
**Effort:** Low  
**Files:**
- `src/semantics/pipeline.py` (update to reference mapping)
- `src/datafusion_engine/views/registry_specs.py` (apply mapping)

**Purpose:**  
Define a single mapping from internal view names to output names (with `_v1` suffix decisions) and reuse it everywhere.

**Acceptance Criteria:**
- [x] One canonical mapping governs semantic output names
- [x] No ad-hoc name construction across modules

## Phase 2: Integration

**Goal:** Replace legacy relationship builders with semantic pipeline.

### Task 2.1: Replace `_relspec_view_nodes()` (and potentially `_cpg_view_nodes()`) with Semantic Nodes  
**Status:** ✅ DONE — `_relspec_view_nodes()` removed; semantic views are registered via `_semantics_view_nodes()`. `_cpg_view_nodes()` now only emits non‑semantic CPG views (props + edges by src/dst).

**Priority:** 🔴 HIGH
**Effort:** Medium
**Files:**
- `src/datafusion_engine/views/registry_specs.py:281-387` (modify)

**Implementation (Completed):**
- `view_graph_nodes()` uses `_semantics_view_nodes()` to register semantic outputs.
- Legacy `_relspec_view_nodes()` has been removed.
- `_cpg_view_nodes()` only emits non‑semantic CPG views (props + edges by src/dst).

**Acceptance Criteria:**
- [x] Semantic views are registered via `_semantics_view_nodes()` from `semantics.pipeline`
- [x] `_cpg_view_nodes()` does not duplicate semantic CPG nodes/edges
- [x] All relationship views are registered via semantic pipeline
- [x] Existing tests pass without modification

---

### Task 2.2: Ensure Semantic Outputs Match Legacy Schema Contracts  
**Status:** ✅ DONE — projection adapters are wired in `registry_specs.py` and emit legacy‑compatible schemas.

**Priority:** 🔴 HIGH
**Effort:** Medium
**Files:**
- `src/semantics/compiler.py` (modify)
- `src/schema_spec/relationship_specs.py` (reference)

**Schema contracts to match:**

```python
# schema_spec/relationship_specs.py (reference)
# rel_name_symbol_v1 schema:
#   - ref_id: string
#   - symbol: string
#   - symbol_roles: int32 (nullable)
#   - path: string
#   - edge_owner_file_id: string
#   - bstart: int64
#   - bend: int64
#   - resolution_method: string
#   - confidence: float64 (nullable)
#   - edge_kind: string
#   - task_name: string
#   - task_priority: int32
```

**Current semantic output:**

```python
# compiler.py:614-621
return joined.select(
    left_sem.entity_id_col().alias("entity_id"),
    right_sem.symbol_col().alias("symbol"),
    left_sem.path_col().alias("path"),
    left_sem.span_start_col().alias("bstart"),
    left_sem.span_end_col().alias("bend"),
    lit(origin).alias("origin"),
).distinct()
```

**Implementation:**
Prefer a thin projection layer in `registry_specs.py` (or a dedicated adapter) instead of pushing legacy schema concerns into `SemanticCompiler`. This keeps the compiler canonical and makes the compatibility surface explicit.

```python
# registry_specs.py (adapter example)
def _legacy_rel_projection(df: DataFrame, *, edge_kind: str, task_name: str, task_priority: int) -> DataFrame:
    from datafusion import col, lit
    import pyarrow as pa

    return df.select(
        col("entity_id").alias("ref_id"),
        col("symbol"),
        lit(None).cast(pa.int32()).alias("symbol_roles"),
        col("path"),
        col("path").alias("edge_owner_file_id"),
        col("bstart"),
        col("bend"),
        col("origin").alias("resolution_method"),
        lit(None).cast(pa.float64()).alias("confidence"),
        lit(edge_kind).alias("edge_kind"),
        lit(task_name).alias("task_name"),
        lit(task_priority).cast(pa.int32()).alias("task_priority"),
    )
```

**Acceptance Criteria:**
- [x] Semantic outputs match legacy schema specs (via adapter)
- [x] Schema contract validation passes
- [x] No downstream schema errors

---

### Task 2.3: Join Strategy Inference (Programmatic Joins)  
**Status:** ✅ DONE — `SemanticCompiler` uses `require_join_strategy()` with annotated schemas and hints.

**Priority:** 🔴 HIGH  
**Effort:** Medium  
**Files:**
- `src/semantics/joins/strategies.py` (NEW)
- `src/semantics/joins/inference.py` (NEW)
- `src/semantics/compiler.py` (use inference for `relate`)

**Purpose:**  
Replace hardcoded join logic with **schema‑driven join inference** (span overlap, contains, FK, equi).

**Acceptance Criteria:**
- [x] Inference derives file+span joins when spans + file identity exist
- [x] FK and symbol joins inferred when available
- [x] Compiler selects inferred strategy or fails with clear diagnostics

---

### Task 2.4: Declarative Relationship Specifications  
**Status:** ✅ DONE — `RELATIONSHIP_SPECS` drives pipeline relationship builders.

**Priority:** 🟡 MEDIUM  
**Effort:** Medium  
**Files:**
- `src/semantics/specs.py` (extend)
- `src/semantics/pipeline.py` (consume spec declarations)

**Purpose:**  
Declare relationship intent (left/right + hints) and let join inference determine execution.

**Acceptance Criteria:**
- [x] Relationship declarations replace hand‑coded `RelateSpec` lists
- [x] Intent hints (`overlap`/`contains`/`ownership`) flow into inference
- [x] New relationships require spec changes only (no pipeline edits)

### Task 2.5: Add Semantic Views to Relspec Output Registry  
**Status:** ✅ DONE — semantic intermediate views added to view defs and execution plan outputs.

**Priority:** 🟡 MEDIUM
**Effort:** Low
**Files:**
- `src/relspec/execution_plan.py:82-85` (modify)
- `src/relspec/view_defs.py` (modify)

**Current:**

```python
# execution_plan.py:82-85
_RELSPEC_OUTPUT_VIEWS: frozenset[str] = frozenset({
    REL_NAME_SYMBOL_OUTPUT,
    REL_DEF_SYMBOL_OUTPUT,
    REL_IMPORT_SYMBOL_OUTPUT,
    REL_CALLSITE_SYMBOL_OUTPUT,
    REL_CALLSITE_QNAME_OUTPUT,
    RELATION_OUTPUT_NAME,
})
```

**Implementation:**
Add semantic intermediate views (names should follow the same `_v1` decision made in Task 2.2 / view‑graph registration):

```python
# relspec/view_defs.py (add)
# Semantic intermediate views
SCIP_OCCURRENCES_NORM_OUTPUT: Final[str] = "scip_occurrences_norm_v1"
CST_REFS_NORM_OUTPUT: Final[str] = "cst_refs_norm_v1"
CST_DEFS_NORM_OUTPUT: Final[str] = "cst_defs_norm_v1"
CST_IMPORTS_NORM_OUTPUT: Final[str] = "cst_imports_norm_v1"
CST_CALLS_NORM_OUTPUT: Final[str] = "cst_calls_norm_v1"

SEMANTIC_INTERMEDIATE_VIEWS: Final[tuple[str, ...]] = (
    SCIP_OCCURRENCES_NORM_OUTPUT,
    CST_REFS_NORM_OUTPUT,
    CST_DEFS_NORM_OUTPUT,
    CST_IMPORTS_NORM_OUTPUT,
    CST_CALLS_NORM_OUTPUT,
)
```

```python
# execution_plan.py (modify)
_RELSPEC_OUTPUT_VIEWS: frozenset[str] = frozenset({
    *RELATION_VIEW_NAMES,
    *SEMANTIC_INTERMEDIATE_VIEWS,  # Add semantic views
})
```

**Acceptance Criteria:**
- [x] Semantic intermediate views included in output registry
- [x] Execution plan recognizes semantic views as valid outputs

---

### Task 2.6: Choose a Single Execution Authority (View Graph vs Hamilton)  
**Status:** ✅ DONE — view graph is the sole registration point; Hamilton consumes semantic outputs only.

**Priority:** 🔴 HIGH  
**Effort:** Medium  
**Files:**
- `src/datafusion_engine/views/registry.py` (registration entrypoint)
- `src/hamilton_pipeline/driver_factory.py` (only if Hamilton needs to depend on view artifacts)

**Recommendation:**  
Avoid registering semantic views in two places. Prefer the existing view graph registration (`register_all_views` / `registry_specs.py`) as the single source of view creation. If Hamilton needs to consume semantic outputs, it should **depend on the view artifacts** (or on a pre‑registration step) rather than re‑registering the views itself.

**Acceptance Criteria:**
- [x] Semantic views are registered exactly once (via view graph)
- [x] Hamilton nodes only **consume** semantic outputs, no duplicate registration

---

### Task 2.7: Deprecate Legacy Relationship Builders  
**Status:** ⚠️ PARTIAL — deprecation warnings added; legacy modules still exist for compatibility.

**Priority:** 🟢 LOW (after integration verified)
**Effort:** Low
**Files:**
- `src/relspec/relationship_datafusion.py` (deprecate)
- `src/cpg/relationship_builder.py` (deprecate)
- `src/cpg/relationship_specs.py` (deprecate)

**Implementation:**
Add deprecation warnings:

```python
# relspec/relationship_datafusion.py (add at module level)
import warnings

warnings.warn(
    "relspec.relationship_datafusion is deprecated. "
    "Use semantics.compiler.SemanticCompiler instead.",
    DeprecationWarning,
    stacklevel=2,
)
```

**Acceptance Criteria:**
- [x] Deprecation warnings added to legacy modules
- [ ] No direct imports of legacy modules remain (internal legacy cross‑imports still present)
- [x] Plan for removal in future version documented

---

## Phase 3: Observability

**Goal:** Add comprehensive OpenTelemetry instrumentation to semantic operations.

### Task 3.1: Instrument `SemanticCompiler` Methods  
**Status:** ✅ DONE — compiler spans added with semantic scope.

**Priority:** 🟡 MEDIUM
**Effort:** Medium
**Files:**
- `src/semantics/compiler.py` (modify)

**Implementation:**

```python
# semantics/compiler.py (modify imports)
from obs.otel.scopes import SCOPE_SEMANTICS
from obs.otel.tracing import stage_span

# Modify normalize() method:
def normalize(self, table_name: str, *, prefix: str) -> DataFrame:
    """Apply normalization rules to an evidence table."""
    with stage_span(
        "semantics.normalize",
        scope=SCOPE_SEMANTICS,
        attributes={
            "table_name": table_name,
            "prefix": prefix,
        },
    ):
        # ... existing implementation ...

# Modify relate() method:
def relate(
    self,
    left_table: str,
    right_table: str,
    *,
    join_type: Literal["overlap", "contains"] = "overlap",
    filter_sql: str | None = None,
    origin: str,
) -> DataFrame:
    """Build a relationship between two tables."""
    with stage_span(
        f"semantics.relate.{join_type}",
        scope=SCOPE_SEMANTICS,
        attributes={
            "left_table": left_table,
            "right_table": right_table,
            "join_type": join_type,
            "origin": origin,
            "has_filter": filter_sql is not None,
        },
    ):
        # ... existing implementation ...

# Modify union_nodes() and union_edges():
def union_nodes(self, table_names: list[str], *, discriminator: str = "node_kind") -> DataFrame:
    """Union node tables after canonical projection."""
    with stage_span(
        "semantics.union_nodes",
        scope=SCOPE_SEMANTICS,
        attributes={
            "table_count": len(table_names),
            "tables": ",".join(table_names),
        },
    ):
        # ... existing implementation ...
```

**Acceptance Criteria:**
- [x] All major `SemanticCompiler` methods instrumented
- [x] Spans include relevant attributes for debugging
- [x] No performance regression from instrumentation

---

### Task 3.2: Instrument `build_cpg()` Pipeline  
**Status:** ✅ DONE — pipeline spans added with semantic scope.

**Priority:** 🟡 MEDIUM
**Effort:** Low
**Files:**
- `src/semantics/pipeline.py` (modify)

**Implementation:**

```python
# semantics/pipeline.py (modify)
from obs.otel.scopes import SCOPE_SEMANTICS
from obs.otel.tracing import stage_span

def build_cpg(
    ctx: SessionContext,
    *,
    runtime_profile: DataFusionRuntimeProfile | None = None,
    options: CpgBuildOptions | None = None,
) -> None:
    """Build the complete CPG from extraction tables."""
    resolved = options or CpgBuildOptions()

    with stage_span(
        "semantics.build_cpg",
        scope=SCOPE_SEMANTICS,
        attributes={
            "validate_schema": resolved.validate_schema,
            "use_cdf": resolved.use_cdf,
            "has_cache_policy": resolved.cache_policy is not None,
        },
    ):
        # ... existing implementation ...
```

**Acceptance Criteria:**
- [x] `build_cpg()` wrapped with span
- [x] Pipeline execution visible in traces

---

### Task 3.3: Instrument `scip_to_byte_offsets()`  
**Status:** ✅ DONE — SCIP normalization wrapped in semantic spans.

**Priority:** 🟢 LOW
**Effort:** Low
**Files:**
- `src/semantics/scip_normalize.py` (modify)

**Implementation:**

```python
# semantics/scip_normalize.py (modify)
from obs.otel.scopes import SCOPE_SEMANTICS
from obs.otel.tracing import stage_span

def scip_to_byte_offsets(
    ctx: SessionContext,
    *,
    occurrences_table: str = "scip_occurrences",
    line_index_table: str = "file_line_index_v1",
) -> DataFrame:
    """Return SCIP occurrences with byte-span columns added."""
    with stage_span(
        "semantics.scip_normalize",
        scope=SCOPE_SEMANTICS,
        attributes={
            "occurrences_table": occurrences_table,
            "line_index_table": line_index_table,
        },
    ):
        # ... existing implementation ...
```

**Acceptance Criteria:**
- [x] SCIP normalization visible in traces
- [x] Timing data available for performance analysis

---

### Task 3.4: Add Semantic Metrics Collection  
**Status:** ✅ DONE — metrics collected per semantic view and emitted as artifacts.

**Priority:** 🟢 LOW
**Effort:** Medium
**Files:**
- `src/semantics/metrics.py` (NEW)
- `src/obs/metrics.py` (reference)

**Implementation:**

```python
# src/semantics/metrics.py (NEW)
"""Metrics collection for semantic pipeline operations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datafusion.dataframe import DataFrame


@dataclass
class SemanticOperationMetrics:
    """Metrics for a semantic operation."""

    operation: str
    input_table: str
    output_name: str
    input_row_count: int | None = None
    output_row_count: int | None = None
    elapsed_ms: float | None = None
    plan_fingerprint: str | None = None


def collect_dataframe_metrics(
    df: DataFrame,
    *,
    operation: str,
    input_table: str,
    output_name: str,
) -> SemanticOperationMetrics:
    """Collect metrics from a DataFrame operation.

    Note: Row counts require execution, so they're optional.
    """
    return SemanticOperationMetrics(
        operation=operation,
        input_table=input_table,
        output_name=output_name,
    )


__all__ = [
    "SemanticOperationMetrics",
    "collect_dataframe_metrics",
]
```

**Acceptance Criteria:**
- [x] Metrics dataclass defined
- [x] Metrics artifacts emitted (`semantic_pipeline_metrics_v1`)

---

### Task 3.5: Substrait Plan Fingerprints + Cache  
**Status:** ✅ DONE — plan fingerprints computed (Substrait when available) and stored as artifacts.

**Priority:** 🟡 MEDIUM  
**Effort:** Medium  
**Files:**
- `src/semantics/plans/fingerprints.py` (NEW)
- `src/datafusion_engine/views/registry_specs.py` (store plan fingerprint artifacts)

**Purpose:**  
Persist portable plan fingerprints for semantic views (Substrait if available, logical plan fallback) to enable caching and diff‑based debugging.

**Acceptance Criteria:**
- [x] Plan fingerprints stored for each semantic view
- [x] Substrait serialization used when available; fallback to logical plan text

---

### Task 3.6: Statistics‑Aware Optimization (Python‑Side)  
**Status:** ⚠️ PARTIAL — stats collector integrated + stored; optimizer injection remains deferred.

**Priority:** 🟡 MEDIUM  
**Effort:** Medium  
**Files:**
- `src/semantics/stats/collector.py` (NEW)
- `src/semantics/catalog/catalog.py` (store estimates)

**Purpose:**  
Collect/estimate row counts and column stats for semantic views and expose them as artifacts for planning insights.  
Note: A custom TableProvider for true optimizer injection is **Rust‑side** and deferred.

**Acceptance Criteria:**
- [x] Row count estimates available per semantic view
- [x] Column stats optionally computed and stored as artifacts

---

### Task 3.7: Incremental CDF Join Framework  
**Status:** ⚠️ PARTIAL — framework wired into `SemanticCompiler` via `use_cdf`, but merge logic remains simplified.

**Priority:** 🟡 MEDIUM  
**Effort:** Medium  
**Files:**
- `src/semantics/incremental/cdf_joins.py` (NEW)
- `src/semantics/pipeline.py` (optional integration switch)

**Purpose:**  
Provide a change‑data‑feed pathway for incremental relationship recomputation.

**Acceptance Criteria:**
- [x] Incremental join spec + merge strategies implemented
- [x] Pipeline can choose incremental path when CDF inputs exist

## Phase 4: Cleanup & Documentation

**Goal:** Remove deprecated code and document the new architecture.

### Task 4.1: Remove Direct Imports of Legacy Modules  
**Status:** ⚠️ PARTIAL — external imports removed; legacy compatibility modules still remain.

**Priority:** 🟡 MEDIUM (after Phase 2 complete)
**Effort:** Medium
**Files:**
- Search all files importing from `relspec/relationship_datafusion.py`
- Search all files importing from `cpg/relationship_builder.py`

**Process:**
1. Run: `rg -n "from relspec.relationship_datafusion" src/`
2. Run: `rg -n "from cpg.relationship_builder" src/`
3. Update each import to use `semantics.*` equivalents
4. Verify tests still pass

**Acceptance Criteria:**
- [x] No direct imports of deprecated modules outside the legacy compatibility layer
- [ ] Legacy modules fully removed

---

### Task 4.2: Update Architecture Documentation  
**Status:** ⚠️ PARTIAL — semantic pipeline graph generated and CLAUDE.md updated; broader narrative consolidation pending.

**Priority:** 🟢 LOW
**Effort:** Medium
**Files:**
- `docs/architecture/` (if exists)
- `CLAUDE.md` (update)

**Content to document:**
1. Semantic pipeline architecture
2. Data flow from extraction to CPG
3. View dependency graph
4. Configuration options

**Acceptance Criteria:**
- [x] Architecture documented in appropriate location (semantic pipeline graph)
- [x] CLAUDE.md updated with semantic module guidance
- [ ] Consolidated narrative doc covering end‑to‑end semantics wiring

---

### Task 4.3: Add Integration Tests  
**Status:** ✅ DONE — integration and unit tests cover semantic pipeline components.

**Priority:** 🟡 MEDIUM
**Effort:** Medium-High
**Files:**
- `tests/integration/test_semantic_pipeline.py` (NEW)

**Test scenarios:**

```python
# tests/integration/test_semantic_pipeline.py (NEW)
"""Integration tests for semantic pipeline."""

from __future__ import annotations

import pytest


@pytest.mark.integration
class TestSemanticPipelineIntegration:
    """Integration tests for end-to-end semantic pipeline."""

    def test_build_cpg_creates_expected_views(
        self,
        session_with_extraction_data,
        runtime_profile,
    ):
        """Verify build_cpg creates all expected views."""
        from semantics.pipeline import build_cpg

        build_cpg(
            session_with_extraction_data,
            runtime_profile=runtime_profile,
        )

        # Verify views exist
        expected_views = [
            "scip_occurrences_norm",
            "cst_refs_norm",
            "cst_defs_norm",
            "rel_name_symbol",
            "rel_def_symbol",
            "cpg_nodes",
            "cpg_edges",
        ]
        for view in expected_views:
            assert session_with_extraction_data.table_exist(view)

    def test_relation_output_schema_matches_contract(
        self,
        session_with_semantic_views,
    ):
        """Verify relation_output_v1 matches expected schema."""
        from schema_spec.relationship_specs import relation_output_spec

        df = session_with_semantic_views.table("relation_output_v1")
        expected_spec = relation_output_spec()

        # Verify all expected columns present
        actual_names = {f.name for f in df.schema()}
        expected_names = {f.name for f in expected_spec.schema}

        assert expected_names.issubset(actual_names)

    def test_semantic_views_have_lineage(
        self,
        session_with_semantic_views,
        runtime_profile,
    ):
        """Verify semantic views have extractable lineage."""
        from semantics.pipeline import build_cpg_from_inferred_deps

        deps = build_cpg_from_inferred_deps(
            session_with_semantic_views,
            runtime_profile=runtime_profile,
        )

        # Verify lineage extracted
        assert "rel_name_symbol" in deps
        assert deps["rel_name_symbol"]["inputs"]
```

**Acceptance Criteria:**
- [x] Integration tests cover happy path
- [x] Tests verify schema contracts
- [x] Tests verify lineage extraction

---

### Task 4.4: Self‑Documenting Graph Exports  
**Status:** ✅ DONE — graph docs generator and output artifacts are in place.

**Priority:** 🟢 LOW  
**Effort:** Medium  
**Files:**
- `src/semantics/docs/graph_docs.py` (NEW)
- `docs/architecture/` (generated artifacts)

**Purpose:**  
Auto‑generate Mermaid diagrams + Markdown documentation from the semantic catalog and relationship declarations.

**Acceptance Criteria:**
- [x] Mermaid diagram generated from registered views/relationships
- [x] Markdown export produced for semantic graph

---

## Dependency Graph

```
Phase 1 (Foundation)
├── 1.1 file_line_index_v1 extraction  ◄── BLOCKER
├── 1.2 SCOPE_SEMANTICS
├── 1.3 input_registry.py
├── 1.4 integrate semantics into registry_specs  ◄── Depends on 1.2, 1.3
├── 1.5 semantic type system + annotated schema
├── 1.6 semantic catalog + builder protocol  ◄── Depends on 1.5
└── 1.7 canonical output naming policy  ◄── Depends on 1.4, 1.6

Phase 2 (Integration)
├── 2.1 Replace _relspec_view_nodes()  ◄── Depends on 1.4, 1.6
├── 2.2 Schema compatibility (adapter)  ◄── Depends on 2.1
├── 2.3 Join strategy inference  ◄── Depends on 1.5
├── 2.4 Declarative relationship specs  ◄── Depends on 2.3
├── 2.5 Relspec output registry  ◄── Depends on 2.1
├── 2.6 Execution authority decision  ◄── Depends on 2.1, 2.2
└── 2.7 Deprecate legacy  ◄── Depends on 2.6

Phase 3 (Observability)
├── 3.1 Instrument compiler  ◄── Depends on 1.2
├── 3.2 Instrument pipeline  ◄── Depends on 1.2
├── 3.3 Instrument SCIP normalize  ◄── Depends on 1.2
├── 3.4 Metrics collection  ◄── Optional
├── 3.5 Substrait plan fingerprints  ◄── Optional
├── 3.6 Stats-aware optimization  ◄── Optional
└── 3.7 Incremental CDF joins  ◄── Optional

Phase 4 (Cleanup)
├── 4.1 Remove legacy imports  ◄── Depends on 2.7
├── 4.2 Update documentation  ◄── Depends on 2.6
├── 4.3 Integration tests  ◄── Depends on 2.6
└── 4.4 Self-documenting graph  ◄── Depends on 2.6
```

---

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| `file_line_index_v1` extraction missing | High | Critical | Investigate immediately (Task 1.1) |
| Schema mismatch breaks downstream | Medium | High | Comprehensive schema tests (Task 2.2) |
| Join inference selects wrong strategy | Medium | High | Add inference validation + explicit intent hints (Task 2.3/2.4) |
| Performance regression | Low | Medium | Benchmark before/after (Phase 3) |
| Substrait support unavailable | Medium | Low | Fallback to logical plan fingerprints (Task 3.5) |
| CDF incremental path correctness | Medium | High | Gate with integration tests + optional feature flag (Task 3.7) |
| Hamilton DAG ordering issues | Medium | Medium | Explicit dependencies in module |
| Observability overhead | Low | Low | Sampling, async span export |

---

## Success Criteria

1. **Functional:** All CPG outputs identical between legacy and semantic paths
2. **Performance:** No more than 10% regression in pipeline execution time
3. **Observability:** All semantic operations visible in traces
4. **Maintainability:** Single code path for relationship building
5. **Test Coverage:** Integration tests for critical paths

---

## Timeline Estimate

| Phase | Effort | Dependencies |
|-------|--------|--------------|
| Phase 1 | 4-6 days | None |
| Phase 2 | 6-9 days | Phase 1 complete |
| Phase 3 | 3-5 days | Phase 1.2 complete |
| Phase 4 | 3-4 days | Phase 2 complete |

**Total:** ~16-24 days of focused work

---

## Appendix: File Change Summary

### New Files
- `src/semantics/adapters.py`
- `src/semantics/input_registry.py`
- `src/semantics/metrics.py`
- `src/semantics/naming.py`
- `src/semantics/spec_registry.py`
- `src/semantics/specs.py`
- `src/semantics/types/core.py`
- `src/semantics/types/annotated_schema.py`
- `src/semantics/joins/strategies.py`
- `src/semantics/joins/inference.py`
- `src/semantics/catalog/catalog.py`
- `src/semantics/builders/protocol.py`
- `src/semantics/builders/registry.py`
- `src/semantics/plans/fingerprints.py`
- `src/semantics/stats/collector.py`
- `src/semantics/incremental/cdf_joins.py`
- `src/semantics/docs/graph_docs.py`
- `scripts/generate_semantic_graph_docs.py`
- `docs/architecture/semantic_pipeline_graph.md`
- `src/extract/extractors/file_index/line_index.py`
- `tests/integration/test_semantic_pipeline.py`

### Modified Files
- `src/obs/otel/scopes.py`
- `src/obs/otel/constants.py`
- `src/semantics/compiler.py`
- `src/semantics/pipeline.py`
- `src/semantics/scip_normalize.py`
- `src/datafusion_engine/views/registry_specs.py`
- `src/datafusion_engine/views/graph.py`
- `src/datafusion_engine/extract/templates.py`
- `src/extract/__init__.py`
- `src/extract/scanning/repo_scan.py`
- `src/relspec/execution_plan.py`
- `src/relspec/view_defs.py`
- `src/hamilton_pipeline/driver_factory.py`

### Deprecated Files (Phase 4)
- `src/relspec/relationship_datafusion.py`
- `src/cpg/relationship_builder.py`
- `src/cpg/relationship_specs.py`

---

## Appendix: Best‑in‑Class Modularity & DataFusion‑First Extensions

This appendix turns the “programmatic / extensible / robust” recommendations into concrete scope items. It is designed to be **incremental** and **spec‑driven**, avoiding ad‑hoc wiring and maximizing DataFusion’s planning + catalog capabilities.

### A. Spec‑Driven Pipeline Generation (Single Source of Truth)

**Goal:** Generate all semantic view builders directly from `spec_registry.py` without hand‑maintained lists.
**Status:** ✅ DONE — spec index drives normalize/relate/union builders with topological ordering.

**Tasks:**
- Add a **spec index** with explicit metadata (kind, role, required inputs, output naming policy).
- Derive normalize/relate/union builders from the registry.
- Emit a **deterministic view order** based on dependency topology.

**Implementation sketch:**
```python
# semantics/spec_registry.py (add)
@dataclass(frozen=True)
class SemanticSpecIndex:
    name: str
    kind: Literal["normalize", "relate", "union"]
    outputs: tuple[str, ...]
    inputs: tuple[str, ...]
    output_policy: Literal["raw", "v1"]

SEMANTIC_SPEC_INDEX: Final[tuple[SemanticSpecIndex, ...]] = (
    # normalize + relate + union specs built from registry tables
)
```

```python
# semantics/pipeline.py (replace hand lists)
def semantic_builders_from_specs(
    *,
    cdf_inputs: Mapping[str, str] | None,
    config: SemanticConfig | None,
) -> list[tuple[str, DataFrameBuilder]]:
    # Use SEMANTIC_SPEC_INDEX to build all view builders
```

**Acceptance Criteria:**
- [x] No hand‑maintained view name lists in `pipeline.py`
- [x] View order computed from dependency graph
- [x] New spec entries automatically appear in pipeline

---

### B. Canonical Output Naming & Compatibility Layer

**Goal:** Centralize naming policy and remove duplicate `_v1` logic.
**Status:** ⚠️ PARTIAL — naming policy + adapters wired; legacy alias mapping not fully centralized.

**Tasks:**
- Define a **single mapping** from internal view names → output view names.
- Enforce naming policy in the view‑graph registration layer.
- For legacy compatibility, use a **projection adapter** (not compiler changes).

**Acceptance Criteria:**
- [x] One mapping table governs semantic output names
- [x] `relation_output_v1` is produced via adapter only
- [ ] Legacy alias mapping (e.g., deprecated names) centralized in view‑graph aliasing

---

### C. Information‑Schema‑Driven Validation

**Goal:** Replace hardcoded schema checks with catalog‑driven validation.
**Status:** ❌ NOT DONE — validation still uses explicit checks, not information_schema.

**Tasks:**
- Enable `information_schema` and validate inputs via `information_schema.tables/columns`.
- Add schema contract checks at **view registration time**.
- Emit **schema mismatch diagnostics** as artifacts.

**Acceptance Criteria:**
- [ ] Missing/invalid columns fail early with actionable errors
- [ ] Validation logic does not depend on in‑memory Python schema definitions

---

### D. Plan‑First Lineage + Substrait Fingerprints

**Goal:** Make execution plans a first‑class artifact for reproducibility and caching.
**Status:** ⚠️ PARTIAL — plan fingerprints implemented and stored; CI gating not yet wired.

**Tasks:**
- Store optimized logical plans in Substrait for each semantic view.
- Persist plan fingerprints with outputs (in artifacts table).
- Use plan fingerprint changes as a gating signal in CI.

**Acceptance Criteria:**
- [x] Each semantic view has a stored plan fingerprint
- [ ] Plan diffs are observable between runs (CI gate pending)

---

### E. DataFusion Runtime Policy (Caching + Stats)

**Goal:** Make caching and statistics explicit policy, not incidental behavior.
**Status:** ⚠️ PARTIAL — cache policies + stats artifacts exist; explicit runtime cache configuration pending.

**Tasks:**
- Configure metadata + listing cache limits for view‑graph sessions.
- Ensure `collect_statistics` is enabled for key semantic inputs.
- Add a cache/metadata **introspection artifact** per run.

**Acceptance Criteria:**
- [ ] Caching knobs are explicitly configured
- [x] Stats exist for semantic views (verified via artifacts)

---

### F. Delta‑First Materialization & Schema Enforcement

**Goal:** Make output storage robust, versioned, and schema‑safe.
**Status:** ❌ NOT DONE — Delta enforcement not wired into semantic outputs.

**Tasks:**
- Register Delta tables as providers (not Arrow dataset fallback).
- Enforce schema evolution policy (strict/additive) before writes.
- Persist schema hash per output view.

**Acceptance Criteria:**
- [ ] Outputs are Delta‑backed and versioned
- [ ] Schema changes require explicit approval

---

### G. UDF Governance + Function Catalog

**Goal:** Centralize UDF registration and ensure deterministic availability.
**Status:** ✅ DONE — UDF validation + catalog artifact emitted via view graph.

**Tasks:**
- Register all Rust UDFs once and validate availability pre‑plan.
- Produce a runtime UDF catalog artifact.
- Gate semantic pipeline on required UDFs.

**Acceptance Criteria:**
- [x] Missing UDFs fail before any view registration
- [x] UDF catalog artifact stored per run

---

### H. Observability + Diagnostics

**Goal:** Make semantic operations fully observable and debuggable.
**Status:** ⚠️ PARTIAL — spans + metrics + fingerprints + stats are emitted; lineage/cache artifacts still missing.

**Tasks:**
- Standardize OpenTelemetry span names + attributes for all semantic ops.
- Emit per‑view metrics (row counts, latency, plan fingerprint).
- Store view‑graph lineage and cache stats as artifacts.

**Acceptance Criteria:**
- [x] All semantic ops emit spans
- [ ] Artifacts include metrics, lineage, and cache info

---

### I. Deferred / Rust‑Side Extensions (Documented, Not Blocking)

These items are best implemented once the Rust embedding surface is available:

- **Custom TableProvider with statistics/pushdown hints** (true optimizer injection)
- **Join plan templates with explicit join hints** (broadcast/sort‑merge/interval index)

They remain desirable but are **not required** to complete the integration wiring.
