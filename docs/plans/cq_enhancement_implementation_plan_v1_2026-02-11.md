# CQ Enhancement Implementation Plan v1

**Date:** 2026-02-11
**Source Design:** `docs/plans/cq_enhancement_design.md`
**Reference Docs:** `docs/python_library_reference/pyrefly_lsp_data.md`, `rust_lsp.md`, `rich_md_overview.md`

---

## Scope Overview

This plan decomposes the CQ Enhancement Design into **7 discrete scope items**, ordered by dependency and ROI. Each scope item is independently shippable and testable.

| Scope | Title | Depends On | Est. Files |
|-------|-------|------------|------------|
| S1 | SNB Core Schema + `details.kind` Registry | — | 3 new, 2 edit |
| S2 | Rust LSP Session + Collector Module | — | 3 new, 2 edit |
| S3 | Structural Neighborhood Collector (AST-based) | S1 | 2 new, 2 edit |
| S4 | SNB Assembler + Bundle Builder Pipeline | S1, S2, S3 | 2 new, 3 edit |
| S5 | LDMD Progressive Disclosure Format + Extractor | — | 2 new |
| S6 | Section Layout Renderer + Bundle View Materializer | S1, S4, S5 | 2 new, 2 edit |
| S7 | CLI Integration + `cq neighborhood` Command | S4, S6 | 2 new, 3 edit |

---

## S1: SNB Core Schema + `details.kind` Registry

### Goal

Define the `SemanticNeighborhoodBundleV1` contract and all supporting structs as `CqStruct` (msgspec.Struct) types. Register all new `details.kind` values. This is the foundational data model that all other scopes consume.

### Key Architectural Elements

The SNB schema follows the existing CQ contract discipline:
- `CqStruct(msgspec.Struct, kw_only=True, frozen=True, omit_defaults=True)` base
- Heavy payloads behind `Artifact` pointers (path + format)
- `BundleViewV1` maps 1:1 to existing `Finding`/`Section` lists
- `NeighborhoodSliceV1` is the primary "expandable section" unit

### Representative Code

```python
# tools/cq/core/snb_schema.py
from __future__ import annotations

from typing import Literal, TypeAlias

import msgspec

from tools.cq.core.schema import Anchor, Artifact, Finding, Section
from tools.cq.core.structs import CqStruct

LanguageId: TypeAlias = Literal["python", "rust"]

NeighborhoodSliceKind: TypeAlias = Literal[
    "parents", "children", "siblings", "enclosing_context",
    "callers", "callees", "references", "implementations",
    "type_supertypes", "type_subtypes",
    "imports", "importers", "related",
]

RelationKind: TypeAlias = Literal[
    "contains", "calls", "references", "imports",
    "type_super", "type_sub", "implements", "implemented_by",
]


class LspPositionV1(CqStruct, frozen=True):
    line: int
    character: int


class LspRangeV1(CqStruct, frozen=True):
    start: LspPositionV1
    end: LspPositionV1


class LspSpanRefV1(CqStruct, frozen=True):
    uri: str
    pos: LspPositionV1
    selection: LspRangeV1


class SemanticNodeRefV1(CqStruct, frozen=True):
    """Lightweight node descriptor. Heavy payloads go in attachments."""
    node_id: str
    language: LanguageId
    display_name: str
    symbol_kind: str | None = None
    qname: str | None = None
    anchor: Anchor | None = None
    lsp: LspSpanRefV1 | None = None
    container_id: str | None = None
    relevance: float | None = None
    attachments: tuple[AttachmentRefV1, ...] = ()


class SemanticEdgeV1(CqStruct, frozen=True):
    kind: RelationKind
    src: str  # node_id
    dst: str  # node_id
    confidence: float | None = None
    note: str | None = None
    evidence_artifact: Artifact | None = None


class AttachmentRefV1(CqStruct, frozen=True):
    """Typed pointer to heavy data (raw LSP, full list, rich-md doc)."""
    kind: str
    preview: dict[str, object] = msgspec.field(default_factory=dict)
    artifact: Artifact | None = None


class NeighborhoodSliceV1(CqStruct, frozen=True):
    """Maps to a Section in CQ output."""
    kind: NeighborhoodSliceKind
    title: str | None = None
    total: int = 0
    preview: tuple[SemanticNodeRefV1, ...] = ()
    full_artifact: Artifact | None = None
    collapsed: bool = True
    notes: tuple[str, ...] = ()


class NeighborhoodGraphSummaryV1(CqStruct, frozen=True):
    node_count: int = 0
    edge_count: int = 0
    full_graph_artifact: Artifact | None = None


class BundleViewV1(CqStruct):
    """1:1 bridge to CQ presentation model (Finding/Section lists)."""
    key_findings: list[Finding] = msgspec.field(default_factory=list)
    sections: list[Section] = msgspec.field(default_factory=list)
    evidence: list[Finding] = msgspec.field(default_factory=list)


class SemanticNeighborhoodMetaV1(CqStruct, frozen=True):
    tool: str = "cq"
    tool_version: str | None = None
    workspace_root: str | None = None
    query_text: str | None = None
    created_at_ms: float | None = None
    lsp_servers: tuple[dict[str, object], ...] = ()
    limits: dict[str, int] = msgspec.field(default_factory=dict)


class SemanticNeighborhoodBundleV1(CqStruct, frozen=True):
    """Canonical semantic neighborhood contract."""
    bundle_kind: Literal["cq.semantic_neighborhood_bundle.v1"] = (
        "cq.semantic_neighborhood_bundle.v1"
    )
    meta: SemanticNeighborhoodMetaV1 = msgspec.field(
        default_factory=SemanticNeighborhoodMetaV1
    )
    subject: SemanticNodeRefV1 | None = None
    slices: tuple[NeighborhoodSliceV1, ...] = ()
    graph: NeighborhoodGraphSummaryV1 | None = None
    view: BundleViewV1 = msgspec.field(default_factory=BundleViewV1)
    artifacts: tuple[Artifact, ...] = ()
    diagnostics: tuple[str, ...] = ()


class SemanticNeighborhoodBundleRefV1(CqStruct, frozen=True):
    """Stub Finding.details.data pointing to full bundle artifact."""
    bundle_kind: Literal["cq.semantic_neighborhood_bundle.v1"] = (
        "cq.semantic_neighborhood_bundle.v1"
    )
    bundle_artifact: Artifact
    subject_node_id: str | None = None
    counts: dict[str, int] = msgspec.field(default_factory=dict)
    notes: tuple[str, ...] = ()
```

```python
# tools/cq/core/snb_registry.py — details.kind registry
from __future__ import annotations

# Registry mapping details.kind strings → struct types for validation
DETAILS_KIND_REGISTRY: dict[str, str] = {
    # CQ Semantic Neighborhood
    "cq.snb.bundle_ref.v1": "SemanticNeighborhoodBundleRefV1",
    "cq.snb.node.v1": "SemanticNodeRefV1",
    "cq.snb.edge.v1": "SemanticEdgeV1",
    "cq.snb.slice.v1": "NeighborhoodSliceV1",
    # Pyrefly LSP
    "pyrefly.lsp.symbol_grounding.v1": "PyreflySymbolGroundingBundleV1",
    "pyrefly.lsp.hover.v1": "hover",
    "pyrefly.lsp.signature_help.v1": "signature_help",
    "pyrefly.lsp.inlay_hints.v1": "inlay_hints",
    # Rust LSP
    "rust.lsp.call_hierarchy.v1": "RustCallHierarchyBundleV1",
    "rust.lsp.type_hierarchy.v1": "RustTypeHierarchyBundleV1",
    "rust.lsp.document_symbols.v1": "document_symbols",
}
```

### Target Files

| Action | File |
|--------|------|
| **Create** | `tools/cq/core/snb_schema.py` |
| **Create** | `tools/cq/core/snb_registry.py` |
| **Create** | `tests/unit/cq/core/test_snb_schema.py` |
| **Edit** | `tools/cq/core/__init__.py` (export new modules) |
| **Edit** | `tools/cq/core/enrichment_facts.py` (add SNB fact cluster paths) |

### Implementation Checklist

- [ ] Define all `CqStruct` types in `snb_schema.py` matching design doc
- [ ] Define `details.kind` registry in `snb_registry.py`
- [ ] Add `LanguageId`, `NeighborhoodSliceKind`, `RelationKind` type aliases
- [ ] Ensure `BundleViewV1` uses non-frozen Struct (mutable lists needed for assembly)
- [ ] Add `SemanticNeighborhoodBundleV1` frozen variant for serialization
- [ ] Write msgspec round-trip tests (encode → decode → assert equality)
- [ ] Test `SemanticNodeRefV1` with and without optional fields
- [ ] Test `NeighborhoodSliceV1` collapsed/uncollapsed states
- [ ] Test `AttachmentRefV1` with artifact pointer vs inline preview
- [ ] Verify `omit_defaults=True` produces compact JSON
- [ ] Verify compatibility with existing `Finding.details.data` dict protocol

---

## S2: Rust LSP Session + Collector Module

### Goal

Build a `rust-analyzer` LSP client session module mirroring the existing `pyrefly_lsp.py` architecture: stdio JSON-RPC, fail-open, session caching by workspace root, and a `probe()` method that returns normalized payloads. This provides Rust semantic enrichment parity with the existing Python/pyrefly path.

### Key Architectural Elements

- Mirrors `_PyreflyLspSession` pattern: subprocess.Popen + selectors + JSON-RPC framing
- Same fail-open discipline: any error returns `None`, no exceptions escape
- Session caching: global `_SESSIONS` dict keyed by resolved root path
- Capabilities negotiation: request `linkSupport`, `hierarchicalDocumentSymbolSupport`, position encoding
- Normalized payload structure matches existing enrichment fact clusters

### Representative Code

```python
# tools/cq/search/rust_lsp.py
from __future__ import annotations

import atexit
import contextlib
import json
import os
import selectors
import subprocess
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from tools.cq.core.structs import CqStruct

_DEFAULT_TIMEOUT_SECONDS = 0.5
_DEFAULT_STARTUP_TIMEOUT_SECONDS = 8.0  # rust-analyzer indexing is slower
_DEFAULT_MAX_CALLERS = 8
_DEFAULT_MAX_CALLEES = 8
_DEFAULT_MAX_SUPERTYPES = 6
_DEFAULT_MAX_SUBTYPES = 6
_DEFAULT_MAX_REFERENCES = 16
_DEFAULT_MAX_DOC_SYMBOLS = 50


class RustLspRequest(CqStruct, frozen=True):
    """Request envelope for per-anchor rust-analyzer enrichment."""
    root: Path
    file_path: Path
    line: int
    col: int
    symbol_hint: str | None = None
    timeout_seconds: float = _DEFAULT_TIMEOUT_SECONDS
    startup_timeout_seconds: float = _DEFAULT_STARTUP_TIMEOUT_SECONDS
    max_callers: int = _DEFAULT_MAX_CALLERS
    max_callees: int = _DEFAULT_MAX_CALLEES
    max_supertypes: int = _DEFAULT_MAX_SUPERTYPES
    max_subtypes: int = _DEFAULT_MAX_SUBTYPES
    max_references: int = _DEFAULT_MAX_REFERENCES
    max_doc_symbols: int = _DEFAULT_MAX_DOC_SYMBOLS


class _RustLspSession:
    """Minimal stdio JSON-RPC client for rust-analyzer."""

    def __init__(self, root: Path) -> None:
        self.root = root.resolve()
        self._proc: subprocess.Popen[bytes] | None = None
        self._selector: selectors.BaseSelector | None = None
        self._buffer = bytearray()
        self._next_id = 0
        self._position_encoding = "utf-16"
        self._docs: dict[str, _SessionDocState] = {}
        self._diagnostics_by_uri: dict[str, list[dict[str, object]]] = {}

    def probe(self, request: RustLspRequest) -> dict[str, object] | None:
        """Execute enrichment probe. Returns None on any failure."""
        self.ensure_started(timeout_seconds=request.startup_timeout_seconds)
        if self._proc is None or self._proc.poll() is not None:
            return None
        try:
            uri = self._open_or_update_document(request.file_path)
        except Exception:  # noqa: BLE001 - fail-open
            return None

        pos = {"line": max(0, request.line - 1), "character": max(0, request.col)}
        td = {"uri": uri}
        tdp = {"textDocument": td, "position": pos}

        # Phase 1: core grounding + hover + doc symbols (pipelined)
        request_ids: dict[str, int] = {
            "hover": self._request("textDocument/hover", tdp),
            "definition": self._request("textDocument/definition", tdp),
            "type_definition": self._request("textDocument/typeDefinition", tdp),
            "implementation": self._request("textDocument/implementation", tdp),
            "references": self._request(
                "textDocument/references",
                {"textDocument": td, "position": pos,
                 "context": {"includeDeclaration": False}},
            ),
            "doc_symbols": self._request(
                "textDocument/documentSymbol", {"textDocument": td}
            ),
            "call_prepare": self._request(
                "textDocument/prepareCallHierarchy", tdp
            ),
        }
        responses = self._collect_responses(
            request_ids, timeout_seconds=request.timeout_seconds
        )

        # Phase 2: conditional call/type hierarchy expansion
        incoming, outgoing = self._expand_call_hierarchy(
            responses.get("call_prepare"), request
        )
        supertypes, subtypes = self._expand_type_hierarchy(
            tdp, request
        )

        return self._assemble_payload(
            responses, incoming, outgoing, supertypes, subtypes, uri, request
        )

    def _expand_call_hierarchy(
        self, prepare_result: object, request: RustLspRequest
    ) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
        """Expand call hierarchy from prepared item. Fail-open."""
        call_item = _first_call_hierarchy_item(prepare_result)
        if call_item is None:
            return [], []
        try:
            inc_id = self._request(
                "callHierarchy/incomingCalls", {"item": call_item}
            )
            out_id = self._request(
                "callHierarchy/outgoingCalls", {"item": call_item}
            )
            resps = self._collect_responses(
                {"incoming": inc_id, "outgoing": out_id},
                timeout_seconds=request.timeout_seconds,
            )
            return (
                _normalize_call_links(resps.get("incoming"), request.max_callers),
                _normalize_call_links(resps.get("outgoing"), request.max_callees),
            )
        except Exception:  # noqa: BLE001 - fail-open
            return [], []

    def _expand_type_hierarchy(
        self, tdp: dict[str, object], request: RustLspRequest
    ) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
        """Expand type hierarchy. Fail-open."""
        try:
            prep_id = self._request(
                "textDocument/prepareTypeHierarchy", tdp
            )
            prep_resp = self._collect_responses(
                {"prepare": prep_id},
                timeout_seconds=request.timeout_seconds,
            )
            type_item = _first_type_hierarchy_item(prep_resp.get("prepare"))
            if type_item is None:
                return [], []
            sup_id = self._request(
                "typeHierarchy/supertypes", {"item": type_item}
            )
            sub_id = self._request(
                "typeHierarchy/subtypes", {"item": type_item}
            )
            resps = self._collect_responses(
                {"supertypes": sup_id, "subtypes": sub_id},
                timeout_seconds=request.timeout_seconds,
            )
            return (
                _normalize_type_links(
                    resps.get("supertypes"), request.max_supertypes
                ),
                _normalize_type_links(
                    resps.get("subtypes"), request.max_subtypes
                ),
            )
        except Exception:  # noqa: BLE001 - fail-open
            return [], []

    # ... _start, _request, _notify, _collect_responses, close, etc.
    # follow exact same pattern as _PyreflyLspSession


# Module-level session cache (mirrors pyrefly_lsp.py pattern)
_SESSIONS: dict[str, _RustLspSession] = {}
_SESSION_LOCK = threading.Lock()


def enrich_with_rust_lsp(
    request: RustLspRequest,
) -> dict[str, object] | None:
    """Main enrichment entry point. Fail-open."""
    key = str(request.root.resolve())
    with _SESSION_LOCK:
        session = _SESSIONS.get(key)
        if session is None:
            session = _RustLspSession(request.root)
            _SESSIONS[key] = session
    try:
        return session.probe(request)
    except Exception:  # noqa: BLE001 - fail-open
        return None


def close_rust_lsp_sessions() -> None:
    with _SESSION_LOCK:
        for session in _SESSIONS.values():
            with contextlib.suppress(Exception):
                session.close()
        _SESSIONS.clear()


atexit.register(close_rust_lsp_sessions)
```

```python
# tools/cq/search/rust_lsp_contracts.py — normalized response structs
from __future__ import annotations

from tools.cq.core.structs import CqStruct


class RustLspTarget(CqStruct, frozen=True):
    """Normalized location target from goto-family responses."""
    kind: str = ""  # definition | type_definition | implementation
    uri: str | None = None
    file: str | None = None
    line: int = 0
    col: int = 0
    end_line: int = 0
    end_col: int = 0
    name: str | None = None
    container_name: str | None = None


class RustDocSymbol(CqStruct, frozen=True):
    """Normalized DocumentSymbol from textDocument/documentSymbol."""
    name: str
    kind: str  # function | class | method | struct | enum | trait | module
    range_start_line: int = 0
    range_end_line: int = 0
    selection_start_line: int = 0
    selection_start_col: int = 0
    children_count: int = 0
    detail: str | None = None


class RustCallLink(CqStruct, frozen=True):
    """Normalized incoming/outgoing call hierarchy link."""
    name: str
    kind: str | None = None
    uri: str | None = None
    file: str | None = None
    line: int = 0
    col: int = 0
    detail: str | None = None
    call_ranges: tuple[tuple[int, int], ...] = ()  # (line, col) pairs


class RustTypeLink(CqStruct, frozen=True):
    """Normalized supertype/subtype hierarchy link."""
    name: str
    kind: str | None = None
    uri: str | None = None
    file: str | None = None
    line: int = 0
    col: int = 0
    detail: str | None = None
```

### Target Files

| Action | File |
|--------|------|
| **Create** | `tools/cq/search/rust_lsp.py` |
| **Create** | `tools/cq/search/rust_lsp_contracts.py` |
| **Create** | `tests/unit/cq/search/test_rust_lsp_contracts.py` |
| **Edit** | `tools/cq/search/rust_enrichment.py` (wire in `enrich_with_rust_lsp`) |
| **Edit** | `tools/cq/core/enrichment_facts.py` (add Rust LSP fact paths) |

### Implementation Checklist

- [ ] Implement `_RustLspSession` mirroring `_PyreflyLspSession` structure
- [ ] JSON-RPC framing: `Content-Length` header, `\r\n\r\n` separator
- [ ] `_start()`: spawn `rust-analyzer` with stdio pipes, negotiate capabilities
  - Request `linkSupport` for definition/typeDefinition/implementation/declaration
  - Request `hierarchicalDocumentSymbolSupport`
  - Request `positionEncodings: ["utf-8", "utf-16"]`
- [ ] `_open_or_update_document()`: `didOpen`/`didChange` with content hash caching
  - Use `languageId: "rust"` instead of `"python"`
- [ ] `probe()` phase 1: pipeline hover + definition + typeDefinition + implementation + references + documentSymbol + prepareCallHierarchy
- [ ] `probe()` phase 2: conditional callHierarchy incoming/outgoing
- [ ] `probe()` phase 3: conditional typeHierarchy supertypes/subtypes
- [ ] `_assemble_payload()`: normalize into enrichment-compatible dict matching fact cluster paths
- [ ] Normalize `Location | Location[] | LocationLink[] | null` into `RustLspTarget[]`
- [ ] Normalize `DocumentSymbol[]` (hierarchical) vs `SymbolInformation[]` (flat) into `RustDocSymbol[]`
- [ ] Normalize call/type hierarchy items into `RustCallLink[]` / `RustTypeLink[]`
- [ ] Handle `publishDiagnostics` notifications (capture in `_diagnostics_by_uri`)
- [ ] Session caching with `threading.Lock` and `atexit.register`
- [ ] Timeout handling: separate startup timeout (8s default) vs query timeout (0.5s)
- [ ] Test: roundtrip contracts encode/decode
- [ ] Test: `enrich_with_rust_lsp` returns None when rust-analyzer unavailable
- [ ] Wire into `rust_enrichment.py` alongside existing tree-sitter enrichment

---

## S3: Structural Neighborhood Collector (AST-based)

### Goal

Build a fast, local, parser-only collector that computes parent/child/sibling relationships from CQ's existing `ScanContext.interval_index` and def/call records. This works without any LSP server and provides the baseline structural neighborhood for every query target.

### Key Architectural Elements

- Uses existing `ScanContext` infrastructure: `interval_index` (O(log n) containment), `def_records`, `call_records`, `calls_by_def`
- Pure structural computation — no network calls, no subprocess
- Outputs `NeighborhoodSliceV1` objects directly
- Reconciles with LSP `documentSymbol` results when available (S2/S4)

### Representative Code

```python
# tools/cq/neighborhood/structural_collector.py
from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from tools.cq.core.schema import Anchor
from tools.cq.core.snb_schema import (
    NeighborhoodSliceV1,
    RelationKind,
    SemanticEdgeV1,
    SemanticNodeRefV1,
)

if TYPE_CHECKING:
    from tools.cq.query.executor import ScanContext

_MAX_SIBLINGS = 20
_MAX_CHILDREN = 30


def collect_structural_neighborhood(
    anchor: Anchor,
    scan_ctx: ScanContext,
    root: Path,
    *,
    max_siblings: int = _MAX_SIBLINGS,
    max_children: int = _MAX_CHILDREN,
) -> StructuralNeighborhood:
    """Compute parent/child/sibling from interval index + def records.

    Parameters
    ----------
    anchor
        The target source location.
    scan_ctx
        Shared scan context with interval_index, def_records, etc.
    root
        Repository root for relative path computation.

    Returns
    -------
    StructuralNeighborhood
        Slices + edges for the structural neighborhood.
    """
    target_file = anchor.file
    target_line = anchor.line

    # 1. Find containing def (parent) via interval index
    containing_def = _find_containing_def(target_file, target_line, scan_ctx)

    # 2. Find children (nested defs inside the containing def)
    children = _find_children(containing_def, scan_ctx, limit=max_children)

    # 3. Find siblings (other children of containing def's parent)
    parent_of_container = (
        _find_containing_def(
            containing_def.file, containing_def.start_line, scan_ctx
        )
        if containing_def is not None
        else None
    )
    siblings = _find_siblings(
        containing_def, parent_of_container, scan_ctx, limit=max_siblings
    )

    # 4. Build enclosing context (containing function signature + scope)
    enclosing = _build_enclosing_context(containing_def, scan_ctx)

    return StructuralNeighborhood(
        parents=_to_parent_slice(containing_def, parent_of_container),
        children=_to_slice("children", children, max_children),
        siblings=_to_slice("siblings", siblings, max_siblings),
        enclosing_context=enclosing,
        edges=_build_containment_edges(
            containing_def, children, siblings
        ),
    )


def _find_containing_def(
    file: str, line: int, scan_ctx: ScanContext
) -> DefRecord | None:
    """Find smallest enclosing def via interval index O(log n) lookup."""
    # Use scan_ctx.interval_index for containment query
    # Return the tightest-enclosing def record
    ...


def _find_children(
    parent: DefRecord | None, scan_ctx: ScanContext, *, limit: int
) -> list[DefRecord]:
    """Find nested defs whose range is contained in parent's range."""
    ...


def _find_siblings(
    target: DefRecord | None,
    parent: DefRecord | None,
    scan_ctx: ScanContext,
    *,
    limit: int,
) -> list[DefRecord]:
    """Find other children of parent, excluding target."""
    ...


def _def_to_node(rec: DefRecord, node_id: str) -> SemanticNodeRefV1:
    """Convert a def record to a SemanticNodeRefV1."""
    return SemanticNodeRefV1(
        node_id=node_id,
        language="python",  # or "rust" based on file extension
        display_name=rec.name,
        symbol_kind=rec.kind,
        qname=rec.qualified_name,
        anchor=Anchor(file=rec.file, line=rec.start_line),
    )
```

```python
# tools/cq/neighborhood/structural_collector.py (continued)
from __future__ import annotations

from dataclasses import dataclass

from tools.cq.core.snb_schema import (
    NeighborhoodSliceV1,
    SemanticEdgeV1,
    SemanticNodeRefV1,
)


@dataclass(frozen=True)
class StructuralNeighborhood:
    """Result of structural neighborhood collection."""

    parents: NeighborhoodSliceV1
    children: NeighborhoodSliceV1
    siblings: NeighborhoodSliceV1
    enclosing_context: NeighborhoodSliceV1 | None
    edges: tuple[SemanticEdgeV1, ...]
```

### Target Files

| Action | File |
|--------|------|
| **Create** | `tools/cq/neighborhood/__init__.py` |
| **Create** | `tools/cq/neighborhood/structural_collector.py` |
| **Create** | `tests/unit/cq/neighborhood/test_structural_collector.py` |
| **Edit** | `tools/cq/query/executor.py` (expose `ScanContext` for reuse) |
| **Edit** | `tools/cq/core/enrichment_facts.py` (register structural paths) |

### Implementation Checklist

- [ ] Create `tools/cq/neighborhood/` package
- [ ] Implement `collect_structural_neighborhood()` function
- [ ] `_find_containing_def()`: interval index containment query, smallest enclosing
- [ ] `_find_children()`: filter def_records whose range is inside parent
- [ ] `_find_siblings()`: same parent's children, exclude target
- [ ] `_build_enclosing_context()`: containing function name, signature, return type
- [ ] `_def_to_node()`: convert def/call records to `SemanticNodeRefV1`
- [ ] `_to_slice()`: convert record lists to `NeighborhoodSliceV1` with counts
- [ ] `_build_containment_edges()`: create `SemanticEdgeV1` with `kind="contains"`
- [ ] Language detection: use file extension (`.py` → python, `.rs` → rust)
- [ ] Handle module-level scope (no containing def → parent is module)
- [ ] Handle class methods (containing class → parent chain)
- [ ] Test: function inside class inside module → correct 3-level parent chain
- [ ] Test: class with multiple methods → correct sibling set
- [ ] Test: nested functions → correct child discovery
- [ ] Test: empty scan context → empty slices (no crash)

---

## S4: SNB Assembler + Bundle Builder Pipeline

### Goal

Build the assembler that combines outputs from structural collector (S3), pyrefly LSP (existing), and rust LSP (S2) into a unified `SemanticNeighborhoodBundleV1`. This is the "Collector → Normalizer → Assembler" pipeline from the design doc.

### Key Architectural Elements

- Orchestrates parallel collectors with timeouts
- Merges structural + LSP results into a single bundle
- Generates `BundleViewV1` (Finding/Section lists) for CQ output compatibility
- Stores heavy payloads as artifacts (JSON/msgpack)
- Applies top-K caps and relevance scoring

### Representative Code

```python
# tools/cq/neighborhood/bundle_builder.py
from __future__ import annotations

import hashlib
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import TYPE_CHECKING

from tools.cq.core.schema import Anchor, Artifact, Finding, Section, ms
from tools.cq.core.snb_schema import (
    BundleViewV1,
    NeighborhoodGraphSummaryV1,
    NeighborhoodSliceV1,
    SemanticNeighborhoodBundleV1,
    SemanticNeighborhoodMetaV1,
    SemanticNodeRefV1,
)
from tools.cq.neighborhood.structural_collector import (
    StructuralNeighborhood,
    collect_structural_neighborhood,
)

if TYPE_CHECKING:
    from tools.cq.query.executor import ScanContext

_MAX_PARALLEL_LSP = 2
_TOP_K_DEFAULT = 5


class BundleBuildRequest:
    """Request for building a semantic neighborhood bundle."""

    def __init__(
        self,
        *,
        anchor: Anchor,
        root: Path,
        scan_ctx: ScanContext,
        language: str,
        symbol_hint: str | None = None,
        top_k: int = _TOP_K_DEFAULT,
        enable_lsp: bool = True,
        artifact_dir: Path | None = None,
    ) -> None:
        self.anchor = anchor
        self.root = root
        self.scan_ctx = scan_ctx
        self.language = language
        self.symbol_hint = symbol_hint
        self.top_k = top_k
        self.enable_lsp = enable_lsp
        self.artifact_dir = artifact_dir


def build_neighborhood_bundle(
    request: BundleBuildRequest,
) -> SemanticNeighborhoodBundleV1:
    """Build a complete semantic neighborhood bundle.

    Parameters
    ----------
    request
        Build request with anchor, context, and configuration.

    Returns
    -------
    SemanticNeighborhoodBundleV1
        Assembled bundle with structural + LSP-enriched slices.
    """
    started = ms()

    # Phase 1: Always-available structural neighborhood
    structural = collect_structural_neighborhood(
        anchor=request.anchor,
        scan_ctx=request.scan_ctx,
        root=request.root,
    )

    # Phase 2: Optional LSP enrichment (parallel, fail-open)
    lsp_callers: list[dict[str, object]] = []
    lsp_callees: list[dict[str, object]] = []
    lsp_payload: dict[str, object] | None = None

    if request.enable_lsp:
        lsp_payload = _collect_lsp_enrichment(request)
        if lsp_payload is not None:
            call_graph = lsp_payload.get("call_graph")
            if isinstance(call_graph, dict):
                raw_inc = call_graph.get("incoming_callers")
                if isinstance(raw_inc, list):
                    lsp_callers = raw_inc
                raw_out = call_graph.get("outgoing_callees")
                if isinstance(raw_out, list):
                    lsp_callees = raw_out

    # Phase 3: Assemble slices
    slices = _assemble_slices(
        structural=structural,
        lsp_callers=lsp_callers,
        lsp_callees=lsp_callees,
        lsp_payload=lsp_payload,
        top_k=request.top_k,
    )

    # Phase 4: Build subject node
    subject = _build_subject_node(request, lsp_payload)

    # Phase 5: Build graph summary
    graph = _build_graph_summary(structural, lsp_callers, lsp_callees)

    # Phase 6: Materialize view (Finding/Section lists)
    view = _materialize_view(subject, slices, lsp_payload)

    # Phase 7: Store artifacts if artifact_dir is set
    artifacts = _store_artifacts(
        request.artifact_dir, slices, lsp_payload
    )

    meta = SemanticNeighborhoodMetaV1(
        workspace_root=str(request.root),
        query_text=request.symbol_hint,
        created_at_ms=started,
        limits={"top_k": request.top_k},
    )

    return SemanticNeighborhoodBundleV1(
        meta=meta,
        subject=subject,
        slices=tuple(slices),
        graph=graph,
        view=view,
        artifacts=tuple(artifacts),
    )


def _collect_lsp_enrichment(
    request: BundleBuildRequest,
) -> dict[str, object] | None:
    """Dispatch to pyrefly or rust-analyzer based on language."""
    if request.language == "python":
        from tools.cq.search.pyrefly_lsp import (
            PyreflyLspRequest,
            enrich_with_pyrefly_lsp,
        )
        return enrich_with_pyrefly_lsp(PyreflyLspRequest(
            root=request.root,
            file_path=request.root / request.anchor.file,
            line=request.anchor.line,
            col=request.anchor.col or 0,
            symbol_hint=request.symbol_hint,
        ))
    if request.language == "rust":
        from tools.cq.search.rust_lsp import (
            RustLspRequest,
            enrich_with_rust_lsp,
        )
        return enrich_with_rust_lsp(RustLspRequest(
            root=request.root,
            file_path=request.root / request.anchor.file,
            line=request.anchor.line,
            col=request.anchor.col or 0,
            symbol_hint=request.symbol_hint,
        ))
    return None


def _assemble_slices(
    *,
    structural: StructuralNeighborhood,
    lsp_callers: list[dict[str, object]],
    lsp_callees: list[dict[str, object]],
    lsp_payload: dict[str, object] | None,
    top_k: int,
) -> list[NeighborhoodSliceV1]:
    """Merge structural + LSP into ordered slice list."""
    slices: list[NeighborhoodSliceV1] = [
        structural.parents,
        structural.children,
        structural.siblings,
    ]
    if structural.enclosing_context is not None:
        slices.append(structural.enclosing_context)

    # Add caller/callee slices from LSP
    if lsp_callers:
        slices.append(_callers_to_slice(lsp_callers, top_k))
    if lsp_callees:
        slices.append(_callees_to_slice(lsp_callees, top_k))

    return slices


def _materialize_view(
    subject: SemanticNodeRefV1 | None,
    slices: list[NeighborhoodSliceV1],
    lsp_payload: dict[str, object] | None,
) -> BundleViewV1:
    """Convert bundle into CQ-compatible Finding/Section lists."""
    view = BundleViewV1()

    # Key finding: target TL;DR
    if subject is not None:
        view.key_findings.append(Finding(
            category="symbol.identity",
            message=f"{subject.symbol_kind or 'symbol'}: {subject.display_name}",
            anchor=subject.anchor,
            details={"kind": "cq.snb.node.v1"},
        ))

    # Sections from slices
    for s in slices:
        section_findings: list[Finding] = []
        for node in s.preview:
            section_findings.append(Finding(
                category=f"neighborhood.{s.kind}",
                message=f"{node.symbol_kind or ''}: {node.display_name}",
                anchor=node.anchor,
            ))
        if s.total > len(s.preview):
            section_findings.append(Finding(
                category="neighborhood.overflow",
                message=f"… and {s.total - len(s.preview)} more",
            ))
        view.sections.append(Section(
            title=s.title or s.kind,
            findings=section_findings,
            collapsed=s.collapsed,
        ))

    return view
```

### Target Files

| Action | File |
|--------|------|
| **Create** | `tools/cq/neighborhood/bundle_builder.py` |
| **Create** | `tests/unit/cq/neighborhood/test_bundle_builder.py` |
| **Edit** | `tools/cq/core/schema.py` (no changes needed — uses existing types) |
| **Edit** | `tools/cq/search/pyrefly_lsp.py` (import path only, no changes) |
| **Edit** | `tools/cq/search/rust_lsp.py` (import path only, no changes) |

### Implementation Checklist

- [ ] Implement `BundleBuildRequest` with all configuration fields
- [ ] Implement `build_neighborhood_bundle()` orchestrator
- [ ] `_collect_lsp_enrichment()`: language-dispatched LSP probe (pyrefly/rust-analyzer)
- [ ] `_assemble_slices()`: merge structural + LSP slices, apply top-K caps
- [ ] `_build_subject_node()`: create subject `SemanticNodeRefV1` from anchor + LSP hover
- [ ] `_build_graph_summary()`: count nodes/edges for `NeighborhoodGraphSummaryV1`
- [ ] `_materialize_view()`: convert slices → `BundleViewV1` (Finding/Section lists)
- [ ] `_store_artifacts()`: write full slice data to JSON files if artifact_dir is set
- [ ] `_callers_to_slice()`: convert LSP caller list → `NeighborhoodSliceV1`
- [ ] `_callees_to_slice()`: convert LSP callee list → `NeighborhoodSliceV1`
- [ ] Apply relevance scoring to sort previews within slices
- [ ] Ensure fail-open: LSP failure → structural-only bundle (no exception)
- [ ] Test: Python anchor → pyrefly enrichment path dispatched
- [ ] Test: Rust anchor → rust-analyzer enrichment path dispatched
- [ ] Test: No LSP → structural-only bundle produced
- [ ] Test: BundleViewV1 sections have correct collapsed defaults per design
- [ ] Test: Artifact paths are relative, format is "json"

---

## S5: LDMD Progressive Disclosure Format + Extractor

### Goal

Implement the LDMD (LLM-friendly progressive disclosure markdown) format: marker scheme, section index, and extraction protocol (INDEX/GET/SEARCH/NEIGHBORS). This is independent of the SNB pipeline and can be used by any CQ output.

### Key Architectural Elements

- HTML comment markers (`<!--LDMD:BEGIN ...-->`) invisible in Rich terminal rendering
- Regex-based marker parsing (deterministic, byte-offset stable)
- Section index with line/byte ranges, titles, IDs, parent chains
- Slice extraction with mode control (tldr/body/full) and pagination

### Representative Code

```python
# tools/cq/ldmd/format.py
from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

# Marker regexes (must be on own line, no indentation)
_BEGIN_RE = re.compile(
    r'^<!--LDMD:BEGIN\b[^>]*\bid="([^"]+)"'
    r'(?:[^>]*\btitle="([^"]*)")?'
    r'(?:[^>]*\blevel="(\d+)")?'
    r'(?:[^>]*\bparent="([^"]*)")?'
    r'(?:[^>]*\btags="([^"]*)")?'
    r'[^>]*-->$',
    re.MULTILINE,
)
_END_RE = re.compile(
    r'^<!--LDMD:END\b[^>]*\bid="([^"]+)"[^>]*-->$',
    re.MULTILINE,
)
_TLDR_BEGIN_RE = re.compile(
    r'^<!--LDMD:TLDR_BEGIN\b[^>]*\bid="([^"]+)"[^>]*-->$',
    re.MULTILINE,
)
_TLDR_END_RE = re.compile(
    r'^<!--LDMD:TLDR_END\b[^>]*\bid="([^"]+)"[^>]*-->$',
    re.MULTILINE,
)
_MANIFEST_RE = re.compile(
    r'^<!--LDMD:MANIFEST\b(.*?)-->$',
    re.MULTILINE | re.DOTALL,
)


@dataclass(frozen=True)
class LdmdSectionEntry:
    """Indexed section metadata (no content)."""
    id: str
    title: str
    parent: str | None
    level: int
    tags: tuple[str, ...]
    line_start: int
    line_end: int
    byte_start: int
    byte_end: int
    byte_size: int
    has_tldr: bool
    tldr_byte_start: int | None = None
    tldr_byte_end: int | None = None


@dataclass(frozen=True)
class LdmdIndex:
    """Full document index (metadata only, no content)."""
    doc_id: str | None
    schema: int
    sections: tuple[LdmdSectionEntry, ...]


@dataclass(frozen=True)
class LdmdSlice:
    """Extracted content slice from a section."""
    id: str
    mode: str  # tldr | body | full
    content: str
    truncated: bool
    cursor: int | None = None  # byte offset for continuation


def build_index(content: bytes) -> LdmdIndex:
    """Build section index from LDMD-marked document.

    Parameters
    ----------
    content
        Raw document bytes (stable offsets).

    Returns
    -------
    LdmdIndex
        Indexed sections with byte ranges.
    """
    text = content.decode("utf-8", errors="replace")
    lines = text.split("\n")

    sections: list[LdmdSectionEntry] = []
    open_sections: dict[str, dict[str, object]] = {}

    byte_offset = 0
    for line_num, line in enumerate(lines, start=1):
        line_bytes = len(line.encode("utf-8")) + 1  # +1 for newline

        begin_match = _BEGIN_RE.match(line)
        if begin_match:
            sid = begin_match.group(1)
            open_sections[sid] = {
                "title": begin_match.group(2) or sid,
                "level": int(begin_match.group(3) or "2"),
                "parent": begin_match.group(4) or None,
                "tags": tuple(
                    (begin_match.group(5) or "").split(",")
                ) if begin_match.group(5) else (),
                "line_start": line_num,
                "byte_start": byte_offset,
                "has_tldr": False,
                "tldr_byte_start": None,
                "tldr_byte_end": None,
            }

        end_match = _END_RE.match(line)
        if end_match:
            sid = end_match.group(1)
            if sid in open_sections:
                info = open_sections.pop(sid)
                entry = LdmdSectionEntry(
                    id=sid,
                    title=str(info["title"]),
                    parent=info.get("parent"),  # type: ignore[arg-type]
                    level=int(info.get("level", 2)),  # type: ignore[arg-type]
                    tags=info.get("tags", ()),  # type: ignore[arg-type]
                    line_start=int(info["line_start"]),  # type: ignore[arg-type]
                    line_end=line_num,
                    byte_start=int(info["byte_start"]),  # type: ignore[arg-type]
                    byte_end=byte_offset + line_bytes,
                    byte_size=byte_offset + line_bytes - int(info["byte_start"]),  # type: ignore[arg-type]
                    has_tldr=bool(info.get("has_tldr")),
                    tldr_byte_start=info.get("tldr_byte_start"),  # type: ignore[arg-type]
                    tldr_byte_end=info.get("tldr_byte_end"),  # type: ignore[arg-type]
                )
                sections.append(entry)

        # Track TL;DR markers
        tldr_begin = _TLDR_BEGIN_RE.match(line)
        if tldr_begin:
            sid = tldr_begin.group(1)
            if sid in open_sections:
                open_sections[sid]["has_tldr"] = True
                open_sections[sid]["tldr_byte_start"] = byte_offset

        tldr_end = _TLDR_END_RE.match(line)
        if tldr_end:
            sid = tldr_end.group(1)
            if sid in open_sections:
                open_sections[sid]["tldr_byte_end"] = byte_offset + line_bytes

        byte_offset += line_bytes

    # Extract doc_id from manifest if present
    manifest_match = _MANIFEST_RE.search(text)
    doc_id = None
    if manifest_match:
        for mline in manifest_match.group(1).splitlines():
            if "doc_id" in mline and ":" in mline:
                doc_id = mline.split(":", 1)[1].strip().strip('"')

    return LdmdIndex(
        doc_id=doc_id, schema=1, sections=tuple(sections)
    )


def get_slice(
    content: bytes,
    index: LdmdIndex,
    section_id: str,
    *,
    mode: str = "full",
    limit_bytes: int = 0,
    cursor: int = 0,
) -> LdmdSlice | None:
    """Extract content slice from indexed section.

    Parameters
    ----------
    content
        Raw document bytes.
    index
        Pre-built section index.
    section_id
        Section ID to extract.
    mode
        "tldr" | "body" | "full"
    limit_bytes
        Max bytes to return (0 = unlimited).
    cursor
        Byte offset to resume from (pagination).

    Returns
    -------
    LdmdSlice or None
        Extracted slice, or None if section not found.
    """
    entry = next((s for s in index.sections if s.id == section_id), None)
    if entry is None:
        return None

    if mode == "tldr" and entry.has_tldr and entry.tldr_byte_start is not None:
        start = entry.tldr_byte_start
        end = entry.tldr_byte_end or entry.byte_end
    elif mode == "body" and entry.has_tldr and entry.tldr_byte_end is not None:
        start = entry.tldr_byte_end
        end = entry.byte_end
    else:
        start = entry.byte_start
        end = entry.byte_end

    actual_start = start + cursor
    raw = content[actual_start:end]
    text = raw.decode("utf-8", errors="replace")

    # Strip marker lines from output
    cleaned_lines = [
        line for line in text.splitlines(keepends=True)
        if not line.strip().startswith("<!--LDMD:")
    ]
    cleaned = "".join(cleaned_lines)

    truncated = False
    next_cursor: int | None = None
    if limit_bytes > 0 and len(cleaned.encode("utf-8")) > limit_bytes:
        cleaned = cleaned.encode("utf-8")[:limit_bytes].decode(
            "utf-8", errors="ignore"
        )
        truncated = True
        next_cursor = cursor + limit_bytes

    return LdmdSlice(
        id=section_id,
        mode=mode,
        content=cleaned,
        truncated=truncated,
        cursor=next_cursor,
    )
```

```python
# tools/cq/ldmd/writer.py — LDMD document writer
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tools.cq.core.snb_schema import (
        NeighborhoodSliceV1,
        SemanticNeighborhoodBundleV1,
        SemanticNodeRefV1,
    )


def render_ldmd_document(
    bundle: SemanticNeighborhoodBundleV1,
    *,
    include_manifest: bool = True,
) -> str:
    """Render SNB bundle as LDMD-marked markdown document.

    Parameters
    ----------
    bundle
        The semantic neighborhood bundle to render.
    include_manifest
        Whether to include the LDMD manifest header.

    Returns
    -------
    str
        LDMD-marked markdown string.
    """
    parts: list[str] = []

    if include_manifest:
        parts.append(
            f'<!--LDMD:MANIFEST\n'
            f'schema: 1\n'
            f'doc_id: "{bundle.bundle_kind}"\n'
            f'created_at: "{bundle.meta.created_at_ms}"\n'
            f'-->\n\n'
        )

    # Subject section (always uncollapsed)
    if bundle.subject is not None:
        parts.append(_render_subject_section(bundle.subject))

    # Neighborhood slices
    for i, s in enumerate(bundle.slices):
        section_id = f"S{i:02d}"
        parts.append(_render_slice_section(s, section_id))

    return "".join(parts)


def _render_subject_section(subject: SemanticNodeRefV1) -> str:
    kind_label = subject.symbol_kind or "symbol"
    loc = f" ({subject.anchor.file}:{subject.anchor.line})" if subject.anchor else ""
    return (
        f'<!--LDMD:BEGIN id="S00" title="Target Summary" level="1"-->\n'
        f"# {kind_label}: {subject.display_name}{loc}\n\n"
        f'<!--LDMD:END id="S00"-->\n\n'
    )


def _render_slice_section(s: NeighborhoodSliceV1, section_id: str) -> str:
    title = s.title or s.kind
    lines: list[str] = []
    lines.append(
        f'<!--LDMD:BEGIN id="{section_id}" '
        f'title="{title}" level="2"-->\n'
    )
    lines.append(f"## {title} ({s.total} total)\n\n")

    # TL;DR block
    lines.append(f'<!--LDMD:TLDR_BEGIN id="{section_id}"-->\n')
    lines.append(f"- {s.total} items in this category\n")
    if s.notes:
        for note in s.notes:
            lines.append(f"- {note}\n")
    lines.append(f'<!--LDMD:TLDR_END id="{section_id}"-->\n\n')

    # Body: preview items
    for node in s.preview:
        anchor_ref = ""
        if node.anchor:
            anchor_ref = f" `{node.anchor.file}:{node.anchor.line}`"
        lines.append(
            f"- **{node.display_name}** ({node.symbol_kind or '?'}){anchor_ref}\n"
        )
    if s.total > len(s.preview):
        lines.append(f"\n*… {s.total - len(s.preview)} more (expand for full list)*\n")

    lines.append(f'\n<!--LDMD:END id="{section_id}"-->\n\n')
    return "".join(lines)
```

### Target Files

| Action | File |
|--------|------|
| **Create** | `tools/cq/ldmd/__init__.py` |
| **Create** | `tools/cq/ldmd/format.py` |
| **Create** | `tools/cq/ldmd/writer.py` |
| **Create** | `tests/unit/cq/ldmd/test_format.py` |
| **Create** | `tests/unit/cq/ldmd/test_writer.py` |

### Implementation Checklist

- [ ] Create `tools/cq/ldmd/` package
- [ ] Implement `build_index()`: parse BEGIN/END/TLDR markers into `LdmdIndex`
- [ ] Implement `get_slice()`: extract content by section ID with mode/limit/cursor
- [ ] Implement `search_sections()`: grep within scoped section IDs (returns snippets)
- [ ] Implement `get_neighbors()`: return prev/next section IDs
- [ ] Implement `render_ldmd_document()`: SNB bundle → LDMD markdown
- [ ] Implement `_render_subject_section()`: target TL;DR in LDMD format
- [ ] Implement `_render_slice_section()`: slice → LDMD section with markers
- [ ] Marker validation: BEGIN/END IDs must match, no nesting of same ID
- [ ] Byte-offset stability: read as `rb`, compute offsets from raw bytes
- [ ] Test: roundtrip write → index → get_slice returns correct content
- [ ] Test: TLDR extraction returns only TLDR block
- [ ] Test: body extraction excludes TLDR block
- [ ] Test: limit_bytes truncation returns continuation cursor
- [ ] Test: marker lines are stripped from extracted content
- [ ] Test: Rich `Markdown(...)` renders LDMD doc without visible markers
- [ ] Test: nested sections with parent attribute build correct tree

---

## S6: Section Layout Renderer + Bundle View Materializer

### Goal

Implement the ordered section layout from the design doc (01-Target TL;DR through 12-Provenance) and a renderer that converts `SemanticNeighborhoodBundleV1` into markdown output matching CQ's existing report style. Integrate with LDMD for progressive disclosure.

### Key Architectural Elements

- Section ordering follows design doc (01-12)
- Collapse heuristics: default collapsed states per section type
- Markdown output reuses existing `_format_finding()` patterns from `report.py`
- LDMD markers embedded for progressive disclosure
- Can produce both "inline" markdown and "artifact + stub" output

### Representative Code

```python
# tools/cq/neighborhood/section_layout.py
from __future__ import annotations

from tools.cq.core.schema import Artifact, Finding, Section
from tools.cq.core.snb_schema import (
    BundleViewV1,
    NeighborhoodSliceV1,
    SemanticNeighborhoodBundleV1,
    SemanticNodeRefV1,
)

# Section order constants matching design doc
SECTION_ORDER: tuple[str, ...] = (
    "target_tldr",         # 01 - collapsed: False
    "neighborhood_summary", # 02 - collapsed: False
    "parents",             # 03 - collapsed: dynamic
    "children",            # 04 - collapsed: True
    "siblings",            # 05 - collapsed: True
    "callers",             # 06 - collapsed: True
    "callees",             # 07 - collapsed: True
    "callsite_context",    # 08 - collapsed: True (auto if <=3)
    "imports_context",     # 09 - collapsed: True
    "lsp_deep_signals",    # 10 - collapsed: True
    "suggested_followups", # 11 - collapsed: False
    "provenance",          # 12 - collapsed: True
)

_UNCOLLAPSED_SECTIONS = frozenset({
    "target_tldr", "neighborhood_summary", "suggested_followups"
})
_PARENT_COLLAPSE_THRESHOLD = 3


def materialize_section_layout(
    bundle: SemanticNeighborhoodBundleV1,
) -> BundleViewV1:
    """Convert bundle into ordered Section/Finding layout.

    Parameters
    ----------
    bundle
        Assembled SNB bundle.

    Returns
    -------
    BundleViewV1
        Ordered, collapse-configured sections.
    """
    view = BundleViewV1()

    # 01: Target TL;DR
    view.key_findings.extend(
        _build_target_findings(bundle.subject)
    )

    # 02: Neighborhood Summary
    view.sections.append(
        _build_summary_section(bundle)
    )

    # 03-07: Structural + call graph slices
    for s in bundle.slices:
        section = _slice_to_section(s)
        view.sections.append(section)

    # 11: Suggested Follow-ups
    view.sections.append(
        _build_followup_section(bundle)
    )

    # 12: Provenance
    view.sections.append(
        _build_provenance_section(bundle)
    )

    return view


def _build_target_findings(
    subject: SemanticNodeRefV1 | None,
) -> list[Finding]:
    if subject is None:
        return []
    kind = subject.symbol_kind or "symbol"
    msg = f"{kind}: {subject.display_name}"
    if subject.qname:
        msg += f" ({subject.qname})"
    return [Finding(
        category="symbol.identity",
        message=msg,
        anchor=subject.anchor,
        details={"kind": "cq.snb.node.v1"},
    )]


def _build_summary_section(
    bundle: SemanticNeighborhoodBundleV1,
) -> Section:
    findings: list[Finding] = []
    for s in bundle.slices:
        findings.append(Finding(
            category="neighborhood.summary",
            message=f"{s.title or s.kind}: {s.total} items",
        ))
    return Section(
        title="Neighborhood Summary",
        findings=findings,
        collapsed=False,
    )


def _slice_to_section(s: NeighborhoodSliceV1) -> Section:
    """Convert a single slice to a CQ Section."""
    findings: list[Finding] = []
    for node in s.preview:
        findings.append(Finding(
            category=f"neighborhood.{s.kind}",
            message=f"{node.symbol_kind or ''}: {node.display_name}",
            anchor=node.anchor,
        ))
    if s.total > len(s.preview):
        findings.append(Finding(
            category="neighborhood.overflow",
            message=f"… and {s.total - len(s.preview)} more",
            severity="info",
        ))

    collapsed = s.collapsed
    if s.kind == "parents" and s.total <= _PARENT_COLLAPSE_THRESHOLD:
        collapsed = False

    return Section(
        title=s.title or s.kind.replace("_", " ").title(),
        findings=findings,
        collapsed=collapsed,
    )


def _build_followup_section(
    bundle: SemanticNeighborhoodBundleV1,
) -> Section:
    """Suggest targeted expansion commands."""
    suggestions: list[Finding] = []
    for s in bundle.slices:
        if s.total > len(s.preview):
            suggestions.append(Finding(
                category="suggested_followup",
                message=f"Expand {s.kind} (full list: {s.total} items)",
            ))
    return Section(
        title="Suggested Follow-ups",
        findings=suggestions,
        collapsed=False,
    )


def _build_provenance_section(
    bundle: SemanticNeighborhoodBundleV1,
) -> Section:
    findings: list[Finding] = []
    if bundle.meta.workspace_root:
        findings.append(Finding(
            category="provenance",
            message=f"Workspace: {bundle.meta.workspace_root}",
        ))
    if bundle.meta.limits:
        findings.append(Finding(
            category="provenance",
            message=f"Limits: {bundle.meta.limits}",
        ))
    for diag in bundle.diagnostics:
        findings.append(Finding(
            category="provenance.diagnostic",
            message=diag,
        ))
    return Section(
        title="Provenance & Budgets",
        findings=findings,
        collapsed=True,
    )
```

```python
# tools/cq/neighborhood/snb_renderer.py
from __future__ import annotations

from pathlib import Path

from tools.cq.core.report import (
    _format_finding,  # reuse existing finding formatter
)
from tools.cq.core.schema import CqResult, Section
from tools.cq.core.snb_schema import SemanticNeighborhoodBundleV1
from tools.cq.ldmd.writer import render_ldmd_document
from tools.cq.neighborhood.section_layout import materialize_section_layout


def render_bundle_markdown(
    bundle: SemanticNeighborhoodBundleV1,
    *,
    root: Path | None = None,
    ldmd_mode: bool = False,
) -> str:
    """Render bundle as markdown output.

    Parameters
    ----------
    bundle
        The SNB bundle to render.
    root
        Repository root for anchor formatting.
    ldmd_mode
        If True, emit LDMD markers for progressive disclosure.

    Returns
    -------
    str
        Markdown string.
    """
    if ldmd_mode:
        return render_ldmd_document(bundle)

    view = materialize_section_layout(bundle)
    lines: list[str] = []

    # Key findings (always shown)
    if view.key_findings:
        lines.append("## Target\n")
        for f in view.key_findings:
            lines.append(_format_finding(f, show_anchor=True, root=root))
            lines.append("\n")
        lines.append("\n")

    # Sections
    for section in view.sections:
        if section.collapsed:
            lines.append(f"### {section.title} (collapsed)\n\n")
            # Show count only
            lines.append(f"*{len(section.findings)} items*\n\n")
        else:
            lines.append(f"### {section.title}\n\n")
            for f in section.findings:
                lines.append(
                    _format_finding(f, show_anchor=True, root=root)
                )
                lines.append("\n")
            lines.append("\n")

    return "".join(lines)
```

### Target Files

| Action | File |
|--------|------|
| **Create** | `tools/cq/neighborhood/section_layout.py` |
| **Create** | `tools/cq/neighborhood/snb_renderer.py` |
| **Edit** | `tools/cq/core/report.py` (extract `_format_finding` as public or importable) |
| **Edit** | `tools/cq/cli_app/result.py` (add bundle rendering dispatch) |
| **Create** | `tests/unit/cq/neighborhood/test_section_layout.py` |

### Implementation Checklist

- [ ] Implement `materialize_section_layout()` with design-doc ordering
- [ ] Implement collapse heuristics per section type
  - [ ] `target_tldr`, `neighborhood_summary`, `suggested_followups` → uncollapsed
  - [ ] `parents` → uncollapsed if ≤3, else collapsed
  - [ ] All others → collapsed by default
- [ ] Implement `_build_target_findings()` with identity/signature/doc summary
- [ ] Implement `_build_summary_section()` with counts per relationship category
- [ ] Implement `_slice_to_section()` with overflow indicators
- [ ] Implement `_build_followup_section()` with expandable suggestions
- [ ] Implement `_build_provenance_section()` with meta/limits/diagnostics
- [ ] Implement `render_bundle_markdown()` for both inline and LDMD modes
- [ ] Refactor `report.py` to make `_format_finding` importable (or duplicate minimal logic)
- [ ] Test: section ordering matches design doc (01-12)
- [ ] Test: collapsed defaults are correct per section type
- [ ] Test: overflow indicators appear when total > preview count
- [ ] Test: LDMD mode produces valid LDMD markers
- [ ] Test: inline mode produces clean markdown without markers

---

## S7: CLI Integration + `cq neighborhood` Command

### Goal

Add a new `cq neighborhood` (alias `cq nb`) CLI command that orchestrates the full SNB pipeline: scan → collect → assemble → render. Wire into the existing CLI framework, output format dispatch, and multi-step execution system.

### Key Architectural Elements

- New command in `tools/cq/cli_app/commands/`
- Reuses existing `Toolchain`, `RunContext`, query execution infrastructure
- Supports `--format md|json|ldmd` output modes
- Supports `--expand` for on-demand slice expansion
- Integrates with `cq run` step system (`"type": "neighborhood"`)

### Representative Code

```python
# tools/cq/cli_app/commands/neighborhood.py
from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Annotated

import cyclopts

from tools.cq.cli_app.config import build_config_chain
from tools.cq.core.run_context import RunContext
from tools.cq.core.schema import CqResult, mk_result, mk_runmeta, ms
from tools.cq.neighborhood.bundle_builder import (
    BundleBuildRequest,
    build_neighborhood_bundle,
)
from tools.cq.neighborhood.section_layout import materialize_section_layout
from tools.cq.neighborhood.snb_renderer import render_bundle_markdown

if TYPE_CHECKING:
    from tools.cq.core.toolchain import Toolchain

neighborhood_app = cyclopts.App(name="neighborhood", help="Semantic neighborhood analysis")
nb_app = cyclopts.App(name="nb", help="Alias for neighborhood")


@neighborhood_app.default
@nb_app.default
def cmd_neighborhood(
    target: Annotated[str, cyclopts.Parameter(help="Target symbol or file:line")],
    *,
    root: str | None = None,
    in_dir: str | None = None,
    lang: str = "auto",
    top_k: int = 5,
    no_lsp: bool = False,
    expand: str | None = None,
    artifact_dir: str | None = None,
    format: str = "md",
) -> CqResult:
    """Analyze semantic neighborhood of a target symbol.

    Parameters
    ----------
    target
        Symbol name, "file:line", or "file:line:col" reference.
    root
        Repository root path.
    in_dir
        Restrict search scope to directory.
    lang
        Language hint: auto, python, rust.
    top_k
        Max items per neighborhood slice.
    no_lsp
        Disable LSP enrichment (structural only).
    expand
        Expand specific slice: parents, children, siblings, callers, callees.
    artifact_dir
        Directory to write artifact files.
    format
        Output format: md, json, ldmd.
    """
    started = ms()
    config = build_config_chain(root=root)
    repo_root = Path(config.root)

    # Resolve target to anchor + language
    anchor, language = _resolve_target(
        target, repo_root, in_dir, lang
    )

    # Build scan context (reuse query executor infrastructure)
    from tools.cq.query.executor import _build_scan_context
    scan_ctx = _build_scan_context(repo_root, language)

    # Build bundle
    request = BundleBuildRequest(
        anchor=anchor,
        root=repo_root,
        scan_ctx=scan_ctx,
        language=language,
        symbol_hint=target,
        top_k=top_k,
        enable_lsp=not no_lsp,
        artifact_dir=Path(artifact_dir) if artifact_dir else None,
    )
    bundle = build_neighborhood_bundle(request)

    # Convert to CqResult
    view = materialize_section_layout(bundle)
    run = mk_runmeta(
        macro="neighborhood",
        argv=["neighborhood", target],
        root=str(repo_root),
        started_ms=started,
    )
    result = mk_result(run)
    result.key_findings.extend(view.key_findings)
    result.sections.extend(view.sections)
    result.evidence.extend(view.evidence)

    # Store bundle as artifact if artifact_dir is set
    if request.artifact_dir:
        import msgspec
        bundle_path = request.artifact_dir / "snb_bundle.json"
        bundle_path.parent.mkdir(parents=True, exist_ok=True)
        bundle_path.write_bytes(
            msgspec.json.encode(bundle)
        )
        from tools.cq.core.schema import Artifact
        result.artifacts.append(
            Artifact(path=str(bundle_path), format="json")
        )

    return result


def _resolve_target(
    target: str,
    root: Path,
    in_dir: str | None,
    lang: str,
) -> tuple:
    """Resolve target string to (Anchor, language).

    Supports formats:
    - "function_name" → search for definition
    - "file.py:42" → direct file:line
    - "file.py:42:8" → direct file:line:col
    """
    from tools.cq.core.schema import Anchor

    # Try file:line:col format
    parts = target.rsplit(":", 2)
    if len(parts) >= 2:
        try:
            file_path = parts[0]
            line = int(parts[1])
            col = int(parts[2]) if len(parts) == 3 else 0
            language = _detect_language(file_path, lang)
            return Anchor(file=file_path, line=line, col=col), language
        except ValueError:
            pass

    # Fall back to symbol search
    from tools.cq.search.smart_search import smart_search_impl
    # Find first definition match
    ...
```

```python
# tools/cq/macros/neighborhood.py — macro wrapper for run/chain integration
from __future__ import annotations

from pathlib import Path

from tools.cq.core.schema import CqResult
from tools.cq.neighborhood.bundle_builder import (
    BundleBuildRequest,
    build_neighborhood_bundle,
)
from tools.cq.neighborhood.section_layout import materialize_section_layout


def cmd_neighborhood(
    target: str,
    root: Path,
    *,
    lang: str = "auto",
    top_k: int = 5,
    no_lsp: bool = False,
) -> CqResult:
    """Macro entry point for neighborhood analysis.

    Used by cq run step integration.
    """
    ...
```

### Target Files

| Action | File |
|--------|------|
| **Create** | `tools/cq/cli_app/commands/neighborhood.py` |
| **Create** | `tools/cq/macros/neighborhood.py` |
| **Edit** | `tools/cq/cli_app/app.py` (register `neighborhood_app` + `nb_app`) |
| **Edit** | `tools/cq/run/spec.py` (add `"neighborhood"` step type) |
| **Edit** | `tools/cq/run/runner.py` (dispatch `"neighborhood"` step) |
| **Create** | `tests/unit/cq/cli/test_neighborhood_command.py` |

### Implementation Checklist

- [ ] Create `neighborhood_app` and `nb_app` (alias) cyclopts apps
- [ ] Implement `cmd_neighborhood()` with all CLI parameters
- [ ] `_resolve_target()`: support "symbol_name", "file:line", "file:line:col" formats
- [ ] `_detect_language()`: use file extension (`.py` → python, `.rs` → rust) with `auto` fallback
- [ ] Wire into `app.py`: register both `neighborhood` and `nb` subcommands
- [ ] Support `--format md|json|ldmd` output dispatch
  - `md` → standard markdown via `render_bundle_markdown()`
  - `json` → msgspec JSON encode of `CqResult`
  - `ldmd` → LDMD-marked markdown via `render_ldmd_document()`
- [ ] Support `--artifact-dir` for writing bundle + slice artifacts
- [ ] Create `cmd_neighborhood()` macro wrapper for `cq run` integration
- [ ] Add `"neighborhood"` to `RunStepType` in `run/spec.py`
- [ ] Add dispatch case in `runner.py` for `"neighborhood"` step type
- [ ] Test: `cq neighborhood build_graph` produces valid CqResult
- [ ] Test: `cq nb src/semantics/pipeline.py:42` resolves file:line anchor
- [ ] Test: `--no-lsp` produces structural-only output
- [ ] Test: `--format json` produces valid JSON
- [ ] Test: `--format ldmd` produces LDMD-marked output
- [ ] Test: `cq run --steps '[{"type":"neighborhood","target":"foo"}]'` works

---

## Dependency Graph

```
S1 (SNB Schema)
 ├──► S3 (Structural Collector) ──► S4 (Bundle Builder) ──► S6 (Renderer) ──► S7 (CLI)
 └──► S4 ◄── S2 (Rust LSP)          ▲
                                      │
S5 (LDMD Format) ─────────────────────┘
```

- **S1** and **S2** are independent and can start in parallel
- **S5** is independent and can start in parallel with S1/S2
- **S3** depends on S1 (uses `NeighborhoodSliceV1` types)
- **S4** depends on S1 + S2 + S3
- **S6** depends on S1 + S4 + S5
- **S7** depends on S4 + S6

## Recommended Implementation Order

1. **Week 1**: S1 (Schema) + S2 (Rust LSP) + S5 (LDMD) — all independent
2. **Week 2**: S3 (Structural Collector)
3. **Week 3**: S4 (Bundle Builder)
4. **Week 4**: S6 (Renderer) + S7 (CLI)

## Cross-Cutting Concerns

### Fail-Open Discipline
Every LSP integration point must follow the existing pattern from `pyrefly_lsp.py`:
- `try/except Exception: return None` at the outermost boundary
- No exceptions escape to search/query/run pipelines
- Missing LSP data → structural-only results (never empty/error)

### msgspec Contract Stability
All new `CqStruct` types must follow schema evolution rules from `core/schema.py`:
- New fields must have defaults
- For array-like structs, append fields only; never reorder
- Do not change existing field types; add new fields instead

### Position Encoding
Both LSP clients must negotiate and persist position encoding:
- Request `utf-8` preference, accept `utf-16` fallback
- Store encoding in bundle `meta.lsp_position_encoding`
- Convert to byte spans when joining with CQ's byte-span-canonical model

### Testing Strategy
- **Contract tests** (`tests/msgspec_contract/`): roundtrip encode/decode for all new structs
- **Unit tests** (`tests/unit/cq/`): per-module function-level tests
- **Integration tests** (`tests/integration/`): full pipeline with mock LSP responses
- No real LSP server required for unit/integration tests — mock at `_collect_responses` boundary
