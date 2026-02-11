# Data Models and Contract Architecture

## Overview

CQ's data model architecture enforces a strict boundary between serialized cross-module contracts and runtime-only objects. This document provides a comprehensive reference for the contract types, their relationships, and the architectural principles governing their design.

**Target audience:** Advanced LLM programmers proposing architectural improvements to the CQ subsystem.

## Design Philosophy

### Serialization Boundary Protocol

CQ enforces a three-tier type system:

1. **Serialized Contracts** - msgspec.Struct subclasses that cross module boundaries
2. **Runtime-Only Objects** - dataclass or plain classes for in-process state
3. **External Handles** - Parser/cache objects that are never serialized

The boundary is explicit and enforced through type annotations and code organization:

```python
# Serialized contract (crosses module boundaries)
class Finding(msgspec.Struct):
    category: str
    message: str
    anchor: Anchor | None = None

# Runtime-only state (same process only)
@dataclass
class ScanContext:
    root: Path
    query: Query
    limits: SearchLimits
```

### Base Contract: CqStruct

All serialized contracts inherit from `CqStruct` (defined in `tools/cq/core/structs.py`):

```python
class CqStruct(msgspec.Struct, kw_only=True, frozen=True, omit_defaults=True):
    """Base class for all CQ serialized contracts."""
```

This provides:
- **kw_only=True** - Prevents positional argument confusion
- **frozen=True** - Immutability for thread safety and caching
- **omit_defaults=True** - Compact JSON output, explicit changes only

**Architectural Rationale:** msgspec provides 10-50x faster serialization than Pydantic while enforcing stricter contracts. The frozen/kw_only defaults prevent entire classes of bugs (mutation, argument ordering) at compile time.

### Pydantic Exclusion Policy

Pydantic is explicitly avoided in CQ hot paths (`tools/cq/search`, `tools/cq/query`, `tools/cq/run`) due to:
- Slower serialization performance (10-50x slower than msgspec)
- Heavier runtime overhead (validation, model construction)
- Implicit coercion behavior that masks contract violations

Pydantic remains acceptable for:
- CLI configuration parsing (not performance-critical)
- External API boundaries where implicit validation is desired

## Core Schema Models

Location: `tools/cq/core/schema.py`

### ScoreDetails

Structured scoring metadata for confidence and impact analysis:

```python
class ScoreDetails(msgspec.Struct, omit_defaults=True):
    impact_score: float | None = None
    impact_bucket: str | None = None      # "high" | "med" | "low"
    confidence_score: float | None = None
    confidence_bucket: str | None = None   # "high" | "med" | "low"
    evidence_kind: str | None = None       # Evidence classification
```

**Design Notes:**
- Separate numeric scores and categorical buckets for dual-mode consumption
- Optional fields allow partial scoring (e.g., impact without confidence)
- evidence_kind provides provenance for confidence assessment

### DetailPayload

Flexible details container with dict-like protocol:

```python
class DetailPayload(msgspec.Struct, omit_defaults=True):
    kind: str | None = None
    score: ScoreDetails | None = None
    data: dict[str, object] = msgspec.field(default_factory=dict)

    # Dict-like protocol
    def __getitem__(self, key: str) -> object: ...
    def __setitem__(self, key: str, value: object) -> None: ...
    def __contains__(self, key: str) -> bool: ...
    def get(self, key: str, default: object = None) -> object: ...

    # Legacy compatibility
    @staticmethod
    def from_legacy(d: dict[str, object] | DetailPayload) -> DetailPayload: ...

    def to_legacy_dict(self) -> dict[str, object]: ...
```

**Architectural Observations for Improvement Proposals:**

The DetailPayload design reveals a tension between structured contracts and flexible extension:

1. **Untyped data field** - The `data: dict[str, object]` field is an escape hatch for unstructured metadata. This enables rapid prototyping but sacrifices type safety and contract clarity.

2. **Dict-like protocol** - Implementing `__getitem__`/`__setitem__` creates two conflicting APIs: attribute access (`payload.score`) and key access (`payload["score"]`). This increases cognitive load and potential for bugs.

3. **Legacy compatibility** - The `from_legacy()`/`to_legacy_dict()` methods exist to bridge old dict-based details to structured contracts. This adds complexity and maintenance burden.

**Improvement Vectors:**
- **Typed detail variants** - Replace `data: dict[str, object]` with discriminated unions of known detail types
- **Remove dict protocol** - Eliminate `__getitem__`/`__setitem__` to enforce attribute-only access
- **Deprecation timeline** - Set a date to remove legacy compatibility methods

### Anchor

Source code location reference:

```python
class Anchor(msgspec.Struct, frozen=True, omit_defaults=True):
    file: str
    line: Annotated[int, msgspec.Meta(ge=1)]   # 1-indexed
    col: int | None = None                       # 0-indexed
    end_line: int | None = None
    end_col: int | None = None

    def to_ref(self) -> str:
        """Format as 'file:line'."""

    @staticmethod
    def from_span(span: SourceSpan) -> Anchor:
        """Convert SourceSpan to Anchor."""
```

**Design Notes:**
- Lines are 1-indexed (human convention)
- Columns are 0-indexed (LSP convention)
- Optional end_line/end_col support range spans
- Separate from SourceSpan (which includes end position guarantees)

**Architectural Observations for Improvement Proposals:**

The Anchor/SourceSpan split creates ambiguity about which type to use:

1. **Overlapping concerns** - Both represent source locations, but Anchor is "optional end" while SourceSpan requires end positions
2. **Conversion friction** - `from_span()` exists but there's no reverse `to_span()` (because Anchor may lack end positions)
3. **Indexing mismatch** - Anchor.col is 0-indexed, but Anchor.line is 1-indexed. This mixed convention is error-prone.

**Improvement Vectors:**
- **Consolidate types** - Merge Anchor into SourceSpan with optional end positions
- **Consistent indexing** - Either 0-index everything (internal) or 1-index everything (external)
- **Factory methods** - Add `SourceSpan.from_line_only()` to make partial spans explicit

### Finding

Individual code analysis result:

```python
class Finding(msgspec.Struct):
    category: str      # "call_site", "import", "definition", etc.
    message: str
    anchor: Anchor | None = None
    severity: Literal["info", "warning", "error"] = "info"
    details: DetailPayload = msgspec.field(default_factory=DetailPayload)

    def __post_init__(self) -> None:
        """Normalize legacy dict details to DetailPayload."""
```

**Design Notes:**
- category is free-form string (not enum) for extensibility
- severity uses Literal for LSP-style levels
- __post_init__ provides legacy compatibility by normalizing dict to DetailPayload

**Category Taxonomy (Observed in Practice):**
- `call_site` / `callsite` - Function invocation
- `import` - Import statement
- `definition` - Symbol definition
- `reference` - Variable/function reference
- `comment` - Comment mention
- `string` - String literal match

**Architectural Observations for Improvement Proposals:**

Finding's category system is underspecified:

1. **No enum enforcement** - category is `str`, not `Literal["call_site", "import", ...]`. This allows typos and inconsistency.
2. **Variant spelling** - Both "call_site" and "callsite" exist in practice. No canonical form.
3. **Overlapping semantics** - "reference" is ambiguous: does it include call_site? Import?

**Improvement Vectors:**
- **Category enum** - Define `FindingCategory = Literal["call_site", "import", ...]` and enforce at type level
- **Canonical spelling** - Choose one spelling ("call_site" recommended per LSP) and migrate all usages
- **Hierarchical categories** - Model category as structured type with primary/secondary classification

### Section

Grouped findings with collapse control:

```python
class Section(msgspec.Struct):
    title: str
    findings: list[Finding] = msgspec.field(default_factory=list)
    collapsed: bool = False
```

**Design Notes:**
- collapsed flag controls default rendering (CLI/UI)
- No ordering or priority field (implicit in list order)

### Artifact

External output reference:

```python
class Artifact(msgspec.Struct):
    path: str
    format: str = "json"
```

**Design Notes:**
- path is relative to output directory (not absolute)
- format is free-form string (common: "json", "msgpack", "dot", "mermaid")

### RunMeta

Execution metadata and provenance:

```python
class RunMeta(msgspec.Struct):
    macro: str                                    # Command name (e.g., "search")
    argv: list[str]                               # Full argv for reproducibility
    root: str                                     # Repository root
    started_ms: float                             # Unix timestamp (ms)
    elapsed_ms: float                             # Duration (ms)
    toolchain: dict[str, str | None] = msgspec.field(default_factory=dict)
    schema_version: str = SCHEMA_VERSION          # "1.0.0"
    run_id: str | None = None                     # UUID7 for correlation
```

**Design Notes:**
- started_ms/elapsed_ms use float for sub-millisecond precision
- toolchain captures tool versions (e.g., {"ast-grep": "0.30.0", "rg": "14.1.1"})
- schema_version enables forward/backward compatibility tracking

### CqResult

Top-level result envelope:

```python
class CqResult(msgspec.Struct):
    run: RunMeta
    summary: dict[str, object] = msgspec.field(default_factory=dict)
    key_findings: list[Finding] = msgspec.field(default_factory=list)
    evidence: list[Finding] = msgspec.field(default_factory=list)
    sections: list[Section] = msgspec.field(default_factory=list)
    artifacts: list[Artifact] = msgspec.field(default_factory=list)
```

**Priority Ordering:**
1. key_findings - High-priority results (shown first)
2. sections - Grouped results (main content)
3. evidence - Supporting findings (lower priority)

### CliTextResult

Text payload for protocol commands (LDMD, admin):

```python
class CliTextResult(CqStruct, frozen=True):
    text: str
    media_type: str = "text/plain"
```

**Design Notes:**
- Used by protocol commands that return raw text instead of CqResult
- Examples: LDMD progressive disclosure, admin commands, help text
- media_type supports content negotiation (text/plain, text/markdown, application/json)

**Usage Pattern:**
```python
# LDMD index command returns text table
result = CliTextResult(
    text="| ID | Title | Depth |\n|----+-------+-------|\n| R1 | Overview | 1 |",
    media_type="text/plain"
)

# Admin version command returns JSON text
result = CliTextResult(
    text='{"version": "0.5.0", "schema": "1.0.0"}',
    media_type="application/json"
)
```

**Architectural Notes:**

CliTextResult exists as an escape hatch for commands that don't fit the CqResult structure (which assumes findings/sections/artifacts). This creates a dual result type system:

1. **CqResult** - Structured analysis results with findings, sections, artifacts
2. **CliTextResult** - Raw text for protocol/admin commands

This split is pragmatic but creates ambiguity:
- Some consumers expect CqResult, others CliTextResult
- No unified Result[T] envelope to discriminate result types
- media_type is free-form string (not Literal enum)

**Improvement Vectors:**
- **Unified envelope** - Create `CommandResult = CqResult | CliTextResult | ErrorResult` discriminated union
- **Media type enum** - Replace `str` with `MediaType = Literal["text/plain", "text/markdown", "application/json", ...]`
- **Structured text results** - For JSON/markdown text, consider embedding parsed structure instead of raw text

**Architectural Observations for Improvement Proposals:**

CqResult's structure conflates presentation and data:

1. **Overlapping containers** - key_findings, sections, and evidence all hold Finding objects. The semantic difference is presentation priority, not data type.
2. **Untyped summary** - `summary: dict[str, object]` is an escape hatch for structured metadata. No contract enforcement.
3. **Flat artifact list** - No grouping or categorization of artifacts (e.g., by output format or purpose).

**Improvement Vectors:**
- **Discriminated finding groups** - Replace separate lists with single `findings: list[FindingGroup]` where group has priority/presentation metadata
- **Typed summary** - Replace `dict[str, object]` with structured SummaryBlock types
- **Artifact taxonomy** - Group artifacts by purpose (diagnostics, visualizations, data exports)

## Location Models

Location: `tools/cq/core/locations.py`

### SourceSpan

Byte-offset and line/column based source location:

```python
class SourceSpan(CqStruct, frozen=True):
    file: str
    start_line: Annotated[int, msgspec.Meta(ge=1)]   # 1-indexed
    start_col: Annotated[int, msgspec.Meta(ge=0)]     # 0-indexed
    end_line: int | None = None
    end_col: int | None = None

    # Backward compatibility aliases
    @property
    def line(self) -> int:
        return self.start_line

    @property
    def col(self) -> int:
        return self.start_col

    # Conversion helpers
    @staticmethod
    def byte_col_to_char_col(line_bytes: bytes, byte_col: int) -> int:
        """Convert byte offset to character offset."""

    @staticmethod
    def byte_offset_to_line_col(source_bytes: bytes, byte_offset: int) -> tuple[int, int]:
        """Convert absolute byte offset to (line, col)."""

    @staticmethod
    def line_relative_byte_range_to_absolute(
        source_bytes: bytes,
        line: int,
        start_byte: int,
        end_byte: int
    ) -> tuple[int, int]:
        """Convert line-relative byte range to absolute byte offsets."""
```

**Design Notes:**
- start_line uses msgspec.Meta(ge=1) for compile-time validation
- Backward compat properties (line, col) bridge old API
- Conversion helpers handle byte/char offset mapping

**Architectural Observations for Improvement Proposals:**

SourceSpan has accumulated complexity from coordinate system conversions:

1. **Mixed indexing** - start_line is 1-indexed, start_col is 0-indexed. This is LSP convention but error-prone in practice.
2. **Optional end positions** - end_line/end_col are optional, but many operations require them. Forces defensive None checks throughout codebase.
3. **Byte vs char** - The byte_col_to_char_col() helper exists because some parsers produce byte offsets, others character offsets. This dual representation is a source of bugs.

**Improvement Vectors:**
- **Single indexing convention** - Use 0-indexing for all offsets (lines and columns) internally. Convert to 1-indexed only at display boundaries.
- **Required end positions** - Make end_line/end_col required fields. Use factory methods (e.g., `from_point()`) for point locations.
- **Canonical offset type** - Choose byte offsets as canonical. All parsers must normalize to bytes. Add conversion helpers at parser boundaries only.

## Enrichment Fact Models

Location: `tools/cq/core/enrichment_facts.py`

CQ's enrichment system structures metadata into six semantic clusters with 50+ fields.

### Fact Cluster System

#### 1. Identity & Grounding (8 fields)

**Purpose:** Symbol identity, binding, and definition targeting.

```python
# Specification (not shown in full, representative fields)
FactClusterSpec(
    title="Identity & Grounding",
    fields=(
        FactFieldSpec(label="Language", paths=(("lang",),)),
        FactFieldSpec(label="Symbol Role", paths=(("role",),)),
        FactFieldSpec(label="Qualified Name", paths=(("qname",), ("full_name",))),
        FactFieldSpec(label="Binding Candidates", paths=(("bindings",),)),
        FactFieldSpec(label="Definition Target", paths=(("def_target",), ("definition",))),
        FactFieldSpec(label="Declaration Target", paths=(("decl_target",), ("declaration",))),
        FactFieldSpec(label="Type Definition Target", paths=(("typedef_target",), ("type_definition",))),
        FactFieldSpec(label="Implementation Target", paths=(("impl_target",), ("implementation",))),
    )
)
```

**Design Notes:**
- Multiple path fallbacks for cross-tool compatibility (e.g., "qname" vs "full_name")
- "Target" suffix indicates jump-to-definition locations
- Binding candidates represent multiple possible definitions (e.g., overloads)

#### 2. Type Contract (8 fields)

**Purpose:** Type signatures, parameters, return types, generic constraints.

**Representative Fields:**
- Signature (full type signature string)
- Resolved Type (canonical type name)
- Parameters (parameter list with types)
- Return Type
- Generic Params (type parameters)
- Async (async/sync indicator)
- Generator (generator/normal function)
- Visibility (public/private/protected)

**Language Applicability:**
- Python: All fields applicable
- Rust: All fields applicable

#### 3. Call Graph (2 fields, Python-only)

**Purpose:** Caller/callee relationships for impact analysis.

**Fields:**
- Incoming Callers (list of functions calling this symbol)
- Outgoing Callees (list of functions called by this symbol)

**Design Notes:**
- Python-only due to Pyrefly integration
- Used for transitive impact analysis
- Optional field (not all enrichers provide call graph)

#### 4. Class/Method Context (6 fields)

**Purpose:** Object-oriented structure and inheritance.

**Representative Fields:**
- Enclosing Class
- Base Classes (inheritance chain)
- Overridden Methods
- Overriding Methods
- Struct Fields (Rust-specific)
- Enum Variants (Rust-specific)

**Language Applicability:**
- Python: Class/method/inheritance fields
- Rust: Struct/enum/trait impl fields

#### 5. Local Scope Context (5 fields, Python-only)

**Purpose:** Intra-function symbol resolution and control flow.

**Fields:**
- Enclosing Callable (containing function/method)
- Same-Scope Symbols (sibling symbols in same scope)
- Nearest Assignments (recent assignments to this symbol)
- Narrowing Hints (type narrowing from conditionals)
- Reference Locations (all uses of this symbol)

**Design Notes:**
- Python-only (Pyrefly-specific)
- Supports local refactoring and variable tracking
- Narrowing hints aid type inference

#### 6. Imports/Aliases (2 fields)

**Purpose:** Import resolution and aliasing chains.

**Fields:**
- Import Alias Chain (sequence of aliases from import to use)
- Resolved Import Path (canonical module path)

**Design Notes:**
- Cross-language (Python and Rust have different import systems)
- Alias chain helps understand indirect imports

### Specification Types

```python
class FactFieldSpec(CqStruct, frozen=True):
    label: str                                      # Human-readable field name
    paths: tuple[tuple[str, ...], ...]              # Multi-level key paths
    applicable_languages: frozenset[str] | None = None
    applicable_kinds: frozenset[str] | None = None
    fallback_reason: NAReason = "not_resolved"      # Default N/A reason
```

**Design Notes:**
- paths is tuple of tuples for multiple fallback keys (e.g., ("qname",), ("full_name",))
- applicable_languages filters fields by language (e.g., frozenset({"python"}))
- applicable_kinds filters by symbol kind (e.g., frozenset({"function", "method"}))

```python
class FactClusterSpec(CqStruct, frozen=True):
    title: str
    fields: tuple[FactFieldSpec, ...]
```

```python
class ResolvedFact(CqStruct, frozen=True):
    label: str
    value: object | None
    reason: NAReason | None = None
```

**NAReason values:**
- "not_applicable" - Field doesn't apply to this symbol (e.g., "Return Type" for a class)
- "not_resolved" - Enricher couldn't determine value
- "enrichment_unavailable" - No enricher available for this language/context

```python
class ResolvedFactCluster(CqStruct, frozen=True):
    title: str
    rows: tuple[ResolvedFact, ...]
```

**Architectural Observations for Improvement Proposals:**

The fact cluster system has several design tensions:

1. **Path fallbacks** - The tuple-of-tuples paths design allows graceful degradation across tools, but creates ambiguity about which key is canonical. No provenance tracking for which path was used.

2. **Language/kind filtering** - applicable_languages and applicable_kinds provide compile-time filtering, but many fields have implicit language constraints not captured in the spec (e.g., "Narrowing Hints" is Python-only but not marked as such in all cases).

3. **Flat value type** - ResolvedFact.value is `object | None`, losing type information. Downstream code must know the expected type for each label.

4. **Reason overloading** - NAReason conflates three distinct states: filter mismatch (not_applicable), lookup failure (not_resolved), and missing tooling (enrichment_unavailable). These have different remediation strategies.

**Improvement Vectors:**
- **Provenance tracking** - Add `resolved_path: tuple[str, ...]` to ResolvedFact to record which fallback key succeeded
- **Typed values** - Replace `value: object | None` with discriminated union of known types (StrValue, ListValue, LocationValue, etc.)
- **Split reason enum** - Separate FilterReason, ResolutionFailure, and ToolingGap into distinct enums with different fields
- **Static validation** - Generate TypedDict types from FactFieldSpec for static type checking of enrichment payloads

## Search Contracts

Location: `tools/cq/search/contracts.py`

### LanguagePartitionStats

Per-language search result statistics:

```python
class LanguagePartitionStats(CqStruct, frozen=True):
    matches: int                    # Total candidate matches
    files_scanned: int              # Files examined
    matched_files: int              # Files with at least one match
    total_matches: int              # Post-classification match count
    truncated: bool = False         # Hit match limit
    timed_out: bool = False         # Hit timeout
    caps_hit: frozenset[str] = frozenset()  # Which limits were hit
    error: str | None = None        # Fatal error message
```

**Design Notes:**
- matches is pre-classification (raw rg output)
- total_matches is post-classification (after filtering)
- caps_hit tracks which specific limits were exceeded (e.g., {"max_files", "max_matches_per_file"})

### EnrichmentTelemetry

Per-language enrichment pipeline metrics:

```python
class EnrichmentTelemetry(CqStruct, frozen=True):
    applied: int = 0                # Successfully enriched
    degraded: int = 0               # Partial enrichment
    skipped: int = 0                # Not enriched
    cache_hits: int = 0
    cache_misses: int = 0
    cache_evictions: int = 0
```

**Design Notes:**
- applied + degraded + skipped = total candidates
- degraded indicates enrichment succeeded but with missing fields
- Cache metrics support performance tuning

### PyreflyOverview

Pyrefly-specific enrichment summary:

```python
class PyreflyOverview(CqStruct, frozen=True):
    primary_symbol: str | None = None
    enclosing_class: str | None = None
    callers_count: int = 0
    callees_count: int = 0
    implementations_count: int = 0
    diagnostics: tuple[str, ...] = ()
```

**Design Notes:**
- Top-level summary for quick symbol understanding
- Counts support impact estimation
- diagnostics captures Pyrefly errors/warnings

### PyreflyTelemetry

Pyrefly execution metrics:

```python
class PyreflyTelemetry(CqStruct, frozen=True):
    queries: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    errors: int = 0
    duration_ms: float = 0.0
```

### SearchSummaryContract

Canonical multi-language search summary:

```python
class SearchSummaryContract(CqStruct, frozen=True):
    lang_scope: str                                     # "auto" | "python" | "rust"
    language_order: tuple[str, ...]                     # Execution order
    languages: dict[str, LanguagePartitionStats]        # Per-language stats
    cross_language_diagnostics: tuple[str, ...] = ()    # Cross-language warnings
    language_capabilities: dict[str, object] = msgspec.field(default_factory=dict)
    enrichment_telemetry: dict[str, EnrichmentTelemetry] = msgspec.field(default_factory=dict)
```

**Design Notes:**
- language_order preserves execution sequence (important for understanding priority)
- cross_language_diagnostics reports conflicts (e.g., same symbol defined in Python and Rust)
- language_capabilities documents which features are available per language

**Architectural Observations for Improvement Proposals:**

SearchSummaryContract has grown complex through feature accretion:

1. **Dict-based partitioning** - languages, language_capabilities, and enrichment_telemetry all use dict[str, ...] where str is language name. This is untyped and error-prone (typos, missing entries).

2. **Overlapping metrics** - LanguagePartitionStats and EnrichmentTelemetry both track per-language data but are in separate dicts. No enforcement that they cover the same languages.

3. **Untyped capabilities** - language_capabilities is `dict[str, object]` with no contract. Different languages may report incompatible data structures.

**Improvement Vectors:**
- **Typed language partition** - Create LanguagePartition(stats, enrichment, capabilities) and use dict[str, LanguagePartition]
- **Enum for languages** - Define SupportedLanguage = Literal["python", "rust"] to prevent typos
- **Capability protocol** - Define LanguageCapabilities(enrichers: set[str], features: set[str]) as structured type

## Semantic Neighborhood Bundle Schema

Location: `tools/cq/core/snb_schema.py`

The Semantic Neighborhood Bundle (SNB) schema provides a versioned, deterministic representation of semantic neighborhoods combining structural, LSP, and enrichment planes. All SNB types inherit from `CqStruct` for deterministic serialization.

Cross-references: See `08_neighborhood_subsystem.md` for operational details and `09_ldmd_format.md` for progressive disclosure mechanisms.

### SemanticNeighborhoodBundleV1

The canonical bundle type containing a complete semantic neighborhood:

```python
class SemanticNeighborhoodBundleV1(CqStruct, frozen=True):
    bundle_id: str
    subject: SemanticNodeRefV1 | None = None
    subject_label: str = ""
    meta: BundleMetaV1 | None = None
    slices: tuple[NeighborhoodSliceV1, ...] = ()
    graph: NeighborhoodGraphSummaryV1 | None = None
    node_index: dict[str, SemanticNodeRefV1] | None = None
    artifacts: tuple[ArtifactPointerV1, ...] = ()
    diagnostics: tuple[DegradeEventV1, ...] = ()
    schema_version: str = "cq.snb.v1"
```

**Design Notes:**
- bundle_id provides unique identity for caching and reference
- subject is the focal node (None for multi-subject bundles)
- slices contain relationship-typed neighborhoods (callers, callees, etc.)
- node_index provides O(1) node lookup by ID
- diagnostics track degradation events across enrichment pipeline
- artifacts point to related outputs (graphs, detailed reports)

**Bundle Lifecycle:**
1. Anchor resolution → subject node identification
2. Structural slice assembly (tree-sitter, AST)
3. LSP enrichment (rust-analyzer, Pyrefly)
4. Graph summary computation
5. Serialization to LDMD or JSON

### SemanticNodeRefV1

Minimal node reference for slice membership:

```python
class SemanticNodeRefV1(CqStruct, frozen=True):
    node_id: str
    kind: str                                    # "function", "class", "import", etc.
    name: str
    display_label: str = ""
    file_path: str = ""
    byte_span: tuple[int, int] | None = None    # (bstart, bend)
    signature: str | None = None
    qualname: str | None = None
```

**Design Notes:**
- node_id is unique within bundle (often file_path:byte_span)
- kind follows LSP SymbolKind conventions
- byte_span is canonical location (not line/col)
- Full node details live in node_index, slices hold lightweight refs

### NeighborhoodSliceV1

A single relationship-typed slice with progressive disclosure:

```python
NeighborhoodSliceKind = Literal[
    "callers",
    "callees",
    "references",
    "implementations",
    "type_supertypes",
    "type_subtypes",
    "parents",
    "children",
    "siblings",
    "enclosing_context",
    "imports",
    "importers",
    "related",
]

class NeighborhoodSliceV1(CqStruct, frozen=True):
    kind: NeighborhoodSliceKind
    title: str
    total: int = 0
    preview: tuple[SemanticNodeRefV1, ...] = ()
    edges: tuple[SemanticEdgeV1, ...] = ()
    collapsed: bool = True
    metadata: dict[str, object] | None = None
```

**Design Notes:**
- kind discriminates relationship type (13 values)
- total tracks full count for pagination/UI
- preview contains first N nodes (progressive disclosure)
- edges connect subject to slice members
- collapsed controls default UI presentation

**Slice Kind Semantics:**
- **callers/callees** - Function call relationships
- **references** - Symbol usage sites
- **implementations** - Interface/trait implementations
- **type_supertypes/type_subtypes** - Type hierarchy (superclasses/subclasses)
- **parents/children** - AST containment hierarchy
- **siblings** - Same-scope symbols
- **enclosing_context** - Containing functions/classes
- **imports/importers** - Module dependency relationships
- **related** - Catch-all for other semantic relationships

### SemanticEdgeV1

Directed edge with evidence provenance:

```python
class SemanticEdgeV1(CqStruct, frozen=True):
    edge_id: str
    source_node_id: str
    target_node_id: str
    edge_kind: str                               # "calls", "imports", "extends", etc.
    weight: float = 1.0
    evidence_source: str = ""                    # "ast", "lsp.rust", "bytecode"
    metadata: dict[str, object] | None = None
```

**Design Notes:**
- edge_id enables deduplication and reference
- weight supports ranking (e.g., call frequency)
- evidence_source tracks which enricher produced edge
- metadata holds enricher-specific details

### DegradeEventV1

Typed degradation event for failure tracking:

```python
class DegradeEventV1(CqStruct, frozen=True):
    stage: str                                   # "lsp.rust", "structural.interval_index"
    severity: Literal["info", "warning", "error"] = "warning"
    category: str = ""                           # "timeout", "unavailable", "parse_error"
    message: str = ""
    correlation_key: str | None = None
```

**Design Notes:**
- stage identifies which enrichment plane failed
- category enables programmatic filtering (group all timeouts, etc.)
- correlation_key groups related events (e.g., same file parse errors)

**Common Categories:**
- "timeout" - LSP server timeout
- "unavailable" - Tool/server not running
- "parse_error" - Syntax error preventing analysis
- "capability_missing" - Feature not supported by server

### BundleMetaV1

Bundle creation metadata and provenance:

```python
class BundleMetaV1(CqStruct, frozen=True):
    tool: str = "cq"
    tool_version: str | None = None
    workspace_root: str | None = None
    query_text: str | None = None
    created_at_ms: float | None = None
    lsp_servers: tuple[dict[str, object], ...] = ()
    limits: dict[str, int] | None = None
```

**Design Notes:**
- tool_version enables compatibility checks
- query_text supports reproducibility
- lsp_servers documents which servers contributed enrichment
- limits records query constraints (max depth, timeout, etc.)

### NeighborhoodGraphSummaryV1

Lightweight graph summary:

```python
class NeighborhoodGraphSummaryV1(CqStruct, frozen=True):
    node_count: int = 0
    edge_count: int = 0
    full_graph_artifact: ArtifactPointerV1 | None = None
```

**Design Notes:**
- Counts support UI rendering decisions
- full_graph_artifact points to detailed graph export (DOT, Mermaid)

### ArtifactPointerV1

Generic artifact reference with cache support:

```python
class ArtifactPointerV1(CqStruct, frozen=True):
    artifact_kind: str                           # "snb.bundle", "lsp.call_graph"
    artifact_id: str
    deterministic_id: str                        # SHA256 of normalized content
    byte_size: int = 0
    storage_path: str | None = None
    metadata: dict[str, object] | None = None
```

**Design Notes:**
- deterministic_id enables content-addressed caching
- storage_path supports lazy loading from disk/object store
- artifact_kind discriminates artifact types

### SemanticNeighborhoodBundleRefV1

Lightweight bundle reference for lazy loading:

```python
class SemanticNeighborhoodBundleRefV1(CqStruct, frozen=True):
    bundle_id: str
    deterministic_id: str
    byte_size: int = 0
    artifact_path: str | None = None
    preview_slices: tuple[NeighborhoodSliceKind, ...] = ()
    subject_node_id: str = ""
    subject_label: str = ""
```

**Design Notes:**
- Enables catalog of bundles without loading full payloads
- preview_slices advertises available relationships
- deterministic_id supports cache validation

**Architectural Observations for Improvement Proposals:**

The SNB schema has several design tensions:

1. **Untyped metadata** - SemanticEdgeV1.metadata, NeighborhoodSliceV1.metadata, and ArtifactPointerV1.metadata are all `dict[str, object]`, losing type safety. Different enrichers may produce incompatible structures.

2. **node_index redundancy** - node_index duplicates data from slice previews. No enforcement that node_index is complete or that slice nodes are present in index.

3. **Edge orphans** - NeighborhoodSliceV1.edges references node_ids but no validation that referenced nodes exist in node_index or preview.

4. **Kind proliferation** - NeighborhoodSliceKind has 13 values but no formal taxonomy. "related" is catch-all escape hatch.

**Improvement Vectors:**
- **Typed metadata unions** - Replace `dict[str, object]` with discriminated unions: `EdgeMetadata = AstEdgeMeta | LspEdgeMeta | BytecodeEdgeMeta`
- **Index validation** - Add post_init validation ensuring node_index contains all referenced nodes
- **Edge integrity** - Add referential integrity checks or FK constraints on node_ids
- **Slice taxonomy** - Formalize slice kind hierarchy: PrimaryRelationships (callers, callees) vs StructuralContext (parents, siblings) vs TypeRelationships (supertypes, subtypes)

## SNB Typed Registry

Location: `tools/cq/core/snb_registry.py`

The SNB registry provides runtime type resolution and validation for SNB schema types embedded in Finding details payloads.

### DETAILS_KIND_REGISTRY

Maps kind strings to struct types for dynamic deserialization:

```python
DETAILS_KIND_REGISTRY: dict[str, type[msgspec.Struct]] = {
    "cq.snb.bundle_ref.v1": SemanticNeighborhoodBundleRefV1,
    "cq.snb.bundle.v1": SemanticNeighborhoodBundleV1,
    "cq.snb.node.v1": SemanticNodeRefV1,
    "cq.snb.edge.v1": SemanticEdgeV1,
    "cq.snb.slice.v1": NeighborhoodSliceV1,
    "cq.snb.degrade.v1": DegradeEventV1,
    "cq.snb.bundle_meta.v1": BundleMetaV1,
    "cq.snb.graph_summary.v1": NeighborhoodGraphSummaryV1,
}
```

**Design Notes:**
- Kind strings follow `cq.snb.<type>.v1` convention for versioning
- Registry enables polymorphic deserialization from Finding.details
- All types are msgspec.Struct for validation at decode time

**Kind Naming Convention:**
```
cq.snb.<short_name>.<major_version>
│   │   │            │
│   │   │            └─ Schema version (v1, v2, etc.)
│   │   └────────────── Type identifier (bundle, node, edge, etc.)
│   └────────────────── Namespace (snb = Semantic Neighborhood Bundle)
└────────────────────── Tool prefix (cq)
```

### resolve_kind()

Resolve kind string to struct type:

```python
def resolve_kind(kind: str) -> type[msgspec.Struct] | None:
    """Resolve kind string to struct type for runtime validation."""
    return DETAILS_KIND_REGISTRY.get(kind)
```

**Usage:**
```python
struct_type = resolve_kind("cq.snb.node.v1")
assert struct_type is SemanticNodeRefV1
```

### validate_finding_details()

Validate finding details has registered kind:

```python
def validate_finding_details(details: dict[str, object]) -> bool:
    """Validate that finding details dict has valid kind and structure."""
    kind = details.get("kind")
    if not isinstance(kind, str):
        return False
    struct_type = resolve_kind(kind)
    return struct_type is not None
```

**Usage:**
```python
details = {"kind": "cq.snb.node.v1", "node_id": "...", "name": "foo", ...}
if validate_finding_details(details):
    # Safe to decode
    node = decode_finding_details(details)
```

**Design Notes:**
- Fail-fast validation before expensive decoding
- Used by enrichment pipeline to catch malformed payloads early
- Returns bool (not exceptions) for graceful degradation

### decode_finding_details()

Decode finding details dict into typed struct:

```python
def decode_finding_details(details: dict[str, object]) -> msgspec.Struct | None:
    """Decode finding details dict into typed struct."""
    kind = details.get("kind")
    if not isinstance(kind, str):
        return None

    struct_type = resolve_kind(kind)
    if struct_type is None:
        return None

    try:
        return msgspec.convert(details, type=struct_type)
    except (msgspec.ValidationError, TypeError):
        return None
```

**Usage Pattern:**
```python
# Finding with SNB node in details
finding = Finding(
    category="definition",
    message="Function foo",
    details=DetailPayload(data={
        "kind": "cq.snb.node.v1",
        "node_id": "src/foo.py:100-120",
        "name": "foo",
        "kind": "function",
        ...
    })
)

# Decode to typed struct
details_dict = finding.details.data
if validate_finding_details(details_dict):
    node = decode_finding_details(details_dict)
    assert isinstance(node, SemanticNodeRefV1)
    print(f"Function: {node.name} at {node.file_path}")
```

**Design Notes:**
- Returns None on failure (not exceptions) for fail-open behavior
- Uses msgspec.convert() for validation + type coercion
- Validation errors are silent (logged internally if needed)

**Integration with Finding Details:**

The registry enables polymorphic details payloads in Finding:

```python
# Before: Untyped details
finding = Finding(
    category="neighborhood",
    message="Found 42 callers",
    details=DetailPayload(data={"callers": [...], "callees": [...]})
)

# After: Typed SNB bundle in details
finding = Finding(
    category="neighborhood",
    message="Semantic neighborhood for foo",
    details=DetailPayload(data={
        "kind": "cq.snb.bundle.v1",
        "bundle_id": "...",
        "subject": {...},
        "slices": [...],
        ...
    })
)

# Consumer can decode and validate
if validate_finding_details(finding.details.data):
    bundle = decode_finding_details(finding.details.data)
    print(f"Bundle has {len(bundle.slices)} slices")
```

**Architectural Observations for Improvement Proposals:**

The typed registry pattern addresses some DetailPayload issues but has limitations:

1. **Silent failures** - decode_finding_details() returns None on validation errors with no diagnostic context. Consumer doesn't know why decoding failed.

2. **No schema validation** - Registry validates kind exists but doesn't validate struct completeness. Missing required fields fail at decode time, not registration time.

3. **Global registry** - DETAILS_KIND_REGISTRY is module-level dict with no extension mechanism. Adding new kinds requires modifying core module.

**Improvement Vectors:**
- **Error detail** - Return `Result[T, ValidationError]` instead of `T | None` to preserve error context
- **Pre-validation** - Add JSON Schema validation before msgspec conversion to catch structure errors early
- **Plugin registry** - Replace global dict with PluginRegistry that supports runtime registration from external modules
- **Kind discovery** - Add `list_registered_kinds() -> list[str]` for introspection and testing

## Search Request Models

Location: `tools/cq/search/`

### RgRunRequest (requests.py)

Request for ripgrep execution:

```python
class RgRunRequest(CqStruct, frozen=True):
    root: str
    pattern: str
    mode: SearchMode                        # "identifier" | "regex" | "literal"
    lang_types: tuple[str, ...]             # rg --type values
    limits: SearchLimits
    include_globs: tuple[str, ...] = ()
    exclude_globs: tuple[str, ...] = ()
```

**Design Notes:**
- lang_types maps to rg's --type flag (e.g., "python", "rust")
- Globs support path filtering beyond language types

### CandidateCollectionRequest (requests.py)

Request for candidate collection (post-rg, pre-enrichment):

```python
class CandidateCollectionRequest(CqStruct, frozen=True):
    root: str
    pattern: str
    mode: SearchMode
    limits: SearchLimits
    lang: str                               # Single language (not multi-language)
    globs: tuple[str, ...]
```

**Design Notes:**
- lang is single language (CandidateCollectionRequest is per-language)
- globs are resolved includes+excludes from RgRunRequest

### PythonNodeEnrichmentRequest (requests.py)

Request for Python AST node enrichment:

```python
class PythonNodeEnrichmentRequest(CqStruct, frozen=True):
    sg_root: str                            # tree-sitter grammar root
    node: str                               # tree-sitter node type
    source_bytes: bytes
    line: int                               # 1-indexed
    col: int                                # 0-indexed
    cache_key: str
    byte_start: int | None = None
    byte_end: int | None = None
    session: str | None = None              # Pyrefly session ID
```

**Design Notes:**
- node is tree-sitter node kind (e.g., "identifier", "call")
- byte_start/byte_end override line/col for byte-offset enrichers
- session enables Pyrefly caching across requests

### PythonByteRangeEnrichmentRequest (requests.py)

Request for Python byte-range enrichment (alternative to node-based):

```python
class PythonByteRangeEnrichmentRequest(CqStruct, frozen=True):
    sg_root: str
    source_bytes: bytes
    byte_start: int
    byte_end: int
    cache_key: str
    resolved_node: str | None = None        # Hint: tree-sitter node type
    resolved_line: int | None = None        # Hint: 1-indexed line
    resolved_col: int | None = None         # Hint: 0-indexed col
    session: str | None = None
```

**Design Notes:**
- Byte-range-first API (node/line/col are optional hints)
- Used when byte offsets are canonical (e.g., from SCIP)

### RustEnrichmentRequest (requests.py)

Request for Rust enrichment:

```python
class RustEnrichmentRequest(CqStruct, frozen=True):
    source: str                             # File path or source code
    byte_start: int
    byte_end: int
    cache_key: str
    max_scope_depth: int = 5
```

**Design Notes:**
- Rust enrichment is byte-range-only (no line/col API)
- max_scope_depth limits scope chain traversal

### SearchConfig (models.py)

Resolved execution configuration:

```python
class SearchConfig(CqStruct, frozen=True):
    root: Path
    query: str
    mode: SearchMode
    limits: SearchLimits
    lang_scope: QueryLanguageScope          # "auto" | "python" | "rust"
    include_globs: tuple[str, ...] = ()
    exclude_globs: tuple[str, ...] = ()
    include_strings: bool = False
    include_comments: bool = False
    fallback_to_search: bool = True
```

**Design Notes:**
- Resolved from SearchRequest + config file + env vars
- include_strings/include_comments control non-code match filtering
- fallback_to_search enables query → search degradation

### SearchRequest (models.py)

User input request (pre-resolution):

```python
class SearchRequest(CqStruct, frozen=True):
    query: str
    mode: SearchMode | None = None          # None = auto-detect
    limits: SearchLimits | None = None      # None = use defaults
    lang_scope: QueryLanguageScope = "auto"
    in_path: str | None = None              # Path scope filter
    include_strings: bool = False
    include_comments: bool = False
```

**Design Notes:**
- Optional mode enables auto-detection (identifier vs regex vs literal)
- Optional limits enables profile-based defaults

### CandidateSearchRequest (models.py)

Internal candidate pipeline request:

```python
class CandidateSearchRequest(CqStruct, frozen=True):
    root: Path
    pattern: str
    mode: SearchMode
    limits: SearchLimits
    lang: str
    globs: tuple[str, ...]
```

**Design Notes:**
- Per-language request (not multi-language)
- Used internally by search executor

### Search Profiles (profiles.py)

Predefined limit configurations:

```python
class SearchLimits(CqStruct, frozen=True):
    max_files: int = 5000
    max_matches_per_file: int = 1000
    max_total_matches: int = 10000
    timeout_seconds: float = 30.0
    max_depth: int = 25
    max_file_size_bytes: int = 2097152      # 2 MB
```

**Predefined Profiles:**
- **DEFAULT** - Balanced limits (shown above)
- **INTERACTIVE** - Fast/shallow (max_files=500, timeout=5s)
- **AUDIT** - Comprehensive (max_files=50000, timeout=300s)
- **LITERAL** - String-search focused (max_matches_per_file=5000)

**Design Notes:**
- max_file_size_bytes prevents parsing large generated files
- max_depth limits directory traversal depth

**Architectural Observations for Improvement Proposals:**

The search request architecture has accumulated overlapping types:

1. **Request explosion** - Five request types (RgRunRequest, CandidateCollectionRequest, SearchConfig, SearchRequest, CandidateSearchRequest) with significant field overlap. Unclear which type is authoritative.

2. **Resolution opacity** - SearchRequest → SearchConfig resolution is implicit. No record of which defaults were applied or which config sources were used.

3. **Mode auto-detection** - SearchRequest.mode is optional (auto-detect), but CandidateCollectionRequest.mode is required. The detection logic is hidden in the executor.

**Improvement Vectors:**
- **Consolidate requests** - Merge SearchConfig and CandidateSearchRequest (they're nearly identical)
- **Resolution provenance** - Add `resolution_trace: dict[str, str]` to SearchConfig showing which value came from which source (CLI, config file, env, default)
- **Explicit mode detection** - Make mode detection a separate function that returns (detected_mode, confidence, evidence)

## Query IR Models

Location: `tools/cq/query/ir.py`

The Query struct is the central parsed representation of query commands. See `03_query_subsystem.md` for comprehensive documentation.

**Core Structure:**

```python
class Query(CqStruct, frozen=True):
    # Entity selectors
    entity: str | None = None               # "function", "class", "import", etc.
    name: str | None = None                 # Identifier or regex pattern

    # Relational constraints
    pattern: PatternSpec | None = None      # Structural pattern
    inside: str | None = None               # Enclosing context
    has: str | None = None                  # Must contain
    precedes: str | None = None             # Must come before
    follows: str | None = None              # Must come after

    # Composite logic
    all: tuple[str, ...] | None = None      # AND of patterns
    any: tuple[str, ...] | None = None      # OR of patterns
    not_: PatternSpec | None = None         # Negation

    # Scope filters
    scope: str | None = None                # "closure", "global", etc.
    in_: str | None = None                  # Path scope
    lang: QueryLanguage | None = None       # "python" | "rust"

    # Expansion
    expand: str | None = None               # "callers", "callees", "deps"

    # Output control
    format: str | None = None               # "md", "json", "mermaid", etc.
```

**Design Notes:**
- 17 fields total (most optional for orthogonal composition)
- PatternSpec is discriminated union for pattern disambiguation
- Expansion support transitive queries (e.g., "callers(depth=2)")

**Architectural Observations:**

See `03_query_subsystem.md` for detailed architectural analysis.

## Scoring Models

Location: `tools/cq/core/scoring.py`

### ImpactSignals

Raw signals for impact score calculation:

```python
class ImpactSignals(CqStruct, frozen=True):
    sites: int = 0                          # Number of call sites
    files: int = 0                          # Number of affected files
    depth: int = 0                          # Max call chain depth
    breakages: int = 0                      # Potential breaking changes
    ambiguities: int = 0                    # Ambiguous references
```

**Design Notes:**
- Raw counts (not normalized scores)
- Used by scorer to compute impact_score and impact_bucket

### ConfidenceSignals

Raw signals for confidence score calculation:

```python
class ConfidenceSignals(CqStruct, frozen=True):
    evidence_kind: str = "unresolved"       # Type of evidence backing confidence
```

**Design Notes:**
- Minimal structure (under-developed in current implementation)
- evidence_kind values (observed): "ast", "bytecode", "scip", "static_analysis", "unresolved"

**Architectural Observations for Improvement Proposals:**

The scoring model architecture is underdeveloped:

1. **Implicit scoring** - ImpactSignals → impact_score conversion is opaque. No formula documentation or tests.

2. **Sparse confidence signals** - ConfidenceSignals only has evidence_kind. Missing: agreement across tools, contradictions, staleness, coverage.

3. **No scorer interface** - Scoring logic is embedded in ad-hoc functions. No Scorer protocol or strategy pattern.

**Improvement Vectors:**
- **Document scoring formulas** - Add docstrings with formulas (e.g., `impact_score = log2(sites + 1) + 0.5 * files + 0.3 * depth`)
- **Rich confidence signals** - Add fields: tool_agreement (int), contradictions (int), last_indexed (datetime), coverage (float)
- **Scorer protocol** - Define `Scorer` protocol with `score_impact(signals: ImpactSignals) -> ScoreDetails` method

## Serialization Architecture

Location: `tools/cq/core/`

### Codec (codec.py)

Canonical encoders/decoders:

```python
JSON_ENCODER = msgspec.json.Encoder(order="deterministic")
JSON_DECODER = msgspec.json.Decoder(strict=True)
JSON_RESULT_DECODER = msgspec.json.Decoder(type=CqResult, strict=True)

MSGPACK_ENCODER = msgspec.msgpack.Encoder()
MSGPACK_DECODER = msgspec.msgpack.Decoder(type=object)
MSGPACK_RESULT_DECODER = msgspec.msgpack.Decoder(type=CqResult)
```

**Design Notes:**
- order="deterministic" ensures reproducible JSON (for git diffs, caching)
- strict=True rejects unknown fields (fails fast on schema mismatches)
- Type-specific decoders (JSON_RESULT_DECODER) provide validation at parse time

### Serialization API (serialization.py)

High-level serialization functions:

```python
def dumps_json(obj: object) -> bytes: ...
def loads_json(data: bytes) -> object: ...

def dumps_msgpack(obj: object) -> bytes: ...
def loads_msgpack(data: bytes) -> object: ...
def loads_msgpack_result(data: bytes) -> CqResult: ...

def to_builtins(obj: object) -> object: ...
```

**Design Notes:**
- All functions return/accept bytes (not str) for encoding clarity
- to_builtins() converts msgspec structs to plain dicts (for JSON serialization)

### Contract Envelope (contracts.py)

Wrapper for legacy dict-based contracts:

```python
class ContractEnvelope(msgspec.Struct):
    payload: dict[str, object]

def contract_to_builtins(contract: msgspec.Struct) -> dict[str, object]: ...
def summary_contract_to_mapping(contract: msgspec.Struct) -> dict[str, object]: ...
def require_mapping(data: object) -> dict[str, object]: ...
```

**Design Notes:**
- ContractEnvelope bridges structured contracts and untyped dict consumers
- require_mapping() validates shape and raises TypeError on mismatch

**Architectural Observations for Improvement Proposals:**

The serialization architecture has legacy compatibility debt:

1. **Dual representation** - Contracts exist as both msgspec.Struct (typed) and dict[str, object] (untyped). Conversion functions (contract_to_builtins, to_builtins) create ambiguity.

2. **Envelope overhead** - ContractEnvelope wraps payloads in unnecessary structure. It exists solely to bridge dict-based APIs.

3. **Untyped exports** - to_builtins() returns `object`, losing type information. Downstream code must know the expected structure.

**Improvement Vectors:**
- **Deprecate dict exports** - Remove contract_to_builtins() and summary_contract_to_mapping(). Force consumers to use msgspec.Struct directly.
- **Remove envelope** - Delete ContractEnvelope. If wrapping is needed, use generic Wrapper[T] with type parameter.
- **Typed exports** - Replace `to_builtins() -> object` with `to_dict[T](contract: T) -> dict[str, object]` that preserves generic type.

## Configuration

Location: `cli_app/config_types.py`

### CqConfig

Global configuration container:

```python
class CqConfig(CqStruct, frozen=True):
    root: str | None = None
    verbose: int | None = None
    output_format: str | None = None
    artifact_dir: str | None = None
    save_artifact: bool | None = None
```

**Design Notes:**
- All fields optional (for partial overrides)
- Loaded from pyproject.toml [tool.cq] or CQ_* env vars
- Resolution order: CLI flags → env vars → config file → defaults

**Architectural Observations for Improvement Proposals:**

CqConfig is underspecified:

1. **All optional** - Every field is optional, making it impossible to distinguish "user set to None" from "not configured". Resolution logic must handle this ambiguity.

2. **No provenance** - No record of where each value came from (CLI, env, file, default).

3. **String typing** - output_format and artifact_dir are `str | None`, not validated enums or Path types.

**Improvement Vectors:**
- **Sentinel values** - Use `Unset = object()` sentinel instead of None for "not configured"
- **Provenance tracking** - Add ConfigSource enum and `sources: dict[str, ConfigSource]` field
- **Typed fields** - Replace `output_format: str | None` with `output_format: OutputFormat | None` where OutputFormat is Literal enum

## Language Types

Location: `tools/cq/query/language.py`

### Language Enums

```python
QueryLanguage = Literal["python", "rust"]
QueryLanguageScope = Literal["auto", "python", "rust"]
```

**Extension Mapping:**
- python → .py, .pyi
- rust → .rs

**Scope Expansion:**
- auto → (python, rust)
- python → (python,)
- rust → (rust,)

**Design Notes:**
- Extension mapping is authoritative (not configurable)
- Scope expansion is deterministic (no heuristics)

**Architectural Observations for Improvement Proposals:**

Language support is hardcoded:

1. **Literal enums** - Adding a new language requires changing Literal type and all downstream switch statements.

2. **Extension mapping** - No registry or plugin system. Extensions are hardcoded in multiple locations.

3. **Scope expansion** - The auto → (python, rust) mapping is implicit. No way to configure language priorities or exclusions.

**Improvement Vectors:**
- **Language registry** - Define Language(name, extensions, enrichers) registry. Load from config or plugins.
- **Configurable scope** - Allow .cq.toml to specify `[language_scope] default = ["python", "rust", "typescript"]`
- **Priority hints** - Support scope="auto(priority=python)" to prefer Python when multiple languages match

## Boundary Protocol Summary

### Cross-Module (Serialized)

**Must use msgspec.Struct:**
- CqResult
- SearchSummaryContract
- CrossLanguageDiagnostic
- EnrichmentPayload
- CqConfig (when serialized)
- All *Request types (RgRunRequest, etc.)
- All *Contract types (SearchSummaryContract, etc.)

**Rationale:** These types cross module boundaries (disk, network, IPC). Serialization must be deterministic and validated.

### Runtime-Only (NOT Serialized)

**May use dataclass or plain class:**
- RunContext
- Toolchain (partially serialized, partially runtime-only)
- SearchConfig (when not persisted)
- Query (when not persisted)
- PatternSpec (when not persisted)
- ScanContext
- EntityExecutionState

**Rationale:** These types exist only within a single process lifetime. No serialization needed.

### Schema Evolution

**Backward Compatibility Strategy:**

1. **Append-only fields** - New fields must have defaults
2. **No field removal** - Deprecate fields but keep them in schema
3. **No type changes** - Create new field with new type, deprecate old field
4. **Version tracking** - RunMeta.schema_version documents contract version

**Compatibility Helpers:**

- DetailPayload.from_legacy() - Bridge dict to struct
- DetailPayload.to_legacy_dict() - Bridge struct to dict
- Anchor.from_span() - Convert SourceSpan to Anchor
- SourceSpan backward compat properties (line, col)

**Schema Version Format:** Semantic versioning (e.g., "1.0.0")

**Breaking Change Policy:**
- Major version bump for field removal or type changes
- Minor version bump for new fields
- Patch version bump for documentation or defaults

**Architectural Observations for Improvement Proposals:**

Schema evolution is manual and error-prone:

1. **No enforcement** - Schema version is tracked but not validated. Old clients can deserialize new schemas and silently ignore new fields.

2. **Compatibility helpers** - from_legacy/to_legacy methods proliferate as schema evolves. No deprecation timeline.

3. **No migration** - No automated migration from old schemas to new schemas. Clients must handle all versions.

**Improvement Vectors:**
- **Version validation** - Add MIN_SUPPORTED_VERSION and MAX_SUPPORTED_VERSION checks at deserialization
- **Deprecation tracking** - Add @deprecated decorator that logs warnings and tracks usage
- **Auto-migration** - Generate migration functions from schema diffs (e.g., rename field, add default)
- **Contract testing** - Add tests that serialize old versions and deserialize with new schema (forward compat) and vice versa (backward compat)

## Summary

CQ's data model architecture enforces strict boundaries between serialized contracts (msgspec.Struct) and runtime-only objects (dataclass/plain class). This provides:

**Strengths:**
- Type safety via msgspec validation
- Performance via msgspec's optimized codec
- Deterministic serialization for reproducibility
- Clear module boundaries

**Architectural Tensions:**
- Overlapping request types (5 search request variants)
- Untyped escape hatches (DetailPayload.data, summary dicts)
- Mixed indexing conventions (1-indexed lines, 0-indexed columns)
- Manual schema evolution with no automated migration
- Legacy compatibility creating dual APIs (dict + struct)

**Primary Improvement Vectors:**
1. Consolidate overlapping request types
2. Replace untyped dicts with discriminated unions
3. Standardize indexing conventions (0-indexed internal, 1-indexed display)
4. Add provenance tracking to configuration and resolution
5. Implement automated schema migration
6. Deprecate legacy dict-based APIs with timeline
