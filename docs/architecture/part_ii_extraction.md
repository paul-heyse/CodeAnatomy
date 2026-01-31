# Part II: Extraction Stage

## Overview

The Extraction Stage transforms Python source files into structured evidence tables through multiple parsing strategies. Each extractor captures a distinct view of code structure—AST for syntax trees, LibCST for concrete syntax with metadata, symtable for scoping and bindings, bytecode for compiled instructions and control flow, SCIP for cross-repository symbols, and tree-sitter for error-tolerant parsing. These complementary evidence layers enable rich downstream analysis.

The extraction pipeline follows a shared-nothing architecture where each file is processed independently. The `FileContext` abstraction (`src/extract/coordination/context.py:36-100`) bundles file-level state (file ID, path, content, SHA-256 hash, encoding) passed to all extractors. The `ExtractExecutionContext` (`src/extract/coordination/context.py:116-158`) provides session-wide resources (runtime profile, evidence plan, scope manifest, extract session). This separation enables parallel execution via fork-based `ProcessPoolExecutor` while maintaining per-file isolation.

A key architectural invariant is the **bytes-first design**: all coordinate systems anchor to byte offsets. The `SpanSpec` dataclass (`src/extract/coordination/context.py:160-172`) defines a universal span representation with `byte_start` and `byte_len` fields alongside optional line/column coordinates. Line/column semantics vary by parser (AST uses 1-indexed lines with byte columns, LibCST uses UTF-32 columns), but byte spans provide deterministic join keys for the normalization stage.

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         EXTRACTION PIPELINE                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                     Repository Scanner                                │   │
│  │  scan_repo() → scan_repo_blobs() / iter_worklist_contexts()          │   │
│  │                                                                       │   │
│  │  Output: repo_files_v1 (file_id, path, abs_path, file_sha256, ...)   │   │
│  └───────────────────────────┬──────────────────────────────────────────┘   │
│                                  │                                          │
│              ┌───────────────────┼───────────────────┐                      │
│              │                   │                   │                      │
│              ▼                   ▼                   ▼                      │
│  ┌────────────────┐  ┌────────────────┐  ┌────────────────┐                │
│  │  AST Extractor │  │  CST Extractor │  │ Symtable Ext.  │                │
│  │  extract_ast   │  │  extract_cst   │  │extract_symtable│                │
│  │  _tables       │  │  _tables       │  │   s_table      │                │
│  │                │  │  (LibCST)      │  │                │                │
│  │ - ast_nodes    │  │ - cst_refs     │  │ - symtable_    │                │
│  │ - ast_edges    │  │ - cst_imports  │  │   blocks       │                │
│  │ - ast_         │  │ - cst_         │  │                │                │
│  │   docstrings   │  │   callsites    │  │                │                │
│  │ - ast_imports  │  │ - cst_defs     │  │                │                │
│  │ - ast_defs     │  │ - cst_type_    │  │                │                │
│  │ - ast_calls    │  │   exprs        │  │                │                │
│  └───────┬────────┘  └───────┬────────┘  └───────┬────────┘                │
│          │                   │                   │                          │
│          ▼                   ▼                   ▼                          │
│       ast_*_v1           cst_*_v1          symtable_*_v1                    │
│                                                                              │
│              ┌───────────────────┬───────────────────┐                      │
│              │                   │                   │                      │
│              ▼                   ▼                   ▼                      │
│  ┌────────────────┐  ┌────────────────┐  ┌────────────────┐                │
│  │Bytecode Extract│  │ SCIP Extractor │  │ Tree-sitter    │                │
│  │extract_bytecode│  │extract_scip    │  │ extract_ts     │                │
│  │     _table     │  │   _tables      │  │   _tables      │                │
│  │                │  │                │  │                │                │
│  │ - bytecode_    │  │ - scip_index   │  │ - ts_nodes     │                │
│  │   code_objs    │  │                │  │ - ts_errors    │                │
│  │                │  │                │  │ - ts_missing   │                │
│  │                │  │                │  │ - ts_edges     │                │
│  │                │  │                │  │ - ts_captures  │                │
│  └───────┬────────┘  └───────┬────────┘  └───────┬────────┘                │
│          │                   │                   │                          │
│          ▼                   ▼                   ▼                          │
│    bytecode_*_v1         scip_*_v1            ts_*_v1                       │
│                                                                              │
│              ┌───────────────────┬───────────────────┐                      │
│              │                   │                   │                      │
│              ▼                   ▼                   ▼                      │
│  ┌────────────────┐  ┌────────────────┐  ┌────────────────┐                │
│  │ Python Imports │  │External Scope  │  │ Row Builder/   │                │
│  │extract_python  │  │extract_python  │  │ Schema         │                │
│  │  _imports      │  │  _external     │  │ Derivation     │                │
│  │   _tables      │  │   _tables      │  │                │                │
│  │                │  │                │  │ ExtractionRow  │                │
│  │ - python_      │  │ - python_      │  │ Builder        │                │
│  │   imports      │  │   external     │  │                │                │
│  └────────────────┘  └────────────────┘  └────────────────┘                │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Module Organization

The extraction layer is organized into the following subdirectories:

- **`extractors/`** - Evidence layer implementations (AST, CST, symtable, bytecode, SCIP, tree-sitter, imports, external scope)
- **`coordination/`** - Execution context, evidence planning, materialization, schema operations
- **`scanning/`** - Repository scanning, scope filtering, scope manifests
- **`git/`** - Git repository integration (blobs, history, remotes, submodules)
- **`python/`** - Python-specific scope and environment profiling
- **`infrastructure/`** - Caching, parallelization, worklists, result types

## Core Patterns

### FileContext

**File:** `src/extract/coordination/context.py` (lines 36-100)

The `FileContext` dataclass bundles per-file state passed to extractors:

```python
@dataclass(frozen=True)
class FileContext:
    file_id: str              # Stable file identifier
    path: str                 # Relative path from repo root
    abs_path: str | None      # Absolute filesystem path
    file_sha256: str | None   # Content hash for caching
    encoding: str | None      # Detected encoding
    text: str | None          # Decoded text content
    data: bytes | None        # Raw byte content
```

**Factory Method** (`src/extract/coordination/context.py:69-100`):
```python
@classmethod
def from_repo_row(cls, row: Mapping[str, object]) -> FileContext:
    """Build FileContext from a repo_files_v1 row."""
    payload = cls._payload_from_row(row)
    return cls(
        file_id=payload.file_id or "",
        path=payload.path or "",
        abs_path=payload.abs_path,
        file_sha256=payload.file_sha256,
        encoding=payload.encoding,
        text=payload.text,
        data=payload.data,
    )
```

### ExtractExecutionContext

**File:** `src/extract/coordination/context.py` (lines 116-158)

The `ExtractExecutionContext` provides session-level resources:

```python
@dataclass(frozen=True)
class ExtractExecutionContext:
    file_contexts: Iterable[FileContext] | None
    evidence_plan: EvidencePlan | None
    scope_manifest: ScopeManifest | None
    session: ExtractSession | None
    runtime_spec: RuntimeProfileSpec | None
    profile: str = "default"
```

This context is shared across all extractors in a session, enabling:
- Consistent runtime configuration
- Centralized evidence planning
- Scope manifest coordination
- Extract session management

### SpanSpec

**File:** `src/extract/coordination/context.py` (lines 160-172)

The `SpanSpec` dataclass defines universal coordinate spans:

```python
@dataclass(frozen=True)
class SpanSpec:
    start_line0: int | None       # 0-indexed line start
    start_col: int | None         # Column start (unit varies)
    end_line0: int | None         # 0-indexed line end
    end_col: int | None           # Column end
    end_exclusive: bool | None    # Whether end position is exclusive
    col_unit: str | None          # "byte" | "utf32" | ...
    byte_start: int | None        # Canonical byte offset
    byte_len: int | None          # Canonical byte length
```

**Key Invariant:** `byte_start` and `byte_len` provide the canonical coordinate system. Line/column coordinates are auxiliary display information.

### ExtractionRowBuilder

**File:** `src/extract/row_builder.py` (lines 127-220)

The `ExtractionRowBuilder` provides standardized row construction with file identity, span handling, and metadata attributes:

```python
@dataclass(frozen=True)
class ExtractionRowBuilder:
    file_id: str
    path: str
    file_sha256: str | None = None
    repo_id: str | None = None

    def file_identity(self) -> dict[str, str | None]:
        """Return standard file identity columns."""

    def with_span(self, bstart: int, bend: int) -> dict[str, int]:
        """Return byte span columns."""

    def with_attrs(self, values: Mapping[str, object]) -> list[tuple[str, str]]:
        """Return attrs map entries."""
```

This builder ensures consistent field naming and schema alignment across all extractors.

## Individual Extractors

### AST Extraction

**File:** `src/extract/extractors/ast_extract.py` (~1300 lines)

**Purpose:** Parse Python Abstract Syntax Tree via `ast` module, capturing nodes, edges, docstrings, imports, definitions, and calls.

**Entry Point:** `extract_ast_tables()` (line 1246)

**Parsing Pipeline:**
1. Detect Python feature version from `pyproject.toml`
2. Select parser: `ast.parse()` or `compile()` for advanced features
3. Walk AST via iterative stack traversal
4. Emit rows for nodes, edges, docstrings, imports, defs, calls

**Output Tables** (multiple `ast_*_v1` tables):
- `ast_nodes_v1`: AST node records with kind, name, span, attrs
- `ast_edges_v1`: Parent-child AST edges with slot/index
- `ast_docstrings_v1`: Docstring content with owner references
- `ast_imports_v1`: Import statements with module/name/asname
- `ast_defs_v1`: Function/class definitions with decorator/arg counts
- `ast_calls_v1`: Call expressions with callee info

**Options:** `AstExtractOptions` (lines 66-78)
- `type_comments`: Extract type comments (default: True)
- `feature_version`: Python version for parsing
- `mode`: Parsing mode (exec, eval, single, func_type)
- `max_bytes`: Maximum file size for processing
- `cache_by_sha`: Enable SHA-based caching

### LibCST Extraction

**File:** `src/extract/extractors/cst_extract.py` (~1850 lines)

**Purpose:** Parse concrete syntax tree via LibCST with metadata provider integration for qualified names, scopes, and type inference.

**Entry Point:** `extract_cst_tables()` (line 1795)

**Metadata Providers:**
- `ByteSpanPositionProvider`: Byte offsets for all nodes
- `QualifiedNameProvider`: Fully qualified names
- `ScopeProvider`: Lexical scope information
- `TypeInferenceProvider`: Inferred types (when available)
- `ParentNodeProvider`: Parent node relationships
- `ExpressionContextProvider`: Load/store/delete contexts

**Output Tables** (multiple `cst_*_v1` tables):
- `cst_refs_v1`: Name/attribute references with qualified names
- `cst_imports_v1`: Import statements with kind/module/name
- `cst_callsites_v1`: Callsites with callee shape/qnames
- `cst_defs_v1`: Function/class definitions with qnames/fqns
- `cst_type_exprs_v1`: Type expressions from annotations
- `cst_docstrings_v1`: Docstrings with owner references
- `cst_decorators_v1`: Decorator applications
- `cst_call_args_v1`: Call argument details

**Options:** `CstExtractOptions` (lines 77-98)
- `compute_qualified_names`: Enable QualifiedNameProvider (default: True)
- `compute_fully_qualified_names`: Enable FullyQualifiedNameProvider (default: True)
- `compute_scope`: Enable ScopeProvider (default: True)
- `compute_type_inference`: Enable TypeInferenceProvider (default: False)

### Symtable Extraction

**File:** `src/extract/extractors/symtable_extract.py` (~910 lines)

**Purpose:** Extract Python symbol table metadata via `symtable.symtable()`, capturing scopes, symbols, and namespace relationships.

**Entry Point:** `extract_symtables_table()` (line 850)

**Scope Walk Mechanism:**
1. Parse source to get top-level `SymbolTable`
2. Walk scope tree via iterative stack traversal
3. Build qualified paths (e.g., `top::MyClass::my_method`)
4. Extract symbol flags (is_local, is_free, is_global, etc.)

**Symbol Flags:**
- `is_referenced`: Symbol is used
- `is_assigned`: Symbol has assignment
- `is_parameter`: Function parameter
- `is_global`: Global scope reference
- `is_nonlocal`: Nonlocal scope reference
- `is_free`: Free variable (closure)

**Output Tables:**
- `symtable_blocks_v1`: Scope blocks with nested symbols list

**Options:** `SymtableExtractOptions` (lines 59-63)
- `compile_type`: Compilation mode (exec, eval, single)

### Bytecode Extraction

**File:** `src/extract/extractors/bytecode_extract.py` (~1810 lines)

**Purpose:** Disassemble Python bytecode via `dis` module, extracting instructions, exception tables, CFG blocks, and data flow scaffolding.

**Entry Point:** `extract_bytecode_table()` (line 1753)

**Code Unit Hierarchy:**
- Recursively iterate nested code objects
- Extract instructions from each code object
- Derive CFG blocks from jump targets and terminators
- Construct CFG edges (jump, branch, next, exc)

**CFG Derivation:**
1. Identify block boundaries (jump targets, terminators, exception handlers)
2. Construct blocks as `(start_offset, end_offset)` pairs
3. Derive edges: `jump`, `branch`, `next`, `exc`

**Output Tables:**
- `bytecode_code_objects_v1`: Code objects with instructions, blocks, cfg_edges

**Options:** `BytecodeExtractOptions` (lines 76-98)
- `optimize`: Optimization level (default: 0)
- `adaptive`: Use adaptive instructions (default: False for stability)
- `include_cfg_derivations`: Derive CFG blocks/edges (default: True)

### SCIP Extraction

**File:** `src/extract/extractors/scip/extract.py` (~350+ lines across submodules)

**Purpose:** Process SCIP (Semantic Code Intelligence Protocol) index files to extract cross-repository symbol information.

**Entry Point:** `extract_scip_tables()` and `run_scip_python_index()`

SCIP provides:
- Symbol definitions with semantic information
- Cross-reference occurrences
- Type information when available
- Package/module relationships

**Output Tables:**
- `scip_index_v1`: SCIP index data with symbols, occurrences, references

**Options:** `SCIPIndexOptions` (lines 69-83)
- `project_name`: Project name for SCIP indexing
- `scip_python_bin`: Path to scip-python binary
- `timeout_s`: Indexing timeout

### Tree-sitter Extraction

**File:** `src/extract/extractors/tree_sitter/extract.py` (~600+ lines across submodules)

**Purpose:** Error-tolerant parsing via tree-sitter, capturing syntax errors and missing nodes that other parsers might reject.

**Entry Point:** `extract_ts_tables()` and `extract_ts()`

Useful for:
- Incomplete or malformed source files
- Incremental parsing scenarios
- Error detection and diagnostics

**Output Tables:**
- `ts_nodes_v1`: Tree-sitter nodes with kind/text/span
- `ts_errors_v1`: Parse errors with locations
- `ts_missing_v1`: Missing/expected nodes
- `ts_edges_v1`: Parent-child relationships
- `ts_captures_v1`: Query captures from tree-sitter queries
- `ts_defs_v1`: Definition nodes
- `ts_calls_v1`: Call nodes
- `ts_imports_v1`: Import nodes
- `ts_docstrings_v1`: Docstring nodes
- `ts_stats_v1`: Parse statistics

**Options:** `TreeSitterExtractOptions` (lines 71-96)
- `include_nodes/errors/missing/edges/captures`: Control output tables
- `parser_timeout_micros`: Parser timeout
- `query_match_limit`: Maximum query matches

### Python Imports Extraction

**File:** `src/extract/extractors/imports_extract.py` (~230 lines)

**Purpose:** Materialize unified Python import rows from AST, CST, and tree-sitter imports.

**Entry Point:** `extract_python_imports_tables()`

**Output Tables:**
- `python_imports_v1`: Unified import records

**Options:** `PythonImportsExtractOptions` (lines 28-32)
- `repo_id`: Repository identifier

### External Scope Extraction

**File:** `src/extract/extractors/external_scope.py` (~430 lines)

**Purpose:** Extract external Python interface information from unified import tables, resolving to installed packages and stdlib modules.

**Entry Point:** `extract_python_external_tables()`

**Output Tables:**
- `python_external_v1`: External interface records with distribution metadata

**Options:** `ExternalInterfaceExtractOptions` (lines 34-42)
- `include_stdlib`: Include stdlib modules (default: True)
- `include_unresolved`: Include unresolved imports (default: True)
- `depth`: Resolution depth (metadata, full)

## Repository Scanning

**File:** `src/extract/scanning/repo_scan.py` (~1150 lines)

**Entry Point:** `scan_repo()` (line 160+)

**Dual-Track Discovery:**
1. **Git-based:** Uses libgit2/pygit2 for index-based discovery
2. **Filesystem fallback:** Uses glob-based matching

**Options** (`RepoScanOptions`, lines 60-82):
```python
@dataclass(frozen=True)
class RepoScanOptions(RepoOptions):
    scope_policy: RepoScopeOptions | Mapping[str, object]
    include_sha256: bool = True
    max_file_bytes: int | None = None
    max_files: int | None = 200_000
    diff_base_ref: str | None = None
    diff_head_ref: str | None = None
    changed_only: bool = False
    update_submodules: bool = False
    record_blame: bool = False
    record_pathspec_trace: bool = False
```

**Git Integration:**
- `diff_paths()`: Compute changed files between refs
- `submodule_roots()`: Discover git submodules
- `worktree_roots()`: Discover linked worktrees
- `blame_hunks()`: Extract line-level authorship

**Caching:**
- `@memoize_stampede` on scan results
- Cache key: `{repo_root, schema_fingerprint, options}`
- `@throttle(count=1, seconds=5)` prevents concurrent scans

**Output:**
- `repo_files_v1`: File metadata with paths, SHA256, encoding, text/data

## Parallel Execution

**File:** `src/extract/infrastructure/parallel.py` (~135 lines)

**Mechanism:**
```python
def parallel_map(items, fn, max_workers=None):
    if _gil_disabled():
        # GIL-free Python: use ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers) as executor:
            for item in executor.map(fn, items): yield item
    else:
        # Standard Python: use fork-based ProcessPoolExecutor
        with _process_executor(max_workers) as executor:
            for item in executor.map(fn, items): yield item
```

**Worker Count Resolution:**
```python
def resolve_max_workers(max_workers, kind="cpu"):
    if max_workers is not None:
        return max(1, max_workers)
    return max(1, os.cpu_count() or 1)
```

**Extractor-Specific Patterns:**
- **AST:** Fork-based parallelism with per-file cache lookups
- **CST:** Serial when `FullRepoManager` active (shared state)
- **Symtable:** ThreadPoolExecutor (I/O-bound)
- **Bytecode:** Conditional based on file count/size thresholds
- **Tree-sitter:** Parallel with optional incremental cache

## Evidence Plan Gating

**File:** `src/extract/coordination/evidence_plan.py` (~190 lines)

Skip unnecessary extractors when downstream graph only requires subset of evidence:

```python
@dataclass(frozen=True)
class EvidencePlan:
    sources: tuple[str, ...]              # Required dataset names
    requirements: tuple[EvidenceRequirement, ...]
    required_columns: Mapping[str, tuple[str, ...]]
    telemetry: Mapping[str, Mapping[str, ScanTelemetry]]

    def requires_dataset(self, name: str) -> bool:
        return name in self.sources

    def required_columns_for(self, name: str) -> tuple[str, ...]:
        return self.required_columns.get(name, ())
```

This enables projection pushdown—if downstream only needs `(file_id, bstart, bend)` from `cst_refs`, other columns are not materialized.

## Schema Derivation

**File:** `src/extract/schema_derivation.py` (~850 lines)

**Purpose:** Provide fluent builder interface for constructing extraction schemas from composable field bundles, with automatic schema derivation from DataFusion plans.

**ExtractionSchemaBuilder:**
```python
schema = (
    ExtractionSchemaBuilder("my_extractor", version=1)
    .with_file_identity(include_sha256=True)
    .with_span(prefix="")
    .with_evidence_metadata()
    .with_fields(FieldSpec(name="custom_col", dtype=pa.string()))
    .build()
)
```

**Schema Templates:**
- File identity fields (`schema_spec.file_identity`)
- Span fields (`schema_spec.specs`, `datafusion_engine.arrow.semantic`)
- Evidence metadata (`schema_spec.evidence_metadata`)
- Nested type builders (`datafusion_engine.arrow.nested`)

This module integrates with schema inference via DataFusion lineage to reduce manual schema specification.

## Design Patterns

1. **Visitor Pattern:** LibCST (`CSTCollector`), AST (iterative stack traversal)
2. **Builder Pattern:** `FileContext.from_repo_row()`, `ExtractionRowBuilder`, `ExtractionSchemaBuilder`
3. **Strategy Pattern:** Dual-track file discovery (git vs. filesystem)
4. **Cache-Aside:** SHA256-keyed caching with `@memoize_stampede`
5. **Graceful Degradation:** Parse errors → emit error rows + continue
6. **Fluent Interface:** `ExtractionSchemaBuilder` with chainable methods

## Critical Files Reference

| File | Lines | Purpose |
|------|-------|---------|
| `src/extract/coordination/context.py` | ~350 | FileContext, SpanSpec, ExtractExecutionContext |
| `src/extract/coordination/evidence_plan.py` | ~190 | Evidence requirement gating |
| `src/extract/coordination/materialization.py` | ~850 | Extract plan materialization |
| `src/extract/row_builder.py` | ~500 | Standardized row construction |
| `src/extract/schema_derivation.py` | ~850 | Schema builder and derivation |
| `src/extract/scanning/repo_scan.py` | ~1150 | Repository file discovery |
| `src/extract/extractors/ast_extract.py` | ~1300 | AST parsing and extraction |
| `src/extract/extractors/cst_extract.py` | ~1850 | LibCST with metadata providers |
| `src/extract/extractors/symtable_extract.py` | ~910 | Symbol table extraction |
| `src/extract/extractors/bytecode_extract.py` | ~1810 | Bytecode, CFG, DFG extraction |
| `src/extract/extractors/scip/extract.py` | ~350+ | SCIP index extraction |
| `src/extract/extractors/tree_sitter/extract.py` | ~600+ | Tree-sitter extraction |
| `src/extract/extractors/imports_extract.py` | ~230 | Unified imports materialization |
| `src/extract/extractors/external_scope.py` | ~430 | External interface resolution |
| `src/extract/infrastructure/parallel.py` | ~135 | Fork-based parallelism utilities |
| `src/extract/infrastructure/worklists.py` | ~11380 | Worklist queue management |
| `src/extract/session.py` | ~60 | Extract session lifecycle |

## Key Differences from Earlier Documentation

1. **Module Organization Changed:** Files reorganized into `coordination/`, `scanning/`, `infrastructure/`, `git/`, `python/` subdirectories
2. **FileContext Location:** Moved from `src/extract/helpers.py` to `src/extract/coordination/context.py`
3. **ExtractExecutionContext Simplified:** Now focused on session-level resources (evidence plan, scope manifest, session)
4. **SpanSpec Updated:** Added `end_exclusive` field; moved to coordination layer
5. **New Extractors:** Added `imports_extract.py` (unified imports) and `external_scope.py` (external interface resolution)
6. **ExtractionRowBuilder Added:** New standardized row builder in `row_builder.py` for consistent field handling
7. **Schema Derivation Added:** New `schema_derivation.py` module with fluent builder interface for extraction schemas
8. **Evidence Plan Enhanced:** Now includes telemetry tracking and template-based requirements
9. **Parallel Execution Enhanced:** Added GIL-free detection and OpenTelemetry context propagation
10. **Output Schema Changes:** Extractors now emit multiple versioned tables (`ast_nodes_v1`, `cst_refs_v1`, etc.) instead of single composite tables
