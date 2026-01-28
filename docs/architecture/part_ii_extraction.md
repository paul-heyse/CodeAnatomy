# Part II: Extraction Stage

## Overview

The Extraction Stage transforms Python source files into structured evidence tables through multiple parsing strategies. Each extractor captures a distinct view of code structure—AST for syntax trees, LibCST for concrete syntax with metadata, symtable for scoping and bindings, bytecode for compiled instructions and control flow, SCIP for cross-repository symbols, and tree-sitter for error-tolerant parsing. These complementary evidence layers enable rich downstream analysis.

The extraction pipeline follows a shared-nothing architecture where each file is processed independently. The `FileContext` abstraction (`src/extract/helpers.py:77-130`) bundles file-level state (path, content, SHA-256 hash, encoding) passed to all extractors. The `ExtractExecutionContext` (`src/extract/helpers.py:180-220`) provides session-wide resources (runtime profile, diagnostics collector, cache configuration). This separation enables parallel execution via fork-based `ProcessPoolExecutor` while maintaining per-file isolation.

A key architectural invariant is the **bytes-first design**: all coordinate systems anchor to byte offsets. The `SpanSpec` dataclass (`src/extract/helpers.py:248-267`) defines a universal span representation with `byte_start` and `byte_len` fields alongside optional line/column coordinates. Line/column semantics vary by parser (AST uses 1-indexed lines with byte columns, LibCST uses UTF-32 columns), but byte spans provide deterministic join keys for the normalization stage.

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         EXTRACTION PIPELINE                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                     Repository Scanner                                │   │
│  │  scan_repo() → iter_repo_files_pygit2() / iter_repo_files_fs()       │   │
│  │                                                                       │   │
│  │  Output: repo_files_v1 (file_id, path, size_bytes, file_sha256)      │   │
│  └───────────────────────────────┬──────────────────────────────────────┘   │
│                                  │                                          │
│              ┌───────────────────┼───────────────────┐                      │
│              │                   │                   │                      │
│              ▼                   ▼                   ▼                      │
│  ┌────────────────┐  ┌────────────────┐  ┌────────────────┐                │
│  │  AST Extractor │  │  CST Extractor │  │ Symtable Ext.  │                │
│  │  ast_extract   │  │  cst_extract   │  │symtable_extract│                │
│  │                │  │  (LibCST)      │  │                │                │
│  │ - nodes        │  │ - refs         │  │ - scopes       │                │
│  │ - edges        │  │ - imports      │  │ - symbols      │                │
│  │ - docstrings   │  │ - callsites    │  │ - bindings     │                │
│  │ - imports      │  │ - defs         │  │                │                │
│  │ - defs         │  │ - type_exprs   │  │                │                │
│  │ - calls        │  │ - qnames       │  │                │                │
│  └───────┬────────┘  └───────┬────────┘  └───────┬────────┘                │
│          │                   │                   │                          │
│          ▼                   ▼                   ▼                          │
│       ast_files_v1     libcst_files_v1   symtable_files_v1                 │
│                                                                              │
│              ┌───────────────────┬───────────────────┐                      │
│              │                   │                   │                      │
│              ▼                   ▼                   ▼                      │
│  ┌────────────────┐  ┌────────────────┐  ┌────────────────┐                │
│  │Bytecode Extract│  │ SCIP Extractor │  │ Tree-sitter    │                │
│  │bytecode_extract│  │ scip_extract   │  │ tree_sitter_*  │                │
│  │                │  │                │  │                │                │
│  │ - instructions │  │ - symbols      │  │ - nodes        │                │
│  │ - cfg_blocks   │  │ - occurrences  │  │ - errors       │                │
│  │ - cfg_edges    │  │ - references   │  │ - missing      │                │
│  │ - dfg_edges    │  │                │  │                │                │
│  │ - exceptions   │  │                │  │                │                │
│  └───────┬────────┘  └───────┬────────┘  └───────┬────────┘                │
│          │                   │                   │                          │
│          ▼                   ▼                   ▼                          │
│    bytecode_files_v1    scip_files_v1    tree_sitter_files_v1              │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Core Patterns

### FileContext

**File:** `src/extract/helpers.py` (lines 77-130)

The `FileContext` dataclass bundles per-file state passed to extractors:

```python
@dataclass(frozen=True)
class FileContext:
    file_id: str              # Stable file identifier
    path: str                 # Relative path from repo root
    abs_path: Path            # Absolute filesystem path
    content: bytes            # Raw file content
    content_str: str | None   # Decoded content (when text)
    size_bytes: int           # File size
    file_sha256: str          # Content hash for caching
    encoding: str | None      # Detected encoding
    mtime_ns: int | None      # Modification timestamp
```

**Factory Method** (`src/extract/helpers.py:95-125`):
```python
@classmethod
def from_repo_row(cls, row: Mapping[str, object], *, repo_root: Path) -> FileContext:
    """Build FileContext from a repo_files_v1 row."""
    abs_path = repo_root / row["path"]
    content = abs_path.read_bytes()
    # ... decode content, compute hash
    return cls(file_id=row["file_id"], path=row["path"], ...)
```

### ExtractExecutionContext

**File:** `src/extract/helpers.py` (lines 180-220)

The `ExtractExecutionContext` provides session-level resources:

```python
@dataclass
class ExtractExecutionContext:
    runtime_profile: DataFusionRuntimeProfile | None
    diagnostics: DiagnosticsCollector | None
    cache_profile: CacheProfile | None
    cache_ttl: int | None
    batch_size: int
    max_workers: int | None
```

This context is shared across all extractors in a session, enabling:
- Consistent runtime configuration
- Centralized diagnostics collection
- Cache coordination

### SpanSpec

**File:** `src/extract/helpers.py` (lines 248-267)

The `SpanSpec` dataclass defines universal coordinate spans:

```python
@dataclass(frozen=True)
class SpanSpec:
    line0_start: int | None     # 0-indexed line start
    col_start: int | None       # Column start (unit varies)
    line0_end: int | None       # 0-indexed line end
    col_end: int | None         # Column end
    col_unit: str               # "byte" | "utf32" | ...
    byte_start: int | None      # Canonical byte offset
    byte_len: int | None        # Canonical byte length
```

**Key Invariant:** `byte_start` and `byte_len` provide the canonical coordinate system. Line/column coordinates are auxiliary display information.

## Individual Extractors

### AST Extraction

**File:** `src/extract/ast_extract.py` (~1200 lines)

**Purpose:** Parse Python Abstract Syntax Tree via `ast` module, capturing nodes, edges, docstrings, imports, definitions, and calls.

**Entry Point:** `extract_ast()` (line 979)

**Parsing Pipeline:**
1. Detect Python feature version from `pyproject.toml`
2. Select parser: `ast.parse()` or `compile()` for advanced features
3. Walk AST via iterative stack traversal (`_walk_ast()`)
4. Emit rows for nodes, edges, docstrings, imports, defs, calls

**Output Schema** (`ast_files_v1`):
```
{
  file_id, path, file_sha256,
  nodes: list<struct<ast_id, kind, name, span, attrs>>,
  edges: list<struct<src, dst, kind, slot, idx>>,
  docstrings: list<struct<owner_ast_id, docstring, span>>,
  imports: list<struct<module, name, asname, level>>,
  defs: list<struct<kind, name, decorator_count, arg_count>>,
  calls: list<struct<func_kind, func_name, arg_count>>,
}
```

### LibCST Extraction

**File:** `src/extract/cst_extract.py` (~1700 lines)

**Purpose:** Parse concrete syntax tree via LibCST with metadata provider integration for qualified names, scopes, and type inference.

**Entry Point:** `extract_cst()` (line 1616)

**Metadata Providers:**
- `ByteSpanPositionProvider`: Byte offsets for all nodes
- `QualifiedNameProvider`: Fully qualified names
- `ScopeProvider`: Lexical scope information
- `TypeInferenceProvider`: Inferred types (when available)

**CSTCollector Visitor** (lines 640-1284):
- `visit_FunctionDef()`: Function definitions, docstrings, decorators
- `visit_ClassDef()`: Class definitions
- `visit_Name()`: Name references
- `visit_Attribute()`: Attribute references
- `visit_Call()`: Callsites with callee analysis

**Output Schema** (`libcst_files_v1`):
```
{
  file_id, path, file_sha256,
  refs: list<struct<ref_kind, ref_text, bstart, bend, scope_type, inferred_type>>,
  imports: list<struct<kind, module, name, asname, relative_level>>,
  callsites: list<struct<callee_shape, callee_text, callee_qnames, arg_count>>,
  defs: list<struct<kind, name, qnames, def_fqns, decorator_count>>,
  type_exprs: list<struct<owner_def_kind, param_name, expr_role, expr_text>>,
}
```

### Symtable Extraction

**File:** `src/extract/symtable_extract.py` (~770 lines)

**Purpose:** Extract Python symbol table metadata via `symtable.symtable()`, capturing scopes, symbols, and namespace relationships.

**Entry Point:** `extract_symtable()` (line 721)

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

**Output Schema** (`symtable_files_v1`):
```
{
  file_id, path, file_sha256,
  blocks: list<struct<
    block_id, parent_block_id, block_type, name, lineno1, qualpath,
    symbols: list<struct<name, is_referenced, is_local, is_free, ...>>
  >>,
}
```

### Bytecode Extraction

**File:** `src/extract/bytecode_extract.py` (~1700 lines)

**Purpose:** Disassemble Python bytecode via `dis` module, extracting instructions, exception tables, CFG blocks, and data flow scaffolding.

**Entry Point:** `extract_bytecode()` (line 1626)

**Code Unit Hierarchy:**
```python
def _iter_code_objects(co, parent=None):
    yield co, parent
    for c in co.co_consts:
        if isinstance(c, CodeType):
            yield from _iter_code_objects(c, co)
```

**CFG Derivation:**
1. Identify block boundaries (jump targets, terminators, exception handlers)
2. Construct blocks as `(start_offset, end_offset)` pairs
3. Derive edges: `jump`, `branch`, `next`, `exc`

**DFG Scaffolding:**
- Track stack depth via `dis.stack_effect()`
- Emit `USE_STACK` edges connecting producers to consumers

**Output Schema** (`bytecode_files_v1`):
```
{
  file_id, path, file_sha256,
  code_objects: list<struct<
    qualpath, argcount, nlocals, flags, stacksize,
    varnames, freevars, cellvars,
    instructions: list<struct<offset, opname, arg, argval, line_number>>,
    blocks: list<struct<start_offset, end_offset, kind>>,
    cfg_edges: list<struct<src_block, dst_block, kind>>,
    dfg_edges: list<struct<src_instr_index, dst_instr_index, kind>>,
  >>,
}
```

### SCIP Extraction

**File:** `src/extract/scip_extract.py`

**Purpose:** Process SCIP (Semantic Code Intelligence Protocol) index files to extract cross-repository symbol information.

SCIP provides:
- Symbol definitions with semantic information
- Cross-reference occurrences
- Type information when available
- Package/module relationships

### Tree-sitter Extraction

**File:** `src/extract/tree_sitter_extract.py`

**Purpose:** Error-tolerant parsing via tree-sitter, capturing syntax errors and missing nodes that other parsers might reject.

Useful for:
- Incomplete or malformed source files
- Incremental parsing scenarios
- Error detection and diagnostics

## Repository Scanning

**File:** `src/extract/repo_scan.py` (~620 lines)

**Entry Point:** `scan_repo()` (line 349)

**Dual-Track Discovery:**
1. **Git-based:** `iter_repo_files_pygit2()` leverages libgit2 index
2. **Filesystem fallback:** `iter_repo_files_fs()` uses glob-based matching

**Options** (line 50-83):
```python
@dataclass
class RepoScanOptions:
    include_globs: Sequence[str] = ("**/*.py",)
    exclude_dirs: Sequence[str] = (".git", "__pycache__", ".venv", ...)
    max_file_bytes: int | None = None
    max_files: int | None = 200_000
    changed_only: bool = False          # Git diff filtering
    include_submodules: bool = False
    include_worktrees: bool = False
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

## Parallel Execution

**File:** `src/extract/parallel.py` (~100 lines)

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
- **LibCST:** Serial when `FullRepoManager` active (shared state)
- **Symtable:** ThreadPoolExecutor (I/O-bound)
- **Bytecode:** Conditional based on `_should_parallelize()`

## Evidence Plan Gating

**File:** `src/extract/evidence_plan.py` (lines 21-71)

Skip unnecessary extractors when downstream graph only requires subset of evidence:

```python
@dataclass
class EvidencePlan:
    sources: tuple[str, ...]              # Required dataset names
    requirements: tuple[EvidenceRequirement, ...]
    required_columns: Mapping[str, tuple[str, ...]]

    def requires_dataset(self, name: str) -> bool:
        return name in self.sources

    def required_columns_for(self, name: str) -> tuple[str, ...]:
        return self.required_columns.get(name, ())
```

This enables projection pushdown—if downstream only needs `(node_id, bstart)` from `cst_nodes`, other columns are not materialized.

## Design Patterns

1. **Visitor Pattern:** LibCST (`CSTCollector`), AST (`_walk_ast`)
2. **Builder Pattern:** `FileContext.from_repo_row()`, `_build_scope_context()`
3. **Strategy Pattern:** Dual-track file discovery (git vs. filesystem)
4. **Cache-Aside:** SHA256-keyed caching with `@memoize_stampede`
5. **Graceful Degradation:** Parse errors → emit error rows + continue

## Critical Files Reference

| File | Lines | Purpose |
|------|-------|---------|
| `src/extract/helpers.py` | ~600 | Core patterns: FileContext, SpanSpec, ExtractExecutionContext |
| `src/extract/repo_scan.py` | ~620 | Repository file discovery with git integration |
| `src/extract/ast_extract.py` | ~1200 | AST parsing with walk-based extraction |
| `src/extract/cst_extract.py` | ~1700 | LibCST with metadata providers |
| `src/extract/symtable_extract.py` | ~770 | Symbol table extraction |
| `src/extract/bytecode_extract.py` | ~1700 | Bytecode, CFG, DFG extraction |
| `src/extract/parallel.py` | ~100 | Fork-based parallelism utilities |
| `src/extract/evidence_plan.py` | ~80 | Evidence requirement gating |
| `src/extract/session.py` | ~150 | Session lifecycle management |
