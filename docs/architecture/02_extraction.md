# Extraction Pipeline (Python)

## Purpose

The extraction subsystem orchestrates multi-source evidence collection from Python repositories, transforming source files into structured PyArrow tables that serve as inputs to the semantic compilation pipeline. Extraction operates in four sequential stages with parallel execution within stages, producing byte-span-anchored evidence from AST, LibCST, bytecode, symtable, tree-sitter, and SCIP sources.

Evidence extraction is **deterministic** and **idempotent**. Given the same source files and configuration, extractors produce identical outputs across runs. All outputs conform to pre-defined Arrow schemas and are persisted to Delta Lake for downstream consumption.

## Architecture Overview

### Four-Stage Execution Model

Extraction executes as a directed acyclic graph with four execution stages, each stage dependent on outputs from prior stages:

```
┌─────────────────────────────────────────────────────────────────────┐
│ Stage 0: Repository Scan                                            │
│ ─────────────────────────────────────────────────────────────────── │
│  scan_repo_tables()                                                 │
│    ├─ Git-based scan (primary path)                                 │
│    └─ Filesystem fallback (non-git workdirs)                        │
│                                                                      │
│  Outputs: repo_files_v1, file_line_index_v1                         │
└──────────────────────┬──────────────────────────────────────────────┘
                       │ repo_files_v1 (identity + metadata)
                       │
┌──────────────────────▼──────────────────────────────────────────────┐
│ Stage 1: Parallel Extractors (ThreadPoolExecutor, max_workers=6)   │
│ ─────────────────────────────────────────────────────────────────── │
│  extract_ast_tables()        → ast_files                            │
│  extract_cst_tables()        → libcst_files                         │
│  extract_bytecode_table()    → bytecode_files_v1                    │
│  extract_symtables_table()   → symtable_files_v1                    │
│  extract_ts_tables()         → tree_sitter_files (optional)         │
│  extract_scip_tables()       → scip_index (optional)                │
└──────────────────────┬──────────────────────────────────────────────┘
                       │ ast_files, libcst_files, tree_sitter_files
                       │
┌──────────────────────▼──────────────────────────────────────────────┐
│ Stage 2: Import Combiner (sequential)                               │
│ ─────────────────────────────────────────────────────────────────── │
│  extract_python_imports_tables()                                    │
│    ├─ Combines ast_files, libcst_files, tree_sitter_files          │
│    └─ Deduplicates and normalizes import statements                 │
│                                                                      │
│  Outputs: python_imports                                            │
└──────────────────────┬──────────────────────────────────────────────┘
                       │ python_imports
                       │
┌──────────────────────▼──────────────────────────────────────────────┐
│ Stage 3: External Interface Discovery (sequential)                  │
│ ─────────────────────────────────────────────────────────────────── │
│  extract_python_external_tables()                                   │
│    ├─ Resolves external package symbols                             │
│    └─ Builds external reference catalog                             │
│                                                                      │
│  Outputs: python_external_interfaces                                │
└─────────────────────────────────────────────────────────────────────┘
```

### Orchestration Entry Point

Extraction is orchestrated by a single function:

```python
from extraction.orchestrator import run_extraction

result = run_extraction(
    repo_root=Path("."),
    work_dir=Path(".codeanatomy"),
    tree_sitter_enabled=True,
    max_workers=6,
    options=ExtractionRunOptions(
        include_globs=("**/*.py",),
        exclude_globs=("venv/**", "build/**"),
    ),
)
```

**Entry point:** `src/extraction/orchestrator.py::run_extraction()`

**Output contract:** `ExtractionResult` (msgspec.Struct) containing:
- `delta_locations: dict[str, str]` - Mapping of dataset names to Delta table paths
- `semantic_input_locations: dict[str, str]` - Canonical semantic input names mapped to concrete tables
- `errors: list[dict[str, object]]` - Per-extractor error records (non-blocking)
- `timing: dict[str, float]` - Stage-level timing telemetry

All extractors produce PyArrow tables written to `{work_dir}/extract/{dataset_name}/` as Delta tables.

### Coordination Infrastructure

**FileContext** (`src/extract/coordination/context.py`):
```python
@dataclass(frozen=True)
class FileContext:
    file_id: str         # Stable file identifier (hash-based)
    path: str            # Repository-relative path
    abs_path: str | None # Absolute filesystem path
    file_sha256: str | None
    encoding: str | None
    text: str | None     # Decoded file content
    data: bytes | None   # Raw file bytes
```

**SpanSpec** (byte-span specification):
```python
@dataclass(frozen=True)
class SpanSpec:
    start_line0: int | None    # 0-based line number
    start_col: int | None      # Column offset (unit-dependent)
    end_line0: int | None
    end_col: int | None
    end_exclusive: bool | None # Whether end position is exclusive
    col_unit: str | None       # "utf8" | "utf16" | "utf32"
    byte_start: int | None     # Canonical byte offset (start)
    byte_len: int | None       # Canonical byte length
```

**ExtractSession** (`src/extract/session.py`):
- Wraps `EngineSession` for DataFusion access
- Provides unified session context for all extractors
- Manages runtime profile and caching policy

**ExtractExecutionContext** (execution bundle):
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

## Stage 0: Repository Scanning

### Purpose

Discover Python source files within the repository scope and capture file-level metadata (path, size, mtime, SHA-256, encoding). Provides the identity manifest (`repo_files_v1`) consumed by all downstream extractors.

### Public API

**Primary function:** `src/extract/scanning/repo_scan.py::scan_repo_tables()`

```python
def scan_repo_tables(
    repo_root: str | Path,
    *,
    options: RepoScanOptions,
    context: ExtractExecutionContext,
    prefer_reader: bool = False,
) -> dict[str, pa.Table]
```

**Outputs:**
- `repo_files_v1` - File identity and metadata table
- `file_line_index_v1` - Line offset index for byte-to-line mapping

**Options:** `RepoScanOptions` (msgspec.Struct, frozen=True)

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `scope_policy` | `RepoScopeOptions` | default | Include/exclude glob patterns |
| `include_sha256` | `bool` | `True` | Compute SHA-256 file hashes |
| `include_encoding` | `bool` | `True` | Detect file encoding |
| `max_files` | `int \| None` | `200_000` | Maximum file limit |
| `diff_base_ref` | `str \| None` | `None` | Git base reference for diff filtering |
| `diff_head_ref` | `str \| None` | `None` | Git head reference for diff filtering |
| `changed_only` | `bool` | `False` | Restrict to files changed between refs |

### Execution Path

1. **Git-based scan (primary path):**
   - Uses `pygit2` to enumerate tracked files
   - Respects `.gitignore` and `include_globs`/`exclude_globs`
   - Supports incremental scanning via `diff_base_ref`/`diff_head_ref`
   - Includes optional submodule and worktree enumeration

2. **Filesystem fallback (non-git workdirs):**
   - Activated when git scan fails (non-repo, corrupted repo, git not available)
   - Walks filesystem with glob patterns
   - Produces minimal `repo_files_v1` without git metadata

**Fallback trigger:** Any `OSError`, `RuntimeError`, `TypeError`, or `ValueError` from git scan.

**Fallback implementation:** `_run_repo_scan_fallback()` in orchestrator produces:
- Walks `repo_root` with `include_globs` (default: `**/*.py`)
- Applies `exclude_globs` filtering
- Computes `file_id`, `path`, `abs_path`, `size_bytes`, `mtime_ns`, `file_sha256`, `encoding`
- Raises `ValueError` if zero Python files discovered

### Output Schema

**repo_files_v1:**
```python
pa.schema([
    pa.field("file_id", pa.string()),
    pa.field("path", pa.string()),
    pa.field("abs_path", pa.string()),
    pa.field("size_bytes", pa.int64()),
    pa.field("mtime_ns", pa.int64()),
    pa.field("file_sha256", pa.string()),
    pa.field("encoding", pa.string()),
])
```

## Stage 1: Parallel Extractors

### Orchestration

Stage 1 extractors execute concurrently via `ThreadPoolExecutor` (default `max_workers=6`). All extractors depend on `repo_files_v1` from Stage 0 but have no inter-extractor dependencies.

**Orchestrator logic:**
```python
with ThreadPoolExecutor(max_workers=max_workers) as executor:
    futures = {name: executor.submit(fn) for name, fn in stage1_extractors.items()}
    for name, future in futures.items():
        result_table = future.result()
        delta_locations[name] = _write_delta(result_table, extract_dir / name, name)
```

**Error handling:** Individual extractor failures are logged and recorded in `errors` list but do not block other extractors.

### AST Extractor

**Purpose:** Parse Python source into Python AST (`ast` module), extracting symbols, imports, and syntax tree metadata.

**Public API:** `src/extract/extractors/ast_extract.py::extract_ast_tables()`

```python
def extract_ast_tables(
    *,
    repo_files: pa.Table,
    session: ExtractSession,
    profile: str = "default",
    prefer_reader: bool = False,
) -> dict[str, pa.Table | pa.RecordBatchReader]
```

**Output:** `ast_files` - Unified AST table containing symbols, imports, parse metadata.

**Key options (AstExtractOptions):**
- `type_comments: bool = True` - Parse PEP 484 type comments
- `feature_version: tuple[int, int] | None` - Python version for parser (auto-detected from `pyproject.toml`)
- `max_bytes: int = 50_000_000` - Skip files exceeding byte limit
- `max_nodes: int = 1_000_000` - Skip files exceeding AST node count

**Extraction process:**
1. Read file content from `FileContext`
2. Parse with `ast.parse()` using configured feature version
3. Walk AST tree extracting:
   - Definitions (functions, classes, assignments)
   - References (Name nodes, attribute access)
   - Imports (ImportFrom, Import)
   - Callsites (Call nodes)
4. Emit rows with byte-span anchoring

**Error handling:** Parse failures are recorded in `parse_errors` column; invalid syntax produces empty symbol rows.

### LibCST Extractor

**Purpose:** Parse Python source using LibCST for syntax-preserving parsing with optional metadata providers (scope, qualified names, type inference).

**Public API:** `src/extract/extractors/cst_extract.py::extract_cst_tables()`

```python
def extract_cst_tables(
    *,
    repo_files: pa.Table,
    session: ExtractSession,
    profile: str = "default",
    prefer_reader: bool = False,
) -> dict[str, pa.Table | pa.RecordBatchReader]
```

**Output:** `libcst_files` - Unified CST table containing refs, defs, imports, callsites, docstrings, decorators.

**Key options (CstExtractOptions):**
- `repo_root: Path | None` - Required for metadata providers
- `include_refs: bool = True` - Extract reference nodes
- `include_imports: bool = True` - Extract import statements
- `include_callsites: bool = True` - Extract call sites
- `include_defs: bool = True` - Extract definitions
- `include_docstrings: bool = True` - Extract docstrings
- `include_decorators: bool = True` - Extract decorator nodes
- `compute_scope: bool = True` - Enable `ScopeProvider` metadata
- `compute_qualified_names: bool = True` - Enable `QualifiedNameProvider`
- `compute_fully_qualified_names: bool = True` - Enable `FullyQualifiedNameProvider`

**Metadata providers:**
- `ByteSpanPositionProvider` - Byte-span anchoring (always enabled)
- `ScopeProvider` - Symbol scope analysis
- `QualifiedNameProvider` - Qualified name resolution
- `FullyQualifiedNameProvider` - Fully-qualified name resolution
- `ParentNodeProvider` - Parent-child tree relationships
- `TypeInferenceProvider` - Type inference (expensive, disabled by default)

**Extraction process:**
1. Parse file with `cst.parse_module()`
2. Wrap module with `MetadataWrapper` and enabled providers
3. Visit CST tree with matchers for:
   - `Name` nodes → refs
   - `FunctionDef`, `ClassDef`, `Assign` → defs
   - `Import`, `ImportFrom` → imports
   - `Call` → callsites
   - Docstring nodes → docstrings
   - Decorator nodes → decorators
4. Emit rows with byte-span and metadata annotations

**Span conventions:**
- `CST_LINE_BASE = 1` (1-based line numbers)
- `CST_COL_UNIT = "utf32"` (UTF-32 code unit offsets)
- `CST_END_EXCLUSIVE = True` (end positions are exclusive)

### Bytecode Extractor

**Purpose:** Compile Python source to bytecode and extract opcode sequences, control flow, and stack effects.

**Public API:** `src/extract/extractors/bytecode_extract.py::extract_bytecode_table()`

```python
def extract_bytecode_table(
    *,
    repo_files: pa.Table,
    session: ExtractSession,
    profile: str = "default",
    prefer_reader: bool = False,
) -> pa.Table | pa.RecordBatchReader
```

**Output:** `bytecode_files_v1` - Bytecode instruction table.

**Extraction process:**
1. Compile file with `compile()` → code object
2. Disassemble with `dis.get_instructions()`
3. Extract per-instruction rows:
   - Opcode name and numeric code
   - Argument value
   - Jump targets
   - Line number anchoring
4. Compute control flow edges (jumps, branches)

**Error handling:** Compilation failures produce empty rows with error annotations.

### Symtable Extractor

**Purpose:** Extract Python symbol table information using `symtable` module for scope and binding analysis.

**Public API:** `src/extract/extractors/symtable_extract.py::extract_symtables_table()`

```python
def extract_symtables_table(
    *,
    repo_files: pa.Table,
    session: ExtractSession,
    profile: str = "default",
    prefer_reader: bool = False,
) -> pa.Table | pa.RecordBatchReader
```

**Output:** `symtable_files_v1` - Symbol table with scopes and symbols.

**Extraction process:**
1. Compile file with `symtable.symtable()`
2. Walk symbol table hierarchy (module → class → function)
3. Extract scope-level metadata:
   - Scope type (module, class, function)
   - Scope name and nesting level
   - Namespace kind (global, local, enclosing)
4. Extract symbol-level metadata:
   - Symbol name
   - Binding kind (global, local, free, cell)
   - Usage flags (referenced, assigned, parameter)

**Error handling:** Symtable build failures produce empty rows.

### Tree-sitter Extractor

**Purpose:** Parse Python source with tree-sitter for fast syntax tree extraction and structural pattern matching.

**Public API:** `src/extract/extractors/tree_sitter/extract.py::extract_ts_tables()`

```python
def extract_ts_tables(
    *,
    repo_files: pa.Table,
    session: ExtractSession,
    profile: str = "default",
    prefer_reader: bool = False,
) -> dict[str, pa.Table | pa.RecordBatchReader]
```

**Output:** `tree_sitter_files` - Tree-sitter parse table with symbols and imports.

**Enabled by:** `tree_sitter_enabled` option (default: `True`).

**Extraction process:**
1. Parse file with tree-sitter Python grammar
2. Query tree-sitter CST for structural patterns:
   - Function definitions
   - Class definitions
   - Import statements
   - Expression patterns
3. Emit rows with byte-span anchoring from tree-sitter node ranges

**Span conventions:** Tree-sitter provides byte offsets directly; no line-to-byte conversion required.

### SCIP Extractor

**Purpose:** Extract SCIP (SCIP Code Intelligence Protocol) index data from pre-built SCIP index files.

**Public API:** `src/extract/extractors/scip/extract.py::extract_scip_tables()`

```python
def extract_scip_tables(
    *,
    context: ScipExtractContext,
    options: ScipExtractOptions,
    prefer_reader: bool = False,
) -> dict[str, pa.Table | pa.RecordBatchReader]
```

**Output:** `scip_index` - SCIP occurrence and diagnostic table.

**Enabled by:** Providing `scip_index_config` with `index_path_override` or `scip_index_path`.

**Extraction process:**
1. Load SCIP index from specified path
2. Parse SCIP protobuf or JSON format
3. Extract occurrences (symbol usages, definitions)
4. Extract diagnostics (errors, warnings)
5. Map SCIP positions to byte spans

**Context:** `ScipExtractContext` provides:
- `scip_index_path: str | None` - Path to SCIP index file
- `repo_root: str` - Repository root for path resolution
- `session: ExtractSession`
- `runtime_spec: RuntimeProfileSpec`

## Stage 2: Import Combiner

### Purpose

Combine import statements extracted from AST, LibCST, and tree-sitter into a unified, deduplicated `python_imports` table. Normalizes import syntax and resolves import targets.

### Public API

**Function:** `src/extract/extractors/imports_extract.py::extract_python_imports_tables()`

```python
def extract_python_imports_tables(
    *,
    ast_imports: pa.Table | None,
    cst_imports: pa.Table | None,
    ts_imports: pa.Table | None,
    session: ExtractSession,
    profile: str = "default",
    prefer_reader: bool = False,
) -> dict[str, pa.Table | pa.RecordBatchReader]
```

**Inputs:**
- `ast_imports` - Import rows from `ast_files` (loaded from Delta location)
- `cst_imports` - Import rows from `libcst_files`
- `ts_imports` - Import rows from `tree_sitter_files`

**Output:** `python_imports` - Deduplicated import table.

### Extraction Process

1. **Load inputs from Delta locations:**
   - Read `ast_files`, `libcst_files`, `tree_sitter_files` from Stage 1 Delta output
   - Extract import-related columns from each table

2. **Combine imports:**
   - Union import rows from all sources
   - Normalize import syntax (ImportFrom → module + name, Import → module)

3. **Deduplicate:**
   - Group by (`file_id`, `module`, `name`, `alias`)
   - Prefer LibCST imports (highest fidelity) > AST > tree-sitter

4. **Emit unified table:**
   - Columns: `file_id`, `module`, `name`, `alias`, `level`, `bstart`, `bend`

**Error handling:** Missing input tables are treated as empty; partial failures produce partial output.

## Stage 3: External Interface Discovery

### Purpose

Resolve external package symbols referenced by imports, building a catalog of external interfaces for cross-repository reference resolution.

### Public API

**Function:** `src/extract/extractors/external_scope.py::extract_python_external_tables()`

```python
def extract_python_external_tables(
    *,
    python_imports: pa.Table,
    repo_root: str,
    session: ExtractSession,
    profile: str = "default",
    prefer_reader: bool = False,
) -> dict[str, pa.Table | pa.RecordBatchReader]
```

**Inputs:**
- `python_imports` - Import table from Stage 2 (loaded from Delta location)
- `repo_root` - Repository root for module resolution

**Output:** `python_external_interfaces` - External symbol catalog.

### Extraction Process

1. **Load python_imports table:**
   - Read `python_imports` from Stage 2 Delta output
   - Extract distinct module names

2. **Resolve external modules:**
   - Classify imports as internal (repo-local) vs. external (third-party)
   - For external imports, attempt symbol resolution via:
     - Installed package introspection (when available)
     - Stub file discovery (`.pyi` files)
     - Type annotation extraction

3. **Build interface catalog:**
   - Columns: `module`, `symbol`, `kind` (function, class, constant), `signature`
   - Includes docstring and type annotation metadata

**Error handling:** Resolution failures for individual modules are logged but do not block catalog construction.

## Extraction Output Contract

### Delta Table Locations

All extractors write outputs to Delta tables at:
```
{work_dir}/extract/{dataset_name}/
```

**Example:**
```
.codeanatomy/extract/
├── repo_files_v1/
├── file_line_index_v1/
├── ast_files/
├── libcst_files/
├── bytecode_files_v1/
├── symtable_files_v1/
├── tree_sitter_files/
├── scip_index/
├── python_imports/
└── python_external_interfaces/
```

### Semantic Input Mapping

**Function:** `src/extraction/contracts.py::resolve_semantic_input_locations()`

Maps canonical semantic input names (expected by semantic compiler) to concrete extraction dataset names.

**Mapping table (`_SEMANTIC_INPUT_CANDIDATES`):**

| Semantic Input | Candidates (priority order) |
|----------------|----------------------------|
| `cst_refs` | `cst_refs`, `libcst_files`, `libcst_files_v1` |
| `cst_defs` | `cst_defs`, `libcst_files`, `libcst_files_v1` |
| `cst_imports` | `cst_imports`, `libcst_files`, `libcst_files_v1` |
| `cst_callsites` | `cst_callsites`, `libcst_files`, `libcst_files_v1` |
| `cst_docstrings` | `cst_docstrings`, `libcst_files`, `libcst_files_v1` |
| `scip_occurrences` | `scip_occurrences`, `scip_index`, `scip_index_v1` |
| `scip_diagnostics` | `scip_diagnostics`, `scip_index`, `scip_index_v1` |
| `symtable_scopes` | `symtable_scopes`, `symtable_files_v1`, `symtable_files` |
| `symtable_symbols` | `symtable_symbols`, `symtable_files_v1`, `symtable_files` |

**Compatibility aliases (`_COMPAT_ALIASES`):**

Legacy dataset names are aliased to current names for backward compatibility:

| Alias | Canonical |
|-------|-----------|
| `repo_files` | `repo_files_v1` |
| `ast_imports` | `ast_files` |
| `cst_imports` | `libcst_files` |
| `ts_imports` | `tree_sitter_files` |
| `symtable_files` | `symtable_files_v1` |
| `bytecode_files` | `bytecode_files_v1` |
| `python_external` | `python_external_interfaces` |

**Function:** `src/extraction/contracts.py::with_compat_aliases()`

Extends `delta_locations` with compatibility aliases for legacy callers.

### Extraction Result

**Contract:** `src/extraction/orchestrator.py::ExtractionResult` (msgspec.Struct, frozen=True)

```python
class ExtractionResult(msgspec.Struct, frozen=True):
    delta_locations: dict[str, str]
    semantic_input_locations: dict[str, str]
    errors: list[dict[str, object]]
    timing: dict[str, float]
```

**Fields:**
- `delta_locations` - All extraction dataset names → Delta table paths (includes compat aliases)
- `semantic_input_locations` - Canonical semantic input names → resolved Delta table paths
- `errors` - Per-extractor error records: `{"extractor": str, "error": str}`
- `timing` - Stage-level timing: `{"repo_scan": float, "ast_files": float, ...}`

## Parallel Execution

### Stage 1 Parallelism

**Mechanism:** `ThreadPoolExecutor` with configurable `max_workers` (default: 6).

**Rationale:**
- Extractors are I/O-bound (file reading) and GIL-constrained (Python parsing)
- Thread pool avoids process spawn overhead
- Concurrent extraction improves wall-clock time by ~3-5x on multi-core systems

**Worker limit:** `max_workers` parameter controls thread pool size. Higher values improve throughput for large repositories but increase memory pressure.

**Error isolation:** Each extractor executes in a separate thread. Exceptions are caught per-future and recorded in `errors` list without killing other extractors.

### Extractor-Internal Parallelism

Individual extractors may use internal parallelism for file-level processing:

**AST/LibCST extractors:**
- Use `parallel_map()` helper from `src/extract/infrastructure/parallel.py`
- Spawn worker processes (multiprocessing `spawn` context) for per-file parsing
- Respect `max_workers` option for extractor-specific worker pools

**Error handling:** Individual file parse failures are recorded in per-file error columns; failed files produce empty symbol rows.

## Design Invariants

### Byte-Span Anchoring

**Invariant:** All extractors emit byte-based spans (`bstart`, `bend`) as the canonical location representation.

**Rationale:** Line/column positions are ambiguous (encoding-dependent, newline-convention-dependent). Byte offsets are deterministic and stable across parsers.

**Normalization:** Extractors may compute line/column positions internally (AST, LibCST) but **must** convert to byte offsets via `FileContext` and `SpanSpec` before emitting rows.

**Span utilities:**
- `src/extract/coordination/context.py::SpanSpec` - Span specification with byte and line/column fields
- `src/extract/coordination/line_offsets.py::LineOffsets` - Line-to-byte mapping

### Graceful Degradation

**Invariant:** Individual extractor failures do not block pipeline completion. Partial outputs are valid.

**Failure modes:**
1. **Stage 0 git scan failure:** Falls back to filesystem scan
2. **Stage 1 extractor failure:** Logs error, produces empty output, continues other extractors
3. **Individual file parse failure:** Produces empty symbol rows for failed file, continues other files

**Error recording:** All errors are captured in `ExtractionResult.errors` with `{"extractor": str, "error": str}` structure.

**Partial outputs:** Missing optional inputs (e.g., `tree_sitter_files` when disabled) produce valid empty tables with correct schema.

### Schema Normalization

**Invariant:** All extractor outputs conform to pre-registered Arrow schemas from `src/datafusion_engine/extract/registry.py`.

**Schema enforcement:**
- `dataset_schema(name: str)` returns canonical schema for dataset name
- Extractors **must** produce tables matching canonical schema
- Schema mismatches raise `ValueError` during Delta write

**Schema evolution:** Schema changes require coordinated updates to:
1. Registry schema definition (`src/datafusion_engine/extract/registry.py`)
2. Extractor emission logic
3. Semantic compiler input expectations

### Determinism

**Invariant:** Extraction is deterministic. Given identical source files and configuration, extractors produce byte-identical outputs.

**Enforcement mechanisms:**
- Stable file ordering (lexicographic path sort)
- Deterministic hashing for `file_id` generation
- No timestamp dependencies in output rows
- Fixed random seeds for any non-deterministic operations (none currently)

**Non-deterministic metadata:** File modification times (`mtime_ns`) and absolute paths (`abs_path`) are included for traceability but excluded from semantic analysis.

## Extraction Options

### ExtractionRunOptions

**Contract:** `src/extraction/options.py::ExtractionRunOptions` (msgspec.Struct, frozen=True)

```python
class ExtractionRunOptions(msgspec.Struct, frozen=True):
    include_globs: tuple[str, ...] = ()
    exclude_globs: tuple[str, ...] = ()
    include_untracked: bool = True
    include_submodules: bool = False
    include_worktrees: bool = False
    follow_symlinks: bool = False
    tree_sitter_enabled: bool = True
    max_workers: int = 6
    diff_base_ref: str | None = None
    diff_head_ref: str | None = None
    changed_only: bool = False
```

**Option semantics:**

| Option | Description |
|--------|-------------|
| `include_globs` | Glob patterns for included files (default: all Python files) |
| `exclude_globs` | Glob patterns for excluded files (applied after includes) |
| `include_untracked` | Include untracked files in git repositories |
| `include_submodules` | Include files from git submodules |
| `include_worktrees` | Include files from git worktrees |
| `follow_symlinks` | Follow symlinks during filesystem scan |
| `tree_sitter_enabled` | Enable tree-sitter extractor (disable to skip) |
| `max_workers` | Thread pool size for Stage 1 parallelism |
| `diff_base_ref` | Git base reference for incremental extraction |
| `diff_head_ref` | Git head reference for incremental extraction |
| `changed_only` | Restrict extraction to files changed between refs |

**Incremental extraction:**
- Requires both `diff_base_ref` and `diff_head_ref` when `changed_only=True`
- Validation: `normalize_extraction_options()` enforces ref requirement

**Normalization:** `src/extraction/options.py::normalize_extraction_options()` accepts:
- `ExtractionRunOptions` (passthrough)
- `Mapping[str, object]` (coerced to struct)
- `None` (default options)

**Compatibility mapping:**
- `enable_tree_sitter` → `tree_sitter_enabled`
- `git_base_ref` → `diff_base_ref`
- `git_head_ref` → `diff_head_ref`
- `git_changed_only` → `changed_only`

## Cross-References

### Related Architecture Docs

- **01_overview.md** - High-level pipeline architecture and stage sequencing
- **03_semantic_compiler.md** - Semantic normalization and relationship compilation (consumes extraction outputs)
- **04_boundary_contract.md** - CPG output contracts and schema definitions
- **08_storage.md** - Delta Lake persistence and table versioning

### Key Source Modules

- `src/extraction/orchestrator.py` - Extraction orchestration and stage sequencing
- `src/extraction/options.py` - Option contracts and normalization
- `src/extraction/contracts.py` - Output contract mapping and compatibility aliases
- `src/extract/coordination/context.py` - FileContext, SpanSpec, ExtractExecutionContext
- `src/extract/coordination/materialization.py` - Plan materialization and Delta write
- `src/extract/scanning/repo_scan.py` - Repository scanning (Stage 0)
- `src/extract/extractors/ast_extract.py` - AST extractor
- `src/extract/extractors/cst_extract.py` - LibCST extractor
- `src/extract/extractors/bytecode_extract.py` - Bytecode extractor
- `src/extract/extractors/symtable_extract.py` - Symtable extractor
- `src/extract/extractors/tree_sitter/extract.py` - Tree-sitter extractor
- `src/extract/extractors/scip/extract.py` - SCIP extractor
- `src/extract/extractors/imports_extract.py` - Import combiner (Stage 2)
- `src/extract/extractors/external_scope.py` - External interface discovery (Stage 3)

### External Dependencies

- **Python stdlib:** `ast`, `symtable`, `dis`, `compile`
- **LibCST:** Concrete syntax tree parsing and metadata providers
- **tree-sitter:** Fast incremental parsing
- **PyArrow:** Table representation and schema enforcement
- **Delta Lake:** Table persistence and versioning
- **pygit2:** Git repository operations
