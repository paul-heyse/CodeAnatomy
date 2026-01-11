# Implementation Plan: Architecture Alignment and Evidence Layer Completion

This plan operationalizes the agreed shortfalls into concrete work items. Each scope item includes
an implementation description, code patterns, target file list, and a checklist.

## Scope 1: Make `kinds_ultimate` Canonical in Runtime Emission

### Description
Unify runtime emission and validation against `cpg.kinds_ultimate` so edge and node kinds are
single-source-of-truth. Eliminate drift between `cpg.kinds` and `cpg.kinds_ultimate`, and align
the call edge name (`PY_CALLS_QNAME`) across emitters, validators, and contracts.

### Code patterns
```python
# src/cpg/kinds.py (thin re-export)
from __future__ import annotations

from cpg.kinds_ultimate import EdgeKind, NodeKind

SCIP_ROLE_DEFINITION = 1
SCIP_ROLE_IMPORT = 2
SCIP_ROLE_WRITE = 4
SCIP_ROLE_READ = 8

__all__ = [
    "EdgeKind",
    "NodeKind",
    "SCIP_ROLE_DEFINITION",
    "SCIP_ROLE_IMPORT",
    "SCIP_ROLE_WRITE",
    "SCIP_ROLE_READ",
]
```

```python
# src/cpg/build_edges.py (edge emission uses canonical enum)
EdgeEmitSpec(
    edge_kind=EdgeKind.PY_CALLS_QNAME,
    src_col="call_id",
    dst_col="qname_id",
    path_col="path",
    bstart_col=bstart_col,
    bend_col=bend_col,
    origin="qnp",
    default_resolution_method="QNP_CALLEE_FALLBACK",
)
```

### Target files
- `src/cpg/kinds.py`
- `src/cpg/build_edges.py`
- `src/cpg/build_nodes.py`
- `src/cpg/build_props.py`
- `src/cpg/__init__.py`
- `src/relspec/edge_contract_validator.py`

### Implementation checklist
- [ ] Replace direct imports of `cpg.kinds` in emitters with `cpg.kinds_ultimate`.
- [ ] Re-export canonical kinds from `cpg.kinds` to preserve compatibility.
- [ ] Align call edge kind to `PY_CALLS_QNAME` in emitters and edge validation mapping.
- [ ] Confirm `EDGE_KIND_CONTRACTS` coverage matches emitted kinds.

## Scope 2: Make `DerivationSpec.extractor` Import-Resolvable

### Description
Ensure every `DerivationSpec.extractor` string is a real, importable callable reference for
implemented derivations. Canonicalize on real module paths (no alias namespace) and add a resolver
that parses `module:function` and validates imports at startup, with clear errors if any extractor
is missing. Add an optional alias package only if backwards compatibility is required for legacy
strings.

### Code patterns
```python
# src/cpg/kinds_ultimate.py
from dataclasses import dataclass
import importlib

@dataclass(frozen=True)
class CallableRef:
    module: str
    func: str

    def resolve(self) -> object:
        mod = importlib.import_module(self.module)
        return getattr(mod, self.func)

def parse_extractor(value: str) -> CallableRef:
    module, func = value.split(":", 1)
    return CallableRef(module=module, func=func)

def validate_derivation_extractors(*, allow_planned: bool = False) -> None:
    for spec in _iter_derivation_specs(allow_planned=allow_planned):
        ref = parse_extractor(spec.extractor)
        ref.resolve()
```

### Target files
- `src/cpg/kinds_ultimate.py`
- `src/cpg/__init__.py` (export validation helper)
- `docs/architecture/Architecture_Summary.md` (update to canonical import namespace if needed)

### Implementation checklist
- [ ] Normalize `extractor` strings to real module paths and treat them as canonical.
- [ ] Add a resolver with importlib-based validation.
- [ ] Add an alias package only if legacy `codeintel_cpg.*` strings must be supported.
- [ ] Decide whether planned derivations are validated in CI or only in dev.
- [ ] Wire validation into a lightweight startup check or registry validation.

## Scope 3: Wire AST Byte-Span Normalization into Hamilton

### Description
AST nodes currently carry line/col only. Add Hamilton normalization to convert AST spans to
byte offsets using `RepoTextIndex`, producing join-ready `bstart/bend` columns.

### Code patterns
```python
# src/hamilton_pipeline/modules/normalization.py
@cache()
@tag(layer="normalize", artifact="ast_nodes_norm", kind="table")
def ast_nodes_norm(
    repo_text_index: RepoTextIndex,
    ast_nodes: pa.Table,
    ctx: ExecutionContext,
) -> pa.Table:
    _ = ctx
    return add_ast_byte_spans(repo_text_index, ast_nodes)
```

### Target files
- `src/hamilton_pipeline/modules/normalization.py`
- `src/normalize/spans.py`
- `src/normalize/__init__.py`

### Implementation checklist
- [ ] Add `ast_nodes_norm` node using `add_ast_byte_spans`.
- [ ] Ensure AST normalization bundles use the normalized table downstream.
- [ ] Update any join rules that expect `bstart/bend` for AST-derived data.

## Scope 4: Bytecode Anchor, CFG, and DFG Normalization

### Description
Implement join-ready bytecode normalization: map instruction positions to byte spans, build CFG
blocks/edges, and emit DFG def/use + reaching-def edges. Ensure deterministic ordering, stable IDs,
and contract compliance.

### Code patterns
```python
# src/normalize/bytecode_anchor.py
def anchor_instructions(
    repo_index: RepoTextIndex,
    py_bc_instructions: pa.Table,
) -> pa.Table:
    spans = _compute_byte_spans(repo_index, py_bc_instructions)
    return (
        py_bc_instructions.append_column("bstart", pa.array(spans.bstart, type=pa.int64()))
        .append_column("bend", pa.array(spans.bend, type=pa.int64()))
    )
```

```python
# src/normalize/bytecode_cfg.py
def build_cfg(py_bc_blocks: pa.Table, py_bc_cfg_edges: pa.Table) -> pa.Table:
    cfg = _normalize_block_edges(py_bc_cfg_edges)
    return canonical_sort(cfg, sort_keys=_cfg_sort_keys())
```

### Target files
- `src/normalize/bytecode_anchor.py` (new)
- `src/normalize/bytecode_cfg.py` (new)
- `src/normalize/bytecode_dfg.py` (new)
- `src/hamilton_pipeline/modules/normalization.py`
- `src/cpg/kinds_ultimate.py` (mark derivations implemented)

### Implementation checklist
- [ ] Add instruction anchoring with `RepoTextIndex`.
- [ ] Implement CFG node/edge normalization with stable IDs.
- [ ] Implement DFG def/use and reaching-def edges with deterministic sort + dedupe.
- [ ] Wire outputs into Hamilton normalization layer.

## Scope 5: Tree-sitter Extraction Layer (CST + Diagnostics)

### Description
Implement `tree-sitter` extraction to produce CST nodes and diagnostic nodes with byte spans.
Use the 0.25+ API, parse bytes, and capture `ERROR`/`MISSING` nodes for diagnostics.

### Code patterns
```python
# src/extract/tree_sitter_extract.py
from tree_sitter import Language, Parser, Query, QueryCursor
import tree_sitter_python as tspython

def _parser() -> Parser:
    language = Language(tspython.language())
    return Parser(language)

def _error_query(language: Language) -> Query:
    return Query(language, "(ERROR) @error (MISSING) @missing")

def extract_ts_tables(repo_root: str, repo_files: pa.Table) -> dict[str, pa.Table]:
    parser = _parser()
    query = _error_query(parser.language)
    cursor = QueryCursor(query)
    # Parse bytes, walk nodes, emit ts_nodes/ts_errors/ts_missing tables.
```

### Target files
- `src/extract/tree_sitter_extract.py` (new)
- `src/extract/__init__.py`
- `src/hamilton_pipeline/modules/extraction.py`
- `pyproject.toml` (tree-sitter deps)

### Implementation checklist
- [ ] Implement byte-based parsing with `Parser(Language(...))`.
- [ ] Emit node table with `start_byte/end_byte`, `kind`, `is_error`, `is_missing`.
- [ ] Emit diagnostics table from `(ERROR)/(MISSING)` captures.
- [ ] Add Hamilton extraction node gated by config.

## Scope 6: Runtime Inspect Overlay (Sandboxed, Optional)

### Description
Implement runtime inspection in a subprocess with strict allowlisting and timeouts. Produce runtime
objects, signatures, parameters, and member edges with clear provenance.

### Code patterns
```python
# src/extract/runtime_inspect_extract.py
def extract_runtime_objects(
    repo_root: str,
    *,
    module_allowlist: Sequence[str],
    timeout_s: int,
) -> pa.Table:
    result = _run_inspect_subprocess(repo_root, module_allowlist, timeout_s)
    return pa.Table.from_pylist(result.objects)
```

### Target files
- `src/extract/runtime_inspect_extract.py` (new)
- `src/hamilton_pipeline/modules/extraction.py`
- `src/hamilton_pipeline/pipeline_types.py` (config for allowlist/timeouts)
- `src/cpg/kinds_ultimate.py`

### Implementation checklist
- [ ] Implement a subprocess runner with isolated env and timeouts.
- [ ] Enforce allowlist and disable unsafe execution paths.
- [ ] Emit runtime tables and edge datasets with `SourceKind.INSPECT`.
- [ ] Gate in Hamilton with `@config.when(enable_runtime_inspect=True)`.

## Scope 7: Type Normalization Layer

### Description
Implement a best-effort type layer that prefers SCIP, falls back to LibCST type inference (Pyre),
and then annotation parsing. Emit `type_exprs`, `types`, and type edges.

### Code patterns
```python
# src/normalize/types.py
def normalize_types(
    scip_symbol_information: pa.Table | None,
    cst_defs: pa.Table | None,
    *,
    ctx: ExecutionContext,
) -> pa.Table:
    if scip_symbol_information is not None:
        return _types_from_scip(scip_symbol_information)
    return _types_from_annotations(cst_defs)
```

### Target files
- `src/normalize/types.py` (new)
- `src/hamilton_pipeline/modules/normalization.py`
- `src/cpg/kinds_ultimate.py`
- `src/relspec/` (type relationship rules)

### Implementation checklist
- [ ] Implement SCIP-first extraction of type facts.
- [ ] Add LibCST/Pyre integration behind config gate.
- [ ] Emit type edges and validate against contracts.
- [ ] Wire into Hamilton normalization and relspec rules.

## Scope 8: Diagnostics Normalization Layer

### Description
Aggregate diagnostics across LibCST parse errors, tree-sitter errors, and SCIP diagnostics into a
single normalized diagnostics dataset and edge emission (`HAS_DIAGNOSTIC`).

### Code patterns
```python
# src/normalize/diagnostics.py
def collect_diags(
    *,
    cst_parse_errors: pa.Table | None,
    ts_errors: pa.Table | None,
    scip_diagnostics: pa.Table | None,
) -> pa.Table:
    tables = [t for t in (cst_parse_errors, ts_errors, scip_diagnostics) if t is not None]
    if not tables:
        return pa.Table.from_pylist([], schema=DIAG_SCHEMA)
    return pa.concat_tables(tables, promote=True)
```

### Target files
- `src/normalize/diagnostics.py` (new)
- `src/hamilton_pipeline/modules/normalization.py`
- `src/cpg/build_edges.py` (diagnostic edges if needed)
- `src/cpg/kinds_ultimate.py`

### Implementation checklist
- [ ] Normalize diagnostic schemas to a shared contract.
- [ ] Emit `HAS_DIAGNOSTIC` edges with stable IDs and provenance.
- [ ] Wire diagnostics into Hamilton normalization and output bundles.

## Scope 9: Hamilton Wiring + Registry Validation

### Description
Integrate new extraction/normalization nodes with Hamilton, and validate registry completeness and
derivation extractors during driver build or pipeline startup.

### Code patterns
```python
# src/hamilton_pipeline/driver_factory.py
def build_driver(...):
    validate_registry_completeness()
    validate_derivation_extractors(allow_planned=False)
    ...
```

### Target files
- `src/hamilton_pipeline/driver_factory.py`
- `src/hamilton_pipeline/modules/extraction.py`
- `src/hamilton_pipeline/modules/normalization.py`
- `src/cpg/kinds_ultimate.py`

### Implementation checklist
- [ ] Add new Hamilton nodes (extraction + normalization) with tags and caching.
- [ ] Gate optional layers via `@config.when`.
- [ ] Call registry + derivation validation during driver build.
- [ ] Verify bundles include new datasets for manifest/run bundle snapshots.

## Quality Gates and Acceptance Checks

### Local gates
- `uv run ruff check --fix`
- `uv run pyrefly check`
- `uv run pyright --warnings --pythonversion=3.14`

### Acceptance criteria
- All emitted node/edge kinds align with `kinds_ultimate`.
- Every implemented `DerivationSpec.extractor` resolves to a callable.
- AST and bytecode evidence are join-ready via byte spans.
- Tree-sitter and runtime inspect layers are optional but functional behind config gates.
- Diagnostics and type layers produce contract-validated outputs.
