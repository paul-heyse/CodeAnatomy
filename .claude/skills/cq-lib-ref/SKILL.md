---
name: cq-lib-ref
description: Library reference manual for tools/cq dependencies. Covers tree-sitter (core + Python/Rust grammars), ast-grep-py, msgspec, diskcache, cyclopts, pathspec, rich, pygit2, uuid6, and rustworkx. Use lookup + local probes; do not guess APIs.
allowed-tools: Read, Grep, Glob, Bash
---

## Operating rule: never guess library APIs used by tools/cq

When uncertain about any library API used in `tools/cq`:
1) Probe local environment (versions + available methods).
2) Search `tools/cq/` for how we already use it.
3) Open the relevant reference file below (only the section you need).
4) Implement using *existing local patterns* unless the plan says otherwise.

## Library inventory (tools/cq)

| Package | Import | Role in CQ |
|---------|--------|------------|
| `tree-sitter` | `tree_sitter` | Core parse/query runtime: `Parser`, `Language`, `Node`, `Tree`, `Query`, `QueryCursor`, `Point`, `Range` |
| `tree-sitter-python` | `tree_sitter_python` | Python grammar bundle (language object) |
| `tree-sitter-rust` | `tree_sitter_rust` | Rust grammar bundle (language object) |
| `ast-grep-py` | `ast_grep_py` | Structural AST pattern matching (SgRoot, SgNode, metavariables) |
| `msgspec` | `msgspec` | Fast serialization: `Struct` (frozen contracts), `msgpack` encoder/decoder, JSON codec |
| `diskcache` | `diskcache` | Disk-backed cache: `FanoutCache`, `Lock`, `RLock`, `BoundedSemaphore`, `Deque`, `Index`, `memoize_stampede`, `transact` |
| `cyclopts` | `cyclopts` | CLI framework: `App`, commands, parameter types, validators |
| `pathspec` | `pathspec` | Gitignore-style path matching: `PathSpec.from_lines()` |
| `rich` | `rich` | Terminal rendering: `Console`, `Table`, `Panel`, `Syntax`, `Markdown`, `Tree` |
| `pygit2` | `pygit2` | Git repository access: `Repository`, `discover_repository`, diff, blame |
| `uuid6` | `uuid6` | UUID generation: `uuid7()`, `uuid6()`, `uuid1_to_uuid6()` |
| `rustworkx` | `rustworkx` | Graph structures and algorithms (DAG, shortest path, topological sort) |

## CQ contract conventions

- Use `msgspec.Struct` (frozen) for serialized contracts crossing module boundaries.
- Keep parser/cache handles as runtime-only objects; never serialize them.
- Avoid `pydantic` in CQ hot paths (`tools/cq/search`, `tools/cq/query`, `tools/cq/run`).
- Use `diskcache` native coordination primitives (`Lock`, `RLock`, `BoundedSemaphore`, `barrier`) before bespoke implementations.
- Cache identity is deterministic and fingerprint-based; UUIDs are never part of cache keys.

## Reference map (open these files as needed)

### tree-sitter (primary structural plane)
- Core runtime (Parser, Language, Node, Tree, Query, QueryCursor, Point, Range): reference/tree-sitter.md
- Advanced (incremental parse, `old_tree`/`tree.edit`/`changed_ranges`, `included_ranges`, query cursor bounds, `progress_callback`, `match_limit`, `did_exceed_match_limit`, `set_max_start_depth`, `set_byte_range`/`set_point_range`, `pattern_settings`/`pattern_assertions`, `is_pattern_rooted`/`is_pattern_non_local`, `capture_quantifier`, `next_state`/`lookahead_iterator`, `named_descendant_for_point_range`/`named_descendant_for_byte_range`, field-aware navigation): reference/tree-sitter_advanced.md
- Output structures (node-types.json ABI, grammar schema contracts, supertype/subtype, field definitions): reference/tree-sitter_outputs_overview.md
- Neighborhood creation (tree-sitter-driven neighborhood assembly, structural slices, anchor resolution): reference/tree_sitter_neighborhood_creation.md
- Rust grammar bundle (tree-sitter.json, queries/*.scm, src/node-types.json, macro-aware extraction, tags/injections packs): reference/tree-sitter-rust.md

### ast-grep-py (structural pattern matching)
- Python bindings (SgRoot, SgNode, pattern matching, rule objects, metavariables, find/findAll, language support): reference/ast-grep-py.md
- Rust deep dive (ast-grep-core internals, pattern compilation, rule evaluation, NAPI/PyO3 bridge): reference/ast-grep-py_rust_deepdive.md

### msgspec (serialization)
- Struct definitions, frozen structs, field defaults, JSON/msgpack encode/decode, schema generation, validation, type coercion, tagged unions, gc-free structs: reference/msgspec.md

### diskcache (caching + coordination)
- FanoutCache, Cache, transact, stats, expire/cull/check, Lock/RLock/BoundedSemaphore, barrier, memoize_stampede, Deque, Index, read/write file handles, tag-based eviction, size limits: reference/diskcache.md

### cyclopts (CLI framework)
- App, command registration, parameter types, validators, groups, help generation, meta parameters: reference/cyclopts.md

### pathspec (path matching)
- PathSpec.from_lines(), gitignore pattern syntax, match_file/match_files, pattern types: reference/pathspec.md

### rich (terminal rendering)
- Console, Table, Panel, Syntax, Markdown, Tree, progress bars, live display, themes, markup: reference/rich.md

### pygit2 (git access)
- Repository, discover_repository, diff, blame, Index, references, revparse, TreeBuilder, commit/tree walking: reference/pygit2.md

### uuid6 (UUID generation)
- uuid7() sortable IDs, uuid6() v6 generation, uuid1_to_uuid6() legacy conversion, time-based ordering, version detection: reference/uuid6.md

### rustworkx (graph algorithms)
- PyDiGraph, PyGraph, DAG operations, topological_sort, shortest_path, adjacency, node/edge data, visualization: reference/rustworkx.md

## Quick version probe commands

```bash
# Check installed versions of all CQ dependencies
uv run python -c "
import tree_sitter; print(f'tree-sitter: {tree_sitter.__version__}')
import ast_grep_py; print(f'ast-grep-py: {ast_grep_py.__version__}')
import msgspec; print(f'msgspec: {msgspec.__version__}')
import diskcache; print(f'diskcache: {diskcache.__version__}')
import cyclopts; print(f'cyclopts: {cyclopts.__version__}')
import pathspec; print(f'pathspec: {pathspec.__version__}')
import rich; print(f'rich: {rich.__version__}')
import pygit2; print(f'pygit2: {pygit2.__version__}')
import rustworkx; print(f'rustworkx: {rustworkx.__version__}')
"

# Check tree-sitter grammar versions
uv run python -c "
import tree_sitter_python; print(f'tree-sitter-python: {tree_sitter_python.__version__}')
import tree_sitter_rust; print(f'tree-sitter-rust: {tree_sitter_rust.__version__}')
"

# Check uuid6 availability
uv run python -c "
try:
    import uuid6; print(f'uuid6: {uuid6.__version__}')
except ImportError:
    import uuid; print(f'uuid (stdlib): uuid7={hasattr(uuid, \"uuid7\")}')
"
```

## When to open which reference

| Task | Reference file |
|------|---------------|
| Parse a file, walk nodes, query captures | reference/tree-sitter.md |
| Incremental parse, query cursor bounds, progress callback, pattern metadata | reference/tree-sitter_advanced.md |
| node-types.json schema, grammar ABI contracts | reference/tree-sitter_outputs_overview.md |
| Build neighborhood slices from tree-sitter | reference/tree_sitter_neighborhood_creation.md |
| Rust grammar, macro expansion, injection/tags packs | reference/tree-sitter-rust.md |
| Structural pattern match with SgRoot/SgNode | reference/ast-grep-py.md |
| ast-grep internal compilation, rule evaluation | reference/ast-grep-py_rust_deepdive.md |
| Define frozen Struct contracts, encode/decode msgpack/JSON | reference/msgspec.md |
| Cache operations, locking, coordination, stampede protection | reference/diskcache.md |
| CLI commands, parameter parsing, help text | reference/cyclopts.md |
| Gitignore-style path filtering | reference/pathspec.md |
| Terminal tables, syntax highlighting, panels, markdown rendering | reference/rich.md |
| Git repo access, diffs, blame, tree walking | reference/pygit2.md |
| UUID generation, v7 sortable IDs, v1-to-v6 conversion | reference/uuid6.md |
| Graph construction, topological sort, shortest path | reference/rustworkx.md |
