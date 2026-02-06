# AGENTS.md

Agent operating protocol for the CodeAnatomy codebase.

---

## 1. Read This First

**Non-negotiables:**

- **Always `uv run`** - Direct `python` calls will fail due to import issues
- **CQ exception** - Use `./cq` or `/cq` for the CQ tool; use `uv run` for all other Python invocations
- **Python 3.13.12** - Pinned, do not change
- **Bootstrap first** - Run `scripts/bootstrap_codex.sh && uv sync` on fresh clones
- **Ruff is canonical** - All style rules live in `pyproject.toml`; do not duplicate
- **pyrefly is the strict gate** - pyright runs in "basic" mode for IDE support

**Project:** CodeAnatomy is an inference-driven Code Property Graph (CPG) builder. It extracts
evidence from Python source (LibCST, AST, symtable, bytecode, SCIP, tree-sitter) and compiles
to a queryable graph using Hamilton DAG + DataFusion.

---

## 2. Skills (Use Before Guessing)

**CRITICAL:** These skills exist to prevent incorrect assumptions. Use them proactively.

| Skill | Trigger | Command |
|-------|---------|---------|
| `/cq search` | Quick identifier search | `/cq search build_graph` |
| `/cq search` | Regex code search | `/cq search "config.*path" --regex` |
| `/cq search` | Include strings/comments | `/cq search foo --include-strings` |
| `/cq search` | Scoped search | `/cq search CqResult --in tools/cq/core/` |
| `/cq` | Modifying any function/class | `/cq calls <function>` |
| `/cq` | Changing parameters | `/cq impact <function> --param <param>` |
| `/cq` | Signature changes | `/cq sig-impact <function> --to "<new>"` |
| `/cq q` | Finding entities by pattern | `/cq q "entity=function name=~pattern"` |
| `/cq q` | Understanding imports | `/cq q "entity=import in=<dir>"` |
| `/cq q "pattern=..."` | Structural code search | `/cq q "pattern='getattr(\$X, \$Y)'"` |
| `/cq q "...inside=..."` | Contextual code search | `/cq q "entity=function inside='class Config'"` |
| `/cq q "...scope=..."` | Closure analysis | `/cq q "entity=function scope=closure"` |
| `/cq q "all=..."` | Combined patterns (AND) | `/cq q "entity=function all='await \$X,return \$Y'"` |
| `/cq q "any=..."` | Alternative patterns (OR) | `/cq q "any='logger.\$M(\$$$),print(\$$$)'"` |
| `/cq q "not=..."` | Pattern exclusion | `/cq q "entity=function not.has='pass'"` |
| `/cq q "pattern='eval(\$X)'"` | Security pattern scan | `/cq q "pattern='eval(\$X)'"` |
| `/cq q --format mermaid` | Visualize code structure | `/cq q "entity=function expand=callers" --format mermaid` |
| `/cq q --format mermaid-cfg` | Control flow graphs | `/cq q "entity=function name=fn" --format mermaid-cfg` |
| `/cq run` | Multi-step execution (shared scan) | `/cq run --steps '[{"type":"q",...},{"type":"calls",...}]'` |
| `/cq run` | Plan file execution | `/cq run --plan analysis.toml` |
| `/cq chain` | Command chaining | `/cq chain q "..." AND calls foo AND search foo` |
| `/ast-grep` | Structural search/rewrite | `/ast-grep pattern 'def $F($_): ...'` |
| `/datafusion-and-deltalake-stack` | DataFusion + DeltaLake operations (query engine, storage, UDFs) | `/datafusion-and-deltalake-stack` |

### Default: Start with Smart Search

**For any code discovery task, start with Smart Search:**
```bash
/cq search <identifier>
```

Smart Search automatically:
- Classifies matches (definition, callsite, import, etc.)
- Groups by containing function/scope
- Hides non-code matches (strings/comments)
- Suggests follow-up commands

**CQ-First Policy**
- Use `/cq search` for code discovery instead of `rg`/`grep`.
- Use `/cq q "pattern=..."` for AST-exact matching.
- Use `/cq calls` or `/cq impact` before refactors.
- Use `/cq run` for multi-step workflows to avoid repeated scans.
- Use `rg` only for non-Python assets or explicit raw text needs.

Only use pattern queries (`/cq q "pattern=..."`) when you need:
- Exact AST-level structural matching
- Zero false positives from strings/comments
- Complex relational constraints (inside, has, precedes)

### When to Use cq (Mandatory)

**Before proposing changes to:**
- Function signatures → `/cq sig-impact`
- Function parameters → `/cq impact --param`
- Any function body → `/cq calls` (find all callers first)
- Import structure → `/cq q "entity=import"`
- Class definitions → `/cq q "entity=class name=<name>"`

**Before analyzing code patterns:**
- Dynamic dispatch patterns → `/cq q "pattern='getattr(\$X, \$Y)'"`
- Eval/exec usage → `/cq q "pattern='eval(\$X)'"` or `pattern='exec(\$X)'`
- Security patterns → `/cq q "pattern='eval(\$X)'"` or `/cq q "pattern='pickle.load(\$X)'"`

**Before refactoring closures:**
- Find all closures → `/cq q "entity=function scope=closure in=<dir>"`
- Understand capture → `/cq scopes <file>`

**Before understanding call flow:**
- Visualize callers → `/cq q "entity=function name=<fn> expand=callers" --format mermaid`

### Smart Search Examples

Smart Search provides fast, semantically-enriched code search. It automatically groups
results by containing function/scope and classifies matches as definitions, callsites,
imports, comments, etc.

```bash
# Simple identifier search (auto-detected)
/cq search build_graph

# Regex search
/cq search "config.*path" --regex

# Literal string search
/cq search "hello world" --literal

# Scoped search (only in directory)
/cq search CqResult --in tools/cq/core/

# Include matches in strings/comments/docstrings
/cq search build_graph --include-strings

# Plain queries in /cq q also fall back to search
/cq q build_graph  # Same as: /cq search build_graph
```

**Output structure:**
- **Top Contexts**: Matches grouped by containing function
- **Definitions**: Function/class definitions (identifier mode)
- **Imports**: Import statements
- **Callsites**: Function calls
- **Uses by Kind**: Category breakdown
- **Non-Code Matches**: Strings/comments (collapsed)
- **Hot Files**: Files with most matches
- **Suggested Follow-ups**: Next commands to explore

### Query Command Examples

```bash
# Find all imports of a module
/cq q "entity=import name=Path"

# Find functions by pattern
/cq q "entity=function name=~^build"

# Find callers of functions in a module
/cq q "entity=function in=src/semantics/ expand=callers"

# Understand import structure before refactoring
/cq q "entity=import in=src/extract/"
```

### Pattern Query Examples

Pattern queries find structural code patterns without false positives from strings/comments.

```bash
# Find all getattr usage (dynamic attribute access)
/cq q "pattern='getattr(\$X, \$Y)'"

# Find eval/exec patterns
/cq q "pattern='eval(\$X)'"

# Find pickle.load (unsafe deserialization pattern)
/cq q "pattern='pickle.load(\$X)'"

# Find functions that take **kwargs
/cq q "pattern='def \$F(\$\$\$, **\$K)'"

# Find try blocks without except
/cq q "pattern='try: \$\$\$ finally: \$\$\$'"

# Find methods inside specific class contexts
/cq q "entity=function inside='class Config'"

# Find closures before extraction
/cq q "entity=function scope=closure in=src/semantics/"
```

### Advanced Query Patterns

**Composite Queries (AND/OR/NOT):**
```bash
# Functions with both await AND return
/cq q "entity=function all='await \$X,return \$Y'"

# Any logging pattern
/cq q "any='logger.\$M(\$$$),print(\$$$),console.log(\$$$)'"

# Functions WITHOUT docstrings
/cq q "entity=function not.has='\"\"\"'"

# Async functions that don't await
/cq q "entity=function has='async def' not.has='await'"
```

**Pattern Disambiguation:**
```bash
# When patterns are ambiguous, use context + selector
/cq q "pattern.context='{ \"\$K\": \$V }' pattern.selector=pair"

# Extract specific argument positions
/cq q "pattern.context='func(\$A, \$B)' pattern.selector=argument"
```

**Security-Focused Queries:**
```bash
# Find code execution patterns
/cq q "any='eval(\$X),exec(\$X),compile(\$X, \$Y, \"exec\")'"

# Find deserialization patterns
/cq q "any='pickle.load(\$X),yaml.load(\$X),marshal.load(\$X)'"

# Shell injection risks
/cq q "pattern='subprocess.\$M(\$$$, shell=True)'"
```

**Bytecode Analysis:**
```bash
# Find functions using specific opcodes
/cq q "entity=function bytecode.opname=LOAD_GLOBAL"

# Visualize control flow
/cq q "entity=function name=complex_fn" --format mermaid-cfg
```

### Query Selection Guide

| Need | Query Type | Example |
|------|-----------|---------|
| Find by name | `entity=` + `name=` | `/cq q "entity=function name=~^build"` |
| Structural pattern | `pattern=` | `/cq q "pattern='getattr(\$X, \$Y)'"` |
| Combined conditions | `all=` | `/cq q "all='await \$X,try:'"` |
| Alternative patterns | `any=` | `/cq q "any='log.\$M,print'"` |
| Exclude matches | `not=` | `/cq q "entity=function not.has='pass'"` |
| Context-aware | `inside=` / `has=` | `/cq q "pattern='\$X' inside='class Config'"` |
| Security scan | `pattern=...` | `/cq q "pattern='eval(\$X)'"` |
| Closures | `scope=closure` | `/cq q "entity=function scope=closure"` |
| Visualization | `--format mermaid*` | `/cq q "expand=callers" --format mermaid` |

**When to Prefer Pattern Queries over Grep:**
- Finding code in strings/comments → Pattern query (no false positives)
- Dynamic dispatch patterns → Pattern query (AST-aware)
- Combining multiple conditions → Composite queries
- Security pattern detection → Pattern queries (AST-based)

### Multi-Step Workflows

For complex analysis requiring multiple queries, use `cq run` or `cq chain`:

**cq run (JSON/TOML Plans):**
```bash
# Agent-friendly JSON steps
/cq run --steps '[{"type":"search","query":"build_graph"},{"type":"q","query":"entity=function name=build_graph expand=callers"},{"type":"calls","function":"build_graph"}]'

# Human-friendly TOML plan file
/cq run --plan docs/plans/refactor_check.toml

# Mixed: plan + inline steps
/cq run --plan base.toml --step '{"type":"impact","function":"foo","param":"x"}'
```

**cq chain (Command Chaining):**
```bash
# Default AND delimiter
/cq chain search build_graph AND q "entity=function expand=callers" AND calls build_graph

# Custom delimiter
/cq chain q "entity=function" OR calls foo --delimiter OR
```

**When to Use:**
| Scenario | Approach |
|----------|----------|
| Ad-hoc multi-query | `/cq chain cmd1 AND cmd2 AND cmd3` |
| Repeatable workflow | `/cq run --plan workflow.toml` |
| Agent automation | `/cq run --steps '[...]'` |

**Performance:** Multiple `q` steps share a single scan for better performance.

### Global Options (All cq Commands)

All `/cq` commands support global options:

| Option | Env Var | Description |
|--------|---------|-------------|
| `--format` | `CQ_FORMAT` | Output format (md, json, mermaid, mermaid-class, mermaid-cfg, dot) |
| `--root` | `CQ_ROOT` | Repository root path |
| `--verbose` | `CQ_VERBOSE` | Verbosity level (0-3) |

Config file: `.cq.toml` in repo root. See `/cq --help` for full options.

### Deprecated Commands

The `cq index` and `cq cache` admin commands are deprecated stubs that print deprecation notices. Caching infrastructure has been removed.

### Why This Matters

- **AST-based analysis** - No false positives from strings/comments
- **Confidence scoring** - Know how reliable the results are
- **Impact scoring** - Prioritize high-impact changes
- **Transitive analysis** - Find indirect callers via `expand=callers(depth=N)`
- **Hazard detection** - Spots dynamic dispatch, forwarding patterns
- **Pattern queries** - Structural search without string/comment false positives
- **Composite logic** - Combine patterns with `all`/`any`/`not` operators
- **Pattern disambiguation** - Use `context`/`selector` for ambiguous patterns
- **Relational constraints** - Find code in specific contexts (inside classes, containing patterns)
- **Scope filtering** - Identify closures before extraction
- **Visualization** - Call graphs, class diagrams, and CFGs via `--format mermaid*`
- **Bytecode analysis** - Query opcodes, build CFGs, analyze stack effects
- **Pattern queries** - Structural search without string/comment false positives

---

## 3. Quality Gate

**Run after task completion**, not during implementation. Single command for all checks:

```bash
uv run ruff format && uv run ruff check --fix && uv run pyrefly check && uv run pyright && uv run pytest -q
```

**In order:**
1. `ruff format` - Auto-format
2. `ruff check --fix` - Lint + autofix. You must run ruff check with the "--fix" argument to apply autofixes, which should always be applied
3. `pyrefly check` - Strict type/contract validation (the real gate). Never include a " ." after "uv run pyrefly check". The unmodified "uv run pyrefly check" will check the whole repo in the manner that we intend.
4. `pyright` - IDE-level type checking (basic mode)
5. `pytest -q` - Unit tests (add `-m "not e2e"` for fast iteration)

**Timing:** Complete your implementation first, then run the full gate. Don't interrupt workflow with mid-task linting.

---

## 4. Repo Map

```
src/
├── cli/                 # CLI entrypoint
├── semantics/           # Semantic compiler + normalization (canonical)
├── datafusion_engine/   # DataFusion integration, sessions, UDFs
├── hamilton_pipeline/   # Hamilton DAG orchestration
├── cpg/                 # CPG schema definitions, node/edge contracts
├── relspec/             # Task/plan catalog, rustworkx scheduling
├── extract/             # Multi-source extractors
├── storage/             # Delta Lake integration
├── obs/                 # Observability (OpenTelemetry)
└── utils/               # Shared utilities

rust/                    # Rust extensions (DataFusion plugins, UDFs)
tests/                   # See tests/AGENTS.md
scripts/                 # Build and maintenance scripts
schemas/                 # JSON Schema definitions
docs/                    # Architecture docs (auto-generated)
```

**Entry point:**
```python
from graph import GraphProductBuildRequest, build_graph_product
result = build_graph_product(GraphProductBuildRequest(repo_root="."))
```

---

## 5. Project Primitives

**Persisted artifacts:**
- `src/serde_artifacts.py` - Serialization format definitions
- `src/serde_schema_registry.py` - Schema evolution registry

**Span semantics:**
- `bstart/bend` are **byte offsets**, not line/column
- All normalizations anchor to byte spans

**Schema specs:**
- `src/schema_spec/` - Declarative schema definitions
- JSON Schema 2020-12 for contracts

**DataFusion/UDF:**
- `src/datafusion_engine/` - Python integration
- `rust/` - Rust extensions
- Rebuild: `bash scripts/rebuild_rust_artifacts.sh`

---

## 6. Sources of Truth

| Concern | Location |
|---------|----------|
| Ruff rules | `pyproject.toml` → `[tool.ruff]`, `[tool.ruff.lint]` |
| Pyrefly config | `pyrefly.toml` |
| Pyright config | `pyrightconfig.json` (basic mode) |
| Dependencies | `pyproject.toml` → `[project.dependencies]` |
| Test markers | `pyproject.toml` → `[tool.pytest.ini_options]` |

**Do not duplicate** tool configuration in documentation. Point to the source.

---

## 7. Path-Scoped Guidance

Detailed policies live in context-specific files:

- `tests/AGENTS.md` - Test categories, markers, mocking policy
- `rust/AGENTS.md` - Rust build, crate structure, artifacts
- `src/datafusion_engine/AGENTS.md` - DataFusion integration guidance
- `src/semantics/AGENTS.md` - Span canonicalization, compiler rules

---

## 8. Quick Reference

**Run tests:**
```bash
uv run pytest tests/unit/           # Unit only
uv run pytest -m "not e2e"          # Skip slow tests
uv run pytest tests/unit/test_foo.py -v  # Single file
```

**Update golden snapshots:**
```bash
uv run pytest tests/cli_golden/ --update-golden
uv run pytest tests/plan_golden/ --update-goldens
```

**Rebuild Rust:**
```bash
bash scripts/rebuild_rust_artifacts.sh
```

**Format + lint:**
```bash
uv run ruff format && uv run ruff check --fix
```
