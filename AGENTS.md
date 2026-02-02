# AGENTS.md

Agent operating protocol for the CodeAnatomy codebase.

---

## 1. Read This First

**Non-negotiables:**

- **Always `uv run`** - Direct `python` calls will fail due to import issues
- **Python 3.13.11** - Pinned, do not change
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
| `/cq` | Modifying any function/class | `/cq calls <function>` |
| `/cq` | Changing parameters | `/cq impact <function> --param <param>` |
| `/cq` | Signature changes | `/cq sig-impact <function> --to "<new>"` |
| `/cq q` | Finding entities by pattern | `/cq q "entity=function name=~pattern"` |
| `/cq q` | Understanding imports | `/cq q "entity=import in=<dir>"` |
| `/cq q "pattern=..."` | Structural code search | `/cq q "pattern='getattr(\$X, \$Y)'"` |
| `/cq q "...inside=..."` | Contextual code search | `/cq q "entity=function inside='class Config'"` |
| `/cq q "...scope=..."` | Closure analysis | `/cq q "entity=function scope=closure"` |
| `/cq q --format mermaid` | Visualize code structure | `/cq q "entity=function expand=callers" --format mermaid` |
| `/ast-grep` | Structural search/rewrite | `/ast-grep pattern 'def $F($_): ...'` |
| `/datafusion-stack` | DataFusion/Delta/UDF work | `/datafusion-stack` |

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
- Security hazards → `/cq q "entity=function fields=hazards"`

**Before refactoring closures:**
- Find all closures → `/cq q "entity=function scope=closure in=<dir>"`
- Understand capture → `/cq scopes <file>`

**Before understanding call flow:**
- Visualize callers → `/cq q "entity=function name=<fn> expand=callers" --format mermaid`

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

# Find eval/exec hazards
/cq q "pattern='eval(\$X)'"

# Find pickle.load (security hazard)
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

### Why This Matters

- **AST-based analysis** - No false positives from strings/comments
- **Confidence scoring** - Know how reliable the results are
- **Impact scoring** - Prioritize high-impact changes
- **Transitive analysis** - Find indirect callers via `expand=callers(depth=N)`
- **Hazard detection** - Spots dynamic dispatch, forwarding patterns
- **Pattern queries** - Structural search without string/comment false positives
- **Relational constraints** - Find code in specific contexts (inside classes, containing patterns)
- **Scope filtering** - Identify closures before extraction
- **Visualization** - Understand call graphs and class hierarchies visually
- **30+ builtin hazards** - Security and correctness hazard patterns

---

## 3. Quality Gate

**Run after task completion**, not during implementation. Single command for all checks:

```bash
uv run ruff format && uv run ruff check --fix && uv run pyrefly check && uv run pyright && uv run pytest -q
```

**In order:**
1. `ruff format` - Auto-format
2. `ruff check --fix` - Lint + autofix
3. `pyrefly check` - Strict type/contract validation (the real gate)
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
