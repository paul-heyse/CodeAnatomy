# CQ ast-grep-py Migration Plan

> **Status:** Ready for Review
> **Author:** Claude
> **Created:** 2026-02-02
> **Objective:** Replace ast-grep CLI subprocess calls with native ast-grep-py Python bindings and enable new capabilities

---

## Executive Summary

This document proposes migrating the cq tool from ast-grep CLI subprocess calls to the native `ast-grep-py` Python bindings. This migration eliminates subprocess overhead, removes the external binary dependency, and enables powerful new capabilities including direct AST traversal, in-process metavariable capture, and programmatic code rewriting.

**Key Benefits:**
- **Performance:** Eliminate subprocess spawn overhead (~5-20ms per call)
- **Reliability:** Remove external binary dependency; `ast-grep-py` is a pure pip package
- **Capabilities:** Enable programmatic AST traversal, refinement predicates, and code transformation
- **Developer Experience:** Native Python types, direct access to captures, better error handling

---

## Part 1: Current State Analysis

### 1.1 CLI Usage Locations

The cq tool currently invokes ast-grep via subprocess in two locations:

| Location | Function | Purpose |
|----------|----------|---------|
| `tools/cq/query/sg_parser.py:142-173` | `_run_scan()` | Config-based fact extraction |
| `tools/cq/query/executor.py:522-559` | `_run_ast_grep_inline_rules()` | Pattern queries and hazard detection |

### 1.2 Command Patterns

**Config-based scanning:**
```bash
ast-grep scan -c tools/cq/astgrep/sgconfig.yml --json=stream [--globs pattern]* [paths...]
```

**Inline rule scanning:**
```bash
ast-grep scan --inline-rules <yaml_string> --json=stream [--globs pattern]* [paths...]
```

### 1.3 Current Data Flow

```
Source Files → subprocess(ast-grep) → NDJSON stdout → JSON parse → SgRecord list
```

**Record Types Extracted:**
- `def` (function, class, async_function, class_bases, etc.)
- `call` (name_call, attr_call)
- `import` (import, from_import, import_as, etc.)
- `raise` (raise, raise_from, raise_bare)
- `except` (except, except_as, except_bare)
- `assign_ctor` (ctor_assign_name, ctor_assign_attr)

### 1.4 Current Limitations

1. **Subprocess overhead:** ~5-20ms spawn time per invocation
2. **String serialization:** JSON parsing for every match
3. **No direct traversal:** Cannot navigate AST after matching
4. **No refinement predicates:** Cannot filter matches programmatically
5. **External dependency:** Requires `ast-grep` or `sg` binary in PATH
6. **Limited error context:** Subprocess stderr is harder to diagnose

---

## Part 2: ast-grep-py API Mapping

### 2.1 Core Objects

| CLI Concept | ast-grep-py Object | Description |
|-------------|-------------------|-------------|
| Source file | `SgRoot(src, "python")` | Parse entry point |
| AST tree | `root.root() → SgNode` | Root node handle |
| Pattern match | `node.find(pattern="...")` | Single match |
| Multiple matches | `node.find_all(pattern="...")` | All matches |
| Match location | `node.range() → Range` | Start/end positions |
| Match text | `node.text()` | Matched source text |
| Metavariable | `node.get_match("A")` | Single capture ($A) |
| Multi-metavar | `node.get_multiple_matches("ARGS")` | Multi capture ($$$ARGS) |

### 2.2 Search Modes

**Kwargs mode (simple patterns):**
```python
matches = node.find_all(pattern="print($A)")
matches = node.find_all(kind="function_definition")
```

**Config mode (full YAML-like):**
```python
config = {
    "rule": {"pattern": "eval($X)", "inside": {"pattern": "def $_($$$): $$$"}},
    "constraints": {"X": {"regex": "^['\"]"}}
}
matches = node.find_all(config)
```

### 2.3 Position Mapping

| Current `SgRecord` | ast-grep-py `Range` |
|-------------------|---------------------|
| `start_line` (1-indexed) | `range.start.line + 1` |
| `start_col` (0-indexed) | `range.start.column` |
| `end_line` (1-indexed) | `range.end.line + 1` |
| `end_col` (0-indexed) | `range.end.column` |
| N/A | `range.start.index` (byte offset) |

### 2.4 Refinement Predicates (NEW)

ast-grep-py provides boolean refinement predicates not available via CLI:

```python
# Check if match is inside a pattern
node.inside(pattern="class $C: $$$")  → bool

# Check if match contains a pattern
node.has(kind="call")  → bool

# Check if match precedes/follows
node.precedes(pattern="return $X")  → bool
node.follows(pattern="# type: ignore")  → bool

# Check if match satisfies a rule
node.matches(pattern="async def $_($$$): $$$")  → bool
```

### 2.5 AST Traversal (NEW)

```python
node.parent()           # Parent node
node.children()         # All children
node.child(0)           # Nth child
node.field("name")      # Named field (e.g., function name)
node.field_children()   # Children via named fields
node.ancestors()        # All ancestors
node.next() / node.prev()     # Siblings
node.next_all() / node.prev_all()  # All following/preceding siblings
```

### 2.6 Edit Operations (NEW)

```python
edit = node.replace("new_text")  # Create Edit object
# edit.start_pos, edit.end_pos, edit.inserted_text

new_source = node.commit_edits([edit1, edit2, ...])  # Apply edits
```

---

## Part 3: Migration Phases

### Phase 1: Core Parser Migration

**Goal:** Replace `_run_scan()` with ast-grep-py equivalent.

**File:** `tools/cq/query/sg_parser.py`

**Changes:**

1. **New function `_scan_with_sgpy()`:**
```python
from ast_grep_py import SgRoot

def _scan_with_sgpy(
    files: list[Path],
    rules: list[RuleSpec],
    root: Path,
) -> list[SgRecord]:
    """Scan files using ast-grep-py native bindings."""
    records: list[SgRecord] = []

    for file_path in files:
        try:
            src = file_path.read_text(encoding="utf-8")
        except OSError:
            continue

        sg_root = SgRoot(src, "python")
        node = sg_root.root()

        for rule in rules:
            matches = node.find_all(rule.to_config())
            for match in matches:
                record = _match_to_record(match, file_path, rule, root)
                records.append(record)

    return records
```

2. **Rule specification dataclass:**
```python
@dataclass(frozen=True)
class RuleSpec:
    """Python-side rule specification for ast-grep-py."""
    rule_id: str
    record_type: RecordType
    kind: str
    config: dict[str, Any]

    def to_config(self) -> dict:
        return self.config
```

3. **Match to record conversion:**
```python
def _match_to_record(
    match: SgNode,
    file_path: Path,
    rule: RuleSpec,
    root: Path,
) -> SgRecord:
    rng = match.range()
    return SgRecord(
        record=rule.record_type,
        kind=rule.kind,
        file=file_path.relative_to(root).as_posix(),
        start_line=rng.start.line + 1,  # Convert to 1-indexed
        start_col=rng.start.column,
        end_line=rng.end.line + 1,
        end_col=rng.end.column,
        text=match.text(),
        rule_id=rule.rule_id,
    )
```

**Migration of YAML rules:**

Current YAML rules in `tools/cq/astgrep/rules/python_facts/` can be converted to Python:

```python
# From py_def_function.yml:
#   kind: function_definition
#   regex: "^def "
#   not:
#     has:
#       kind: type_parameter

PY_DEF_FUNCTION = RuleSpec(
    rule_id="py_def_function",
    record_type="def",
    kind="function",
    config={
        "rule": {
            "kind": "function_definition",
            "regex": "^def ",
            "not": {"has": {"kind": "type_parameter"}}
        }
    }
)
```

### Phase 2: Pattern Query Migration

**Goal:** Replace `_run_ast_grep_inline_rules()` with ast-grep-py.

**File:** `tools/cq/query/executor.py`

**Changes:**

1. **New function `_execute_pattern_with_sgpy()`:**
```python
def _execute_pattern_with_sgpy(
    rules: tuple[AstGrepRule, ...],
    paths: list[Path],
    root: Path,
    query: Query | None = None,
) -> tuple[list[Finding], list[SgRecord], list[dict]]:
    """Execute pattern queries using ast-grep-py."""
    findings: list[Finding] = []
    records: list[SgRecord] = []
    raw_matches: list[dict] = []

    for file_path in paths:
        try:
            src = file_path.read_text(encoding="utf-8")
        except OSError:
            continue

        sg_root = SgRoot(src, "python")
        node = sg_root.root()

        for rule in rules:
            config = rule.to_sgpy_config()
            matches = node.find_all(config)

            for match in matches:
                # Apply metavar filters directly
                if query and query.metavar_filters:
                    if not _apply_metavar_filters_sgpy(match, query.metavar_filters):
                        continue

                finding, record = _match_to_finding_and_record(
                    match, file_path, root
                )
                findings.append(finding)
                records.append(record)

    return findings, records, raw_matches
```

2. **Direct metavar access:**
```python
def _apply_metavar_filters_sgpy(
    match: SgNode,
    filters: dict[str, str],
) -> bool:
    """Apply metavar filters using native capture access."""
    for name, pattern in filters.items():
        capture = match.get_match(name)
        if capture is None:
            return False
        if not re.match(pattern, capture.text()):
            return False
    return True
```

3. **Update `AstGrepRule.to_sgpy_config()`:**
```python
def to_sgpy_config(self) -> dict:
    """Convert to ast-grep-py Config format."""
    rule: dict = {}

    if self.context:
        rule["pattern"] = {
            "context": self.context,
            "selector": self.selector,
        }
        if self.strictness != "smart":
            rule["pattern"]["strictness"] = self.strictness
    else:
        rule["pattern"] = self.pattern
        if self.strictness != "smart":
            rule["strictness"] = self.strictness

    if self.kind:
        rule["kind"] = self.kind

    # Relational constraints
    if self.inside:
        inside_rule = {"pattern": self.inside}
        if self.inside_stop_by:
            inside_rule["stopBy"] = self.inside_stop_by
        if self.inside_field:
            inside_rule["field"] = self.inside_field
        rule["inside"] = inside_rule

    # Similar for has, precedes, follows...

    return {"rule": rule}
```

### Phase 3: Hazard Detection Migration

**Goal:** Migrate hazard detection to ast-grep-py.

**File:** `tools/cq/query/hazards.py`

**Changes:**

1. **New `HazardDetector.detect_with_sgpy()`:**
```python
def detect_with_sgpy(
    self,
    node: SgNode,
    file_path: Path,
    root: Path,
) -> list[HazardMatch]:
    """Detect hazards using ast-grep-py."""
    matches: list[HazardMatch] = []

    for spec in self.specs:
        config = spec.to_sgpy_config()
        hits = node.find_all(config)

        for hit in hits:
            # Use refinement predicates for additional filtering
            if spec.not_inside:
                if hit.inside(pattern=spec.not_inside):
                    continue

            matches.append(HazardMatch(
                spec=spec,
                node=hit,
                file_path=file_path,
            ))

    return matches
```

### Phase 4: Toolchain Simplification

**Goal:** Remove `sg_path` detection since ast-grep-py is a pip package.

**File:** `tools/cq/core/toolchain.py`

**Changes:**

1. Replace binary detection with package version check:
```python
@dataclass
class Toolchain:
    rpygrep_available: bool
    rpygrep_version: str | None
    sgpy_available: bool      # NEW: ast-grep-py availability
    sgpy_version: str | None  # NEW: ast-grep-py version
    py_path: str
    py_version: str

    @staticmethod
    def detect() -> Toolchain:
        # Check ast-grep-py
        sgpy_available = False
        sgpy_version = None
        try:
            import ast_grep_py
            sgpy_available = True
            import importlib.metadata
            sgpy_version = importlib.metadata.version("ast-grep-py")
        except ImportError:
            pass

        # ... rest of detection
```

2. Remove `sg_path`, `sg_version`, `require_sg()`, `has_sg` (replaced by `sgpy_*`).

---

## Part 4: New Capabilities Enabled

### 4.1 Programmatic Refinement Predicates

**Use case:** Filter functions that are methods (inside a class).

**Current approach:** Post-process SgRecords with interval index.

**New approach:**
```python
for func in node.find_all(kind="function_definition"):
    if func.inside(pattern="class $C: $$$"):
        # This is a method
        ...
```

### 4.2 Direct AST Traversal

**Use case:** Find the class containing a method.

**Current approach:** Build IntervalIndex, query by line number.

**New approach:**
```python
def find_enclosing_class(method: SgNode) -> SgNode | None:
    for ancestor in method.ancestors():
        if ancestor.kind() == "class_definition":
            return ancestor
    return None
```

### 4.3 Field-Based Access

**Use case:** Get function name without regex extraction.

**Current approach:** `re.match(r"def\s+(\w+)", text).group(1)`

**New approach:**
```python
name_node = func.field("name")
name = name_node.text() if name_node else None
```

### 4.4 Native Metavar Capture

**Use case:** Extract captured metavariables.

**Current approach:** Parse JSON `metaVariables` field.

**New approach:**
```python
match = node.find(pattern="getattr($OBJ, $ATTR)")
obj_text = match.get_match("OBJ").text()
attr_text = match.get_match("ATTR").text()
```

### 4.5 Code Transformation

**Use case:** Automated codemod (e.g., replace deprecated API).

**Not currently possible with CLI.** Would require external tooling.

**New capability:**
```python
def modernize_print(src: str) -> str:
    root = SgRoot(src, "python").root()
    edits = []

    for match in root.find_all(pattern="print $A"):
        arg = match.get_match("A")
        new_call = f"print({arg.text()})"
        edits.append(match.replace(new_call))

    return root.commit_edits(edits)
```

### 4.6 Transform Support

**Use case:** Case conversion, substring extraction.

ast-grep-py supports transforms via Config:
```python
config = {
    "rule": {"pattern": "const $NAME = $VALUE"},
    "transform": {
        "SNAKE_NAME": {
            "source": "$NAME",
            "convert": {"toCase": "snake"}
        }
    }
}
# Access via: match.get_transformed("SNAKE_NAME")
```

### 4.7 Composite Rules (any/all/not)

**Use case:** Match multiple patterns in one pass.

```python
config = {
    "rule": {
        "any": [
            {"pattern": "eval($X)"},
            {"pattern": "exec($X)"},
            {"pattern": "compile($X, $Y, 'exec')"},
        ]
    }
}
```

### 4.8 Enhanced Strictness Control

ast-grep-py exposes all strictness levels:
- `cst`: Exact match including trivia
- `smart`: Default, balanced
- `ast`: Ignore trivia, match structure
- `relaxed`: Very permissive
- `signature`: Match only function signatures

---

## Part 5: Performance Considerations

### 5.1 Expected Gains

| Operation | CLI Overhead | ast-grep-py |
|-----------|-------------|-------------|
| Subprocess spawn | ~5-20ms | 0ms |
| JSON serialization | ~0.5ms/match | 0ms |
| Parse per file | (included in CLI) | ~1-5ms |
| Match iteration | JSON decode | Native Python |

### 5.2 Memory Considerations

- CLI: One process per scan, results streamed
- ast-grep-py: All nodes in memory per file

**Mitigation:** Process files incrementally, not all at once.

### 5.3 Parallelism

- CLI: Single-threaded subprocess
- ast-grep-py: Can use `concurrent.futures.ProcessPoolExecutor` for multi-file scans

```python
from concurrent.futures import ProcessPoolExecutor

def scan_file(file_path: Path) -> list[SgRecord]:
    src = file_path.read_text()
    root = SgRoot(src, "python").root()
    return [_match_to_record(m) for m in root.find_all(config)]

with ProcessPoolExecutor() as executor:
    results = list(executor.map(scan_file, files))
```

### 5.4 Caching Strategy

Current caching (IndexCache) can be preserved:
- Cache key: file path + mtime + rule set hash
- Cache value: serialized SgRecord list

---

## Part 6: Implementation Plan

### 6.1 File Changes

| File | Changes |
|------|---------|
| `tools/cq/query/sg_parser.py` | Add `_scan_with_sgpy()`, migrate `sg_scan()` |
| `tools/cq/query/executor.py` | Migrate `_run_ast_grep_inline_rules()` |
| `tools/cq/query/hazards.py` | Add `detect_with_sgpy()` |
| `tools/cq/query/planner.py` | Add `AstGrepRule.to_sgpy_config()` |
| `tools/cq/core/toolchain.py` | Replace sg_path with sgpy detection |
| `tools/cq/astgrep/rules_py.py` | NEW: Python-native rule definitions |

### 6.2 New Files

```
tools/cq/
├── astgrep/
│   ├── rules_py.py          # Python rule definitions
│   └── sgpy_adapter.py      # ast-grep-py adapter utilities
├── query/
│   └── sgpy_scanner.py      # Native scanner implementation
```

### 6.3 Backward Compatibility

**Option A: Feature flag (recommended for initial rollout):**
```python
USE_SGPY = env_bool("CQ_USE_SGPY", default=False)

def sg_scan(...):
    if USE_SGPY:
        return _scan_with_sgpy(...)
    return _scan_with_cli(...)
```

**Option B: Full replacement (after validation):**
Remove CLI code paths entirely.

### 6.4 Testing Strategy

1. **Unit tests:** Add tests for `_match_to_record()`, `_scan_with_sgpy()`
2. **Golden tests:** Ensure output matches CLI for existing test cases
3. **Benchmark tests:** Compare performance (files/second)
4. **Integration tests:** Run existing cq e2e tests with `CQ_USE_SGPY=1`

---

## Part 7: New Feature Opportunities

### 7.1 Interactive AST Explorer

Add `/cq explore <file>` command for interactive AST navigation:
```
$ /cq explore src/module.py
> find kind=function_definition
Found 5 matches
> select 0
Selected: def process(data):
> parent
module (1:1-50:1)
> field name
identifier: process
> has pattern="return $X"
True
```

### 7.2 Codemod Command

Add `/cq transform --rule <rule> --fix`:
```bash
/cq transform --pattern "print(\$A)" --replacement "logger.info(\$A)" src/
```

### 7.3 Semantic Type Annotations

Use AST traversal to infer types from context:
```python
def infer_receiver_type(call: SgNode) -> str | None:
    """Infer type of method call receiver."""
    func = call.field("function")
    if func and func.kind() == "attribute":
        obj = func.field("object")
        # Check if obj is self/cls
        if obj and obj.text() in ("self", "cls"):
            # Find enclosing class
            for ancestor in call.ancestors():
                if ancestor.kind() == "class_definition":
                    return ancestor.field("name").text()
    return None
```

### 7.4 Enhanced Scope Detection

Use `inside` predicate for accurate scope determination:
```python
def get_scope_type(node: SgNode) -> Literal["module", "class", "function", "closure"]:
    if node.inside(pattern="def $_($$$): $$$"):
        outer = find_enclosing_function(node)
        if outer and outer.inside(pattern="def $_($$$): $$$"):
            return "closure"
        return "function"
    if node.inside(pattern="class $_: $$$"):
        return "class"
    return "module"
```

### 7.5 Call Graph Enhancement

Direct traversal enables more accurate call resolution:
```python
def resolve_call_target(call: SgNode) -> str | None:
    """Resolve call target with AST context."""
    func = call.field("function")
    if not func:
        return None

    if func.kind() == "identifier":
        return func.text()

    if func.kind() == "attribute":
        attr = func.field("attribute")
        obj = func.field("object")
        if obj and obj.text() in ("self", "cls"):
            # Method call - include class context
            cls = find_enclosing_class(call)
            if cls:
                cls_name = cls.field("name").text()
                return f"{cls_name}.{attr.text()}"
        return attr.text() if attr else None

    return None
```

### 7.6 Control Flow Graph Generation

Build CFG from function body using traversal:
```python
def build_cfg(func: SgNode) -> ControlFlowGraph:
    body = func.field("body")
    cfg = ControlFlowGraph()

    for stmt in body.children():
        if stmt.kind() == "if_statement":
            cfg.add_branch(...)
        elif stmt.kind() == "for_statement":
            cfg.add_loop(...)
        elif stmt.kind() == "return_statement":
            cfg.add_exit(...)
        # ...

    return cfg
```

---

## Part 8: Risks and Mitigations

### 8.1 API Stability

**Risk:** ast-grep-py API may change between versions.

**Mitigation:**
- Pin version in `pyproject.toml`
- Add version check in Toolchain.detect()
- Abstract behind adapter layer

### 8.2 Memory Usage

**Risk:** Large files may consume significant memory.

**Mitigation:**
- Add file size limit (e.g., 2MB)
- Process files in batches
- Release SgRoot after processing each file

### 8.3 Unicode Handling

**Risk:** Byte offset vs character offset mismatches.

**Mitigation:**
- ast-grep-py uses byte offsets (matching our existing model)
- Test with unicode-heavy fixtures

### 8.4 Error Handling

**Risk:** Parse errors in malformed files.

**Mitigation:**
```python
try:
    root = SgRoot(src, "python")
except Exception as e:
    logger.warning(f"Parse error in {file_path}: {e}")
    return []
```

---

## Part 9: Success Metrics

1. **Performance:** 2x+ improvement in scan throughput
2. **Reliability:** Zero regressions in existing cq tests
3. **Coverage:** All 6 record types migrated
4. **Features:** At least 2 new capabilities enabled
5. **Maintenance:** Remove ast-grep binary from requirements

---

## Part 10: Timeline

| Phase | Duration | Deliverables |
|-------|----------|--------------|
| Phase 1: Core Parser | 1 week | `sg_parser.py` migration, tests |
| Phase 2: Pattern Queries | 1 week | `executor.py` migration, tests |
| Phase 3: Hazard Detection | 3 days | `hazards.py` migration |
| Phase 4: Toolchain | 2 days | Remove sg_path, add sgpy detection |
| Phase 5: New Features | 2 weeks | AST traversal, codemod support |
| Phase 6: Cleanup | 3 days | Remove CLI code, update docs |

**Total:** ~5 weeks

---

## Appendix A: Rule Conversion Examples

### A.1 Function Definition

**YAML (`py_def_function.yml`):**
```yaml
id: py_def_function
language: python
severity: hint
rule:
  kind: function_definition
  regex: "^def "
  not:
    has:
      kind: type_parameter
metadata:
  record: def
  kind: function
```

**Python:**
```python
PY_DEF_FUNCTION = RuleSpec(
    rule_id="py_def_function",
    record_type="def",
    kind="function",
    config={
        "rule": {
            "kind": "function_definition",
            "regex": "^def ",
            "not": {"has": {"kind": "type_parameter"}}
        }
    }
)
```

### A.2 Attribute Call

**YAML (`py_call_attr.yml`):**
```yaml
id: py_call_attr
language: python
severity: hint
rule:
  kind: call
  has:
    kind: attribute
    field: function
metadata:
  record: call
  kind: attr_call
```

**Python:**
```python
PY_CALL_ATTR = RuleSpec(
    rule_id="py_call_attr",
    record_type="call",
    kind="attr_call",
    config={
        "rule": {
            "kind": "call",
            "has": {"kind": "attribute", "field": "function"}
        }
    }
)
```

---

## Appendix B: API Quick Reference

### B.1 SgRoot

```python
SgRoot(src: str, language: str)
    .root() -> SgNode
```

### B.2 SgNode

```python
# Search
.find(pattern=..., kind=..., ...) -> SgNode | None
.find(config: dict) -> SgNode | None
.find_all(...) -> list[SgNode]

# Inspection
.text() -> str
.kind() -> str
.range() -> Range
.is_leaf() -> bool
.is_named() -> bool

# Traversal
.parent() -> SgNode | None
.children() -> list[SgNode]
.child(n: int) -> SgNode | None
.field(name: str) -> SgNode | None
.ancestors() -> list[SgNode]
.next() / .prev() -> SgNode | None
.next_all() / .prev_all() -> list[SgNode]

# Captures
.get_match(name: str) -> SgNode | None
.get_multiple_matches(name: str) -> list[SgNode]
.get_transformed(name: str) -> str | None

# Predicates
.matches(**rule) -> bool
.inside(**rule) -> bool
.has(**rule) -> bool
.precedes(**rule) -> bool
.follows(**rule) -> bool

# Edits
.replace(text: str) -> Edit
.commit_edits(edits: list[Edit]) -> str
```

### B.3 Range/Pos

```python
Range:
    .start -> Pos
    .end -> Pos

Pos:
    .line -> int      # 0-indexed
    .column -> int    # 0-indexed
    .index -> int     # byte offset
```

---

## Appendix C: Feature Flag Configuration

```python
# tools/cq/core/config.py

from src.utils.env_utils import env_bool

# Feature flags for ast-grep-py migration
CQ_USE_SGPY = env_bool("CQ_USE_SGPY", default=False)
CQ_SGPY_PARALLEL = env_bool("CQ_SGPY_PARALLEL", default=False)
CQ_SGPY_MAX_FILE_SIZE = int(os.getenv("CQ_SGPY_MAX_FILE_SIZE", "2000000"))
```

---

## References

1. [ast-grep-py Python API Documentation](https://ast-grep.github.io/guide/api-usage/py-api.html)
2. [ast-grep Rule Object Reference](https://ast-grep.github.io/reference/rule.html)
3. [ast-grep Configuration Reference](https://ast-grep.github.io/reference/yaml.html)
4. Local reference: `docs/python_library_reference/ast-grep-py.md`
