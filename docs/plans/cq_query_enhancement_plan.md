# CQ Query Enhancement Plan - Detailed Design Document

## Overview

This document details enhancement opportunities for the `cq` tool and specifically the declarative query command (`cq q`), drawing from:
1. **ast-grep capabilities** not yet leveraged (pattern syntax, relational rules, composite rules)
2. **Python introspection tools** (dis, inspect, symtable) for deeper analysis
3. **Semantic graph traversal** patterns for more powerful queries

---

## Current State

### Implemented Entities
- `function`, `class`, `method`, `module`, `callsite`, `import`

### Implemented Expanders
- `callers`, `callees`, `imports`, `raises`, `scope`, `bytecode_surface`

### Implemented Fields
- `def`, `loc`, `callers`, `callees`, `evidence`, `hazards`, `imports`

### Current Architecture
```
tools/cq/
├── cli.py                 # CLI entry point
├── query/
│   ├── ir.py              # Query intermediate representation
│   ├── parser.py          # Query string parser
│   ├── planner.py         # Converts Query → ToolPlan
│   ├── executor.py        # Executes ToolPlan → CqResult
│   ├── sg_parser.py       # ast-grep output parser
│   ├── hazards.py         # Hazard detection
│   ├── enrichment.py      # Result enrichment
│   └── symbol_resolver.py # Symbol resolution
├── index/
│   ├── def_index.py       # Function/class index
│   ├── call_resolver.py   # Call target resolution
│   └── arg_binder.py      # Argument binding
├── macros/                # Individual analysis macros
│   ├── impact.py
│   ├── calls.py
│   └── ...
└── core/
    ├── schema.py          # CqResult, Finding, etc.
    ├── scoring.py         # Impact/confidence scoring
    └── report.py          # Output formatting
```

---

## Enhancement Categories

### A. New Entity Types
### B. ast-grep Pattern Enhancements
### C. Python Introspection Integration
### D. Relational & Composite Queries
### E. Semantic Graph Traversal
### F. Output & Analysis Improvements
### G. Hazard Detection Enhancements
### H. Performance & Caching

---

# Phase 1: Foundation Enhancements

---

## B1. Inline Pattern Support

### Rationale
Currently, `cq q` only supports entity-based queries. Direct ast-grep pattern support enables powerful structural searches without needing to define new entity types.

### Query Syntax
```bash
# Use ast-grep pattern directly
/cq q "pattern='$X = getattr($Y, $Z)'"

# Pattern with language specification
/cq q "pattern='async def $F($$$)' lang=python"

# Pattern with selector for disambiguation
/cq q "pattern='class A { $FIELD = $INIT }' selector=field_definition"

# Pattern with strictness control
/cq q "pattern='def process($A, $B)' strictness=signature"
```

### Key Architectural Elements

#### Extended Query IR (`tools/cq/query/ir.py`)

```python
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

# Strictness modes supported by ast-grep
StrictnessMode = Literal["cst", "smart", "ast", "relaxed", "signature"]

@dataclass(frozen=True)
class PatternSpec:
    """Specification for an ast-grep pattern.

    Attributes
    ----------
    pattern
        The ast-grep pattern string (e.g., "$X = getattr($Y, $Z)")
    context
        Optional surrounding code for disambiguation
    selector
        Optional node kind to select from parsed context
    strictness
        Match strictness mode (default: smart)
    """

    pattern: str
    context: str | None = None
    selector: str | None = None
    strictness: StrictnessMode = "smart"


@dataclass(frozen=True)
class Query:
    """Parsed query representation.

    Extended to support direct pattern queries in addition to entity queries.
    """

    # Entity-based query (mutually exclusive with pattern_spec)
    entity: EntityType | None = None
    name: str | None = None

    # Pattern-based query (mutually exclusive with entity)
    pattern_spec: PatternSpec | None = None

    # Common fields
    expand: tuple[Expander, ...] = ()
    scope: Scope = field(default_factory=Scope)
    fields: tuple[FieldType, ...] = ("def",)
    limit: int | None = None
    explain: bool = False

    def __post_init__(self) -> None:
        """Validate that query has either entity or pattern_spec."""
        if self.entity is None and self.pattern_spec is None:
            raise ValueError("Query must have either entity or pattern_spec")
        if self.entity is not None and self.pattern_spec is not None:
            raise ValueError("Query cannot have both entity and pattern_spec")

    @property
    def is_pattern_query(self) -> bool:
        """Return True if this is a pattern-based query."""
        return self.pattern_spec is not None
```

#### Extended Parser (`tools/cq/query/parser.py`)

```python
from __future__ import annotations

import re
from typing import Any

from tools.cq.query.ir import (
    EntityType,
    Expander,
    FieldType,
    PatternSpec,
    Query,
    Scope,
    StrictnessMode,
)


def parse_query(query_str: str) -> Query:
    """Parse a query string into a Query object.

    Supports both entity-based and pattern-based queries:
    - Entity: "entity=function name=foo"
    - Pattern: "pattern='$X = getattr($Y, $Z)'"

    Parameters
    ----------
    query_str
        The query string to parse.

    Returns
    -------
    Query
        Parsed query object.

    Raises
    ------
    ValueError
        If query string is malformed.
    """
    tokens = _tokenize_query(query_str)

    # Determine query type
    if "pattern" in tokens:
        return _parse_pattern_query(tokens)
    elif "entity" in tokens:
        return _parse_entity_query(tokens)
    else:
        raise ValueError("Query must specify either 'entity' or 'pattern'")


def _parse_pattern_query(tokens: dict[str, str]) -> Query:
    """Parse a pattern-based query."""
    pattern_str = tokens.pop("pattern")

    # Extract pattern-specific options
    context = tokens.pop("context", None)
    selector = tokens.pop("selector", None)
    strictness = tokens.pop("strictness", "smart")

    if strictness not in ("cst", "smart", "ast", "relaxed", "signature"):
        raise ValueError(f"Invalid strictness: {strictness}")

    pattern_spec = PatternSpec(
        pattern=pattern_str,
        context=context,
        selector=selector,
        strictness=strictness,  # type: ignore[arg-type]
    )

    # Parse common options
    scope = _parse_scope(tokens)
    fields = _parse_fields(tokens)
    expand = _parse_expanders(tokens)
    limit = int(tokens.pop("limit")) if "limit" in tokens else None
    explain = tokens.pop("explain", "false").lower() == "true"

    return Query(
        pattern_spec=pattern_spec,
        scope=scope,
        fields=fields,
        expand=expand,
        limit=limit,
        explain=explain,
    )


def _tokenize_query(query_str: str) -> dict[str, str]:
    """Tokenize query string into key=value pairs.

    Handles quoted values containing spaces and special characters.
    """
    tokens: dict[str, str] = {}

    # Regex to match key=value or key='quoted value'
    pattern = r"(\w+)=(?:'([^']*)'|\"([^\"]*)\"|(\S+))"

    for match in re.finditer(pattern, query_str):
        key = match.group(1)
        # Value is in group 2 (single quoted), 3 (double quoted), or 4 (unquoted)
        value = match.group(2) or match.group(3) or match.group(4)
        tokens[key] = value

    return tokens
```

#### Extended Planner (`tools/cq/query/planner.py`)

```python
from __future__ import annotations

from dataclasses import dataclass, field

from tools.cq.query.ir import PatternSpec, Query, Scope


@dataclass(frozen=True)
class AstGrepRule:
    """Compiled ast-grep rule for execution.

    Represents either a pattern query or an entity query translated
    to ast-grep rule format.
    """

    # Core rule
    pattern: str | None = None
    kind: str | None = None

    # Pattern object fields (for disambiguation)
    context: str | None = None
    selector: str | None = None
    strictness: str = "smart"

    # Relational constraints (Phase 1+)
    inside: AstGrepRule | None = None
    has: AstGrepRule | None = None

    def to_yaml_dict(self) -> dict:
        """Convert to ast-grep rule YAML format."""
        rule: dict = {}

        if self.pattern:
            if self.context or self.selector or self.strictness != "smart":
                rule["pattern"] = {
                    "context": self.context or self.pattern,
                    "selector": self.selector,
                    "strictness": self.strictness,
                }
                if not self.context:
                    # Simple pattern with strictness
                    rule["pattern"] = self.pattern
                    rule["strictness"] = self.strictness
            else:
                rule["pattern"] = self.pattern

        if self.kind:
            rule["kind"] = self.kind

        if self.inside:
            rule["inside"] = self.inside.to_yaml_dict()

        if self.has:
            rule["has"] = self.has.to_yaml_dict()

        return rule


@dataclass(frozen=True)
class ToolPlan:
    """Execution plan for a query."""

    # Ripgrep prefilter (optional)
    rg_pattern: str | None = None

    # Scope constraints
    scope: Scope = field(default_factory=Scope)

    # ast-grep execution
    sg_rules: tuple[AstGrepRule, ...] = ()
    sg_record_types: frozenset[str] = frozenset({"def", "call", "import"})

    # Introspection needs
    need_symtable: bool = False
    need_bytecode: bool = False

    # Post-processing
    expand_ops: tuple = ()
    explain: bool = False


def compile_query(query: Query) -> ToolPlan:
    """Compile a Query into a ToolPlan."""
    if query.is_pattern_query:
        return _compile_pattern_query(query)
    else:
        return _compile_entity_query(query)


def _compile_pattern_query(query: Query) -> ToolPlan:
    """Compile a pattern-based query."""
    assert query.pattern_spec is not None

    spec = query.pattern_spec

    # Build ast-grep rule
    rule = AstGrepRule(
        pattern=spec.pattern,
        context=spec.context,
        selector=spec.selector,
        strictness=spec.strictness,
    )

    # Determine ripgrep prefilter from pattern
    rg_pattern = _extract_rg_pattern_from_ast_pattern(spec.pattern)

    return ToolPlan(
        rg_pattern=rg_pattern,
        scope=query.scope,
        sg_rules=(rule,),
        sg_record_types=frozenset({"pattern_match"}),
        expand_ops=query.expand,
        explain=query.explain,
    )


def _extract_rg_pattern_from_ast_pattern(pattern: str) -> str | None:
    """Extract a ripgrep prefilter pattern from an ast-grep pattern.

    Extracts literal strings that can be used to narrow file search.
    """
    # Remove meta-variables
    literals = re.sub(r"\$+[A-Z_][A-Z0-9_]*", "", pattern)

    # Extract longest literal substring
    words = re.findall(r"[a-zA-Z_][a-zA-Z0-9_]*", literals)

    if not words:
        return None

    # Use the longest word as prefilter
    longest = max(words, key=len)
    return longest if len(longest) >= 3 else None
```

#### Extended Executor (`tools/cq/query/executor.py`)

```python
from __future__ import annotations

import json
import subprocess
from pathlib import Path

from tools.cq.core.schema import CqResult, Finding, Anchor
from tools.cq.query.planner import AstGrepRule, ToolPlan


def execute_plan(plan: ToolPlan, root: Path) -> CqResult:
    """Execute a ToolPlan and return results."""
    findings: list[Finding] = []

    # Execute ast-grep rules
    for rule in plan.sg_rules:
        rule_findings = _execute_ast_grep_rule(rule, plan.scope, root)
        findings.extend(rule_findings)

    # Apply limit
    # ... rest of execution logic

    return CqResult(
        run=_build_run_meta(plan),
        summary={"total_findings": len(findings)},
        key_findings=findings[:10],
        sections=[],
        evidence=findings[10:],
        artifacts=[],
    )


def _execute_ast_grep_rule(
    rule: AstGrepRule,
    scope,
    root: Path,
) -> list[Finding]:
    """Execute a single ast-grep rule."""
    # Build inline rule YAML
    rule_yaml = _build_inline_rule(rule)

    # Determine paths to scan
    paths = _scope_to_paths(scope, root)

    # Run ast-grep scan with inline rule
    cmd = [
        "ast-grep",
        "scan",
        "--inline-rules",
        rule_yaml,
        "--json=stream",
        *[str(p) for p in paths],
    ]

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=root,
    )

    # Parse NDJSON output
    findings: list[Finding] = []
    for line in result.stdout.strip().split("\n"):
        if not line:
            continue
        match = json.loads(line)
        finding = _match_to_finding(match)
        findings.append(finding)

    return findings


def _build_inline_rule(rule: AstGrepRule) -> str:
    """Build inline rule YAML for ast-grep scan."""
    import yaml

    rule_dict = {
        "id": "cq-pattern-query",
        "language": "Python",
        "rule": rule.to_yaml_dict(),
        "severity": "info",
        "message": "Pattern match",
    }

    return yaml.dump(rule_dict)


def _match_to_finding(match: dict) -> Finding:
    """Convert ast-grep match to Finding."""
    return Finding(
        category="pattern_match",
        message=match.get("text", "")[:100],
        anchor=Anchor(
            file=match.get("file", ""),
            line=match.get("range", {}).get("start", {}).get("line", 0),
            col=match.get("range", {}).get("start", {}).get("column"),
            end_line=match.get("range", {}).get("end", {}).get("line"),
            end_col=match.get("range", {}).get("end", {}).get("column"),
        ),
        severity="info",
        details={
            "text": match.get("text", ""),
            "meta_variables": match.get("metaVariables", {}),
        },
    )
```

### Target Files

| File | Action | Description |
|------|--------|-------------|
| `tools/cq/query/ir.py` | Modify | Add `PatternSpec`, extend `Query` |
| `tools/cq/query/parser.py` | Modify | Add pattern query parsing |
| `tools/cq/query/planner.py` | Modify | Add `AstGrepRule`, pattern compilation |
| `tools/cq/query/executor.py` | Modify | Add ast-grep inline rule execution |
| `tools/cq/cli.py` | Modify | Update help text for pattern queries |
| `tests/unit/cq/test_pattern_queries.py` | Create | Unit tests for pattern queries |

### Deprecations After Completion

None - this is additive functionality.

### Implementation Checklist

- [ ] Add `PatternSpec` dataclass to `ir.py`
- [ ] Extend `Query` with `pattern_spec` field and validation
- [ ] Add `_tokenize_query()` with quoted value support to `parser.py`
- [ ] Add `_parse_pattern_query()` to `parser.py`
- [ ] Add `AstGrepRule` dataclass with `to_yaml_dict()` to `planner.py`
- [ ] Add `_compile_pattern_query()` to `planner.py`
- [ ] Add `_extract_rg_pattern_from_ast_pattern()` for prefiltering
- [ ] Add `_execute_ast_grep_rule()` with inline rule support to `executor.py`
- [ ] Add `_build_inline_rule()` YAML generation
- [ ] Add `_match_to_finding()` conversion with meta-variable capture
- [ ] Update CLI help text
- [ ] Add unit tests for pattern parsing
- [ ] Add unit tests for pattern compilation
- [ ] Add integration tests with actual ast-grep execution
- [ ] Update SKILL.md documentation

---

## B2. Relational Rule Support

### Rationale
ast-grep's relational rules (`inside`, `has`, `precedes`, `follows`) enable powerful contextual queries that find code based on its surrounding context.

### Query Syntax
```bash
# Find calls inside async functions
/cq q "entity=callsite inside='async def $F($$$)'"

# Find imports that precede class definitions
/cq q "entity=import precedes='class $C'"

# Find functions that have a return statement
/cq q "entity=function has='return $X'"

# Find methods inside specific class
/cq q "entity=method inside='class Config'"

# With stopBy control
/cq q "entity=callsite inside='def $F' stopBy=end"
```

### Key Architectural Elements

#### Extended Query IR (`tools/cq/query/ir.py`)

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

# Relational operator types
RelationalOp = Literal["inside", "has", "precedes", "follows"]

# StopBy modes for relational rules
StopByMode = Literal["neighbor", "end"] | str  # str for custom rule


@dataclass(frozen=True)
class RelationalConstraint:
    """A relational constraint for contextual matching.

    Attributes
    ----------
    operator
        The relational operator (inside, has, precedes, follows)
    pattern
        The pattern to match for the related node
    stop_by
        How far to search (neighbor=1 hop, end=unlimited, or custom rule)
    field
        For inside/has: restrict to specific child field
    """

    operator: RelationalOp
    pattern: str
    stop_by: StopByMode = "neighbor"
    field: str | None = None

    def to_ast_grep_dict(self) -> dict:
        """Convert to ast-grep rule format."""
        result: dict = {"pattern": self.pattern}

        if self.stop_by != "neighbor":
            result["stopBy"] = self.stop_by

        if self.field and self.operator in ("inside", "has"):
            result["field"] = self.field

        return {self.operator: result}


@dataclass(frozen=True)
class Query:
    """Extended Query with relational constraints."""

    entity: EntityType | None = None
    name: str | None = None
    pattern_spec: PatternSpec | None = None

    # Relational constraints (new)
    relational: tuple[RelationalConstraint, ...] = ()

    # ... rest of fields
    expand: tuple[Expander, ...] = ()
    scope: Scope = field(default_factory=Scope)
    fields: tuple[FieldType, ...] = ("def",)
    limit: int | None = None
    explain: bool = False

    def with_relational(self, *constraints: RelationalConstraint) -> Query:
        """Return a new Query with additional relational constraints."""
        return Query(
            entity=self.entity,
            name=self.name,
            pattern_spec=self.pattern_spec,
            relational=self.relational + constraints,
            expand=self.expand,
            scope=self.scope,
            fields=self.fields,
            limit=self.limit,
            explain=self.explain,
        )
```

#### Extended Parser (`tools/cq/query/parser.py`)

```python
def _parse_relational_constraints(
    tokens: dict[str, str],
) -> tuple[RelationalConstraint, ...]:
    """Parse relational constraints from tokens."""
    constraints: list[RelationalConstraint] = []

    for op in ("inside", "has", "precedes", "follows"):
        if op in tokens:
            pattern = tokens.pop(op)

            # Check for associated stopBy
            stop_by_key = f"{op}_stopBy"
            stop_by = tokens.pop(stop_by_key, "neighbor")

            # Check for associated field (inside/has only)
            field_key = f"{op}_field"
            field_val = tokens.pop(field_key, None)

            constraints.append(
                RelationalConstraint(
                    operator=op,  # type: ignore[arg-type]
                    pattern=pattern,
                    stop_by=stop_by,
                    field=field_val,
                )
            )

    return tuple(constraints)


def _parse_entity_query(tokens: dict[str, str]) -> Query:
    """Parse an entity-based query with relational support."""
    entity = tokens.pop("entity")
    if entity not in ("function", "class", "method", "module", "callsite", "import"):
        raise ValueError(f"Unknown entity type: {entity}")

    name = tokens.pop("name", None)

    # Parse relational constraints (new)
    relational = _parse_relational_constraints(tokens)

    # Parse other options
    scope = _parse_scope(tokens)
    fields = _parse_fields(tokens)
    expand = _parse_expanders(tokens)
    limit = int(tokens.pop("limit")) if "limit" in tokens else None
    explain = tokens.pop("explain", "false").lower() == "true"

    return Query(
        entity=entity,  # type: ignore[arg-type]
        name=name,
        relational=relational,
        scope=scope,
        fields=fields,
        expand=expand,
        limit=limit,
        explain=explain,
    )
```

#### Extended Planner (`tools/cq/query/planner.py`)

```python
def _compile_entity_query(query: Query) -> ToolPlan:
    """Compile an entity-based query with relational constraints."""
    # Build base rule for entity
    base_rule = _entity_to_ast_grep_rule(query.entity, query.name)

    # Apply relational constraints
    if query.relational:
        rule = _apply_relational_constraints(base_rule, query.relational)
    else:
        rule = base_rule

    # Determine record types needed
    sg_record_types = _determine_record_types(query)

    return ToolPlan(
        rg_pattern=_determine_rg_pattern(query),
        scope=query.scope,
        sg_rules=(rule,),
        sg_record_types=frozenset(sg_record_types),
        need_symtable=_needs_symtable(query),
        need_bytecode=_needs_bytecode(query),
        expand_ops=query.expand,
        explain=query.explain,
    )


def _entity_to_ast_grep_rule(
    entity: str,
    name: str | None,
) -> AstGrepRule:
    """Convert entity type to ast-grep rule."""
    entity_patterns = {
        "function": "def $FUNC($$$): $$$",
        "class": "class $CLASS($$$): $$$",
        "method": "def $METHOD($$$): $$$",  # Will be constrained by inside
        "callsite": "$FUNC($$$)",
        "import": None,  # Uses kind matching
    }

    pattern = entity_patterns.get(entity)

    if entity == "import":
        # Import uses kind-based matching with any
        return AstGrepRule(
            kind="import_statement",  # Simplified; full impl uses any
        )

    if name and not name.startswith("~"):
        # Exact name match - substitute into pattern
        if entity == "function":
            pattern = f"def {name}($$$): $$$"
        elif entity == "class":
            pattern = f"class {name}($$$): $$$"
        elif entity == "method":
            pattern = f"def {name}($$$): $$$"
        elif entity == "callsite":
            pattern = f"{name}($$$)"

    return AstGrepRule(pattern=pattern)


def _apply_relational_constraints(
    base_rule: AstGrepRule,
    constraints: tuple[RelationalConstraint, ...],
) -> AstGrepRule:
    """Apply relational constraints to a base rule.

    Builds nested rule structure for ast-grep.
    """
    rule = base_rule

    for constraint in constraints:
        if constraint.operator == "inside":
            # Wrap in inside constraint
            inside_rule = AstGrepRule(pattern=constraint.pattern)
            rule = AstGrepRule(
                pattern=rule.pattern,
                kind=rule.kind,
                inside=inside_rule,
            )
        elif constraint.operator == "has":
            # Add has constraint
            has_rule = AstGrepRule(pattern=constraint.pattern)
            rule = AstGrepRule(
                pattern=rule.pattern,
                kind=rule.kind,
                has=has_rule,
            )
        elif constraint.operator in ("precedes", "follows"):
            # Precedes/follows require different handling
            # These are added as sibling constraints
            rule = _add_sibling_constraint(rule, constraint)

    return rule


@dataclass(frozen=True)
class AstGrepRule:
    """Extended with all relational operators."""

    pattern: str | None = None
    kind: str | None = None
    context: str | None = None
    selector: str | None = None
    strictness: str = "smart"

    # Relational
    inside: AstGrepRule | None = None
    has: AstGrepRule | None = None
    precedes: AstGrepRule | None = None
    follows: AstGrepRule | None = None

    # stopBy for relational
    inside_stop_by: str | None = None
    has_stop_by: str | None = None

    def to_yaml_dict(self) -> dict:
        """Convert to ast-grep rule YAML format."""
        rule: dict = {}

        if self.pattern:
            rule["pattern"] = self.pattern

        if self.kind:
            rule["kind"] = self.kind

        # Relational constraints
        if self.inside:
            inside_dict = self.inside.to_yaml_dict()
            if self.inside_stop_by:
                inside_dict["stopBy"] = self.inside_stop_by
            rule["inside"] = inside_dict

        if self.has:
            has_dict = self.has.to_yaml_dict()
            if self.has_stop_by:
                has_dict["stopBy"] = self.has_stop_by
            rule["has"] = has_dict

        if self.precedes:
            rule["precedes"] = self.precedes.to_yaml_dict()

        if self.follows:
            rule["follows"] = self.follows.to_yaml_dict()

        return rule
```

### Target Files

| File | Action | Description |
|------|--------|-------------|
| `tools/cq/query/ir.py` | Modify | Add `RelationalConstraint`, extend `Query` |
| `tools/cq/query/parser.py` | Modify | Add relational constraint parsing |
| `tools/cq/query/planner.py` | Modify | Add relational rule compilation |
| `tests/unit/cq/test_relational_queries.py` | Create | Unit tests |

### Deprecations After Completion

None - additive functionality.

### Implementation Checklist

- [ ] Add `RelationalConstraint` dataclass to `ir.py`
- [ ] Add `relational` field to `Query` dataclass
- [ ] Add `_parse_relational_constraints()` to `parser.py`
- [ ] Update `_parse_entity_query()` to handle relational
- [ ] Extend `AstGrepRule` with `inside`, `has`, `precedes`, `follows`
- [ ] Add `_apply_relational_constraints()` to `planner.py`
- [ ] Add `_entity_to_ast_grep_rule()` for entity→pattern mapping
- [ ] Update `to_yaml_dict()` for relational rules
- [ ] Add unit tests for relational parsing
- [ ] Add unit tests for rule compilation
- [ ] Add integration tests with ast-grep
- [ ] Update documentation

---

## C1. Symtable-Based Scope Analysis

### Rationale
Python's `symtable` module provides compile-time scope classification that reveals closures, captured variables, and scope relationships not visible in AST alone.

### Query Syntax
```bash
# Find closures (functions with free variables)
/cq q "entity=function scope=closure"

# Find functions that capture specific variable
/cq q "entity=function captures=config"

# Find global variable usage
/cq q "entity=assignment scope=global"

# Find nonlocal declarations
/cq q "entity=assignment scope=nonlocal"

# Find functions with cell variables (provide to nested)
/cq q "entity=function has_cells=true"
```

### Key Architectural Elements

#### New Scope Extraction Module (`tools/cq/introspection/symtable_extract.py`)

```python
"""Symtable-based scope extraction for cq queries.

Extracts scope classification, free/cell variables, and scope graph
from Python source using the symtable module.
"""

from __future__ import annotations

import symtable
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Iterator


class ScopeType(Enum):
    """Symbol table scope types (aligned with Python 3.13 SymbolTableType)."""

    MODULE = "module"
    FUNCTION = "function"
    CLASS = "class"
    ANNOTATION = "annotation"
    TYPE_ALIAS = "type_alias"
    TYPE_PARAMETERS = "type_parameters"
    TYPE_VARIABLE = "type_variable"


@dataclass(frozen=True)
class ScopeFact:
    """Facts about a scope extracted from symtable.

    Attributes
    ----------
    scope_id
        Unique identifier (from symtable.get_id())
    scope_type
        Type of scope (MODULE, FUNCTION, CLASS, etc.)
    name
        Scope name (function/class name or "top")
    lineno
        Line number where scope starts
    is_nested
        True if scope is nested inside another non-module scope
    is_optimized
        True if scope uses fast locals
    file
        Source file path
    """

    scope_id: int
    scope_type: ScopeType
    name: str
    lineno: int
    is_nested: bool
    is_optimized: bool
    file: str


@dataclass(frozen=True)
class SymbolFact:
    """Facts about a symbol within a scope.

    Attributes
    ----------
    scope_id
        ID of containing scope
    name
        Symbol name
    is_local
        True if local to this scope
    is_free
        True if captured from enclosing scope (closure variable)
    is_global
        True if global (implicit or explicit)
    is_nonlocal
        True if declared nonlocal
    is_parameter
        True if function parameter
    is_assigned
        True if assigned in this scope
    is_referenced
        True if referenced in this scope
    is_imported
        True if imported
    is_annotated
        True if has annotation
    """

    scope_id: int
    name: str
    is_local: bool
    is_free: bool
    is_global: bool
    is_nonlocal: bool
    is_parameter: bool
    is_assigned: bool
    is_referenced: bool
    is_imported: bool
    is_annotated: bool


@dataclass
class ScopeGraph:
    """Complete scope graph for a file.

    Attributes
    ----------
    scopes
        All scope facts
    symbols
        All symbol facts
    edges
        Parent → child scope edges
    """

    scopes: list[ScopeFact] = field(default_factory=list)
    symbols: list[SymbolFact] = field(default_factory=list)
    edges: list[tuple[int, int]] = field(default_factory=list)  # parent_id → child_id


def extract_scope_graph(source: str, filename: str) -> ScopeGraph:
    """Extract complete scope graph from Python source.

    Parameters
    ----------
    source
        Python source code
    filename
        Filename for error messages

    Returns
    -------
    ScopeGraph
        Extracted scope information
    """
    try:
        st = symtable.symtable(source, filename, "exec")
    except SyntaxError:
        return ScopeGraph()

    graph = ScopeGraph()
    _walk_table(st, graph, filename, parent_id=None)
    return graph


def _walk_table(
    table: symtable.SymbolTable,
    graph: ScopeGraph,
    filename: str,
    parent_id: int | None,
) -> None:
    """Recursively walk symbol table and extract facts."""
    # Map symtable type to our enum
    type_map = {
        "module": ScopeType.MODULE,
        "function": ScopeType.FUNCTION,
        "class": ScopeType.CLASS,
        "annotation": ScopeType.ANNOTATION,
        "type alias": ScopeType.TYPE_ALIAS,
        "type parameters": ScopeType.TYPE_PARAMETERS,
        "type variable": ScopeType.TYPE_VARIABLE,
    }

    # Get type - handle both old string and new enum API
    table_type = table.get_type()
    if hasattr(table_type, "name"):
        # Python 3.13+ returns enum
        scope_type = ScopeType(table_type.name.lower())
    else:
        # Older Python returns string
        scope_type = type_map.get(str(table_type).lower(), ScopeType.MODULE)

    scope_id = table.get_id()

    # Create scope fact
    scope_fact = ScopeFact(
        scope_id=scope_id,
        scope_type=scope_type,
        name=table.get_name(),
        lineno=table.get_lineno(),
        is_nested=table.is_nested(),
        is_optimized=table.is_optimized(),
        file=filename,
    )
    graph.scopes.append(scope_fact)

    # Add edge to parent
    if parent_id is not None:
        graph.edges.append((parent_id, scope_id))

    # Extract symbols
    for name in table.get_identifiers():
        sym = table.lookup(name)
        symbol_fact = SymbolFact(
            scope_id=scope_id,
            name=name,
            is_local=sym.is_local(),
            is_free=sym.is_free(),
            is_global=sym.is_global(),
            is_nonlocal=sym.is_nonlocal(),
            is_parameter=sym.is_parameter(),
            is_assigned=sym.is_assigned(),
            is_referenced=sym.is_referenced(),
            is_imported=sym.is_imported(),
            is_annotated=sym.is_annotated(),
        )
        graph.symbols.append(symbol_fact)

    # Recurse to children
    for child in table.get_children():
        _walk_table(child, graph, filename, parent_id=scope_id)


def get_free_vars(graph: ScopeGraph, scope_id: int) -> list[str]:
    """Get free variables (closure captures) for a scope."""
    return [s.name for s in graph.symbols if s.scope_id == scope_id and s.is_free]


def get_cell_vars(graph: ScopeGraph, scope_id: int) -> list[str]:
    """Get cell variables (provided to nested scopes) for a scope.

    A variable is a cell var if it's local to this scope and free in
    any child scope.
    """
    # Get local vars in this scope
    locals_in_scope = {
        s.name for s in graph.symbols if s.scope_id == scope_id and s.is_local
    }

    # Get child scopes
    child_ids = {child for parent, child in graph.edges if parent == scope_id}

    # Find vars that are free in children
    cell_vars = []
    for sym in graph.symbols:
        if sym.scope_id in child_ids and sym.is_free and sym.name in locals_in_scope:
            cell_vars.append(sym.name)

    return list(set(cell_vars))


def is_closure(graph: ScopeGraph, scope_id: int) -> bool:
    """Check if a scope is a closure (has free variables)."""
    return any(s.is_free for s in graph.symbols if s.scope_id == scope_id)
```

#### Integration with Query System (`tools/cq/query/enrichment.py`)

```python
"""Enrichment module for adding introspection data to findings."""

from __future__ import annotations

from pathlib import Path

from tools.cq.core.schema import Finding
from tools.cq.introspection.symtable_extract import (
    ScopeGraph,
    extract_scope_graph,
    get_cell_vars,
    get_free_vars,
    is_closure,
)


class SymtableEnricher:
    """Enriches findings with symtable scope information."""

    def __init__(self, root: Path) -> None:
        self._root = root
        self._cache: dict[str, ScopeGraph] = {}

    def _get_scope_graph(self, file: str) -> ScopeGraph:
        """Get cached scope graph for a file."""
        if file not in self._cache:
            file_path = self._root / file
            if file_path.exists():
                source = file_path.read_text()
                self._cache[file] = extract_scope_graph(source, file)
            else:
                self._cache[file] = ScopeGraph()
        return self._cache[file]

    def enrich_function_finding(self, finding: Finding) -> Finding:
        """Add scope information to a function finding."""
        if not finding.anchor:
            return finding

        graph = self._get_scope_graph(finding.anchor.file)

        # Find matching scope by line number
        matching_scope = None
        for scope in graph.scopes:
            if scope.lineno == finding.anchor.line:
                matching_scope = scope
                break

        if not matching_scope:
            return finding

        # Add scope details
        details = dict(finding.details) if finding.details else {}
        details["scope_type"] = matching_scope.scope_type.value
        details["is_closure"] = is_closure(graph, matching_scope.scope_id)
        details["free_vars"] = get_free_vars(graph, matching_scope.scope_id)
        details["cell_vars"] = get_cell_vars(graph, matching_scope.scope_id)
        details["is_nested"] = matching_scope.is_nested

        return Finding(
            category=finding.category,
            message=finding.message,
            anchor=finding.anchor,
            severity=finding.severity,
            details=details,
        )


def filter_by_scope(
    findings: list[Finding],
    scope_filter: str,
    enricher: SymtableEnricher,
) -> list[Finding]:
    """Filter findings by scope criteria.

    Parameters
    ----------
    findings
        Findings to filter
    scope_filter
        Scope filter (e.g., "closure", "global", "nonlocal")
    enricher
        Symtable enricher instance

    Returns
    -------
    list[Finding]
        Filtered findings
    """
    result: list[Finding] = []

    for finding in findings:
        enriched = enricher.enrich_function_finding(finding)
        details = enriched.details or {}

        if scope_filter == "closure" and details.get("is_closure"):
            result.append(enriched)
        elif scope_filter == "nested" and details.get("is_nested"):
            result.append(enriched)
        # Add more filters as needed

    return result
```

#### Extended Query IR for Scope Filters

```python
# In tools/cq/query/ir.py

@dataclass(frozen=True)
class ScopeFilter:
    """Filter for scope-based queries.

    Attributes
    ----------
    scope_type
        Filter by scope type (closure, nested, global, etc.)
    captures
        Filter by captured variable name
    has_cells
        Filter for functions that provide cell vars
    """

    scope_type: str | None = None
    captures: str | None = None
    has_cells: bool | None = None


@dataclass(frozen=True)
class Query:
    """Extended with scope filtering."""

    # ... existing fields ...

    # Scope filter (new)
    scope_filter: ScopeFilter | None = None
```

### Target Files

| File | Action | Description |
|------|--------|-------------|
| `tools/cq/introspection/__init__.py` | Create | Package init |
| `tools/cq/introspection/symtable_extract.py` | Create | Symtable extraction |
| `tools/cq/query/enrichment.py` | Modify | Add `SymtableEnricher` |
| `tools/cq/query/ir.py` | Modify | Add `ScopeFilter` |
| `tools/cq/query/parser.py` | Modify | Parse scope filters |
| `tools/cq/query/executor.py` | Modify | Integrate symtable enrichment |
| `tests/unit/cq/test_symtable_extract.py` | Create | Unit tests |

### Deprecations After Completion

| File | Reason |
|------|--------|
| `tools/cq/macros/scopes.py` | Can be reimplemented as saved query using new infrastructure |

The `scopes` macro can be replaced with:
```bash
/cq q "entity=function in=<file> fields=scope"
```

### Implementation Checklist

- [ ] Create `tools/cq/introspection/` package
- [ ] Implement `ScopeFact`, `SymbolFact`, `ScopeGraph` dataclasses
- [ ] Implement `extract_scope_graph()` with recursive table walking
- [ ] Implement `get_free_vars()`, `get_cell_vars()`, `is_closure()` helpers
- [ ] Handle Python 3.13 `SymbolTableType` enum vs older string API
- [ ] Create `SymtableEnricher` class with caching
- [ ] Implement `enrich_function_finding()` to add scope details
- [ ] Add `ScopeFilter` to Query IR
- [ ] Add scope filter parsing to parser
- [ ] Integrate enrichment into executor pipeline
- [ ] Add `scope` field to output fields
- [ ] Write unit tests for symtable extraction
- [ ] Write unit tests for enrichment
- [ ] Add integration tests with real Python files
- [ ] Update documentation

---

## G1. Extended Hazard Types

### Rationale
The current hazard detection is limited to `dynamic_dispatch` and `forwarding`. Many more hazards are detectable via ast-grep patterns.

### New Hazard Types

| Hazard ID | Pattern | Severity | Category |
|-----------|---------|----------|----------|
| `eval_usage` | `eval($$$)` | error | security |
| `exec_usage` | `exec($$$)` | error | security |
| `pickle_load` | `pickle.load($$$)` | error | security |
| `subprocess_shell` | `subprocess.run($$$, shell=True, $$$)` | error | security |
| `sql_format` | `execute($SQL)` where $SQL contains f-string | warning | security |
| `mutable_default` | `def $F($$$, $P=[], $$$)` or `def $F($$$, $P={}, $$$)` | warning | correctness |
| `bare_except` | `except:` | warning | correctness |
| `assert_validation` | `assert $COND` at module level | info | correctness |
| `global_state` | `global $VAR` | info | design |
| `star_import` | `from $M import *` | info | design |

### Key Architectural Elements

#### Hazard Registry (`tools/cq/query/hazards.py`)

```python
"""Extended hazard detection for cq queries.

Provides a registry of hazard patterns and detection logic.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Literal


class HazardCategory(Enum):
    """Hazard categories for filtering."""

    SECURITY = "security"
    CORRECTNESS = "correctness"
    DESIGN = "design"
    PERFORMANCE = "performance"


class HazardSeverity(Enum):
    """Hazard severity levels."""

    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclass(frozen=True)
class HazardSpec:
    """Specification for a detectable hazard.

    Attributes
    ----------
    id
        Unique hazard identifier (e.g., "eval_usage")
    pattern
        ast-grep pattern to match
    message
        Human-readable description
    category
        Hazard category for filtering
    severity
        Severity level
    inside
        Optional: only match if inside this pattern
    not_inside
        Optional: exclude if inside this pattern
    """

    id: str
    pattern: str
    message: str
    category: HazardCategory
    severity: HazardSeverity
    inside: str | None = None
    not_inside: str | None = None

    def to_ast_grep_rule(self) -> dict:
        """Convert to ast-grep rule format."""
        rule: dict = {"pattern": self.pattern}

        if self.inside:
            rule["inside"] = {"pattern": self.inside, "stopBy": "end"}

        if self.not_inside:
            rule["not"] = {"inside": {"pattern": self.not_inside, "stopBy": "end"}}

        return rule


# Built-in hazard registry
BUILTIN_HAZARDS: dict[str, HazardSpec] = {
    # Security hazards
    "eval_usage": HazardSpec(
        id="eval_usage",
        pattern="eval($$$)",
        message="Use of eval() is a security risk",
        category=HazardCategory.SECURITY,
        severity=HazardSeverity.ERROR,
    ),
    "exec_usage": HazardSpec(
        id="exec_usage",
        pattern="exec($$$)",
        message="Use of exec() is a security risk",
        category=HazardCategory.SECURITY,
        severity=HazardSeverity.ERROR,
    ),
    "pickle_load": HazardSpec(
        id="pickle_load",
        pattern="pickle.load($$$)",
        message="pickle.load() can execute arbitrary code",
        category=HazardCategory.SECURITY,
        severity=HazardSeverity.ERROR,
    ),
    "pickle_loads": HazardSpec(
        id="pickle_loads",
        pattern="pickle.loads($$$)",
        message="pickle.loads() can execute arbitrary code",
        category=HazardCategory.SECURITY,
        severity=HazardSeverity.ERROR,
    ),
    "subprocess_shell": HazardSpec(
        id="subprocess_shell",
        pattern="subprocess.$FUNC($$$, shell=True, $$$)",
        message="subprocess with shell=True is vulnerable to injection",
        category=HazardCategory.SECURITY,
        severity=HazardSeverity.ERROR,
    ),
    "os_system": HazardSpec(
        id="os_system",
        pattern="os.system($$$)",
        message="os.system() is vulnerable to shell injection",
        category=HazardCategory.SECURITY,
        severity=HazardSeverity.ERROR,
    ),

    # Correctness hazards
    "mutable_default_list": HazardSpec(
        id="mutable_default_list",
        pattern="def $F($$$, $P=[], $$$): $$$",
        message="Mutable default argument (list) can cause unexpected behavior",
        category=HazardCategory.CORRECTNESS,
        severity=HazardSeverity.WARNING,
    ),
    "mutable_default_dict": HazardSpec(
        id="mutable_default_dict",
        pattern="def $F($$$, $P={}, $$$): $$$",
        message="Mutable default argument (dict) can cause unexpected behavior",
        category=HazardCategory.CORRECTNESS,
        severity=HazardSeverity.WARNING,
    ),
    "mutable_default_set": HazardSpec(
        id="mutable_default_set",
        pattern="def $F($$$, $P=set(), $$$): $$$",
        message="Mutable default argument (set) can cause unexpected behavior",
        category=HazardCategory.CORRECTNESS,
        severity=HazardSeverity.WARNING,
    ),
    "bare_except": HazardSpec(
        id="bare_except",
        pattern="except:",
        message="Bare except catches all exceptions including SystemExit",
        category=HazardCategory.CORRECTNESS,
        severity=HazardSeverity.WARNING,
    ),
    "assert_in_tests_only": HazardSpec(
        id="assert_in_tests_only",
        pattern="assert $COND",
        message="Assert statements are removed with -O flag",
        category=HazardCategory.CORRECTNESS,
        severity=HazardSeverity.INFO,
        not_inside="def test_$$$($$$): $$$",  # OK in test functions
    ),

    # Design hazards
    "star_import": HazardSpec(
        id="star_import",
        pattern="from $M import *",
        message="Star imports pollute namespace and hide dependencies",
        category=HazardCategory.DESIGN,
        severity=HazardSeverity.INFO,
    ),
    "global_statement": HazardSpec(
        id="global_statement",
        pattern="global $VAR",
        message="Global variables make code harder to reason about",
        category=HazardCategory.DESIGN,
        severity=HazardSeverity.INFO,
    ),

    # Existing hazards (keep for compatibility)
    "dynamic_dispatch": HazardSpec(
        id="dynamic_dispatch",
        pattern="getattr($OBJ, $ATTR)",
        message="Dynamic attribute access may resolve at runtime",
        category=HazardCategory.DESIGN,
        severity=HazardSeverity.INFO,
    ),
    "forwarding_args": HazardSpec(
        id="forwarding_args",
        pattern="$FUNC(*$ARGS)",
        message="Argument forwarding obscures call signature",
        category=HazardCategory.DESIGN,
        severity=HazardSeverity.INFO,
    ),
    "forwarding_kwargs": HazardSpec(
        id="forwarding_kwargs",
        pattern="$FUNC(**$KWARGS)",
        message="Keyword argument forwarding obscures call signature",
        category=HazardCategory.DESIGN,
        severity=HazardSeverity.INFO,
    ),
}


def get_hazards_by_category(category: HazardCategory) -> list[HazardSpec]:
    """Get all hazards in a category."""
    return [h for h in BUILTIN_HAZARDS.values() if h.category == category]


def get_hazards_by_severity(severity: HazardSeverity) -> list[HazardSpec]:
    """Get all hazards at or above a severity level."""
    severity_order = [HazardSeverity.INFO, HazardSeverity.WARNING, HazardSeverity.ERROR]
    min_idx = severity_order.index(severity)
    return [
        h for h in BUILTIN_HAZARDS.values()
        if severity_order.index(h.severity) >= min_idx
    ]


def get_security_hazards() -> list[HazardSpec]:
    """Get all security-related hazards."""
    return get_hazards_by_category(HazardCategory.SECURITY)


class HazardDetector:
    """Detects hazards in code using ast-grep patterns."""

    def __init__(self, hazard_ids: list[str] | None = None) -> None:
        """Initialize detector with specific hazard IDs or all."""
        if hazard_ids:
            self._hazards = [BUILTIN_HAZARDS[h] for h in hazard_ids if h in BUILTIN_HAZARDS]
        else:
            self._hazards = list(BUILTIN_HAZARDS.values())

    def get_ast_grep_rules(self) -> list[dict]:
        """Get ast-grep rules for all configured hazards."""
        rules = []
        for hazard in self._hazards:
            rule = {
                "id": f"cq-hazard-{hazard.id}",
                "language": "Python",
                "rule": hazard.to_ast_grep_rule(),
                "severity": hazard.severity.value,
                "message": hazard.message,
            }
            rules.append(rule)
        return rules

    def build_inline_rules_yaml(self) -> str:
        """Build YAML string for ast-grep inline rules."""
        import yaml

        rules = self.get_ast_grep_rules()
        # Join multiple rules with ---
        return "\n---\n".join(yaml.dump(r) for r in rules)
```

#### Integration with Query Execution

```python
# In tools/cq/query/executor.py

from tools.cq.query.hazards import HazardDetector, BUILTIN_HAZARDS


def _execute_hazard_detection(
    plan: ToolPlan,
    root: Path,
    hazard_filter: str | None,
) -> list[Finding]:
    """Execute hazard detection as part of query.

    Parameters
    ----------
    plan
        Execution plan
    root
        Repository root
    hazard_filter
        Filter string: "all", "security", category name, or comma-separated IDs

    Returns
    -------
    list[Finding]
        Hazard findings
    """
    if not hazard_filter:
        return []

    # Determine which hazards to check
    if hazard_filter == "all":
        detector = HazardDetector()
    elif hazard_filter == "security":
        detector = HazardDetector([h.id for h in get_security_hazards()])
    elif hazard_filter in ("correctness", "design", "performance"):
        category = HazardCategory(hazard_filter)
        detector = HazardDetector([h.id for h in get_hazards_by_category(category)])
    else:
        # Comma-separated hazard IDs
        hazard_ids = [h.strip() for h in hazard_filter.split(",")]
        detector = HazardDetector(hazard_ids)

    # Build and execute ast-grep scan
    inline_rules = detector.build_inline_rules_yaml()

    paths = _scope_to_paths(plan.scope, root)

    cmd = [
        "ast-grep",
        "scan",
        "--inline-rules",
        inline_rules,
        "--json=stream",
        *[str(p) for p in paths],
    ]

    result = subprocess.run(cmd, capture_output=True, text=True, cwd=root)

    # Parse results
    findings: list[Finding] = []
    for line in result.stdout.strip().split("\n"):
        if not line:
            continue
        match = json.loads(line)

        # Extract hazard ID from rule ID
        rule_id = match.get("ruleId", "")
        hazard_id = rule_id.replace("cq-hazard-", "") if rule_id.startswith("cq-hazard-") else None

        hazard_spec = BUILTIN_HAZARDS.get(hazard_id) if hazard_id else None

        finding = Finding(
            category=f"hazard:{hazard_id}" if hazard_id else "hazard",
            message=match.get("message", "Hazard detected"),
            anchor=Anchor(
                file=match.get("file", ""),
                line=match.get("range", {}).get("start", {}).get("line", 0),
            ),
            severity=hazard_spec.severity.value if hazard_spec else "warning",
            details={
                "hazard_id": hazard_id,
                "hazard_category": hazard_spec.category.value if hazard_spec else None,
                "text": match.get("text", ""),
            },
        )
        findings.append(finding)

    return findings
```

### Target Files

| File | Action | Description |
|------|--------|-------------|
| `tools/cq/query/hazards.py` | Rewrite | Complete hazard registry and detector |
| `tools/cq/query/ir.py` | Modify | Add hazard filter to Query |
| `tools/cq/query/parser.py` | Modify | Parse hazard filters |
| `tools/cq/query/executor.py` | Modify | Integrate hazard detection |
| `tests/unit/cq/test_hazards.py` | Create | Unit tests |

### Deprecations After Completion

| File | Reason |
|------|--------|
| Inline hazard logic in `tools/cq/query/hazards.py` (old) | Replaced by registry-based approach |

### Implementation Checklist

- [ ] Define `HazardCategory` and `HazardSeverity` enums
- [ ] Create `HazardSpec` dataclass with `to_ast_grep_rule()`
- [ ] Build `BUILTIN_HAZARDS` registry with all patterns
- [ ] Implement `HazardDetector` class
- [ ] Add `get_hazards_by_category()`, `get_hazards_by_severity()` helpers
- [ ] Implement `build_inline_rules_yaml()` for multi-rule YAML
- [ ] Add hazard filter to Query IR
- [ ] Add hazard filter parsing to parser
- [ ] Implement `_execute_hazard_detection()` in executor
- [ ] Handle rule ID → hazard ID mapping in results
- [ ] Add `hazard` category prefix to findings
- [ ] Write unit tests for hazard registry
- [ ] Write unit tests for detector
- [ ] Add integration tests with ast-grep
- [ ] Update documentation with new hazard types

---

# Phase 2: Advanced Queries

---

## A1. Decorator Queries

### Rationale
Decorators significantly affect function behavior (caching, authentication, logging) but are currently not queryable.

### Query Syntax
```bash
# Find all uses of a decorator
/cq q "entity=decorator name=pytest.mark"

# Find functions with specific decorator
/cq q "entity=function decorated_by=cache"

# Find decorator definitions
/cq q "entity=decorator kind=definition"

# Find functions with multiple decorators
/cq q "entity=function decorator_count>2"
```

### Key Architectural Elements

#### Extended IR

```python
# In tools/cq/query/ir.py

# Add decorator to entity types
EntityType = Literal[
    "function", "class", "method", "module", "callsite", "import",
    "decorator",  # New
]

# Add decorator-specific fields
FieldType = Literal[
    "def", "loc", "callers", "callees", "evidence", "hazards", "imports",
    "decorators",  # New - list of decorators on a function/class
    "decorated_functions",  # New - functions using this decorator
]


@dataclass(frozen=True)
class DecoratorFilter:
    """Filter for decorator queries.

    Attributes
    ----------
    decorated_by
        Filter functions/classes by decorator name
    decorator_count_min
        Minimum number of decorators
    decorator_count_max
        Maximum number of decorators
    """

    decorated_by: str | None = None
    decorator_count_min: int | None = None
    decorator_count_max: int | None = None
```

#### ast-grep Patterns for Decorators

```python
# In tools/cq/query/planner.py

DECORATOR_PATTERNS = {
    # Match decorator usage
    "decorator_usage": """
        @$DECORATOR
        def $FUNC($$$): $$$
    """,

    # Match decorator with arguments
    "decorator_with_args": """
        @$DECORATOR($$$)
        def $FUNC($$$): $$$
    """,

    # Match specific decorator by name (template)
    "decorator_by_name": """
        @{name}
        def $FUNC($$$): $$$
    """,

    # Match decorated class
    "decorated_class": """
        @$DECORATOR
        class $CLASS($$$): $$$
    """,
}


def _compile_decorator_query(query: Query) -> ToolPlan:
    """Compile decorator entity query."""
    rules: list[AstGrepRule] = []

    if query.entity == "decorator":
        # Query for decorator definitions or usages
        if query.name:
            # Specific decorator
            if query.name.startswith("~"):
                # Regex match - use general pattern
                rules.append(AstGrepRule(pattern="@$DECORATOR"))
            else:
                # Exact match
                rules.append(AstGrepRule(pattern=f"@{query.name}"))
                rules.append(AstGrepRule(pattern=f"@{query.name}($$$)"))
        else:
            # All decorators
            rules.append(AstGrepRule(pattern="@$DECORATOR"))

    return ToolPlan(
        sg_rules=tuple(rules),
        sg_record_types=frozenset({"decorator"}),
        scope=query.scope,
        expand_ops=query.expand,
        explain=query.explain,
    )
```

#### Decorator Extraction from Findings

```python
# In tools/cq/query/enrichment.py

def extract_decorators_from_function(
    function_finding: Finding,
    source_lines: list[str],
) -> list[str]:
    """Extract decorator names from lines above a function definition.

    Parameters
    ----------
    function_finding
        Finding for a function definition
    source_lines
        Source code lines

    Returns
    -------
    list[str]
        List of decorator names/expressions
    """
    if not function_finding.anchor:
        return []

    decorators: list[str] = []
    line_idx = function_finding.anchor.line - 2  # 0-indexed, start above def

    while line_idx >= 0:
        line = source_lines[line_idx].strip()
        if line.startswith("@"):
            # Extract decorator expression
            decorator = line[1:].split("(")[0].strip()
            decorators.append(decorator)
            line_idx -= 1
        elif not line or line.startswith("#"):
            # Skip blank lines and comments
            line_idx -= 1
        else:
            # Hit non-decorator line
            break

    return list(reversed(decorators))


def enrich_with_decorators(
    findings: list[Finding],
    root: Path,
) -> list[Finding]:
    """Add decorator information to function/class findings."""
    file_cache: dict[str, list[str]] = {}
    result: list[Finding] = []

    for finding in findings:
        if not finding.anchor:
            result.append(finding)
            continue

        # Load source if needed
        file_path = finding.anchor.file
        if file_path not in file_cache:
            full_path = root / file_path
            if full_path.exists():
                file_cache[file_path] = full_path.read_text().splitlines()
            else:
                file_cache[file_path] = []

        source_lines = file_cache[file_path]

        # Extract decorators
        decorators = extract_decorators_from_function(finding, source_lines)

        # Add to details
        details = dict(finding.details) if finding.details else {}
        details["decorators"] = decorators
        details["decorator_count"] = len(decorators)

        result.append(Finding(
            category=finding.category,
            message=finding.message,
            anchor=finding.anchor,
            severity=finding.severity,
            details=details,
        ))

    return result
```

### Target Files

| File | Action | Description |
|------|--------|-------------|
| `tools/cq/query/ir.py` | Modify | Add `decorator` entity, `DecoratorFilter` |
| `tools/cq/query/parser.py` | Modify | Parse decorator queries |
| `tools/cq/query/planner.py` | Modify | Add decorator patterns and compilation |
| `tools/cq/query/enrichment.py` | Modify | Add decorator extraction |
| `tests/unit/cq/test_decorator_queries.py` | Create | Unit tests |

### Deprecations After Completion

None - new functionality.

### Implementation Checklist

- [ ] Add `decorator` to `EntityType`
- [ ] Add `decorators`, `decorated_functions` to `FieldType`
- [ ] Add `DecoratorFilter` dataclass
- [ ] Add decorator filter fields to `Query`
- [ ] Implement `_compile_decorator_query()` in planner
- [ ] Add decorator ast-grep patterns (with/without args)
- [ ] Implement `extract_decorators_from_function()` in enrichment
- [ ] Implement `enrich_with_decorators()` for batch processing
- [ ] Add `decorated_by` filter logic
- [ ] Add `decorator_count` comparison filters
- [ ] Write unit tests for decorator extraction
- [ ] Write integration tests
- [ ] Update documentation

---

## D1. Join Queries (Cross-Entity)

### Rationale
Find relationships between different entity types, such as which imports are used by a function.

### Query Syntax
```bash
# Find imports used by specific function
/cq q "entity=import used_by='function:build_graph_product'"

# Find classes that define specific method
/cq q "entity=class defines='method:__init__'"

# Find functions that raise specific exception
/cq q "entity=function raises='ValueError'"

# Find modules that export specific symbol
/cq q "entity=module exports='GraphProductBuildRequest'"
```

### Key Architectural Elements

#### Join Specification IR

```python
# In tools/cq/query/ir.py

@dataclass(frozen=True)
class JoinTarget:
    """Target for a join query.

    Attributes
    ----------
    entity
        Target entity type
    name
        Target entity name or pattern
    """

    entity: EntityType
    name: str

    @classmethod
    def parse(cls, spec: str) -> JoinTarget:
        """Parse join target from 'entity:name' format."""
        if ":" not in spec:
            raise ValueError(f"Join target must be 'entity:name', got: {spec}")
        entity, name = spec.split(":", 1)
        return cls(entity=entity, name=name)  # type: ignore[arg-type]


@dataclass(frozen=True)
class JoinConstraint:
    """A join constraint linking entities.

    Attributes
    ----------
    relationship
        Type of relationship (used_by, defines, raises, exports)
    target
        Target entity to join with
    """

    relationship: Literal["used_by", "defines", "raises", "exports", "imports", "calls"]
    target: JoinTarget


@dataclass(frozen=True)
class Query:
    """Extended with join constraints."""

    # ... existing fields ...

    # Join constraints (new)
    joins: tuple[JoinConstraint, ...] = ()

    def with_join(self, relationship: str, target: str) -> Query:
        """Add a join constraint."""
        join = JoinConstraint(
            relationship=relationship,  # type: ignore[arg-type]
            target=JoinTarget.parse(target),
        )
        return Query(
            entity=self.entity,
            name=self.name,
            joins=self.joins + (join,),
            # ... copy other fields ...
        )
```

#### Join Executor

```python
# In tools/cq/query/join_executor.py

"""Join query execution for cross-entity queries."""

from __future__ import annotations

from pathlib import Path

from tools.cq.core.schema import Finding
from tools.cq.index.def_index import DefIndex
from tools.cq.query.ir import JoinConstraint, Query


class JoinExecutor:
    """Executes join queries across entity types."""

    def __init__(self, root: Path, index: DefIndex) -> None:
        self._root = root
        self._index = index

    def execute_join(
        self,
        base_findings: list[Finding],
        join: JoinConstraint,
    ) -> list[Finding]:
        """Filter base findings by join constraint.

        Parameters
        ----------
        base_findings
            Initial findings to filter
        join
            Join constraint to apply

        Returns
        -------
        list[Finding]
            Filtered findings that satisfy the join
        """
        if join.relationship == "used_by":
            return self._filter_used_by(base_findings, join.target)
        elif join.relationship == "defines":
            return self._filter_defines(base_findings, join.target)
        elif join.relationship == "raises":
            return self._filter_raises(base_findings, join.target)
        elif join.relationship == "exports":
            return self._filter_exports(base_findings, join.target)
        else:
            return base_findings

    def _filter_used_by(
        self,
        findings: list[Finding],
        target: JoinTarget,
    ) -> list[Finding]:
        """Filter imports used by a specific function/class.

        For each import finding, check if it's referenced in the target.
        """
        # Find the target function/class
        target_fns = self._index.find_function_by_name(target.name)
        if not target_fns:
            return []

        target_fn = target_fns[0]

        # Get imports used in target function's file
        # This requires analyzing the function body for name references
        # Simplified: check if import name appears in function's local scope

        result: list[Finding] = []
        for finding in findings:
            import_name = finding.details.get("name") if finding.details else None
            if import_name and self._is_import_used_in_function(import_name, target_fn):
                result.append(finding)

        return result

    def _is_import_used_in_function(self, import_name: str, fn) -> bool:
        """Check if an import is used in a function.

        Uses symtable to check if the name is referenced in the function's scope.
        """
        # Implementation would use symtable to check references
        # Placeholder for now
        return True

    def _filter_defines(
        self,
        findings: list[Finding],
        target: JoinTarget,
    ) -> list[Finding]:
        """Filter classes that define a specific method."""
        result: list[Finding] = []

        for finding in findings:
            # Get class name from finding
            if finding.category != "definition" or finding.details.get("kind") != "class":
                continue

            class_name = finding.details.get("name")
            if not class_name:
                continue

            # Check if class defines the target method
            classes = self._index.find_class_by_name(class_name)
            for cls in classes:
                method_names = [m.name for m in cls.methods]
                if target.name in method_names:
                    result.append(finding)
                    break

        return result

    def _filter_raises(
        self,
        findings: list[Finding],
        target: JoinTarget,
    ) -> list[Finding]:
        """Filter functions that raise a specific exception."""
        # This would require analyzing function bodies for raise statements
        # Could use ast-grep pattern: 'raise {target.name}($$$)'
        return findings  # Placeholder

    def _filter_exports(
        self,
        findings: list[Finding],
        target: JoinTarget,
    ) -> list[Finding]:
        """Filter modules that export a specific symbol."""
        result: list[Finding] = []

        for finding in findings:
            if finding.category != "module":
                continue

            module_file = finding.anchor.file if finding.anchor else None
            if not module_file:
                continue

            # Check __all__ or top-level definitions
            # Simplified: check if symbol is defined at module level
            if self._module_exports_symbol(module_file, target.name):
                result.append(finding)

        return result

    def _module_exports_symbol(self, module_file: str, symbol: str) -> bool:
        """Check if a module exports a symbol."""
        # Would parse module and check __all__ or top-level definitions
        return True  # Placeholder
```

### Target Files

| File | Action | Description |
|------|--------|-------------|
| `tools/cq/query/ir.py` | Modify | Add `JoinTarget`, `JoinConstraint` |
| `tools/cq/query/parser.py` | Modify | Parse join constraints |
| `tools/cq/query/join_executor.py` | Create | Join execution logic |
| `tools/cq/query/executor.py` | Modify | Integrate join executor |
| `tests/unit/cq/test_join_queries.py` | Create | Unit tests |

### Deprecations After Completion

None - new functionality.

### Implementation Checklist

- [ ] Add `JoinTarget` dataclass with `parse()` method
- [ ] Add `JoinConstraint` dataclass
- [ ] Add `joins` field to `Query`
- [ ] Add join constraint parsing (used_by, defines, raises, exports)
- [ ] Create `JoinExecutor` class
- [ ] Implement `_filter_used_by()` with symtable integration
- [ ] Implement `_filter_defines()` using DefIndex
- [ ] Implement `_filter_raises()` with ast-grep pattern
- [ ] Implement `_filter_exports()` with __all__ analysis
- [ ] Integrate `JoinExecutor` into main executor
- [ ] Write unit tests
- [ ] Add integration tests
- [ ] Update documentation

---

# Phase 3: Analysis & Output

---

## F1. Visualization Formats (Mermaid)

### Rationale
Graph-based results are easier to understand with visual diagrams.

### Query Syntax
```bash
# Output as Mermaid diagram
/cq q "entity=function expand=callers format=mermaid"

# Output as DOT graph
/cq q "entity=module expand=imports format=dot"
```

### Key Architectural Elements

#### Mermaid Renderer

```python
# In tools/cq/core/renderers/mermaid.py

"""Mermaid diagram renderer for cq results."""

from __future__ import annotations

from tools.cq.core.schema import CqResult, Finding


def render_mermaid_flowchart(
    result: CqResult,
    direction: str = "TB",
) -> str:
    """Render findings as Mermaid flowchart.

    Parameters
    ----------
    result
        Query result to render
    direction
        Graph direction (TB, BT, LR, RL)

    Returns
    -------
    str
        Mermaid diagram source
    """
    lines: list[str] = [f"flowchart {direction}"]

    # Track nodes and edges
    nodes: set[str] = set()
    edges: list[tuple[str, str, str]] = []  # (from, to, label)

    # Process findings
    for finding in result.key_findings + result.evidence:
        node_id = _finding_to_node_id(finding)
        nodes.add(node_id)

        # Check for caller/callee relationships in details
        details = finding.details or {}

        if "callers" in details:
            for caller in details["callers"]:
                caller_id = _name_to_node_id(caller)
                nodes.add(caller_id)
                edges.append((caller_id, node_id, "calls"))

        if "callees" in details:
            for callee in details["callees"]:
                callee_id = _name_to_node_id(callee)
                nodes.add(callee_id)
                edges.append((node_id, callee_id, "calls"))

    # Generate node definitions
    for node_id in sorted(nodes):
        label = node_id.replace("_", " ")
        lines.append(f"    {node_id}[{label}]")

    # Generate edges
    for from_id, to_id, label in edges:
        lines.append(f"    {from_id} -->|{label}| {to_id}")

    return "\n".join(lines)


def render_mermaid_class_diagram(result: CqResult) -> str:
    """Render class hierarchy as Mermaid class diagram."""
    lines: list[str] = ["classDiagram"]

    for finding in result.key_findings:
        if finding.category == "definition":
            details = finding.details or {}
            if details.get("kind") == "class":
                class_name = details.get("name", "Unknown")
                lines.append(f"    class {class_name}")

                # Add methods
                for method in details.get("methods", []):
                    lines.append(f"    {class_name} : {method}()")

                # Add inheritance
                for base in details.get("bases", []):
                    lines.append(f"    {base} <|-- {class_name}")

    return "\n".join(lines)


def _finding_to_node_id(finding: Finding) -> str:
    """Convert finding to valid Mermaid node ID."""
    name = finding.details.get("name", "unknown") if finding.details else "unknown"
    return _name_to_node_id(name)


def _name_to_node_id(name: str) -> str:
    """Convert name to valid Mermaid node ID."""
    # Replace invalid characters
    return name.replace(".", "_").replace("-", "_").replace(" ", "_")
```

#### Format Selection in CLI

```python
# In tools/cq/cli.py

from tools.cq.core.renderers.mermaid import (
    render_mermaid_flowchart,
    render_mermaid_class_diagram,
)


def format_output(result: CqResult, fmt: str) -> str:
    """Format result according to requested format."""
    if fmt == "md":
        return render_markdown(result)
    elif fmt == "json":
        return render_json(result)
    elif fmt == "mermaid":
        return render_mermaid_flowchart(result)
    elif fmt == "mermaid-class":
        return render_mermaid_class_diagram(result)
    elif fmt == "dot":
        return render_dot(result)
    else:
        return render_markdown(result)
```

### Target Files

| File | Action | Description |
|------|--------|-------------|
| `tools/cq/core/renderers/__init__.py` | Create | Renderers package |
| `tools/cq/core/renderers/mermaid.py` | Create | Mermaid renderer |
| `tools/cq/core/renderers/dot.py` | Create | DOT/Graphviz renderer |
| `tools/cq/cli.py` | Modify | Add format selection |
| `tools/cq/query/ir.py` | Modify | Add format to Query |
| `tests/unit/cq/test_renderers.py` | Create | Unit tests |

### Deprecations After Completion

None - new functionality.

### Implementation Checklist

- [ ] Create `tools/cq/core/renderers/` package
- [ ] Implement `render_mermaid_flowchart()` for call graphs
- [ ] Implement `render_mermaid_class_diagram()` for class hierarchies
- [ ] Implement node ID sanitization
- [ ] Add edge label support
- [ ] Create DOT renderer for Graphviz output
- [ ] Add `format` field to Query IR
- [ ] Add `--format` CLI option
- [ ] Update `format_output()` dispatcher
- [ ] Write unit tests for each renderer
- [ ] Add examples to documentation

---

# Phase 4: Performance & Caching

---

## H1. Incremental Query Cache

### Rationale
Expensive computations (ast-grep scans, symtable compilation) should be cached and invalidated only when files change.

### Key Architectural Elements

#### Cache Implementation

```python
# In tools/cq/index/query_cache.py

"""Incremental query cache for cq.

Caches expensive computations with file-hash-based invalidation.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class CacheEntry:
    """A cached computation result.

    Attributes
    ----------
    key
        Cache key (query + file hash)
    value
        Cached result (JSON-serializable)
    file_hash
        Hash of source file when cached
    timestamp
        Cache time (Unix timestamp)
    """

    key: str
    value: Any
    file_hash: str
    timestamp: float


class QueryCache:
    """SQLite-backed query cache with file hash invalidation."""

    def __init__(self, cache_dir: Path) -> None:
        self._cache_dir = cache_dir
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        self._db_path = cache_dir / "query_cache.db"
        self._conn = self._init_db()

    def _init_db(self) -> sqlite3.Connection:
        """Initialize cache database."""
        conn = sqlite3.connect(self._db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS cache (
                key TEXT PRIMARY KEY,
                value TEXT,
                file_hash TEXT,
                timestamp REAL
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_file_hash ON cache(file_hash)
        """)
        conn.commit()
        return conn

    def get(self, key: str, file_path: Path) -> Any | None:
        """Get cached value if still valid.

        Parameters
        ----------
        key
            Cache key
        file_path
            Source file to check hash against

        Returns
        -------
        Any | None
            Cached value if valid, None if miss or stale
        """
        current_hash = self._compute_file_hash(file_path)

        cursor = self._conn.execute(
            "SELECT value, file_hash FROM cache WHERE key = ?",
            (key,),
        )
        row = cursor.fetchone()

        if row is None:
            return None

        cached_value, cached_hash = row

        if cached_hash != current_hash:
            # File changed, invalidate
            self._conn.execute("DELETE FROM cache WHERE key = ?", (key,))
            self._conn.commit()
            return None

        return json.loads(cached_value)

    def set(self, key: str, value: Any, file_path: Path) -> None:
        """Store value in cache.

        Parameters
        ----------
        key
            Cache key
        value
            Value to cache (must be JSON-serializable)
        file_path
            Source file to compute hash from
        """
        import time

        file_hash = self._compute_file_hash(file_path)
        value_json = json.dumps(value)

        self._conn.execute(
            """
            INSERT OR REPLACE INTO cache (key, value, file_hash, timestamp)
            VALUES (?, ?, ?, ?)
            """,
            (key, value_json, file_hash, time.time()),
        )
        self._conn.commit()

    def invalidate_file(self, file_path: Path) -> int:
        """Invalidate all cache entries for a file.

        Returns number of entries invalidated.
        """
        file_hash = self._compute_file_hash(file_path)
        cursor = self._conn.execute(
            "DELETE FROM cache WHERE file_hash = ?",
            (file_hash,),
        )
        self._conn.commit()
        return cursor.rowcount

    def clear(self) -> None:
        """Clear entire cache."""
        self._conn.execute("DELETE FROM cache")
        self._conn.commit()

    def stats(self) -> dict[str, int]:
        """Get cache statistics."""
        cursor = self._conn.execute("SELECT COUNT(*) FROM cache")
        total = cursor.fetchone()[0]

        cursor = self._conn.execute(
            "SELECT COUNT(DISTINCT file_hash) FROM cache"
        )
        files = cursor.fetchone()[0]

        return {"total_entries": total, "unique_files": files}

    def _compute_file_hash(self, file_path: Path) -> str:
        """Compute hash of file contents."""
        if not file_path.exists():
            return "missing"

        hasher = hashlib.sha256()
        hasher.update(file_path.read_bytes())
        return hasher.hexdigest()[:16]


def make_cache_key(query_type: str, file: str, params: dict) -> str:
    """Generate cache key from query parameters."""
    params_str = json.dumps(params, sort_keys=True)
    key_input = f"{query_type}:{file}:{params_str}"
    return hashlib.sha256(key_input.encode()).hexdigest()[:32]
```

#### Integration with Executor

```python
# In tools/cq/query/executor.py

from tools.cq.index.diskcache_query_cache import QueryCache, make_cache_key


class CachedExecutor:
    """Query executor with caching support."""

    def __init__(self, root: Path, cache: QueryCache | None = None) -> None:
        self._root = root
        self._cache = cache or QueryCache(root / ".cq" / "cache")

    def execute_with_cache(
        self,
        plan: ToolPlan,
        no_cache: bool = False,
    ) -> CqResult:
        """Execute plan with caching."""
        if no_cache:
            return self._execute_uncached(plan)

        # Try cache for each file
        cached_findings: list[Finding] = []
        files_to_scan: list[Path] = []

        for file_path in _scope_to_paths(plan.scope, self._root):
            cache_key = make_cache_key(
                "ast_grep",
                str(file_path.relative_to(self._root)),
                {"rules": [r.to_yaml_dict() for r in plan.sg_rules]},
            )

            cached = self._cache.get(cache_key, file_path)
            if cached is not None:
                cached_findings.extend(_deserialize_findings(cached))
            else:
                files_to_scan.append(file_path)

        # Scan uncached files
        if files_to_scan:
            new_findings = self._execute_ast_grep(plan.sg_rules, files_to_scan)

            # Cache results per file
            for file_path, findings in _group_findings_by_file(new_findings).items():
                cache_key = make_cache_key(
                    "ast_grep",
                    file_path,
                    {"rules": [r.to_yaml_dict() for r in plan.sg_rules]},
                )
                self._cache.set(
                    cache_key,
                    _serialize_findings(findings),
                    self._root / file_path,
                )

            cached_findings.extend(new_findings)

        return self._build_result(cached_findings, plan)
```

### Target Files

| File | Action | Description |
|------|--------|-------------|
| `tools/cq/index/query_cache.py` | Create | Cache implementation |
| `tools/cq/query/executor.py` | Modify | Add caching integration |
| `tools/cq/cli.py` | Modify | Add --no-cache, --cache-stats flags |
| `tests/unit/cq/test_query_cache.py` | Create | Unit tests |

### Deprecations After Completion

| File | Reason |
|------|--------|
| `tools/cq/index/file_hash.py` | Functionality merged into query_cache |
| `tools/cq/index/sqlite_cache.py` | Replaced by query_cache |

### Implementation Checklist

- [ ] Create `QueryCache` class with SQLite backend
- [ ] Implement `get()` with hash validation
- [ ] Implement `set()` with timestamp
- [ ] Implement `invalidate_file()` for manual invalidation
- [ ] Implement `stats()` for monitoring
- [ ] Create `make_cache_key()` helper
- [ ] Add `CachedExecutor` wrapper class
- [ ] Implement per-file caching in executor
- [ ] Add `--no-cache` CLI flag
- [ ] Add `--cache-stats` CLI flag
- [ ] Write unit tests for cache
- [ ] Write integration tests
- [ ] Document cache behavior

---

## Integration with Existing Macros

After completing Phase 1-4, existing macros can be reimplemented as saved query compositions:

| Macro | Query Equivalent | Deprecation Target |
|-------|-----------------|-------------------|
| `calls` | `entity=callsite name=<fn>` | `macros/calls.py` |
| `impact` | `entity=function name=<fn> expand=callers param=<p> fields=taint` | `macros/impact.py` |
| `sig-impact` | `entity=callsite name=<fn> fields=binding_analysis` | `macros/sig_impact.py` |
| `imports` | `entity=import [circular=true]` | `macros/imports.py` |
| `exceptions` | `entity=exception` | `macros/exceptions.py` |
| `side-effects` | `entity=assignment scope=module has_call=true` | `macros/side_effects.py` |
| `scopes` | `entity=function fields=scope` | `macros/scopes.py` |
| `async-hazards` | `entity=function is_async=true hazards=blocking` | `macros/async_hazards.py` |
| `bytecode-surface` | `entity=function fields=bytecode` | `macros/bytecode.py` |

**Deprecation Strategy:**
1. Implement query equivalents in Phase 1-2
2. Create macro wrapper that calls query system
3. Mark direct macro code as deprecated
4. Remove macro implementations in Phase 4+

---

## Summary: Target Files by Phase

### Phase 1: Foundation (14 files)
- Modify: `ir.py`, `parser.py`, `planner.py`, `executor.py`, `cli.py`, `enrichment.py`, `hazards.py`
- Create: `introspection/__init__.py`, `introspection/symtable_extract.py`
- Create tests: 4 test files

### Phase 2: Advanced (8 files)
- Modify: `ir.py`, `parser.py`, `planner.py`, `enrichment.py`
- Create: `join_executor.py`
- Create tests: 2 test files

### Phase 3: Output (6 files)
- Create: `renderers/__init__.py`, `renderers/mermaid.py`, `renderers/dot.py`
- Modify: `cli.py`, `ir.py`
- Create tests: 1 test file

### Phase 4: Performance (4 files)
- Create: `query_cache.py`
- Modify: `executor.py`, `cli.py`
- Create tests: 1 test file
- Delete: `file_hash.py`, `sqlite_cache.py` (functionality merged)

### Total Deprecations After All Phases
- `tools/cq/macros/scopes.py` - replaced by scope query
- `tools/cq/index/file_hash.py` - merged into query_cache
- `tools/cq/index/sqlite_cache.py` - replaced by query_cache
- Future: All macro files once query equivalents are stable

---

## Appendix: ast-grep Feature Matrix

| Feature | Phase | Implementation Status |
|---------|-------|----------------------|
| Pattern syntax | B1 | Phase 1 |
| Meta-variables | B1 | Phase 1 |
| Multi-meta ($$$) | B1 | Phase 1 |
| Relational: inside | B2 | Phase 1 |
| Relational: has | B2 | Phase 1 |
| Relational: precedes | B2 | Phase 1 |
| Relational: follows | B2 | Phase 1 |
| Composite: all | B3 | Phase 2 |
| Composite: any | B3 | Phase 2 |
| Composite: not | B3 | Phase 2 |
| Strictness modes | B5 | Phase 1 |
| Pattern objects | B1 | Phase 1 |
| Context/selector | B1 | Phase 1 |
| stopBy control | B2 | Phase 1 |
| Field constraints | B2 | Phase 2 |
| JSON streaming | All | Already used |
| Inline rules | B1 | Phase 1 |

---

## Appendix: Python Introspection Feature Matrix

| Module | Feature | Phase | Implementation Status |
|--------|---------|-------|----------------------|
| symtable | Scope classification | C1 | Phase 1 |
| symtable | Free/cell vars | C1 | Phase 1 |
| symtable | Type scopes (3.13) | C1 | Phase 1 |
| symtable | Symbol flags | C1 | Phase 1 |
| dis | Instruction stream | C2 | Phase 3 |
| dis | Stack effect | C2 | Phase 3 |
| dis | Exception table | C2 | Phase 3 |
| dis | Positions | C2 | Phase 3 |
| inspect | Signature | C3 | Phase 2 |
| inspect | Annotations | C4 | Phase 2 |
| inspect | getmembers_static | C3 | Phase 2 |
| inspect | Unwrap chain | A1 | Phase 2 |
