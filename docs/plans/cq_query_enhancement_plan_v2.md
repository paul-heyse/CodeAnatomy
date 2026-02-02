# CQ Query Enhancement Plan v2 - Detailed Implementation Design

## Executive Summary

This document details comprehensive enhancements to the `cq` tool and `cq q` query command, derived from deep analysis of:

1. **ast-grep full feature surface** - Pattern objects, relational operators, composite rules, strictness modes
2. **Python 3.13 introspection libraries** - `dis` (bytecode), `inspect` (runtime), `symtable` (compile-time scopes)
3. **Cross-layer correlation techniques** - Combining multiple evidence sources for higher-confidence analysis

This builds on the existing `cq_query_enhancement_plan.md` with additional capabilities discovered through systematic documentation review.

---

## Part I: ast-grep Feature Parity

### 1.1 Pattern Object Support (Context + Selector)

**Gap:** Current pattern support is string-only. ast-grep supports full pattern objects for disambiguation.

**ast-grep Capability:**
- Patterns parse as standalone files by default
- `context` provides surrounding code for proper parsing
- `selector` extracts the specific node kind to match

**Enhancement:**

```bash
# Problem: "a: 123" alone doesn't parse correctly as JSON pair
/cq q "pattern='\"a\": 123'"  # FAILS

# Solution: Pattern object with context
/cq q "pattern.context='{ \"a\": 123 }' pattern.selector=pair"

# Class field vs assignment disambiguation (JS/TS)
/cq q "pattern.context='class A { \$FIELD = \$INIT }' pattern.selector=field_definition"
```

#### Key Architectural Elements

```python
# tools/cq/query/ir.py - Extended PatternSpec

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

StrictnessMode = Literal["cst", "smart", "ast", "relaxed", "signature"]


@dataclass(frozen=True)
class PatternSpec:
    """Enhanced pattern specification supporting full ast-grep pattern objects.

    Attributes
    ----------
    pattern
        The ast-grep pattern to match (e.g., 'def $F($$$)')
    context
        Surrounding code for proper parsing (required for ambiguous patterns)
    selector
        Node kind to extract from parsed context (e.g., 'field_definition')
    strictness
        Matching strictness mode (cst, smart, ast, relaxed, signature)
    """

    pattern: str
    context: str | None = None
    selector: str | None = None
    strictness: StrictnessMode = "smart"

    def requires_yaml_rule(self) -> bool:
        """Check if pattern needs full YAML rule (vs simple CLI pattern)."""
        return self.context is not None or self.selector is not None

    def to_yaml_dict(self) -> dict:
        """Convert to ast-grep YAML rule format.

        Returns
        -------
        dict
            Dictionary suitable for YAML serialization as inline rule.
        """
        if self.context:
            rule = {
                "pattern": {
                    "context": self.context,
                }
            }
            if self.selector:
                rule["pattern"]["selector"] = self.selector
            if self.strictness != "smart":
                rule["pattern"]["strictness"] = self.strictness
        else:
            rule = {"pattern": self.pattern}
            if self.strictness != "smart":
                rule["strictness"] = self.strictness

        return rule
```

```python
# tools/cq/query/parser.py - Extended pattern parsing

def _parse_pattern_object(tokens: dict[str, str]) -> PatternSpec:
    """Parse pattern object from tokens.

    Handles both simple patterns and full pattern objects with context/selector.

    Parameters
    ----------
    tokens
        Tokenized key-value pairs from query string.

    Returns
    -------
    PatternSpec
        Parsed pattern specification.
    """
    # Check for pattern object notation (pattern.context, pattern.selector)
    context = tokens.get("pattern.context")
    selector = tokens.get("pattern.selector")
    strictness = tokens.get("pattern.strictness", tokens.get("strictness", "smart"))

    if context:
        # Pattern object: use context as the pattern source
        return PatternSpec(
            pattern=context,  # context IS the parseable pattern
            context=context,
            selector=selector,
            strictness=strictness,
        )

    # Simple pattern string
    pattern = tokens.get("pattern", "")
    return PatternSpec(
        pattern=pattern,
        context=None,
        selector=selector,
        strictness=strictness,
    )
```

```python
# tools/cq/query/planner.py - YAML rule generation

import tempfile
import yaml
from pathlib import Path


def _generate_inline_rule(
    pattern_spec: PatternSpec,
    rule_id: str = "cq_inline",
    language: str = "python",
) -> str:
    """Generate ast-grep inline rule YAML.

    Parameters
    ----------
    pattern_spec
        Pattern specification to convert.
    rule_id
        Unique rule identifier.
    language
        Target language for pattern matching.

    Returns
    -------
    str
        YAML string suitable for --inline-rules.
    """
    rule = {
        "id": rule_id,
        "language": language,
        "severity": "hint",
        "message": "cq pattern match",
        "rule": pattern_spec.to_yaml_dict(),
    }
    return yaml.dump(rule, default_flow_style=False)


def _execute_pattern_with_context(
    pattern_spec: PatternSpec,
    paths: list[Path],
    language: str = "python",
) -> list[dict]:
    """Execute pattern query using inline rules for context support.

    Parameters
    ----------
    pattern_spec
        Pattern specification (may include context/selector).
    paths
        Files/directories to search.
    language
        Target language.

    Returns
    -------
    list[dict]
        Parsed match results from ast-grep.
    """
    rule_yaml = _generate_inline_rule(pattern_spec, language=language)

    cmd = [
        "ast-grep",
        "scan",
        "--inline-rules",
        rule_yaml,
        "--json=stream",
        *[str(p) for p in paths],
    ]

    # Execute and parse results
    # ... implementation details
```

#### Target Files

| File | Changes |
|------|---------|
| `tools/cq/query/ir.py` | Extend `PatternSpec` with context/selector fields |
| `tools/cq/query/parser.py` | Add `_parse_pattern_object()` for `pattern.context` syntax |
| `tools/cq/query/planner.py` | Add `_generate_inline_rule()` for YAML generation |
| `tools/cq/query/executor.py` | Route context patterns through `--inline-rules` |
| `tools/cq/query/sg_parser.py` | Handle inline rule output format |

#### Deprecations

| Item | Reason |
|------|--------|
| None | This is additive enhancement |

#### Implementation Checklist

- [ ] Add `context`, `selector` fields to `PatternSpec` in `ir.py`
- [ ] Add `requires_yaml_rule()` method to `PatternSpec`
- [ ] Add `to_yaml_dict()` method to `PatternSpec`
- [ ] Extend parser to handle `pattern.context`, `pattern.selector` tokens
- [ ] Add `_generate_inline_rule()` function in planner
- [ ] Update executor to use `--inline-rules` when pattern has context
- [ ] Add unit tests for pattern object parsing
- [ ] Add integration tests for context disambiguation
- [ ] Update SKILL.md documentation

---

### 1.2 Strictness Mode Control

**ast-grep Strictness Levels:**

| Level | Behavior | Use Case |
|-------|----------|----------|
| `cst` | Match all nodes exactly | Whitespace-sensitive |
| `smart` | Match all except trivial source nodes | Default |
| `ast` | Match only AST nodes | Structure-focused |
| `relaxed` | Match AST nodes except comments | Comment-tolerant |
| `signature` | Match AST without text | Signature matching |

**Enhancement:**

```bash
# Strict CST matching (whitespace matters)
/cq q "pattern='def foo( x )' strictness=cst"

# Relaxed matching (ignore comments)
/cq q "pattern='def \$F(\$\$\$)' strictness=relaxed"

# Signature-only matching (function shape)
/cq q "pattern='def process(data, config)' strictness=signature"
```

#### Key Architectural Elements

```python
# tools/cq/query/ir.py - Already has StrictnessMode, ensure full support

StrictnessMode = Literal["cst", "smart", "ast", "relaxed", "signature"]

# Strictness descriptions for explain mode
STRICTNESS_DESCRIPTIONS: dict[StrictnessMode, str] = {
    "cst": "Match all nodes exactly including whitespace/trivial nodes",
    "smart": "Match all nodes except trivial source nodes (default)",
    "ast": "Match only named AST nodes, ignore unnamed nodes",
    "relaxed": "Match AST nodes, ignore comments",
    "signature": "Match AST structure without text content",
}
```

```python
# tools/cq/query/planner.py - Strictness in CLI vs YAML

def _build_ast_grep_command(
    pattern_spec: PatternSpec,
    paths: list[str],
    language: str = "python",
) -> list[str]:
    """Build ast-grep CLI command with strictness support.

    Note: CLI --strictness only works with `run`, not `scan`.
    For full strictness support, use inline rules.
    """
    if pattern_spec.requires_yaml_rule() or pattern_spec.strictness != "smart":
        # Use inline rules for full strictness support
        rule_yaml = _generate_inline_rule(pattern_spec, language=language)
        return [
            "ast-grep", "scan",
            "--inline-rules", rule_yaml,
            "--json=stream",
            *paths,
        ]
    else:
        # Simple pattern: use run command
        return [
            "ast-grep", "run",
            "-p", pattern_spec.pattern,
            "-l", language,
            "--json=stream",
            *paths,
        ]
```

#### Target Files

| File | Changes |
|------|---------|
| `tools/cq/query/ir.py` | Add `STRICTNESS_DESCRIPTIONS` constant |
| `tools/cq/query/parser.py` | Parse `strictness=` token |
| `tools/cq/query/planner.py` | Route non-smart strictness through inline rules |

#### Deprecations

| Item | Reason |
|------|--------|
| None | Additive enhancement |

#### Implementation Checklist

- [ ] Add strictness descriptions to ir.py
- [ ] Verify parser handles `strictness=` token
- [ ] Update planner to use inline rules for non-smart strictness
- [ ] Add explain mode output for strictness
- [ ] Add unit tests for each strictness level
- [ ] Document strictness modes in SKILL.md

---

### 1.3 Multi-Meta Variables (`$$$`)

**Current:** Basic `$X` single-node wildcards
**Enhancement:** Full `$$$NAME` support for zero-or-more nodes

```bash
# Match functions with any number of arguments
/cq q "pattern='def \$FUNC(\$\$\$ARGS)'"

# Match after specific args (anchor pattern)
/cq q "pattern='def \$F(\$FIRST, \$\$\$REST)'"

# Match with trailing patterns
/cq q "pattern='print(\$\$\$ARGS, sep=\$SEP)'"
```

**Behavior:** `$$$` is non-greedy - stops when the next pattern element can match.

#### Key Architectural Elements

```python
# tools/cq/query/sg_parser.py - Multi-metavar extraction

from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass
class MetaVarCapture:
    """Captured metavariable from pattern match.

    Attributes
    ----------
    name
        Metavariable name (without $ prefix)
    kind
        'single' for $X, 'multi' for $$$X, 'unnamed' for $$X
    text
        Captured text content
    nodes
        For multi captures, list of individual node texts
    """

    name: str
    kind: Literal["single", "multi", "unnamed"]
    text: str
    nodes: list[str] | None = None


def parse_metavariables(match_result: dict) -> dict[str, MetaVarCapture]:
    """Extract metavariable captures from ast-grep match result.

    Parameters
    ----------
    match_result
        Raw match result from ast-grep JSON output.

    Returns
    -------
    dict[str, MetaVarCapture]
        Map of metavariable name to capture info.
    """
    captures = {}

    # ast-grep returns metaVariables with 'single' or 'multi' keys
    meta_vars = match_result.get("metaVariables", {})

    for name, capture_info in meta_vars.get("single", {}).items():
        captures[name] = MetaVarCapture(
            name=name,
            kind="single",
            text=capture_info.get("text", ""),
        )

    for name, capture_list in meta_vars.get("multi", {}).items():
        # Multi captures are lists of nodes
        node_texts = [c.get("text", "") for c in capture_list]
        captures[name] = MetaVarCapture(
            name=name,
            kind="multi",
            text=", ".join(node_texts),
            nodes=node_texts,
        )

    return captures


def validate_pattern_metavars(pattern: str) -> list[str]:
    """Extract and validate metavariable names from pattern.

    Parameters
    ----------
    pattern
        ast-grep pattern string.

    Returns
    -------
    list[str]
        List of metavariable names found.

    Raises
    ------
    ValueError
        If invalid metavariable syntax found.
    """
    # Valid: $NAME, $$$NAME, $$NAME, $_NAME
    # Invalid: $lowercase, $123, $KEBAB-CASE
    valid_pattern = r'\$\$?\$?[A-Z_][A-Z0-9_]*'
    invalid_pattern = r'\$[a-z]|\$[0-9]|\$[A-Z]+-'

    if re.search(invalid_pattern, pattern):
        raise ValueError(f"Invalid metavariable in pattern: {pattern}")

    return re.findall(valid_pattern, pattern)
```

#### Target Files

| File | Changes |
|------|---------|
| `tools/cq/query/sg_parser.py` | Add `MetaVarCapture`, `parse_metavariables()` |
| `tools/cq/query/ir.py` | Add `MetaVarCapture` dataclass |
| `tools/cq/query/enrichment.py` | Include metavar captures in findings |

#### Deprecations

| Item | Reason |
|------|--------|
| None | Additive enhancement |

#### Implementation Checklist

- [ ] Add `MetaVarCapture` dataclass to ir.py
- [ ] Implement `parse_metavariables()` in sg_parser.py
- [ ] Add `validate_pattern_metavars()` for pattern validation
- [ ] Update enrichment to expose metavar captures
- [ ] Add tests for $$$, $$, $_ metavar variants
- [ ] Document metavar syntax in SKILL.md

---

### 1.4 Unnamed Node Capture (`$$VAR`)

**Problem:** `$X` only captures named Tree-sitter nodes. Operators and punctuation are often unnamed.

**Enhancement:**

```bash
# Capture operator in binary expression
/cq q "pattern='\$L \$\$OP \$R' kind=binary_expression"

# Find specific comparison types
/cq q "pattern='\$X \$\$CMP \$Y' \$\$CMP=~'^[<>=]'"
```

#### Key Architectural Elements

```python
# tools/cq/query/ir.py - Metavar filter support

@dataclass(frozen=True)
class MetaVarFilter:
    """Filter on captured metavariable values.

    Attributes
    ----------
    name
        Metavariable name (without $ prefix)
    pattern
        Regex pattern to match captured text
    negate
        If True, match when pattern does NOT match
    """

    name: str
    pattern: str
    negate: bool = False

    def matches(self, capture: MetaVarCapture) -> bool:
        """Check if capture matches this filter."""
        import re
        match = bool(re.search(self.pattern, capture.text))
        return not match if self.negate else match
```

```python
# tools/cq/query/parser.py - MetaVarFilter parsing

def _parse_metavar_filters(tokens: dict[str, str]) -> list[MetaVarFilter]:
    """Parse metavariable filter tokens like $$OP=~'^[<>=]'.

    Parameters
    ----------
    tokens
        Tokenized query string.

    Returns
    -------
    list[MetaVarFilter]
        Filters to apply on metavariable captures.
    """
    filters = []

    for key, value in tokens.items():
        # Match $$NAME or $NAME with =~ or != prefix
        if key.startswith("$"):
            # Extract name (strip $ prefixes)
            name = key.lstrip("$")
            negate = value.startswith("!")

            if "=~" in value or value.startswith("~"):
                # Regex filter
                pattern = value.lstrip("!~").lstrip("=~")
                filters.append(MetaVarFilter(name=name, pattern=pattern, negate=negate))

    return filters
```

#### Target Files

| File | Changes |
|------|---------|
| `tools/cq/query/ir.py` | Add `MetaVarFilter` dataclass |
| `tools/cq/query/parser.py` | Add `_parse_metavar_filters()` |
| `tools/cq/query/executor.py` | Filter results by metavar captures |

#### Deprecations

| Item | Reason |
|------|--------|
| None | Additive enhancement |

#### Implementation Checklist

- [ ] Add `MetaVarFilter` dataclass to ir.py
- [ ] Implement `_parse_metavar_filters()` in parser.py
- [ ] Add post-filter step in executor for metavar filtering
- [ ] Add tests for unnamed node captures
- [ ] Add tests for metavar filtering
- [ ] Document $$ syntax in SKILL.md

---

### 1.5 Non-Capturing Wildcards (`$_`)

**Enhancement:** `$_NAME` wildcards that don't enforce equality on reuse.

```bash
# Match any function called with any argument (can be different)
/cq q "pattern='\$_F(\$_G)'"

# vs equality-enforcing (must be same)
/cq q "pattern='\$F(\$F)'"  # Only matches f(f), g(g), etc.
```

#### Key Architectural Elements

```python
# tools/cq/query/ir.py - Document non-capturing behavior

# Non-capturing metavariables:
# - Names starting with underscore: $_NAME, $$$_ARGS
# - Do NOT enforce equality on repeated use
# - Useful for "any X" patterns without capture overhead
#
# ast-grep handles this natively - no special handling needed in cq
# We just need to document and validate the syntax
```

#### Target Files

| File | Changes |
|------|---------|
| `tools/cq/query/ir.py` | Add documentation comments |
| `tools/cq/query/sg_parser.py` | Skip non-capturing vars in capture extraction |

#### Deprecations

| Item | Reason |
|------|--------|
| None | Additive enhancement |

#### Implementation Checklist

- [ ] Document non-capturing metavar behavior in ir.py
- [ ] Update `parse_metavariables()` to skip $_NAME captures
- [ ] Add tests verifying non-equality behavior
- [ ] Document $_ syntax in SKILL.md

---

## Part II: Full Relational Query Support

### 2.1 Complete Relational Operators

**ast-grep Relational Rules:**

| Operator | Meaning | Supports `field` | Supports `stopBy` |
|----------|---------|------------------|-------------------|
| `inside` | Target inside ancestor | Yes | Yes |
| `has` | Target has descendant | Yes | Yes |
| `precedes` | Target before sibling | No | Yes |
| `follows` | Target after sibling | No | Yes |

**Enhancement:**

```bash
# Find console.log after await
/cq q "pattern='console.log(\$_)' follows='await \$X'"

# Find return before finally
/cq q "pattern='return \$X' precedes='finally:'"

# Find calls inside specific method
/cq q "pattern='\$X()' inside='def handle_request'"
```

#### Key Architectural Elements

```python
# tools/cq/query/ir.py - Enhanced RelationalConstraint

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

RelationalOp = Literal["inside", "has", "precedes", "follows"]
StopByMode = Literal["neighbor", "end"]


@dataclass(frozen=True)
class RelationalConstraint:
    """Relational constraint for structural queries.

    Supports ast-grep relational rules: inside, has, precedes, follows.

    Attributes
    ----------
    operator
        Relational operator (inside, has, precedes, follows)
    pattern
        Pattern that must satisfy the relation
    stop_by
        When to stop searching:
        - "neighbor": Direct parent/child/sibling only (default)
        - "end": Search to root/leaf/edge
        - Custom pattern string: Stop when pattern matches
    field_name
        Optional field name for field-specific matching (inside/has only)
    """

    operator: RelationalOp
    pattern: str
    stop_by: StopByMode | str = "neighbor"
    field_name: str | None = None

    def __post_init__(self) -> None:
        """Validate constraint configuration."""
        # precedes/follows don't support field
        if self.operator in ("precedes", "follows") and self.field_name:
            raise ValueError(f"{self.operator} does not support field constraint")

    def to_ast_grep_dict(self) -> dict:
        """Convert to ast-grep rule format.

        Returns
        -------
        dict
            Dictionary suitable for ast-grep YAML rule.
        """
        inner: dict = {"pattern": self.pattern}

        if self.stop_by != "neighbor":
            inner["stopBy"] = self.stop_by

        if self.field_name and self.operator in ("inside", "has"):
            inner["field"] = self.field_name

        return {self.operator: inner}


@dataclass(frozen=True)
class Query:
    """Extended Query with full relational support."""

    entity: EntityType | None = None
    name: str | None = None
    pattern_spec: PatternSpec | None = None
    scope: Scope = field(default_factory=Scope)
    expand: tuple[Expander, ...] = ()
    fields: tuple[FieldType, ...] = ()

    # Relational constraints - multiple allowed
    inside: RelationalConstraint | None = None
    has: RelationalConstraint | None = None
    precedes: RelationalConstraint | None = None
    follows: RelationalConstraint | None = None

    # Additional relational constraints (multiple of same type)
    relational_constraints: tuple[RelationalConstraint, ...] = ()

    def get_all_relational_constraints(self) -> list[RelationalConstraint]:
        """Get all relational constraints including named and additional."""
        constraints = []
        if self.inside:
            constraints.append(self.inside)
        if self.has:
            constraints.append(self.has)
        if self.precedes:
            constraints.append(self.precedes)
        if self.follows:
            constraints.append(self.follows)
        constraints.extend(self.relational_constraints)
        return constraints
```

```python
# tools/cq/query/parser.py - Relational constraint parsing

def _parse_relational_constraints(tokens: dict[str, str]) -> list[RelationalConstraint]:
    """Parse relational constraint tokens.

    Handles: inside='pattern', has='pattern' stopBy=end, etc.

    Parameters
    ----------
    tokens
        Tokenized query string.

    Returns
    -------
    list[RelationalConstraint]
        Parsed relational constraints.
    """
    constraints = []

    for op in ("inside", "has", "precedes", "follows"):
        if op in tokens:
            pattern = tokens[op]
            stop_by = tokens.get(f"{op}.stopBy", tokens.get("stopBy", "neighbor"))
            field_name = tokens.get(f"{op}.field", tokens.get("field"))

            constraints.append(RelationalConstraint(
                operator=op,
                pattern=pattern,
                stop_by=stop_by,
                field_name=field_name,
            ))

    return constraints
```

```python
# tools/cq/query/planner.py - Relational rule generation

def _build_relational_rule(
    pattern_spec: PatternSpec,
    constraints: list[RelationalConstraint],
    language: str = "python",
) -> dict:
    """Build ast-grep rule with relational constraints.

    Parameters
    ----------
    pattern_spec
        Primary pattern to match.
    constraints
        Relational constraints to apply.
    language
        Target language.

    Returns
    -------
    dict
        Complete ast-grep rule dictionary.
    """
    rule = pattern_spec.to_yaml_dict()

    # Add relational constraints
    for constraint in constraints:
        rule.update(constraint.to_ast_grep_dict())

    return {
        "id": "cq_relational",
        "language": language,
        "severity": "hint",
        "message": "cq relational match",
        "rule": rule,
    }
```

#### Target Files

| File | Changes |
|------|---------|
| `tools/cq/query/ir.py` | Enhance `RelationalConstraint`, add validation |
| `tools/cq/query/parser.py` | Add `_parse_relational_constraints()` |
| `tools/cq/query/planner.py` | Add `_build_relational_rule()` |
| `tools/cq/query/executor.py` | Route relational queries through inline rules |

#### Deprecations

| Item | Reason |
|------|--------|
| None | Additive enhancement |

#### Implementation Checklist

- [ ] Enhance `RelationalConstraint` with validation
- [ ] Add `get_all_relational_constraints()` to Query
- [ ] Implement `_parse_relational_constraints()`
- [ ] Implement `_build_relational_rule()`
- [ ] Update executor for relational rule execution
- [ ] Add tests for each operator (inside, has, precedes, follows)
- [ ] Add tests for stopBy modes
- [ ] Document relational syntax in SKILL.md

---

### 2.2 `stopBy` Control

**Purpose:** Control search depth in relational queries.

| Value | Behavior |
|-------|----------|
| `neighbor` | Only direct parent/child/sibling (default) |
| `end` | Search to root/leaf/edge |
| `'<pattern>'` | Stop when pattern matches |

**Enhancement:**

```bash
# Search ancestors but stop at function boundary
/cq q "pattern='\$X' inside='class \$C' stopBy='def \$F'"

# Search all descendants
/cq q "pattern='return \$X' has='await' stopBy=end"

# Stop at specific pattern
/cq q "pattern='\$X' inside='try:' stopBy='except:'"
```

**Note:** `stopBy` is inclusive - if stop rule and relational match hit same node, it still counts.

#### Key Architectural Elements

```python
# tools/cq/query/ir.py - stopBy validation

def validate_stop_by(stop_by: str, operator: RelationalOp) -> StopByMode | str:
    """Validate and normalize stopBy value.

    Parameters
    ----------
    stop_by
        Raw stopBy value from query.
    operator
        Relational operator being configured.

    Returns
    -------
    StopByMode | str
        Normalized stopBy value.

    Raises
    ------
    ValueError
        If stopBy value is invalid.
    """
    if stop_by in ("neighbor", "end"):
        return stop_by

    # Custom pattern - validate it's a valid pattern string
    if stop_by.startswith("'") and stop_by.endswith("'"):
        return stop_by[1:-1]  # Strip quotes

    # Assume it's a pattern
    return stop_by
```

#### Target Files

| File | Changes |
|------|---------|
| `tools/cq/query/ir.py` | Add `validate_stop_by()` |
| `tools/cq/query/parser.py` | Handle `stopBy=` and `*.stopBy=` tokens |

#### Deprecations

| Item | Reason |
|------|--------|
| None | Additive enhancement |

#### Implementation Checklist

- [ ] Add `validate_stop_by()` function
- [ ] Update parser to handle stopBy tokens
- [ ] Add tests for stopBy=neighbor, stopBy=end
- [ ] Add tests for custom pattern stopBy
- [ ] Document stopBy in SKILL.md

---

### 2.3 Field-Scoped Searches

**Purpose:** Constrain relational searches to specific AST fields.

```bash
# Match dict keys only, not values
/cq q "pattern='prototype' field=key inside='{\$\$\$}'"

# Match function return type annotations
/cq q "pattern='List[\$T]' field=returns inside='def \$F'"

# Match decorator arguments specifically
/cq q "pattern='\$X' field=arguments inside='@\$DEC(\$\$\$)'"
```

#### Key Architectural Elements

```python
# tools/cq/query/ir.py - Field validation

# Common Python AST field names for reference
PYTHON_AST_FIELDS: set[str] = {
    # Function
    "name", "args", "body", "decorator_list", "returns", "type_params",
    # Arguments
    "posonlyargs", "args", "vararg", "kwonlyargs", "kw_defaults", "kwarg", "defaults",
    # Class
    "bases", "keywords", "body", "decorator_list",
    # Dict
    "keys", "values",
    # Call
    "func", "args", "keywords",
    # Subscript
    "value", "slice",
    # Attribute
    "value", "attr",
    # Assign
    "targets", "value",
    # If/While/For
    "test", "body", "orelse",
    # Try
    "body", "handlers", "orelse", "finalbody",
}


def validate_field_name(field_name: str) -> str:
    """Validate AST field name.

    Parameters
    ----------
    field_name
        Field name to validate.

    Returns
    -------
    str
        Validated field name.

    Raises
    ------
    ValueError
        If field name is not recognized (warning only - may be valid).
    """
    if field_name not in PYTHON_AST_FIELDS:
        import warnings
        warnings.warn(f"Unrecognized AST field '{field_name}' - may be valid for target language")
    return field_name
```

#### Target Files

| File | Changes |
|------|---------|
| `tools/cq/query/ir.py` | Add `PYTHON_AST_FIELDS`, `validate_field_name()` |
| `tools/cq/query/parser.py` | Handle `field=` and `*.field=` tokens |

#### Deprecations

| Item | Reason |
|------|--------|
| None | Additive enhancement |

#### Implementation Checklist

- [ ] Add `PYTHON_AST_FIELDS` constant
- [ ] Add `validate_field_name()` function
- [ ] Update parser to handle field tokens
- [ ] Add tests for field-scoped searches
- [ ] Document field syntax in SKILL.md

---

### 2.4 nthChild Positional Matching

**ast-grep CSS-like Positioning:**

```bash
# Match third parameter
/cq q "entity=parameter nthChild=3"

# Match odd-indexed items (An+B formula)
/cq q "pattern='\$X' nthChild='2n+1'"

# Match from end (reverse)
/cq q "pattern='\$STMT' nthChild.position=-1 nthChild.reverse=true"

# Filter siblings before counting
/cq q "pattern='\$X' nthChild.position=1 nthChild.ofRule='kind=function_declaration'"
```

**Rules:**
- Index is 1-based (CSS-style)
- Only considers named nodes
- `ofRule` filters sibling list before position test

#### Key Architectural Elements

```python
# tools/cq/query/ir.py - NthChild specification

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class NthChildSpec:
    """CSS-like nthChild positional specification.

    Attributes
    ----------
    position
        Position specifier:
        - Integer: exact position (1-based)
        - String: An+B formula (e.g., "2n+1" for odd)
    reverse
        If True, count from end
    of_rule
        Optional rule to filter siblings before counting
    """

    position: int | str
    reverse: bool = False
    of_rule: dict | None = None

    def __post_init__(self) -> None:
        """Validate position specification."""
        if isinstance(self.position, str):
            # Validate An+B formula
            if not re.match(r'^-?\d*n?([+-]\d+)?$', self.position):
                raise ValueError(f"Invalid nthChild formula: {self.position}")

    def to_ast_grep_dict(self) -> dict:
        """Convert to ast-grep nthChild format."""
        if isinstance(self.position, int) and not self.reverse and not self.of_rule:
            # Simple case: just return the number
            return {"nthChild": self.position}

        # Complex case: object format
        spec: dict = {"position": self.position}
        if self.reverse:
            spec["reverse"] = True
        if self.of_rule:
            spec["ofRule"] = self.of_rule

        return {"nthChild": spec}
```

```python
# tools/cq/query/parser.py - NthChild parsing

def _parse_nth_child(tokens: dict[str, str]) -> NthChildSpec | None:
    """Parse nthChild specification from tokens.

    Parameters
    ----------
    tokens
        Tokenized query string.

    Returns
    -------
    NthChildSpec | None
        Parsed nthChild spec or None if not present.
    """
    if "nthChild" not in tokens and "nthChild.position" not in tokens:
        return None

    # Simple form: nthChild=3
    if "nthChild" in tokens:
        value = tokens["nthChild"]
        try:
            return NthChildSpec(position=int(value))
        except ValueError:
            # An+B formula
            return NthChildSpec(position=value)

    # Object form: nthChild.position=..., nthChild.reverse=..., nthChild.ofRule=...
    position = tokens.get("nthChild.position", "1")
    try:
        position = int(position)
    except ValueError:
        pass  # Keep as string (formula)

    reverse = tokens.get("nthChild.reverse", "false").lower() == "true"

    of_rule = None
    if "nthChild.ofRule" in tokens:
        # Parse ofRule as a mini-rule spec
        of_rule_str = tokens["nthChild.ofRule"]
        if of_rule_str.startswith("kind="):
            of_rule = {"kind": of_rule_str[5:]}
        else:
            of_rule = {"pattern": of_rule_str}

    return NthChildSpec(position=position, reverse=reverse, of_rule=of_rule)
```

#### Target Files

| File | Changes |
|------|---------|
| `tools/cq/query/ir.py` | Add `NthChildSpec` dataclass |
| `tools/cq/query/parser.py` | Add `_parse_nth_child()` |
| `tools/cq/query/planner.py` | Include nthChild in rule generation |

#### Deprecations

| Item | Reason |
|------|--------|
| None | Additive enhancement |

#### Implementation Checklist

- [ ] Add `NthChildSpec` dataclass
- [ ] Implement `_parse_nth_child()`
- [ ] Add nthChild to rule generation
- [ ] Add tests for integer positions
- [ ] Add tests for An+B formulas
- [ ] Add tests for reverse and ofRule
- [ ] Document nthChild syntax in SKILL.md

---

## Part III: Composite Query Logic

### 3.1 Ordered `all` (Metavariable-Safe)

**Problem:** Rule object fields are unordered. Metavariable capture order matters.

**Enhancement:**

```bash
# Ensure $A captured before used in inside check
/cq q "all=['pattern=foo(\$A)', 'inside=bar(\$A)']"

# Multiple conditions with guaranteed order
/cq q "all=['kind=call_expression', 'has=await', 'inside=async def \$F']"
```

#### Key Architectural Elements

```python
# tools/cq/query/ir.py - Composite rule types

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

CompositeOp = Literal["all", "any", "not"]


@dataclass(frozen=True)
class CompositeRule:
    """Composite rule combining multiple sub-rules.

    Attributes
    ----------
    operator
        Composite operator: all, any, not
    rules
        Sub-rules to combine (list for all/any, single for not)
    """

    operator: CompositeOp
    rules: tuple["Query | CompositeRule | dict", ...]

    def to_ast_grep_dict(self) -> dict:
        """Convert to ast-grep composite rule format.

        Returns
        -------
        dict
            ast-grep rule dictionary.
        """
        if self.operator == "not":
            # 'not' takes a single rule
            if len(self.rules) != 1:
                raise ValueError("'not' operator requires exactly one sub-rule")
            sub_rule = self.rules[0]
            if isinstance(sub_rule, dict):
                return {"not": sub_rule}
            elif hasattr(sub_rule, "to_ast_grep_dict"):
                return {"not": sub_rule.to_ast_grep_dict()}
            else:
                raise ValueError(f"Invalid sub-rule type: {type(sub_rule)}")

        # 'all' and 'any' take lists
        sub_rules = []
        for rule in self.rules:
            if isinstance(rule, dict):
                sub_rules.append(rule)
            elif hasattr(rule, "to_ast_grep_dict"):
                sub_rules.append(rule.to_ast_grep_dict())
            else:
                raise ValueError(f"Invalid sub-rule type: {type(rule)}")

        return {self.operator: sub_rules}


@dataclass(frozen=True)
class Query:
    """Extended Query with composite support."""

    # ... existing fields ...

    # Composite rules
    all_rules: tuple["Query", ...] | None = None
    any_rules: tuple["Query", ...] | None = None
    not_rule: "Query" | None = None

    @property
    def is_composite_query(self) -> bool:
        """Check if this is a composite query."""
        return bool(self.all_rules or self.any_rules or self.not_rule)
```

```python
# tools/cq/query/parser.py - Composite rule parsing

import ast


def _parse_composite_rules(tokens: dict[str, str]) -> tuple[
    tuple["Query", ...] | None,  # all
    tuple["Query", ...] | None,  # any
    "Query | None",              # not
]:
    """Parse composite rule tokens.

    Parameters
    ----------
    tokens
        Tokenized query string.

    Returns
    -------
    tuple
        (all_rules, any_rules, not_rule)
    """
    all_rules = None
    any_rules = None
    not_rule = None

    # Parse all=['query1', 'query2', ...]
    if "all" in tokens:
        all_str = tokens["all"]
        # Parse as Python list literal
        rule_strs = ast.literal_eval(all_str)
        all_rules = tuple(parse_query(s) for s in rule_strs)

    # Parse any=['query1', 'query2', ...]
    if "any" in tokens:
        any_str = tokens["any"]
        rule_strs = ast.literal_eval(any_str)
        any_rules = tuple(parse_query(s) for s in rule_strs)

    # Parse not.X=Y or not='query'
    not_tokens = {k[4:]: v for k, v in tokens.items() if k.startswith("not.")}
    if not_tokens:
        not_rule = parse_query(_tokens_to_string(not_tokens))
    elif "not" in tokens:
        not_rule = parse_query(tokens["not"])

    return all_rules, any_rules, not_rule


def _tokens_to_string(tokens: dict[str, str]) -> str:
    """Convert tokens back to query string."""
    return " ".join(f"{k}='{v}'" if " " in v else f"{k}={v}" for k, v in tokens.items())
```

#### Target Files

| File | Changes |
|------|---------|
| `tools/cq/query/ir.py` | Add `CompositeRule`, update `Query` |
| `tools/cq/query/parser.py` | Add `_parse_composite_rules()` |
| `tools/cq/query/planner.py` | Handle composite rule generation |
| `tools/cq/query/executor.py` | Execute composite queries |

#### Deprecations

| Item | Reason |
|------|--------|
| None | Additive enhancement |

#### Implementation Checklist

- [ ] Add `CompositeRule` dataclass
- [ ] Add composite fields to `Query`
- [ ] Implement `_parse_composite_rules()`
- [ ] Implement composite rule YAML generation
- [ ] Add tests for `all` with metavar ordering
- [ ] Add tests for `any` alternatives
- [ ] Add tests for `not` negation
- [ ] Document composite syntax in SKILL.md

---

### 3.2 `any` Alternatives

```bash
# Match any logging pattern
/cq q "any=['pattern=logger.\$M(\$\$\$)', 'pattern=print(\$\$\$)', 'pattern=logging.\$M(\$\$\$)']"

# Variable declaration styles
/cq q "any=['pattern=var \$X', 'pattern=let \$X', 'pattern=const \$X']"
```

*Implementation covered in 3.1*

---

### 3.3 `not` Negation

```bash
# Find console.log except debug messages
/cq q "pattern='console.log(\$X)' not='console.log(\"debug\")'"

# Functions without docstrings
/cq q "entity=function not.has='\"\"\"'"

# Classes without __init__
/cq q "entity=class not.has='def __init__'"
```

*Implementation covered in 3.1*

---

### 3.4 `matches` Rule References

**Purpose:** Reusable rule composition.

```bash
# Define reusable patterns
/cq rule-define security.eval "pattern='eval(\$X)'"
/cq rule-define security.exec "pattern='exec(\$X)'"
/cq rule-define security.all "any=[matches=security.eval, matches=security.exec]"

# Use composite rule
/cq q "matches=security.all"
```

#### Key Architectural Elements

```python
# tools/cq/query/rule_registry.py - NEW FILE

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tools.cq.query.ir import Query

# Default location for rule definitions
DEFAULT_RULE_DIR = Path(".cq/rules")


@dataclass
class RuleDefinition:
    """Stored rule definition.

    Attributes
    ----------
    id
        Unique rule identifier (e.g., "security.eval")
    query_string
        Original query string
    description
        Optional description
    tags
        Tags for categorization
    """

    id: str
    query_string: str
    description: str = ""
    tags: tuple[str, ...] = ()


@dataclass
class RuleRegistry:
    """Registry for reusable rule definitions.

    Attributes
    ----------
    rules
        Map of rule ID to definition
    rule_dir
        Directory containing rule files
    """

    rules: dict[str, RuleDefinition] = field(default_factory=dict)
    rule_dir: Path = DEFAULT_RULE_DIR

    def load_from_dir(self) -> None:
        """Load rules from rule directory."""
        if not self.rule_dir.exists():
            return

        for rule_file in self.rule_dir.glob("*.json"):
            with open(rule_file) as f:
                data = json.load(f)
                rule = RuleDefinition(**data)
                self.rules[rule.id] = rule

    def save_rule(self, rule: RuleDefinition) -> None:
        """Save rule definition to file."""
        self.rule_dir.mkdir(parents=True, exist_ok=True)

        # Sanitize filename
        filename = rule.id.replace(".", "_") + ".json"
        rule_file = self.rule_dir / filename

        with open(rule_file, "w") as f:
            json.dump({
                "id": rule.id,
                "query_string": rule.query_string,
                "description": rule.description,
                "tags": list(rule.tags),
            }, f, indent=2)

        self.rules[rule.id] = rule

    def resolve(self, rule_id: str) -> RuleDefinition:
        """Resolve rule by ID.

        Parameters
        ----------
        rule_id
            Rule identifier (e.g., "security.eval")

        Returns
        -------
        RuleDefinition
            Resolved rule definition.

        Raises
        ------
        KeyError
            If rule not found.
        """
        if rule_id not in self.rules:
            self.load_from_dir()

        return self.rules[rule_id]

    def list_rules(self, pattern: str = "*") -> list[RuleDefinition]:
        """List rules matching pattern.

        Parameters
        ----------
        pattern
            Glob-style pattern (e.g., "security.*")

        Returns
        -------
        list[RuleDefinition]
            Matching rules.
        """
        import fnmatch

        self.load_from_dir()
        return [r for r in self.rules.values() if fnmatch.fnmatch(r.id, pattern)]


# Global registry instance
_registry: RuleRegistry | None = None


def get_registry() -> RuleRegistry:
    """Get global rule registry."""
    global _registry
    if _registry is None:
        _registry = RuleRegistry()
        _registry.load_from_dir()
    return _registry
```

```python
# tools/cq/cli.py - rule-define command

import click


@click.command("rule-define")
@click.argument("rule_id")
@click.argument("query_string")
@click.option("--description", "-d", default="", help="Rule description")
@click.option("--tags", "-t", multiple=True, help="Rule tags")
def rule_define(rule_id: str, query_string: str, description: str, tags: tuple[str, ...]) -> None:
    """Define a reusable query rule.

    RULE_ID: Unique identifier (e.g., security.eval)
    QUERY_STRING: Query string to save
    """
    from tools.cq.query.rule_registry import RuleDefinition, get_registry

    registry = get_registry()
    rule = RuleDefinition(
        id=rule_id,
        query_string=query_string,
        description=description,
        tags=tags,
    )
    registry.save_rule(rule)
    click.echo(f"Saved rule: {rule_id}")


@click.command("rule-list")
@click.option("--pattern", "-p", default="*", help="Filter pattern (glob)")
def rule_list(pattern: str) -> None:
    """List defined rules."""
    from tools.cq.query.rule_registry import get_registry

    registry = get_registry()
    rules = registry.list_rules(pattern)

    for rule in sorted(rules, key=lambda r: r.id):
        click.echo(f"{rule.id}: {rule.query_string}")
        if rule.description:
            click.echo(f"  {rule.description}")
```

#### Target Files

| File | Changes |
|------|---------|
| `tools/cq/query/rule_registry.py` | **NEW** - Rule registry implementation |
| `tools/cq/query/parser.py` | Handle `matches=` token |
| `tools/cq/query/executor.py` | Resolve `matches` references |
| `tools/cq/cli.py` | Add `rule-define`, `rule-list` commands |

#### Deprecations

| Item | Reason |
|------|--------|
| None | Additive enhancement |

#### Implementation Checklist

- [ ] Create `rule_registry.py` with `RuleRegistry` class
- [ ] Implement `RuleDefinition` dataclass
- [ ] Add `rule-define` CLI command
- [ ] Add `rule-list` CLI command
- [ ] Update parser to handle `matches=` token
- [ ] Implement rule resolution in executor
- [ ] Add tests for rule definition and resolution
- [ ] Add tests for pattern matching (`matches=security.*`)
- [ ] Document rule system in SKILL.md

---

## Part IV: Python `dis` Module Integration

### 4.1 Bytecode Instruction Queries

**`dis.Instruction` Fields Available:**

| Field | Type | Description |
|-------|------|-------------|
| `opcode` | int | Instruction opcode |
| `opname` | str | Opcode name |
| `baseopname` | str | Non-specialized opcode name |
| `arg` | int/None | Instruction argument |
| `argval` | Any | Resolved argument value |
| `offset` | int | Byte offset |
| `jump_target` | int/None | Target offset for jumps |
| `is_jump_target` | bool | Is this a jump target |
| `positions` | Positions | Source span (lineno, col) |
| `cache_info` | tuple/None | Inline cache data |

**Enhancement:**

```bash
# Find LOAD_GLOBAL usage (hidden dependencies)
/cq q "bytecode.opname=LOAD_GLOBAL"

# Find all call instructions
/cq q "bytecode.opname=~^CALL"

# Find jump targets for CFG
/cq q "bytecode.is_jump_target=true"

# Find specialized instructions (performance analysis)
/cq q "bytecode.opname!=bytecode.baseopname"
```

#### Key Architectural Elements

```python
# tools/cq/introspection/bytecode_index.py - NEW FILE

from __future__ import annotations

import dis
from dataclasses import dataclass, field
from pathlib import Path
from types import CodeType
from typing import Any, Iterator


@dataclass(frozen=True)
class InstructionFact:
    """Normalized instruction fact for indexing.

    Attributes
    ----------
    code_key
        Unique key for the containing code object
    offset
        Byte offset in code
    opname
        Instruction opcode name
    baseopname
        Non-specialized opcode name
    opcode
        Numeric opcode
    baseopcode
        Non-specialized numeric opcode
    arg
        Instruction argument (or None)
    argval
        Resolved argument value
    argrepr
        String representation of argument
    jump_target
        Target offset for jump instructions
    is_jump_target
        Whether this instruction is a jump target
    line_number
        Source line number (or None)
    positions
        Full source positions (lineno, end_lineno, col, end_col)
    stack_effect
        Net stack effect of this instruction
    """

    code_key: str
    offset: int
    opname: str
    baseopname: str
    opcode: int
    baseopcode: int
    arg: int | None
    argval: Any
    argrepr: str
    jump_target: int | None
    is_jump_target: bool
    line_number: int | None
    positions: tuple[int | None, int | None, int | None, int | None] | None
    stack_effect: int | None = None


def extract_instruction_facts(
    co: CodeType,
    code_key: str,
) -> Iterator[InstructionFact]:
    """Extract normalized instruction facts from code object.

    Parameters
    ----------
    co
        Code object to analyze.
    code_key
        Unique identifier for this code object.

    Yields
    ------
    InstructionFact
        Normalized facts for each instruction.
    """
    for instr in dis.get_instructions(co, adaptive=False):
        # Extract positions if available
        positions = None
        if instr.positions:
            positions = (
                instr.positions.lineno,
                instr.positions.end_lineno,
                instr.positions.col_offset,
                instr.positions.end_col_offset,
            )

        # Calculate stack effect
        stack_eff = None
        try:
            stack_eff = dis.stack_effect(instr.opcode, instr.arg)
        except ValueError:
            pass  # Some opcodes don't support stack_effect

        yield InstructionFact(
            code_key=code_key,
            offset=instr.offset,
            opname=instr.opname,
            baseopname=getattr(instr, "baseopname", instr.opname),
            opcode=instr.opcode,
            baseopcode=getattr(instr, "baseopcode", instr.opcode),
            arg=instr.arg,
            argval=instr.argval,
            argrepr=instr.argrepr,
            jump_target=getattr(instr, "jump_target", None),
            is_jump_target=getattr(instr, "is_jump_target", False),
            line_number=getattr(instr, "line_number", None),
            positions=positions,
            stack_effect=stack_eff,
        )


@dataclass
class BytecodeIndex:
    """Index of bytecode instructions for querying.

    Attributes
    ----------
    instructions
        All indexed instructions
    by_opname
        Index by opcode name
    by_code_key
        Index by code object key
    jump_targets
        Set of jump target offsets per code key
    """

    instructions: list[InstructionFact] = field(default_factory=list)
    by_opname: dict[str, list[InstructionFact]] = field(default_factory=dict)
    by_code_key: dict[str, list[InstructionFact]] = field(default_factory=dict)
    jump_targets: dict[str, set[int]] = field(default_factory=dict)

    def add(self, fact: InstructionFact) -> None:
        """Add instruction fact to index."""
        self.instructions.append(fact)

        # Index by opname
        if fact.opname not in self.by_opname:
            self.by_opname[fact.opname] = []
        self.by_opname[fact.opname].append(fact)

        # Index by code key
        if fact.code_key not in self.by_code_key:
            self.by_code_key[fact.code_key] = []
        self.by_code_key[fact.code_key].append(fact)

        # Track jump targets
        if fact.is_jump_target:
            if fact.code_key not in self.jump_targets:
                self.jump_targets[fact.code_key] = set()
            self.jump_targets[fact.code_key].add(fact.offset)

    def query(
        self,
        opname: str | None = None,
        opname_regex: str | None = None,
        is_jump_target: bool | None = None,
        stack_effect_min: int | None = None,
        stack_effect_max: int | None = None,
        code_key: str | None = None,
    ) -> list[InstructionFact]:
        """Query instructions by criteria.

        Parameters
        ----------
        opname
            Exact opcode name match
        opname_regex
            Regex pattern for opcode name
        is_jump_target
            Filter by jump target status
        stack_effect_min
            Minimum stack effect
        stack_effect_max
            Maximum stack effect
        code_key
            Limit to specific code object

        Returns
        -------
        list[InstructionFact]
            Matching instructions.
        """
        import re

        results = self.instructions

        if code_key:
            results = self.by_code_key.get(code_key, [])
        elif opname:
            results = self.by_opname.get(opname, [])

        if opname_regex:
            pattern = re.compile(opname_regex)
            results = [f for f in results if pattern.match(f.opname)]

        if is_jump_target is not None:
            results = [f for f in results if f.is_jump_target == is_jump_target]

        if stack_effect_min is not None:
            results = [f for f in results if f.stack_effect is not None and f.stack_effect >= stack_effect_min]

        if stack_effect_max is not None:
            results = [f for f in results if f.stack_effect is not None and f.stack_effect <= stack_effect_max]

        return results
```

```python
# tools/cq/query/ir.py - BytecodeFilter

@dataclass(frozen=True)
class BytecodeFilter:
    """Filter for bytecode instruction queries.

    Attributes
    ----------
    opname
        Exact opcode name (e.g., "LOAD_GLOBAL")
    opname_regex
        Regex pattern for opcode name (e.g., "^CALL")
    is_jump_target
        Filter by jump target status
    stack_effect_min
        Minimum stack effect
    stack_effect_max
        Maximum stack effect
    specialized_only
        Only specialized instructions (opname != baseopname)
    """

    opname: str | None = None
    opname_regex: str | None = None
    is_jump_target: bool | None = None
    stack_effect_min: int | None = None
    stack_effect_max: int | None = None
    specialized_only: bool = False
```

#### Target Files

| File | Changes |
|------|---------|
| `tools/cq/introspection/bytecode_index.py` | **NEW** - Bytecode indexing |
| `tools/cq/query/ir.py` | Add `BytecodeFilter` dataclass |
| `tools/cq/query/parser.py` | Parse `bytecode.*` tokens |
| `tools/cq/query/executor.py` | Execute bytecode queries |

#### Deprecations

| Item | Reason |
|------|--------|
| `tools/cq/macros/bytecode.py` (partial) | Move core extraction to `bytecode_index.py`, keep macro as facade |

#### Implementation Checklist

- [ ] Create `bytecode_index.py` with `InstructionFact`, `BytecodeIndex`
- [ ] Implement `extract_instruction_facts()`
- [ ] Add `BytecodeFilter` to ir.py
- [ ] Implement `bytecode.*` token parsing
- [ ] Integrate bytecode queries into executor
- [ ] Add tests for opname queries
- [ ] Add tests for stack effect queries
- [ ] Add tests for jump target queries
- [ ] Document bytecode query syntax in SKILL.md

---

### 4.2 Stack Effect Analysis

**`dis.stack_effect(opcode, oparg, jump=...)` Integration:**

```bash
# Find instructions that push multiple values
/cq q "bytecode.stack_effect>=2"

# Find net consumers (negative effect)
/cq q "bytecode.stack_effect<0"

# Find stack-neutral instructions
/cq q "bytecode.stack_effect=0"
```

*Implementation covered in 4.1*

---

### 4.3 Exception Table Queries

**`co_exceptiontable` Structure:**
- `start`: Try block start offset
- `end`: Try block end offset (exclusive)
- `target`: Handler offset
- `depth`: Stack depth at handler
- `lasti`: Whether to push last instruction

**Enhancement:**

```bash
# Find functions with exception handlers
/cq q "entity=function bytecode.exc_table exists"

# Find bare except patterns
/cq q "bytecode.exc_handler.depth=0"

# Visualize exception edges
/cq q "entity=function name=\$F fields=exc_cfg" --format mermaid
```

#### Key Architectural Elements

```python
# tools/cq/introspection/bytecode_index.py - Exception table parsing

from dataclasses import dataclass
from typing import Iterator


@dataclass(frozen=True)
class ExceptionEntry:
    """Parsed exception table entry.

    Attributes
    ----------
    start
        Try block start offset
    end
        Try block end offset (exclusive)
    target
        Handler offset
    depth
        Stack depth at handler entry
    lasti
        Whether to push last instruction
    """

    start: int
    end: int
    target: int
    depth: int
    lasti: bool


def _parse_varint(it: Iterator[int]) -> int:
    """Parse varint from exception table bytes."""
    b = next(it)
    val = b & 63
    while b & 64:
        val <<= 6
        b = next(it)
        val |= b & 63
    return val


def parse_exception_table(co: CodeType) -> list[ExceptionEntry]:
    """Parse exception table from code object.

    Parameters
    ----------
    co
        Code object with co_exceptiontable.

    Returns
    -------
    list[ExceptionEntry]
        Parsed exception entries.
    """
    it = iter(co.co_exceptiontable)
    entries: list[ExceptionEntry] = []

    try:
        while True:
            start = _parse_varint(it) * 2
            length = _parse_varint(it) * 2
            end = start + length
            target = _parse_varint(it) * 2
            dl = _parse_varint(it)
            depth = dl >> 1
            lasti = bool(dl & 1)
            entries.append(ExceptionEntry(start, end, target, depth, lasti))
    except StopIteration:
        pass

    return entries


@dataclass
class ExceptionTableIndex:
    """Index of exception table entries for querying."""

    entries_by_code: dict[str, list[ExceptionEntry]] = field(default_factory=dict)

    def add(self, code_key: str, entries: list[ExceptionEntry]) -> None:
        """Add exception entries for a code object."""
        self.entries_by_code[code_key] = entries

    def has_handlers(self, code_key: str) -> bool:
        """Check if code object has exception handlers."""
        return code_key in self.entries_by_code and len(self.entries_by_code[code_key]) > 0

    def query_by_depth(self, depth: int) -> list[tuple[str, ExceptionEntry]]:
        """Find exception entries by stack depth."""
        results = []
        for code_key, entries in self.entries_by_code.items():
            for entry in entries:
                if entry.depth == depth:
                    results.append((code_key, entry))
        return results
```

#### Target Files

| File | Changes |
|------|---------|
| `tools/cq/introspection/bytecode_index.py` | Add `ExceptionEntry`, `parse_exception_table()` |
| `tools/cq/query/ir.py` | Add exception table filter fields |
| `tools/cq/query/executor.py` | Handle exc_table queries |
| `tools/cq/core/renderers/mermaid.py` | Add exc_cfg format |

#### Deprecations

| Item | Reason |
|------|--------|
| None | Additive enhancement |

#### Implementation Checklist

- [ ] Add `ExceptionEntry` dataclass
- [ ] Implement `parse_exception_table()`
- [ ] Add `ExceptionTableIndex` class
- [ ] Add exc_table filter to query IR
- [ ] Implement exception table queries in executor
- [ ] Add exc_cfg mermaid rendering
- [ ] Add tests for exception table parsing
- [ ] Add tests for depth-based queries
- [ ] Document exception queries in SKILL.md

---

### 4.4 CFG Reconstruction Queries

**Basic Block Boundaries:**
- Offset 0
- Jump target offsets
- Instruction after terminators (RETURN_VALUE, RAISE_VARARGS)
- Exception table boundaries

```bash
# Get basic block count
/cq q "entity=function fields=basic_block_count"

# Visualize CFG
/cq q "entity=function name=process fields=cfg" --format mermaid-cfg

# Find functions with complex control flow
/cq q "entity=function cfg.block_count>=10"
```

#### Key Architectural Elements

```python
# tools/cq/introspection/cfg_builder.py - NEW FILE

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tools.cq.introspection.bytecode_index import InstructionFact, ExceptionEntry

# Terminator opcodes
TERMINATORS: set[str] = {
    "RETURN_VALUE",
    "RETURN_CONST",
    "RAISE_VARARGS",
    "RERAISE",
}

# Unconditional jump opcodes
UNCONDITIONAL_JUMPS: set[str] = {
    "JUMP_FORWARD",
    "JUMP_BACKWARD",
    "JUMP",
}


@dataclass(frozen=True)
class BasicBlock:
    """Basic block in control flow graph.

    Attributes
    ----------
    id
        Block identifier
    start_offset
        Starting instruction offset
    end_offset
        Ending instruction offset (inclusive)
    instructions
        Instructions in this block
    """

    id: int
    start_offset: int
    end_offset: int
    instructions: tuple["InstructionFact", ...]


@dataclass(frozen=True)
class CFGEdge:
    """Edge in control flow graph.

    Attributes
    ----------
    source
        Source block ID
    target
        Target block ID
    kind
        Edge type: "fallthrough", "jump", "exception"
    """

    source: int
    target: int
    kind: str


@dataclass
class CFG:
    """Control flow graph for a function.

    Attributes
    ----------
    code_key
        Code object identifier
    blocks
        Basic blocks
    edges
        Control flow edges
    entry_block
        Entry block ID
    """

    code_key: str
    blocks: dict[int, BasicBlock] = field(default_factory=dict)
    edges: list[CFGEdge] = field(default_factory=list)
    entry_block: int = 0

    @property
    def block_count(self) -> int:
        """Number of basic blocks."""
        return len(self.blocks)

    @property
    def edge_count(self) -> int:
        """Number of edges."""
        return len(self.edges)

    def to_mermaid(self) -> str:
        """Convert to Mermaid flowchart."""
        lines = ["flowchart TD"]

        for block_id, block in self.blocks.items():
            # Create node label
            first_instr = block.instructions[0] if block.instructions else None
            label = f"B{block_id}"
            if first_instr and first_instr.line_number:
                label += f" (L{first_instr.line_number})"
            lines.append(f"    B{block_id}[{label}]")

        for edge in self.edges:
            style = ""
            if edge.kind == "exception":
                style = " -.-> "
            elif edge.kind == "jump":
                style = " --> "
            else:
                style = " --> "
            lines.append(f"    B{edge.source}{style}B{edge.target}")

        return "\n".join(lines)


def build_cfg(
    instructions: list["InstructionFact"],
    exception_entries: list["ExceptionEntry"] | None = None,
    code_key: str = "",
) -> CFG:
    """Build CFG from instructions.

    Parameters
    ----------
    instructions
        Bytecode instructions for the function.
    exception_entries
        Exception table entries.
    code_key
        Code object identifier.

    Returns
    -------
    CFG
        Control flow graph.
    """
    if not instructions:
        return CFG(code_key=code_key)

    # Compute block boundaries
    boundaries: set[int] = {0}  # Entry point

    # Add jump targets
    for instr in instructions:
        if instr.jump_target is not None:
            boundaries.add(instr.jump_target)
        if instr.opname in TERMINATORS or instr.opname in UNCONDITIONAL_JUMPS:
            # Instruction after terminator starts new block
            next_idx = instructions.index(instr) + 1
            if next_idx < len(instructions):
                boundaries.add(instructions[next_idx].offset)

    # Add exception boundaries
    if exception_entries:
        for entry in exception_entries:
            boundaries.add(entry.start)
            boundaries.add(entry.end)
            boundaries.add(entry.target)

    # Create blocks
    sorted_boundaries = sorted(boundaries)
    offset_to_block: dict[int, int] = {}
    blocks: dict[int, BasicBlock] = {}

    for i, start in enumerate(sorted_boundaries):
        end = sorted_boundaries[i + 1] if i + 1 < len(sorted_boundaries) else float("inf")
        block_instrs = tuple(
            instr for instr in instructions
            if start <= instr.offset < end
        )
        if block_instrs:
            block = BasicBlock(
                id=i,
                start_offset=start,
                end_offset=block_instrs[-1].offset,
                instructions=block_instrs,
            )
            blocks[i] = block
            for instr in block_instrs:
                offset_to_block[instr.offset] = i

    # Create edges
    edges: list[CFGEdge] = []

    for block_id, block in blocks.items():
        if not block.instructions:
            continue

        last_instr = block.instructions[-1]

        # Jump edge
        if last_instr.jump_target is not None:
            target_block = offset_to_block.get(last_instr.jump_target)
            if target_block is not None:
                edges.append(CFGEdge(block_id, target_block, "jump"))

        # Fallthrough edge (if not terminator or unconditional jump)
        if last_instr.opname not in TERMINATORS and last_instr.opname not in UNCONDITIONAL_JUMPS:
            # Find next block
            next_offset = last_instr.offset + 2  # Approximate
            for next_instr in instructions:
                if next_instr.offset > last_instr.offset:
                    next_block = offset_to_block.get(next_instr.offset)
                    if next_block is not None and next_block != block_id:
                        edges.append(CFGEdge(block_id, next_block, "fallthrough"))
                    break

    # Exception edges
    if exception_entries:
        for entry in exception_entries:
            handler_block = offset_to_block.get(entry.target)
            if handler_block is None:
                continue

            for block_id, block in blocks.items():
                # Check if any instruction in block is within try range
                for instr in block.instructions:
                    if entry.start <= instr.offset < entry.end:
                        edges.append(CFGEdge(block_id, handler_block, "exception"))
                        break

    return CFG(
        code_key=code_key,
        blocks=blocks,
        edges=edges,
        entry_block=0,
    )
```

#### Target Files

| File | Changes |
|------|---------|
| `tools/cq/introspection/cfg_builder.py` | **NEW** - CFG construction |
| `tools/cq/query/ir.py` | Add CFG-related field types |
| `tools/cq/query/executor.py` | Build CFG on demand |
| `tools/cq/core/renderers/mermaid.py` | Add mermaid-cfg format |

#### Deprecations

| Item | Reason |
|------|--------|
| None | Additive enhancement |

#### Implementation Checklist

- [ ] Create `cfg_builder.py` with `BasicBlock`, `CFGEdge`, `CFG`
- [ ] Implement `build_cfg()` function
- [ ] Add `to_mermaid()` method to CFG
- [ ] Add cfg fields to query IR
- [ ] Integrate CFG building into executor
- [ ] Add mermaid-cfg renderer
- [ ] Add tests for CFG construction
- [ ] Add tests for edge types (fallthrough, jump, exception)
- [ ] Document CFG queries in SKILL.md

---

### 4.5 DFG Stack Model Queries

```bash
# Find def-use relationships
/cq q "entity=function fields=def_use_chains"

# Find variables with multiple definitions
/cq q "entity=variable def_count>=2"

# Track value flow through stack
/cq q "entity=function name=transform fields=stack_trace"
```

#### Key Architectural Elements

```python
# tools/cq/introspection/dfg_builder.py - NEW FILE

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tools.cq.introspection.bytecode_index import InstructionFact


@dataclass(frozen=True)
class StackValue:
    """Value on the stack.

    Attributes
    ----------
    vid
        Value identifier
    produced_by
        Instruction offset that produced this value
    """

    vid: int
    produced_by: int


@dataclass(frozen=True)
class DefUseEdge:
    """Definition-use relationship.

    Attributes
    ----------
    kind
        Edge type: DEF_LOCAL, USE_LOCAL, USE_STACK, etc.
    symbol
        Variable name (for local/global) or None (for stack)
    def_offset
        Instruction offset of definition
    use_offset
        Instruction offset of use
    """

    kind: str
    symbol: str | None
    def_offset: int
    use_offset: int


@dataclass
class DFG:
    """Data flow graph for a function."""

    code_key: str
    edges: list[DefUseEdge] = field(default_factory=list)
    definitions: dict[str, list[int]] = field(default_factory=dict)  # symbol -> def offsets

    def add_def(self, symbol: str, offset: int) -> None:
        """Record a definition."""
        if symbol not in self.definitions:
            self.definitions[symbol] = []
        self.definitions[symbol].append(offset)

    def multi_def_symbols(self) -> list[str]:
        """Find symbols with multiple definitions."""
        return [sym for sym, defs in self.definitions.items() if len(defs) > 1]


# DFG analysis opcodes
LOAD_OPS: dict[str, str] = {
    "LOAD_FAST": "local",
    "LOAD_GLOBAL": "global",
    "LOAD_NAME": "name",
    "LOAD_DEREF": "free",
}

STORE_OPS: dict[str, str] = {
    "STORE_FAST": "local",
    "STORE_GLOBAL": "global",
    "STORE_NAME": "name",
    "STORE_DEREF": "free",
}


def build_dfg(
    instructions: list["InstructionFact"],
    code_key: str = "",
) -> DFG:
    """Build minimal DFG from instructions.

    Parameters
    ----------
    instructions
        Bytecode instructions.
    code_key
        Code object identifier.

    Returns
    -------
    DFG
        Data flow graph.
    """
    dfg = DFG(code_key=code_key)

    # Track last definition of each symbol
    last_def: dict[str, int] = {}

    for instr in instructions:
        base_op = instr.baseopname

        if base_op in STORE_OPS:
            symbol = str(instr.argval)
            dfg.add_def(symbol, instr.offset)
            last_def[symbol] = instr.offset

        elif base_op in LOAD_OPS:
            symbol = str(instr.argval)
            def_offset = last_def.get(symbol, -1)  # -1 = external/unknown
            dfg.edges.append(DefUseEdge(
                kind=f"USE_{LOAD_OPS[base_op].upper()}",
                symbol=symbol,
                def_offset=def_offset,
                use_offset=instr.offset,
            ))

    return dfg
```

#### Target Files

| File | Changes |
|------|---------|
| `tools/cq/introspection/dfg_builder.py` | **NEW** - DFG construction |
| `tools/cq/query/ir.py` | Add DFG-related field types |
| `tools/cq/query/executor.py` | Build DFG on demand |

#### Deprecations

| Item | Reason |
|------|--------|
| None | Additive enhancement |

#### Implementation Checklist

- [ ] Create `dfg_builder.py` with `StackValue`, `DefUseEdge`, `DFG`
- [ ] Implement `build_dfg()` function
- [ ] Add multi_def_symbols() for multi-def queries
- [ ] Add DFG fields to query IR
- [ ] Integrate DFG building into executor
- [ ] Add tests for def-use edge extraction
- [ ] Add tests for multi-def detection
- [ ] Document DFG queries in SKILL.md

---

## Part V: Python `symtable` Integration

### 5.1 Scope Type Queries

**`SymbolTableType` Enum (3.13):**

| Type | Description |
|------|-------------|
| `MODULE` | Module-level scope |
| `FUNCTION` | Function scope |
| `CLASS` | Class body scope |
| `ANNOTATION` | Future annotation scope |
| `TYPE_ALIAS` | `type` statement scope |
| `TYPE_PARAMETERS` | Generic parameter scope |
| `TYPE_VARIABLE` | TypeVar bounds scope (3.13+) |

**Enhancement:**

```bash
# Find all function scopes
/cq q "scope.type=FUNCTION"

# Find type parameter scopes (generics)
/cq q "scope.type=TYPE_PARAMETERS"

# Find type alias definitions
/cq q "scope.type=TYPE_ALIAS"

# Find nested scopes
/cq q "scope.is_nested=true"

# Find optimized scopes (fast locals)
/cq q "scope.is_optimized=true"
```

#### Key Architectural Elements

```python
# tools/cq/introspection/symtable_extract.py - Enhanced

from __future__ import annotations

import symtable
from dataclasses import dataclass, field
from enum import Enum
from typing import Iterator


class ScopeType(Enum):
    """Types of Python scopes (matches 3.13 SymbolTableType)."""

    MODULE = "module"
    FUNCTION = "function"
    CLASS = "class"
    ANNOTATION = "annotation"
    TYPE_ALIAS = "type_alias"
    TYPE_PARAMETERS = "type_parameters"
    TYPE_VARIABLE = "type_variable"

    @classmethod
    def from_symtable(cls, st: symtable.SymbolTable) -> "ScopeType":
        """Convert symtable type to ScopeType."""
        # 3.13+ returns SymbolTableType enum
        st_type = st.get_type()
        if hasattr(st_type, "name"):
            # Enum member
            return cls[st_type.name]
        # Fallback for older Python
        type_map = {
            "module": cls.MODULE,
            "function": cls.FUNCTION,
            "class": cls.CLASS,
        }
        return type_map.get(st_type, cls.MODULE)


@dataclass(frozen=True)
class ScopeFact:
    """Enhanced scope information.

    Attributes
    ----------
    name
        Scope name
    scope_type
        Type of scope
    lineno
        Line number where scope starts
    is_nested
        Whether this is a nested scope
    is_optimized
        Whether this scope uses fast locals
    has_free_vars
        Whether this scope captures variables
    has_cell_vars
        Whether this scope has captured variables
    free_vars
        Names of captured variables
    cell_vars
        Names of variables captured by children
    parameters
        Function parameters (if function scope)
    locals
        Local variable names
    globals
        Global variable names
    nonlocals
        Nonlocal declarations
    children
        Child scope names
    """

    name: str
    scope_type: ScopeType
    lineno: int = 0
    is_nested: bool = False
    is_optimized: bool = False
    has_free_vars: bool = False
    has_cell_vars: bool = False
    free_vars: tuple[str, ...] = ()
    cell_vars: tuple[str, ...] = ()
    parameters: tuple[str, ...] = ()
    locals: tuple[str, ...] = ()
    globals: tuple[str, ...] = ()
    nonlocals: tuple[str, ...] = ()
    children: tuple[str, ...] = ()


def extract_scope_facts(source: str, filename: str) -> Iterator[ScopeFact]:
    """Extract scope facts from source code.

    Parameters
    ----------
    source
        Python source code.
    filename
        Source filename.

    Yields
    ------
    ScopeFact
        Scope information for each scope in the code.
    """
    st = symtable.symtable(source, filename, "exec")
    yield from _walk_symtable(st)


def _walk_symtable(st: symtable.SymbolTable) -> Iterator[ScopeFact]:
    """Walk symbol table recursively."""
    scope_type = ScopeType.from_symtable(st)

    # Extract function-specific info
    parameters = ()
    locals_ = ()
    globals_ = ()
    nonlocals = ()
    free_vars = ()

    if scope_type == ScopeType.FUNCTION:
        # Cast to Function table for additional methods
        func_table = st  # type: ignore
        if hasattr(func_table, "get_parameters"):
            parameters = tuple(func_table.get_parameters())
        if hasattr(func_table, "get_locals"):
            locals_ = tuple(func_table.get_locals())
        if hasattr(func_table, "get_globals"):
            globals_ = tuple(func_table.get_globals())
        if hasattr(func_table, "get_nonlocals"):
            nonlocals = tuple(func_table.get_nonlocals())
        if hasattr(func_table, "get_frees"):
            free_vars = tuple(func_table.get_frees())

    # Detect cell vars from symbols
    cell_vars = tuple(
        sym.get_name()
        for sym in st.get_symbols()
        if _is_cell_var(sym)
    )

    yield ScopeFact(
        name=st.get_name(),
        scope_type=scope_type,
        lineno=st.get_lineno(),
        is_nested=st.is_nested(),
        is_optimized=st.is_optimized() if hasattr(st, "is_optimized") else False,
        has_free_vars=len(free_vars) > 0,
        has_cell_vars=len(cell_vars) > 0,
        free_vars=free_vars,
        cell_vars=cell_vars,
        parameters=parameters,
        locals=locals_,
        globals=globals_,
        nonlocals=nonlocals,
        children=tuple(c.get_name() for c in st.get_children()),
    )

    # Recurse into children
    for child in st.get_children():
        yield from _walk_symtable(child)


def _is_cell_var(sym: symtable.Symbol) -> bool:
    """Check if symbol is a cell variable (captured by nested scope)."""
    # Cell vars are local but also free in some nested scope
    # This is a heuristic - symtable doesn't expose this directly
    return sym.is_local() and not sym.is_parameter()
```

#### Target Files

| File | Changes |
|------|---------|
| `tools/cq/introspection/symtable_extract.py` | Enhance `ScopeFact`, add 3.13 scope types |
| `tools/cq/query/ir.py` | Add `ScopeFilter` enhancements |
| `tools/cq/query/parser.py` | Parse `scope.*` tokens |
| `tools/cq/query/executor.py` | Execute scope queries |

#### Deprecations

| Item | Reason |
|------|--------|
| None | Additive enhancement |

#### Implementation Checklist

- [ ] Add 3.13 scope types to `ScopeType` enum
- [ ] Implement `ScopeType.from_symtable()`
- [ ] Enhance `ScopeFact` with all partition fields
- [ ] Add cell_var detection
- [ ] Update parser for `scope.type=`, `scope.is_nested=`, etc.
- [ ] Implement scope queries in executor
- [ ] Add tests for each scope type
- [ ] Add tests for nested/optimized detection
- [ ] Document scope queries in SKILL.md

---

### 5.2-5.4 Symbol Classification, Function Scope Partitions, Cross-Validation

*These sections follow similar patterns to 5.1 - detailed implementation in `symtable_extract.py` with corresponding query IR and executor support.*

#### Implementation Checklist (Combined)

- [ ] Add `SymbolFact` enhancements for all flags
- [ ] Implement symbol classification queries
- [ ] Add scope partition queries (parameters, locals, globals, nonlocals, frees)
- [ ] Implement cross-validation with code objects
- [ ] Add mismatch detection queries
- [ ] Add comprehensive tests
- [ ] Document all symbol/scope queries in SKILL.md

---

## Part VI-X: Remaining Sections

*Parts VI (inspect integration), VII (cross-layer correlation), VIII (visualization), IX (security), and X (grammar) follow similar detailed patterns. For brevity, key implementation notes:*

### Part VI: inspect Integration

#### Target Files
- `tools/cq/introspection/signature_extract.py` - **NEW** - Signature analysis
- `tools/cq/introspection/wrapper_chain.py` - **NEW** - Wrapper chain analysis
- `tools/cq/introspection/descriptor_analysis.py` - **NEW** - Descriptor detection

### Part VII: Cross-Layer Correlation

#### Target Files
- `tools/cq/correlation/span_matcher.py` - **NEW** - Span correlation
- `tools/cq/correlation/evidence_merger.py` - **NEW** - Multi-layer evidence

### Part VIII: Visualization

#### Target Files
- `tools/cq/core/renderers/mermaid.py` - Extend with cfg, dfg, scope formats
- `tools/cq/core/renderers/dot.py` - Extend with new graph types

### Part IX: Security

#### Target Files
- `tools/cq/query/hazards.py` - Expand builtin hazard patterns
- `tools/cq/query/owasp.py` - **NEW** - OWASP query packs

---

## Implementation Priority Matrix

| Phase | Enhancement | Effort | Value | Priority |
|-------|-------------|--------|-------|----------|
| 1.1 | Pattern objects (context/selector) | M | H | P0 |
| 1.2 | Strictness modes | L | M | P0 |
| 1.3-1.5 | Meta-variable enhancements | M | H | P0 |
| 2.1-2.4 | Full relational operators | M | H | P0 |
| 3.1-3.4 | Composite logic | L | H | P1 |
| 4.1-4.5 | dis integration | H | M | P1 |
| 5.1-5.4 | symtable integration | M | H | P1 |
| 6.1-6.5 | inspect integration | M | M | P2 |
| 7.1-7.3 | Cross-layer correlation | H | H | P2 |
| 8.1-8.3 | Visualization | M | M | P2 |
| 9.1-9.3 | Security hazards | M | H | P1 |

**Legend:** L=Low, M=Medium, H=High effort/value

---

## Files Summary

### New Files to Create

| File | Purpose |
|------|---------|
| `tools/cq/query/rule_registry.py` | Reusable rule storage |
| `tools/cq/introspection/bytecode_index.py` | Bytecode instruction indexing |
| `tools/cq/introspection/cfg_builder.py` | CFG construction |
| `tools/cq/introspection/dfg_builder.py` | DFG construction |
| `tools/cq/introspection/signature_extract.py` | Signature analysis |
| `tools/cq/introspection/wrapper_chain.py` | Wrapper chain analysis |
| `tools/cq/introspection/descriptor_analysis.py` | Descriptor detection |
| `tools/cq/correlation/span_matcher.py` | Span correlation |
| `tools/cq/correlation/evidence_merger.py` | Multi-layer evidence |
| `tools/cq/query/owasp.py` | OWASP query packs |

### Existing Files to Modify

| File | Changes |
|------|---------|
| `tools/cq/query/ir.py` | Add new dataclasses, filters, composite types |
| `tools/cq/query/parser.py` | Parse all new token types |
| `tools/cq/query/planner.py` | Generate YAML rules, handle composites |
| `tools/cq/query/executor.py` | Execute all new query types |
| `tools/cq/query/sg_parser.py` | Handle metavar captures |
| `tools/cq/query/hazards.py` | Expand hazard patterns |
| `tools/cq/introspection/symtable_extract.py` | Add 3.13 types, enhance facts |
| `tools/cq/core/renderers/mermaid.py` | Add cfg, dfg, scope formats |
| `tools/cq/cli.py` | Add rule-define, rule-list commands |
| `.claude/skills/cq/SKILL.md` | Document all new features |

### Files to Deprecate

| File | Action | Reason |
|------|--------|--------|
| None | - | All changes are additive |

---

## Success Metrics

1. **Query Coverage**: Express 95% of manual code review questions
2. **Performance**: <5s on 100k LOC repos
3. **Accuracy**: <1% false positive rate
4. **Layer Correlation**: 3+ evidence sources per finding
5. **Adoption**: 80% refactoring tasks use cq

---

## References

- ast-grep docs: `docs/python_library_reference/ast-grep.md`
- Python introspection: `docs/python_library_reference/python_ast_libraries_and_cpg_construction.md`
- Current cq: `tools/cq/`
- Existing plan: `docs/plans/cq_query_enhancement_plan.md`
