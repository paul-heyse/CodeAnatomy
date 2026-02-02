"""Query planner for cq queries.

Compiles Query IR into a ToolPlan that specifies which tools to run.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

from tools.cq.query.ir import (
    STRICTNESS_DESCRIPTIONS,
    CompositeRule,
    Expander,
    NthChildSpec,
    Query,
    RelationalConstraint,
    Scope,
)

if TYPE_CHECKING:
    from tools.cq.query.ir import StrictnessMode


@dataclass(frozen=True)
class AstGrepRule:
    """ast-grep rule specification.

    Supports full ast-grep rule features including pattern objects and
    relational constraints.

    Attributes
    ----------
    pattern
        Pattern to match (for simple patterns)
    kind
        Node kind selector
    context
        Surrounding code for disambiguation (creates pattern object)
    selector
        CSS-like selector for extracting specific node from context
    strictness
        Matching strictness mode (cst, smart, ast, relaxed, signature)
    inside
        Inside relational constraint pattern
    has
        Has relational constraint pattern
    precedes
        Precedes relational constraint pattern
    follows
        Follows relational constraint pattern
    inside_stop_by
        Stop-by mode for inside constraint
    has_stop_by
        Stop-by mode for has constraint
    inside_field
        Field constraint for inside operator
    has_field
        Field constraint for has operator
    composite
        Composite rule (all/any/not) combining multiple patterns
    nth_child
        Positional matching specification
    """

    pattern: str
    kind: str | None = None
    context: str | None = None
    selector: str | None = None
    strictness: StrictnessMode = "smart"
    inside: str | None = None
    has: str | None = None
    precedes: str | None = None
    follows: str | None = None
    inside_stop_by: str | None = None
    has_stop_by: str | None = None
    inside_field: str | None = None
    has_field: str | None = None
    composite: CompositeRule | None = None
    nth_child: NthChildSpec | None = None

    def requires_inline_rule(self) -> bool:
        """Check if this rule requires inline YAML rule execution.

        Returns
        -------
        bool
            True if rule has features requiring inline rule format.
        """
        return (
            self.context is not None
            or self.selector is not None
            or self.strictness != "smart"
            or self.inside is not None
            or self.has is not None
            or self.precedes is not None
            or self.follows is not None
            or self.composite is not None
            or self.nth_child is not None
        )

    def to_yaml_dict(self) -> dict:
        """Convert to ast-grep YAML rule format.

        Returns
        -------
        dict
            Dictionary suitable for YAML serialization.

        Notes
        -----
        Handles both simple patterns and pattern objects with context.
        When context is provided, generates pattern object format:
        `{"pattern": {"context": "...", "selector": "..."}}`
        """
        # Build pattern section - handle context for disambiguation
        if self.context:
            pattern_obj: dict = {"context": self.context}
            if self.selector:
                pattern_obj["selector"] = self.selector
            if self.strictness != "smart":
                pattern_obj["strictness"] = self.strictness
            rule: dict = {"pattern": pattern_obj}
        else:
            rule = {"pattern": self.pattern}
            if self.strictness != "smart":
                rule["strictness"] = self.strictness

        if self.kind:
            rule["kind"] = self.kind
        if self.selector and not self.context:
            # Selector without context is node kind filter
            rule["selector"] = self.selector

        # Add relational constraints
        if self.inside:
            inside_rule: dict = {"pattern": self.inside}
            if self.inside_stop_by:
                inside_rule["stopBy"] = self.inside_stop_by
            if self.inside_field:
                inside_rule["field"] = self.inside_field
            rule["inside"] = inside_rule

        if self.has:
            has_rule: dict = {"pattern": self.has}
            if self.has_stop_by:
                has_rule["stopBy"] = self.has_stop_by
            if self.has_field:
                has_rule["field"] = self.has_field
            rule["has"] = has_rule

        if self.precedes:
            rule["precedes"] = {"pattern": self.precedes}

        if self.follows:
            rule["follows"] = {"pattern": self.follows}

        # Add composite rule if present
        if self.composite:
            rule.update(self.composite.to_ast_grep_dict())

        # Add nthChild if present
        if self.nth_child:
            nth = {"position": self.nth_child.position}
            if self.nth_child.reverse:
                nth["reverse"] = True
            if self.nth_child.of_rule:
                nth["ofRule"] = {"kind": self.nth_child.of_rule}
            rule["nthChild"] = nth

        return rule


@dataclass(frozen=True)
class ToolPlan:
    """Execution plan for a query.

    Attributes
    ----------
    rg_pattern
        Ripgrep pattern for file narrowing (optional)
    scope
        File scope constraints
    sg_record_types
        Set of ast-grep record types to collect
    need_symtable
        Whether to collect symtable information
    need_bytecode
        Whether to analyze bytecode
    expand_ops
        Graph expansion operations to perform
    explain
        Whether to include plan explanation in output
    sg_rules
        Custom ast-grep rules for pattern queries
    is_pattern_query
        Whether this is a pattern-based query
    """

    rg_pattern: str | None = None
    scope: Scope = field(default_factory=Scope)
    sg_record_types: frozenset[str] = frozenset({"def", "call", "import"})
    need_symtable: bool = False
    need_bytecode: bool = False
    expand_ops: tuple[Expander, ...] = ()
    explain: bool = False
    sg_rules: tuple[AstGrepRule, ...] = ()
    is_pattern_query: bool = False


def compile_query(query: Query) -> ToolPlan:
    """Compile a Query into a ToolPlan.

    Parameters
    ----------
    query
        Parsed query to compile.

    Returns
    -------
    ToolPlan
        Execution plan for the query.
    """
    # Handle pattern queries differently
    if query.is_pattern_query:
        return _compile_pattern_query(query)

    return _compile_entity_query(query)


def _compile_entity_query(query: Query) -> ToolPlan:
    """Compile an entity-based query into a ToolPlan."""
    # Determine required ast-grep record types based on query
    sg_record_types = _determine_record_types(query)

    # Determine ripgrep pattern for file narrowing
    rg_pattern = _determine_rg_pattern(query)

    # Check if we need symtable or bytecode
    need_symtable = _needs_symtable(query)
    need_bytecode = _needs_bytecode(query)

    # Generate ast-grep rules if entity has relational constraints
    sg_rules = _entity_to_ast_grep_rules(query) if query.relational else ()

    return ToolPlan(
        rg_pattern=rg_pattern,
        scope=query.scope,
        sg_record_types=frozenset(sg_record_types),
        need_symtable=need_symtable,
        need_bytecode=need_bytecode,
        expand_ops=query.expand,
        explain=query.explain,
        sg_rules=sg_rules,
        is_pattern_query=False,
    )


def _compile_pattern_query(query: Query) -> ToolPlan:
    """Compile a pattern-based query into a ToolPlan."""
    assert query.pattern_spec is not None

    # Build ast-grep rule from pattern spec
    rule = AstGrepRule(
        pattern=query.pattern_spec.pattern,
        context=query.pattern_spec.context,
        selector=query.pattern_spec.selector,
        strictness=query.pattern_spec.strictness,
    )

    # Apply relational constraints
    rule = _apply_relational_constraints(rule, query.relational)

    # Extract ripgrep pattern for file prefiltering
    rg_pattern = _extract_rg_pattern_from_ast_pattern(query.pattern_spec.pattern)

    return ToolPlan(
        rg_pattern=rg_pattern,
        scope=query.scope,
        sg_record_types=frozenset(),  # Not using standard record types
        need_symtable=_needs_symtable(query),
        need_bytecode=_needs_bytecode(query),
        expand_ops=query.expand,
        explain=query.explain,
        sg_rules=(rule,),
        is_pattern_query=True,
    )


def _entity_to_ast_grep_rules(query: Query) -> tuple[AstGrepRule, ...]:
    """Convert entity query with relational constraints to ast-grep rules."""
    assert query.entity is not None

    # Map entity types to ast-grep patterns
    entity_patterns = {
        "function": "def $FUNC($$$): $$$",
        "class": "class $CLASS($$$): $$$",
        "method": "def $METHOD(self, $$$): $$$",
        "decorator": "@$DECORATOR($$$)",
        "import": "import $MODULE",
        "callsite": "$FUNC($$$)",
    }

    base_pattern = entity_patterns.get(query.entity)
    if not base_pattern:
        return ()

    # Apply name filter if present
    if query.name:
        if query.name.startswith("~"):
            # Regex pattern - metavars need to stay for post-filtering
            # The regex will be applied during metavar filtering phase
            _ = query.name[1:]  # regex for later metavar filtering
            pass  # Keep metavars as-is for pattern matching
        else:
            # Exact name - substitute directly
            base_pattern = base_pattern.replace("$FUNC", query.name)
            base_pattern = base_pattern.replace("$CLASS", query.name)
            base_pattern = base_pattern.replace("$METHOD", query.name)
            base_pattern = base_pattern.replace("$DECORATOR", query.name)
            base_pattern = base_pattern.replace("$MODULE", query.name)

    rule = AstGrepRule(pattern=base_pattern)

    # Apply relational constraints
    rule = _apply_relational_constraints(rule, query.relational)

    return (rule,)


def _apply_relational_constraints(
    rule: AstGrepRule,
    constraints: tuple[RelationalConstraint, ...],
) -> AstGrepRule:
    """Apply relational constraints to an ast-grep rule.

    Parameters
    ----------
    rule
        Base ast-grep rule to extend.
    constraints
        Relational constraints from the query.

    Returns
    -------
    AstGrepRule
        New rule with relational constraints applied.
    """
    inside: str | None = rule.inside
    has: str | None = rule.has
    precedes: str | None = rule.precedes
    follows: str | None = rule.follows
    inside_stop_by: str | None = rule.inside_stop_by
    has_stop_by: str | None = rule.has_stop_by
    inside_field: str | None = rule.inside_field
    has_field: str | None = rule.has_field

    for constraint in constraints:
        if constraint.operator == "inside":
            inside = constraint.pattern
            if constraint.stop_by != "neighbor":
                inside_stop_by = str(constraint.stop_by)
            if constraint.field_name:
                inside_field = constraint.field_name
        elif constraint.operator == "has":
            has = constraint.pattern
            if constraint.stop_by != "neighbor":
                has_stop_by = str(constraint.stop_by)
            if constraint.field_name:
                has_field = constraint.field_name
        elif constraint.operator == "precedes":
            precedes = constraint.pattern
        elif constraint.operator == "follows":
            follows = constraint.pattern

    return AstGrepRule(
        pattern=rule.pattern,
        kind=rule.kind,
        context=rule.context,
        selector=rule.selector,
        strictness=rule.strictness,
        inside=inside,
        has=has,
        precedes=precedes,
        follows=follows,
        inside_stop_by=inside_stop_by,
        has_stop_by=has_stop_by,
        inside_field=inside_field,
        has_field=has_field,
        composite=rule.composite,
        nth_child=rule.nth_child,
    )


def _extract_rg_pattern_from_ast_pattern(ast_pattern: str) -> str | None:
    """Extract a ripgrep pattern from an ast-grep pattern for prefiltering.

    Finds literal strings in the pattern that can be used for fast file filtering.
    """
    # Remove metavariables and extract literal parts
    # Metavariables: $NAME, $$$, $$NAME, $_ etc.
    cleaned = re.sub(r"\$+\w*", "", ast_pattern)
    cleaned = re.sub(r"\$_", "", cleaned)

    # Find longest literal word sequence
    words = re.findall(r"\b[a-zA-Z_][a-zA-Z_0-9]*\b", cleaned)
    if not words:
        return None

    # Return the first keyword (likely def, class, import, etc.)
    keywords = {"def", "class", "import", "from", "async", "return", "raise", "yield"}
    for word in words:
        if word in keywords:
            return word

    # Fall back to longest word
    return max(words, key=len) if words else None


def _determine_record_types(query: Query) -> set[str]:
    """Determine which ast-grep record types are needed."""
    record_types: set[str] = set()

    # Base record types based on entity
    entity_records: dict[str, set[str]] = {
        "function": {"def"},
        "class": {"def"},
        "method": {"def"},
        "module": {"def"},
        "callsite": {"call"},
        "import": {"import"},
        "decorator": {"def"},  # Decorators are on function/class defs
    }
    if query.entity is not None:
        record_types.update(entity_records.get(query.entity, set()))

    # Add record types based on expanders
    for expander in query.expand:
        if expander.kind in ("callers", "callees"):
            record_types.add("def")
            record_types.add("call")
        elif expander.kind == "imports":
            record_types.add("import")
        elif expander.kind == "raises":
            record_types.add("raise")
            record_types.add("except")
        elif expander.kind == "scope":
            record_types.add("def")

    # Add record types based on output fields
    if "hazards" in query.fields:
        record_types.add("call")
        record_types.add("assign_ctor")

    return record_types


def _determine_rg_pattern(query: Query) -> str | None:
    """Determine ripgrep pattern for initial file filtering.

    Returns None if no pattern-based filtering is needed.
    """
    if not query.name:
        return None

    # If name starts with ~, it's a regex pattern
    if query.name.startswith("~"):
        return query.name[1:]

    # For exact names, create a pattern that matches common Python patterns
    name = query.name

    if query.entity == "function":
        # Match function definitions
        return rf"def\s+{name}\s*\("
    if query.entity == "class":
        # Match class definitions
        return rf"class\s+{name}\s*[:\[(]"
    if query.entity == "method":
        # Match method definitions (same as function, context determines method)
        return rf"def\s+{name}\s*\("
    if query.entity == "import":
        # Match import statements
        return rf"(import\s+{name}|from\s+\S+\s+import\s+.*{name})"
    if query.entity == "callsite":
        # Match function calls
        return rf"{name}\s*\("

    return None


def _needs_symtable(query: Query) -> bool:
    """Check if query requires symtable information."""
    # Symtable needed for scope analysis
    if any(e.kind == "scope" for e in query.expand):
        return True

    # Symtable useful for accurate caller/callee resolution
    if any(e.kind in ("callers", "callees") for e in query.expand):
        return True

    # Symtable needed for scope filtering (closure detection, etc.)
    if query.scope_filter is not None:
        return True

    return False


def _needs_bytecode(query: Query) -> bool:
    """Check if query requires bytecode analysis."""
    # Bytecode needed for bytecode_surface expander
    if any(e.kind == "bytecode_surface" for e in query.expand):
        return True

    # Bytecode useful for more accurate call resolution
    if "evidence" in query.fields:
        return True

    return False


def scope_to_paths(scope: Scope, root: Path) -> list[Path]:
    """Convert scope constraints to list of paths to scan.

    Parameters
    ----------
    scope
        Scope constraints
    root
        Repository root

    Returns
    -------
    list[Path]
        Paths to scan
    """
    if scope.in_dir:
        base = root / scope.in_dir
        if not base.exists():
            return []
        paths = [base]
    else:
        paths = [root]

    # Apply glob patterns if specified
    if scope.globs:
        expanded: list[Path] = []
        for path in paths:
            for glob in scope.globs:
                expanded.extend(path.glob(glob))
        paths = expanded

    return paths
