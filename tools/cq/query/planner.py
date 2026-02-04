"""Query planner for cq queries.

Compiles Query IR into a ToolPlan that specifies which tools to run.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import msgspec

from tools.cq.query.ir import (
    CompositeRule,
    Expander,
    NthChildSpec,
    Query,
    RelationalConstraint,
    Scope,
)

if TYPE_CHECKING:
    from tools.cq.query.ir import StrictnessMode


class AstGrepRule(msgspec.Struct, frozen=True):
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

    def to_yaml_dict(self) -> dict[str, object]:
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
            pattern_obj: dict[str, object] = {"context": self.context}
            if self.selector:
                pattern_obj["selector"] = self.selector
            if self.strictness != "smart":
                pattern_obj["strictness"] = self.strictness
            rule: dict[str, object] = {"pattern": pattern_obj}
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
            inside_rule: dict[str, object] = {"pattern": self.inside}
            if self.inside_stop_by:
                inside_rule["stopBy"] = self.inside_stop_by
            if self.inside_field:
                inside_rule["field"] = self.inside_field
            rule["inside"] = inside_rule

        if self.has:
            has_rule: dict[str, object] = {"pattern": self.has}
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
                nth["ofRule"] = self.nth_child.of_rule
            rule["nthChild"] = nth

        return rule


class ToolPlan(msgspec.Struct, frozen=True):
    """Execution plan for a query.

    Attributes
    ----------
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

    scope: Scope = msgspec.field(default_factory=Scope)
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

    Used by the CLI query command and bundle builders to create execution plans.

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
    """Compile an entity-based query into a ToolPlan.

    Used by ``compile_query`` when a query targets entities (functions, classes, etc.).

    Returns
    -------
    ToolPlan
        Execution plan for the entity query.
    """
    # Determine required ast-grep record types based on query
    sg_record_types = _determine_record_types(query)

    # Check if we need symtable or bytecode
    need_symtable = _needs_symtable(query)
    need_bytecode = _needs_bytecode(query)

    # Generate ast-grep rules if entity has relational constraints
    sg_rules = _entity_to_ast_grep_rules(query) if query.relational else ()

    return ToolPlan(
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
    """Compile a pattern-based query into a ToolPlan.

    Used by ``compile_query`` when a query uses ast-grep patterns.

    Returns
    -------
    ToolPlan
        Execution plan for the pattern query.
    """
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

    return ToolPlan(
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
    """Convert entity query with relational constraints to ast-grep rules.

    Used by ``_compile_entity_query`` to translate entity filters into ast-grep rules.

    Returns
    -------
    tuple[AstGrepRule, ...]
        One or more ast-grep rules representing the entity query.
    """
    assert query.entity is not None

    # Map entity types to ast-grep patterns/kinds
    entity_patterns = {
        "function": "$FUNC",
        "class": "$CLASS",
        "method": "$METHOD",
        "decorator": "@$DECORATOR($$$)",
        "import": "import $MODULE",
        "callsite": "$FUNC($$$)",
    }
    entity_kinds = {
        "function": "function_definition",
        "class": "class_definition",
        "method": "function_definition",
    }

    base_pattern = entity_patterns.get(query.entity)
    if not base_pattern:
        return ()

    kind = entity_kinds.get(query.entity)

    # Apply name filter if present (only when kind-based matching isn't used)
    if query.name and kind is None:
        if query.name.startswith("~"):
            _ = query.name[1:]  # regex for later metavar filtering
        else:
            base_pattern = base_pattern.replace("$FUNC", query.name)
            base_pattern = base_pattern.replace("$CLASS", query.name)
            base_pattern = base_pattern.replace("$METHOD", query.name)
            base_pattern = base_pattern.replace("$DECORATOR", query.name)
            base_pattern = base_pattern.replace("$MODULE", query.name)

    rule = AstGrepRule(pattern=base_pattern, kind=kind)

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

    def _normalize_stop_by(constraint: RelationalConstraint) -> str | None:
        if constraint.stop_by != "neighbor":
            return str(constraint.stop_by)
        if constraint.operator not in {"inside", "has"}:
            return None
        pattern = constraint.pattern.strip()
        if pattern.startswith(("class ", "def ", "async def ")):
            return "end"
        return None

    for constraint in constraints:
        if constraint.operator == "inside":
            inside = constraint.pattern
            inside_stop_by = _normalize_stop_by(constraint)
            if constraint.field_name:
                inside_field = constraint.field_name
        elif constraint.operator == "has":
            has = constraint.pattern
            has_stop_by = _normalize_stop_by(constraint)
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


def _determine_record_types(query: Query) -> set[str]:
    """Determine which ast-grep record types are needed.

    Used by ``_compile_entity_query`` to determine ast-grep extraction types.

    Returns
    -------
    set[str]
        Set of ast-grep record types required by the query.
    """
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
        if query.entity in {"function", "class", "method"}:
            record_types.add("call")

    # Add record types based on expanders
    for expander in query.expand:
        if expander.kind in {"callers", "callees"}:
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
    if "callers" in query.fields or "callees" in query.fields:
        record_types.add("def")
        record_types.add("call")
    if "imports" in query.fields:
        record_types.add("import")

    return record_types


def _needs_symtable(query: Query) -> bool:
    """Check if query requires symtable information.

    Used by plan compilation to decide if symtable analysis is needed.

    Returns
    -------
    bool
        True if symtable analysis is required.
    """
    # Symtable needed for scope analysis
    if any(e.kind == "scope" for e in query.expand):
        return True

    # Symtable useful for accurate caller/callee resolution
    if any(e.kind in {"callers", "callees"} for e in query.expand):
        return True

    # Symtable needed for scope filtering (closure detection, etc.)
    return query.scope_filter is not None


def _needs_bytecode(query: Query) -> bool:
    """Check if query requires bytecode analysis.

    Used by plan compilation to decide if bytecode inspection is needed.

    Returns
    -------
    bool
        True if bytecode analysis is required.
    """
    # Bytecode needed for bytecode_surface expander
    if any(e.kind == "bytecode_surface" for e in query.expand):
        return True

    # Bytecode useful for more accurate call resolution
    return "evidence" in query.fields


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

    return paths


def scope_to_globs(scope: Scope) -> list[str]:
    """Convert scope constraints to ast-grep globs.

    Used by the search executor to configure ast-grep include/exclude patterns.

    Returns
    -------
    list[str]
        Glob patterns for ast-grep (including exclusions).
    """
    globs: list[str] = []
    if scope.globs:
        globs.extend(scope.globs)
    if scope.exclude:
        globs.extend([f"!{pattern}" for pattern in scope.exclude])
    return globs
