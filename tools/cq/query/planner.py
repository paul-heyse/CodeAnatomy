"""Query planner for cq queries.

Compiles Query IR into a ToolPlan that specifies which tools to run.
"""

from __future__ import annotations

from dataclasses import dataclass
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
from tools.cq.query.language import (
    DEFAULT_QUERY_LANGUAGE,
    DEFAULT_QUERY_LANGUAGE_SCOPE,
    QueryLanguage,
    QueryLanguageScope,
)

if TYPE_CHECKING:
    from tools.cq.query.ir import StrictnessMode


class AstGrepRule(msgspec.Struct, frozen=True):
    """ast-grep rule specification.

    Supports full ast-grep rule features including pattern objects and
    relational constraints.

    Attributes:
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

        Returns:
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

        Returns:
        -------
        dict
            Dictionary suitable for YAML serialization.

        Notes:
        -----
        Handles both simple patterns and pattern objects with context.
        When context is provided, generates pattern object format:
        `{"pattern": {"context": "...", "selector": "..."}}`
        """
        rule = _build_pattern_rule(self)
        _apply_kind_and_selector(rule, self)
        _apply_relational_rule(rule, "inside", self.inside, self.inside_stop_by, self.inside_field)
        _apply_relational_rule(rule, "has", self.has, self.has_stop_by, self.has_field)
        _apply_simple_pattern_rule(rule, "precedes", self.precedes)
        _apply_simple_pattern_rule(rule, "follows", self.follows)
        if self.composite:
            rule.update(self.composite.to_ast_grep_dict())
        _apply_nth_child(rule, self.nth_child)
        return rule


def _build_pattern_rule(rule: AstGrepRule) -> dict[str, object]:
    if rule.context:
        pattern_obj: dict[str, object] = {"context": rule.context}
        if rule.selector:
            pattern_obj["selector"] = rule.selector
        if rule.strictness != "smart":
            pattern_obj["strictness"] = rule.strictness
        return {"pattern": pattern_obj}
    base: dict[str, object] = {"pattern": rule.pattern}
    if rule.strictness != "smart":
        base["strictness"] = rule.strictness
    return base


def _apply_kind_and_selector(rule_dict: dict[str, object], rule: AstGrepRule) -> None:
    if rule.kind:
        rule_dict["kind"] = rule.kind
    if rule.selector and not rule.context:
        rule_dict["selector"] = rule.selector


def _apply_relational_rule(
    rule_dict: dict[str, object],
    key: str,
    pattern: str | None,
    stop_by: str | None,
    field_name: str | None,
) -> None:
    if not pattern:
        return
    nested: dict[str, object] = {"pattern": pattern}
    if stop_by:
        nested["stopBy"] = stop_by
    if field_name:
        nested["field"] = field_name
    rule_dict[key] = nested


def _apply_simple_pattern_rule(
    rule_dict: dict[str, object],
    key: str,
    pattern: str | None,
) -> None:
    if pattern:
        rule_dict[key] = {"pattern": pattern}


def _apply_nth_child(rule_dict: dict[str, object], spec: NthChildSpec | None) -> None:
    if spec is None:
        return
    nth: dict[str, object] = {"position": spec.position}
    if spec.reverse:
        nth["reverse"] = True
    if spec.of_rule:
        nth["ofRule"] = spec.of_rule
    rule_dict["nthChild"] = nth


class ToolPlan(msgspec.Struct, frozen=True):
    """Execution plan for a query.

    Attributes:
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
    lang
        Concrete query language for parsing and scan routing.
    lang_scope
        Language scope requested by the caller.
    """

    scope: Scope = msgspec.field(default_factory=Scope)
    sg_record_types: frozenset[str] = frozenset({"def", "call", "import"})
    need_symtable: bool = False
    need_bytecode: bool = False
    expand_ops: tuple[Expander, ...] = ()
    explain: bool = False
    sg_rules: tuple[AstGrepRule, ...] = ()
    is_pattern_query: bool = False
    lang: QueryLanguage = DEFAULT_QUERY_LANGUAGE
    lang_scope: QueryLanguageScope = DEFAULT_QUERY_LANGUAGE_SCOPE


_ENTITY_RECORDS: dict[str, set[str]] = {
    "function": {"def"},
    "class": {"def"},
    "method": {"def"},
    "module": {"def"},
    "callsite": {"call"},
    "import": {"import"},
    "decorator": {"def"},
}
_ENTITY_EXTRA_RECORDS: dict[str, set[str]] = {
    "function": {"call"},
    "class": {"call"},
    "method": {"call"},
}
_EXPANDER_RECORDS: dict[str, set[str]] = {
    "callers": {"def", "call"},
    "callees": {"def", "call"},
    "imports": {"import"},
    "raises": {"raise", "except"},
    "scope": {"def"},
}
_FIELD_RECORDS: dict[str, set[str]] = {
    "callers": {"def", "call"},
    "callees": {"def", "call"},
    "imports": {"import"},
}


def compile_query(query: Query) -> ToolPlan:
    """Compile a Query into a ToolPlan.

    Parameters
    ----------
    query
        Parsed query to compile.

    Used by the CLI query command and bundle builders to create execution plans.

    Returns:
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

    Returns:
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
        lang=query.primary_language,
        lang_scope=query.lang_scope,
    )


def _compile_pattern_query(query: Query) -> ToolPlan:
    """Compile a pattern-based query into a ToolPlan.

    Used by ``compile_query`` when a query uses ast-grep patterns.

    Returns:
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
        composite=query.composite,
        nth_child=query.nth_child,
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
        lang=query.primary_language,
        lang_scope=query.lang_scope,
    )


def _entity_to_ast_grep_rules(query: Query) -> tuple[AstGrepRule, ...]:
    """Convert entity query with relational constraints to ast-grep rules.

    Used by ``_compile_entity_query`` to translate entity filters into ast-grep rules.

    Returns:
    -------
    tuple[AstGrepRule, ...]
        One or more ast-grep rules representing the entity query.
    """
    assert query.entity is not None

    if query.primary_language == "rust":
        return _rust_entity_to_ast_grep_rules(query)

    # Python defaults
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
    if query.name and kind is None and not query.name.startswith("~"):
        base_pattern = base_pattern.replace("$FUNC", query.name)
        base_pattern = base_pattern.replace("$CLASS", query.name)
        base_pattern = base_pattern.replace("$METHOD", query.name)
        base_pattern = base_pattern.replace("$DECORATOR", query.name)
        base_pattern = base_pattern.replace("$MODULE", query.name)

    return (
        _apply_relational_constraints(
            AstGrepRule(pattern=base_pattern, kind=kind), query.relational
        ),
    )


def _rust_entity_to_ast_grep_rules(query: Query) -> tuple[AstGrepRule, ...]:
    """Build ast-grep rules for Rust entity constraints.

    Returns:
    -------
    tuple[AstGrepRule, ...]
        One or more ast-grep rules representing the Rust entity query.
    """
    assert query.entity is not None
    entity = query.entity
    if entity == "decorator":
        return ()

    if entity == "class":
        type_kinds = ("struct_item", "enum_item", "trait_item")
        return tuple(
            _apply_relational_constraints(AstGrepRule(pattern="$TYPE", kind=kind), query.relational)
            for kind in type_kinds
        )

    # Rust methods are function_item nodes scoped inside impl blocks.
    if entity == "method":
        base = AstGrepRule(pattern="$METHOD", kind="function_item", inside="impl $TYPE { $$$ }")
        return (_apply_relational_constraints(base, query.relational),)

    # Callsite matches both regular calls and macro invocations.
    if entity == "callsite":
        return tuple(
            _apply_relational_constraints(rule, query.relational)
            for rule in (
                AstGrepRule(pattern="$CALL", kind="call_expression"),
                AstGrepRule(pattern="$MAC", kind="macro_invocation"),
            )
        )

    base_rules: dict[str, AstGrepRule] = {
        "function": AstGrepRule(pattern="$FUNC", kind="function_item"),
        "module": AstGrepRule(pattern="$MOD", kind="mod_item"),
        "import": AstGrepRule(pattern="$USE", kind="use_declaration"),
    }
    base_rule = base_rules.get(entity)
    if base_rule is None:
        return ()
    return (_apply_relational_constraints(base_rule, query.relational),)


@dataclass
class _RelationalState:
    inside: str | None
    has: str | None
    precedes: str | None
    follows: str | None
    inside_stop_by: str | None
    has_stop_by: str | None
    inside_field: str | None
    has_field: str | None


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

    Returns:
    -------
    AstGrepRule
        New rule with relational constraints applied.
    """
    state = _RelationalState(
        inside=rule.inside,
        has=rule.has,
        precedes=rule.precedes,
        follows=rule.follows,
        inside_stop_by=rule.inside_stop_by,
        has_stop_by=rule.has_stop_by,
        inside_field=rule.inside_field,
        has_field=rule.has_field,
    )

    for constraint in constraints:
        _apply_relational_constraint(state, constraint)

    return _merge_relational_state(rule, state)


def _apply_relational_constraint(
    state: _RelationalState,
    constraint: RelationalConstraint,
) -> None:
    if constraint.operator == "inside":
        _apply_inside_constraint(state, constraint)
    elif constraint.operator == "has":
        _apply_has_constraint(state, constraint)
    elif constraint.operator == "precedes":
        state.precedes = constraint.pattern
    elif constraint.operator == "follows":
        state.follows = constraint.pattern


def _apply_inside_constraint(state: _RelationalState, constraint: RelationalConstraint) -> None:
    state.inside = constraint.pattern
    state.inside_stop_by = _normalize_stop_by(constraint)
    if constraint.field_name:
        state.inside_field = constraint.field_name


def _apply_has_constraint(state: _RelationalState, constraint: RelationalConstraint) -> None:
    state.has = constraint.pattern
    state.has_stop_by = _normalize_stop_by(constraint)
    if constraint.field_name:
        state.has_field = constraint.field_name


def _normalize_stop_by(constraint: RelationalConstraint) -> str | None:
    if constraint.stop_by != "neighbor":
        return str(constraint.stop_by)
    if constraint.operator not in {"inside", "has"}:
        return None
    pattern = constraint.pattern.strip()
    if pattern.startswith(("class ", "def ", "async def ")):
        return "end"
    return None


def _merge_relational_state(rule: AstGrepRule, state: _RelationalState) -> AstGrepRule:
    return AstGrepRule(
        pattern=rule.pattern,
        kind=rule.kind,
        context=rule.context,
        selector=rule.selector,
        strictness=rule.strictness,
        inside=state.inside,
        has=state.has,
        precedes=state.precedes,
        follows=state.follows,
        inside_stop_by=state.inside_stop_by,
        has_stop_by=state.has_stop_by,
        inside_field=state.inside_field,
        has_field=state.has_field,
        composite=rule.composite,
        nth_child=rule.nth_child,
    )


def _determine_record_types(query: Query) -> set[str]:
    """Determine which ast-grep record types are needed.

    Used by ``_compile_entity_query`` to determine ast-grep extraction types.

    Returns:
    -------
    set[str]
        Set of ast-grep record types required by the query.
    """
    record_types: set[str] = set()

    if query.entity is not None:
        record_types.update(_ENTITY_RECORDS.get(query.entity, set()))
        record_types.update(_ENTITY_EXTRA_RECORDS.get(query.entity, set()))

    for expander in query.expand:
        record_types.update(_EXPANDER_RECORDS.get(expander.kind, set()))

    for field in query.fields:
        record_types.update(_FIELD_RECORDS.get(field, set()))

    return record_types


def _needs_symtable(query: Query) -> bool:
    """Check if query requires symtable information.

    Used by plan compilation to decide if symtable analysis is needed.

    Returns:
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

    Returns:
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

    Returns:
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

    Returns:
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
