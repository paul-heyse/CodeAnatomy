"""Query IR (Intermediate Representation) for cq queries.

Defines the dataclasses that represent parsed queries before execution.
"""

from __future__ import annotations

from typing import Annotated, Literal

import msgspec

# Entity types that can be queried
EntityType = Literal["function", "class", "method", "module", "callsite", "import", "decorator"]

# Expander kinds for graph traversal
ExpanderKind = Literal[
    "callers",
    "callees",
    "imports",
    "raises",
    "scope",
    "bytecode_surface",
]

# Output field types
FieldType = Literal[
    "def",
    "loc",
    "callers",
    "callees",
    "evidence",
    "hazards",
    "imports",
    "decorators",
    "decorated_functions",
]

# Strictness modes for pattern matching
StrictnessMode = Literal["cst", "smart", "ast", "relaxed", "signature"]

# Strictness mode descriptions for explain mode and documentation
STRICTNESS_DESCRIPTIONS: dict[str, str] = {
    "cst": "Match all nodes exactly including whitespace and trivial source nodes",
    "smart": "Match all nodes except trivial source nodes (default)",
    "ast": "Match only named AST nodes, ignore unnamed nodes",
    "relaxed": "Match AST nodes, ignore comments",
    "signature": "Match AST structure without comparing text content",
}

# Relational operators for structural constraints
RelationalOp = Literal["inside", "has", "precedes", "follows"]

# Stop-by mode for relational constraints
StopByMode = Literal["neighbor", "end"]

# Metavariable capture kinds
MetaVarKind = Literal["single", "multi", "unnamed"]

# Common Python AST field names for field-scoped searches
PYTHON_AST_FIELDS: frozenset[str] = frozenset({
    # Function
    "name", "args", "body", "decorator_list", "returns", "type_params",
    # Arguments
    "posonlyargs", "vararg", "kwonlyargs", "kw_defaults", "kwarg", "defaults",
    # Class
    "bases", "keywords",
    # Dict
    "keys", "values",
    # Call
    "func",
    # Subscript
    "value", "slice",
    # Attribute
    "attr",
    # Assign
    "targets",
    # If/While/For
    "test", "orelse",
    # Try
    "handlers", "finalbody",
    # With
    "items",
    # Match
    "subject", "cases",
})


class Expander(msgspec.Struct, frozen=True):
    """Graph expansion operator.

    Attributes
    ----------
    kind
        Type of expansion (callers, callees, imports, etc.)
    depth
        Maximum traversal depth (default: 1)
    """

    kind: ExpanderKind
    depth: Annotated[int, msgspec.Meta(ge=1)] = 1


class Scope(msgspec.Struct, frozen=True):
    """File scope constraints for queries.

    Attributes
    ----------
    in_dir
        Only search in this directory (relative to repo root)
    exclude
        Directories/patterns to exclude
    globs
        Glob patterns for file matching
    """

    in_dir: str | None = None
    exclude: tuple[str, ...] = ()
    globs: tuple[str, ...] = ()


class MetaVarCapture(msgspec.Struct, frozen=True):
    """Captured metavariable from pattern match.

    Represents a value captured by a metavariable during ast-grep pattern matching.

    Attributes
    ----------
    name
        Metavariable name (without $ prefix)
    kind
        Capture type: 'single' for $X, 'multi' for $$$X, 'unnamed' for $$X
    text
        Captured text content (for multi captures, comma-separated)
    nodes
        For multi captures ($$$), list of individual node texts.
        None for single captures.

    Notes
    -----
    Metavariable naming conventions:
    - `$NAME`: Named single capture (equality enforced on reuse)
    - `$$$NAME`: Named multi capture (zero-or-more nodes)
    - `$$NAME`: Unnamed node capture (operators, punctuation)
    - `$_NAME`: Non-capturing wildcard (no equality enforcement)
    """

    name: str
    kind: MetaVarKind
    text: str
    nodes: list[str] | None = None


class MetaVarFilter(msgspec.Struct, frozen=True):
    """Filter on captured metavariable values.

    Used to post-filter pattern matches based on captured metavariable content.

    Attributes
    ----------
    name
        Metavariable name (without $ prefix, e.g., 'OP' for $$OP)
    pattern
        Regex pattern to match against captured text
    negate
        If True, match when pattern does NOT match

    Examples
    --------
    Filter binary expressions by operator:
    ```
    /cq q "pattern='$L $$OP $R' $$OP=~'^[<>=]'"
    ```

    Exclude specific patterns:
    ```
    /cq q "pattern='$X' $$X!=~debug"
    ```
    """

    name: str
    pattern: str
    negate: bool = False

    def matches(self, capture: MetaVarCapture) -> bool:
        """Check if capture matches this filter.

        Parameters
        ----------
        capture
            Metavariable capture to test.

        Returns
        -------
        bool
            True if capture passes the filter.
        """
        import re

        match = bool(re.search(self.pattern, capture.text))
        return not match if self.negate else match


class NthChildSpec(msgspec.Struct, frozen=True):
    """Positional matching specification for nthChild queries.

    Attributes
    ----------
    position
        Exact position (1-indexed) or formula like '2n+1' for odd positions
    reverse
        If True, count from end instead of start
    of_rule
        Optional rule to filter which siblings count (e.g., 'kind=identifier')
    """

    position: str | int
    reverse: bool = False
    of_rule: str | None = None


class PatternSpec(msgspec.Struct, frozen=True):
    """Pattern specification for ast-grep based queries.

    Supports full ast-grep pattern objects for disambiguation and advanced matching.

    Attributes
    ----------
    pattern
        The ast-grep pattern to match (e.g., 'def $F($$$)')
    context
        Surrounding code for proper parsing. Required for ambiguous patterns
        that cannot be parsed standalone (e.g., JSON pairs, class fields).
        When provided, the context becomes the parseable pattern and selector
        extracts the specific node to match.
    selector
        Tree-sitter node kind to extract from parsed context (e.g.,
        'function_definition', 'pair', 'field_definition'). Used with context
        to disambiguate what part of the pattern to match.
    strictness
        Matching strictness mode (cst, smart, ast, relaxed, signature).
        Controls how strictly the pattern must match the source code.

    Notes
    -----
    Pattern objects with context/selector support disambiguation for patterns
    that don't parse correctly as standalone code:

    - JSON pair parsing: `context='{ "a": 123 }' selector=pair`
    - Class fields vs assignments: `context='class A { $F = $V }' selector=field_definition`

    Metavariable syntax:
    - `$NAME`: Single named node capture (enforces equality on reuse)
    - `$$$NAME`: Zero-or-more nodes (non-greedy)
    - `$$NAME`: Unnamed node capture (operators, punctuation)
    - `$_NAME`: Non-capturing wildcard (no equality enforcement)
    """

    pattern: str
    context: str | None = None
    selector: str | None = None
    strictness: StrictnessMode = "smart"

    def requires_yaml_rule(self) -> bool:
        """Check if pattern needs full YAML rule (vs simple CLI pattern).

        Returns
        -------
        bool
            True if pattern requires YAML inline rule for execution.
            This is needed when:
            - context is provided (for disambiguation)
            - selector is provided (for node extraction)
            - strictness is not "smart" (default)
        """
        return (
            self.context is not None
            or self.selector is not None
            or self.strictness != "smart"
        )

    def to_yaml_dict(self) -> dict:
        """Convert to ast-grep YAML rule format.

        Returns
        -------
        dict
            Dictionary suitable for YAML serialization as inline rule.

        Notes
        -----
        Generates the appropriate ast-grep rule structure:
        - With context: `{"pattern": {"context": ..., "selector": ...}}`
        - Without context: `{"pattern": "...", "strictness": ...}`
        """
        if self.context:
            # Pattern object format with context for disambiguation
            pattern_obj: dict = {"context": self.context}
            if self.selector:
                pattern_obj["selector"] = self.selector
            if self.strictness != "smart":
                pattern_obj["strictness"] = self.strictness
            return {"pattern": pattern_obj}

        # Simple pattern format
        result: dict = {"pattern": self.pattern}
        if self.strictness != "smart":
            result["strictness"] = self.strictness
        return result


class RelationalConstraint(msgspec.Struct, frozen=True):
    """Relational constraint for structural queries.

    Supports ast-grep relational rules: inside, has, precedes, follows.

    Attributes
    ----------
    operator
        Relational operator (inside, has, precedes, follows):
        - inside: Match target inside ancestor matching pattern
        - has: Match target containing descendant matching pattern
        - precedes: Match target appearing before sibling matching pattern
        - follows: Match target appearing after sibling matching pattern
    pattern
        Pattern that must satisfy the relation
    stop_by
        When to stop searching:
        - "neighbor": Direct parent/child/sibling only (default)
        - "end": Search to root/leaf/edge
        - Custom pattern string: Stop when pattern matches
    field_name
        Optional field name for field-specific matching (inside/has only).
        Constrains search to specific AST fields (e.g., 'keys', 'returns').

    Notes
    -----
    Field constraints are only valid for `inside` and `has` operators.
    Using field_name with `precedes` or `follows` will raise ValueError.

    The stopBy parameter is inclusive - if both the stop pattern and
    relational pattern match the same node, it still counts as a match.
    """

    operator: RelationalOp
    pattern: str
    stop_by: StopByMode | str = "neighbor"
    field_name: str | None = None

    def __post_init__(self) -> None:
        """Validate constraint configuration."""
        # precedes/follows don't support field constraint
        if self.operator in ("precedes", "follows") and self.field_name:
            msg = f"{self.operator!r} operator does not support 'field' constraint"
            raise ValueError(msg)

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


# Composite rule operator types
CompositeOp = Literal["all", "any", "not"]


class CompositeRule(msgspec.Struct, frozen=True):
    """Composite rule for combining multiple pattern rules.

    Supports ast-grep composite rules: all, any, not.

    Attributes
    ----------
    operator
        Composite operator:
        - "all": All sub-rules must match (AND logic)
        - "any": At least one sub-rule must match (OR logic)
        - "not": Sub-rule must NOT match (negation)
    patterns
        List of patterns or nested CompositeRule specs to combine
    metavar_order
        For "all" operator, specifies metavariable extraction order

    Examples
    --------
    Match functions with specific patterns:
    ```
    all=['def $F($$$)', 'inside=class Config']
    ```

    Match any logging pattern:
    ```
    any=['logger.$M($$$)', 'print($$$)', 'console.log($$$)']
    ```

    Exclude debug statements:
    ```
    not='console.log("debug")'
    ```
    """

    operator: CompositeOp
    patterns: tuple[str, ...]
    metavar_order: tuple[str, ...] | None = None

    def to_ast_grep_dict(self) -> dict:
        """Convert to ast-grep rule format.

        Returns
        -------
        dict
            Dictionary suitable for ast-grep YAML rule.
        """
        if self.operator == "not":
            # Not takes a single rule
            if len(self.patterns) != 1:
                msg = "'not' operator requires exactly one pattern"
                raise ValueError(msg)
            return {"not": {"pattern": self.patterns[0]}}

        # all/any take list of rules
        rules = [{"pattern": p} for p in self.patterns]
        return {self.operator: rules}


class ScopeFilter(msgspec.Struct, frozen=True):
    """Scope-based filter using symtable analysis.

    Attributes
    ----------
    scope_type
        Filter to specific scope types (closure, generator, coroutine, etc.)
    captures
        Filter to scopes that capture specific variables
    has_cells
        Filter to scopes with/without cell variables (closures)
    """

    scope_type: str | None = None
    captures: str | None = None
    has_cells: bool | None = None


class DecoratorFilter(msgspec.Struct, frozen=True):
    """Filter for decorator-related queries.

    Attributes
    ----------
    decorated_by
        Filter to items decorated by specific decorator
    decorator_count_min
        Minimum number of decorators
    decorator_count_max
        Maximum number of decorators
    """

    decorated_by: str | None = None
    decorator_count_min: int | None = None
    decorator_count_max: int | None = None


class JoinTarget(msgspec.Struct, frozen=True):
    """Target for join queries.

    Attributes
    ----------
    entity
        Target entity type
    name
        Optional name filter
    """

    entity: EntityType
    name: str | None = None

    @classmethod
    def parse(cls, spec: str) -> JoinTarget:
        """Parse a join target specification.

        Parameters
        ----------
        spec
            Target spec like 'function' or 'function:foo'

        Returns
        -------
        JoinTarget
            Parsed join target.
        """
        if ":" in spec:
            entity_str, name = spec.split(":", 1)
        else:
            entity_str = spec
            name = None
        return cls(entity=entity_str, name=name)  # type: ignore[arg-type]


class JoinConstraint(msgspec.Struct, frozen=True):
    """Join constraint for cross-entity queries.

    Attributes
    ----------
    join_type
        Type of join (used_by, defines, raises, exports)
    target
        Target entity specification
    """

    join_type: Literal["used_by", "defines", "raises", "exports"]
    target: JoinTarget


class Query(msgspec.Struct, frozen=True):
    """Parsed query representation.

    Supports two modes:
    1. Entity queries: Search by entity type (function, class, etc.)
    2. Pattern queries: Search by ast-grep pattern

    Attributes
    ----------
    entity
        Type of entity to search for (function, class, method, module, callsite, import)
    name
        Name pattern to match (exact or regex with ~prefix)
    expand
        List of graph expansion operators
    scope
        File scope constraints
    fields
        Output fields to include
    limit
        Maximum number of results
    explain
        Whether to include query plan explanation
    pattern_spec
        Pattern specification for ast-grep based queries (mutually exclusive with entity)
    relational
        Relational constraints (inside, has, precedes, follows)
    scope_filter
        Symtable-based scope filtering
    decorator_filter
        Decorator-based filtering
    joins
        Cross-entity join constraints
    metavar_filters
        Post-filters on captured metavariable values
    composite
        Composite rule (all/any/not) for combining patterns
    nth_child
        Positional matching specification
    """

    entity: EntityType | None = None
    name: str | None = None
    expand: tuple[Expander, ...] = ()
    scope: Scope = msgspec.field(default_factory=Scope)
    fields: tuple[FieldType, ...] = ("def",)
    limit: int | None = None
    explain: bool = False
    pattern_spec: PatternSpec | None = None
    relational: tuple[RelationalConstraint, ...] = ()
    scope_filter: ScopeFilter | None = None
    decorator_filter: DecoratorFilter | None = None
    joins: tuple[JoinConstraint, ...] = ()
    metavar_filters: tuple[MetaVarFilter, ...] = ()
    composite: CompositeRule | None = None
    nth_child: NthChildSpec | None = None

    def __post_init__(self) -> None:
        """Validate query configuration."""
        if self.entity is None and self.pattern_spec is None:
            msg = "Query must specify either 'entity' or 'pattern_spec'"
            raise ValueError(msg)
        if self.entity is not None and self.pattern_spec is not None:
            msg = "Query cannot specify both 'entity' and 'pattern_spec'"
            raise ValueError(msg)

    @property
    def is_pattern_query(self) -> bool:
        """Check if this is a pattern-based query."""
        return self.pattern_spec is not None

    def with_scope(self, scope: Scope) -> Query:
        """Return a new Query with updated scope."""
        return Query(
            entity=self.entity,
            name=self.name,
            expand=self.expand,
            scope=scope,
            fields=self.fields,
            limit=self.limit,
            explain=self.explain,
            pattern_spec=self.pattern_spec,
            relational=self.relational,
            scope_filter=self.scope_filter,
            decorator_filter=self.decorator_filter,
            joins=self.joins,
            metavar_filters=self.metavar_filters,
            composite=self.composite,
            nth_child=self.nth_child,
        )

    def with_expand(self, *expanders: Expander) -> Query:
        """Return a new Query with additional expanders."""
        return Query(
            entity=self.entity,
            name=self.name,
            expand=self.expand + expanders,
            scope=self.scope,
            fields=self.fields,
            limit=self.limit,
            explain=self.explain,
            pattern_spec=self.pattern_spec,
            relational=self.relational,
            scope_filter=self.scope_filter,
            decorator_filter=self.decorator_filter,
            joins=self.joins,
            metavar_filters=self.metavar_filters,
            composite=self.composite,
            nth_child=self.nth_child,
        )

    def with_fields(self, *fields: FieldType) -> Query:
        """Return a new Query with specified fields."""
        return Query(
            entity=self.entity,
            name=self.name,
            expand=self.expand,
            scope=self.scope,
            fields=fields,
            limit=self.limit,
            explain=self.explain,
            pattern_spec=self.pattern_spec,
            relational=self.relational,
            scope_filter=self.scope_filter,
            decorator_filter=self.decorator_filter,
            joins=self.joins,
            metavar_filters=self.metavar_filters,
            composite=self.composite,
            nth_child=self.nth_child,
        )

    def with_relational(self, *constraints: RelationalConstraint) -> Query:
        """Return a new Query with additional relational constraints."""
        return Query(
            entity=self.entity,
            name=self.name,
            expand=self.expand,
            scope=self.scope,
            fields=self.fields,
            limit=self.limit,
            explain=self.explain,
            pattern_spec=self.pattern_spec,
            relational=self.relational + constraints,
            scope_filter=self.scope_filter,
            decorator_filter=self.decorator_filter,
            joins=self.joins,
            metavar_filters=self.metavar_filters,
            composite=self.composite,
            nth_child=self.nth_child,
        )

    def get_all_relational_constraints(self) -> list[RelationalConstraint]:
        """Get all relational constraints.

        Returns
        -------
        list[RelationalConstraint]
            All relational constraints in this query.
        """
        return list(self.relational)
