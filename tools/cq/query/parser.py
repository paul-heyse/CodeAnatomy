"""Query string parser for cq queries.

Parses key=value query strings into Query IR objects.

Example query strings:
    entity=function name=foo
    entity=function name=build expand=callers(depth=2) in=src/
    entity=class in=src/relspec/ fields=def,imports
    pattern='def $F($$$)' in=src/
    entity=function inside='class Config' scope=closure
"""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal, cast

from tools.cq.query.ir import (
    CompositeRule,
    DecoratorFilter,
    Expander,
    JoinConstraint,
    JoinTarget,
    MetaVarFilter,
    NthChildSpec,
    PatternSpec,
    Query,
    RelationalConstraint,
    Scope,
    ScopeFilter,
)

if TYPE_CHECKING:
    from tools.cq.query.ir import EntityType, ExpanderKind, FieldType, RelationalOp, StrictnessMode

JoinType = Literal["used_by", "defines", "raises", "exports"]

_TokenHandler = Callable[[dict[str, str], Any], None]


class QueryParseError(ValueError):
    """Raised when a query string cannot be parsed."""


_MISSING_ENTITY_MESSAGE = (
    "Query must specify 'entity' (function, class, method, module, callsite, import, decorator)"
)
_MISSING_PATTERN_MESSAGE = "Pattern query must specify 'pattern'"
_INVALID_COMPOSITE_MESSAGE = "'not' operator requires exactly one pattern"


def _invalid_entity_message(entity_str: str, valid: tuple[str, ...]) -> str:
    return f"Invalid entity type: {entity_str!r}. Valid types: {', '.join(valid)}"


def _invalid_strictness_message(strictness_str: str, valid: tuple[str, ...]) -> str:
    return f"Invalid strictness mode: {strictness_str!r}. Valid modes: {', '.join(valid)}"


def _invalid_relational_op_message(op_str: str, valid: tuple[str, ...]) -> str:
    return f"Invalid relational operator: {op_str!r}. Valid operators: {', '.join(valid)}"


def _invalid_expander_format_message(part: str) -> str:
    return f"Invalid expander format: {part!r}"


def _invalid_expander_params_message(params: str) -> str:
    return f"Invalid expander params: {params!r}. Expected 'depth=N'"


def _invalid_expander_kind_message(kind: str, valid: tuple[str, ...]) -> str:
    return f"Invalid expander kind: {kind!r}. Valid kinds: {', '.join(valid)}"


def _invalid_field_message(field: str, valid: tuple[str, ...]) -> str:
    return f"Invalid field: {field!r}. Valid fields: {', '.join(valid)}"


@dataclass
class _EntityQueryState:
    entity: EntityType
    name: str | None = None
    expand: tuple[Expander, ...] = ()
    scope: Scope = field(default_factory=Scope)
    fields: tuple[FieldType, ...] = field(default_factory=tuple)
    limit: int | None = None
    explain: bool = False
    relational: tuple[RelationalConstraint, ...] = ()
    scope_filter: ScopeFilter | None = None
    decorator_filter: DecoratorFilter | None = None
    joins: tuple[JoinConstraint, ...] = ()
    metavar_filters: tuple[MetaVarFilter, ...] = ()
    composite: CompositeRule | None = None
    nth_child: NthChildSpec | None = None

    def build(self) -> Query:
        return Query(
            entity=self.entity,
            name=self.name,
            expand=self.expand,
            scope=self.scope,
            fields=self.fields,
            limit=self.limit,
            explain=self.explain,
            relational=self.relational,
            scope_filter=self.scope_filter,
            decorator_filter=self.decorator_filter,
            joins=self.joins,
            metavar_filters=self.metavar_filters,
            composite=self.composite,
            nth_child=self.nth_child,
        )


@dataclass
class _PatternQueryState:
    pattern_spec: PatternSpec
    scope: Scope = field(default_factory=Scope)
    fields: tuple[FieldType, ...] = field(default_factory=tuple)
    limit: int | None = None
    explain: bool = False
    relational: tuple[RelationalConstraint, ...] = ()
    metavar_filters: tuple[MetaVarFilter, ...] = ()
    composite: CompositeRule | None = None
    nth_child: NthChildSpec | None = None

    def build(self) -> Query:
        return Query(
            pattern_spec=self.pattern_spec,
            scope=self.scope,
            fields=self.fields,
            limit=self.limit,
            explain=self.explain,
            relational=self.relational,
            metavar_filters=self.metavar_filters,
            composite=self.composite,
            nth_child=self.nth_child,
        )


def _apply_token_handlers(
    tokens: dict[str, str],
    handlers: dict[str, _TokenHandler],
    order: tuple[str, ...],
    state: object,
) -> None:
    for key in order:
        handler = handlers.get(key)
        if handler is not None:
            handler(tokens, state)


def _handle_name(tokens: dict[str, str], state: _EntityQueryState) -> None:
    state.name = tokens.get("name")


def _handle_expand(tokens: dict[str, str], state: _EntityQueryState) -> None:
    expand_str = tokens.get("expand", "")
    state.expand = _parse_expanders(expand_str) if expand_str else ()


def _handle_scope(tokens: dict[str, str], state: Any) -> None:
    state.scope = _parse_scope(tokens)


def _handle_fields(tokens: dict[str, str], state: Any) -> None:
    state.fields = _parse_fields(tokens.get("fields", "def"))


def _handle_limit(tokens: dict[str, str], state: Any) -> None:
    limit_str = tokens.get("limit")
    state.limit = int(limit_str) if limit_str else None


def _handle_explain(tokens: dict[str, str], state: Any) -> None:
    state.explain = tokens.get("explain", "").lower() in {"true", "1", "yes"}


_ENTITY_TOKEN_HANDLERS: dict[str, _TokenHandler] = {
    "name": _handle_name,
    "expand": _handle_expand,
    "scope": _handle_scope,
    "fields": _handle_fields,
    "limit": _handle_limit,
    "explain": _handle_explain,
}
_ENTITY_HANDLER_ORDER = ("name", "expand", "scope", "fields", "limit", "explain")

_PATTERN_TOKEN_HANDLERS: dict[str, _TokenHandler] = {
    "scope": _handle_scope,
    "fields": _handle_fields,
    "limit": _handle_limit,
    "explain": _handle_explain,
}
_PATTERN_HANDLER_ORDER = ("scope", "fields", "limit", "explain")


def parse_query(query_string: str) -> Query:
    """Parse a query string into a Query object.

    Parameters
    ----------
    query_string
        Query string in key=value format.

    Returns
    -------
    Query
        Parsed query object.

    Examples
    --------
    >>> q = parse_query("entity=function name=foo")
    >>> q.entity
    'function'
    >>> q.name
    'foo'

    >>> q = parse_query("entity=function expand=callers(depth=2)")
    >>> q.expand[0].kind
    'callers'
    >>> q.expand[0].depth
    2

    >>> q = parse_query("pattern='def $F($$$)'")
    >>> q.is_pattern_query
    True
    """
    tokens = _tokenize(query_string)

    # Dispatch to appropriate parser based on query type
    # Pattern queries can use either 'pattern' or 'pattern.context'
    if "pattern" in tokens or "pattern.context" in tokens:
        return _parse_pattern_query(tokens)
    return _parse_entity_query(tokens)


def _tokenize(query_string: str) -> dict[str, str]:
    """Tokenize query string into key-value pairs.

    Handles quoted values (single and double quotes), nested parentheses,
    and dot-notation keys (e.g., pattern.context, inside.stopBy).

    Also handles metavariable filter tokens like $$OP=~pattern.

    Used by ``parse_query`` to normalize the CLI query string before parsing.

    Returns
    -------
    dict[str, str]
        Mapping of token keys to token values.
    """
    tokens: dict[str, str] = {}
    # Match key=value pairs, handling:
    # - Dot-notation keys: pattern.context, inside.stopBy
    # - Metavar filter keys: $NAME, $$NAME, $$$NAME
    # - Quoted strings (single or double)
    # - Unquoted values
    # Pattern order matters: try single quotes, then double quotes, then unquoted
    pattern = r"([\w.]+|\$+\w+)=(?:'([^']+)'|\"([^\"]+)\"|([^\s]+))"

    for match in re.finditer(pattern, query_string):
        key = match.group(1)
        # Use first non-None value from capture groups
        value = match.group(2) or match.group(3) or match.group(4) or ""
        tokens[key] = value

    return tokens


def _parse_entity_query(tokens: dict[str, str]) -> Query:
    """Parse an entity-based query from tokens.

    Used by ``parse_query`` when an ``entity=`` selector is present.

    Returns
    -------
    Query
        Query configured for entity-based execution.

    Raises
    ------
    QueryParseError
        If required entity fields are missing or invalid.
    """
    entity_str = tokens.get("entity")
    if not entity_str:
        raise QueryParseError(_MISSING_ENTITY_MESSAGE)

    state = _EntityQueryState(entity=_parse_entity(entity_str))
    _apply_token_handlers(tokens, _ENTITY_TOKEN_HANDLERS, _ENTITY_HANDLER_ORDER, state)
    state.relational = _parse_relational_constraints(tokens)
    state.scope_filter = _parse_scope_filter(tokens)
    state.decorator_filter = _parse_decorator_filter(tokens)
    state.joins = _parse_joins(tokens)
    state.metavar_filters = _parse_metavar_filters(tokens)
    state.composite = _parse_composite_rule(tokens)
    state.nth_child = _parse_nth_child(tokens)
    return state.build()


def _parse_pattern_query(tokens: dict[str, str]) -> Query:
    """Parse a pattern-based query from tokens.

    Used by ``parse_query`` when a ``pattern`` token is present.

    Returns
    -------
    Query
        Query configured for ast-grep pattern execution.

    """
    state = _PatternQueryState(pattern_spec=_parse_pattern_object(tokens))
    _apply_token_handlers(tokens, _PATTERN_TOKEN_HANDLERS, _PATTERN_HANDLER_ORDER, state)
    state.relational = _parse_relational_constraints(tokens)
    state.metavar_filters = _parse_metavar_filters(tokens)
    state.composite = _parse_composite_rule(tokens)
    state.nth_child = _parse_nth_child(tokens)
    return state.build()


def _parse_pattern_object(tokens: dict[str, str]) -> PatternSpec:
    """Parse pattern object from tokens.

    Handles both simple patterns and full pattern objects with context/selector.
    Supports dot-notation: pattern.context, pattern.selector, pattern.strictness

    Used by ``_parse_pattern_query`` to build the pattern IR.

    Parameters
    ----------
    tokens
        Tokenized key-value pairs from query string.

    Returns
    -------
    PatternSpec
        Parsed pattern specification.

    Raises
    ------
    QueryParseError
        If required pattern is missing.
    """
    # Check for pattern object notation (pattern.context, pattern.selector)
    context = tokens.get("pattern.context") or tokens.get("context")
    selector = tokens.get("pattern.selector") or tokens.get("selector")
    strictness_str = tokens.get("pattern.strictness") or tokens.get("strictness", "smart")
    strictness = _parse_strictness(strictness_str)

    # Get the pattern string
    pattern_str = tokens.get("pattern")

    if context:
        # Pattern object: context provides the parseable code
        # The pattern within context is what we're looking for
        return PatternSpec(
            pattern=pattern_str or context,
            context=context,
            selector=selector,
            strictness=strictness,
        )

    if not pattern_str:
        raise QueryParseError(_MISSING_PATTERN_MESSAGE)

    # Simple pattern string
    return PatternSpec(
        pattern=pattern_str,
        context=None,
        selector=selector,
        strictness=strictness,
    )


def _parse_entity(entity_str: str) -> EntityType:
    """Parse and validate entity type.

    Used by ``_parse_entity_query`` to validate CLI entity selectors.

    Returns
    -------
    EntityType
        Validated entity type.

    Raises
    ------
    QueryParseError
        If the entity type is not supported.
    """
    valid_entities: tuple[EntityType, ...] = (
        "function",
        "class",
        "method",
        "module",
        "callsite",
        "import",
        "decorator",
    )
    valid_entity_set = set(valid_entities)
    if entity_str not in valid_entity_set:
        msg = _invalid_entity_message(entity_str, valid_entities)
        raise QueryParseError(msg)
    return cast("EntityType", entity_str)


def _parse_strictness(strictness_str: str) -> StrictnessMode:
    """Parse and validate strictness mode.

    Used by ``_parse_pattern_object`` to validate ``strictness=`` tokens.

    Returns
    -------
    StrictnessMode
        Validated strictness mode.

    Raises
    ------
    QueryParseError
        If the strictness mode is not supported.
    """
    valid_modes: tuple[StrictnessMode, ...] = ("cst", "smart", "ast", "relaxed", "signature")
    valid_mode_set = set(valid_modes)
    if strictness_str not in valid_mode_set:
        msg = _invalid_strictness_message(strictness_str, valid_modes)
        raise QueryParseError(msg)
    return cast("StrictnessMode", strictness_str)


def _parse_relational_op(op_str: str) -> RelationalOp:
    """Parse and validate relational operator.

    Used by ``_parse_relational_constraints`` to validate relation keys.

    Returns
    -------
    RelationalOp
        Validated relational operator.

    Raises
    ------
    QueryParseError
        If the operator is not supported.
    """
    valid_ops: tuple[RelationalOp, ...] = ("inside", "has", "precedes", "follows")
    valid_op_set = set(valid_ops)
    if op_str not in valid_op_set:
        msg = _invalid_relational_op_message(op_str, valid_ops)
        raise QueryParseError(msg)
    return cast("RelationalOp", op_str)


def _parse_relational_constraints(tokens: dict[str, str]) -> tuple[RelationalConstraint, ...]:
    """Parse relational constraints from tokens.

    Supported keys: inside, has, precedes, follows
    Optional modifiers:
    - Dot notation: inside.stopBy, inside.field
    - Underscore notation: inside_stop_by, inside_field (legacy)
    - Global: stopBy, field (applies to all operators)

    Used by ``_parse_entity_query`` and ``_parse_pattern_query`` for relational filters.

    Returns
    -------
    tuple[RelationalConstraint, ...]
        Parsed relational constraint list.
    """
    constraints: list[RelationalConstraint] = []

    relational_ops: tuple[RelationalOp, ...] = ("inside", "has", "precedes", "follows")

    # Global stopBy/field (applies to all if not overridden)
    global_stop_by: str = tokens.get("stopBy") or "neighbor"
    global_field = tokens.get("field")

    for op in relational_ops:
        pattern = tokens.get(op)
        if pattern:
            # Try dot notation first, then underscore, then global
            stop_by: str = (
                tokens.get(f"{op}.stopBy") or tokens.get(f"{op}_stop_by") or global_stop_by
            )
            field_name = tokens.get(f"{op}.field") or tokens.get(f"{op}_field") or global_field

            constraints.append(
                RelationalConstraint(
                    operator=op,
                    pattern=pattern,
                    stop_by=stop_by,
                    field_name=field_name,
                )
            )

    return tuple(constraints)


def _parse_scope_filter(tokens: dict[str, str]) -> ScopeFilter | None:
    """Parse scope filter from tokens.

    Supported keys: scope (type), captures, has_cells

    Used by ``_parse_entity_query`` to add symtable-driven scope filters.

    Returns
    -------
    ScopeFilter | None
        Parsed scope filter, or None if not present.
    """
    scope_type = tokens.get("scope")
    captures = tokens.get("captures")
    has_cells_str = tokens.get("has_cells")

    if not any([scope_type, captures, has_cells_str]):
        return None

    has_cells: bool | None = None
    if has_cells_str:
        has_cells = has_cells_str.lower() in {"true", "1", "yes"}

    return ScopeFilter(
        scope_type=scope_type,
        captures=captures,
        has_cells=has_cells,
    )


def _parse_decorator_filter(tokens: dict[str, str]) -> DecoratorFilter | None:
    """Parse decorator filter from tokens.

    Supported keys: decorated_by, decorator_count_min, decorator_count_max

    Used by ``_parse_entity_query`` to add decorator constraints.

    Returns
    -------
    DecoratorFilter | None
        Parsed decorator filter, or None if not present.
    """
    decorated_by = tokens.get("decorated_by")
    count_min_str = tokens.get("decorator_count_min")
    count_max_str = tokens.get("decorator_count_max")

    if not any([decorated_by, count_min_str, count_max_str]):
        return None

    count_min = int(count_min_str) if count_min_str else None
    count_max = int(count_max_str) if count_max_str else None

    return DecoratorFilter(
        decorated_by=decorated_by,
        decorator_count_min=count_min,
        decorator_count_max=count_max,
    )


def _parse_joins(tokens: dict[str, str]) -> tuple[JoinConstraint, ...]:
    """Parse join constraints from tokens.

    Supported keys: used_by, defines, raises, exports

    Used by ``_parse_entity_query`` to build join constraints.

    Returns
    -------
    tuple[JoinConstraint, ...]
        Parsed join constraints.

    """
    constraints: list[JoinConstraint] = []

    join_types: tuple[JoinType, ...] = ("used_by", "defines", "raises", "exports")

    for join_type in join_types:
        target_str = tokens.get(join_type)
        if target_str:
            target = JoinTarget.parse(target_str)
            constraints.append(
                JoinConstraint(
                    join_type=join_type,
                    target=target,
                )
            )

    return tuple(constraints)


def _parse_expanders(expand_str: str) -> tuple[Expander, ...]:
    """Parse expander specifications.

    Format: kind(depth=N) or just kind for depth=1
    Multiple expanders separated by comma.

    Used by ``_parse_entity_query`` to translate ``expand=`` arguments.

    Returns
    -------
    tuple[Expander, ...]
        Parsed expander specifications.

    Raises
    ------
    QueryParseError
        If the expander format or depth is invalid.
    """
    expanders: list[Expander] = []

    # Split by comma, handling nested parentheses
    parts = _split_expanders(expand_str)

    for raw_part in parts:
        part = raw_part.strip()
        if not part:
            continue

        # Parse expander: kind(depth=N) or just kind
        match = re.match(r"(\w+)(?:\(([^)]*)\))?", part)
        if not match:
            msg = _invalid_expander_format_message(part)
            raise QueryParseError(msg)

        kind_str = match.group(1)
        params_str = match.group(2) or ""

        kind = _parse_expander_kind(kind_str)
        depth = _parse_expander_params(params_str)

        expanders.append(Expander(kind=kind, depth=depth))

    return tuple(expanders)


def _split_expanders(expand_str: str) -> list[str]:
    """Split expander string by comma, respecting parentheses.

    Used by ``_parse_expanders`` to segment ``expand=`` specs.

    Returns
    -------
    list[str]
        Raw expander string segments.
    """
    parts: list[str] = []
    current: list[str] = []
    depth = 0

    for char in expand_str:
        if char in {"(", "["}:
            depth += 1
            current.append(char)
        elif char in {")", "]"}:
            depth -= 1
            current.append(char)
        elif char == "," and depth == 0:
            parts.append("".join(current))
            current = []
        else:
            current.append(char)

    if current:
        parts.append("".join(current))

    return parts


def _parse_expander_kind(kind_str: str) -> ExpanderKind:
    """Parse and validate expander kind.

    Used by ``_parse_expanders`` to validate expander kinds.

    Returns
    -------
    ExpanderKind
        Validated expander kind.

    Raises
    ------
    QueryParseError
        If the expander kind is not supported.
    """
    valid_kinds: tuple[ExpanderKind, ...] = (
        "callers",
        "callees",
        "imports",
        "raises",
        "scope",
        "bytecode_surface",
    )
    valid_kind_set = set(valid_kinds)
    if kind_str not in valid_kind_set:
        msg = _invalid_expander_kind_message(kind_str, valid_kinds)
        raise QueryParseError(msg)
    return cast("ExpanderKind", kind_str)


def _parse_expander_params(params_str: str) -> int:
    """Parse expander parameters (depth=N).

    Used by ``_parse_expanders`` to parse depth settings.

    Returns
    -------
    int
        Parsed depth value.

    Raises
    ------
    QueryParseError
        If the depth parameter is malformed.
    """
    if not params_str:
        return 1

    # Parse depth=N
    match = re.match(r"depth=(\d+)", params_str.strip())
    if match:
        return int(match.group(1))

    msg = _invalid_expander_params_message(params_str)
    raise QueryParseError(msg)


def _parse_scope(tokens: dict[str, str]) -> Scope:
    """Parse scope constraints from tokens.

    Used by ``_parse_entity_query`` and ``_parse_pattern_query`` to apply in/exclude/globs.

    Returns
    -------
    Scope
        Parsed scope constraints.
    """
    in_dir = tokens.get("in")
    exclude_str = tokens.get("exclude", "")
    globs_str = tokens.get("globs", "")

    exclude = tuple(e.strip() for e in exclude_str.split(",") if e.strip())
    globs = tuple(g.strip() for g in globs_str.split(",") if g.strip())

    return Scope(in_dir=in_dir, exclude=exclude, globs=globs)


def _parse_fields(fields_str: str) -> tuple[FieldType, ...]:
    """Parse and validate field types.

    Used by ``_parse_entity_query`` and ``_parse_pattern_query`` to validate field lists.

    Returns
    -------
    tuple[FieldType, ...]
        Parsed field list.

    Raises
    ------
    QueryParseError
        If any field name is not supported.
    """
    valid_fields: tuple[FieldType, ...] = (
        "def",
        "loc",
        "callers",
        "callees",
        "evidence",
        "imports",
        "decorators",
        "decorated_functions",
    )
    valid_field_set = set(valid_fields)

    fields: list[FieldType] = []
    for raw_field in fields_str.split(","):
        field_str = raw_field.strip()
        if not field_str:
            continue
        if field_str not in valid_field_set:
            msg = _invalid_field_message(field_str, valid_fields)
            raise QueryParseError(msg)
        fields.append(cast("FieldType", field_str))

    return tuple(fields) if fields else ("def",)


def _parse_metavar_filters(tokens: dict[str, str]) -> tuple[MetaVarFilter, ...]:
    """Parse metavariable filter tokens.

    Handles tokens like:
    - $$OP=~'^[<>=]'  (regex match)
    - $X!=~debug      (negated regex match)
    - $NAME=~pattern  (single var filter)

    Parameters
    ----------
    tokens
        Tokenized query string.

    Used by ``_parse_entity_query`` and ``_parse_pattern_query`` for metavariable filters.

    Returns
    -------
    tuple[MetaVarFilter, ...]
        Filters to apply on metavariable captures.
    """
    filters: list[MetaVarFilter] = []

    for key, value in tokens.items():
        # Match metavar filter keys: $NAME, $$NAME, $$$NAME
        if not key.startswith("$"):
            continue

        # Extract name (strip $ prefixes)
        name = key.lstrip("$")
        if not name:
            continue

        # Must have ~ to indicate regex filter
        if "~" not in value:
            continue

        # Check for negation prefix (! before or after =)
        # Formats: !~pattern, =!~pattern, !=~pattern
        negate = "!" in value.split("~", 1)[0]
        work_value = value.replace("!", "")

        # Extract pattern after ~ (handle both =~ and just ~)
        if "=~" in work_value:
            pattern = work_value.split("=~", 1)[1]
        elif work_value.startswith("~"):
            pattern = work_value[1:]
        else:
            continue

        # Strip surrounding quotes from pattern if present
        pattern = pattern.strip("'\"")

        if pattern:
            filters.append(MetaVarFilter(name=name, pattern=pattern, negate=negate))

    return tuple(filters)


def _parse_composite_rule(tokens: dict[str, str]) -> CompositeRule | None:
    """Parse composite rule from tokens.

    Handles:
    - all='pattern1,pattern2'
    - any='pattern1,pattern2'
    - not='pattern'

    Parameters
    ----------
    tokens
        Tokenized query string.

    Used by ``_parse_entity_query`` and ``_parse_pattern_query`` for composite rules.

    Returns
    -------
    CompositeRule | None
        Parsed composite rule, or None if not present.

    Raises
    ------
    QueryParseError
        If the composite rule is invalid (e.g., ``not`` with multiple patterns).
    """
    for op in ("all", "any", "not"):
        value = tokens.get(op)
        if not value:
            continue

        # Parse comma-separated patterns (respecting quotes)
        patterns = _split_composite_patterns(value)

        if op == "not" and len(patterns) != 1:
            raise QueryParseError(_INVALID_COMPOSITE_MESSAGE)

        return CompositeRule(
            operator=op,  # type: ignore[arg-type]
            patterns=tuple(patterns),
        )

    return None


def _split_composite_patterns(value: str) -> list[str]:
    """Split composite pattern value, respecting brackets.

    Used by ``_parse_composite_rule`` to extract individual patterns.

    Parameters
    ----------
    value
        Value string, possibly containing brackets like [p1,p2]

    Returns
    -------
    list[str]
        Individual patterns.
    """
    # Handle bracketed list syntax: [p1, p2, p3]
    value = value.strip()
    if value.startswith("[") and value.endswith("]"):
        value = value[1:-1]

    # Split by comma, respecting nested quotes/parens
    patterns: list[str] = []
    current: list[str] = []
    depth = 0

    for char in value:
        if char in "([{":
            depth += 1
            current.append(char)
        elif char in ")]}":
            depth -= 1
            current.append(char)
        elif char == "," and depth == 0:
            pattern = "".join(current).strip().strip("'\"")
            if pattern:
                patterns.append(pattern)
            current = []
        else:
            current.append(char)

    # Don't forget the last pattern
    pattern = "".join(current).strip().strip("'\"")
    if pattern:
        patterns.append(pattern)

    return patterns


def _parse_nth_child(tokens: dict[str, str]) -> NthChildSpec | None:
    """Parse nthChild positional matching from tokens.

    Handles:
    - nthChild=3          (exact position)
    - nthChild='2n+1'     (formula)
    - nthChild.reverse=true
    - nthChild.ofRule='kind=identifier'

    Parameters
    ----------
    tokens
        Tokenized query string.

    Used by ``_parse_entity_query`` and ``_parse_pattern_query`` to parse nthChild specs.

    Returns
    -------
    NthChildSpec | None
        Parsed nthChild spec, or None if not present.
    """
    position_str = tokens.get("nthChild")
    if not position_str:
        return None

    # Try to parse as integer
    try:
        position: str | int = int(position_str)
    except ValueError:
        # Keep as formula string
        position = position_str

    reverse_str = tokens.get("nthChild.reverse", "false")
    reverse = reverse_str.lower() in {"true", "1", "yes"}

    of_rule = tokens.get("nthChild.ofRule")

    return NthChildSpec(
        position=position,
        reverse=reverse,
        of_rule=of_rule,
    )
