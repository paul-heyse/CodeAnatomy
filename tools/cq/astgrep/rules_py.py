"""Python-native rule definitions for ast-grep-py.

Converts all YAML rules from tools/cq/astgrep/rules/python_facts/ to Python.
"""

from __future__ import annotations

from tools.cq.astgrep.sgpy_scanner import RecordType, RuleSpec

# =============================================================================
# Function Definitions
# =============================================================================

PY_DEF_FUNCTION = RuleSpec(
    rule_id="py_def_function",
    record_type="def",
    kind="function",
    config={
        "rule": {
            "kind": "function_definition",
            "regex": "^def ",
            "not": {"has": {"kind": "type_parameter"}},
        }
    },
)

PY_DEF_ASYNC_FUNCTION = RuleSpec(
    rule_id="py_def_async_function",
    record_type="def",
    kind="async_function",
    config={
        "rule": {
            "kind": "function_definition",
            "regex": "^async def ",
        }
    },
)

PY_DEF_FUNCTION_TYPEPARAMS = RuleSpec(
    rule_id="py_def_function_typeparams",
    record_type="def",
    kind="function_typeparams",
    config={
        "rule": {
            "kind": "function_definition",
            "has": {"kind": "type_parameter"},
        }
    },
)

# =============================================================================
# Class Definitions
# =============================================================================

PY_DEF_CLASS = RuleSpec(
    rule_id="py_def_class",
    record_type="def",
    kind="class",
    config={
        "rule": {
            "kind": "class_definition",
            "not": {
                "any": [
                    {"has": {"kind": "argument_list"}},
                    {"has": {"kind": "type_parameter"}},
                ]
            },
        }
    },
)

PY_DEF_CLASS_BASES = RuleSpec(
    rule_id="py_def_class_bases",
    record_type="def",
    kind="class_bases",
    config={
        "rule": {
            "kind": "class_definition",
            "has": {"kind": "argument_list"},
            "not": {"has": {"kind": "type_parameter"}},
        }
    },
)

PY_DEF_CLASS_TYPEPARAMS = RuleSpec(
    rule_id="py_def_class_typeparams",
    record_type="def",
    kind="class_typeparams",
    config={
        "rule": {
            "kind": "class_definition",
            "has": {"kind": "type_parameter"},
            "not": {"has": {"kind": "argument_list"}},
        }
    },
)

PY_DEF_CLASS_TYPEPARAMS_BASES = RuleSpec(
    rule_id="py_def_class_typeparams_bases",
    record_type="def",
    kind="class_typeparams_bases",
    config={
        "rule": {
            "kind": "class_definition",
            "all": [
                {"has": {"kind": "type_parameter"}},
                {"has": {"kind": "argument_list"}},
            ],
        }
    },
)

# =============================================================================
# Call Expressions
# =============================================================================

PY_CALL_NAME = RuleSpec(
    rule_id="py_call_name",
    record_type="call",
    kind="name_call",
    config={
        "rule": {
            "kind": "call",
            "has": {"kind": "identifier", "field": "function"},
        }
    },
)

PY_CALL_ATTR = RuleSpec(
    rule_id="py_call_attr",
    record_type="call",
    kind="attr_call",
    config={
        "rule": {
            "kind": "call",
            "has": {"kind": "attribute", "field": "function"},
        }
    },
)

# =============================================================================
# Import Statements
# =============================================================================

PY_IMPORT = RuleSpec(
    rule_id="py_import",
    record_type="import",
    kind="import",
    config={
        "rule": {
            "kind": "import_statement",
            "not": {"has": {"kind": "aliased_import"}},
        }
    },
)

PY_IMPORT_AS = RuleSpec(
    rule_id="py_import_as",
    record_type="import",
    kind="import_as",
    config={
        "rule": {
            "kind": "import_statement",
            "has": {"kind": "aliased_import"},
        }
    },
)

PY_FROM_IMPORT = RuleSpec(
    rule_id="py_from_import",
    record_type="import",
    kind="from_import",
    config={
        "rule": {
            "kind": "import_from_statement",
            # Match single-name from-imports only. Multi-imports (comma) and
            # parenthesized imports are handled by dedicated rules to avoid
            # duplicate findings for the same statement.
            "not": {
                "any": [
                    {"has": {"kind": "aliased_import"}},
                    # Multi-import (comma-separated) or parenthesized import list.
                    {"regex": r"import\s+[^\n#]*,"},
                    {"regex": r"import\s*\("},
                ]
            },
            "has": {"kind": "dotted_name", "field": "name"},
        }
    },
)

PY_FROM_IMPORT_AS = RuleSpec(
    rule_id="py_from_import_as",
    record_type="import",
    kind="from_import_as",
    config={
        "rule": {
            "kind": "import_from_statement",
            "has": {"kind": "aliased_import"},
        }
    },
)

PY_FROM_IMPORT_MULTI = RuleSpec(
    rule_id="py_from_import_multi",
    record_type="import",
    kind="from_import_multi",
    config={
        "rule": {
            "kind": "import_from_statement",
            # Any comma-separated import list (non-parenthesized, non-aliased).
            "regex": r"import\s+[^\n#]*,",
            "not": {
                "any": [
                    {"regex": r"import\s*\("},
                    {"has": {"kind": "aliased_import"}},
                ]
            },
        }
    },
)

PY_FROM_IMPORT_PAREN = RuleSpec(
    rule_id="py_from_import_paren",
    record_type="import",
    kind="from_import_paren",
    config={
        "rule": {
            "kind": "import_from_statement",
            "regex": r"import\s*\(",
            "not": {"has": {"kind": "aliased_import"}},
        }
    },
)

# =============================================================================
# Raise Statements
# =============================================================================

PY_RAISE = RuleSpec(
    rule_id="py_raise",
    record_type="raise",
    kind="raise",
    config={
        "rule": {
            "kind": "raise_statement",
            "regex": r"^raise [^f]",
            "not": {"regex": " from "},
        }
    },
)

PY_RAISE_FROM = RuleSpec(
    rule_id="py_raise_from",
    record_type="raise",
    kind="raise_from",
    config={
        "rule": {
            "kind": "raise_statement",
            "regex": " from ",
        }
    },
)

PY_RAISE_BARE = RuleSpec(
    rule_id="py_raise_bare",
    record_type="raise",
    kind="raise_bare",
    config={
        "rule": {
            "kind": "raise_statement",
            "regex": r"^raise$",
        }
    },
)

# =============================================================================
# Except Clauses
# =============================================================================

PY_EXCEPT = RuleSpec(
    rule_id="py_except",
    record_type="except",
    kind="except",
    config={
        "rule": {
            "kind": "except_clause",
            "regex": r"^except [A-Za-z]",
            "not": {"regex": " as "},
        }
    },
)

PY_EXCEPT_AS = RuleSpec(
    rule_id="py_except_as",
    record_type="except",
    kind="except_as",
    config={
        "rule": {
            "kind": "except_clause",
            "regex": " as ",
        }
    },
)

PY_EXCEPT_BARE = RuleSpec(
    rule_id="py_except_bare",
    record_type="except",
    kind="except_bare",
    config={
        "rule": {
            "kind": "except_clause",
            "regex": r"^except:",
        }
    },
)

# =============================================================================
# Constructor Assignments
# =============================================================================

PY_CTOR_ASSIGN_NAME = RuleSpec(
    rule_id="py_ctor_assign_name",
    record_type="assign_ctor",
    kind="ctor_assign_name",
    config={
        "rule": {
            "kind": "assignment",
            "has": {
                "kind": "call",
                "has": {"kind": "identifier", "field": "function", "regex": "^[A-Z]"},
            },
        }
    },
)

PY_CTOR_ASSIGN_ATTR = RuleSpec(
    rule_id="py_ctor_assign_attr",
    record_type="assign_ctor",
    kind="ctor_assign_attr",
    config={
        "rule": {
            "kind": "assignment",
            "has": {
                "kind": "call",
                "has": {
                    "kind": "attribute",
                    "field": "function",
                    "has": {
                        "kind": "identifier",
                        "field": "attribute",
                        "regex": "^[A-Z]",
                    },
                },
            },
        }
    },
)

# =============================================================================
# Rule Collections
# =============================================================================

# All Python fact rules in a tuple
PYTHON_FACT_RULES: tuple[RuleSpec, ...] = (
    # Function definitions
    PY_DEF_FUNCTION,
    PY_DEF_ASYNC_FUNCTION,
    PY_DEF_FUNCTION_TYPEPARAMS,
    # Class definitions
    PY_DEF_CLASS,
    PY_DEF_CLASS_BASES,
    PY_DEF_CLASS_TYPEPARAMS,
    PY_DEF_CLASS_TYPEPARAMS_BASES,
    # Calls
    PY_CALL_NAME,
    PY_CALL_ATTR,
    # Imports
    PY_IMPORT,
    PY_IMPORT_AS,
    PY_FROM_IMPORT,
    PY_FROM_IMPORT_AS,
    PY_FROM_IMPORT_MULTI,
    PY_FROM_IMPORT_PAREN,
    # Raises
    PY_RAISE,
    PY_RAISE_FROM,
    PY_RAISE_BARE,
    # Excepts
    PY_EXCEPT,
    PY_EXCEPT_AS,
    PY_EXCEPT_BARE,
    # Constructor assignments
    PY_CTOR_ASSIGN_NAME,
    PY_CTOR_ASSIGN_ATTR,
)

# Rules grouped by record type for efficient filtering
RULES_BY_RECORD_TYPE: dict[RecordType, tuple[RuleSpec, ...]] = {
    "def": (
        PY_DEF_FUNCTION,
        PY_DEF_ASYNC_FUNCTION,
        PY_DEF_FUNCTION_TYPEPARAMS,
        PY_DEF_CLASS,
        PY_DEF_CLASS_BASES,
        PY_DEF_CLASS_TYPEPARAMS,
        PY_DEF_CLASS_TYPEPARAMS_BASES,
    ),
    "call": (
        PY_CALL_NAME,
        PY_CALL_ATTR,
    ),
    "import": (
        PY_IMPORT,
        PY_IMPORT_AS,
        PY_FROM_IMPORT,
        PY_FROM_IMPORT_AS,
        PY_FROM_IMPORT_MULTI,
        PY_FROM_IMPORT_PAREN,
    ),
    "raise": (
        PY_RAISE,
        PY_RAISE_FROM,
        PY_RAISE_BARE,
    ),
    "except": (
        PY_EXCEPT,
        PY_EXCEPT_AS,
        PY_EXCEPT_BARE,
    ),
    "assign_ctor": (
        PY_CTOR_ASSIGN_NAME,
        PY_CTOR_ASSIGN_ATTR,
    ),
}


def get_rules_for_types(record_types: set[RecordType] | None) -> tuple[RuleSpec, ...]:
    """Get rules for the specified record types.

    Parameters
    ----------
    record_types
        Set of record types to get rules for. If None, returns all rules.

    Returns:
    -------
    tuple[RuleSpec, ...]
        Rules matching the requested record types.
    """
    if record_types is None:
        return PYTHON_FACT_RULES

    rules: list[RuleSpec] = []
    for record_type in record_types:
        if record_type in RULES_BY_RECORD_TYPE:
            rules.extend(RULES_BY_RECORD_TYPE[record_type])

    return tuple(rules)


def get_all_rule_ids() -> frozenset[str]:
    """Get all rule IDs.

    Returns:
    -------
    frozenset[str]
        Set of all rule IDs.
    """
    return frozenset(rule.rule_id for rule in PYTHON_FACT_RULES)
