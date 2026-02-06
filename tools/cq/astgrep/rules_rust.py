"""Rust-native rule definitions for ast-grep-py."""

from __future__ import annotations

from tools.cq.astgrep.sgpy_scanner import RecordType, RuleSpec

# =============================================================================
# Definitions
# =============================================================================

RS_DEF_FUNCTION = RuleSpec(
    rule_id="rs_def_function",
    record_type="def",
    kind="function",
    config={"rule": {"kind": "function_item"}},
)

RS_DEF_STRUCT = RuleSpec(
    rule_id="rs_def_struct",
    record_type="def",
    kind="struct",
    config={"rule": {"kind": "struct_item"}},
)

RS_DEF_ENUM = RuleSpec(
    rule_id="rs_def_enum",
    record_type="def",
    kind="enum",
    config={"rule": {"kind": "enum_item"}},
)

RS_DEF_TRAIT = RuleSpec(
    rule_id="rs_def_trait",
    record_type="def",
    kind="trait",
    config={"rule": {"kind": "trait_item"}},
)

# =============================================================================
# Calls
# =============================================================================

RS_CALL_EXPRESSION = RuleSpec(
    rule_id="rs_call_expression",
    record_type="call",
    kind="call_expression",
    config={"rule": {"kind": "call_expression"}},
)

# =============================================================================
# Imports
# =============================================================================

RS_USE_DECLARATION = RuleSpec(
    rule_id="rs_use_declaration",
    record_type="import",
    kind="use_declaration",
    config={"rule": {"kind": "use_declaration"}},
)

# =============================================================================
# Rule Collections
# =============================================================================

RUST_FACT_RULES: tuple[RuleSpec, ...] = (
    RS_DEF_FUNCTION,
    RS_DEF_STRUCT,
    RS_DEF_ENUM,
    RS_DEF_TRAIT,
    RS_CALL_EXPRESSION,
    RS_USE_DECLARATION,
)

RULES_BY_RECORD_TYPE: dict[RecordType, tuple[RuleSpec, ...]] = {
    "def": (
        RS_DEF_FUNCTION,
        RS_DEF_STRUCT,
        RS_DEF_ENUM,
        RS_DEF_TRAIT,
    ),
    "call": (RS_CALL_EXPRESSION,),
    "import": (RS_USE_DECLARATION,),
    "raise": (),
    "except": (),
    "assign_ctor": (),
}


def get_rules_for_types(record_types: set[RecordType] | None) -> tuple[RuleSpec, ...]:
    """Get Rust rules for specific record types.

    Returns:
    -------
    tuple[RuleSpec, ...]
        Rust rules filtered to the requested record types.
    """
    if record_types is None:
        return RUST_FACT_RULES

    rules: list[RuleSpec] = []
    for record_type in record_types:
        rules.extend(RULES_BY_RECORD_TYPE.get(record_type, ()))
    return tuple(rules)


def get_all_rule_ids() -> frozenset[str]:
    """Get all Rust rule IDs.

    Returns:
    -------
    frozenset[str]
        All known Rust rule identifiers.
    """
    return frozenset(rule.rule_id for rule in RUST_FACT_RULES)


__all__ = [
    "RS_CALL_EXPRESSION",
    "RS_DEF_ENUM",
    "RS_DEF_FUNCTION",
    "RS_DEF_STRUCT",
    "RS_DEF_TRAIT",
    "RS_USE_DECLARATION",
    "RULES_BY_RECORD_TYPE",
    "RUST_FACT_RULES",
    "get_all_rule_ids",
    "get_rules_for_types",
]
