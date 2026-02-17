"""Language-aware ast-grep rule dispatch."""

from __future__ import annotations

from tools.cq.astgrep.rulepack_loader import load_default_rulepacks
from tools.cq.astgrep.sgpy_scanner import RecordType, RuleSpec
from tools.cq.core.types import QueryLanguage


def _filter_rules_for_types(
    rules: tuple[RuleSpec, ...],
    record_types: set[RecordType] | None,
) -> tuple[RuleSpec, ...]:
    if record_types is None:
        return rules
    allowed = set(record_types)
    return tuple(rule for rule in rules if rule.record_type in allowed)


def get_rules_for_types(
    record_types: set[RecordType] | None,
    *,
    lang: QueryLanguage,
) -> tuple[RuleSpec, ...]:
    """Get ast-grep rules for record types in a specific language.

    Returns:
    -------
    tuple[RuleSpec, ...]
        Loaded rules filtered by requested record types.
    """
    packs = load_default_rulepacks()
    selected = packs.get(lang, ())
    return _filter_rules_for_types(selected, record_types)


__all__ = ["get_rules_for_types"]
