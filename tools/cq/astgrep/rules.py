"""Language-aware ast-grep rule dispatch."""

from __future__ import annotations

from tools.cq.astgrep.rulepack_registry import RulePackRegistry
from tools.cq.astgrep.sgpy_scanner import RecordType, RuleSpec
from tools.cq.core.types import QueryLanguage

_RULEPACK_REGISTRY_STATE: dict[str, RulePackRegistry | None] = {"registry": None}


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
    registry: RulePackRegistry | None = None,
) -> tuple[RuleSpec, ...]:
    """Get ast-grep rules for record types in a specific language.

    Returns:
    -------
    tuple[RuleSpec, ...]
        Loaded rules filtered by requested record types.
    """
    active_registry = registry or _get_default_rulepack_registry()
    packs = active_registry.load_default()
    selected = packs.get(lang, ())
    return _filter_rules_for_types(selected, record_types)


def set_rulepack_registry(registry: RulePackRegistry | None) -> None:
    """Set or clear the process-default rulepack registry."""
    _RULEPACK_REGISTRY_STATE["registry"] = registry


def _get_default_rulepack_registry() -> RulePackRegistry:
    registry = _RULEPACK_REGISTRY_STATE["registry"]
    if registry is None:
        registry = RulePackRegistry()
        _RULEPACK_REGISTRY_STATE["registry"] = registry
    return registry


__all__ = ["get_rules_for_types", "set_rulepack_registry"]
