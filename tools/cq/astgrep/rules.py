"""Language-aware ast-grep rule dispatch."""

from __future__ import annotations

import threading

from tools.cq.astgrep.rulepack_registry import RulePackRegistry
from tools.cq.astgrep.sgpy_scanner import RecordType, RuleSpec
from tools.cq.core.types import QueryLanguage

_DEFAULT_RULEPACK_REGISTRY_LOCK = threading.Lock()


class _RulepackRegistryState:
    def __init__(self) -> None:
        self.registry = RulePackRegistry()


_RULEPACK_STATE = _RulepackRegistryState()


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
    active_registry = registry or get_default_rulepack_registry()
    packs = active_registry.load_default()
    selected = packs.get(lang, ())
    return _filter_rules_for_types(selected, record_types)


def set_rulepack_registry(registry: RulePackRegistry | None) -> None:
    """Set or clear the process-default rulepack registry."""
    with _DEFAULT_RULEPACK_REGISTRY_LOCK:
        _RULEPACK_STATE.registry = registry or RulePackRegistry()


def get_default_rulepack_registry() -> RulePackRegistry:
    """Return process-default rulepack registry."""
    return _RULEPACK_STATE.registry


__all__ = [
    "get_default_rulepack_registry",
    "get_rules_for_types",
    "set_rulepack_registry",
]
