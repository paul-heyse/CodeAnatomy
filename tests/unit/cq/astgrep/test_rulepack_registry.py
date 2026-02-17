"""Tests for injectable ast-grep rulepack registry."""

from __future__ import annotations

from pathlib import Path

from tools.cq.astgrep.rulepack_registry import RulePackRegistry
from tools.cq.astgrep.rules import (
    get_default_rulepack_registry,
    get_rules_for_types,
    set_rulepack_registry,
)
from tools.cq.astgrep.sgpy_scanner import RuleSpec


def test_load_default_caches_per_registry_instance() -> None:
    """Verify default rule packs are cached and returned as copies."""
    registry = RulePackRegistry()
    first = registry.load_default()
    second = registry.load_default()

    assert "python" in first
    assert "rust" in first
    assert first == second
    assert first is not second


def test_reset_clears_cached_rulepacks() -> None:
    """Verify registry reset clears cache and rehydrates data."""
    registry = RulePackRegistry()
    original = registry.load_default()

    registry.reset()
    refreshed = registry.load_default()

    assert original == refreshed
    assert original is not refreshed


class _FakeRegistry(RulePackRegistry):
    def load_default(self, *, base: Path | None = None) -> dict[str, tuple[RuleSpec, ...]]:
        _ = (self, base)
        return {
            "python": (
                RuleSpec(
                    rule_id="test_rule",
                    record_type="def",
                    kind="function",
                    config={"rule": {"kind": "function_definition"}},
                ),
            )
        }


def test_get_rules_uses_injected_registry() -> None:
    """Verify lookup uses an injected process-default registry instance."""
    set_rulepack_registry(_FakeRegistry())
    try:
        rules = get_rules_for_types({"def"}, lang="python")
    finally:
        set_rulepack_registry(None)
    assert [rule.rule_id for rule in rules] == ["test_rule"]


def test_set_rulepack_registry_updates_default_registry() -> None:
    """Setter should replace the process-default registry instance."""
    fake = _FakeRegistry()
    try:
        set_rulepack_registry(fake)
        assert get_default_rulepack_registry() is fake
    finally:
        set_rulepack_registry(None)
