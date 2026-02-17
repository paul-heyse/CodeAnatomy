"""Tests for YAML-based ast-grep rulepack loading through registry."""

from __future__ import annotations

from tools.cq.astgrep.rulepack_registry import RulePackRegistry

PYTHON_RULE_COUNT = 23
RUST_RULE_COUNT = 8


def test_load_default_rulepacks_contains_python_and_rust() -> None:
    """Test load default rulepacks contains python and rust."""
    packs = RulePackRegistry().load_default()

    assert "python" in packs
    assert "rust" in packs
    assert len(packs["python"]) == PYTHON_RULE_COUNT
    assert len(packs["rust"]) == RUST_RULE_COUNT


def test_python_rule_ids_from_yaml_pack() -> None:
    """Test python rule ids from yaml pack."""
    python_rules = RulePackRegistry().load_default().get("python", ())
    rule_ids = {rule.rule_id for rule in python_rules}

    assert "py_def_function" in rule_ids
    assert "py_call_name" in rule_ids
    assert "py_import" in rule_ids


def test_rust_rule_ids_from_yaml_pack() -> None:
    """Test rust rule ids from yaml pack."""
    rust_rules = RulePackRegistry().load_default().get("rust", ())
    rule_ids = {rule.rule_id for rule in rust_rules}

    assert "rs_def_module" in rule_ids
    assert "rs_call_macro" in rule_ids
    assert "rs_use_declaration" in rule_ids
