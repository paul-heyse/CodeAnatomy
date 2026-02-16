"""Tests for YAML-based ast-grep rulepack loading."""

from __future__ import annotations

from tools.cq.astgrep.rulepack_loader import clear_rulepack_cache, load_default_rulepacks


def test_load_default_rulepacks_contains_python_and_rust() -> None:
    clear_rulepack_cache()
    packs = load_default_rulepacks()

    assert "python" in packs
    assert "rust" in packs
    assert len(packs["python"]) == 23
    assert len(packs["rust"]) == 8


def test_python_rule_ids_from_yaml_pack() -> None:
    clear_rulepack_cache()
    python_rules = load_default_rulepacks().get("python", ())
    rule_ids = {rule.rule_id for rule in python_rules}

    assert "py_def_function" in rule_ids
    assert "py_call_name" in rule_ids
    assert "py_import" in rule_ids


def test_rust_rule_ids_from_yaml_pack() -> None:
    clear_rulepack_cache()
    rust_rules = load_default_rulepacks().get("rust", ())
    rule_ids = {rule.rule_id for rule in rust_rules}

    assert "rs_def_module" in rule_ids
    assert "rs_call_macro" in rule_ids
    assert "rs_use_declaration" in rule_ids
