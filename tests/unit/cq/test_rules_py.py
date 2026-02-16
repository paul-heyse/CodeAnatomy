"""Tests for Python ast-grep rule packs via YAML loader."""

from __future__ import annotations

from tools.cq.astgrep.rulepack_loader import clear_rulepack_cache, load_default_rulepacks
from tools.cq.astgrep.rules import get_rules_for_types
from tools.cq.astgrep.sgpy_scanner import RecordType, RuleSpec

PYTHON_RULEPACK_COUNT = 23
DEF_RULE_COUNT = 7
CALL_RULE_COUNT = 2
IMPORT_RULE_COUNT = 6
RAISE_RULE_COUNT = 3
EXCEPT_RULE_COUNT = 3


def _python_rules() -> tuple[RuleSpec, ...]:
    clear_rulepack_cache()
    packs = load_default_rulepacks()
    return packs.get("python", ())


def test_python_rulepack_count_matches_yaml() -> None:
    """Test python rulepack count matches yaml."""
    rules = _python_rules()
    assert len(rules) == PYTHON_RULEPACK_COUNT


def test_python_rule_ids_are_unique_and_prefixed() -> None:
    """Test python rule ids are unique and prefixed."""
    rules = _python_rules()
    rule_ids = [rule.rule_id for rule in rules]
    assert len(rule_ids) == len(set(rule_ids))
    assert all(rule_id.startswith("py_") for rule_id in rule_ids)


def test_get_rules_for_types_uses_loader_dispatch() -> None:
    """Verify loader dispatch applies language and record-type filters correctly."""
    def_types: set[RecordType] = {"def"}
    call_types: set[RecordType] = {"call"}
    import_types: set[RecordType] = {"import"}
    raise_types: set[RecordType] = {"raise"}
    except_types: set[RecordType] = {"except"}

    def_rules = get_rules_for_types(def_types, lang="python")
    call_rules = get_rules_for_types(call_types, lang="python")
    import_rules = get_rules_for_types(import_types, lang="python")
    raise_rules = get_rules_for_types(raise_types, lang="python")
    except_rules = get_rules_for_types(except_types, lang="python")

    assert len(def_rules) == DEF_RULE_COUNT
    assert len(call_rules) == CALL_RULE_COUNT
    assert len(import_rules) == IMPORT_RULE_COUNT
    assert len(raise_rules) == RAISE_RULE_COUNT
    assert len(except_rules) == EXCEPT_RULE_COUNT


def test_python_rule_config_shape_from_loader() -> None:
    """Test python rule config shape from loader."""
    rules_by_id = {rule.rule_id: rule for rule in _python_rules()}

    def_rule = rules_by_id["py_def_function"]
    assert def_rule.record_type == "def"
    assert def_rule.kind == "function"
    config = def_rule.to_config()
    assert config.get("kind") == "function_definition"
    assert config.get("regex") == "^def "

    call_rule = rules_by_id["py_call_name"]
    call_config = call_rule.to_config()
    has_constraint = call_config.get("has")
    assert isinstance(has_constraint, dict)
    assert has_constraint.get("kind") == "identifier"
