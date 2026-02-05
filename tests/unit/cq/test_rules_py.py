"""Tests for rules_py module with Python rule definitions."""

from __future__ import annotations

from tools.cq.astgrep.rules_py import (
    PY_CALL_NAME,
    PY_DEF_ASYNC_FUNCTION,
    PY_DEF_CLASS,
    PY_DEF_FUNCTION,
    PY_IMPORT,
    PYTHON_FACT_RULES,
    RULES_BY_RECORD_TYPE,
    get_all_rule_ids,
    get_rules_for_types,
)
from tools.cq.astgrep.sgpy_scanner import RecordType


class TestPythonFactRules:
    """Tests for Python fact rule definitions."""

    def test_all_rules_have_rule_id(self) -> None:
        """All rules should have a non-empty rule_id."""
        for rule in PYTHON_FACT_RULES:
            assert rule.rule_id, f"Rule missing rule_id: {rule}"
            assert rule.rule_id.startswith("py_"), (
                f"Rule ID should start with 'py_': {rule.rule_id}"
            )

    def test_all_rules_have_record_type(self) -> None:
        """All rules should have a valid record type."""
        valid_types: set[RecordType] = {"def", "call", "import", "raise", "except", "assign_ctor"}
        for rule in PYTHON_FACT_RULES:
            assert rule.record_type in valid_types, f"Invalid record type: {rule.record_type}"

    def test_all_rules_have_kind(self) -> None:
        """All rules should have a non-empty kind."""
        for rule in PYTHON_FACT_RULES:
            assert rule.kind, f"Rule missing kind: {rule.rule_id}"

    def test_rule_count_matches_yaml(self) -> None:
        """Should have same number of rules as YAML files (23)."""
        assert len(PYTHON_FACT_RULES) == 23

    def test_rule_ids_unique(self) -> None:
        """All rule IDs should be unique."""
        rule_ids = [rule.rule_id for rule in PYTHON_FACT_RULES]
        assert len(rule_ids) == len(set(rule_ids))


class TestGetRulesForTypes:
    """Tests for get_rules_for_types function."""

    def test_get_rules_for_types_def(self) -> None:
        """Should return def rules for 'def' type."""
        record_types: set[RecordType] = {"def"}
        rules = get_rules_for_types(record_types)
        assert len(rules) > 0
        for rule in rules:
            assert rule.record_type == "def"

    def test_get_rules_for_types_call(self) -> None:
        """Should return call rules for 'call' type."""
        record_types: set[RecordType] = {"call"}
        rules = get_rules_for_types(record_types)
        assert len(rules) == 2  # name_call and attr_call
        for rule in rules:
            assert rule.record_type == "call"

    def test_get_rules_for_types_import(self) -> None:
        """Should return import rules for 'import' type."""
        record_types: set[RecordType] = {"import"}
        rules = get_rules_for_types(record_types)
        assert len(rules) == 6  # Various import variants
        for rule in rules:
            assert rule.record_type == "import"

    def test_get_rules_for_types_raise(self) -> None:
        """Should return raise rules for 'raise' type."""
        record_types: set[RecordType] = {"raise"}
        rules = get_rules_for_types(record_types)
        assert len(rules) == 3  # raise, raise_from, raise_bare
        for rule in rules:
            assert rule.record_type == "raise"

    def test_get_rules_for_types_except(self) -> None:
        """Should return except rules for 'except' type."""
        record_types: set[RecordType] = {"except"}
        rules = get_rules_for_types(record_types)
        assert len(rules) == 3  # except, except_as, except_bare
        for rule in rules:
            assert rule.record_type == "except"

    def test_get_rules_for_types_none_returns_all(self) -> None:
        """None should return all rules."""
        rules = get_rules_for_types(None)
        assert len(rules) == len(PYTHON_FACT_RULES)

    def test_get_rules_for_multiple_types(self) -> None:
        """Should combine rules for multiple types."""
        record_types: set[RecordType] = {"def", "call"}
        rules = get_rules_for_types(record_types)
        def_types: set[RecordType] = {"def"}
        call_types: set[RecordType] = {"call"}
        def_rules = get_rules_for_types(def_types)
        call_rules = get_rules_for_types(call_types)
        assert len(rules) == len(def_rules) + len(call_rules)


class TestRulesByRecordType:
    """Tests for RULES_BY_RECORD_TYPE mapping."""

    def test_all_record_types_present(self) -> None:
        """All record types should be in the mapping."""
        expected_types: set[RecordType] = {
            "def",
            "call",
            "import",
            "raise",
            "except",
            "assign_ctor",
        }
        assert set(RULES_BY_RECORD_TYPE.keys()) == expected_types

    def test_def_rules_count(self) -> None:
        """Should have correct number of def rules."""
        assert len(RULES_BY_RECORD_TYPE["def"]) == 7

    def test_call_rules_count(self) -> None:
        """Should have correct number of call rules."""
        assert len(RULES_BY_RECORD_TYPE["call"]) == 2


class TestRuleConfigStructure:
    """Tests for rule config structure."""

    def test_function_rule_has_kind(self) -> None:
        """Function rule should have kind in config."""
        config = PY_DEF_FUNCTION.to_config()
        assert "kind" in config
        assert config["kind"] == "function_definition"

    def test_function_rule_has_regex(self) -> None:
        """Function rule should have regex in config."""
        config = PY_DEF_FUNCTION.to_config()
        assert "regex" in config
        assert config["regex"] == "^def "

    def test_async_function_rule_regex(self) -> None:
        """Async function rule should have async regex."""
        config = PY_DEF_ASYNC_FUNCTION.to_config()
        assert "regex" in config
        assert config["regex"] == "^async def "

    def test_class_rule_has_not_constraint(self) -> None:
        """Class rule should have not constraint."""
        config = PY_DEF_CLASS.to_config()
        assert "not" in config

    def test_call_rule_has_has_constraint(self) -> None:
        """Call rules should have 'has' constraint."""
        config = PY_CALL_NAME.to_config()
        assert "has" in config
        has_constraint = config["has"]
        assert isinstance(has_constraint, dict)
        assert has_constraint.get("kind") == "identifier"

    def test_import_rule_has_kind(self) -> None:
        """Import rule should have kind."""
        config = PY_IMPORT.to_config()
        assert "kind" in config
        assert config["kind"] == "import_statement"


class TestGetAllRuleIds:
    """Tests for get_all_rule_ids function."""

    def test_returns_frozenset(self) -> None:
        """Should return a frozenset."""
        rule_ids = get_all_rule_ids()
        assert isinstance(rule_ids, frozenset)

    def test_contains_all_rule_ids(self) -> None:
        """Should contain all rule IDs."""
        rule_ids = get_all_rule_ids()
        assert len(rule_ids) == len(PYTHON_FACT_RULES)
        assert "py_def_function" in rule_ids
        assert "py_call_name" in rule_ids
        assert "py_import" in rule_ids
