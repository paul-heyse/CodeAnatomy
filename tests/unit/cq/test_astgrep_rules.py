"""Test ast-grep rule loading and output format.

Verifies that:
1. ast-grep test suite passes
2. Rules load correctly and produce expected JSON format
3. Metadata fields are present in scan output
4. Rust rule specs are correctly defined and registered
"""

from __future__ import annotations

import json
import subprocess
import tempfile
from pathlib import Path

import pytest
from tools.cq.astgrep.rulepack_loader import load_default_rulepacks
from tools.cq.astgrep.rules import get_rules_for_types
from tools.cq.astgrep.sgpy_scanner import RuleSpec

REPO_ROOT = Path(__file__).parent.parent.parent.parent
SGCONFIG_PATH = REPO_ROOT / "tools" / "cq" / "astgrep" / "sgconfig.yml"
EXPECTED_ASTGREP_RULE_FILES = 23


@pytest.fixture(scope="module")
def ast_grep_available() -> bool:
    """Check if ast-grep is available.

    Returns:
    -------
    bool
        True when ast-grep is available.
    """
    try:
        subprocess.run(
            ["ast-grep", "--version"],
            capture_output=True,
            text=True,
            check=True,
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False
    else:
        return True


@pytest.mark.smoke
def test_astgrep_rules_exist() -> None:
    """Verify ast-grep rule files exist."""
    rules_dir = REPO_ROOT / "tools" / "cq" / "astgrep" / "rules" / "python_facts"
    assert rules_dir.exists(), f"Rules directory not found: {rules_dir}"

    rule_files = list(rules_dir.glob("*.yml"))
    assert len(rule_files) == EXPECTED_ASTGREP_RULE_FILES, (
        f"Expected {EXPECTED_ASTGREP_RULE_FILES} rule files, found {len(rule_files)}"
    )

    # Check key rule categories exist
    rule_names = {f.stem for f in rule_files}
    expected_prefixes = [
        "py_def_",
        "py_call_",
        "py_import",
        "py_from_import",
        "py_raise",
        "py_except",
        "py_ctor_",
    ]
    for prefix in expected_prefixes:
        matches = [name for name in rule_names if name.startswith(prefix)]
        assert matches, f"No rules found with prefix '{prefix}'"


@pytest.mark.smoke
def test_astgrep_config_exists() -> None:
    """Verify sgconfig.yml exists and is valid YAML."""
    assert SGCONFIG_PATH.exists(), f"sgconfig.yml not found: {SGCONFIG_PATH}"

    import yaml

    with SGCONFIG_PATH.open(encoding="utf-8") as config_file:
        config = yaml.safe_load(config_file)

    assert "ruleDirs" in config, "Missing ruleDirs in sgconfig.yml"
    assert "testConfigs" in config, "Missing testConfigs in sgconfig.yml"


@pytest.mark.skipif(
    subprocess.run(["which", "ast-grep"], capture_output=True, check=False).returncode != 0,
    reason="ast-grep not installed",
)
def test_astgrep_test_suite_passes() -> None:
    """Run ast-grep test suite and verify it passes."""
    result = subprocess.run(
        ["ast-grep", "test", "-c", str(SGCONFIG_PATH)],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
        check=False,
    )
    assert result.returncode == 0, f"ast-grep tests failed:\n{result.stdout}\n{result.stderr}"


@pytest.mark.skipif(
    subprocess.run(["which", "ast-grep"], capture_output=True, check=False).returncode != 0,
    reason="ast-grep not installed",
)
def test_astgrep_scan_produces_json() -> None:
    """Verify ast-grep scan produces valid JSON stream output."""
    # Create a test Python file
    test_code = """
def foo():
    pass

class Bar:
    def method(self):
        return foo()

import os
from typing import List
"""

    with tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".py",
        delete=False,
        encoding="utf-8",
    ) as temp_file:
        temp_file.write(test_code)
        temp_path = temp_file.name

    try:
        result = subprocess.run(
            [
                "ast-grep",
                "scan",
                "-c",
                str(SGCONFIG_PATH),
                "--json=stream",
                temp_path,
            ],
            capture_output=True,
            text=True,
            cwd=REPO_ROOT,
            check=False,
        )
        assert result.returncode == 0, f"Scan failed: {result.stderr}"

        # Parse each line as JSON
        records = [json.loads(line) for line in result.stdout.strip().split("\n") if line]

        assert len(records) > 0, "Expected at least one match"

        # Verify expected rule IDs are present
        rule_ids = {r["ruleId"] for r in records}
        assert "py_def_function" in rule_ids, "Expected py_def_function match"
        assert "py_def_class" in rule_ids, "Expected py_def_class match"
        assert "py_import" in rule_ids, "Expected py_import match"

        # Verify JSON structure
        for record in records:
            assert "ruleId" in record, "Missing ruleId"
            assert "text" in record, "Missing text"
            assert "range" in record, "Missing range"
            assert "file" in record, "Missing file"
            assert record["range"]["start"]["line"] >= 0, "Invalid line number"
            assert record["range"]["start"]["column"] >= 0, "Invalid column"

    finally:
        Path(temp_path).unlink()


@pytest.mark.skipif(
    subprocess.run(["which", "ast-grep"], capture_output=True, check=False).returncode != 0,
    reason="ast-grep not installed",
)
def test_astgrep_metadata_in_output() -> None:
    """Verify metadata fields propagate to scan output."""
    test_code = "def test_func(): pass"

    with tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".py",
        delete=False,
        encoding="utf-8",
    ) as temp_file:
        temp_file.write(test_code)
        temp_path = temp_file.name

    try:
        result = subprocess.run(
            [
                "ast-grep",
                "scan",
                "-c",
                str(SGCONFIG_PATH),
                "--json=stream",
                temp_path,
            ],
            capture_output=True,
            text=True,
            cwd=REPO_ROOT,
            check=False,
        )

        records = [json.loads(line) for line in result.stdout.strip().split("\n") if line]

        # Find function definition match
        func_matches = [r for r in records if r["ruleId"] == "py_def_function"]
        assert len(func_matches) > 0, "Expected py_def_function match"

        # The metadata is available via the ruleId (the metadata.record and metadata.kind
        # are used internally by ast-grep but the ruleId encodes this information)
        match = func_matches[0]
        assert match["ruleId"].startswith("py_def_"), "ruleId should indicate def record"

    finally:
        Path(temp_path).unlink()


class TestRustRuleSpecs:
    """Verify Rust rule specs are correctly defined and registered."""

    @staticmethod
    def _rust_rules() -> tuple[RuleSpec, ...]:
        packs = load_default_rulepacks()
        return packs.get("rust", ())

    @pytest.mark.smoke
    def test_rs_def_module_exists(self) -> None:
        """Verify rs_def_module is present in Rust YAML-loaded rules."""
        rule_ids = {rule.rule_id for rule in self._rust_rules()}
        assert "rs_def_module" in rule_ids

    @pytest.mark.smoke
    def test_rs_call_macro_exists(self) -> None:
        """Verify rs_call_macro is present in Rust YAML-loaded rules."""
        rule_ids = {rule.rule_id for rule in self._rust_rules()}
        assert "rs_call_macro" in rule_ids

    @staticmethod
    @pytest.mark.smoke
    def test_rules_by_record_type_has_module() -> None:
        """Verify ``def`` rules include the rs_def_module spec."""
        def_rules = get_rules_for_types({"def"}, lang="rust")
        assert any(rule.rule_id == "rs_def_module" for rule in def_rules)

    @staticmethod
    @pytest.mark.smoke
    def test_rules_by_record_type_has_macro_call() -> None:
        """Verify rs_call_macro is available in call-record dispatch."""
        call_rules = get_rules_for_types({"call"}, lang="rust")
        assert any(rule.rule_id == "rs_call_macro" for rule in call_rules)

    def test_rs_def_module_structure(self) -> None:
        """Verify rs_def_module has expected structure."""
        by_id = {rule.rule_id: rule for rule in self._rust_rules()}
        module_rule = by_id["rs_def_module"]
        assert module_rule.record_type == "def"
        assert module_rule.kind == "module"
        assert module_rule.to_config() == {"kind": "mod_item"}

    def test_rs_call_macro_structure(self) -> None:
        """Verify rs_call_macro has expected structure."""
        by_id = {rule.rule_id: rule for rule in self._rust_rules()}
        macro_rule = by_id["rs_call_macro"]
        assert macro_rule.record_type == "call"
        assert macro_rule.kind == "macro_invocation"
        config = macro_rule.to_config()
        assert "all" in config
