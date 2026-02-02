"""E2E tests for security hazard detection features.

Tests the hazard detection system for finding security issues.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from tools.cq.query.hazards import (
    BUILTIN_HAZARDS,
    Hazard,
    HazardCategory,
    HazardDetector,
    HazardSeverity,
    HazardSpec,
    get_hazards_by_category,
    get_hazards_by_severity,
    get_security_hazards,
    summarize_hazards,
)


class TestHazardSpec:
    """Tests for HazardSpec dataclass."""

    def test_hazard_spec_basic(self) -> None:
        """HazardSpec can be created with basic fields."""
        spec = HazardSpec(
            id="test",
            pattern="eval($X)",
            message="Test hazard",
            category=HazardCategory.SECURITY,
            severity=HazardSeverity.ERROR,
        )
        assert spec.id == "test"
        assert spec.pattern == "eval($X)"
        assert spec.category == HazardCategory.SECURITY

    def test_hazard_spec_to_ast_grep_rule(self) -> None:
        """HazardSpec converts to ast-grep rule."""
        spec = HazardSpec(
            id="eval_check",
            pattern="eval($CODE)",
            message="eval is dangerous",
            category=HazardCategory.SECURITY,
            severity=HazardSeverity.ERROR,
        )
        rule = spec.to_ast_grep_rule()
        assert "rule" in rule
        assert rule["rule"]["pattern"] == "eval($CODE)"
        assert rule["severity"] == "error"

    def test_hazard_spec_with_inside(self) -> None:
        """HazardSpec with inside constraint."""
        spec = HazardSpec(
            id="sql_format",
            pattern="$X.format($$$)",
            message="SQL formatting",
            category=HazardCategory.SECURITY,
            severity=HazardSeverity.WARNING,
            inside="execute($$$)",
        )
        rule = spec.to_ast_grep_rule()
        assert "inside" in rule["rule"]

    def test_hazard_spec_with_not_inside(self) -> None:
        """HazardSpec with not_inside constraint."""
        spec = HazardSpec(
            id="safe_yaml",
            pattern="yaml.load($X)",
            message="yaml.load unsafe",
            category=HazardCategory.SECURITY,
            severity=HazardSeverity.ERROR,
            not_inside="yaml.load($X, Loader=$L)",
        )
        rule = spec.to_ast_grep_rule()
        assert "not" in rule["rule"]


class TestBuiltinHazards:
    """Tests for builtin hazard specifications."""

    def test_has_security_hazards(self) -> None:
        """Builtin hazards include security category."""
        security = get_security_hazards()
        assert len(security) >= 5

    def test_has_eval_exec_hazard(self) -> None:
        """Builtin hazards include eval/exec detection."""
        ids = {h.id for h in BUILTIN_HAZARDS}
        assert "eval_exec" in ids or "exec_call" in ids

    def test_has_pickle_hazard(self) -> None:
        """Builtin hazards include pickle detection."""
        ids = {h.id for h in BUILTIN_HAZARDS}
        assert "pickle_load" in ids

    def test_has_subprocess_shell_hazard(self) -> None:
        """Builtin hazards include subprocess shell detection."""
        ids = {h.id for h in BUILTIN_HAZARDS}
        assert "subprocess_shell" in ids

    def test_all_hazards_have_required_fields(self) -> None:
        """All builtin hazards have required fields."""
        for hazard in BUILTIN_HAZARDS:
            assert hazard.id
            assert hazard.pattern
            assert hazard.message
            assert isinstance(hazard.category, HazardCategory)
            assert isinstance(hazard.severity, HazardSeverity)

    def test_hazards_have_reasonable_confidence(self) -> None:
        """Hazard confidence penalties are in valid range."""
        for hazard in BUILTIN_HAZARDS:
            assert 0.0 <= hazard.confidence_penalty <= 1.0


class TestHazardDetector:
    """Tests for HazardDetector class."""

    def test_detector_initialization(self) -> None:
        """HazardDetector initializes with specs."""
        detector = HazardDetector()
        assert len(detector.specs) > 0

    def test_detector_get_spec(self) -> None:
        """HazardDetector can get spec by ID."""
        detector = HazardDetector()
        spec = detector.get_spec("eval_exec")
        assert spec is not None
        assert spec.id == "eval_exec"

    def test_detector_get_by_category(self) -> None:
        """HazardDetector can filter by category."""
        detector = HazardDetector()
        security = detector.get_by_category(HazardCategory.SECURITY)
        assert len(security) >= 1
        assert all(h.category == HazardCategory.SECURITY for h in security)

    def test_detector_ast_grep_rules(self) -> None:
        """HazardDetector generates ast-grep rules."""
        detector = HazardDetector()
        rules = detector.get_ast_grep_rules()
        assert len(rules) > 0
        assert all("rule" in r for r in rules)


class TestHazardFiltering:
    """Tests for hazard filtering functions."""

    def test_get_hazards_by_category(self) -> None:
        """Filter hazards by category."""
        security = get_hazards_by_category(HazardCategory.SECURITY)
        correctness = get_hazards_by_category(HazardCategory.CORRECTNESS)
        design = get_hazards_by_category(HazardCategory.DESIGN)

        assert all(h.category == HazardCategory.SECURITY for h in security)
        assert all(h.category == HazardCategory.CORRECTNESS for h in correctness)
        assert all(h.category == HazardCategory.DESIGN for h in design)

    def test_get_hazards_by_severity(self) -> None:
        """Filter hazards by severity."""
        errors = get_hazards_by_severity(HazardSeverity.ERROR)
        warnings = get_hazards_by_severity(HazardSeverity.WARNING)

        assert all(h.severity == HazardSeverity.ERROR for h in errors)
        assert all(h.severity == HazardSeverity.WARNING for h in warnings)


class TestHazardSummarization:
    """Tests for hazard summarization."""

    def test_summarize_empty(self) -> None:
        """Summarize empty hazard list."""
        summary = summarize_hazards([])
        assert summary["total"] == 0
        assert summary["aggregate_confidence"] == 1.0

    def test_summarize_single_hazard(self) -> None:
        """Summarize single hazard."""
        from tools.cq.core.schema import Anchor

        hazards = [
            Hazard(
                kind="eval_exec",
                location=Anchor(file="test.py", line=1, col=0),
                reason="eval usage",
                confidence=0.2,
            )
        ]
        summary = summarize_hazards(hazards)
        assert summary["total"] == 1
        assert summary["by_kind"]["eval_exec"] == 1
        assert summary["aggregate_confidence"] == 0.2

    def test_summarize_multiple_hazards(self) -> None:
        """Summarize multiple hazards."""
        from tools.cq.core.schema import Anchor

        hazards = [
            Hazard(
                kind="eval_exec",
                location=Anchor(file="test.py", line=1, col=0),
                reason="eval usage",
                confidence=0.2,
            ),
            Hazard(
                kind="getattr",
                location=Anchor(file="test.py", line=5, col=0),
                reason="getattr usage",
                confidence=0.5,
            ),
        ]
        summary = summarize_hazards(hazards)
        assert summary["total"] == 2
        assert summary["aggregate_confidence"] == 0.2  # Min of 0.2 and 0.5


class TestSecurityHazardPatterns:
    """Tests for specific security hazard patterns."""

    def test_eval_hazard_pattern(self) -> None:
        """Eval hazard has correct pattern."""
        detector = HazardDetector()
        eval_spec = detector.get_spec("eval_exec")
        assert eval_spec is not None
        assert "eval" in eval_spec.pattern

    def test_pickle_hazard_pattern(self) -> None:
        """Pickle hazard has correct pattern."""
        detector = HazardDetector()
        pickle_spec = detector.get_spec("pickle_load")
        assert pickle_spec is not None
        assert "pickle" in pickle_spec.pattern.lower()

    def test_subprocess_hazard_pattern(self) -> None:
        """Subprocess hazard has shell=True pattern."""
        detector = HazardDetector()
        shell_spec = detector.get_spec("subprocess_shell")
        assert shell_spec is not None
        assert "shell=True" in shell_spec.pattern


class TestHazardWithFixtures:
    """Tests using fixture files."""

    @pytest.fixture
    def fixtures_dir(self) -> Path:
        """Get fixtures directory.

        Returns
        -------
        Path
            Path to the fixture directory.
        """
        return Path(__file__).parent / "_fixtures"

    def test_detect_dynamic_dispatch_fixture(self, fixtures_dir: Path) -> None:
        """Fixture file has dynamic dispatch patterns."""
        dispatch_path = fixtures_dir / "dynamic_dispatch.py"
        if not dispatch_path.exists():
            pytest.skip("dynamic_dispatch.py fixture not found")

        source = dispatch_path.read_text()

        # Should contain hazardous patterns
        assert "getattr" in source or "eval" in source or "exec" in source
