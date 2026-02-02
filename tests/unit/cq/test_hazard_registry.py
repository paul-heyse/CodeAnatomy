"""Tests for hazard registry and detection.

Verifies:
1. HazardSpec construction
2. Builtin hazards registry
3. HazardDetector functionality
"""

from __future__ import annotations

from tools.cq.query.hazards import (
    BUILTIN_HAZARDS,
    HazardCategory,
    HazardDetector,
    HazardSeverity,
    HazardSpec,
    get_hazards_by_category,
    get_hazards_by_severity,
    get_security_hazards,
)


class TestHazardSpec:
    """Tests for HazardSpec dataclass."""

    def test_basic_hazard_spec(self) -> None:
        """Create basic hazard spec."""
        spec = HazardSpec(
            id="test_hazard",
            pattern="eval($$$)",
            message="eval is dangerous",
            category=HazardCategory.SECURITY,
            severity=HazardSeverity.ERROR,
        )
        assert spec.id == "test_hazard"
        assert spec.pattern == "eval($$$)"
        assert spec.category == HazardCategory.SECURITY
        assert spec.severity == HazardSeverity.ERROR

    def test_hazard_spec_defaults(self) -> None:
        """HazardSpec has correct defaults."""
        spec = HazardSpec(
            id="test",
            pattern="test($$$)",
            message="test",
            category=HazardCategory.CORRECTNESS,
            severity=HazardSeverity.INFO,
        )
        assert spec.inside is None
        assert spec.not_inside is None
        assert spec.confidence_penalty == 0.5

    def test_to_ast_grep_rule(self) -> None:
        """Convert hazard spec to ast-grep rule."""
        spec = HazardSpec(
            id="eval_exec",
            pattern="eval($$$)",
            message="eval is dangerous",
            category=HazardCategory.SECURITY,
            severity=HazardSeverity.ERROR,
        )
        rule = spec.to_ast_grep_rule()

        assert rule["id"] == "hazard_eval_exec"
        assert rule["language"] == "python"
        assert rule["rule"]["pattern"] == "eval($$$)"
        assert rule["message"] == "eval is dangerous"
        assert rule["severity"] == "error"

    def test_to_ast_grep_rule_with_inside(self) -> None:
        """Convert hazard spec with inside constraint."""
        spec = HazardSpec(
            id="test",
            pattern="dangerous($$$)",
            message="dangerous inside function",
            category=HazardCategory.CORRECTNESS,
            severity=HazardSeverity.WARNING,
            inside="def $F($$$):",
        )
        rule = spec.to_ast_grep_rule()

        assert "inside" in rule["rule"]
        assert rule["rule"]["inside"]["pattern"] == "def $F($$$):"

    def test_to_ast_grep_rule_with_not_inside(self) -> None:
        """Convert hazard spec with not_inside constraint."""
        spec = HazardSpec(
            id="assert_runtime",
            pattern="assert $COND",
            message="assert can be disabled",
            category=HazardCategory.CORRECTNESS,
            severity=HazardSeverity.INFO,
            not_inside="def test_$$$($$$):",
        )
        rule = spec.to_ast_grep_rule()

        assert "not" in rule["rule"]
        assert "inside" in rule["rule"]["not"]


class TestBuiltinHazards:
    """Tests for builtin hazard registry."""

    def test_registry_not_empty(self) -> None:
        """Builtin hazards registry is not empty."""
        assert len(BUILTIN_HAZARDS) > 0

    def test_has_security_hazards(self) -> None:
        """Registry has security hazards."""
        security = [h for h in BUILTIN_HAZARDS if h.category == HazardCategory.SECURITY]
        assert len(security) > 0

    def test_has_correctness_hazards(self) -> None:
        """Registry has correctness hazards."""
        correctness = [h for h in BUILTIN_HAZARDS if h.category == HazardCategory.CORRECTNESS]
        assert len(correctness) > 0

    def test_has_design_hazards(self) -> None:
        """Registry has design hazards."""
        design = [h for h in BUILTIN_HAZARDS if h.category == HazardCategory.DESIGN]
        assert len(design) > 0

    def test_eval_hazard_exists(self) -> None:
        """Eval hazard is in registry."""
        ids = {h.id for h in BUILTIN_HAZARDS}
        assert "eval_exec" in ids

    def test_pickle_hazard_exists(self) -> None:
        """Pickle hazard is in registry."""
        ids = {h.id for h in BUILTIN_HAZARDS}
        assert "pickle_load" in ids

    def test_getattr_hazard_exists(self) -> None:
        """Getattr hazard is in registry."""
        ids = {h.id for h in BUILTIN_HAZARDS}
        assert "getattr_dynamic" in ids

    def test_unique_ids(self) -> None:
        """All hazard IDs are unique."""
        ids = [h.id for h in BUILTIN_HAZARDS]
        assert len(ids) == len(set(ids))


class TestHazardDetector:
    """Tests for HazardDetector class."""

    def test_create_detector(self) -> None:
        """Create HazardDetector with default specs."""
        detector = HazardDetector()
        assert len(detector.specs) > 0

    def test_create_detector_custom_specs(self) -> None:
        """Create HazardDetector with custom specs."""
        custom_specs = (
            HazardSpec(
                id="custom",
                pattern="custom($$$)",
                message="custom hazard",
                category=HazardCategory.CORRECTNESS,
                severity=HazardSeverity.INFO,
            ),
        )
        detector = HazardDetector(specs=custom_specs)
        assert len(detector.specs) == 1

    def test_get_ast_grep_rules(self) -> None:
        """Get ast-grep rules from detector."""
        detector = HazardDetector()
        rules = detector.get_ast_grep_rules()
        assert len(rules) == len(detector.specs)
        assert all("rule" in r for r in rules)

    def test_get_spec_by_id(self) -> None:
        """Get hazard spec by ID."""
        detector = HazardDetector()
        spec = detector.get_spec("eval_exec")
        assert spec is not None
        assert spec.id == "eval_exec"

    def test_get_spec_not_found(self) -> None:
        """Get non-existent spec returns None."""
        detector = HazardDetector()
        spec = detector.get_spec("nonexistent")
        assert spec is None

    def test_get_by_category(self) -> None:
        """Get specs by category."""
        detector = HazardDetector()
        security = detector.get_by_category(HazardCategory.SECURITY)
        assert len(security) > 0
        assert all(s.category == HazardCategory.SECURITY for s in security)


class TestHazardHelpers:
    """Tests for hazard helper functions."""

    def test_get_hazards_by_category(self) -> None:
        """get_hazards_by_category filters correctly."""
        security = get_hazards_by_category(HazardCategory.SECURITY)
        assert len(security) > 0
        assert all(h.category == HazardCategory.SECURITY for h in security)

    def test_get_hazards_by_severity(self) -> None:
        """get_hazards_by_severity filters correctly."""
        errors = get_hazards_by_severity(HazardSeverity.ERROR)
        assert len(errors) > 0
        assert all(h.severity == HazardSeverity.ERROR for h in errors)

    def test_get_security_hazards(self) -> None:
        """get_security_hazards returns security hazards."""
        security = get_security_hazards()
        assert len(security) > 0
        assert all(h.category == HazardCategory.SECURITY for h in security)


class TestHazardCategories:
    """Tests for HazardCategory and HazardSeverity enums."""

    def test_category_values(self) -> None:
        """HazardCategory has expected values."""
        assert HazardCategory.SECURITY.value == "security"
        assert HazardCategory.CORRECTNESS.value == "correctness"
        assert HazardCategory.DESIGN.value == "design"
        assert HazardCategory.PERFORMANCE.value == "performance"

    def test_severity_values(self) -> None:
        """HazardSeverity has expected values."""
        assert HazardSeverity.ERROR.value == "error"
        assert HazardSeverity.WARNING.value == "warning"
        assert HazardSeverity.INFO.value == "info"
