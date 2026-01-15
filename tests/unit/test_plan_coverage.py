"""Coverage checks for plan builder parity."""

from __future__ import annotations

from plan_audit import audit_plan_coverage


def test_normalize_ibis_plan_coverage() -> None:
    """Assert normalize plan builders have Ibis coverage parity."""
    report = audit_plan_coverage()
    normalize = report["normalize"]
    assert normalize["missing_ibis"] == []
    assert normalize["extra_ibis"] == []


def test_extract_ibis_plan_coverage() -> None:
    """Assert extract templates with plan support are Ibis-capable."""
    report = audit_plan_coverage()
    templates = report["extract"]["templates"]
    missing = [
        template["template"]
        for template in templates
        if template["supports_plan"] and not template["ibis_capable"]
    ]
    assert missing == []
