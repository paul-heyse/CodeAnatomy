"""Hazard detection for cq queries.

Identifies patterns that reduce confidence in static analysis results.
Provides ast-grep rule generation for hazard detection.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from typing import Literal

from tools.cq.core.schema import Anchor
from tools.cq.query.sg_parser import SgRecord

# Hazard types
HazardKind = Literal[
    "dynamic_dispatch",
    "getattr",
    "forwarding",
    "alias_chain",
    "eval_exec",
    "dynamic_import",
    "bare_except",
    "broad_except",
    "subprocess_shell",
    "pickle_load",
    "yaml_load",
    "sql_format",
    "assert_runtime",
    "global_mutation",
    "mutable_default",
]


class HazardCategory(Enum):
    """Categories of hazards."""

    SECURITY = "security"
    CORRECTNESS = "correctness"
    DESIGN = "design"
    PERFORMANCE = "performance"


class HazardSeverity(Enum):
    """Severity levels for hazards."""

    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclass(frozen=True)
class Hazard:
    """Detected hazard that may affect analysis accuracy.

    Attributes
    ----------
    kind
        Type of hazard
    location
        Source location where hazard was detected
    reason
        Human-readable explanation
    confidence
        Confidence reduction factor (0.0-1.0)
    category
        Hazard category (security, correctness, design, performance)
    severity
        Severity level (error, warning, info)
    """

    kind: HazardKind
    location: Anchor
    reason: str
    confidence: float
    category: HazardCategory = HazardCategory.CORRECTNESS
    severity: HazardSeverity = HazardSeverity.WARNING


@dataclass(frozen=True)
class HazardSpec:
    """Specification for a hazard pattern.

    Attributes
    ----------
    id
        Unique hazard identifier
    pattern
        ast-grep pattern to match
    message
        Human-readable description
    category
        Hazard category
    severity
        Severity level
    inside
        Optional "inside" constraint pattern
    not_inside
        Optional "not inside" constraint pattern
    confidence_penalty
        Confidence reduction when hazard is found (0.0-1.0)
    """

    id: str
    pattern: str
    message: str
    category: HazardCategory
    severity: HazardSeverity
    inside: str | None = None
    not_inside: str | None = None
    confidence_penalty: float = 0.5

    def to_ast_grep_rule(self) -> dict:
        """Convert to ast-grep rule format.

        Returns
        -------
        dict
            Rule specification for ast-grep YAML.
        """
        rule: dict = {
            "id": f"hazard_{self.id}",
            "language": "python",
            "rule": {"pattern": self.pattern},
            "message": self.message,
            "severity": self.severity.value,
        }

        if self.inside:
            rule["rule"]["inside"] = {"pattern": self.inside}

        if self.not_inside:
            rule["rule"]["not"] = {"inside": {"pattern": self.not_inside}}

        return rule


# Builtin hazard specifications
BUILTIN_HAZARDS: tuple[HazardSpec, ...] = (
    # Security hazards
    HazardSpec(
        id="eval_exec",
        pattern="eval($$$)",
        message="eval() executes arbitrary code - consider safer alternatives",
        category=HazardCategory.SECURITY,
        severity=HazardSeverity.ERROR,
        confidence_penalty=0.2,
    ),
    HazardSpec(
        id="exec_call",
        pattern="exec($$$)",
        message="exec() executes arbitrary code - consider safer alternatives",
        category=HazardCategory.SECURITY,
        severity=HazardSeverity.ERROR,
        confidence_penalty=0.2,
    ),
    HazardSpec(
        id="compile_call",
        pattern="compile($$$)",
        message="compile() can enable code execution - review usage",
        category=HazardCategory.SECURITY,
        severity=HazardSeverity.WARNING,
        confidence_penalty=0.3,
    ),
    HazardSpec(
        id="subprocess_shell",
        pattern="subprocess.$FUNC($$$, shell=True, $$$)",
        message="subprocess with shell=True is vulnerable to injection",
        category=HazardCategory.SECURITY,
        severity=HazardSeverity.ERROR,
        confidence_penalty=0.3,
    ),
    HazardSpec(
        id="pickle_load",
        pattern="pickle.load($$$)",
        message="pickle.load can execute arbitrary code - use safer formats",
        category=HazardCategory.SECURITY,
        severity=HazardSeverity.ERROR,
        confidence_penalty=0.2,
    ),
    HazardSpec(
        id="pickle_loads",
        pattern="pickle.loads($$$)",
        message="pickle.loads can execute arbitrary code - use safer formats",
        category=HazardCategory.SECURITY,
        severity=HazardSeverity.ERROR,
        confidence_penalty=0.2,
    ),
    HazardSpec(
        id="yaml_unsafe_load",
        pattern="yaml.load($X)",
        message="yaml.load without Loader is unsafe - use yaml.safe_load",
        category=HazardCategory.SECURITY,
        severity=HazardSeverity.ERROR,
        not_inside="yaml.load($X, Loader=$L)",
        confidence_penalty=0.3,
    ),
    HazardSpec(
        id="sql_format",
        pattern="$X.format($$$)",
        message="String formatting in SQL may enable injection",
        category=HazardCategory.SECURITY,
        severity=HazardSeverity.WARNING,
        inside="$CURSOR.execute($X.format($$$))",
        confidence_penalty=0.4,
    ),
    HazardSpec(
        id="sql_percent",
        pattern="$CURSOR.execute($SQL % $ARGS)",
        message="% formatting in SQL enables injection - use parameterized queries",
        category=HazardCategory.SECURITY,
        severity=HazardSeverity.ERROR,
        confidence_penalty=0.3,
    ),
    # Dynamic dispatch hazards
    HazardSpec(
        id="getattr_dynamic",
        pattern="getattr($OBJ, $ATTR)",
        message="getattr may resolve dynamically - static analysis limited",
        category=HazardCategory.CORRECTNESS,
        severity=HazardSeverity.INFO,
        confidence_penalty=0.5,
    ),
    HazardSpec(
        id="setattr_dynamic",
        pattern="setattr($OBJ, $ATTR, $VAL)",
        message="setattr modifies objects dynamically",
        category=HazardCategory.CORRECTNESS,
        severity=HazardSeverity.INFO,
        confidence_penalty=0.5,
    ),
    HazardSpec(
        id="delattr_dynamic",
        pattern="delattr($OBJ, $ATTR)",
        message="delattr removes attributes dynamically",
        category=HazardCategory.CORRECTNESS,
        severity=HazardSeverity.INFO,
        confidence_penalty=0.5,
    ),
    HazardSpec(
        id="hasattr_check",
        pattern="hasattr($OBJ, $ATTR)",
        message="hasattr suggests dynamic attribute access patterns",
        category=HazardCategory.CORRECTNESS,
        severity=HazardSeverity.INFO,
        confidence_penalty=0.6,
    ),
    # Dynamic import hazards
    HazardSpec(
        id="dunder_import",
        pattern="__import__($$$)",
        message="__import__ loads modules dynamically",
        category=HazardCategory.CORRECTNESS,
        severity=HazardSeverity.WARNING,
        confidence_penalty=0.3,
    ),
    HazardSpec(
        id="importlib_import",
        pattern="importlib.import_module($$$)",
        message="importlib.import_module loads modules dynamically",
        category=HazardCategory.CORRECTNESS,
        severity=HazardSeverity.WARNING,
        confidence_penalty=0.3,
    ),
    # Forwarding hazards
    HazardSpec(
        id="args_forward",
        pattern="$FUNC(*$ARGS)",
        message="*args forwarding may obscure call targets",
        category=HazardCategory.CORRECTNESS,
        severity=HazardSeverity.INFO,
        confidence_penalty=0.7,
    ),
    HazardSpec(
        id="kwargs_forward",
        pattern="$FUNC(**$KWARGS)",
        message="**kwargs forwarding may obscure call targets",
        category=HazardCategory.CORRECTNESS,
        severity=HazardSeverity.INFO,
        confidence_penalty=0.7,
    ),
    # Exception handling hazards
    HazardSpec(
        id="bare_except",
        pattern="except:\n    $$$",
        message="Bare except catches all exceptions including SystemExit",
        category=HazardCategory.DESIGN,
        severity=HazardSeverity.WARNING,
        confidence_penalty=0.6,
    ),
    HazardSpec(
        id="broad_except",
        pattern="except Exception:\n    $$$",
        message="Catching Exception is too broad - catch specific exceptions",
        category=HazardCategory.DESIGN,
        severity=HazardSeverity.INFO,
        confidence_penalty=0.7,
    ),
    HazardSpec(
        id="except_pass",
        pattern="except $E:\n    pass",
        message="Silent exception swallowing hides errors",
        category=HazardCategory.DESIGN,
        severity=HazardSeverity.WARNING,
        confidence_penalty=0.5,
    ),
    # Design hazards
    HazardSpec(
        id="mutable_default",
        pattern="def $F($$$, $P=[], $$$):\n    $$$",
        message="Mutable default argument - use None and check",
        category=HazardCategory.DESIGN,
        severity=HazardSeverity.WARNING,
        confidence_penalty=0.8,
    ),
    HazardSpec(
        id="mutable_default_dict",
        pattern="def $F($$$, $P={}, $$$):\n    $$$",
        message="Mutable default argument - use None and check",
        category=HazardCategory.DESIGN,
        severity=HazardSeverity.WARNING,
        confidence_penalty=0.8,
    ),
    HazardSpec(
        id="global_stmt",
        pattern="global $VAR",
        message="Global statement indicates mutable global state",
        category=HazardCategory.DESIGN,
        severity=HazardSeverity.INFO,
        confidence_penalty=0.6,
    ),
    HazardSpec(
        id="assert_runtime",
        pattern="assert $COND",
        message="Assert can be disabled with -O flag",
        category=HazardCategory.CORRECTNESS,
        severity=HazardSeverity.INFO,
        not_inside="def test_$$$($$$):\n    $$$",  # Ignore in tests
        confidence_penalty=0.8,
    ),
)


# Legacy confidence penalties for backward compatibility
HAZARD_CONFIDENCE: dict[HazardKind, float] = {
    "dynamic_dispatch": 0.60,
    "getattr": 0.50,
    "forwarding": 0.70,
    "alias_chain": 0.40,
    "eval_exec": 0.20,
    "dynamic_import": 0.30,
    "bare_except": 0.60,
    "broad_except": 0.70,
    "subprocess_shell": 0.30,
    "pickle_load": 0.20,
    "yaml_load": 0.30,
    "sql_format": 0.40,
    "assert_runtime": 0.80,
    "global_mutation": 0.60,
    "mutable_default": 0.80,
}


class HazardDetector:
    """Detector for code hazards using ast-grep rules."""

    def __init__(self, specs: tuple[HazardSpec, ...] = BUILTIN_HAZARDS) -> None:
        """Initialize detector with hazard specifications."""
        self.specs = specs
        self._by_id: dict[str, HazardSpec] = {s.id: s for s in specs}
        self._by_category: dict[HazardCategory, list[HazardSpec]] = {}
        for spec in specs:
            if spec.category not in self._by_category:
                self._by_category[spec.category] = []
            self._by_category[spec.category].append(spec)

    def get_ast_grep_rules(self) -> list[dict]:
        """Get all hazard specs as ast-grep rules.

        Returns
        -------
        list[dict]
            List of ast-grep rule dictionaries.
        """
        return [spec.to_ast_grep_rule() for spec in self.specs]

    def build_inline_rules_yaml(self) -> str:
        """Build YAML string for inline ast-grep rules.

        Returns
        -------
        str
            YAML-formatted rules.
        """
        import yaml

        rules = {"rules": self.get_ast_grep_rules()}
        return yaml.dump(rules, default_flow_style=False)

    def get_spec(self, hazard_id: str) -> HazardSpec | None:
        """Get hazard spec by ID."""
        return self._by_id.get(hazard_id)

    def get_by_category(self, category: HazardCategory) -> list[HazardSpec]:
        """Get hazard specs by category."""
        return self._by_category.get(category, [])


def get_hazards_by_category(category: HazardCategory) -> list[HazardSpec]:
    """Get builtin hazards by category."""
    return [h for h in BUILTIN_HAZARDS if h.category == category]


def get_hazards_by_severity(severity: HazardSeverity) -> list[HazardSpec]:
    """Get builtin hazards by severity."""
    return [h for h in BUILTIN_HAZARDS if h.severity == severity]


def get_security_hazards() -> list[HazardSpec]:
    """Get all security-related hazards."""
    return get_hazards_by_category(HazardCategory.SECURITY)


def detect_hazards(
    records: list[SgRecord],
    symbol_table: dict | None = None,
) -> list[Hazard]:
    """Detect resolution hazards in ast-grep records.

    Parameters
    ----------
    records
        ast-grep records to analyze
    symbol_table
        Optional symbol table for cross-reference analysis

    Returns
    -------
    list[Hazard]
        Detected hazards with locations and confidence impacts
    """
    hazards: list[Hazard] = []

    for record in records:
        detected = _analyze_record(record)
        hazards.extend(detected)

    return hazards


def _analyze_record(record: SgRecord) -> list[Hazard]:
    """Analyze a single record for hazards."""
    hazards: list[Hazard] = []
    text = record.text
    anchor = Anchor(
        file=record.file,
        line=record.start_line,
        col=record.start_col,
    )

    # Check for getattr patterns
    if _has_getattr(text):
        hazards.append(
            Hazard(
                kind="getattr",
                location=anchor,
                reason="getattr usage may resolve attributes dynamically",
                confidence=HAZARD_CONFIDENCE["getattr"],
            )
        )

    # Check for argument forwarding
    if _has_forwarding(text):
        hazards.append(
            Hazard(
                kind="forwarding",
                location=anchor,
                reason="*args/**kwargs forwarding may obscure call targets",
                confidence=HAZARD_CONFIDENCE["forwarding"],
            )
        )

    # Check for eval/exec
    if _has_eval_exec(text):
        hazards.append(
            Hazard(
                kind="eval_exec",
                location=anchor,
                reason="eval/exec may execute arbitrary code",
                confidence=HAZARD_CONFIDENCE["eval_exec"],
            )
        )

    # Check for dynamic imports
    if _has_dynamic_import(text):
        hazards.append(
            Hazard(
                kind="dynamic_import",
                location=anchor,
                reason="__import__ or importlib may load modules dynamically",
                confidence=HAZARD_CONFIDENCE["dynamic_import"],
            )
        )

    # Check for dynamic dispatch patterns (method calls on unknown types)
    if record.record == "call" and record.kind == "attr_call":
        if _is_dynamic_dispatch(text):
            hazards.append(
                Hazard(
                    kind="dynamic_dispatch",
                    location=anchor,
                    reason="method call on dynamically typed receiver",
                    confidence=HAZARD_CONFIDENCE["dynamic_dispatch"],
                )
            )

    return hazards


def _has_getattr(text: str) -> bool:
    """Check for getattr/setattr/delattr patterns."""
    patterns = [
        r"\bgetattr\s*\(",
        r"\bsetattr\s*\(",
        r"\bdelattr\s*\(",
        r"\bhasattr\s*\(",
        r"__getattr__",
        r"__getattribute__",
    ]
    return any(re.search(p, text) for p in patterns)


def _has_forwarding(text: str) -> bool:
    """Check for argument forwarding patterns."""
    # Look for *args or **kwargs in function calls
    if re.search(r"\(\s*\*\w+", text):
        return True
    if re.search(r"\(\s*\*\*\w+", text):
        return True
    if re.search(r",\s*\*\w+", text):
        return True
    if re.search(r",\s*\*\*\w+", text):
        return True
    return False


def _has_eval_exec(text: str) -> bool:
    """Check for eval/exec usage."""
    patterns = [
        r"\beval\s*\(",
        r"\bexec\s*\(",
        r"\bcompile\s*\(",
    ]
    return any(re.search(p, text) for p in patterns)


def _has_dynamic_import(text: str) -> bool:
    """Check for dynamic import patterns."""
    patterns = [
        r"\b__import__\s*\(",
        r"\bimportlib\.import_module\s*\(",
        r"\bimportlib\s*\.\s*import_module\s*\(",
    ]
    return any(re.search(p, text) for p in patterns)


def _is_dynamic_dispatch(text: str) -> bool:
    """Check if a method call has a dynamically typed receiver.

    Heuristic: If the receiver is a simple name (not 'self', 'cls', or
    a known module), and it's being called with a method, it may be
    dynamic dispatch.
    """
    # Extract receiver from method call
    match = re.match(r"(\w+)\.", text)
    if not match:
        return False

    receiver = match.group(1)

    # Known safe receivers
    safe_receivers = {
        "self",
        "cls",
        "super",
        # Common modules that are reliably typed
        "os",
        "sys",
        "re",
        "json",
        "logging",
        "pathlib",
        "typing",
        "collections",
        "itertools",
        "functools",
    }

    if receiver.lower() in safe_receivers:
        return False

    # If receiver starts with uppercase, likely a class reference (safer)
    if receiver[0].isupper():
        return False

    return True


def summarize_hazards(hazards: list[Hazard]) -> dict:
    """Summarize hazards by kind and compute aggregate confidence.

    Parameters
    ----------
    hazards
        List of detected hazards

    Returns
    -------
    dict
        Summary with counts by kind and aggregate confidence
    """
    if not hazards:
        return {
            "total": 0,
            "by_kind": {},
            "aggregate_confidence": 1.0,
        }

    counts: dict[str, int] = {}
    min_confidence = 1.0

    for hazard in hazards:
        counts[hazard.kind] = counts.get(hazard.kind, 0) + 1
        min_confidence = min(min_confidence, hazard.confidence)

    return {
        "total": len(hazards),
        "by_kind": counts,
        "aggregate_confidence": min_confidence,
    }
