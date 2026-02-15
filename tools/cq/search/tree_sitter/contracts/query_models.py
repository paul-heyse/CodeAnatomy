"""Consolidated contracts and rules for tree-sitter query planning and lint."""

from __future__ import annotations

import re
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING

import msgspec

from tools.cq.core.structs import CqStruct
from tools.cq.search.tree_sitter.schema.node_schema import GrammarSchemaIndex

if TYPE_CHECKING:
    from tree_sitter import Query


class QueryPredicateRuleV1(CqStruct, frozen=True):
    """One supported predicate signature in CQ runtime."""

    predicate_name: str
    min_args: int = 0
    max_args: int | None = None


SUPPORTED_QUERY_PREDICATES: tuple[QueryPredicateRuleV1, ...] = (
    QueryPredicateRuleV1(predicate_name="cq-regex?", min_args=2, max_args=2),
    QueryPredicateRuleV1(predicate_name="cq-eq?", min_args=2),
    QueryPredicateRuleV1(predicate_name="cq-match?", min_args=2),
    QueryPredicateRuleV1(predicate_name="cq-any-of?", min_args=2),
)

SUPPORTED_QUERY_PREDICATE_NAMES: frozenset[str] = frozenset(
    row.predicate_name for row in SUPPORTED_QUERY_PREDICATES
)

ALIASED_QUERY_PREDICATE_NAMES: frozenset[str] = frozenset({"eq?", "eq", "match?", "any-of?"})


class QueryPackRulesV1(CqStruct, frozen=True):
    """Validation rules enforced for one query-pack language lane."""

    require_rooted: bool = True
    forbid_non_local: bool = True
    required_metadata_keys: tuple[str, ...] = ("cq.emit", "cq.kind", "cq.anchor")
    forbidden_capture_names: tuple[str, ...] = ()


class QueryPackLintIssueV1(CqStruct, frozen=True):
    """One query-pack lint issue row."""

    language: str
    pack_name: str
    code: str
    message: str


class QueryPackLintSummaryV1(CqStruct, frozen=True):
    """Aggregate lint summary for one language lane."""

    language: str
    issues: tuple[QueryPackLintIssueV1, ...] = ()


class QueryPatternPlanV1(CqStruct, frozen=True):
    """Per-pattern planning row used for pack scheduling."""

    pattern_idx: int
    rooted: bool
    non_local: bool
    guaranteed_step0: bool
    start_byte: int
    end_byte: int
    assertions: dict[str, tuple[str, bool]]
    capture_quantifiers: tuple[str, ...]
    score: float


class QueryPackPlanV1(CqStruct, frozen=True):
    """Plan rows for all patterns in one query pack."""

    pack_name: str
    query_hash: str
    plans: tuple[QueryPatternPlanV1, ...]
    score: float = 0.0


class GrammarDriftReportV1(CqStruct, frozen=True):
    """Compatibility report between grammar schema and query packs."""

    language: str
    grammar_digest: str
    query_digest: str
    compatible: bool = True
    errors: tuple[str, ...] = ()
    schema_diff: dict[str, object] = msgspec.field(default_factory=dict)


class QuerySpecializationProfileV1(CqStruct, frozen=True):
    """Pattern/capture specialization profile for one request surface."""

    request_surface: str
    disabled_pattern_settings: tuple[str, ...] = ()
    disabled_capture_prefixes: tuple[str, ...] = ()
    disabled_capture_names: tuple[str, ...] = ()


_NODE_PATTERN = re.compile(r"\(([A-Za-z_][A-Za-z0-9_]*)")
_FIELD_PATTERN = re.compile(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*:")
_PREDICATE_PATTERN = re.compile(r"#([A-Za-z0-9_.!?-]+)")
_CAPTURE_PATTERN = re.compile(r"@([A-Za-z_][A-Za-z0-9_.-]*)")
_IGNORED_NODE_KINDS = frozenset({"ERROR", "MISSING", "_"})
_RULES_BASE_INDENT = 2


def _contracts_path(language: str) -> Path:
    return Path(__file__).resolve().parents[2] / "queries" / language / "contracts.yaml"


def _truthy(raw: str) -> bool:
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _parse_inline_list(raw: str) -> tuple[str, ...]:
    value = raw.strip()
    if not value:
        return ()
    if value.startswith("[") and value.endswith("]"):
        value = value[1:-1]
    if not value:
        return ()
    return tuple(
        item.strip().strip("'").strip('"')
        for item in value.split(",")
        if item.strip().strip("'").strip('"')
    )


def load_pack_rules(language: str) -> QueryPackRulesV1:  # noqa: C901, PLR0912, PLR0914, PLR0915
    """Load query-pack rules from ``contracts.yaml``, falling back to defaults."""
    path = _contracts_path(language)
    if not path.exists():
        return QueryPackRulesV1()
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError:
        return QueryPackRulesV1()
    require_rooted = True
    forbid_non_local = True
    required_metadata_keys: list[str] = []
    forbidden_capture_names: list[str] = []
    in_rules = False
    active_list_key: str | None = None

    for raw_line in raw.splitlines():
        line_no_comment = raw_line.split("#", maxsplit=1)[0] if "#" in raw_line else raw_line
        stripped_line = line_no_comment.rstrip()
        if not stripped_line.strip():
            continue
        indent = len(stripped_line) - len(stripped_line.lstrip(" "))
        stripped = stripped_line.strip()
        if stripped == "rules:":
            in_rules = True
            active_list_key = None
            continue
        if not in_rules:
            continue
        if indent < _RULES_BASE_INDENT:
            in_rules = False
            active_list_key = None
            continue
        if stripped.startswith("- "):
            item = stripped[2:].strip().strip("'").strip('"')
            if not item:
                continue
            if active_list_key == "required_metadata_keys":
                required_metadata_keys.append(item)
            elif active_list_key == "forbidden_capture_names":
                forbidden_capture_names.append(item)
            continue
        key, sep, value = stripped.partition(":")
        if not sep:
            continue
        key = key.strip()
        value = value.strip()
        if key == "require_rooted":
            require_rooted = _truthy(value)
            active_list_key = None
        elif key == "forbid_non_local":
            forbid_non_local = _truthy(value)
            active_list_key = None
        elif key == "required_metadata_keys":
            active_list_key = key
            required_metadata_keys.extend(_parse_inline_list(value))
        elif key == "forbidden_capture_names":
            active_list_key = key
            forbidden_capture_names.extend(_parse_inline_list(value))
        else:
            active_list_key = None

    unique_required = tuple(key for key in required_metadata_keys if key and key not in {"-", "[]"})
    unique_forbidden = tuple(
        key for key in forbidden_capture_names if key and key not in {"-", "[]"}
    )
    return QueryPackRulesV1(
        require_rooted=require_rooted,
        forbid_non_local=forbid_non_local,
        required_metadata_keys=unique_required
        if unique_required
        else QueryPackRulesV1().required_metadata_keys,
        forbidden_capture_names=unique_forbidden,
    )


def lint_query_pack_source(
    *,
    language: str,
    pack_name: str,
    source: str,
    schema_index: GrammarSchemaIndex,
    compile_query: Callable[[str], Query],
) -> tuple[QueryPackLintIssueV1, ...]:
    """Lint one query source using schema checks and query introspection."""
    issues: list[QueryPackLintIssueV1] = []

    try:
        query = compile_query(source)
    except (RuntimeError, TypeError, ValueError, AttributeError) as exc:
        issues.append(
            QueryPackLintIssueV1(
                language=language,
                pack_name=pack_name,
                code="compile_error",
                message=type(exc).__name__,
            )
        )
        return tuple(issues)

    unknown_nodes = [
        node_kind
        for node_kind in _NODE_PATTERN.findall(source)
        if node_kind not in _IGNORED_NODE_KINDS
        and node_kind not in schema_index.named_node_kinds
        and node_kind not in schema_index.all_node_kinds
    ]
    issues.extend(
        QueryPackLintIssueV1(
            language=language,
            pack_name=pack_name,
            code="unknown_node",
            message=node_kind,
        )
        for node_kind in unknown_nodes
    )

    unknown_fields = [
        field_name
        for field_name in _FIELD_PATTERN.findall(source)
        if field_name not in schema_index.field_names
    ]
    issues.extend(
        QueryPackLintIssueV1(
            language=language,
            pack_name=pack_name,
            code="unknown_field",
            message=field_name,
        )
        for field_name in unknown_fields
    )

    rules = load_pack_rules(language)
    found_captures = set(_CAPTURE_PATTERN.findall(source))
    forbidden_captures = set(rules.forbidden_capture_names)
    for capture_name in sorted(found_captures):
        if capture_name not in forbidden_captures:
            continue
        issues.append(
            QueryPackLintIssueV1(
                language=language,
                pack_name=pack_name,
                code="forbidden_capture_name",
                message=capture_name,
            )
        )

    pattern_count = int(getattr(query, "pattern_count", 0))
    if pattern_count == 0:
        issues.append(
            QueryPackLintIssueV1(
                language=language,
                pack_name=pack_name,
                code="empty_query_pack",
                message="pattern_count=0",
            )
        )

    for pattern_idx in range(pattern_count):
        is_rooted = bool(query.is_pattern_rooted(pattern_idx))
        if rules.require_rooted and not is_rooted:
            issues.append(
                QueryPackLintIssueV1(
                    language=language,
                    pack_name=pack_name,
                    code="pattern_not_rooted",
                    message=f"pattern={pattern_idx}",
                )
            )
        is_non_local = bool(query.is_pattern_non_local(pattern_idx))
        if rules.forbid_non_local and is_non_local:
            issues.append(
                QueryPackLintIssueV1(
                    language=language,
                    pack_name=pack_name,
                    code="pattern_non_local",
                    message=f"pattern={pattern_idx}",
                )
            )

    custom_predicates = {
        name for name in _PREDICATE_PATTERN.findall(source) if name.startswith("cq-")
    }
    for predicate_name in sorted(custom_predicates):
        if predicate_name in SUPPORTED_QUERY_PREDICATE_NAMES:
            continue
        issues.append(
            QueryPackLintIssueV1(
                language=language,
                pack_name=pack_name,
                code="unsupported_custom_predicate",
                message=predicate_name,
            )
        )

    return tuple(issues)


__all__ = [
    "ALIASED_QUERY_PREDICATE_NAMES",
    "SUPPORTED_QUERY_PREDICATES",
    "SUPPORTED_QUERY_PREDICATE_NAMES",
    "GrammarDriftReportV1",
    "QueryPackLintIssueV1",
    "QueryPackLintSummaryV1",
    "QueryPackPlanV1",
    "QueryPackRulesV1",
    "QueryPatternPlanV1",
    "QueryPredicateRuleV1",
    "QuerySpecializationProfileV1",
    "lint_query_pack_source",
    "load_pack_rules",
]
