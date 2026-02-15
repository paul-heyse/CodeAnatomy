"""Consolidated contracts and rules for tree-sitter query planning and lint."""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import TYPE_CHECKING

import msgspec

from tools.cq.core.structs import CqStruct
from tools.cq.core.typed_boundary import BoundaryDecodeError, decode_yaml_strict
from tools.cq.search.tree_sitter.query.support import query_contracts_path
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


class QueryPackContractsFileV1(CqStruct, frozen=True):
    """Top-level tree-sitter query contracts file payload."""

    language: str | None = None
    version: int = 1
    rules: QueryPackRulesV1 = msgspec.field(default_factory=QueryPackRulesV1)


def load_pack_rules(language: str) -> QueryPackRulesV1:
    """Load query-pack rules from ``contracts.yaml``, falling back to defaults."""
    path = query_contracts_path(language)
    if not path.exists():
        return QueryPackRulesV1()
    try:
        payload = decode_yaml_strict(path.read_bytes(), type_=QueryPackContractsFileV1)
    except (OSError, BoundaryDecodeError):
        return QueryPackRulesV1()
    rules = payload.rules
    if rules.required_metadata_keys:
        return rules
    return msgspec.structs.replace(
        rules,
        required_metadata_keys=QueryPackRulesV1().required_metadata_keys,
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
    "QueryPackContractsFileV1",
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
