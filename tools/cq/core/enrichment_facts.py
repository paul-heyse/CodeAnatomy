"""Canonical enrichment fact resolution helpers for markdown rendering."""

from __future__ import annotations

from typing import Literal

from tools.cq.core.structs import CqStruct

NAReason = Literal["not_applicable", "not_resolved", "enrichment_unavailable"]

_LANG_KEYS: tuple[str, ...] = ("python", "rust")
_STRUCTURED_KEYS: frozenset[str] = frozenset(
    {"meta", "resolution", "behavior", "structural", "parse_quality", "agreement"}
)

_FUNCTION_LIKE_KINDS: frozenset[str] = frozenset(
    {
        "function_definition",
        "decorated_definition",
        "function_item",
    }
)
_CLASS_LIKE_KINDS: frozenset[str] = frozenset(
    {
        "class_definition",
        "struct_item",
        "enum_item",
        "trait_item",
        "impl_item",
    }
)
_IMPORT_LIKE_KINDS: frozenset[str] = frozenset(
    {"import_statement", "import_from_statement", "use_declaration"}
)


class FactFieldSpec(CqStruct, frozen=True):
    """Specification for a single code-fact row."""

    label: str
    paths: tuple[tuple[str, ...], ...]
    applicable_languages: frozenset[str] | None = None
    applicable_kinds: frozenset[str] | None = None
    fallback_reason: NAReason = "not_resolved"


class FactClusterSpec(CqStruct, frozen=True):
    """Specification for a code-fact cluster."""

    title: str
    fields: tuple[FactFieldSpec, ...]


class FactContext(CqStruct, frozen=True):
    """Language and node-kind context used for fact applicability."""

    language: str | None
    node_kind: str | None


class ResolvedFact(CqStruct, frozen=True):
    """Resolved fact row payload for renderer output."""

    label: str
    value: object | None
    reason: NAReason | None = None


class ResolvedFactCluster(CqStruct, frozen=True):
    """Resolved fact cluster payload for renderer output."""

    title: str
    rows: tuple[ResolvedFact, ...]


FACT_CLUSTERS: tuple[FactClusterSpec, ...] = (
    FactClusterSpec(
        title="Identity",
        fields=(
            FactFieldSpec(
                label="Language",
                paths=(
                    ("meta", "language"),
                    ("language",),
                ),
            ),
            FactFieldSpec(
                label="Symbol Role",
                paths=(
                    ("resolution", "symbol_role"),
                    ("resolution", "item_role"),
                    ("structural", "item_role"),
                    ("item_role",),
                ),
            ),
            FactFieldSpec(
                label="Qualified Name",
                paths=(
                    ("resolution", "qualified_name_candidates"),
                    ("qualified_name_candidates",),
                ),
            ),
            FactFieldSpec(
                label="Binding Candidates",
                paths=(
                    ("resolution", "binding_candidates"),
                    ("binding_candidates",),
                ),
            ),
        ),
    ),
    FactClusterSpec(
        title="Scope",
        fields=(
            FactFieldSpec(
                label="Enclosing Callable",
                paths=(
                    ("resolution", "enclosing_callable"),
                    ("containing_scope",),
                ),
            ),
            FactFieldSpec(
                label="Enclosing Class",
                paths=(
                    ("resolution", "enclosing_class"),
                    ("enclosing_class",),
                ),
            ),
            FactFieldSpec(
                label="Import Alias Chain",
                paths=(
                    ("resolution", "import_alias_chain"),
                    ("import_alias_chain",),
                ),
            ),
            FactFieldSpec(
                label="Visibility",
                paths=(
                    ("structural", "visibility"),
                    ("visibility",),
                ),
                applicable_languages=frozenset({"rust"}),
            ),
        ),
    ),
    FactClusterSpec(
        title="Interface",
        fields=(
            FactFieldSpec(
                label="Signature",
                paths=(
                    ("structural", "signature"),
                    ("signature",),
                ),
                applicable_kinds=_FUNCTION_LIKE_KINDS | _CLASS_LIKE_KINDS,
            ),
            FactFieldSpec(
                label="Parameters",
                paths=(
                    ("structural", "params"),
                    ("structural", "parameters"),
                    ("params",),
                    ("parameters",),
                ),
                applicable_kinds=_FUNCTION_LIKE_KINDS,
            ),
            FactFieldSpec(
                label="Return Type",
                paths=(
                    ("structural", "return_type"),
                    ("return_type",),
                ),
                applicable_kinds=_FUNCTION_LIKE_KINDS,
            ),
            FactFieldSpec(
                label="Attributes or Decorators",
                paths=(
                    ("structural", "attributes"),
                    ("structural", "decorators"),
                    ("attributes",),
                    ("decorators",),
                ),
                applicable_kinds=_FUNCTION_LIKE_KINDS | _CLASS_LIKE_KINDS,
            ),
        ),
    ),
    FactClusterSpec(
        title="Behavior",
        fields=(
            FactFieldSpec(
                label="Async or Generator",
                paths=(
                    ("behavior", "is_async"),
                    ("behavior", "is_generator"),
                    ("is_async",),
                    ("is_generator",),
                ),
                applicable_kinds=_FUNCTION_LIKE_KINDS,
            ),
            FactFieldSpec(
                label="Await or Yield",
                paths=(
                    ("behavior", "has_await"),
                    ("behavior", "awaits"),
                    ("behavior", "has_yield"),
                    ("behavior", "yields"),
                    ("has_await",),
                    ("awaits",),
                    ("has_yield",),
                    ("yields",),
                ),
                applicable_kinds=_FUNCTION_LIKE_KINDS,
            ),
            FactFieldSpec(
                label="Raises",
                paths=(
                    ("behavior", "has_raise"),
                    ("behavior", "raises_exception"),
                    ("has_raise",),
                    ("raises_exception",),
                ),
                applicable_kinds=_FUNCTION_LIKE_KINDS,
            ),
            FactFieldSpec(
                label="Control Flow Context",
                paths=(
                    ("behavior", "in_try"),
                    ("behavior", "in_except"),
                    ("behavior", "in_with"),
                    ("behavior", "in_loop"),
                    ("in_try",),
                    ("in_except",),
                    ("in_with",),
                    ("in_loop",),
                ),
                applicable_languages=frozenset({"python"}),
                applicable_kinds=_FUNCTION_LIKE_KINDS,
            ),
        ),
    ),
    FactClusterSpec(
        title="Structure",
        fields=(
            FactFieldSpec(
                label="Struct Fields",
                paths=(
                    ("structural", "struct_fields"),
                    ("struct_fields",),
                ),
                applicable_languages=frozenset({"rust"}),
                applicable_kinds=frozenset({"struct_item"}),
            ),
            FactFieldSpec(
                label="Struct Field Count",
                paths=(
                    ("structural", "struct_field_count"),
                    ("struct_field_count",),
                ),
                applicable_languages=frozenset({"rust"}),
                applicable_kinds=frozenset({"struct_item"}),
            ),
            FactFieldSpec(
                label="Enum Variants",
                paths=(
                    ("structural", "enum_variants"),
                    ("enum_variants",),
                ),
                applicable_languages=frozenset({"rust"}),
                applicable_kinds=frozenset({"enum_item"}),
            ),
            FactFieldSpec(
                label="Enum Variant Count",
                paths=(
                    ("structural", "enum_variant_count"),
                    ("enum_variant_count",),
                ),
                applicable_languages=frozenset({"rust"}),
                applicable_kinds=frozenset({"enum_item"}),
            ),
        ),
    ),
)


def has_fact_value(value: object) -> bool:
    """Return whether a value should be treated as a present fact."""
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, tuple, dict, set)):
        return bool(value)
    return True


def resolve_primary_language_payload(
    payload: dict[str, object],
) -> tuple[str | None, dict[str, object] | None]:
    """Resolve primary enrichment language and language payload.

    Returns:
    -------
    tuple[str | None, dict[str, object] | None]
        Resolved language key and corresponding payload map.
    """
    language_value = payload.get("language")
    language = language_value if isinstance(language_value, str) else None
    if language is not None and language in _LANG_KEYS:
        direct = payload.get(language)
        if isinstance(direct, dict):
            nested_lang = _extract_nested_language(direct)
            return nested_lang or language, direct

    for key in _LANG_KEYS:
        candidate = payload.get(key)
        if isinstance(candidate, dict):
            nested_lang = _extract_nested_language(candidate)
            return nested_lang or key, candidate

    if language is not None:
        return language, payload if isinstance(payload, dict) else None
    return None, payload if isinstance(payload, dict) else None


def resolve_fact_context(
    *,
    language: str | None,
    language_payload: dict[str, object] | None,
) -> FactContext:
    """Build language/node-kind context for fact applicability.

    Returns:
    -------
    FactContext
        Language and node-kind context used for applicability filtering.
    """
    if language_payload is None:
        return FactContext(language=language, node_kind=None)
    node_kind = _extract_node_kind(language_payload)
    return FactContext(language=language, node_kind=node_kind)


def resolve_fact_clusters(
    *,
    context: FactContext,
    language_payload: dict[str, object] | None,
) -> tuple[ResolvedFactCluster, ...]:
    """Resolve all fact clusters from enrichment payload.

    Returns:
    -------
    tuple[ResolvedFactCluster, ...]
        Fact clusters with resolved row values and N/A reasons.
    """
    clusters: list[ResolvedFactCluster] = []
    for cluster in FACT_CLUSTERS:
        rows = tuple(
            _resolve_field(
                field=field,
                context=context,
                language_payload=language_payload,
            )
            for field in cluster.fields
        )
        if rows and all(row.reason == "not_applicable" for row in rows):
            continue
        clusters.append(ResolvedFactCluster(title=cluster.title, rows=rows))
    return tuple(clusters)


def additional_language_payload(language_payload: dict[str, object] | None) -> dict[str, object]:
    """Return non-structured payload keys for additional-facts rendering."""
    if not isinstance(language_payload, dict):
        return {}
    return {
        key: value
        for key, value in language_payload.items()
        if key not in _STRUCTURED_KEYS and key != "language"
    }


def _resolve_field(
    *,
    field: FactFieldSpec,
    context: FactContext,
    language_payload: dict[str, object] | None,
) -> ResolvedFact:
    if language_payload is None:
        return ResolvedFact(label=field.label, value=None, reason="enrichment_unavailable")
    if not _field_applicable(field=field, context=context):
        return ResolvedFact(label=field.label, value=None, reason="not_applicable")

    for path in field.paths:
        found, value = _lookup_path(language_payload, path)
        if found and has_fact_value(value):
            return ResolvedFact(label=field.label, value=value, reason=None)
    return ResolvedFact(label=field.label, value=None, reason=field.fallback_reason)


def _field_applicable(*, field: FactFieldSpec, context: FactContext) -> bool:
    language_mismatch = field.applicable_languages is not None and (
        context.language is None or context.language not in field.applicable_languages
    )
    kind_mismatch = field.applicable_kinds is not None and (
        context.node_kind is None or context.node_kind not in field.applicable_kinds
    )
    return not (language_mismatch or kind_mismatch)


def _lookup_path(payload: dict[str, object], path: tuple[str, ...]) -> tuple[bool, object | None]:
    current: object = payload
    for key in path:
        if not isinstance(current, dict) or key not in current:
            return False, None
        current = current[key]
    return True, current


def _extract_nested_language(payload: dict[str, object]) -> str | None:
    meta = payload.get("meta")
    if isinstance(meta, dict):
        value = meta.get("language")
        if isinstance(value, str):
            return value
    value = payload.get("language")
    return value if isinstance(value, str) else None


def _extract_node_kind(payload: dict[str, object]) -> str | None:
    for path in (
        ("structural", "node_kind"),
        ("resolution", "node_kind"),
        ("node_kind",),
    ):
        found, value = _lookup_path(payload, path)
        if found and isinstance(value, str) and value:
            return value

    # Fallback heuristics when node_kind is absent.
    role = payload.get("item_role")
    if role in {"import", "from_import", "use_import"}:
        return "import_statement"
    if has_fact_value(payload.get("call_target")):
        return "call_expression"
    if has_fact_value(payload.get("signature")) and has_fact_value(payload.get("params")):
        return "function_definition"
    has_struct_fields = has_fact_value(payload.get("struct_fields"))
    has_enum_variants = has_fact_value(payload.get("enum_variants"))
    if has_struct_fields or has_enum_variants:
        return "struct_item" if has_struct_fields else "enum_item"
    return None


__all__ = [
    "FactContext",
    "ResolvedFact",
    "ResolvedFactCluster",
    "additional_language_payload",
    "has_fact_value",
    "resolve_fact_clusters",
    "resolve_fact_context",
    "resolve_primary_language_payload",
]
