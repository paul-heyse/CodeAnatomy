"""Typed Python enrichment fact structs."""

from __future__ import annotations

from typing import Literal

import msgspec

from tools.cq.core.structs import CqStruct

ScopeKind = Literal["module", "class", "function", "closure"]
ItemRole = Literal["dataclass", "pydantic", "protocol", "abc", "enum", "regular"]
ClassKind = Literal["dataclass", "pydantic", "protocol", "abc", "enum", "regular"]


class PythonResolutionFacts(CqStruct, frozen=True):
    """Resolved names and scopes from Python enrichment.

    Fields populated by:
    - resolution_index.py: enclosing_callable, enclosing_class, qualified_name_candidates,
      binding_candidates, import_alias_chain
    - tree_sitter: qualified_name_candidates, binding_candidates
    """

    # Enclosing context
    enclosing_callable: str | None = None
    enclosing_class: str | None = None

    # Qualified name resolution
    qualified_name_candidates: list[str] = msgspec.field(default_factory=list)
    binding_candidates: list[str] = msgspec.field(default_factory=list)

    # Import resolution
    import_alias_chain: list[dict[str, object]] = msgspec.field(default_factory=list)


class PythonBehaviorFacts(CqStruct, frozen=True):
    """Behavioral analysis from Python enrichment.

    Fields populated by extractors.py:_extract_behavior_summary.
    """

    is_async: bool = False
    is_generator: bool = False
    returns_value: bool = False
    raises_exception: bool = False
    yields: bool = False
    awaits: bool = False
    has_context_manager: bool = False
    is_dataclass: bool = False


class PythonStructureFacts(CqStruct, frozen=True):
    """Structural metadata from Python enrichment.

    Fields populated by extractors.py:_enrich_ast_grep_core and tree-sitter.
    """

    node_kind: str | None = None
    scope_kind: ScopeKind | None = None
    scope_chain: list[str] = msgspec.field(default_factory=list)
    scope_name: str | None = None
    item_role: ItemRole | None = None

    # Class metadata
    class_name: str | None = None
    class_kind: ClassKind | None = None
    base_classes: list[str] = msgspec.field(default_factory=list)

    # Decorator metadata
    decorators: list[str] = msgspec.field(default_factory=list)


class PythonSignatureFacts(CqStruct, frozen=True):
    """Function signature metadata from Python enrichment.

    Fields populated by extractors.py:_extract_signature.
    """

    signature: str | None = None
    params: list[str] = msgspec.field(default_factory=list)
    return_type: str | None = None


class PythonCallFacts(CqStruct, frozen=True):
    """Call expression metadata from Python enrichment.

    Fields populated by extractors.py:_extract_call_target.
    """

    call_target: str | None = None
    call_receiver: str | None = None
    call_method: str | None = None
    call_args_count: int | None = None


class PythonImportFacts(CqStruct, frozen=True):
    """Import statement metadata from Python enrichment.

    Fields populated by extractors.py:_enrich_import_tier.
    """

    import_module: str | None = None
    import_names: list[str] = msgspec.field(default_factory=list)
    import_alias: str | None = None
    import_level: int = 0
    is_type_import: bool = False


class PythonClassShapeFacts(CqStruct, frozen=True):
    """Class shape metadata from Python enrichment.

    Fields populated by extractors.py:_extract_class_shape.
    """

    method_count: int = 0
    property_names: list[str] = msgspec.field(default_factory=list)
    abstract_member_count: int = 0
    class_markers: list[str] = msgspec.field(default_factory=list)


class PythonLocalsFacts(CqStruct, frozen=True):
    """Local bindings from tree-sitter enrichment.

    Fields populated by tree_sitter/python_lane/facts.py.
    """

    definitions: list[str] = msgspec.field(default_factory=list)
    references: list[str] = msgspec.field(default_factory=list)
    index: list[dict[str, object]] = msgspec.field(default_factory=list)


class PythonParseQualityFacts(CqStruct, frozen=True):
    """Parse quality metadata from tree-sitter.

    Fields populated by tree_sitter enrichment.
    """

    has_error: bool = False
    error_count: int = 0


class PythonEnrichmentFacts(CqStruct, frozen=True):
    """Complete Python enrichment payload.

    Aggregates all Python enrichment fact categories.
    """

    resolution: PythonResolutionFacts | None = None
    behavior: PythonBehaviorFacts | None = None
    structure: PythonStructureFacts | None = None
    signature: PythonSignatureFacts | None = None
    call: PythonCallFacts | None = None
    import_: PythonImportFacts | None = None
    class_shape: PythonClassShapeFacts | None = None
    locals: PythonLocalsFacts | None = None
    parse_quality: PythonParseQualityFacts | None = None


__all__ = [
    "ClassKind",
    "ItemRole",
    "PythonBehaviorFacts",
    "PythonCallFacts",
    "PythonClassShapeFacts",
    "PythonEnrichmentFacts",
    "PythonImportFacts",
    "PythonLocalsFacts",
    "PythonParseQualityFacts",
    "PythonResolutionFacts",
    "PythonSignatureFacts",
    "PythonStructureFacts",
    "ScopeKind",
]
