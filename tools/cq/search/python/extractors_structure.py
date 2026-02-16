"""Python class structure extraction functions for enrichment pipeline.

This module provides functions for extracting class hierarchy, base classes,
class kind classification, and API shape summaries.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ast_grep_py import SgNode

from tools.cq.search.python.extractors_classification import _unwrap_decorated

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_MAX_BASE_CLASSES = 6
_MAX_BASE_CLASS_LEN = 60
_MAX_PROPERTIES_SHOWN = 8
_MAX_PARENT_DEPTH = 20

_ENUM_BASE_NAMES: frozenset[str] = frozenset(
    {
        "Enum",
        "IntEnum",
        "StrEnum",
        "Flag",
        "IntFlag",
    }
)

_PROTOCOL_BASE_NAMES: frozenset[str] = frozenset(
    {
        "Protocol",
        "ABC",
        "ABCMeta",
    }
)

# ---------------------------------------------------------------------------
# Class hierarchy extraction
# ---------------------------------------------------------------------------


def find_enclosing_class(node: SgNode) -> SgNode | None:
    """Walk parent chain to find enclosing class_definition.

    Parameters
    ----------
    node
        Starting ast-grep node.

    Returns:
    -------
    SgNode | None
        The enclosing class_definition node, or None.
    """
    if not node.inside(kind="class_definition"):
        return None

    current = node.parent()
    depth = 0
    while current is not None and depth < _MAX_PARENT_DEPTH:
        kind = current.kind()
        if kind == "class_definition":
            return current
        if kind == "decorated_definition":
            inner = _unwrap_decorated(current)
            if inner.kind() == "class_definition":
                return inner
        depth += 1
        current = current.parent()
    return None


def extract_base_classes(
    class_node: SgNode,
    *,
    truncate: Callable[[str, int, str | None], str],
) -> list[str]:
    """Extract base class names from a class definition.

    Parameters
    ----------
    class_node
        A class_definition node.
    truncate
        Truncation helper function.

    Returns:
    -------
    list[str]
        List of base class names, capped at ``_MAX_BASE_CLASSES``.
    """
    bases: list[str] = []
    # Look for argument_list (superclass list) child
    superclasses = class_node.field("superclasses")
    if superclasses is None:
        # Fallback: look for argument_list child
        for child in class_node.children():
            if child.kind() == "argument_list":
                superclasses = child
                break
    if superclasses is None:
        return bases

    for child in superclasses.children():
        if child.is_named() and child.kind() not in {"(", ")", ","}:
            text = child.text().strip()
            if text:
                bases.append(truncate(text, _MAX_BASE_CLASS_LEN, "base_class"))
            if len(bases) >= _MAX_BASE_CLASSES:
                break
    return bases


def classify_class_kind(class_node: SgNode, bases: list[str]) -> str:
    """Classify a class into a kind category.

    Parameters
    ----------
    class_node
        A class_definition node.
    bases
        Pre-extracted base class names.

    Returns:
    -------
    str
        One of ``"dataclass"``, ``"protocol"``, ``"enum"``, ``"exception"``,
        or ``"class"``.
    """
    # Check for dataclass decorator on parent decorated_definition
    parent = class_node.parent()
    if parent is not None and parent.kind() == "decorated_definition":
        for child in parent.children():
            if child.kind() == "decorator":
                dec_text = child.text().strip()
                if dec_text.startswith("@"):
                    dec_text = dec_text[1:]
                bare = dec_text.split("(")[0].strip()
                if bare.endswith("dataclass"):
                    return "dataclass"

    base_set = frozenset(bases)

    if base_set & _PROTOCOL_BASE_NAMES:
        return "protocol"

    if base_set & _ENUM_BASE_NAMES:
        return "enum"

    # Exception detection
    name_node = class_node.field("name")
    class_name = name_node.text() if name_node is not None else ""
    if "Exception" in base_set or "BaseException" in base_set or class_name.endswith("Error"):
        return "exception"

    return "class"


# ---------------------------------------------------------------------------
# Class API shape extraction
# ---------------------------------------------------------------------------


def count_methods_and_properties(
    body_node: SgNode,
) -> tuple[int, list[str], int]:
    """Count methods, collect property names, and count abstract members.

    Parameters
    ----------
    body_node
        The body node of a class_definition.

    Returns:
    -------
    tuple[int, list[str], int]
        method_count, property_names, abstract_count.
    """
    method_count = 0
    property_names: list[str] = []
    abstract_count = 0

    for child in body_node.children():
        kind = child.kind()
        if kind == "function_definition":
            method_count += 1
        elif kind == "decorated_definition":
            method_count += 1
            _inspect_decorated_member(child, property_names)
            abstract_count += _count_abstract_decorator(child)

    return method_count, property_names, abstract_count


def _inspect_decorated_member(
    child: SgNode,
    property_names: list[str],
) -> None:
    """Inspect decorators on a decorated class member for property names.

    Parameters
    ----------
    child
        A decorated_definition node inside a class body.
    property_names
        Accumulator list for property names (mutated in place).
    """
    for dec_child in child.children():
        if dec_child.kind() != "decorator":
            continue
        dec_text = dec_child.text().strip()
        if dec_text.startswith("@"):
            dec_text = dec_text[1:]
        if dec_text == "property":
            inner_func = _unwrap_decorated(child)
            name_node = inner_func.field("name")
            if name_node is not None and len(property_names) < _MAX_PROPERTIES_SHOWN:
                property_names.append(name_node.text())


def _count_abstract_decorator(child: SgNode) -> int:
    """Return 1 if the decorated member has an ``@abstractmethod`` decorator.

    Parameters
    ----------
    child
        A decorated_definition node.

    Returns:
    -------
    int
        1 if abstract, 0 otherwise.
    """
    for dec_child in child.children():
        if dec_child.kind() != "decorator":
            continue
        dec_text = dec_child.text().strip()
        if dec_text.startswith("@"):
            dec_text = dec_text[1:]
        if dec_text == "abstractmethod":
            return 1
    return 0


def extract_class_markers(inner: SgNode) -> list[str]:
    """Extract class markers from the parent decorated_definition.

    Parameters
    ----------
    inner
        A class_definition node.

    Returns:
    -------
    list[str]
        List of marker strings (e.g., ``"dataclass"``, ``"frozen"``).
    """
    markers: list[str] = []
    parent = inner.parent()
    if parent is None or parent.kind() != "decorated_definition":
        return markers
    for dec_child in parent.children():
        if dec_child.kind() != "decorator":
            continue
        dec_text = dec_child.text().strip()
        if dec_text.startswith("@"):
            dec_text = dec_text[1:]
        bare = dec_text.split("(")[0].strip()
        if bare.endswith("dataclass"):
            markers.append("dataclass")
            if "frozen=True" in dec_text:
                markers.append("frozen")
            if "slots=True" in dec_text:
                markers.append("slots")
    return markers


def extract_class_shape(class_node: SgNode) -> dict[str, object]:
    """Extract class API shape summary.

    Parameters
    ----------
    class_node
        A class_definition node.

    Returns:
    -------
    dict[str, object]
        Shape fields: method_count, property_names, abstract_member_count.
    """
    inner = _unwrap_decorated(class_node)
    if inner.kind() != "class_definition":
        return {}

    body_node = inner.field("body")
    if body_node is None:
        return {}

    method_count, property_names, abstract_count = count_methods_and_properties(body_node)

    result: dict[str, object] = {"method_count": method_count}
    if property_names:
        result["property_names"] = property_names
    if abstract_count:
        result["abstract_member_count"] = abstract_count

    class_markers = extract_class_markers(inner)
    if class_markers:
        result["class_markers"] = class_markers

    return result


__all__ = [
    "classify_class_kind",
    "count_methods_and_properties",
    "extract_base_classes",
    "extract_class_markers",
    "extract_class_shape",
    "find_enclosing_class",
]
