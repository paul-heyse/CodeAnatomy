"""Python role classification functions for enrichment pipeline.

This module provides classification functions used by the enrichment pipeline
to categorize code elements by their semantic role (e.g., method, property,
test function, dataclass).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ast_grep_py import SgNode

# ---------------------------------------------------------------------------
# Test / role constants
# ---------------------------------------------------------------------------

_TEST_DECORATOR_NAMES: frozenset[str] = frozenset(
    {
        "pytest.fixture",
        "pytest.mark.parametrize",
        "pytest.mark.skipif",
        "pytest.mark.skip",
    }
)

_TEST_FUNCTION_PREFIX = "test_"

# ---------------------------------------------------------------------------
# Dispatch tables
# ---------------------------------------------------------------------------

# Dispatch table for simple node-kind to item-role mappings.
_NODE_KIND_ROLE_MAP: dict[str, str] = {
    "call": "callsite",
    "import_statement": "import",
    "import_from_statement": "from_import",
    "assignment": "assignment",
    "augmented_assignment": "assignment",
    "identifier": "reference",
    "attribute": "reference",
}

# Decorator-based role dispatch table: maps decorator name to role string.
_DECORATOR_ROLE_MAP: dict[str, str] = {
    "classmethod": "classmethod",
    "staticmethod": "staticmethod",
    "property": "property_getter",
    "abstractmethod": "abstractmethod",
    "pytest.fixture": "fixture",
}

# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


def _unwrap_decorated(node: SgNode) -> SgNode:
    """Unwrap a ``decorated_definition`` to its inner definition.

    Parameters
    ----------
    node
        An ast-grep node that may be a ``decorated_definition``.

    Returns:
    -------
    SgNode
        The inner definition node, or the original node if not decorated.
    """
    if node.kind() != "decorated_definition":
        return node
    for child in node.children():
        kind = child.kind()
        if kind in {"function_definition", "class_definition"}:
            return child
    return node


def _is_class_node(node: SgNode) -> bool:
    """Check whether a node represents a class definition.

    Parameters
    ----------
    node
        An ast-grep node.

    Returns:
    -------
    bool
        True if the node is or wraps a class definition.
    """
    if node.kind() == "class_definition":
        return True
    if node.kind() == "decorated_definition":
        inner = _unwrap_decorated(node)
        return inner.kind() == "class_definition"
    return False


def _is_function_node(node: SgNode) -> bool:
    """Check whether a node represents a function definition.

    Parameters
    ----------
    node
        An ast-grep node.

    Returns:
    -------
    bool
        True if the node is or wraps a function definition.
    """
    if node.kind() == "function_definition":
        return True
    if node.kind() == "decorated_definition":
        inner = _unwrap_decorated(node)
        return inner.kind() == "function_definition"
    return False


def _get_first_param_name(func_node: SgNode) -> str | None:
    """Get the name of the first parameter of a function.

    Parameters
    ----------
    func_node
        A function_definition node.

    Returns:
    -------
    str | None
        The first parameter name, or None.
    """
    params_node = func_node.field("parameters")
    if params_node is None:
        return None
    for child in params_node.children():
        if child.is_named() and child.kind() not in {"(", ")", ","}:
            text = child.text().strip()
            # Handle typed parameters: "self: Type" or just "self"
            if ":" in text:
                text = text.split(":")[0].strip()
            if "/" in text:
                continue  # Skip positional-only separator
            if "*" in text:
                continue  # Skip *args / **kwargs / * separator
            return text
    return None


# ---------------------------------------------------------------------------
# Classification functions
# ---------------------------------------------------------------------------


def classify_function_role_by_decorator(decorators: list[str]) -> str | None:
    """Classify a function role based on its decorators.

    Parameters
    ----------
    decorators
        Pre-extracted decorator strings.

    Returns:
    -------
    str | None
        A role string if a decorator matches, or None.
    """
    decorator_set = frozenset(decorators)
    for dec_name, role in _DECORATOR_ROLE_MAP.items():
        if dec_name in decorator_set:
            return role
    if any(d.endswith(".setter") for d in decorators):
        return "property_setter"
    return None


def classify_function_role_by_name(
    func_node: SgNode,
    decorators: list[str],
) -> str | None:
    """Classify a function role based on its name (test detection).

    Parameters
    ----------
    func_node
        The unwrapped function_definition node.
    decorators
        Pre-extracted decorator strings.

    Returns:
    -------
    str | None
        ``"test_function"`` if detected, else None.
    """
    name_node = func_node.field("name")
    func_name = name_node.text() if name_node is not None else ""
    if func_name.startswith(_TEST_FUNCTION_PREFIX):
        return "test_function"
    if frozenset(decorators) & _TEST_DECORATOR_NAMES:
        return "test_function"
    return None


def method_role_by_first_param(func_node: SgNode) -> str:
    """Determine method role from first parameter name.

    Parameters
    ----------
    func_node
        A function_definition node.

    Returns:
    -------
    str
        ``"method"`` if first param is self/cls, else ``"static_or_classmethod"``.
    """
    first_param = _get_first_param_name(func_node)
    if first_param in {"self", "cls"}:
        return "method"
    return "static_or_classmethod"


def classify_function_role_by_parent(
    node: SgNode,
    func_node: SgNode,
) -> str:
    """Classify a function role based on its parent chain (method detection).

    Parameters
    ----------
    node
        The original node (may be decorated_definition).
    func_node
        The unwrapped function_definition node.

    Returns:
    -------
    str
        A role string such as ``"method"``, ``"static_or_classmethod"``,
        or ``"free_function"``.
    """
    if not node.inside(kind="class_definition"):
        return "free_function"

    parent = node.parent()
    while parent is not None:
        parent_kind = parent.kind()
        if parent_kind == "class_definition":
            return method_role_by_first_param(func_node)
        if parent_kind == "decorated_definition":
            inner = _unwrap_decorated(parent)
            if inner.kind() == "class_definition":
                return method_role_by_first_param(func_node)
        if parent_kind in {"function_definition", "module"}:
            break
        parent = parent.parent()
    return "free_function"


def classify_function_role(node: SgNode, decorators: list[str]) -> str:
    """Classify a function definition into a specific role.

    Parameters
    ----------
    node
        A function_definition or decorated_definition node.
    decorators
        Pre-extracted decorator strings.

    Returns:
    -------
    str
        A semantic role string.
    """
    func_node = _unwrap_decorated(node)

    # Decorator-based roles
    dec_role = classify_function_role_by_decorator(decorators)
    if dec_role is not None:
        return dec_role

    # Test function detection
    name_role = classify_function_role_by_name(func_node, decorators)
    if name_role is not None:
        return name_role

    # Class method detection via parent chain
    return classify_function_role_by_parent(node, func_node)


def classify_class_role(decorators: list[str]) -> str:
    """Classify a class definition into a specific role.

    Parameters
    ----------
    decorators
        Pre-extracted decorator strings.

    Returns:
    -------
    str
        One of ``"dataclass"`` or ``"class_def"``.
    """
    for dec in decorators:
        # Match "dataclass", "dataclass(...)", "dataclasses.dataclass", etc.
        bare = dec.split("(")[0].strip()
        if bare.endswith("dataclass"):
            return "dataclass"
    return "class_def"


def classify_item_role(node: SgNode, decorators: list[str]) -> dict[str, object]:
    """Classify the semantic role of a node.

    Parameters
    ----------
    node
        The ast-grep node at the match location.
    decorators
        Pre-extracted decorator strings.

    Returns:
    -------
    dict[str, object]
        Dict with ``item_role`` key.
    """
    kind = node.kind()

    # Fast-path: direct kind-to-role lookup
    direct_role = _NODE_KIND_ROLE_MAP.get(kind)
    if direct_role is not None:
        return {"item_role": direct_role}

    # Function definitions
    if kind in {"function_definition", "decorated_definition"} and _is_function_node(node):
        return {"item_role": classify_function_role(node, decorators)}

    # Class definitions
    if _is_class_node(node):
        return {"item_role": classify_class_role(decorators)}

    return {"item_role": kind}


__all__ = [
    "classify_class_role",
    "classify_function_role",
    "classify_function_role_by_decorator",
    "classify_function_role_by_name",
    "classify_function_role_by_parent",
    "classify_item_role",
    "method_role_by_first_param",
]
