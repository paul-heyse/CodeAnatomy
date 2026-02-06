"""Python context enrichment using ast-grep and Python ``ast`` module.

Enrichment Contract
-------------------
All fields produced by this module are strictly additive.
They never affect: confidence scores, match counts, category classification,
or relevance ranking.
They may affect: output detail payload for agent consumption.

Two enrichment tiers:
1. **ast-grep tier** (zero incremental cost - reuses cached ``SgRoot``):
   signature, decorators, item_role, class context, call target, scope chain,
   structural context.
2. **Python ``ast`` tier** (per-file ``ast.parse``, cached):
   generator detection, behavior summary, import normalization.
"""

from __future__ import annotations

import ast
from collections.abc import Callable, Iterator
from hashlib import blake2b
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ast_grep_py import SgNode, SgRoot

# ---------------------------------------------------------------------------
# Cache infrastructure
# ---------------------------------------------------------------------------

_MAX_TREE_CACHE_ENTRIES = 64

# ---------------------------------------------------------------------------
# Payload bounds (mirror Rust pattern)
# ---------------------------------------------------------------------------

_MAX_SIGNATURE_LEN = 200
_MAX_PARAMS = 12
_MAX_RETURN_TYPE_LEN = 100
_MAX_DECORATORS = 8
_MAX_DECORATOR_LEN = 60
_MAX_BASE_CLASSES = 6
_MAX_BASE_CLASS_LEN = 60
_MAX_CALL_TARGET_LEN = 120
_MAX_CALL_RECEIVER_LEN = 80
_MAX_SCOPE_CHAIN = 8
_MAX_IMPORT_NAMES = 12
_MAX_METHODS_SHOWN = 8
_MAX_PROPERTIES_SHOWN = 8

# ---------------------------------------------------------------------------
# Error tuple (fail-open boundary)
# ---------------------------------------------------------------------------

_ENRICHMENT_ERRORS = (RuntimeError, TypeError, ValueError, AttributeError, UnicodeError, SyntaxError)

# ---------------------------------------------------------------------------
# Enrichable node kinds
# ---------------------------------------------------------------------------

_ENRICHABLE_KINDS: frozenset[str] = frozenset({
    "function_definition",
    "decorated_definition",
    "class_definition",
    "call",
    "import_statement",
    "import_from_statement",
    "assignment",
    "augmented_assignment",
    "identifier",
    "attribute",
})

# ---------------------------------------------------------------------------
# Test / role constants
# ---------------------------------------------------------------------------

_TEST_DECORATOR_NAMES: frozenset[str] = frozenset({
    "pytest.fixture",
    "pytest.mark.parametrize",
    "pytest.mark.skipif",
    "pytest.mark.skip",
})

_TEST_FUNCTION_PREFIX = "test_"

# ---------------------------------------------------------------------------
# Structural context mapping
# ---------------------------------------------------------------------------

_STRUCTURAL_CONTEXT_MAP: dict[str, str] = {
    "try_statement": "try_block",
    "except_clause": "except_handler",
    "with_statement": "with_block",
    "for_statement": "for_loop",
    "while_statement": "while_loop",
    "if_statement": "if_block",
    "list_comprehension": "comprehension",
    "dict_comprehension": "comprehension",
    "set_comprehension": "comprehension",
    "generator_expression": "comprehension",
}

_SCOPE_BOUNDARY_NODE_KINDS: frozenset[str] = frozenset({
    "function_definition",
    "class_definition",
    "decorated_definition",
})

# ---------------------------------------------------------------------------
# Python ast scope boundary types
# ---------------------------------------------------------------------------

_AST_SCOPE_BOUNDARY_TYPES: tuple[type, ...] = (
    ast.FunctionDef,
    ast.AsyncFunctionDef,
    ast.ClassDef,
    ast.Lambda,
)

# ---------------------------------------------------------------------------
# Truncation tracking
# ---------------------------------------------------------------------------

_truncation_tracker: list[str] = []

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _truncate(text: str, max_len: int, *, field_name: str = "") -> str:
    """Truncate *text* to *max_len* characters, appending ``...`` when needed.

    Parameters
    ----------
    text
        The string to truncate.
    max_len
        Maximum allowed length.
    field_name
        Optional field name for truncation tracking.

    Returns
    -------
    str
        The original or truncated string.
    """
    if len(text) <= max_len:
        return text
    if field_name:
        _truncation_tracker.append(field_name)
    return text[: max(1, max_len - 3)] + "..."


def _try_extract(
    label: str,
    extractor: Callable[..., dict[str, object]],
    *args: object,
) -> tuple[dict[str, object], str | None]:
    """Call *extractor* with *args*, returning results or a degrade reason.

    Parameters
    ----------
    label
        Human label for the extractor (used in degradation messages).
    extractor
        Callable returning a dict.
    args
        Positional arguments forwarded to extractor.

    Returns
    -------
    tuple[dict[str, object], str | None]
        Extracted fields and an optional degrade reason on failure.
    """
    try:
        result = extractor(*args)
    except _ENRICHMENT_ERRORS as exc:
        return {}, f"{label}: {type(exc).__name__}"
    else:
        return result, None


def _source_hash(source_bytes: bytes) -> str:
    """Compute a fast content hash for cache staleness detection.

    Parameters
    ----------
    source_bytes
        Raw file bytes to hash.

    Returns
    -------
    str
        Hex digest of BLAKE2b-128.
    """
    return blake2b(source_bytes, digest_size=16).hexdigest()


# ---------------------------------------------------------------------------
# Python ast cache (per-file, hash-verified)
# ---------------------------------------------------------------------------

_AST_CACHE: dict[str, tuple[ast.Module, str]] = {}


def _get_ast(source_bytes: bytes, *, cache_key: str) -> ast.Module | None:
    """Get or parse a cached Python AST module.

    Parameters
    ----------
    source_bytes
        Raw source bytes.
    cache_key
        File path for cache keying.

    Returns
    -------
    ast.Module | None
        Parsed AST, or None on syntax error.
    """
    content_hash = _source_hash(source_bytes)
    cached = _AST_CACHE.get(cache_key)
    if cached is not None:
        cached_tree, cached_hash = cached
        if cached_hash == content_hash:
            return cached_tree
    try:
        tree = ast.parse(source_bytes)
    except SyntaxError:
        return None
    if len(_AST_CACHE) >= _MAX_TREE_CACHE_ENTRIES and cache_key not in _AST_CACHE:
        oldest_key = next(iter(_AST_CACHE))
        del _AST_CACHE[oldest_key]
    _AST_CACHE[cache_key] = (tree, content_hash)
    return tree


# ---------------------------------------------------------------------------
# ast-grep tier: node helpers
# ---------------------------------------------------------------------------


def _unwrap_decorated(node: SgNode) -> SgNode:
    """Unwrap a ``decorated_definition`` to its inner definition.

    Parameters
    ----------
    node
        An ast-grep node that may be a ``decorated_definition``.

    Returns
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

    Returns
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

    Returns
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


# ---------------------------------------------------------------------------
# ast-grep tier: Signature extraction (Rec 1)
# ---------------------------------------------------------------------------


def _extract_signature(node: SgNode, _source_bytes: bytes) -> dict[str, object]:
    """Extract function signature details.

    Parameters
    ----------
    node
        A function_definition or decorated_definition node.
    _source_bytes
        Full source bytes (unused, kept for _try_extract compat).

    Returns
    -------
    dict[str, object]
        Signature fields: params, return_type, is_async, signature.
    """
    result: dict[str, object] = {}
    func_node = _unwrap_decorated(node)
    if func_node.kind() != "function_definition":
        return result

    # Parameters
    params_node = func_node.field("parameters")
    if params_node is not None:
        param_list: list[str] = []
        for child in params_node.children():
            if child.is_named() and child.kind() not in {"(", ")", ","}:
                text = child.text().strip()
                if text:
                    param_list.append(text)
                if len(param_list) >= _MAX_PARAMS:
                    break
        result["params"] = param_list

    # Return type
    ret_node = func_node.field("return_type")
    if ret_node is not None:
        ret_text = ret_node.text().strip()
        if ret_text.startswith("->"):
            ret_text = ret_text[2:].strip()
        if ret_text:
            result["return_type"] = _truncate(ret_text, _MAX_RETURN_TYPE_LEN, field_name="return_type")

    # Is async
    func_text = func_node.text()
    result["is_async"] = func_text.lstrip().startswith("async ")

    # Signature (text up to the colon)
    full_text = node.text()
    colon_idx = full_text.find(":")
    if colon_idx > 0:
        # Find the last colon that's part of the def (not inside params/return type)
        # Walk backwards from end looking for the body-starting colon
        sig_text = _extract_sig_text(full_text)
        if sig_text:
            result["signature"] = _truncate(sig_text, _MAX_SIGNATURE_LEN, field_name="signature")

    return result


def _extract_sig_text(full_text: str) -> str:
    """Extract signature text from a function definition.

    Find the colon that starts the body (after params close and optional return type).

    Parameters
    ----------
    full_text
        Full text of the function definition node.

    Returns
    -------
    str
        The signature text up to the colon.
    """
    # Find the first newline or the body colon
    depth = 0
    for i, ch in enumerate(full_text):
        if ch in ("(", "["):
            depth += 1
        elif ch in (")", "]"):
            depth -= 1
        elif ch == ":" and depth == 0 and i > 0:
            return full_text[:i].strip()
    return ""


# ---------------------------------------------------------------------------
# ast-grep tier: Decorator extraction (Rec 2)
# ---------------------------------------------------------------------------


def _extract_decorators(node: SgNode) -> dict[str, object]:
    """Extract decorator list from a decorated definition.

    Parameters
    ----------
    node
        A decorated_definition or function_definition/class_definition node.

    Returns
    -------
    dict[str, object]
        Dict with ``decorators`` key if any decorators found, else empty.
    """
    if node.kind() != "decorated_definition":
        return {}
    decorators: list[str] = []
    for child in node.children():
        if child.kind() == "decorator":
            text = child.text().strip()
            if text.startswith("@"):
                text = text[1:]
            if text:
                decorators.append(_truncate(text, _MAX_DECORATOR_LEN, field_name="decorator"))
            if len(decorators) >= _MAX_DECORATORS:
                break
    if decorators:
        return {"decorators": decorators}
    return {}


# ---------------------------------------------------------------------------
# ast-grep tier: Item role classification (Rec 2)
# ---------------------------------------------------------------------------


def _get_first_param_name(func_node: SgNode) -> str | None:
    """Get the name of the first parameter of a function.

    Parameters
    ----------
    func_node
        A function_definition node.

    Returns
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


def _classify_item_role(node: SgNode, decorators: list[str]) -> dict[str, object]:
    """Classify the semantic role of a node.

    Parameters
    ----------
    node
        The ast-grep node at the match location.
    decorators
        Pre-extracted decorator strings.

    Returns
    -------
    dict[str, object]
        Dict with ``item_role`` key.
    """
    kind = node.kind()

    # Call nodes
    if kind == "call":
        return {"item_role": "callsite"}

    # Import nodes
    if kind == "import_statement":
        return {"item_role": "import"}
    if kind == "import_from_statement":
        return {"item_role": "from_import"}

    # Assignment nodes
    if kind in {"assignment", "augmented_assignment"}:
        return {"item_role": "assignment"}

    # Function definitions
    if kind in {"function_definition", "decorated_definition"} and _is_function_node(node):
        return {"item_role": _classify_function_role(node, decorators)}

    # Class definitions
    if _is_class_node(node):
        return {"item_role": _classify_class_role(decorators)}

    # Identifier / attribute fallback
    if kind in {"identifier", "attribute"}:
        return {"item_role": "reference"}

    return {"item_role": kind}


def _classify_function_role(node: SgNode, decorators: list[str]) -> str:
    """Classify a function definition into a specific role.

    Parameters
    ----------
    node
        A function_definition or decorated_definition node.
    decorators
        Pre-extracted decorator strings.

    Returns
    -------
    str
        A semantic role string.
    """
    decorator_set = frozenset(decorators)
    func_node = _unwrap_decorated(node)

    # Decorator-based roles
    if "classmethod" in decorator_set:
        return "classmethod"
    if "staticmethod" in decorator_set:
        return "staticmethod"
    if "property" in decorator_set:
        return "property_getter"
    if any(d.endswith(".setter") for d in decorators):
        return "property_setter"
    if "abstractmethod" in decorator_set:
        return "abstractmethod"
    if "pytest.fixture" in decorator_set:
        return "fixture"

    # Test function detection
    name_node = func_node.field("name")
    func_name = name_node.text() if name_node is not None else ""
    if func_name.startswith(_TEST_FUNCTION_PREFIX):
        return "test_function"
    if decorator_set & _TEST_DECORATOR_NAMES:
        return "test_function"

    # Class method detection via parent chain
    parent = node.parent()
    while parent is not None:
        parent_kind = parent.kind()
        if parent_kind == "class_definition":
            first_param = _get_first_param_name(func_node)
            if first_param in {"self", "cls"}:
                return "method"
            return "static_or_classmethod"
        if parent_kind == "decorated_definition":
            inner = _unwrap_decorated(parent)
            if inner.kind() == "class_definition":
                first_param = _get_first_param_name(func_node)
                if first_param in {"self", "cls"}:
                    return "method"
                return "static_or_classmethod"
        if parent_kind in {"function_definition", "module"}:
            break
        parent = parent.parent()

    return "free_function"


def _classify_class_role(decorators: list[str]) -> str:
    """Classify a class definition into a specific role.

    Parameters
    ----------
    decorators
        Pre-extracted decorator strings.

    Returns
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


# ---------------------------------------------------------------------------
# ast-grep tier: Class context extraction (Rec 3)
# ---------------------------------------------------------------------------

_ENUM_BASE_NAMES: frozenset[str] = frozenset({
    "Enum", "IntEnum", "StrEnum", "Flag", "IntFlag",
})

_PROTOCOL_BASE_NAMES: frozenset[str] = frozenset({
    "Protocol", "ABC", "ABCMeta",
})


def _extract_class_context(node: SgNode) -> dict[str, object]:
    """Extract class context for a node inside a class.

    Parameters
    ----------
    node
        An ast-grep node.

    Returns
    -------
    dict[str, object]
        Class context fields, or empty dict if not inside a class.
    """
    class_node = _find_enclosing_class(node)
    if class_node is None:
        return {}

    result: dict[str, object] = {}

    # Class name
    name_node = class_node.field("name")
    if name_node is not None:
        result["class_name"] = name_node.text()

    # Base classes
    bases = _extract_base_classes(class_node)
    if bases:
        result["base_classes"] = bases

    # Class kind
    result["class_kind"] = _classify_class_kind(class_node, bases)

    return result


def _find_enclosing_class(node: SgNode) -> SgNode | None:
    """Walk parent chain to find enclosing class_definition.

    Parameters
    ----------
    node
        Starting ast-grep node.

    Returns
    -------
    SgNode | None
        The enclosing class_definition node, or None.
    """
    current = node.parent()
    depth = 0
    while current is not None and depth < 20:
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


def _extract_base_classes(class_node: SgNode) -> list[str]:
    """Extract base class names from a class definition.

    Parameters
    ----------
    class_node
        A class_definition node.

    Returns
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
                bases.append(
                    _truncate(text, _MAX_BASE_CLASS_LEN, field_name="base_class")
                )
            if len(bases) >= _MAX_BASE_CLASSES:
                break
    return bases


def _classify_class_kind(class_node: SgNode, bases: list[str]) -> str:
    """Classify a class into a kind category.

    Parameters
    ----------
    class_node
        A class_definition node.
    bases
        Pre-extracted base class names.

    Returns
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
# ast-grep tier: Call target extraction (Rec 4)
# ---------------------------------------------------------------------------


def _extract_call_target(node: SgNode) -> dict[str, object]:
    """Extract call target information from a call node.

    Parameters
    ----------
    node
        A call node.

    Returns
    -------
    dict[str, object]
        Call target fields.
    """
    if node.kind() != "call":
        return {}

    result: dict[str, object] = {}
    func_node = node.field("function")
    if func_node is None:
        return result

    func_kind = func_node.kind()

    if func_kind == "attribute":
        # Method call: receiver.method(...)
        obj_node = func_node.field("object")
        attr_node = func_node.field("attribute")
        receiver = obj_node.text().strip() if obj_node is not None else ""
        method = attr_node.text().strip() if attr_node is not None else ""
        if receiver and method:
            result["call_target"] = _truncate(
                f"{receiver}.{method}", _MAX_CALL_TARGET_LEN, field_name="call_target"
            )
            result["call_receiver"] = _truncate(
                receiver, _MAX_CALL_RECEIVER_LEN, field_name="call_receiver"
            )
            result["call_method"] = method
        elif method:
            result["call_target"] = method
    elif func_kind == "identifier":
        result["call_target"] = _truncate(
            func_node.text().strip(), _MAX_CALL_TARGET_LEN, field_name="call_target"
        )
    else:
        # Complex expression (e.g., subscript call)
        text = func_node.text().strip()
        if text:
            result["call_target"] = _truncate(
                text, _MAX_CALL_TARGET_LEN, field_name="call_target"
            )

    # Count arguments
    args_node = node.field("arguments")
    if args_node is not None:
        arg_count = sum(
            1 for child in args_node.children()
            if child.is_named() and child.kind() not in {"(", ")", ","}
        )
        result["call_args_count"] = arg_count

    return result


# ---------------------------------------------------------------------------
# ast-grep tier: Scope chain (Rec 5)
# ---------------------------------------------------------------------------


def _extract_scope_chain(node: SgNode) -> dict[str, object]:
    """Extract the scope chain from module down to the node.

    Parameters
    ----------
    node
        An ast-grep node.

    Returns
    -------
    dict[str, object]
        Dict with ``scope_chain`` key.
    """
    chain: list[str] = []
    current = node.parent()
    depth = 0
    while current is not None and depth < 20:
        kind = current.kind()
        if kind == "function_definition":
            name_node = current.field("name")
            if name_node is not None:
                chain.append(name_node.text())
        elif kind == "class_definition":
            name_node = current.field("name")
            if name_node is not None:
                chain.append(name_node.text())
        elif kind == "decorated_definition":
            inner = _unwrap_decorated(current)
            inner_kind = inner.kind()
            if inner_kind in {"function_definition", "class_definition"}:
                name_node = inner.field("name")
                if name_node is not None:
                    chain.append(name_node.text())
        depth += 1
        current = current.parent()

    chain.reverse()
    chain.insert(0, "module")

    return {"scope_chain": chain[:_MAX_SCOPE_CHAIN]}


# ---------------------------------------------------------------------------
# ast-grep tier: Structural context (Rec 5)
# ---------------------------------------------------------------------------


def _extract_structural_context(node: SgNode) -> dict[str, object]:
    """Extract the nearest structural context for a node.

    Parameters
    ----------
    node
        An ast-grep node.

    Returns
    -------
    dict[str, object]
        Dict with ``structural_context`` key if inside a structural block,
        else empty dict.
    """
    current = node.parent()
    depth = 0
    while current is not None and depth < 20:
        kind = current.kind()
        # Stop at function/class boundaries
        if kind in _SCOPE_BOUNDARY_NODE_KINDS:
            break
        ctx = _STRUCTURAL_CONTEXT_MAP.get(kind)
        if ctx is not None:
            return {"structural_context": ctx}
        depth += 1
        current = current.parent()
    return {}


# ---------------------------------------------------------------------------
# Python ast tier: Scope-safe walker (Rec 7, C6)
# ---------------------------------------------------------------------------


def _walk_function_body(
    func_node: ast.FunctionDef | ast.AsyncFunctionDef,
) -> Iterator[ast.AST]:
    """Walk function body nodes, skipping nested scopes.

    Parameters
    ----------
    func_node
        A function definition AST node.

    Yields
    ------
    ast.AST
        Body nodes excluding nested function/class/lambda definitions.
    """
    stack: list[ast.AST] = list(reversed(func_node.body))
    while stack:
        current = stack.pop()
        yield current
        for child in ast.iter_child_nodes(current):
            if not isinstance(child, _AST_SCOPE_BOUNDARY_TYPES):
                stack.append(child)


# ---------------------------------------------------------------------------
# Python ast tier: Function behavior summary (Rec 7)
# ---------------------------------------------------------------------------


def _extract_behavior_summary(
    func_ast: ast.FunctionDef | ast.AsyncFunctionDef,
) -> dict[str, object]:
    """Extract behavioral flags from a function body.

    Parameters
    ----------
    func_ast
        A function definition AST node.

    Returns
    -------
    dict[str, object]
        Behavior flags: returns_value, raises_exception, yields, awaits,
        has_context_manager.
    """
    returns_value = False
    raises_exception = False
    yields = False
    awaits = False
    has_context_manager = False

    for child in _walk_function_body(func_ast):
        if isinstance(child, ast.Return) and child.value is not None:
            returns_value = True
        elif isinstance(child, ast.Raise):
            raises_exception = True
        elif isinstance(child, (ast.Yield, ast.YieldFrom)):
            yields = True
        elif isinstance(child, ast.Await):
            awaits = True
        elif isinstance(child, (ast.With, ast.AsyncWith)):
            has_context_manager = True

    result: dict[str, object] = {}
    if returns_value:
        result["returns_value"] = True
    if raises_exception:
        result["raises_exception"] = True
    if yields:
        result["yields"] = True
    if awaits:
        result["awaits"] = True
    if has_context_manager:
        result["has_context_manager"] = True
    return result


# ---------------------------------------------------------------------------
# Python ast tier: Generator detection (C6)
# ---------------------------------------------------------------------------


def _extract_generator_flag(
    func_ast: ast.FunctionDef | ast.AsyncFunctionDef,
) -> dict[str, object]:
    """Detect whether a function is a generator (scope-safe).

    Parameters
    ----------
    func_ast
        A function definition AST node.

    Returns
    -------
    dict[str, object]
        Dict with ``is_generator`` key.
    """
    for child in _walk_function_body(func_ast):
        if isinstance(child, (ast.Yield, ast.YieldFrom)):
            return {"is_generator": True}
    return {"is_generator": False}


# ---------------------------------------------------------------------------
# Python ast tier: AST function finder
# ---------------------------------------------------------------------------


def _find_ast_function(
    tree: ast.Module,
    line: int,
) -> ast.FunctionDef | ast.AsyncFunctionDef | None:
    """Find a function definition starting on the given line.

    Parameters
    ----------
    tree
        Parsed AST module.
    line
        1-indexed line number.

    Returns
    -------
    ast.FunctionDef | ast.AsyncFunctionDef | None
        The matching function node, or None.
    """
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if node.lineno == line:
                return node
            # Check for decorated functions (decorator may be on a different line)
            if hasattr(node, "decorator_list") and node.decorator_list:
                first_dec_line = node.decorator_list[0].lineno
                if first_dec_line <= line <= node.lineno:
                    return node
    return None


# ---------------------------------------------------------------------------
# Python ast tier: Import detail extraction (Rec 6, C8)
# ---------------------------------------------------------------------------


def _find_ast_import(
    tree: ast.Module,
    line: int,
) -> ast.Import | ast.ImportFrom | None:
    """Find an import statement on the given line.

    Parameters
    ----------
    tree
        Parsed AST module.
    line
        1-indexed line number.

    Returns
    -------
    ast.Import | ast.ImportFrom | None
        The matching import node, or None.
    """
    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            if node.lineno == line:
                return node
    return None


def _is_type_checking_import(tree: ast.Module, import_node: ast.Import | ast.ImportFrom) -> bool:
    """Detect whether an import is inside a TYPE_CHECKING guard.

    Parameters
    ----------
    tree
        Parsed AST module.
    import_node
        The import node to check.

    Returns
    -------
    bool
        True if inside an ``if TYPE_CHECKING:`` block.
    """
    for node in ast.walk(tree):
        if isinstance(node, ast.If):
            # Check if test is TYPE_CHECKING
            test = node.test
            is_tc = False
            if isinstance(test, ast.Name) and test.id == "TYPE_CHECKING":
                is_tc = True
            elif isinstance(test, ast.Attribute) and test.attr == "TYPE_CHECKING":
                is_tc = True

            if is_tc:
                # Check if import_node is in the body
                for child in ast.walk(node):
                    if child is import_node:
                        return True
    return False


def _extract_import_detail(
    node: SgNode,
    source_bytes: bytes,
    *,
    cache_key: str,
    line: int,
) -> dict[str, object]:
    """Extract normalized import details using Python ``ast``.

    Parameters
    ----------
    node
        An import_statement or import_from_statement ast-grep node.
    source_bytes
        Full source bytes.
    cache_key
        File path for AST cache keying.
    line
        1-indexed line number.

    Returns
    -------
    dict[str, object]
        Import detail fields.
    """
    result: dict[str, object] = {}
    ast_tree = _get_ast(source_bytes, cache_key=cache_key)
    if ast_tree is None:
        return result

    import_node = _find_ast_import(ast_tree, line)
    if import_node is None:
        return result

    if isinstance(import_node, ast.Import):
        if import_node.names:
            result["import_module"] = import_node.names[0].name
            if import_node.names[0].asname:
                result["import_alias"] = import_node.names[0].asname
        result["import_level"] = 0

    elif isinstance(import_node, ast.ImportFrom):
        if import_node.module:
            result["import_module"] = import_node.module
        names = [alias.name for alias in import_node.names[:_MAX_IMPORT_NAMES]]
        result["import_names"] = names
        if len(import_node.names) == 1 and import_node.names[0].asname:
            result["import_alias"] = import_node.names[0].asname
        result["import_level"] = import_node.level or 0

    # TYPE_CHECKING detection
    if _is_type_checking_import(ast_tree, import_node):
        result["is_type_import"] = True

    return result


# ---------------------------------------------------------------------------
# ast-grep tier: Class API shape summary (Rec 9)
# ---------------------------------------------------------------------------


def _extract_class_shape(class_node: SgNode) -> dict[str, object]:
    """Extract class API shape summary.

    Parameters
    ----------
    class_node
        A class_definition node.

    Returns
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

    result: dict[str, object] = {}
    method_count = 0
    property_names: list[str] = []
    abstract_count = 0
    class_markers: list[str] = []

    for child in body_node.children():
        kind = child.kind()
        if kind == "function_definition":
            method_count += 1
        elif kind == "decorated_definition":
            method_count += 1
            # Check decorators
            for dec_child in child.children():
                if dec_child.kind() == "decorator":
                    dec_text = dec_child.text().strip()
                    if dec_text.startswith("@"):
                        dec_text = dec_text[1:]
                    if dec_text == "property":
                        inner_func = _unwrap_decorated(child)
                        name_node = inner_func.field("name")
                        if name_node is not None:
                            prop_name = name_node.text()
                            if len(property_names) < _MAX_PROPERTIES_SHOWN:
                                property_names.append(prop_name)
                    if dec_text == "abstractmethod":
                        abstract_count += 1

    result["method_count"] = method_count
    if property_names:
        result["property_names"] = property_names
    if abstract_count:
        result["abstract_member_count"] = abstract_count

    # Class markers from parent decorated_definition
    parent = inner.parent()
    if parent is not None and parent.kind() == "decorated_definition":
        for dec_child in parent.children():
            if dec_child.kind() == "decorator":
                dec_text = dec_child.text().strip()
                if dec_text.startswith("@"):
                    dec_text = dec_text[1:]
                bare = dec_text.split("(")[0].strip()
                if bare.endswith("dataclass"):
                    class_markers.append("dataclass")
                    # Extract frozen/slots args
                    if "frozen=True" in dec_text:
                        class_markers.append("frozen")
                    if "slots=True" in dec_text:
                        class_markers.append("slots")

    if class_markers:
        result["class_markers"] = class_markers

    return result


# ---------------------------------------------------------------------------
# Public entrypoints
# ---------------------------------------------------------------------------


def enrich_python_context(
    sg_root: SgRoot,
    node: SgNode,
    source_bytes: bytes,
    *,
    line: int,
    col: int,
    cache_key: str,
) -> dict[str, object] | None:
    """Enrich a Python match with structured context fields.

    Parameters
    ----------
    sg_root
        Parsed ast-grep root for the file.
    node
        Resolved SgNode at the match position.
    source_bytes
        Raw source bytes (for Python ast tier).
    line
        1-indexed line number.
    col
        0-indexed column offset.
    cache_key
        File path for cache keying.

    Returns
    -------
    dict[str, object] | None
        Enrichment payload, or None if enrichment not applicable.
    """
    _ = sg_root  # Available for future byte-range lookups
    _ = col  # Available for future column-based enrichment

    node_kind = node.kind()
    if node_kind not in _ENRICHABLE_KINDS:
        return None

    _truncation_tracker.clear()

    payload: dict[str, object] = {
        "enrichment_status": "applied",
        "enrichment_sources": ["ast_grep"],
    }
    degrade_reasons: list[str] = []

    # Core identity
    payload["node_kind"] = node_kind

    # Decorators (before item_role, since role depends on decorators)
    dec_result, dec_reason = _try_extract("decorators", _extract_decorators, node)
    payload.update(dec_result)
    if dec_reason:
        degrade_reasons.append(dec_reason)

    decorators: list[str] = dec_result.get("decorators", [])  # type: ignore[assignment]

    # Item role
    role_result, role_reason = _try_extract("item_role", _classify_item_role, node, decorators)
    payload.update(role_result)
    if role_reason:
        degrade_reasons.append(role_reason)

    # Signature (for function definitions)
    if _is_function_node(node):
        sig_result, sig_reason = _try_extract("signature", _extract_signature, node, source_bytes)
        payload.update(sig_result)
        if sig_reason:
            degrade_reasons.append(sig_reason)

    # Class context (for nodes inside classes)
    cls_result, cls_reason = _try_extract("class_context", _extract_class_context, node)
    payload.update(cls_result)
    if cls_reason:
        degrade_reasons.append(cls_reason)

    # Call target (for call nodes)
    if node_kind == "call":
        call_result, call_reason = _try_extract("call_target", _extract_call_target, node)
        payload.update(call_result)
        if call_reason:
            degrade_reasons.append(call_reason)

    # Scope chain
    chain_result, chain_reason = _try_extract("scope_chain", _extract_scope_chain, node)
    payload.update(chain_result)
    if chain_reason:
        degrade_reasons.append(chain_reason)

    # Structural context
    ctx_result, ctx_reason = _try_extract("structural_context", _extract_structural_context, node)
    payload.update(ctx_result)
    if ctx_reason:
        degrade_reasons.append(ctx_reason)

    # Class shape (for class definitions)
    if _is_class_node(node):
        shape_result, shape_reason = _try_extract("class_shape", _extract_class_shape, node)
        payload.update(shape_result)
        if shape_reason:
            degrade_reasons.append(shape_reason)

    # --- Python ast tier (scope-safe semantics) ---

    if _is_function_node(node):
        func_node = _unwrap_decorated(node)
        func_line = func_node.range().start.line + 1  # 0-indexed to 1-indexed
        ast_tree = _get_ast(source_bytes, cache_key=cache_key)
        if ast_tree is not None:
            func_ast = _find_ast_function(ast_tree, func_line)
            if func_ast is not None:
                # Scope-safe generator detection
                gen_result, gen_reason = _try_extract("generator", _extract_generator_flag, func_ast)
                if gen_result:
                    payload["is_generator"] = gen_result.get("is_generator", False)
                if gen_reason:
                    degrade_reasons.append(gen_reason)

                # Behavior summary
                beh_result, beh_reason = _try_extract(
                    "behavior", _extract_behavior_summary, func_ast
                )
                payload.update(beh_result)
                if beh_reason:
                    degrade_reasons.append(beh_reason)

            sources = list(payload.get("enrichment_sources", []))
            if "python_ast" not in sources:
                sources.append("python_ast")
                payload["enrichment_sources"] = sources

    # Import detail (for import nodes)
    if node_kind in {"import_statement", "import_from_statement"}:
        imp_result, imp_reason = _try_extract(
            "import",
            _extract_import_detail,
            node,
            source_bytes,
            cache_key=cache_key,
            line=line,
        )
        payload.update(imp_result)
        if imp_reason:
            degrade_reasons.append(imp_reason)
        sources = list(payload.get("enrichment_sources", []))
        if "python_ast" not in sources:
            sources.append("python_ast")
            payload["enrichment_sources"] = sources

    # Degradation metadata
    if degrade_reasons:
        payload["enrichment_status"] = "degraded"
        payload["degrade_reason"] = "; ".join(degrade_reasons)

    # Truncation metadata (O6)
    if _truncation_tracker:
        payload["truncated_fields"] = list(_truncation_tracker)
        _truncation_tracker.clear()

    return payload


def enrich_python_context_by_byte_range(
    sg_root: SgRoot,
    source_bytes: bytes,
    byte_start: int,
    byte_end: int,
    *,
    cache_key: str,
) -> dict[str, object] | None:
    """Enrich using byte-range anchor (preferred for ripgrep integration).

    Parameters
    ----------
    sg_root
        Parsed ast-grep root for the file.
    source_bytes
        Raw source bytes.
    byte_start
        0-based byte offset of the match start.
    byte_end
        0-based byte offset of the match end (exclusive).
    cache_key
        File path for cache keying.

    Returns
    -------
    dict[str, object] | None
        Enrichment payload, or None if not applicable.
    """
    _ = sg_root, source_bytes, byte_start, byte_end, cache_key
    return None


def clear_python_enrichment_cache() -> None:
    """Clear per-process Python enrichment caches."""
    _AST_CACHE.clear()
    _truncation_tracker.clear()


__all__ = [
    "clear_python_enrichment_cache",
    "enrich_python_context",
    "enrich_python_context_by_byte_range",
]
