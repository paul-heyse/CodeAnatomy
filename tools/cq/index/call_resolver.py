"""Call target resolution for linking call sites to definitions."""

from __future__ import annotations

import ast
from dataclasses import dataclass

from tools.cq.index.def_index import DefIndex, FnDecl

_SELF_CLS: set[str] = {"self", "cls"}
_CALL_SPLIT_PARTS = 2


@dataclass
class CallInfo:
    """Information about a call site.

    Parameters
    ----------
    file : str
        File containing the call.
    line : int
        Line number.
    col : int
        Column offset.
    callee_name : str
        Name of called function/method (possibly qualified).
    args : list[ast.expr]
        Positional arguments.
    keywords : list[ast.keyword]
        Keyword arguments.
    is_method_call : bool
        Whether this is obj.method() style.
    receiver_name : str | None
        Name of receiver (self, cls, or variable) if method call.
    """

    file: str
    line: int
    col: int
    callee_name: str
    args: list[ast.expr]
    keywords: list[ast.keyword]
    is_method_call: bool = False
    receiver_name: str | None = None


@dataclass
class ResolvedCall:
    """A call site resolved to its target definition(s).

    Parameters
    ----------
    call : CallInfo
        The call site.
    targets : list[FnDecl]
        Possible target definitions.
    confidence : str
        Resolution confidence: "exact", "likely", "ambiguous", "unresolved".
    resolution_path : str
        How resolution was performed.
    """

    call: CallInfo
    targets: list[FnDecl]
    confidence: str
    resolution_path: str


def _safe_unparse(node: ast.AST, *, default: str) -> str:
    try:
        return ast.unparse(node)
    except (ValueError, TypeError):
        return default


def _method_name(callee: str) -> str:
    return callee.rsplit(".", maxsplit=1)[-1] if "." in callee else callee


def _confidence_exact_ambiguous(targets: list[FnDecl]) -> str:
    if len(targets) == 1:
        return "exact"
    if targets:
        return "ambiguous"
    return "unresolved"


def _confidence_exact_likely(targets: list[FnDecl]) -> str:
    if len(targets) == 1:
        return "exact"
    if targets:
        return "likely"
    return "unresolved"


def _get_call_name(node: ast.Call) -> tuple[str, bool, str | None]:
    """Extract call name and determine if method call.

    Returns:
    -------
    tuple[str, bool, str | None]
        (callee_name, is_method_call, receiver_name).
    """
    func = node.func

    if isinstance(func, ast.Name):
        return (func.id, False, None)

    if isinstance(func, ast.Attribute):
        receiver = func.value
        method = func.attr
        if isinstance(receiver, ast.Name):
            receiver_name = receiver.id
            if receiver_name in _SELF_CLS:
                return (method, True, receiver_name)
            return (f"{receiver_name}.{method}", True, receiver_name)

        full = _safe_unparse(func, default=method)
        parts = full.rsplit(".", 1)
        if len(parts) == _CALL_SPLIT_PARTS:
            return (parts[1], True, parts[0])
        return (full, True, None)

    callee = _safe_unparse(func, default="<unknown>")
    return (callee, False, None)


def extract_calls_from_file(file: str, source: str) -> list[CallInfo]:
    """Extract all call sites from a Python file.

    Parameters
    ----------
    file : str
        Relative file path.
    source : str
        File contents.

    Returns:
    -------
    list[CallInfo]
        Extracted call sites.
    """
    calls: list[CallInfo] = []

    try:
        tree = ast.parse(source, filename=file)
    except SyntaxError:
        return calls

    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            callee_name, is_method, receiver = _get_call_name(node)
            calls.append(
                CallInfo(
                    file=file,
                    line=node.lineno,
                    col=node.col_offset,
                    callee_name=callee_name,
                    args=node.args,
                    keywords=node.keywords,
                    is_method_call=is_method,
                    receiver_name=receiver,
                )
            )

    return calls


def _resolve_simple_call(
    index: DefIndex,
    call: CallInfo,
) -> tuple[list[FnDecl], str, str]:
    mod, sym = index.resolve_import_alias(call.file, call.callee_name)
    if sym:
        targets = index.find_function_by_name(sym)
        resolution_path = f"import:{mod}.{sym}"
    else:
        targets = index.find_function_by_name(call.callee_name)
        resolution_path = f"direct:{call.callee_name}"
    confidence = _confidence_exact_ambiguous(targets)
    return targets, confidence, resolution_path


def _resolve_self_or_cls_call(
    index: DefIndex,
    call: CallInfo,
    *,
    receiver: str,
) -> tuple[list[FnDecl], str, str]:
    mod_info = index.get_module_for_file(call.file)
    if mod_info is None:
        targets: list[FnDecl] = []
    else:
        target_method = _method_name(call.callee_name)
        targets = [
            method
            for cls in mod_info.classes
            for method in cls.methods
            if method.name == target_method
        ]
    confidence = _confidence_exact_likely(targets)
    return targets, confidence, f"{receiver}:{call.callee_name}"


def _resolve_qualified_call(
    index: DefIndex,
    call: CallInfo,
) -> tuple[list[FnDecl], str, str] | None:
    if "." not in call.callee_name:
        return None
    prefix, name = call.callee_name.split(".", 1)
    targets = [
        method
        for cls in index.find_class_by_name(prefix)
        for method in cls.methods
        if method.name == name
    ]
    if targets:
        confidence = _confidence_exact_ambiguous(targets)
        return targets, confidence, f"class:{prefix}.{name}"
    mod, sym = index.resolve_import_alias(call.file, call.callee_name)
    if sym:
        targets = index.find_function_by_name(sym.split(".")[-1])
        confidence = _confidence_exact_likely(targets)
        return targets, confidence, f"module:{mod}.{sym}"
    return None


def _resolve_typed_receiver(
    index: DefIndex,
    call: CallInfo,
    *,
    var_types: dict[str, str],
) -> tuple[list[FnDecl], str, str] | None:
    receiver = call.receiver_name
    if not receiver:
        return None
    type_name = var_types.get(receiver)
    if type_name is None:
        return None
    method_name = _method_name(call.callee_name)
    targets = [
        method
        for cls in index.find_class_by_name(type_name)
        for method in cls.methods
        if method.name == method_name
    ]
    confidence = _confidence_exact_likely(targets)
    if confidence == "unresolved":
        return None
    return targets, confidence, f"typed:{type_name}.{method_name}"


def resolve_call_targets(
    index: DefIndex,
    call: CallInfo,
    var_types: dict[str, str] | None = None,
) -> ResolvedCall:
    """Resolve a call to its target definition(s).

    Parameters
    ----------
    index : DefIndex
        Definition index.
    call : CallInfo
        Call site to resolve.
    var_types : dict[str, str] | None
        Known variable types for receiver resolution.

    Returns:
    -------
    ResolvedCall
        Resolution result.
    """
    resolved_types = var_types or {}
    if not call.is_method_call:
        targets, confidence, resolution_path = _resolve_simple_call(index, call)
        return ResolvedCall(
            call=call,
            targets=targets,
            confidence=confidence,
            resolution_path=resolution_path,
        )
    receiver = call.receiver_name
    if receiver in _SELF_CLS:
        targets, confidence, resolution_path = _resolve_self_or_cls_call(
            index,
            call,
            receiver=receiver or "self",
        )
        return ResolvedCall(
            call=call,
            targets=targets,
            confidence=confidence,
            resolution_path=resolution_path,
        )
    qualified = _resolve_qualified_call(index, call)
    if qualified is not None:
        targets, confidence, resolution_path = qualified
        return ResolvedCall(
            call=call,
            targets=targets,
            confidence=confidence,
            resolution_path=resolution_path,
        )
    typed = _resolve_typed_receiver(index, call, var_types=resolved_types)
    if typed is None:
        return ResolvedCall(call=call, targets=[], confidence="unresolved", resolution_path="")
    targets, confidence, resolution_path = typed
    return ResolvedCall(
        call=call,
        targets=targets,
        confidence=confidence,
        resolution_path=resolution_path,
    )


def resolve_constructor_class_key(
    index: DefIndex,
    call: CallInfo,
) -> str | None:
    """Resolve a constructor call to its class key.

    Parameters
    ----------
    index : DefIndex
        Definition index.
    call : CallInfo
        Call site (assumed to be ClassName()).

    Returns:
    -------
    str | None
        Class key if resolved.
    """
    callee = call.callee_name

    # Direct class name
    classes = index.find_class_by_name(callee)
    if classes:
        return classes[0].key

    # Imported class
    _mod, sym = index.resolve_import_alias(call.file, callee)
    if sym:
        classes = index.find_class_by_name(sym)
        if classes:
            return classes[0].key

    return None
