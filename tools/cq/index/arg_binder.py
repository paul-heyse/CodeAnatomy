"""Argument to parameter binding for call analysis.

Maps call-site arguments to callee parameter names for taint propagation.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass, field

from tools.cq.index.def_index import FnDecl, ParamInfo

_SELF_CLS: set[str] = {"self", "cls"}


@dataclass
class BoundArg:
    """A single argument bound to a parameter.

    Parameters
    ----------
    param_name : str
        Name of the parameter.
    arg_index : int | None
        Positional index if positional arg.
    arg_value : str
        String representation of the argument expression.
    is_keyword : bool
        Whether passed as keyword argument.
    is_starred : bool
        Whether passed with * or **.
    """

    param_name: str
    arg_index: int | None
    arg_value: str
    is_keyword: bool = False
    is_starred: bool = False


@dataclass
class BoundCall:
    """Complete argument binding for a call.

    Parameters
    ----------
    callee : FnDecl
        Target function declaration.
    bindings : list[BoundArg]
        Bound arguments.
    unbound_args : list[str]
        Arguments that couldn't be bound.
    unbound_params : list[str]
        Parameters without arguments (will use defaults).
    has_var_positional : bool
        Whether callee has *args.
    has_var_keyword : bool
        Whether callee has **kwargs.
    """

    callee: FnDecl
    bindings: list[BoundArg] = field(default_factory=list)
    unbound_args: list[str] = field(default_factory=list)
    unbound_params: list[str] = field(default_factory=list)
    has_var_positional: bool = False
    has_var_keyword: bool = False


@dataclass
class _BindContext:
    positional_params: list[ParamInfo]
    keyword_only_params: dict[str, ParamInfo]
    var_positional: ParamInfo | None
    var_keyword: ParamInfo | None
    param_offset: int
    result: BoundCall
    used_params: set[str]


def _arg_to_str(arg: ast.expr) -> str:
    """Convert argument expression to string.

    Returns
    -------
    str
        String representation of the argument expression.
    """
    try:
        return ast.unparse(arg)
    except (ValueError, TypeError):
        return "<expr>"


def _categorize_params(
    callee: FnDecl,
) -> tuple[
    list[ParamInfo],
    dict[str, ParamInfo],
    ParamInfo | None,
    ParamInfo | None,
    int,
]:
    positional_params: list[ParamInfo] = []
    keyword_only_params: dict[str, ParamInfo] = {}
    var_positional: ParamInfo | None = None
    var_keyword: ParamInfo | None = None
    for param in callee.params:
        if param.kind == "VAR_POSITIONAL":
            var_positional = param
        elif param.kind == "VAR_KEYWORD":
            var_keyword = param
        elif param.kind == "KEYWORD_ONLY":
            keyword_only_params[param.name] = param
        else:
            positional_params.append(param)
    param_offset = (
        1
        if (callee.is_method and positional_params and positional_params[0].name in _SELF_CLS)
        else 0
    )
    return (
        positional_params,
        keyword_only_params,
        var_positional,
        var_keyword,
        param_offset,
    )


def _bind_positional_args(args: list[ast.expr], context: _BindContext) -> list[str]:
    starred_args: list[str] = []
    for index, arg in enumerate(args):
        arg_str = _arg_to_str(arg)
        if isinstance(arg, ast.Starred):
            starred_args.append(arg_str)
            continue
        param_idx = index + context.param_offset
        if param_idx < len(context.positional_params):
            param = context.positional_params[param_idx]
            context.result.bindings.append(
                BoundArg(
                    param_name=param.name,
                    arg_index=index,
                    arg_value=arg_str,
                    is_keyword=False,
                )
            )
            context.used_params.add(param.name)
        elif context.var_positional:
            context.result.bindings.append(
                BoundArg(
                    param_name=context.var_positional.name,
                    arg_index=index,
                    arg_value=arg_str,
                    is_keyword=False,
                    is_starred=True,
                )
            )
        else:
            context.result.unbound_args.append(arg_str)
    return starred_args


def _bind_starred_args(starred_args: list[str], context: _BindContext) -> None:
    for starred in starred_args:
        if context.var_positional:
            context.result.bindings.append(
                BoundArg(
                    param_name=context.var_positional.name,
                    arg_index=None,
                    arg_value=starred,
                    is_starred=True,
                )
            )
        else:
            context.result.unbound_args.append(starred)


def _lookup_param(
    keyword: str,
    positional_params: list[ParamInfo],
    keyword_only_params: dict[str, ParamInfo],
) -> ParamInfo | None:
    for param in positional_params:
        if param.name == keyword:
            return param
    return keyword_only_params.get(keyword)


def _bind_keyword_args(
    keywords: list[ast.keyword],
    context: _BindContext,
) -> list[str]:
    double_starred: list[str] = []
    for kw in keywords:
        kw_val = _arg_to_str(kw.value)
        if kw.arg is None:
            double_starred.append(kw_val)
            continue
        kw_name = kw.arg
        param = _lookup_param(
            kw_name,
            context.positional_params,
            context.keyword_only_params,
        )
        if param is not None and param.name not in context.used_params:
            context.result.bindings.append(
                BoundArg(
                    param_name=param.name,
                    arg_index=None,
                    arg_value=kw_val,
                    is_keyword=True,
                )
            )
            context.used_params.add(param.name)
        elif context.var_keyword:
            context.result.bindings.append(
                BoundArg(
                    param_name=context.var_keyword.name,
                    arg_index=None,
                    arg_value=kw_val,
                    is_keyword=True,
                    is_starred=True,
                )
            )
        else:
            context.result.unbound_args.append(f"{kw_name}={kw_val}")
    return double_starred


def _bind_double_starred_args(
    double_starred: list[str],
    context: _BindContext,
) -> None:
    for starred in double_starred:
        if context.var_keyword:
            context.result.bindings.append(
                BoundArg(
                    param_name=context.var_keyword.name,
                    arg_index=None,
                    arg_value=starred,
                    is_starred=True,
                )
            )
        else:
            context.result.unbound_args.append(starred)


def _collect_unbound_params(
    context: _BindContext,
) -> None:
    for param in context.positional_params[context.param_offset :]:
        if param.name not in context.used_params and param.default is None:
            context.result.unbound_params.append(param.name)
    for name, param in context.keyword_only_params.items():
        if name not in context.used_params and param.default is None:
            context.result.unbound_params.append(name)


def bind_call_to_params(
    args: list[ast.expr],
    keywords: list[ast.keyword],
    callee: FnDecl,
) -> BoundCall:
    """Bind call arguments to callee parameters.

    Parameters
    ----------
    args : list[ast.expr]
        Positional arguments from call.
    keywords : list[ast.keyword]
        Keyword arguments from call.
    callee : FnDecl
        Target function.

    Returns
    -------
    BoundCall
        Binding result.
    """
    result = BoundCall(callee=callee)
    (
        positional_params,
        keyword_only_params,
        var_positional,
        var_keyword,
        param_offset,
    ) = _categorize_params(callee)
    result.has_var_positional = var_positional is not None
    result.has_var_keyword = var_keyword is not None
    context = _BindContext(
        positional_params=positional_params,
        keyword_only_params=keyword_only_params,
        var_positional=var_positional,
        var_keyword=var_keyword,
        param_offset=param_offset,
        result=result,
        used_params=set(),
    )
    starred_args = _bind_positional_args(args, context)
    _bind_starred_args(starred_args, context)
    double_starred = _bind_keyword_args(keywords, context)
    _bind_double_starred_args(double_starred, context)
    _collect_unbound_params(context)
    return result


def tainted_params_from_bound_call(
    bound: BoundCall,
    tainted_args: set[int] | set[str],
) -> set[str]:
    """Determine which parameters receive tainted values.

    Parameters
    ----------
    bound : BoundCall
        Binding information.
    tainted_args : set[int] | set[str]
        Tainted argument indices (positional) or values (expressions).

    Returns
    -------
    set[str]
        Parameter names that receive tainted values.
    """
    tainted_params: set[str] = set()

    for binding in bound.bindings:
        # Check by index
        if binding.arg_index is not None and binding.arg_index in tainted_args:
            tainted_params.add(binding.param_name)
        # Check by value
        if binding.arg_value in tainted_args:
            tainted_params.add(binding.param_name)

    return tainted_params


def expand_unknown_taint(bound: BoundCall) -> set[str]:
    """When call has *args/**kwargs, conservatively taint variadic params.

    Parameters
    ----------
    bound : BoundCall
        Binding information.

    Returns
    -------
    set[str]
        Potentially tainted parameter names.
    """
    tainted: set[str] = set()

    # Any starred bindings could taint variadic params
    for binding in bound.bindings:
        if binding.is_starred:
            tainted.add(binding.param_name)

    return tainted
