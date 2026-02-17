"""Pure signature parsing helpers."""

from __future__ import annotations

import ast
import re

import msgspec


class SigParam(msgspec.Struct, frozen=True):
    """Parsed signature parameter."""

    name: str
    has_default: bool
    is_kwonly: bool
    is_vararg: bool
    is_kwarg: bool


def parse_signature(sig: str) -> list[SigParam]:
    """Parse a signature string like ``foo(a, b, *, c=None)`` into params."""
    match = re.match(r"^\s*\w+\s*\((.*)\)\s*$", sig.strip())
    inside = match.group(1) if match else sig.strip().strip("()")

    tmp = f"def _tmp({inside}):\n  pass\n"
    try:
        tree = ast.parse(tmp)
        fn = tree.body[0]
        if not isinstance(fn, ast.FunctionDef):
            return []
        args = fn.args
    except SyntaxError:
        return []

    params: list[SigParam] = []

    defaults_start = len(args.args) - len(args.defaults)
    for i, arg in enumerate(args.args):
        params.append(
            SigParam(
                name=arg.arg,
                has_default=i >= defaults_start,
                is_kwonly=False,
                is_vararg=False,
                is_kwarg=False,
            )
        )

    if args.vararg:
        params.append(
            SigParam(
                name=args.vararg.arg,
                has_default=False,
                is_kwonly=False,
                is_vararg=True,
                is_kwarg=False,
            )
        )

    for i, arg in enumerate(args.kwonlyargs):
        params.append(
            SigParam(
                name=arg.arg,
                has_default=args.kw_defaults[i] is not None,
                is_kwonly=True,
                is_vararg=False,
                is_kwarg=False,
            )
        )

    if args.kwarg:
        params.append(
            SigParam(
                name=args.kwarg.arg,
                has_default=False,
                is_kwonly=False,
                is_vararg=False,
                is_kwarg=True,
            )
        )

    return params


__all__ = ["SigParam", "parse_signature"]
