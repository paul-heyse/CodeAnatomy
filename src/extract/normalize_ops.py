"""Registry of normalization operations used by relationship evidence."""

from __future__ import annotations

from dataclasses import dataclass
from functools import cache

from normalize.op_specs import normalize_op_specs
from relspec.rules.cache import rule_definitions_cached
from relspec.rules.definitions import RuleDefinition


@dataclass(frozen=True)
class NormalizeOp:
    """Named normalization operator mapping inputs to outputs."""

    name: str
    inputs: tuple[str, ...]
    outputs: tuple[str, ...]


def _normalize_rule_ops() -> tuple[NormalizeOp, ...]:
    specs = normalize_op_specs(_normalize_rule_definitions())
    return tuple(
        NormalizeOp(name=spec.name, inputs=spec.inputs, outputs=spec.outputs) for spec in specs
    )


@cache
def _normalize_rule_definitions() -> tuple[RuleDefinition, ...]:
    return rule_definitions_cached("normalize")


_NORMALIZE_OPS: tuple[NormalizeOp, ...] = _normalize_rule_ops()

_OPS_BY_NAME: dict[str, NormalizeOp] = {op.name: op for op in _NORMALIZE_OPS}
_OPS_BY_OUTPUT: dict[str, tuple[NormalizeOp, ...]] = {}
for _op in _NORMALIZE_OPS:
    for _output in _op.outputs:
        _OPS_BY_OUTPUT.setdefault(_output, ())
        _OPS_BY_OUTPUT[_output] = (*_OPS_BY_OUTPUT[_output], _op)


def normalize_op(name: str) -> NormalizeOp:
    """Return a normalization operator by name.

    Returns
    -------
    NormalizeOp
        Registered normalization operator.
    """
    return _OPS_BY_NAME[name]


def normalize_ops() -> tuple[NormalizeOp, ...]:
    """Return all registered normalization operators.

    Returns
    -------
    tuple[NormalizeOp, ...]
        Normalization operators in registry order.
    """
    return _NORMALIZE_OPS


def normalize_ops_for_output(name: str) -> tuple[NormalizeOp, ...]:
    """Return normalization operators that produce a given output.

    Returns
    -------
    tuple[NormalizeOp, ...]
        Operators producing the output name.
    """
    return _OPS_BY_OUTPUT.get(name, ())


__all__ = ["NormalizeOp", "normalize_op", "normalize_ops", "normalize_ops_for_output"]
