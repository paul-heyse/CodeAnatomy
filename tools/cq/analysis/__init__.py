"""Pure analysis helpers shared across CQ macros."""

from tools.cq.analysis.calls import (
    classify_call_against_signature,
    classify_calls_against_signature,
)
from tools.cq.analysis.signature import SigParam, parse_signature
from tools.cq.analysis.taint import TaintedSite, analyze_function_node, find_function_node
from tools.cq.analysis.visitors import ExceptionVisitor, ImportVisitor, SideEffectVisitor

__all__ = [
    "ExceptionVisitor",
    "ImportVisitor",
    "SideEffectVisitor",
    "SigParam",
    "TaintedSite",
    "analyze_function_node",
    "classify_call_against_signature",
    "classify_calls_against_signature",
    "find_function_node",
    "parse_signature",
]
